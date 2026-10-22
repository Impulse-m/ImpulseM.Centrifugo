// Package kafkautil builds franz-go client options shared by Kafka consumers and sources.
package kafkautil

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/logging"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsv2cfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	mskaws "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"golang.org/x/sync/singleflight"
)

// Config is a connection configuration of a Kafka client.
type Config struct {
	// Name identifies the client in TLS log entries, it is prefixed with "kafka:".
	Name string
	// Brokers is a list of seed brokers.
	Brokers []string
	// TLS is a TLS configuration of the connection.
	TLS configtypes.TLSConfig
	// SASLMechanism when not empty enables SASL auth.
	SASLMechanism string
	SASLUser      string
	SASLPassword  string
	// AssumeRoleARN, when set with aws-msk-iam, obtains MSK IAM credentials by STS AssumeRole.
	AssumeRoleARN string
}

// Options returns franz-go options for seed brokers, logging, TLS and SASL. It mirrors the SASL, TLS and
// MSK IAM client setup of upstream internal/consuming/kafka.go (initClient); keep the two in sync on upstream merges.
func Options(config Config, log zerolog.Logger) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.WithLogger(&zerologAdapter{log: log}),
	}
	if config.TLS.Enabled {
		tlsConfig, err := config.TLS.ToGoTLSConfig("kafka:" + config.Name)
		if err != nil {
			return nil, fmt.Errorf("error making TLS configuration: %w", err)
		}
		dialer := &tls.Dialer{
			NetDialer: &net.Dialer{Timeout: 10 * time.Second},
			Config:    tlsConfig,
		}
		opts = append(opts, kgo.Dialer(dialer.DialContext))
	}

	if config.SASLMechanism != "" {
		switch config.SASLMechanism {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: config.SASLUser,
				Pass: config.SASLPassword,
			}.AsMechanism()))
		case "scram-sha-256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: config.SASLUser,
				Pass: config.SASLPassword,
			}.AsSha256Mechanism()))
		case "scram-sha-512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: config.SASLUser,
				Pass: config.SASLPassword,
			}.AsSha512Mechanism()))
		case "aws-msk-iam":
			if config.AssumeRoleARN != "" {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				mskAuth, err := newMSKAssumeRoleAuth(ctx, config.AssumeRoleARN)
				cancel()
				if err != nil {
					return nil, fmt.Errorf("load AWS config for MSK IAM assume role: %w", err)
				}
				opts = append(opts, kgo.SASL(mskaws.ManagedStreamingIAM(func(ctx context.Context) (mskaws.Auth, error) {
					return mskAuth.auth(ctx)
				})))
			} else {
				opts = append(opts, kgo.SASL(mskaws.Auth{
					AccessKey: config.SASLUser,
					SecretKey: config.SASLPassword,
				}.AsManagedStreamingIAMMechanism()))
			}
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", config.SASLMechanism)
		}
	}
	return opts, nil
}

// mskAssumeRoleSessionName is the STS RoleSessionName for AssumeRole calls.
const mskAssumeRoleSessionName = "centrifugo-msk"

// mskCredentialsLifetimeShare is the part of the lifetime of assumed credentials after which they are assumed again.
// The rest is the margin for a handshake which starts close to the expiry.
const mskCredentialsLifetimeShare = 0.75

// mskAssumeRoleTimeout caps one AssumeRole call. The call runs on its own context, so it is bounded by this and not by
// the handshake which starts it.
const mskAssumeRoleTimeout = 10 * time.Second

// mskAssumeRoleKey is the singleflight key of the AssumeRole call: one call is in flight at a time per auth.
const mskAssumeRoleKey = "assume-role"

// assumeRoler is the STS call of the MSK auth. Satisfied by *sts.Client.
type assumeRoler interface {
	AssumeRole(ctx context.Context, params *sts.AssumeRoleInput, optFns ...func(*sts.Options)) (*sts.AssumeRoleOutput, error)
}

// mskAssumeRoleAuth obtains STS credentials for MSK IAM SASL handshakes. The credentials are kept until
// mskCredentialsLifetimeShare of their lifetime has passed, so the handshakes of a reconnect burst share one AssumeRole
// call. They are never returned past their expiry, which is the failure of CredentialsCache in franz-go #731.
// RoleSessionName stays constant so the broker principal is stable across re-auth (see aws-msk-iam-auth README).
type mskAssumeRoleAuth struct {
	stsClient assumeRoler
	roleARN   string
	now       func() time.Time

	// mu guards the cached credentials. The AssumeRole call runs outside mu, under group: concurrent handshakes share
	// one call, and each waits on it with its own context, so a slow call does not hold a handshake past its deadline.
	mu        sync.Mutex
	creds     mskaws.Auth
	refreshAt time.Time
	cached    bool
	group     singleflight.Group
}

func newMSKAssumeRoleAuth(ctx context.Context, roleARN string) (*mskAssumeRoleAuth, error) {
	awsCfg, err := awsv2cfg.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &mskAssumeRoleAuth{
		stsClient: sts.NewFromConfig(awsCfg),
		roleARN:   roleARN,
		now:       time.Now,
	}, nil
}

// auth returns the credentials of the cache, or assumes the role when the cache has none to share. A waiting handshake
// returns when ctx is done, even while the call it waits on runs.
func (a *mskAssumeRoleAuth) auth(ctx context.Context) (mskaws.Auth, error) {
	a.mu.Lock()
	if a.cached && a.now().Before(a.refreshAt) {
		creds := a.creds
		a.mu.Unlock()
		return creds, nil
	}
	a.mu.Unlock()
	result := a.group.DoChan(mskAssumeRoleKey, func() (any, error) {
		return a.assumeRole(ctx)
	})
	select {
	case res := <-result:
		if res.Err != nil {
			return mskaws.Auth{}, res.Err
		}
		return res.Val.(mskaws.Auth), nil
	case <-ctx.Done():
		return mskaws.Auth{}, ctx.Err()
	}
}

// assumeRole calls AssumeRole on a context detached from the handshake which started it, capped by mskAssumeRoleTimeout,
// and caches the credentials. The shared call must not end because the first handshake gave up.
func (a *mskAssumeRoleAuth) assumeRole(ctx context.Context) (mskaws.Auth, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mskAssumeRoleTimeout)
	defer cancel()
	now := a.now()
	out, err := a.stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(a.roleARN),
		RoleSessionName: aws.String(mskAssumeRoleSessionName),
	})
	if err != nil {
		return mskaws.Auth{}, err
	}
	if out.Credentials == nil {
		return mskaws.Auth{}, errors.New("assume role returned no credentials")
	}
	creds := out.Credentials
	if creds.AccessKeyId == nil || creds.SecretAccessKey == nil {
		return mskaws.Auth{}, errors.New("assume role returned incomplete credentials")
	}
	sessionToken := ""
	if creds.SessionToken != nil {
		sessionToken = *creds.SessionToken
	}
	auth := mskaws.Auth{
		AccessKey:    *creds.AccessKeyId,
		SecretKey:    *creds.SecretAccessKey,
		SessionToken: sessionToken,
	}
	// Credentials without an expiry are not cached: there is no lifetime to share.
	if creds.Expiration != nil {
		lifetime := creds.Expiration.Sub(now)
		a.mu.Lock()
		a.creds = auth
		a.refreshAt = now.Add(time.Duration(float64(lifetime) * mskCredentialsLifetimeShare))
		a.cached = true
		a.mu.Unlock()
	}
	return auth, nil
}

// zerologAdapter adapts zerolog.Logger to kgo.Logger interface.
type zerologAdapter struct {
	log zerolog.Logger
}

func (l *zerologAdapter) Level() kgo.LogLevel {
	switch {
	case logging.Enabled(logging.TraceLevel):
		return kgo.LogLevelDebug // Kafka client debug level is more like our trace.
	case logging.Enabled(logging.InfoLevel):
		return kgo.LogLevelInfo
	case logging.Enabled(logging.WarnLevel):
		return kgo.LogLevelWarn
	case logging.Enabled(logging.ErrorLevel):
		return kgo.LogLevelError
	default:
		return kgo.LogLevelNone
	}
}

func (l *zerologAdapter) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	var e *zerolog.Event
	switch level {
	case kgo.LogLevelError:
		e = l.log.Error()
	case kgo.LogLevelWarn:
		e = l.log.Warn()
	case kgo.LogLevelInfo:
		e = l.log.Info()
	case kgo.LogLevelDebug:
		e = l.log.Trace() // Kafka client debug level is more like our trace.
	default:
		return
	}
	for i := 0; i+1 < len(keyvals); i += 2 {
		if key, ok := keyvals[i].(string); ok {
			e = e.Interface(key, keyvals[i+1])
		}
	}
	e.Msg(msg)
}
