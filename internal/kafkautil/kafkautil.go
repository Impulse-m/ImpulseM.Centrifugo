// Package kafkautil builds franz-go client options shared by Kafka consumers and sources.
package kafkautil

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/logging"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
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
}

// Options returns franz-go options for seed brokers, TLS, SASL and logging.
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
			opts = append(opts, kgo.SASL(aws.Auth{
				AccessKey: config.SASLUser,
				SecretKey: config.SASLPassword,
			}.AsManagedStreamingIAMMechanism()))
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", config.SASLMechanism)
		}
	}
	return opts, nil
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
