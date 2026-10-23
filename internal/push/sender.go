// Package push is the direct send mode of push notifications. A sender node receives explicit provider tokens, sends
// them to APNs and FCM inline, and returns one raw result per recipient. The mode keeps no state: there is no device
// registry, no queue, no retry and no fan-out. It is PRO's raw-token send made synchronous.
package push

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider/apns"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider/fcm"
)

// writeMargin is the time the send route adds to its write deadline after the longest inline run.
const writeMargin = 5 * time.Second

// Environments of an APNs target.
const (
	environmentProduction  = "production"
	environmentDevelopment = "development"
)

// Values of the direct send request counter.
const (
	requestCompleted = "completed"
	requestRefused   = "refused"
)

// Sender is the direct send mode of a node. It is nil when push notifications are off. A node that is not a sender
// keeps no credential and answers every send with a refusal.
type Sender struct {
	sender          bool
	providers       map[string]bool
	poolSizes       map[string]int
	apnsEndpoint    string
	maxRecipients   int
	maxRequestBytes int
	syncTimeout     time.Duration
	requestTimeout  time.Duration
	calls           *semaphore.Weighted
	set             *provider.Set
}

// apnsRouter sends each APNs target through the client of its environment. Both environments share one credential,
// and the environment chooses only the host.
type apnsRouter struct {
	clients map[string]*apns.Client
}

// Send sends the target through the client of its environment. A target with an unknown environment is rejected, and
// the request validator refuses such a target before any call.
func (r *apnsRouter) Send(ctx context.Context, job *provider.Job) *apiproto.PushItemResult {
	client, ok := r.clients[job.Target.GetEnvironment()]
	if !ok {
		return provider.NewResult(job, provider.StatusRejected)
	}
	return client.Send(ctx, job)
}

// Setup returns the direct send mode of cfg, or nil when push notifications are off. Only a sender node that is not in
// dry run loads the credentials of its enabled providers, once, here: a key change needs a restart. processCtx is the
// process-lifetime context, which ends the FCM token fetches at shutdown.
func Setup(processCtx context.Context, cfg configtypes.PushNotifications) (*Sender, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	s := newSender(cfg)
	if !cfg.Sender {
		return s, nil
	}
	if cfg.DryRun {
		s.set = provider.NewDryRunSet()
		return s, nil
	}

	s.set = provider.NewSet()
	err := s.load(processCtx, cfg)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// newSender returns the sender of cfg with its limits and no provider loaded.
func newSender(cfg configtypes.PushNotifications) *Sender {
	s := &Sender{
		sender:          cfg.Sender,
		providers:       make(map[string]bool),
		poolSizes:       make(map[string]int),
		apnsEndpoint:    cfg.APNs.Endpoint,
		maxRecipients:   cfg.MaxRecipientItems,
		maxRequestBytes: cfg.MaxRequestBytes,
		syncTimeout:     time.Duration(cfg.SyncTimeout),
		requestTimeout:  time.Duration(cfg.Providers.RequestTimeout),
		calls:           semaphore.NewWeighted(int64(cfg.MaxConcurrentRequests)),
	}
	for _, name := range cfg.EnabledProviders {
		s.providers[name] = true
	}
	if s.providers[provider.ProviderAPNs] {
		s.poolSizes[provider.ProviderAPNs] = poolSize(cfg.APNs.TokensBatchSize, cfg.APNs.MaxInflight)
	}
	if s.providers[provider.ProviderFCM] {
		s.poolSizes[provider.ProviderFCM] = poolSize(cfg.FCM.TokensBatchSize, cfg.FCM.MaxInflight)
	}
	return s
}

// load loads the credentials of the enabled providers into the set. It returns the first load error, which names the
// configuration key and never the key content.
func (s *Sender) load(processCtx context.Context, cfg configtypes.PushNotifications) error {
	if s.providers[provider.ProviderAPNs] {
		key, err := provider.LoadAPNSKey(cfg.APNs.TokenKeyFile, cfg.APNs.TokenKeyPEM)
		if err != nil {
			return err
		}
		credential, err := apns.NewCredential(key, cfg.APNs.TokenKeyID, cfg.APNs.TokenTeamID)
		if err != nil {
			return err
		}

		router := &apnsRouter{
			clients: make(map[string]*apns.Client),
		}
		for _, environment := range []string{environmentProduction, environmentDevelopment} {
			client, err := apns.NewClient(credential, environment, cfg.APNs.BundleID)
			if err != nil {
				return err
			}
			router.clients[environment] = client
		}
		s.set.Add(provider.ProviderAPNs, router, int64(cfg.APNs.MaxInflight))
	}

	if s.providers[provider.ProviderFCM] {
		account, err := provider.ReadFCMServiceAccount(cfg.FCM.CredentialsFile)
		if err != nil {
			return err
		}
		client, err := fcm.NewClient(processCtx, account, time.Duration(cfg.Providers.RequestTimeout))
		if err != nil {
			return err
		}
		s.set.Add(provider.ProviderFCM, client, int64(cfg.FCM.MaxInflight))
	}
	return nil
}

// poolSize returns the worker count of one provider group of a call: min(tokens_batch_size, max_inflight).
func poolSize(tokensBatchSize int, maxInflight int) int {
	if tokensBatchSize < maxInflight {
		return tokensBatchSize
	}
	return maxInflight
}

// IsSender reports whether the node sends: push notifications are on and the node is a sender.
func (s *Sender) IsSender() bool {
	return s != nil && s.sender
}

// MaxRequestBytes returns the size cap of a send request body.
func (s *Sender) MaxRequestBytes() int64 {
	if s == nil {
		return 0
	}
	return int64(s.maxRequestBytes)
}

// WriteBound returns the longest time a send response can take: the sync wait, the provider timeout and a margin. The
// send route sets its write deadline to this bound from the request start.
func (s *Sender) WriteBound() time.Duration {
	if s == nil {
		return 0
	}
	return s.syncTimeout + s.requestTimeout + writeMargin
}

// Send runs one direct send. It answers with one result per recipient, in request order, or with the error of its
// first refusal, in which case nothing is sent.
func (s *Sender) Send(ctx context.Context, req *apiproto.SendPushNotificationRequest) *apiproto.SendPushNotificationResponse {
	if s == nil {
		return errorResponse(notEnabledError())
	}
	if !s.sender {
		return errorResponse(senderOnlyError())
	}

	jobs, refusal := s.plan(req)
	if refusal != nil {
		countRequest(requestRefused)
		return errorResponse(refusal)
	}

	if !s.calls.TryAcquire(1) {
		countRequest(requestRefused)
		return errorResponse(tooManyCallsError())
	}
	defer s.calls.Release(1)

	uid := req.GetUid()
	if uid == "" {
		generated, err := uuid.NewV7()
		if err != nil {
			countRequest(requestRefused)
			return errorResponse(internalError())
		}
		uid = generated.String()
	}

	results := s.run(ctx, jobs)
	countRequest(requestCompleted)
	log.Debug().Int("targets", len(jobs)).Msg("direct push send")

	return &apiproto.SendPushNotificationResponse{
		Result: &apiproto.SendPushNotificationResult{
			Uid:     uid,
			Results: results,
		},
	}
}

// countRequest counts one send call by its result, when push metrics are registered.
func countRequest(result string) {
	counter := metrics.PushDirectRequests
	if counter == nil {
		return
	}
	counter.WithLabelValues(result).Inc()
}

// errorResponse returns a response that carries only refusal.
func errorResponse(refusal *apiproto.Error) *apiproto.SendPushNotificationResponse {
	return &apiproto.SendPushNotificationResponse{
		Error: refusal,
	}
}
