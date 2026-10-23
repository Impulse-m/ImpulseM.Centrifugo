// Package push is the direct send mode of push notifications. A sender node receives explicit provider tokens, sends
// them to APNs and FCM inline, and returns one raw result per recipient. The mode keeps no state: there is no device
// registry, no queue, no retry and no fan-out. The send is inline: the response carries the provider answers.
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
	// shutdown is the process-lifetime context. Its end stops the waits and the calls of every send.
	shutdown context.Context
}

// writeDeadlineKey is the context key of the write deadline setter of the send route.
type writeDeadlineKey struct{}

// apnsRouter sends each APNs target through the client of its environment. Both environments share one credential,
// and the environment chooses only the host.
type apnsRouter struct {
	clients map[string]*apns.Client
}

// Send sends the target through the client of its environment. The request validator has already refused every
// environment other than production and development, so the lookup always finds a client.
func (r *apnsRouter) Send(ctx context.Context, job *provider.Job) *apiproto.PushItemResult {
	return r.clients[job.Target.GetEnvironment()].Send(ctx, job)
}

// Setup returns the direct send mode of cfg, or nil when push notifications are off. Only a sender node that is not in
// dry run loads the credentials of its enabled providers, once, here: a key change needs a restart. processCtx is the
// process-lifetime context, which ends the FCM token fetches at shutdown.
func Setup(processCtx context.Context, cfg configtypes.PushNotifications) (*Sender, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	s := newSender(cfg)
	s.shutdown = processCtx
	if !cfg.Sender {
		return s, nil
	}
	if cfg.DryRun {
		s.set = provider.NewDryRunSet().WithOversizeCheck(oversized).WithShutdown(processCtx)
		return s, nil
	}

	s.set = provider.NewSet().WithOversizeCheck(oversized).WithShutdown(processCtx)
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
		for _, environment := range []string{provider.EnvironmentProduction, provider.EnvironmentDevelopment} {
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
	return min(tokensBatchSize, maxInflight)
}

// oversized reports whether the item of job is over the payload ceiling of its provider. The set calls it before the dry
// run answer, and the live providers use the same functions, so a dry run answers what a live send answers. For an FCM
// item it also builds the provider body once, which the send then reuses.
func oversized(job *provider.Job) bool {
	switch job.Target.GetProvider() {
	case provider.ProviderAPNs:
		return apns.Oversized(job)
	case provider.ProviderFCM:
		return fcm.Prepare(job)
	default:
		return false
	}
}

// admittedKey is the context key of a call that already holds a call slot of the node.
type admittedKey struct{}

// WithAdmission marks ctx as a call that holds a call slot taken by Admit. Send then takes no second slot for it.
func WithAdmission(ctx context.Context) context.Context {
	return context.WithValue(ctx, admittedKey{}, true)
}

// isAdmitted reports whether ctx is marked by WithAdmission.
func isAdmitted(ctx context.Context) bool {
	admitted, ok := ctx.Value(admittedKey{}).(bool)
	return ok && admitted
}

// Admit takes one call slot of the node. It answers the too-many-requests error when max_concurrent_requests calls hold
// the slots. The send route calls it before it reads the body, so the bodies of the calls in flight are bounded by the
// slots. The returned release frees the slot, and it is called once.
func (s *Sender) Admit() (func(), *apiproto.Error) {
	if !s.IsSender() {
		return func() {}, nil
	}
	if !s.calls.TryAcquire(1) {
		return func() {}, tooManyCallsError()
	}
	return func() {
		s.calls.Release(1)
	}, nil
}

// CountRefusedRequest counts one send call that was refused before the send path, when push metrics are registered.
func CountRefusedRequest() {
	countRequest(requestRefused)
}

// SenderOnlyError returns the answer of a node that is not a sender. The send route answers it before it reads the
// body, and Send answers it for any other caller.
func SenderOnlyError() *apiproto.Error {
	return &apiproto.Error{
		Code:    apiproto.ErrorNotAvailable.Code,
		Message: "direct send is available on sender nodes only",
	}
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

// DefaultCallTimeout returns the deadline of a call that sets no timeout_ms: the sync wait, the provider timeout and a
// margin. It is the longest time a send response can take.
func (s *Sender) DefaultCallTimeout() time.Duration {
	if s == nil {
		return 0
	}
	return s.syncTimeout + s.requestTimeout + writeMargin
}

// CallTimeout returns the deadline of a call with timeoutMs: the default when timeoutMs is zero, and timeoutMs when it is
// positive and below the default. A longer value is clamped to the default, not refused.
func (s *Sender) CallTimeout(timeoutMs uint32) time.Duration {
	limit := s.DefaultCallTimeout()
	if timeoutMs == 0 {
		return limit
	}
	return min(time.Duration(timeoutMs)*time.Millisecond, limit)
}

// WithWriteDeadline returns ctx with set as the setter of the write deadline of the response. The send route passes the
// setter of its writer. Send calls it once, with the deadline of the call, so the response is written within the call.
func WithWriteDeadline(ctx context.Context, set func(time.Time)) context.Context {
	return context.WithValue(ctx, writeDeadlineKey{}, set)
}

// setWriteDeadline calls the write deadline setter of ctx, when there is one.
func setWriteDeadline(ctx context.Context, deadline time.Time) {
	set, ok := ctx.Value(writeDeadlineKey{}).(func(time.Time))
	if !ok || set == nil {
		return
	}
	set(deadline)
}

// Send runs one direct send. It answers with one result per recipient, in request order, or with the error of its
// first refusal, in which case nothing is sent.
func (s *Sender) Send(ctx context.Context, req *apiproto.SendPushNotificationRequest) *apiproto.SendPushNotificationResponse {
	if s == nil {
		return errorResponse(NotEnabledError())
	}
	if !s.sender {
		countRequest(requestRefused)
		return errorResponse(SenderOnlyError())
	}

	jobs, refusal := s.plan(req)
	if refusal != nil {
		countRequest(requestRefused)
		return errorResponse(refusal)
	}

	// A call from the send route holds its slot already. Any other call takes one here.
	if !isAdmitted(ctx) {
		release, busy := s.Admit()
		if busy != nil {
			countRequest(requestRefused)
			return errorResponse(busy)
		}
		defer release()
	}

	uid := req.GetUid()
	if uid == "" {
		generated, err := uuid.NewV7()
		if err != nil {
			countRequest(requestRefused)
			return errorResponse(internalError())
		}
		uid = generated.String()
	}

	// The call deadline is set from the request, once it is decoded. The response is written within it.
	callDeadline := time.Now().Add(s.CallTimeout(req.GetTimeoutMs()))
	setWriteDeadline(ctx, callDeadline)

	results := s.run(ctx, jobs, callDeadline)
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
