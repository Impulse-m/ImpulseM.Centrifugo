package accessrevoke

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/kafkautil"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/cenkalti/backoff/v5"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var _ Source = (*KafkaSource)(nil)

const (
	// seedBackoffMin is the nominal delay before the second offset listing while the seed fails.
	seedBackoffMin time.Duration = time.Second
	// seedBackoffMax caps the nominal delay, so that a strict node accepts connects at most about this long after Kafka
	// recovers.
	seedBackoffMax time.Duration = 10 * time.Second
	// seedLogInterval is the shortest time between two warnings of a failed seed.
	seedLogInterval time.Duration = 30 * time.Second
	// skipLogInterval is the shortest time between two warnings of skipped records of one reason, and between two
	// warnings of records which expire beyond the window. The metrics count every record.
	skipLogInterval time.Duration = 10 * time.Second
)

// KafkaSource reads access revocation events from a Kafka topic and applies them with a Revoker.
// It reads all partitions directly, without consumer group and without commits. On every start it
// reads from now minus window, so events older than the window are not replayed.
type KafkaSource struct {
	source       configtypes.AccessRevokeSource
	cfgContainer *config.Container
	revoker      *Revoker
	tracker      *ReadinessTracker
	clock        func() time.Time
	// seedLog limits the warnings of a failed seed.
	seedLog *tools.IntervalLimiter
	// skipLogs limits the warnings of skipped records, one limiter per reason.
	skipLogs map[string]*tools.IntervalLimiter
	// beyondWindowLog limits the warnings of events which expire beyond the window.
	beyondWindowLog *tools.IntervalLimiter

	// readinessMu guards the readiness timer fields below.
	readinessMu sync.Mutex
	// readinessTimer is created by the first arm.
	readinessTimer *time.Timer
	// readinessStopped is set when Run returns, so that a running callback arms nothing.
	readinessStopped bool
	// failOpenReported and caughtUpReported make each transition be reported once.
	failOpenReported atomic.Bool
	caughtUpReported atomic.Bool
}

// NewKafkaSource creates a KafkaSource for the given source configuration. The configuration must be
// validated already. Feature flags and default TTL are read from cfgContainer for every event.
func NewKafkaSource(source configtypes.AccessRevokeSource, cfgContainer *config.Container, revoker *Revoker) *KafkaSource {
	return newKafkaSource(source, cfgContainer, revoker, time.Now)
}

func newKafkaSource(
	source configtypes.AccessRevokeSource,
	cfgContainer *config.Container,
	revoker *Revoker,
	clock func() time.Time,
) *KafkaSource {
	return &KafkaSource{
		source:       source,
		cfgContainer: cfgContainer,
		revoker:      revoker,
		tracker: NewReadinessTracker(ReadinessConfig{
			StartupTimeout: source.StartupTimeout.ToDuration(),
			LagThreshold:   source.LagThreshold.ToDuration(),
			Strict:         source.RejectConnectOnLag,
		}, clock),
		clock:           clock,
		seedLog:         tools.NewIntervalLimiter(seedLogInterval),
		skipLogs:        newSkipLogs(),
		beyondWindowLog: tools.NewIntervalLimiter(skipLogInterval),
	}
}

// newSkipLogs returns a limiter of the warnings for each skip reason.
func newSkipLogs() map[string]*tools.IntervalLimiter {
	reasons := []string{skipUnknownMethod, skipDecodeError, skipValidation, skipDisabled, skipStale, skipApplyError}
	limiters := make(map[string]*tools.IntervalLimiter, len(reasons))
	for _, reason := range reasons {
		limiters[reason] = tools.NewIntervalLimiter(skipLogInterval)
	}
	return limiters
}

// Ready implements Source.
func (s *KafkaSource) Ready() bool {
	return s.tracker.Ready()
}

// Lagging implements Source.
func (s *KafkaSource) Lagging() bool {
	return s.tracker.Lagging()
}

// CaughtUpAfterFailOpen returns a channel which is closed once the source caught up after the node
// became ready by startup timeout. It is the signal for a catch-up sweep of local connections.
func (s *KafkaSource) CaughtUpAfterFailOpen() <-chan struct{} {
	return s.tracker.CaughtUpAfterFailOpen()
}

// Run implements service.Service. It returns when ctx is done.
func (s *KafkaSource) Run(ctx context.Context) error {
	startMilli := s.clock().Add(-s.source.Window.ToDuration()).UnixMilli()
	opts, err := s.clientOptions(startMilli)
	if err != nil {
		return err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	metrics.BindAccessRevokeSource(s.sourceState)
	defer metrics.BindAccessRevokeSource(nil)
	defer s.stopReadiness()
	// The readiness timer starts before seeding: when Kafka is unreachable seeding retries until the startup
	// timeout, and the fail-open transition must still be logged and exported.
	s.onReadinessTimer()

	adm := kadm.NewClient(client)
	if err := s.seed(ctx, adm, startMilli); err != nil {
		return err
	}
	s.reportTransitions(s.tracker.Snapshot())

	for {
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, fetchErr := range fetches.Errors() {
			log.Warn().Err(fetchErr.Err).Str("topic", fetchErr.Topic).Int32("partition", fetchErr.Partition).
				Msg("error fetching access revocation events")
		}
		fetches.EachPartition(s.handlePartition)
		if !s.caughtUpReported.Load() {
			s.reportTransitions(s.tracker.Snapshot())
		}
	}
}

// clientOptions returns franz-go options for direct consumption of the topic from startMilli.
func (s *KafkaSource) clientOptions(startMilli int64) ([]kgo.Opt, error) {
	connOpts, err := kafkautil.Options(kafkautil.Config{
		Name:          "access_revoke",
		Brokers:       s.source.Kafka.Brokers,
		TLS:           s.source.Kafka.TLS,
		SASLMechanism: s.source.Kafka.SASLMechanism,
		SASLUser:      s.source.Kafka.SASLUser,
		SASLPassword:  s.source.Kafka.SASLPassword,
		AssumeRoleARN: s.source.Kafka.AssumeRoleARN,
	}, log.Logger)
	if err != nil {
		return nil, err
	}
	opts := []kgo.Opt{
		kgo.ConsumeTopics(s.source.Kafka.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(startMilli)),
		kgo.FetchMaxWait(s.source.Kafka.FetchMaxWait.ToDuration()),
		// Read committed: records of aborted transactions are dropped by the client, so they are never applied.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// Control records are kept so that offsets advance past transaction markers. They are skipped when processed.
		kgo.KeepControlRecords(),
		kgo.ClientID("centrifugo-access-revoke"),
		kgo.WithHooks(&fetchHook{tracker: s.tracker}),
	}
	return append(opts, connOpts...), nil
}

// seed sets initial offsets of all partitions of the topic: the first offset from startMilli as the
// next offset, and the last stable offset as the target. Partitions without fetched records are tracked
// from these values. It retries with backoff until listing succeeds or ctx is done.
func (s *KafkaSource) seed(ctx context.Context, adm *kadm.Client, startMilli int64) error {
	return s.seedWith(ctx, func(ctx context.Context) error {
		return s.trySeed(ctx, adm, startMilli)
	})
}

// seedBackOff returns the retry schedule of the seed: the nominal delay starts at seedBackoffMin and doubles up to
// seedBackoffMax, and each delay is drawn from half the nominal delay to all of it, so that nodes started together do
// not retry in step and no delay exceeds the cap. ExponentialBackOff randomizes symmetrically around its interval, so
// the interval is three quarters of the nominal delay and the factor a third: [2I/3, 4I/3] is [d/2, d].
func seedBackOff() *backoff.ExponentialBackOff {
	return &backoff.ExponentialBackOff{
		InitialInterval:     seedBackoffMin * 3 / 4,
		RandomizationFactor: 1.0 / 3,
		Multiplier:          2,
		MaxInterval:         seedBackoffMax * 3 / 4,
	}
}

// seedWith calls try until it succeeds or ctx is done, spaced by seedBackOff, and logs a failure at most once per
// seedLogInterval. It returns nil, or the cause of ctx.
func (s *KafkaSource) seedWith(ctx context.Context, try func(ctx context.Context) error) error {
	_, err := backoff.Retry(
		ctx,
		func() (struct{}, error) {
			return struct{}{}, try(ctx)
		},
		backoff.WithBackOff(seedBackOff()),
		// The default limit of 15 minutes would end the seed in a long outage; it retries until ctx is done.
		backoff.WithMaxElapsedTime(0),
		backoff.WithNotify(func(err error, _ time.Duration) {
			if s.seedLog.Allow() {
				log.Warn().Err(err).Msg("error listing access revocation source offsets, retrying")
			}
		}),
	)
	return err
}

func (s *KafkaSource) trySeed(ctx context.Context, adm *kadm.Client, startMilli int64) error {
	topic := s.source.Kafka.Topic
	// Committed offsets are last stable offsets: they do not move past an open transaction.
	stables, err := adm.ListCommittedOffsets(ctx, topic)
	if err != nil {
		return err
	}
	if err := stables.Error(); err != nil {
		return err
	}
	starts, err := adm.ListOffsetsAfterMilli(ctx, startMilli, topic)
	if err != nil {
		return err
	}
	if err := starts.Error(); err != nil {
		return err
	}
	for partition, stable := range stables[topic] {
		next := stable.Offset
		if start, ok := starts.Lookup(topic, partition); ok && start.Offset >= 0 {
			next = start.Offset
		}
		s.tracker.Observe(partition, next, stable.Offset)
	}
	return nil
}

// handlePartition applies records of a fetched partition and reports its progress to the tracker.
func (s *KafkaSource) handlePartition(partition kgo.FetchTopicPartition) {
	if partition.Err != nil {
		return
	}
	next := int64(-1)
	for _, record := range partition.Records {
		s.handleRecord(record)
		next = record.Offset + 1
	}
	s.tracker.Observe(partition.Partition, next, partition.LastStableOffset)
}

// handleRecord decodes a record and applies the access revocation event it carries. Records which
// can't be applied are skipped with a metric and a log line.
func (s *KafkaSource) handleRecord(record *kgo.Record) {
	if record.Attrs.IsControl() {
		return
	}
	method := recordHeader(record, s.source.Kafka.MethodHeader)
	operation, err := decodeOperation(method, record.Value)
	if err != nil {
		if errors.Is(err, ErrUnknownMethod) {
			s.skip(skipUnknownMethod, err, record, method)
			return
		}
		s.skip(skipDecodeError, err, record, method)
		return
	}
	s.applyOperation(operation, record.Timestamp, record, method)
}

// applyOperation resolves the operation with the record time and applies it locally.
func (s *KafkaSource) applyOperation(operation Operation, recordTime time.Time, record *kgo.Record, method string) {
	cfg := s.cfgContainer.Config()
	if !kindEnabled(cfg, operation.Kind) {
		s.skip(skipDisabled, errors.New("feature is disabled in config"), record, method)
		return
	}
	notification, err := Resolve(operation, recordTime, cfg.AccessRevoke.DefaultTTL.ToDuration())
	if err != nil {
		s.skip(skipValidation, err, record, method)
		return
	}
	// The clamp counts from the processing time, as the max TTL does. A record time would put a replayed record
	// behind the clock, and the stale check below would then drop it.
	now := s.clock()
	notification = ClampExpireAt(notification, now, cfg.AccessRevoke.MaxTTL.ToDuration(), ClampSourceKafka)
	// Staleness is checked against the processing time: an event replayed after its expiration must not
	// disconnect users, even though its record time is recent.
	if notification.Kind != KindUnblockUser && notification.ExpireAt <= now.Unix() {
		s.skip(skipStale, errors.New("expire_at is in the past"), record, method)
		return
	}
	if notification.Kind != KindUnblockUser && notification.ExpireAt-recordTime.Unix() > int64(s.source.Window.ToDuration()/time.Second) {
		metrics.AccessRevokeSourceEventsBeyondWindow.Inc()
		if s.beyondWindowLog.Allow() {
			log.Warn().
				Str("kind", notification.Kind).
				Int64("expire_at", notification.ExpireAt).
				Time("record_time", recordTime).
				Msg("access revocation event expires beyond source window, long bans must be re-emitted")
		}
	}
	err = s.revoker.Apply(notification)
	if err != nil {
		s.skip(skipApplyError, err, record, method)
		return
	}
	metrics.AccessRevokeSourceEventsAppliedTotal.WithLabelValues(notification.Kind).Inc()
}

// skip counts and logs a record which is not applied.
func (s *KafkaSource) skip(reason string, err error, record *kgo.Record, method string) {
	metrics.AccessRevokeSourceEventsSkippedTotal.WithLabelValues(reason).Inc()
	limiter, ok := s.skipLogs[reason]
	if ok && !limiter.Allow() {
		return
	}
	log.Warn().
		Err(err).
		Str("reason", reason).
		Str("method", method).
		Str("topic", record.Topic).
		Int32("partition", record.Partition).
		Int64("offset", record.Offset).
		Msg("skip access revocation event")
}

// sourceState returns the readiness of the source which the gauges read at scrape.
func (s *KafkaSource) sourceState() metrics.AccessRevokeSourceState {
	return metrics.AccessRevokeSourceState{
		Ready:      s.tracker.Ready(),
		Lagging:    s.tracker.Lagging(),
		LagRecords: s.tracker.LagRecords(),
	}
}

// reportTransitions logs and counts the fail-open and catch-up transitions of the snapshot, each once.
func (s *KafkaSource) reportTransitions(snapshot ReadinessSnapshot) {
	if snapshot.FailedOpen && s.failOpenReported.CompareAndSwap(false, true) {
		metrics.AccessRevokeSourceFailOpenTotal.Inc()
		log.Error().Msg("access revocation source startup timeout reached before catch up, node is ready without full state")
	}
	if snapshot.CaughtUp && s.caughtUpReported.CompareAndSwap(false, true) {
		metrics.AccessRevokeSourceCatchUpDuration.Set(snapshot.CatchUpDuration.Seconds())
		log.Info().Dur("duration", snapshot.CatchUpDuration).Msg("access revocation source caught up")
	}
}

// onReadinessTimer publishes the time based readiness transitions, reports them once, and arms the readiness timer for
// the next one. Run calls it once before seeding; afterwards only the timer calls it.
func (s *KafkaSource) onReadinessTimer() {
	snapshot, next := s.tracker.snapshotWithNextCheck()
	s.reportTransitions(snapshot)
	s.readinessMu.Lock()
	defer s.readinessMu.Unlock()
	if s.readinessStopped {
		return
	}
	if s.readinessTimer == nil {
		s.readinessTimer = time.AfterFunc(next, s.onReadinessTimer)
		return
	}
	s.readinessTimer.Reset(next)
}

// stopReadiness stops the readiness timer for good; a run which has started completes and arms nothing.
func (s *KafkaSource) stopReadiness() {
	s.readinessMu.Lock()
	defer s.readinessMu.Unlock()
	s.readinessStopped = true
	if s.readinessTimer != nil {
		s.readinessTimer.Stop()
	}
}

// fetchHook reports successful fetch responses of brokers to the tracker.
type fetchHook struct {
	tracker *ReadinessTracker
}

// fetchAPIKey is the Kafka API key of fetch requests.
var fetchAPIKey = kmsg.NewPtrFetchRequest().Key()

// OnBrokerRead implements kgo.HookBrokerRead.
func (h *fetchHook) OnBrokerRead(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, err error) {
	if key == fetchAPIKey && err == nil {
		h.tracker.FetchSucceeded()
	}
}

// kindEnabled reports whether the feature of the access revocation kind is enabled in config.
func kindEnabled(cfg config.Config, kind string) bool {
	switch kind {
	case KindBlockUser, KindUnblockUser:
		return cfg.UserBlock.Enabled
	case KindRevokeToken:
		return cfg.TokenRevoke.Enabled
	case KindInvalidateUserTokens:
		return cfg.UserTokensInvalidate.Enabled
	default:
		return false
	}
}

// recordHeader returns the value of the header with the given key, or empty string.
func recordHeader(record *kgo.Record, key string) string {
	for _, header := range record.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
