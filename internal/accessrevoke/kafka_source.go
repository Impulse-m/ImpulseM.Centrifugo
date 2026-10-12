package accessrevoke

import (
	"context"
	"errors"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/kafkautil"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Skip reasons of source records, used as metric label values.
const (
	skipUnknownMethod = "unknown_method"
	skipDecodeError   = "decode_error"
	skipValidation    = "validation_error"
	skipDisabled      = "disabled"
	skipStale         = "stale"
	skipApplyError    = "apply_error"
)

var _ Source = (*KafkaSource)(nil)

// KafkaSource reads access revocation events from a Kafka topic and applies them with a Revoker.
// It reads all partitions directly, without consumer group and without commits. On every start it
// reads from now minus window, so events older than the window are not replayed.
type KafkaSource struct {
	source       configtypes.AccessRevokeSource
	cfgContainer *config.Container
	revoker      *Revoker
	tracker      *ReadinessTracker
	clock        func() time.Time
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
		clock: clock,
	}
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

	// Watch starts before seeding: when Kafka is unreachable seeding retries until the startup
	// timeout, and the fail-open transition must still be logged and exported.
	go s.watch(ctx)

	adm := kadm.NewClient(client)
	if err := s.seed(ctx, adm, startMilli); err != nil {
		return err
	}

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
// from these values. It retries until listing succeeds or ctx is done.
func (s *KafkaSource) seed(ctx context.Context, adm *kadm.Client, startMilli int64) error {
	for {
		err := s.trySeed(ctx, adm, startMilli)
		if err == nil {
			return nil
		}
		log.Warn().Err(err).Msg("error listing access revocation source offsets, retrying")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
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
	// Staleness is checked against the processing time: an event replayed after its expiration must not
	// disconnect users, even though its record time is recent.
	if notification.Kind != KindUnblockUser && notification.ExpireAt <= s.clock().Unix() {
		s.skip(skipStale, errors.New("expire_at is in the past"), record, method)
		return
	}
	if notification.Kind != KindUnblockUser && notification.ExpireAt-recordTime.Unix() > int64(s.source.Window.ToDuration()/time.Second) {
		metrics.AccessRevokeSourceEventsBeyondWindow.Inc()
		log.Warn().
			Str("kind", notification.Kind).
			Int64("expire_at", notification.ExpireAt).
			Time("record_time", recordTime).
			Msg("access revocation event expires beyond source window, long bans must be re-emitted")
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
	log.Warn().
		Err(err).
		Str("reason", reason).
		Str("method", method).
		Str("topic", record.Topic).
		Int32("partition", record.Partition).
		Int64("offset", record.Offset).
		Msg("skip access revocation event")
}

// watch keeps readiness metrics up to date and logs readiness transitions until ctx is done.
func (s *KafkaSource) watch(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var failOpenReported bool
	var caughtUpReported bool
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		snapshot := s.tracker.Snapshot()
		metrics.AccessRevokeSourceReady.Set(boolToFloat(snapshot.Ready))
		metrics.AccessRevokeSourceLagging.Set(boolToFloat(snapshot.Lagging))
		metrics.AccessRevokeSourceLagRecords.Set(float64(snapshot.LagRecords))
		if snapshot.FailedOpen && !failOpenReported {
			failOpenReported = true
			metrics.AccessRevokeSourceFailOpenTotal.Inc()
			log.Error().Msg("access revocation source startup timeout reached before catch up, node is ready without full state")
		}
		if snapshot.CaughtUp && !caughtUpReported {
			caughtUpReported = true
			metrics.AccessRevokeSourceCatchUpDuration.Set(snapshot.CatchUpDuration.Seconds())
			log.Info().Dur("duration", snapshot.CatchUpDuration).Msg("access revocation source caught up")
		}
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

func boolToFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
