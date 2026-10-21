package accessrevoke

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

var allAccessChecks = CheckOptions{Block: true, Session: true, UserTokens: true}

func userVerdict(store *Store, userID string) Verdict {
	return store.Check(userID, centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict
}

func sessionVerdict(store *Store, sessionID string, issuedAt int64) Verdict {
	return store.Check("", Identity(sessionID, 0, issuedAt), allAccessChecks).Verdict
}

// sourceTestClock is a fake clock for KafkaSource tests.
type sourceTestClock struct {
	now time.Time
}

func (c *sourceTestClock) Now() time.Time {
	return c.now
}

// newSourceTest creates a KafkaSource over a real Revoker with a fake clock for the store and the source.
func newSourceTest(t *testing.T, cfg config.Config, clock *sourceTestClock) (*KafkaSource, *Store) {
	t.Helper()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)
	store.nowUnix = func() int64 {
		return clock.Now().Unix()
	}
	source := cfg.AccessRevoke.Source
	return newKafkaSource(source, cfgContainer, revoker, clock.Now), store
}

func sourceTestConfig() config.Config {
	cfg := config.DefaultConfig()
	cfg.AccessRevoke.Source = configtypes.AccessRevokeSource{
		Type:           "kafka",
		Window:         configtypes.Duration(30 * time.Minute),
		StartupTimeout: configtypes.Duration(time.Minute),
		LagThreshold:   configtypes.Duration(30 * time.Second),
		Kafka: configtypes.AccessRevokeKafkaSource{
			Brokers:      []string{"localhost:9092"},
			Topic:        "access_revoke",
			MethodHeader: "centrifugo-method",
			FetchMaxWait: configtypes.Duration(2 * time.Second),
		},
	}
	return cfg
}

func testRecord(method string, body string, timestamp time.Time) *kgo.Record {
	return &kgo.Record{
		Topic:     "access_revoke",
		Partition: 0,
		Offset:    1,
		Timestamp: timestamp,
		Value:     []byte(body),
		Headers: []kgo.RecordHeader{
			{Key: "centrifugo-method", Value: []byte(method)},
		},
	}
}

func TestKafkaSourceAppliesBlockUserRecord(t *testing.T) {
	clock := &sourceTestClock{now: time.Now()}
	source, store := newSourceTest(t, sourceTestConfig(), clock)
	before := appliedCount(t, KindBlockUser)

	// Record time is 10 minutes ago, so the default TTL of 20 minutes gives an expiration in the future.
	recordTime := clock.now.Add(-10 * time.Minute)
	source.handleRecord(testRecord("block_user", `{"user":"user-1"}`, recordTime))

	require.Equal(t, Blocked, userVerdict(store, "user-1"))
	require.Equal(t, before+1, appliedCount(t, KindBlockUser))
}

func TestKafkaSourceUsesRecordTimeForDefaults(t *testing.T) {
	clock := &sourceTestClock{now: time.Now()}
	source, store := newSourceTest(t, sourceTestConfig(), clock)

	recordTime := clock.now.Add(-10 * time.Minute)
	source.handleRecord(testRecord("block_user", `{"user":"user-1"}`, recordTime))

	// The block expires at record time plus default TTL, so it is still active at now and not after.
	clock.now = recordTime.Add(21 * time.Minute)
	store.nowUnix = func() int64 {
		return clock.now.Unix()
	}
	require.Equal(t, Allowed, userVerdict(store, "user-1"))
}

func TestKafkaSourceRevokeTokenRecord(t *testing.T) {
	clock := &sourceTestClock{now: time.Now()}
	source, store := newSourceTest(t, sourceTestConfig(), clock)

	recordTime := clock.now.Add(-time.Minute)
	source.handleRecord(testRecord("revoke_token", `{"uid":"session-1","revoked_at":`+strconv.FormatInt(recordTime.Unix(), 10)+`}`, recordTime))

	require.Equal(t, Revoked, sessionVerdict(store, "session-1", recordTime.Unix()-10))
}

func TestKafkaSourceInvalidateUserTokensRecord(t *testing.T) {
	clock := &sourceTestClock{now: time.Now()}
	source, store := newSourceTest(t, sourceTestConfig(), clock)

	recordTime := clock.now.Add(-time.Minute)
	source.handleRecord(testRecord("invalidate_user_tokens", `{"user":"user-1","generation":5}`, recordTime))

	result := store.Check("user-1", Identity("session-1", 4, recordTime.Unix()-10), allAccessChecks)
	require.Equal(t, ReasonGeneration, result.Reason)
}

func TestKafkaSourceUnblockUserRecord(t *testing.T) {
	clock := &sourceTestClock{now: time.Now()}
	source, store := newSourceTest(t, sourceTestConfig(), clock)

	source.handleRecord(testRecord("block_user", `{"user":"user-1"}`, clock.now))
	require.Equal(t, Blocked, userVerdict(store, "user-1"))

	source.handleRecord(testRecord("unblock_user", `{"user":"user-1"}`, clock.now))
	require.Equal(t, Allowed, userVerdict(store, "user-1"))
}

func TestKafkaSourceSkipRules(t *testing.T) {
	clock := &sourceTestClock{now: time.Now()}
	recordTime := clock.now.Add(-time.Minute)

	tests := []struct {
		name   string
		cfg    func(*config.Config)
		record *kgo.Record
		reason string
	}{
		{
			name:   "unknown method",
			record: testRecord("publish", `{"user":"user-1"}`, recordTime),
			reason: skipUnknownMethod,
		},
		{
			name: "missing method header",
			record: &kgo.Record{
				Timestamp: recordTime,
				Value:     []byte(`{"user":"user-1"}`),
			},
			reason: skipUnknownMethod,
		},
		{
			name:   "undecodable body",
			record: testRecord("block_user", `{"user":`, recordTime),
			reason: skipDecodeError,
		},
		{
			name:   "validation error: user required",
			record: testRecord("block_user", `{}`, recordTime),
			reason: skipValidation,
		},
		{
			name:   "validation error: channel not supported",
			record: testRecord("invalidate_user_tokens", `{"user":"user-1","channel":"chan"}`, recordTime),
			reason: skipValidation,
		},
		{
			name: "feature disabled",
			cfg: func(c *config.Config) {
				c.UserBlock.Enabled = false
			},
			record: testRecord("block_user", `{"user":"user-1"}`, recordTime),
			reason: skipDisabled,
		},
		{
			name:   "stale on replay",
			record: testRecord("block_user", `{"user":"user-1","expire_at":`+strconv.FormatInt(clock.now.Add(-time.Minute).Unix(), 10)+`}`, recordTime),
			reason: skipStale,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := sourceTestConfig()
			if tt.cfg != nil {
				tt.cfg(&cfg)
			}
			source, store := newSourceTest(t, cfg, clock)
			before := skippedCount(t, tt.reason)

			source.handleRecord(tt.record)

			require.Equal(t, before+1, skippedCount(t, tt.reason))
			require.Equal(t, Allowed, userVerdict(store, "user-1"))
		})
	}
}

func TestKafkaSourceTargetIsLastStableOffset(t *testing.T) {
	clock := &sourceTestClock{now: time.Now()}
	source, _ := newSourceTest(t, sourceTestConfig(), clock)

	// High watermark is ahead of the last stable offset: an open transaction at offset 2 must not
	// keep the source lagging. Records from offset 2 on are not delivered until the transaction ends.
	recordTime := clock.now.Add(-time.Minute)
	source.handlePartition(kgo.FetchTopicPartition{
		Topic: "access_revoke",
		FetchPartition: kgo.FetchPartition{
			Partition:        0,
			HighWatermark:    5,
			LastStableOffset: 2,
			Records: []*kgo.Record{
				testRecord("block_user", `{"user":"user-1"}`, recordTime),
			},
		},
	})
	snapshot := source.tracker.Snapshot()
	require.True(t, snapshot.CaughtUp)
	require.Equal(t, int64(0), snapshot.LagRecords)
}

func TestKafkaSourceBeyondWindowIsCountedAndApplied(t *testing.T) {
	clock := &sourceTestClock{now: time.Now()}
	source, store := newSourceTest(t, sourceTestConfig(), clock)
	before := beyondWindowCount(t)

	// Window is 30 minutes, expiration is 2 hours after the record time.
	expireAt := clock.now.Add(2 * time.Hour).Unix()
	source.handleRecord(testRecord("block_user", `{"user":"user-1","expire_at":`+strconv.FormatInt(expireAt, 10)+`}`, clock.now))

	require.Equal(t, before+1, beyondWindowCount(t))
	require.Equal(t, Blocked, userVerdict(store, "user-1"))
}

func TestKafkaSourceDecodeOperation(t *testing.T) {
	tests := []struct {
		method string
		body   string
		want   Operation
	}{
		{
			method: "block_user",
			body:   `{"user":"u","expire_at":100}`,
			want:   Operation{Kind: KindBlockUser, User: "u", ExpireAt: 100},
		},
		{
			method: "unblock_user",
			body:   `{"user":"u"}`,
			want:   Operation{Kind: KindUnblockUser, User: "u"},
		},
		{
			method: "revoke_token",
			body:   `{"uid":"s","user":"u","revoked_at":10,"expire_at":20}`,
			want:   Operation{Kind: KindRevokeToken, User: "u", Session: "s", RevokedAt: 10, ExpireAt: 20},
		},
		{
			method: "invalidate_user_tokens",
			body:   `{"user":"u","generation":3,"issued_before":7,"revoked_at":8,"expire_at":9}`,
			want:   Operation{Kind: KindInvalidateUserTokens, User: "u", Generation: 3, IssuedBefore: 7, RevokedAt: 8, ExpireAt: 9},
		},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			operation, err := decodeOperation(tt.method, []byte(tt.body))
			require.NoError(t, err)
			require.Equal(t, tt.want, operation)
		})
	}
}

func TestKafkaSourceDecodeOperationErrors(t *testing.T) {
	_, err := decodeOperation("publish", []byte(`{}`))
	require.ErrorIs(t, err, ErrUnknownMethod)

	_, err = decodeOperation("block_user", []byte(`not json`))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrUnknownMethod)
}

// failOpenCount returns the number of fail-open transitions of the sources of the test registry.
func failOpenCount(t *testing.T) float64 {
	t.Helper()
	return counterValue(t, "centrifugo_access_revoke_source_fail_open_total")
}

func TestKafkaSourceReadinessTimerFollowsTransitions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newKafkaSource(sourceTestConfig().AccessRevoke.Source, nil, nil, time.Now)
		before := failOpenCount(t)
		s.onReadinessTimer()

		time.Sleep(30 * time.Second)
		synctest.Wait()
		require.False(t, s.tracker.Lagging())

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.True(t, s.tracker.Lagging())
		require.False(t, s.tracker.Ready())

		time.Sleep(30*time.Second - 2*time.Nanosecond)
		synctest.Wait()
		require.False(t, s.tracker.Ready())

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.True(t, s.tracker.Ready())
		require.Equal(t, before+1, failOpenCount(t))

		time.Sleep(time.Hour)
		synctest.Wait()
		require.Equal(t, before+1, failOpenCount(t))
		s.stopReadiness()
	})
}

func TestKafkaSourceDetectsSilenceOnTimeWithLazyRearm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newKafkaSource(sourceTestConfig().AccessRevoke.Source, nil, nil, time.Now)
		s.onReadinessTimer()
		for range 30 {
			time.Sleep(10 * time.Second)
			synctest.Wait()
			s.tracker.FetchSucceeded()
		}
		synctest.Wait()
		require.False(t, s.tracker.Lagging())

		time.Sleep(30 * time.Second)
		synctest.Wait()
		require.False(t, s.tracker.Lagging())

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.True(t, s.tracker.Lagging())
		s.stopReadiness()
	})
}

// TestKafkaSourceDetectsSilenceAfterRecoveryBeforeStartupTimeout runs a non-strict source with Kafka unreachable: it
// lags by silence long before its startup timeout, one fetch ends the lag, and fetches stop again. The timer must see
// the new silence at its deadline, not at the startup timeout.
func TestKafkaSourceDetectsSilenceAfterRecoveryBeforeStartupTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := sourceTestConfig().AccessRevoke.Source
		source.StartupTimeout = configtypes.Duration(90 * time.Second)
		source.LagThreshold = configtypes.Duration(10 * time.Second)
		s := newKafkaSource(source, nil, nil, time.Now)
		s.onReadinessTimer()

		time.Sleep(10*time.Second + time.Nanosecond)
		synctest.Wait()
		require.True(t, s.tracker.Lagging())
		require.False(t, s.tracker.Ready())

		time.Sleep(time.Second - time.Nanosecond)
		synctest.Wait()
		s.tracker.FetchSucceeded()
		require.False(t, s.tracker.Lagging())

		time.Sleep(10 * time.Second)
		synctest.Wait()
		require.False(t, s.tracker.Lagging())

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.True(t, s.tracker.Lagging())
		require.False(t, s.tracker.Ready())
		s.stopReadiness()
	})
}

func TestKafkaSourceReportsCaughtUpOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newKafkaSource(sourceTestConfig().AccessRevoke.Source, nil, nil, time.Now)
		s.tracker.Observe(0, 0, 10)
		time.Sleep(12 * time.Second)
		synctest.Wait()
		s.tracker.Observe(0, 10, 10)

		s.reportTransitions(s.tracker.Snapshot())
		s.reportTransitions(s.tracker.Snapshot())
		require.Equal(t, 12.0, counterValue(t, "centrifugo_access_revoke_source_catch_up_duration_seconds"))
		require.True(t, s.caughtUpReported.Load())
	})
}

func TestKafkaSourceGaugesReadAtScrape(t *testing.T) {
	s := newKafkaSource(sourceTestConfig().AccessRevoke.Source, nil, nil, time.Now)
	metrics.BindAccessRevokeSource(s.sourceState)
	defer metrics.BindAccessRevokeSource(nil)

	s.tracker.Observe(0, 2, 5)
	require.Equal(t, 3.0, counterValue(t, "centrifugo_access_revoke_source_lag_records"))
	require.Equal(t, 0.0, counterValue(t, "centrifugo_access_revoke_source_ready"))
	require.Equal(t, 0.0, counterValue(t, "centrifugo_access_revoke_source_lagging"))

	s.tracker.Observe(0, 5, 5)
	require.Equal(t, 1.0, counterValue(t, "centrifugo_access_revoke_source_ready"))

	metrics.BindAccessRevokeSource(nil)
	require.Equal(t, 0.0, counterValue(t, "centrifugo_access_revoke_source_lag_records"))
	require.Equal(t, 0.0, counterValue(t, "centrifugo_access_revoke_source_ready"))
	require.Equal(t, 0.0, counterValue(t, "centrifugo_access_revoke_source_lagging"))
}

func TestKafkaSourceSeedRetriesWithBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newKafkaSource(sourceTestConfig().AccessRevoke.Source, nil, nil, time.Now)
		calls := 0
		try := func(ctx context.Context) error {
			calls++
			if calls <= 3 {
				return errors.New("listing failed")
			}
			return nil
		}
		start := time.Now()
		require.NoError(t, s.seedWith(context.Background(), try))
		require.Equal(t, 4, calls)
		// Three delays: 0.5 to 1 s, 1 to 2 s and 2 to 4 s.
		elapsed := time.Since(start)
		require.GreaterOrEqual(t, elapsed, 3500*time.Millisecond)
		require.LessOrEqual(t, elapsed, 7*time.Second+3*time.Nanosecond)
	})
}

func TestKafkaSourceSeedStopsOnContextDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newKafkaSource(sourceTestConfig().AccessRevoke.Source, nil, nil, time.Now)
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		try := func(ctx context.Context) error {
			calls++
			cancel()
			return errors.New("listing failed")
		}
		start := time.Now()
		require.ErrorIs(t, s.seedWith(ctx, try), context.Canceled)
		require.Equal(t, 1, calls)
		require.Zero(t, time.Since(start))
	})
}

func TestSeedBackOffStaysWithinHalfAndWholeNominal(t *testing.T) {
	nominal := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for range 200 {
		schedule := seedBackOff()
		schedule.Reset()
		for _, n := range nominal {
			delay := schedule.NextBackOff()
			require.GreaterOrEqual(t, delay, n/2)
			require.LessOrEqual(t, delay, n+time.Nanosecond)
		}
	}

	exact := seedBackOff()
	exact.RandomizationFactor = 0
	exact.Reset()
	for _, want := range []time.Duration{750 * time.Millisecond, 1500 * time.Millisecond, 3 * time.Second, 6 * time.Second, 7500 * time.Millisecond, 7500 * time.Millisecond} {
		require.Equal(t, want, exact.NextBackOff())
	}
}

// appliedCount returns the number of applied events of the kind.
func appliedCount(t *testing.T, kind string) float64 {
	t.Helper()
	return metricWithLabel(t, "source_events_applied_total", "kind", kind)
}

// skippedCount returns the number of skipped events with the reason.
func skippedCount(t *testing.T, reason string) float64 {
	t.Helper()
	return metricWithLabel(t, "source_events_skipped_total", "reason", reason)
}

func beyondWindowCount(t *testing.T) float64 {
	t.Helper()
	return counterValue(t, "centrifugo_access_revoke_source_events_beyond_window_total")
}

// metricWithLabel returns the value of the metric with the given name suffix and label value.
func metricWithLabel(t *testing.T, nameSuffix string, labelName string, labelValue string) float64 {
	t.Helper()
	families, err := testRegistry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if !strings.HasSuffix(family.GetName(), nameSuffix) {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == labelName && label.GetValue() == labelValue {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
