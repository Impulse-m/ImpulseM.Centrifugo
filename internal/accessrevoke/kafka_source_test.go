package accessrevoke

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
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

	// Record time is 10 minutes ago, so the default TTL of 20 minutes gives an expiration in the future.
	recordTime := clock.now.Add(-10 * time.Minute)
	source.handleRecord(testRecord("block_user", `{"user":"user-1"}`, recordTime))

	require.Equal(t, Blocked, userVerdict(store, "user-1"))
	require.Equal(t, float64(1), appliedCount(t, KindBlockUser))
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
