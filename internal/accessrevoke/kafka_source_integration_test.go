//go:build integration

package accessrevoke

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const testKafkaBrokerAddr = "localhost:29092"

// integrationSourceTimeout bounds waits for readiness and applied events.
const integrationSourceTimeout = 30 * time.Second

// createIntegrationTopic creates a unique single partition topic for a test.
func createIntegrationTopic(t *testing.T) string {
	t.Helper()
	topic := "access_revoke_" + uuid.NewString()
	cl, err := kgo.NewClient(kgo.SeedBrokers(testKafkaBrokerAddr))
	require.NoError(t, err)
	defer cl.Close()

	adm := kadm.NewClient(cl)
	defer adm.Close()
	resp, err := adm.CreateTopics(context.Background(), 1, 1, nil, topic)
	require.NoError(t, err)
	require.NoError(t, resp[topic].Err)
	// A new topic may be unknown to the broker for a moment: wait until its partition has a leader,
	// otherwise the first produce can fail with UNKNOWN_TOPIC_OR_PARTITION.
	require.Eventually(t, func() bool {
		topics, err := adm.ListTopics(context.Background(), topic)
		if err != nil {
			return false
		}
		details, ok := topics[topic]
		if !ok || details.Err != nil || len(details.Partitions) == 0 {
			return false
		}
		for _, partition := range details.Partitions {
			if partition.Err != nil || partition.Leader < 0 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)
	return topic
}

// produceIntegrationRecords produces records with a sync producer.
func produceIntegrationRecords(t *testing.T, records ...*kgo.Record) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(testKafkaBrokerAddr))
	require.NoError(t, err)
	defer cl.Close()
	require.NoError(t, cl.ProduceSync(context.Background(), records...).FirstErr())
}

// revokeRecord returns a record of the access revocation topic with the method header.
func revokeRecord(topic string, method string, body string, timestamp time.Time) *kgo.Record {
	return &kgo.Record{
		Topic:     topic,
		Value:     []byte(body),
		Timestamp: timestamp,
		Headers: []kgo.RecordHeader{
			{Key: "centrifugo-method", Value: []byte(method)},
		},
	}
}

// startIntegrationSource runs a KafkaSource with real clock over a new store. It returns the source and
// its store. The source is stopped on test cleanup.
func startIntegrationSource(t *testing.T, topic string) (*KafkaSource, *Store) {
	t.Helper()
	cfg := sourceTestConfig()
	cfg.AccessRevoke.Source.Kafka.Brokers = []string{testKafkaBrokerAddr}
	cfg.AccessRevoke.Source.Kafka.Topic = topic
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)
	source := NewKafkaSource(cfg.AccessRevoke.Source, cfgContainer, revoker)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- source.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		require.ErrorIs(t, <-errCh, context.Canceled)
		// Applied events are counted in a process-wide metric. Reset it, so that the counts of these tests
		// do not leak into the unit tests which check the metric by absolute value.
		metrics.AccessRevokeSourceEventsAppliedTotal.Reset()
	})
	return source, store
}

// waitSourceReady waits until the source is ready and fails the test if it is not ready by fail-open.
func waitSourceReady(t *testing.T, source *KafkaSource) {
	t.Helper()
	require.Eventually(t, source.Ready, integrationSourceTimeout, 50*time.Millisecond)
	require.False(t, source.tracker.Snapshot().FailedOpen, "source became ready by startup timeout, not by catch up")
}

// waitVerdict waits until the user has the expected verdict in the store.
func waitVerdict(t *testing.T, store *Store, userID string, verdict Verdict) {
	t.Helper()
	require.Eventually(t, func() bool {
		return userVerdict(store, userID) == verdict
	}, integrationSourceTimeout, 20*time.Millisecond, fmt.Sprintf("user %s is not %v", userID, verdict))
}

func TestKafkaSourceIntegrationAppliesEventsProducedBeforeStart(t *testing.T) {
	topic := createIntegrationTopic(t)
	now := time.Now()
	produceIntegrationRecords(t,
		revokeRecord(topic, "block_user", `{"user":"user-1"}`, now.Add(-time.Minute)),
		revokeRecord(topic, "block_user", `{"user":"user-2"}`, now.Add(-time.Minute)),
		revokeRecord(topic, "unblock_user", `{"user":"user-2"}`, now.Add(-time.Second)),
	)

	source, store := startIntegrationSource(t, topic)
	waitSourceReady(t, source)

	require.Equal(t, Blocked, userVerdict(store, "user-1"))
	require.Equal(t, Allowed, userVerdict(store, "user-2"))
}

func TestKafkaSourceIntegrationRestartReplaysAndRebuildsStore(t *testing.T) {
	topic := createIntegrationTopic(t)
	now := time.Now()
	produceIntegrationRecords(t,
		revokeRecord(topic, "block_user", `{"user":"user-1"}`, now.Add(-time.Minute)),
	)

	source, store := startIntegrationSource(t, topic)
	waitSourceReady(t, source)
	require.Equal(t, Blocked, userVerdict(store, "user-1"))

	// A new source has an empty store: the events are read again from now minus window.
	restarted, restartedStore := startIntegrationSource(t, topic)
	waitSourceReady(t, restarted)
	require.Equal(t, Blocked, userVerdict(restartedStore, "user-1"))
}

func TestKafkaSourceIntegrationSkipsEventsOlderThanWindow(t *testing.T) {
	topic := createIntegrationTopic(t)
	now := time.Now()
	// The record is two hours old, window is 30 minutes. Expiration is in the future, so only the window
	// keeps the event from being applied.
	expireAt := strconv.FormatInt(now.Add(time.Hour).Unix(), 10)
	produceIntegrationRecords(t,
		revokeRecord(topic, "block_user", `{"user":"user-old","expire_at":`+expireAt+`}`, now.Add(-2*time.Hour)),
	)
	// The marker is produced after the old event, so the source starts after it.
	produceIntegrationRecords(t,
		revokeRecord(topic, "block_user", `{"user":"user-new"}`, now),
	)

	source, store := startIntegrationSource(t, topic)
	waitSourceReady(t, source)
	waitVerdict(t, store, "user-new", Blocked)
	require.Equal(t, Allowed, userVerdict(store, "user-old"))
}

func TestKafkaSourceIntegrationBadMessageIsSkipped(t *testing.T) {
	topic := createIntegrationTopic(t)
	now := time.Now()
	produceIntegrationRecords(t,
		revokeRecord(topic, "block_user", `{"user":`, now.Add(-time.Minute)),
		revokeRecord(topic, "publish", `{"user":"user-bad"}`, now.Add(-time.Minute)),
		revokeRecord(topic, "block_user", `{}`, now.Add(-time.Minute)),
		revokeRecord(topic, "block_user", `{"user":"user-ok"}`, now.Add(-time.Minute)),
	)

	source, store := startIntegrationSource(t, topic)
	waitSourceReady(t, source)

	require.Equal(t, Blocked, userVerdict(store, "user-ok"))
	require.Equal(t, Allowed, userVerdict(store, "user-bad"))
}

func TestKafkaSourceIntegrationAbortedTransactionIsNotApplied(t *testing.T) {
	topic := createIntegrationTopic(t)
	now := time.Now()

	txClient, err := kgo.NewClient(
		kgo.SeedBrokers(testKafkaBrokerAddr),
		kgo.TransactionalID("access-revoke-test-"+uuid.NewString()),
		kgo.DefaultProduceTopic(topic),
	)
	require.NoError(t, err)
	defer txClient.Close()

	ctx := context.Background()
	require.NoError(t, txClient.BeginTransaction())
	require.NoError(t, txClient.ProduceSync(ctx,
		revokeRecord(topic, "block_user", `{"user":"user-aborted"}`, now.Add(-time.Minute)),
	).FirstErr())
	require.NoError(t, txClient.EndTransaction(ctx, kgo.TryAbort))

	produceIntegrationRecords(t,
		revokeRecord(topic, "block_user", `{"user":"user-ok"}`, now.Add(-time.Minute)),
	)

	source, store := startIntegrationSource(t, topic)
	waitSourceReady(t, source)
	waitVerdict(t, store, "user-ok", Blocked)
	require.Equal(t, Allowed, userVerdict(store, "user-aborted"))
}

func TestKafkaSourceIntegrationOpenTransactionDoesNotBlockReadiness(t *testing.T) {
	topic := createIntegrationTopic(t)
	now := time.Now()

	txClient, err := kgo.NewClient(
		kgo.SeedBrokers(testKafkaBrokerAddr),
		kgo.TransactionalID("access-revoke-test-"+uuid.NewString()),
		kgo.DefaultProduceTopic(topic),
	)
	require.NoError(t, err)
	defer txClient.Close()

	ctx := context.Background()
	require.NoError(t, txClient.BeginTransaction())
	require.NoError(t, txClient.ProduceSync(ctx,
		revokeRecord(topic, "block_user", `{"user":"user-open"}`, now.Add(-time.Minute)),
	).FirstErr())
	// The transaction stays open: the high watermark is ahead of the last stable offset.

	source, store := startIntegrationSource(t, topic)
	// Readiness is reached at the last stable offset, so the open transaction does not block it.
	waitSourceReady(t, source)
	require.Equal(t, Allowed, userVerdict(store, "user-open"))

	require.NoError(t, txClient.EndTransaction(ctx, kgo.TryAbort))
}
