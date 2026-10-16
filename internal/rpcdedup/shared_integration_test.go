//go:build integration

package rpcdedup

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/redisshard"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// integrationNode is a node of the shared tier in the integration tests: its own node-local store and connector,
// over the same Redis as the other nodes.
type integrationNode struct {
	shared    *Shared
	local     *Memory
	connector *redisshard.Connector
	notified  []notifyCall
	// doneNotified are the done notifications of the node, and observer counts its waits, when the node has the wakeup.
	doneNotified []notifyCall
	observer     *waitObserver
}

// newIntegrationNode returns a node with id nodeID, over the Redis of prefix.
func newIntegrationNode(t *testing.T, prefix string, nodeID string) *integrationNode {
	t.Helper()
	local, _ := newTestMemory(1024)
	connector := newIntegrationConnector(t, prefix)
	node := &integrationNode{local: local, connector: connector}
	node.shared = NewShared(SharedOptions{
		Local:         local,
		Backend:       NewRedis(prefix, connector, 0, nil),
		NodeID:        nodeID,
		TTL:           time.Minute,
		MaxReplyBytes: 1024,
		Notifier: func(owner string, key Key) {
			node.notified = append(node.notified, notifyCall{nodeID: owner, key: key})
		},
		PollInitial:   time.Millisecond,
		PollMax:       5 * time.Millisecond,
		AckFlushEvery: 5 * time.Millisecond,
	})
	return node
}

// deleteRedisKey deletes key of the Redis of connector, as an expiry does.
func deleteRedisKey(connector *redisshard.Connector, key string) {
	integrationDo(connector, func(c rueidis.Client) rueidis.Completed {
		return c.B().Del().Key(key).Build()
	})
}

func TestSharedTwoNodesRemoteJoinAndDoneHit(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationNode(t, prefix, "node-a")
	nodeB := newIntegrationNode(t, prefix, "node-b")
	key := Key{Scope: testScope("u1", "a"), MsgID: 1}
	fingerprint := Fingerprint("users.get", []byte("x"))

	owner := NewCallFlight(fingerprint, time.Second)
	lookup, err := nodeA.shared.Begin(key, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeMiss, lookup.Outcome)

	duplicate := NewCallFlight(fingerprint, time.Second)
	lookup, err = nodeB.shared.Begin(key, duplicate)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	waiting := lookup.Flight

	require.NoError(t, nodeA.shared.Complete(key, owner, Record{Reply: []byte("answer")}))
	waitDone(t, waiting)
	require.Equal(t, []byte("answer"), waiting.Record().Reply)

	// The record is kept on node-b now: its next duplicate is a local hit.
	lookup, err = nodeB.shared.Begin(key, NewFlightWithFingerprint(fingerprint))
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("answer"), lookup.Record.Reply)
}

func TestSharedTwoNodesErrorIsTheSame(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationNode(t, prefix, "node-a")
	nodeB := newIntegrationNode(t, prefix, "node-b")
	key := Key{Scope: testScope("u1", "a"), MsgID: 2}
	fingerprint := Fingerprint("users.get", []byte("x"))

	owner := NewCallFlight(fingerprint, time.Second)
	_, err := nodeA.shared.Begin(key, owner)
	require.NoError(t, err)
	lookup, err := nodeB.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)

	require.NoError(t, nodeA.shared.Complete(key, owner, Record{Err: testCancelled}))
	waitDone(t, lookup.Flight)
	require.Equal(t, testCancelled, lookup.Flight.Record().Err)
}

func TestSharedTwoNodesCancelNotifiesOwner(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationNode(t, prefix, "node-a")
	nodeB := newIntegrationNode(t, prefix, "node-b")
	key := Key{Scope: testScope("u1", "a"), MsgID: 3}
	fingerprint := Fingerprint("users.get", []byte("x"))

	owner := NewCallFlight(fingerprint, time.Second)
	_, err := nodeA.shared.Begin(key, owner)
	require.NoError(t, err)

	// node-b has no flight of the call: the cancel reaches node-a by notification, and node-b records nothing.
	require.NoError(t, nodeB.shared.Cancel(key, testCancelled))
	require.Equal(t, []notifyCall{{nodeID: "node-a", key: key}}, nodeB.notified)
	require.True(t, testIsMiss(t, nodeB.local, key))

	// The notification is handled by node-a, which cancels its call.
	require.True(t, nodeA.shared.CancelFlight(key))
	require.False(t, nodeB.shared.CancelFlight(key))
}

func TestSharedTwoNodesOwnerCrashAbandonsWaiters(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationNode(t, prefix, "node-a")
	nodeB := newIntegrationNode(t, prefix, "node-b")
	key := Key{Scope: testScope("u1", "a"), MsgID: 4}
	fingerprint := Fingerprint("users.get", []byte("x"))

	_, err := nodeA.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	lookup, err := nodeB.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	waiting := lookup.Flight

	// node-a died: its in-flight record expires, and node-b's waiter is abandoned.
	deleteRedisKey(nodeA.connector, RedisKey(prefix, key))
	waitDone(t, waiting)
	require.True(t, waiting.Record().Abandoned)

	// The next call of node-b runs.
	lookup, err = nodeB.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	require.Equal(t, OutcomeMiss, lookup.Outcome)
}

func TestSharedTwoNodesStaleCancelAfterAckAnswersTheSameOnEveryNode(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationNode(t, prefix, "node-a")
	nodeB := newIntegrationNode(t, prefix, "node-b")
	key := Key{Scope: testScope("u1", "a"), MsgID: 12}
	fingerprint := Fingerprint("users.get", []byte("x"))

	owner := NewCallFlight(fingerprint, time.Second)
	_, err := nodeA.shared.Begin(key, owner)
	require.NoError(t, err)
	require.NoError(t, nodeA.shared.Complete(key, owner, Record{Reply: []byte("answer")}))

	// The client acknowledges the reply to node-b, so Redis forgets the record. Node-a still caches it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- nodeB.shared.Run(ctx) }()
	require.NoError(t, nodeB.shared.Ack(key.Scope, []uint64{key.MsgID}))
	redisKey := RedisKey(prefix, key)
	require.Eventually(t, func() bool {
		return !integrationExists(t, nodeA.connector, redisKey)
	}, 5*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	// A stale cancel reaches node-a. The shared tier holds nothing, so it leaves a tombstone, and node-a answers it.
	require.NoError(t, nodeA.shared.Cancel(key, testCancelled))
	lookup, err := nodeA.shared.Begin(key, NewFlightWithFingerprint(fingerprint))
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, testCancelled.Code, lookup.Record.Err.Code)

	// Node-b never cached the reply: it reads the same tombstone from Redis.
	lookup, err = nodeB.shared.Begin(key, NewFlightWithFingerprint(fingerprint))
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, testCancelled.Code, lookup.Record.Err.Code)
	require.Equal(t, testCancelled.Message, lookup.Record.Err.Message)
}

func TestSharedTwoNodesAckRemovesRedisRecordAndKeepsLocalCache(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationNode(t, prefix, "node-a")
	nodeB := newIntegrationNode(t, prefix, "node-b")
	key := Key{Scope: testScope("u1", "a"), MsgID: 5}
	fingerprint := Fingerprint("users.get", []byte("x"))

	owner := NewCallFlight(fingerprint, time.Second)
	_, err := nodeA.shared.Begin(key, owner)
	require.NoError(t, err)
	require.NoError(t, nodeA.shared.Complete(key, owner, Record{Reply: []byte("answer")}))

	// The client acknowledges the reply to node-b, which flushes its acks to Redis.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- nodeB.shared.Run(ctx) }()
	require.NoError(t, nodeB.shared.Ack(key.Scope, []uint64{key.MsgID}))
	redisKey := RedisKey(prefix, key)
	require.Eventually(t, func() bool {
		return !integrationExists(t, nodeA.connector, redisKey)
	}, 5*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	// Acks are per client: node-a keeps its local record, which still answers its duplicates.
	lookup, err := nodeA.shared.Begin(key, NewFlightWithFingerprint(fingerprint))
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("answer"), lookup.Record.Reply)
}
