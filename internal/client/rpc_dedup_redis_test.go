package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

	"github.com/stretchr/testify/require"
)

// failingBeginBackend is a shared tier whose Begin waits until the test releases it, and then fails. The other calls
// fail at once, and are not expected while the test runs.
type failingBeginBackend struct {
	entered chan struct{}
	release chan struct{}
}

// newFailingBeginBackend returns a failingBeginBackend which has not been entered.
func newFailingBeginBackend() *failingBeginBackend {
	return &failingBeginBackend{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

// Begin signals that it was entered, waits for the release, and fails.
func (b *failingBeginBackend) Begin(_ context.Context, _ rpcdedup.Key, _ [32]byte, _ string, _ time.Duration) (rpcdedup.BeginResult, error) {
	b.entered <- struct{}{}
	<-b.release
	return rpcdedup.BeginResult{}, errors.New("shared tier down")
}

// Complete fails: a call which failed to begin keeps no record.
func (b *failingBeginBackend) Complete(_ context.Context, _ rpcdedup.Key, _ string, _ rpcdedup.Record, _ string, _ time.Duration) (bool, error) {
	return false, errors.New("shared tier down")
}

// Cancel fails.
func (b *failingBeginBackend) Cancel(_ context.Context, _ rpcdedup.Key, _ *centrifuge.Error, _ time.Duration) (rpcdedup.CancelResult, error) {
	return rpcdedup.CancelResult{}, errors.New("shared tier down")
}

// Poll fails.
func (b *failingBeginBackend) Poll(_ context.Context, _ rpcdedup.Key) (rpcdedup.PollResult, error) {
	return rpcdedup.PollResult{}, errors.New("shared tier down")
}

// Ack does nothing.
func (b *failingBeginBackend) Ack(_ context.Context, _ []rpcdedup.ScopeAcks) error {
	return nil
}

// Connected reports that the backend is reachable, so that the store tries it.
func (b *failingBeginBackend) Connected() bool {
	return true
}

// A duplicate which joins a call while the shared tier fails to begin it is answered with the later signal, as the
// owner is: the call never ran, so the client may send it again.
func TestRPCDedupSharedBeginFailureJoinerCarriesLaterSignal(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	begin := newFailingBeginBackend()
	h.rpcDedup.store = rpcdedup.NewShared(rpcdedup.SharedOptions{
		Local:         h.rpcDedup.memory,
		Backend:       begin,
		NodeID:        "node-a",
		TTL:           time.Minute,
		MaxReplyBytes: dedupRegistryConfig().Dedup.ReplyLimit(),
	})
	joined := testJoinSignal(h)
	msgID := dedupMsgID(41)

	owner := dedupCallAsync(h, newDedupClient("c1", "42", nil, testInstance), "users.put", msgID, "x")
	select {
	case <-begin.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the shared tier did not start the call")
	}
	duplicate := dedupCallAsync(h, newDedupClient("c2", "42", nil, testInstance), "users.put", msgID, "x")
	requireJoined(t, joined)
	close(begin.release)

	requireRetrySignal(t, waitDedupResult(t, owner).err, rpcRetryLater)
	requireRetrySignal(t, waitDedupResult(t, duplicate).err, rpcRetryLater)
	require.Empty(t, backend.Calls())
}

// TestReplyLimitMatchesMemoryBoundary checks the reply limit, which both tiers take from the configuration, against the
// node-local store: a reply is kept by it exactly when the limit holds, at the boundary and one byte over it.
func TestReplyLimitMatchesMemoryBoundary(t *testing.T) {
	tests := []struct {
		name  string
		dedup configtypes.RPCDedup
	}{
		{
			name:  "defaults",
			dedup: configtypes.RPCDedup{MaxReplySize: 2048, ScopeReserve: 4, ScopeReserveBytes: 8192},
		},
		{
			name:  "reply limit below the share",
			dedup: configtypes.RPCDedup{MaxReplySize: 1000, ScopeReserve: 4, ScopeReserveBytes: 8192},
		},
		{
			name:  "share below the reply limit",
			dedup: configtypes.RPCDedup{MaxReplySize: 2048, ScopeReserve: 4, ScopeReserveBytes: 4096},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limit := tc.dedup.ReplyLimit()
			for _, size := range []int{limit, limit + 1} {
				store := rpcdedup.NewMemory(rpcdedup.MemoryOptions{
					MaxEntries:         1024,
					MaxBytes:           1 << 28,
					MaxRecordsPerScope: 1024,
					ScopeReserve:       tc.dedup.ScopeReserve,
					ScopeReserveBytes:  int64(tc.dedup.ScopeReserveBytes),
					MaxScopesPerUser:   8,
					TTL:                time.Minute,
					MaxReplyBytes:      tc.dedup.ReplyLimit(),
					Shards:             1,
				})
				key := rpcdedup.Key{Scope: rpcdedup.ScopeOf("s", "u1", []byte("session")), MsgID: 1}
				flight := rpcdedup.NewFlight()
				lookup, err := store.Begin(key, flight)
				require.NoError(t, err)
				require.Equal(t, rpcdedup.OutcomeMiss, lookup.Outcome)
				require.NoError(t, store.Complete(key, flight, rpcdedup.Record{Reply: make([]byte, size)}))

				lookup, err = store.Begin(key, rpcdedup.NewFlight())
				require.NoError(t, err)
				require.Equal(t, rpcdedup.OutcomeDone, lookup.Outcome)
				require.Equal(t, size <= limit, !lookup.Record.Uncached, "reply of %d bytes, limit %d", size, limit)
			}
		})
	}
}
