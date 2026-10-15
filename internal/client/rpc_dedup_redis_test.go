package client

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

	"github.com/stretchr/testify/require"
)

// TestSharedReplyLimitMatchesMemoryBoundary checks the reply limit of the shared tier against the node-local store: a
// reply is kept by one exactly when it is kept by the other, at the boundary and one byte over it.
func TestSharedReplyLimitMatchesMemoryBoundary(t *testing.T) {
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
			limit := sharedReplyLimit(tc.dedup)
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
