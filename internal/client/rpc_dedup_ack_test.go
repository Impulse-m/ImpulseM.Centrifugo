package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// ackRecordingStore records the msg_ids which an ack passes to the store. It does not keep records.
type ackRecordingStore struct {
	rpcdedup.Store
	acked []uint64
}

// Ack records msgIDs.
func (s *ackRecordingStore) Ack(_ string, msgIDs []uint64) error {
	s.acked = append(s.acked, msgIDs...)
	return nil
}

// TestRPCAckDropsOutOfWindowAndCapsCount sends an ack frame with 2000 msg_ids out of the window, followed by 1500 in
// the window. The store gets only the first in-window msg_ids, and no more than the cap of one call.
func TestRPCAckDropsOutOfWindowAndCapsCount(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	recorder := &ackRecordingStore{Store: h.rpcDedup.store}
	h.rpcDedup.store = recorder
	client := newDedupClient("c1", "42", nil, testInstance)

	acks := make([]uint64, 0, 3500)
	for range 2000 {
		acks = append(acks, 1<<60)
	}
	inWindow := make([]uint64, 0, 1500)
	for seq := range uint64(1500) {
		inWindow = append(inWindow, dedupMsgID(seq+1))
	}
	acks = append(acks, inWindow...)

	// The reply does not matter here: the ack happens before the method lookup.
	_, _ = h.OnRPC(client, centrifuge.RPCEvent{
		Method: "system.config",
		Data:   []byte("ack"),
		Call:   centrifuge.RPCCallOptions{Ack: acks},
	}, nil)

	require.Len(t, recorder.acked, 1024)
	require.Equal(t, inWindow[:1024], recorder.acked)
}
