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

// TestRPCAckRefusesFrameOverCap sends an ack frame with one msg_id more than the cap. The call is refused with a protocol
// error before anything is acknowledged, so the store gets no msg_id.
func TestRPCAckRefusesFrameOverCap(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	recorder := &ackRecordingStore{Store: h.rpcDedup.store}
	h.rpcDedup.store = recorder
	client := newDedupClient("c1", "42", nil, testInstance)

	acks := make([]uint64, 0, rpcMaxAcksPerCall+1)
	for seq := range uint64(rpcMaxAcksPerCall + 1) {
		acks = append(acks, dedupMsgID(seq+1))
	}

	_, err := h.OnRPC(client, centrifuge.RPCEvent{
		Method: "system.config",
		Data:   []byte("ack"),
		Call:   centrifuge.RPCCallOptions{Ack: acks},
	}, nil)

	require.ErrorIs(t, err, centrifuge.ErrorBadRequest)
	require.Empty(t, recorder.acked)
}

// TestRPCAckDropsOutOfWindowAtCap sends an ack frame at the cap: out-of-window msg_ids followed by in-window ones. The
// store gets the in-window msg_ids only.
func TestRPCAckDropsOutOfWindowAtCap(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	recorder := &ackRecordingStore{Store: h.rpcDedup.store}
	h.rpcDedup.store = recorder
	client := newDedupClient("c1", "42", nil, testInstance)

	const outOfWindow = rpcMaxAcksPerCall - 24
	acks := make([]uint64, 0, rpcMaxAcksPerCall)
	for range outOfWindow {
		acks = append(acks, 1<<60)
	}
	inWindow := make([]uint64, 0, 24)
	for seq := range uint64(24) {
		inWindow = append(inWindow, dedupMsgID(seq+1))
	}
	acks = append(acks, inWindow...)

	// The reply does not matter here: the ack happens before the method lookup.
	_, _ = h.OnRPC(client, centrifuge.RPCEvent{
		Method: "system.config",
		Data:   []byte("ack"),
		Call:   centrifuge.RPCCallOptions{Ack: acks},
	}, nil)

	require.Equal(t, inWindow, recorder.acked)
}
