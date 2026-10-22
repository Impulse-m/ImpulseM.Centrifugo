package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

// TestRPCAckTakesFirstCapOverCap sends an ack frame with one msg_id more than the cap. The store gets the first cap msg_ids,
// the last one is dropped and counted, and the call is not refused.
func TestRPCAckTakesFirstCapOverCap(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	recorder := &ackRecordingStore{Store: h.rpcDedup.store}
	h.rpcDedup.store = recorder
	client := newDedupClient("c1", "42", nil, testInstance)

	acks := make([]uint64, 0, rpcMaxAcksPerCall+1)
	for seq := range uint64(rpcMaxAcksPerCall + 1) {
		acks = append(acks, dedupMsgID(seq+1))
	}
	droppedBefore := testutil.ToFloat64(metrics.RPCRegistryDedupAcksOverCapTotal)

	// The reply does not matter here: the call runs after the ack, and only a refusal is checked.
	_, err := h.OnRPC(client, centrifuge.RPCEvent{
		Method: "system.config",
		Data:   []byte("ack"),
		Call:   centrifuge.RPCCallOptions{Ack: acks},
	}, nil)

	require.NotErrorIs(t, err, centrifuge.ErrorBadRequest)
	require.Equal(t, acks[:rpcMaxAcksPerCall], recorder.acked)
	require.Equal(t, droppedBefore+1, testutil.ToFloat64(metrics.RPCRegistryDedupAcksOverCapTotal))
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

// noopAckStore acknowledges without keeping anything, so an allocation count of an ack is the ack's own.
type noopAckStore struct {
	rpcdedup.Store
}

// Ack does nothing.
func (s *noopAckStore) Ack(_ string, _ []uint64) error {
	return nil
}

// TestRPCAckDoesNotAllocate acks a frame of in-window msg_ids many times into one buffer. The ack filters the list in
// place, so a call with acks costs no allocation on the RPC hot path.
func TestRPCAckDoesNotAllocate(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	h.rpcDedup.store = &noopAckStore{Store: h.rpcDedup.store}
	track := &rpcTrack{scope: "scope-1"}

	source := make([]uint64, 0, 64)
	for seq := range uint64(64) {
		source = append(source, dedupMsgID(seq+1))
	}
	buffer := make([]uint64, len(source))

	allocs := testing.AllocsPerRun(100, func() {
		copy(buffer, source)
		h.rpcAck(track, buffer)
	})

	require.Zero(t, allocs)
}
