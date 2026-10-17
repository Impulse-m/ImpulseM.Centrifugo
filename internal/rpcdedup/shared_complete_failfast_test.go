package rpcdedup

import (
	"errors"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/redisshard"

	"github.com/stretchr/testify/require"
)

// A write which finds no shard of the shared tier connected is made once: a retry would not reach a shard, so the
// reply does not wait for the retry backoff. The record is still kept locally, as when the writes fail.
func TestSharedCompleteNotConnectedIsNotRetried(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	fx.backend.setFail(redisshard.ErrNotConnected)
	fx.backend.resetCalls()

	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))
	require.Equal(t, 1, countCalls(fx.backend.callLog(), "complete"), "a write with no connected shard is not retried")

	lookup := testBegin(t, fx.local, sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("ok"), lookup.Record.Reply)
}

// A failed write which is not ErrNotConnected keeps its retry, as before.
func TestSharedCompleteOtherFailureStillRetried(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	fx.backend.setFail(errors.New("redis is down"))
	fx.backend.resetCalls()

	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))
	require.Equal(t, sharedCompleteAttempts, countCalls(fx.backend.callLog(), "complete"))
}
