package rpcdedup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// An ack removes a done record, but not the tombstone of a cancelled call: a retransmit after the ack is answered as
// cancelled, and the call does not run again.
func TestSharedAckKeepsTombstone(t *testing.T) {
	fx := newSharedFixture(t, false)
	require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))

	require.NoError(t, fx.shared.Ack(sharedKey.Scope, []uint64{sharedKey.MsgID}))
	require.NoError(t, fx.backend.Ack(context.Background(), []ScopeAcks{{Scope: sharedKey.Scope, MsgIDs: []uint64{sharedKey.MsgID}}}))

	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found, "the shared tier keeps the tombstone")
	require.Equal(t, byte('c'), entry.state)

	lookup, err := fx.shared.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome, "the retransmit is answered, and does not run")
	require.NotNil(t, lookup.Record.Err)
}
