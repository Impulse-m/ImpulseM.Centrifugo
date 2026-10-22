package rpcdedup

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A reply over the limit is stored uncached in the shared tier too, so Redis holds no reply for it. A duplicate on the
// node of the owner is answered from its local record, which knows that, and does not poll Redis for the reply each time.
func TestSharedUncachedOversizeDuplicateNoPoll(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte(strings.Repeat("r", 2000))}))

	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.True(t, entry.rec.Uncached)

	for range 3 {
		lookup, err := fx.shared.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
		require.NoError(t, err)
		require.Equal(t, OutcomeDone, lookup.Outcome)
		require.True(t, lookup.Record.Uncached)
	}
	require.Equal(t, 0, countCalls(fx.backend.callLog(), "poll"))
}
