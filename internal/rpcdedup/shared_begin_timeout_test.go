package rpcdedup

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A begin which the shared tier does not answer ends at begin_timeout, when it is set, and not at the IO timeout. The
// begin then fails as a Redis error does, so under fail_closed the call is refused and not run.
func TestSharedBeginBoundedByBeginTimeout(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.beginTimeout = 50 * time.Millisecond
	fx.backend.blockBegin = true

	_, err := fx.shared.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.ErrorIs(t, err, context.DeadlineExceeded, "the begin is cut by begin_timeout")
}
