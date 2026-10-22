package accessrevoke

import (
	"fmt"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// setCompactTestHook sets the compaction hook for the test and removes it when the test ends.
func setCompactTestHook(t *testing.T, hook func(stage string)) {
	t.Helper()
	compactTestHook.Store(&hook)
	t.Cleanup(func() { compactTestHook.Store(nil) })
}

// TestCompactionDoesNotBlockChecks makes a check run while the rebuild of the blocked kind is built. The check takes the
// read lock too, so it returns at once; a rebuild under the write lock would hold it until the rebuild ends.
func TestCompactionDoesNotBlockChecks(t *testing.T) {
	store := NewStore(time.Hour)
	for i := range 10 {
		require.NoError(t, store.BlockUser(fmt.Sprintf("u%d", i), 0))
	}
	for i := range 5 {
		require.NoError(t, store.UnblockUser(fmt.Sprintf("u%d", i)))
	}

	setCompactTestHook(t, func(stage string) {
		if stage != "rebuilt" {
			return
		}
		checked := make(chan Verdict, 1)
		go func() {
			checked <- store.Check("u9", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict
		}()
		select {
		case verdict := <-checked:
			require.Equal(t, Blocked, verdict)
		case <-time.After(5 * time.Second):
			t.Error("check waited for the rebuild of the blocked kind")
		}
	})
	require.NoError(t, store.UnblockUser("u5"))
	require.Len(t, store.blockedExpiry, 4)
}

// TestCompactionSkipsSwapAfterWrite blocks a user between the rebuild and the swap. The swap is skipped, so the rebuild
// does not drop the write, and the stale items stay for the next pass.
func TestCompactionSkipsSwapAfterWrite(t *testing.T) {
	store := NewStore(time.Hour)
	for i := range 10 {
		require.NoError(t, store.BlockUser(fmt.Sprintf("u%d", i), 0))
	}
	for i := range 5 {
		require.NoError(t, store.UnblockUser(fmt.Sprintf("u%d", i)))
	}

	setCompactTestHook(t, func(stage string) {
		if stage == "swap" {
			require.NoError(t, store.BlockUser("late", 0))
		}
	})
	require.NoError(t, store.UnblockUser("u5"))

	require.Equal(t, 5, store.Sizes().Blocked)
	require.Len(t, store.blockedExpiry, 11)
	require.Equal(t, Blocked, store.Check("late", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
	require.Equal(t, Blocked, store.Check("u9", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
	require.Equal(t, Allowed, store.Check("u5", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
}
