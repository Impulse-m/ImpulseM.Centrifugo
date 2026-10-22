package accessrevoke

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// setCompactTestHook sets the compaction hook of the store for the test.
func setCompactTestHook(store *Store, hook func(stage string)) {
	store.compactHook = hook
}

// blockForCompaction blocks ten users with an expiry in the future, so an expiry pass does not pop their items, and
// unblocks the first unblocked of them. The stale items of the unblocked users stay in the order.
func blockForCompaction(t *testing.T, store *Store, unblocked int) {
	t.Helper()
	expireAt := time.Now().Add(time.Hour).Unix()
	for i := range 10 {
		require.NoError(t, store.BlockUser(fmt.Sprintf("u%d", i), expireAt))
	}
	for i := range unblocked {
		require.NoError(t, store.UnblockUser(fmt.Sprintf("u%d", i)))
	}
}

// TestCompactionDoesNotBlockChecks makes a check run while the rebuild of the blocked kind is built. The check takes the
// read lock too, so it returns at once while the rebuild holds no lock.
func TestCompactionDoesNotBlockChecks(t *testing.T) {
	store := NewStore(time.Hour)
	blockForCompaction(t, store, 6)

	var rebuilt bool
	setCompactTestHook(store, func(stage string) {
		if stage != "rebuilt" {
			return
		}
		rebuilt = true
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
	store.expireDue(time.Now().Unix())

	require.True(t, rebuilt, "the expiry pass did not rebuild the blocked kind")
	require.Len(t, store.blockedExpiry, 4)
}

// TestCompactionKeepsChecksFastUnderWrites rebuilds a kind of 100000 live entries with 75000 stale items, while a writer
// blocks and unblocks a user every millisecond and a reader checks in a loop. The longest check must stay under 50 ms,
// so a writer which queues behind the rebuild does not hold the checks for the whole of it.
func TestCompactionKeepsChecksFastUnderWrites(t *testing.T) {
	store := NewStore(time.Hour)
	expireAt := time.Now().Add(time.Hour).Unix()
	for i := range 100000 {
		require.NoError(t, store.BlockUser(fmt.Sprintf("bulk%d", i), expireAt))
	}
	for i := range 75000 {
		require.NoError(t, store.UnblockUser(fmt.Sprintf("bulk%d", i)))
	}

	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
			user := fmt.Sprintf("churn%d", i%100)
			_ = store.BlockUser(user, expireAt)
			_ = store.UnblockUser(user)
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		store.expireDue(time.Now().Unix())
	}()
	var maxWait time.Duration
	for running := true; running; {
		start := time.Now()
		store.Check("bulk99999", centrifuge.TokenIdentity{}, CheckOptions{Block: true})
		maxWait = max(maxWait, time.Since(start))
		select {
		case <-done:
			running = false
		default:
		}
	}
	close(stop)
	writer.Wait()

	require.Less(t, maxWait, 50*time.Millisecond)
	store.expireDue(time.Now().Unix())
	require.LessOrEqual(t, len(store.blockedExpiry), 2*store.Sizes().Blocked+1)
}

// TestUnblockUserDoesNotCompact unblocks enough users to pass the compaction threshold, and expects no rebuild on the
// unblock path. The expiry pass is the one which rebuilds.
func TestUnblockUserDoesNotCompact(t *testing.T) {
	store := NewStore(time.Hour)
	rebuilds := 0
	blockForCompaction(t, store, 0)
	setCompactTestHook(store, func(stage string) {
		if stage == "rebuilt" {
			rebuilds++
		}
	})

	for i := range 6 {
		require.NoError(t, store.UnblockUser(fmt.Sprintf("u%d", i)))
	}
	require.Equal(t, 0, rebuilds)
	require.Len(t, store.blockedExpiry, 10)
	require.Equal(t, 4, store.Sizes().Blocked)

	store.expireDue(time.Now().Unix())
	require.Equal(t, 1, rebuilds)
	require.Len(t, store.blockedExpiry, 4)
}

// TestCompactionSkipsSwapAfterWrite blocks a user between each rebuild and its swap. Each such swap is skipped, so the
// rebuilds do not drop the writes; the attempts stop once the order is within the threshold again.
func TestCompactionSkipsSwapAfterWrite(t *testing.T) {
	store := NewStore(time.Hour)
	blockForCompaction(t, store, 6)

	swaps := 0
	setCompactTestHook(store, func(stage string) {
		if stage == "swap" {
			require.NoError(t, store.BlockUser(fmt.Sprintf("late%d", swaps), 0))
			swaps++
		}
	})
	store.expireDue(time.Now().Unix())

	require.Positive(t, swaps)
	require.LessOrEqual(t, swaps, compactAttempts)
	require.Equal(t, 4+swaps, store.Sizes().Blocked)
	require.Len(t, store.blockedExpiry, 10+swaps)
	for i := range swaps {
		require.Equal(t, Blocked, store.Check(fmt.Sprintf("late%d", i), centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
	}
	require.Equal(t, Blocked, store.Check("u9", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
	require.Equal(t, Allowed, store.Check("u5", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
}

// TestCompactionRetriesSkippedSwap blocks a user between the first rebuild and its swap only. The second attempt
// rebuilds the kind with the write in it, and its swap is made.
func TestCompactionRetriesSkippedSwap(t *testing.T) {
	store := NewStore(time.Hour)
	blockForCompaction(t, store, 6)

	swaps := 0
	setCompactTestHook(store, func(stage string) {
		if stage != "swap" {
			return
		}
		swaps++
		if swaps == 1 {
			require.NoError(t, store.BlockUser("late", 0))
		}
	})
	store.expireDue(time.Now().Unix())

	require.Equal(t, 2, swaps)
	require.Equal(t, 5, store.Sizes().Blocked)
	require.Len(t, store.blockedExpiry, 5)
	require.Equal(t, Blocked, store.Check("late", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
}

// TestCompactionFallbackBoundsHeapUnderChurn unblocks a user between each rebuild and its swap, so every attempt is
// skipped. The pass then falls back to the rebuild under the write lock, and the heap holds at most twice the entries
// plus one, and no unblocked user is blocked again.
func TestCompactionFallbackBoundsHeapUnderChurn(t *testing.T) {
	store := NewStore(time.Hour)
	blockForCompaction(t, store, 6)

	swaps := 0
	setCompactTestHook(store, func(stage string) {
		if stage != "swap" {
			return
		}
		require.NoError(t, store.UnblockUser(fmt.Sprintf("u%d", 6+swaps)))
		swaps++
	})
	store.expireDue(time.Now().Unix())

	require.Equal(t, compactAttempts, swaps)
	sizes := store.Sizes()
	require.Equal(t, 1, sizes.Blocked)
	require.LessOrEqual(t, len(store.blockedExpiry), 2*sizes.Blocked+1)
	require.Equal(t, Blocked, store.Check("u9", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
	require.Equal(t, Allowed, store.Check("u8", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
}
