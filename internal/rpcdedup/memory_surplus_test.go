package rpcdedup

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A scope which goes over its reserve and then keeps inserting can drop its own records to within its reserve while it
// is still in the surplus scopes. When the budget is still full after its last record is gone, the surplus branch of
// insert must not evict from a scope which holds no record.
func TestMemoryInsertSurplusOwnScopeEmpty(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 2048)
	options.MaxBytes = 16384
	options.ScopeReserveBytes = 8192
	store := NewMemory(options)
	scopeA := testScope("a", "a")
	scopeB := testScope("b", "a")

	// A goes over its reserve first, with records of the overhead only. B fills the rest of the budget.
	for msgID := uint64(1); msgID <= 5; msgID++ {
		testAnswer(t, store, Key{Scope: scopeA, MsgID: msgID}, "")
	}
	for msgID := uint64(1); msgID <= 8; msgID++ {
		testAnswer(t, store, Key{Scope: scopeB, MsgID: msgID}, strings.Repeat("b", 1632))
	}
	require.Equal(t, int64(16384), store.Stats().Bytes)

	// A inserts a record which needs the room of all of A and of part of B.
	testAnswer(t, store, Key{Scope: scopeA, MsgID: 6}, strings.Repeat("a", 1792))

	require.LessOrEqual(t, store.Stats().Bytes, int64(16384), "the budget holds")
	checkInvariants(t, store)
}
