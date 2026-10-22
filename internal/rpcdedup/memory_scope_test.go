package rpcdedup

import (
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// The records of a scope share one copy of its scope string, so that a record does not keep the string of its own call.
// The copy is the one the scope was admitted with.
func TestMemoryRecordsShareScopeString(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	store := NewMemory(testMemoryOptions(clock, 1024))
	first := testScope("a", "a")
	second := testScope("a", "a")
	require.False(t, unsafe.StringData(first) == unsafe.StringData(second), "the calls carry separate strings")

	testAnswer(t, store, Key{Scope: first, MsgID: 1}, "one")
	testAnswer(t, store, Key{Scope: second, MsgID: 2}, "two")

	s := store.shardOf(first)
	s.mu.Lock()
	defer s.mu.Unlock()
	sc := s.scopes[first]
	require.NotNil(t, sc)
	r1 := s.done[Key{Scope: first, MsgID: 1}]
	r2 := s.done[Key{Scope: second, MsgID: 2}]
	require.NotNil(t, r1)
	require.NotNil(t, r2)
	require.True(t, unsafe.StringData(r1.key.Scope) == unsafe.StringData(sc.scope), "the first record keeps the admitted string")
	require.True(t, unsafe.StringData(r2.key.Scope) == unsafe.StringData(sc.scope), "the second record keeps the admitted string")
}
