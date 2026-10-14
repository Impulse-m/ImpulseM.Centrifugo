package rpcdedup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testMsgID returns the msg_id of the Unix second sec with sequence seq.
func testMsgID(sec int64, seq uint64) uint64 {
	return uint64(sec)<<MsgIDSeqBits | seq
}

func TestMsgIDInWindow(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	window := 120 * time.Second
	maxSkew := 30 * time.Second
	nowSec := now.Unix()

	testCases := []struct {
		name  string
		msgID uint64
		want  bool
	}{
		{name: "now", msgID: testMsgID(nowSec, 1), want: true},
		{name: "oldest in window", msgID: testMsgID(nowSec-120, 0), want: true},
		{name: "older than window", msgID: testMsgID(nowSec-121, 0), want: false},
		{name: "furthest ahead", msgID: testMsgID(nowSec+30, 0), want: true},
		{name: "ahead of skew", msgID: testMsgID(nowSec+31, 0), want: false},
		{name: "sequence does not move the second", msgID: testMsgID(nowSec, 1<<MsgIDSeqBits-1), want: true},
		{name: "above 53 bits", msgID: 1 << 53, want: false},
		{name: "max uint64", msgID: ^uint64(0), want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, MsgIDInWindow(tc.msgID, now, window, maxSkew))
		})
	}
}

// A call is recorded for RecordTTL, so the record answers a retransmit whose msg_id is ahead by the skew, up to the
// last instant the window accepts it. The store is driven by the clock of the test.
func TestRecordTTLCoversAcceptedMsgIDs(t *testing.T) {
	window := 120 * time.Second
	maxSkew := 30 * time.Second
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.TTL = RecordTTL(window, maxSkew)
	store := NewMemory(options)
	key := Key{Scope: testScope("u1", "a"), MsgID: testMsgID(clock.now().Add(maxSkew).Unix(), 1)}
	testAnswer(t, store, key, "ok")

	// The window accepts the msg_id until its second plus the window, plus one second: the last instant is one
	// nanosecond before that, and the record must still answer then.
	clock.advance(window + maxSkew + time.Second - time.Nanosecond)
	require.True(t, MsgIDInWindow(key.MsgID, clock.now(), window, maxSkew))
	record, done := testDone(t, store, key)
	require.True(t, done)
	require.Equal(t, []byte("ok"), record.Reply)

	clock.advance(time.Nanosecond)
	require.False(t, MsgIDInWindow(key.MsgID, clock.now(), window, maxSkew))
	checkInvariants(t, store)
}
