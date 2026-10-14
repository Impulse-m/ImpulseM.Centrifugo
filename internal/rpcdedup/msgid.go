package rpcdedup

import (
	"time"
)

// MsgIDSeqBits is the number of low bits of a msg_id which hold the per-second sequence. The bits above
// them hold the Unix second, so a msg_id is exact as a JSON number.
const MsgIDSeqBits = 20

// msgIDBits is the number of bits of a msg_id. A msg_id with a higher bit set is out of range.
const msgIDBits = 53

// MsgIDInWindow reports whether msgID fits in 53 bits and its time part, at second resolution, is in
// [now - window, now + maxSkew]. The caller does not check msgID zero, which means no dedup.
func MsgIDInWindow(msgID uint64, now time.Time, window time.Duration, maxSkew time.Duration) bool {
	if msgID>>msgIDBits != 0 {
		return false
	}
	sec := int64(msgID >> MsgIDSeqBits)
	nowSec := now.Unix()
	return sec >= nowSec-int64(window/time.Second) && sec <= nowSec+int64(maxSkew/time.Second)
}

// RecordTTL returns how long the record of a call is kept, for the window and the skew of the msg_ids. A msg_id
// whose second is at most maxSkew ahead of the server time is accepted until that second plus the window, and
// MsgIDInWindow compares whole seconds, so the record is kept one second longer. A record kept this long answers
// every retransmit which the window accepts.
func RecordTTL(window time.Duration, maxSkew time.Duration) time.Duration {
	return window + maxSkew + time.Second
}
