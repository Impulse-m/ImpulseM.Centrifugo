package rpcdedup

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/centrifugal/centrifuge"
)

// Record states of a Redis record, the value of field s.
const (
	redisStateInFlight  = "i"
	redisStateDone      = "d"
	redisStateCancelled = "c"
)

// Field names of a Redis record which the Go side reads: the state, reply, error and uncached flag. The scripts
// name every field they touch, see redis_scripts.go.
const (
	redisFieldState    = "s"
	redisFieldReply    = "r"
	redisFieldError    = "e"
	redisFieldUncached = "u"
)

// maxWaiters is the number of waiter node ids an in-flight record lists. A node beyond it is not registered and relies
// on its fallback poll. It is the 8 of beginScript.
const maxWaiters = 8

// errMalformedReply is returned for a script reply which the Redis backend does not know. It means a foreign write
// under the same prefix, and is counted as a Redis error.
var errMalformedReply = errors.New("malformed reply of the shared tier")

// RedisKey returns the key of a call in Redis: {prefix}.rpc.{scope}.{msg_id}. Braces around the scope make it the
// Redis Cluster hash tag, so all records of a scope live in one slot. The scope is a hash, so it identifies the
// connection's scope only, and no user id or session reaches Redis.
func RedisKey(prefix string, key Key) string {
	return prefix + ".rpc.{" + key.Scope + "}." + strconv.FormatUint(key.MsgID, 10)
}

// decodeError returns the error of the JSON form s of field e. "" is no error.
func decodeError(s string) (*centrifuge.Error, error) {
	if s == "" {
		return nil, nil
	}
	var e redisError
	if err := json.Unmarshal([]byte(s), &e); err != nil {
		return nil, fmt.Errorf("%w: %v", errMalformedReply, err)
	}
	return &centrifuge.Error{
		Code:      e.Code,
		Message:   e.Message,
		Temporary: e.Temporary,
		AppCode:   e.AppCode,
		AppType:   e.AppType,
		Metadata:  e.Metadata,
	}, nil
}

// recordArgs returns the arguments of a record for the complete script: the reply, and "1" when the reply is not kept.
// An uncached record keeps no reply. The error JSON and the abandoned flag are arguments of their own, the caller sends
// them.
func recordArgs(rec Record) (string, string) {
	if rec.Uncached {
		return "", "1"
	}
	return string(rec.Reply), "0"
}

// redisTTLMillis returns d in milliseconds for PEXPIRE, at least one millisecond.
func redisTTLMillis(d time.Duration) string {
	ms := d.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return strconv.FormatInt(ms, 10)
}

// parseBegin returns the result of the begin script from its reply.
func parseBegin(parts []string) (BeginResult, error) {
	if len(parts) == 0 {
		return BeginResult{}, errMalformedReply
	}
	switch parts[0] {
	case "miss":
		return BeginResult{State: BeginMiss}, nil
	case "mismatch":
		return BeginResult{State: BeginMismatch}, nil
	case "cancelled":
		if len(parts) != 2 || parts[1] == "" {
			return BeginResult{}, errMalformedReply
		}
		rec, err := parseRecord("", parts[1], "")
		if err != nil {
			return BeginResult{}, err
		}
		return BeginResult{State: BeginCancelled, Record: rec}, nil
	case "inflight":
		if len(parts) != 2 || parts[1] == "" {
			return BeginResult{}, errMalformedReply
		}
		return BeginResult{State: BeginInFlight, Owner: parts[1]}, nil
	case "done":
		if len(parts) != 4 {
			return BeginResult{}, errMalformedReply
		}
		rec, err := parseRecord(parts[1], parts[2], parts[3])
		if err != nil {
			return BeginResult{}, err
		}
		return BeginResult{State: BeginDone, Record: rec}, nil
	}
	return BeginResult{}, errMalformedReply
}

// parsePoll returns the result of a poll from the fields s, r, e and u of a record. A missing record has no fields. A
// tombstone is PollCancelled, and it is returned without a fingerprint: the watcher keeps it so, as BeginCancelled does.
// A cancelled record without its error is malformed: it would be answered as a success.
func parsePoll(state string, reply string, errJSON string, uncached string) (PollResult, error) {
	switch state {
	case "":
		return PollResult{State: PollMissing}, nil
	case redisStateInFlight:
		return PollResult{State: PollInFlight}, nil
	case redisStateDone:
		rec, err := parseRecord(reply, errJSON, uncached)
		if err != nil {
			return PollResult{}, err
		}
		return PollResult{State: PollDone, Record: rec}, nil
	case redisStateCancelled:
		if errJSON == "" {
			return PollResult{}, errMalformedReply
		}
		rec, err := parseRecord("", errJSON, "")
		if err != nil {
			return PollResult{}, err
		}
		return PollResult{State: PollCancelled, Record: rec}, nil
	}
	return PollResult{}, errMalformedReply
}

// parseCancel returns the result of the cancel script from its reply.
func parseCancel(parts []string) (CancelResult, error) {
	if len(parts) == 0 {
		return CancelResult{}, errMalformedReply
	}
	switch parts[0] {
	case "tombstone":
		return CancelResult{State: CancelTombstone}, nil
	case "done":
		return CancelResult{State: CancelDone}, nil
	case "inflight":
		if len(parts) != 2 || parts[1] == "" {
			return CancelResult{}, errMalformedReply
		}
		return CancelResult{State: CancelInFlight, Owner: parts[1]}, nil
	}
	return CancelResult{}, errMalformedReply
}

// parseComplete returns the result of the complete script from its reply: owned, and the waiters of the record.
func parseComplete(parts []string) (CompleteResult, error) {
	if len(parts) != 2 {
		return CompleteResult{}, errMalformedReply
	}
	switch parts[0] {
	case "0":
		return CompleteResult{}, nil
	case "1":
		return CompleteResult{Owned: true, Waiters: splitWaiters(parts[1])}, nil
	}
	return CompleteResult{}, errMalformedReply
}

// splitWaiters returns the node ids of a waiter list, none for "".
func splitWaiters(w string) []string {
	return strings.Fields(w)
}

// parseRecord returns the record of the reply fields of a done record: reply, error JSON and the uncached flag.
func parseRecord(reply string, errJSON string, uncached string) (Record, error) {
	e, err := decodeError(errJSON)
	if err != nil {
		return Record{}, err
	}
	rec := Record{Err: e}
	if uncached == "1" {
		rec.Uncached = true
		return rec, nil
	}
	rec.Reply = []byte(reply)
	return rec, nil
}
