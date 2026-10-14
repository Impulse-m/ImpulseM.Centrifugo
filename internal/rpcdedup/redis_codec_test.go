package rpcdedup

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func TestRedisKeyFormat(t *testing.T) {
	require.Equal(t, "centrifugo.rpc.{s:aa:bb}.1887436800000001", RedisKey("centrifugo", Key{Scope: "s:aa:bb", MsgID: 1887436800000001}))
	require.Equal(t, "app.rpc.{a:cc}.7", RedisKey("app", Key{Scope: "a:cc", MsgID: 7}))
}

func TestEncodeErrorRoundTrip(t *testing.T) {
	failure := &centrifuge.Error{
		Code:      1001,
		Message:   "boom",
		Temporary: true,
		AppCode:   42,
		AppType:   "validation",
		Metadata:  map[string]string{"field": "amount"},
	}
	encoded := encodeError(failure)
	decoded, err := decodeError(encoded)
	require.NoError(t, err)
	require.Equal(t, failure, decoded)
}

func TestEncodeErrorOmitsEmptyFields(t *testing.T) {
	require.Equal(t, `{"c":107,"m":"cancelled"}`, encodeError(&centrifuge.Error{Code: 107, Message: "cancelled"}))
	require.Equal(t, `{"c":1}`, encodeError(&centrifuge.Error{Code: 1}))
	require.Equal(t, "", encodeError(nil))
}

func TestDecodeErrorEmptyIsNoError(t *testing.T) {
	decoded, err := decodeError("")
	require.NoError(t, err)
	require.Nil(t, decoded)
}

func TestDecodeErrorMalformed(t *testing.T) {
	decoded, err := decodeError("{not json")
	require.ErrorIs(t, err, errMalformedReply)
	require.Nil(t, decoded)
}

func TestRecordArgs(t *testing.T) {
	reply, errJSON, uncached := recordArgs(Record{Reply: []byte("ok")})
	require.Equal(t, "ok", reply)
	require.Equal(t, "", errJSON)
	require.Equal(t, "0", uncached)

	reply, errJSON, uncached = recordArgs(Record{Err: &centrifuge.Error{Code: 3, Message: "bad"}})
	require.Equal(t, "", reply)
	require.Equal(t, `{"c":3,"m":"bad"}`, errJSON)
	require.Equal(t, "0", uncached)

	// An uncached record keeps no reply.
	reply, errJSON, uncached = recordArgs(Record{Reply: []byte("big"), Uncached: true})
	require.Equal(t, "", reply)
	require.Equal(t, "", errJSON)
	require.Equal(t, "1", uncached)

	// An abandoned record is not kept, its args are empty.
	reply, errJSON, uncached = recordArgs(Record{Abandoned: true})
	require.Equal(t, "", reply)
	require.Equal(t, "", errJSON)
	require.Equal(t, "0", uncached)
}

func TestRedisTTLMillis(t *testing.T) {
	require.Equal(t, "1500", redisTTLMillis(1500*time.Millisecond))
	require.Equal(t, "1", redisTTLMillis(time.Microsecond))
	require.Equal(t, "1", redisTTLMillis(0))
}

func TestParseBeginReplies(t *testing.T) {
	res, err := parseBegin([]string{"miss"})
	require.NoError(t, err)
	require.Equal(t, BeginResult{State: BeginMiss}, res)

	res, err = parseBegin([]string{"mismatch"})
	require.NoError(t, err)
	require.Equal(t, BeginResult{State: BeginMismatch}, res)

	res, err = parseBegin([]string{"inflight", "node-a"})
	require.NoError(t, err)
	require.Equal(t, BeginResult{State: BeginInFlight, Owner: "node-a"}, res)

	res, err = parseBegin([]string{"done", "reply", "", ""})
	require.NoError(t, err)
	require.Equal(t, BeginDone, res.State)
	require.Equal(t, []byte("reply"), res.Record.Reply)
	require.Nil(t, res.Record.Err)
	require.False(t, res.Record.Uncached)

	// A done call which failed answers with its error and no reply.
	res, err = parseBegin([]string{"done", "", `{"c":107,"m":"cancelled"}`, ""})
	require.NoError(t, err)
	require.Equal(t, BeginDone, res.State)
	require.Equal(t, &centrifuge.Error{Code: 107, Message: "cancelled"}, res.Record.Err)

	res, err = parseBegin([]string{"done", "", "", "1"})
	require.NoError(t, err)
	require.True(t, res.Record.Uncached)

	// A tombstone answers with its error, and keeps no fingerprint.
	res, err = parseBegin([]string{"cancelled", `{"c":107,"m":"cancelled"}`})
	require.NoError(t, err)
	require.Equal(t, BeginCancelled, res.State)
	require.Equal(t, &centrifuge.Error{Code: 107, Message: "cancelled"}, res.Record.Err)
	require.Equal(t, [32]byte{}, res.Record.Fingerprint)
}

func TestParseBeginMalformed(t *testing.T) {
	for _, parts := range [][]string{
		nil,
		{"unknown"},
		{"inflight"},
		{"inflight", "a", "b"},
		{"cancelled"},
		{"cancelled", "{bad"},
		{"done", "reply"},
		{"done", "", "{bad", ""},
	} {
		_, err := parseBegin(parts)
		require.Error(t, err, "%v", parts)
	}
}

func TestParsePollReplies(t *testing.T) {
	res, err := parsePoll("", "", "", "")
	require.NoError(t, err)
	require.Equal(t, PollMissing, res.State)

	res, err = parsePoll("i", "", "", "")
	require.NoError(t, err)
	require.Equal(t, PollInFlight, res.State)

	res, err = parsePoll("d", "reply", "", "")
	require.NoError(t, err)
	require.Equal(t, PollDone, res.State)
	require.Equal(t, []byte("reply"), res.Record.Reply)

	res, err = parsePoll("d", "", "", "1")
	require.NoError(t, err)
	require.Equal(t, PollDone, res.State)
	require.True(t, res.Record.Uncached)

	res, err = parsePoll("c", "", `{"c":107}`, "")
	require.NoError(t, err)
	require.Equal(t, PollDone, res.State)
	require.Equal(t, &centrifuge.Error{Code: 107}, res.Record.Err)

	_, err = parsePoll("x", "", "", "")
	require.ErrorIs(t, err, errMalformedReply)
	_, err = parsePoll("d", "", "{bad", "")
	require.ErrorIs(t, err, errMalformedReply)
}

func TestParseCancelReplies(t *testing.T) {
	res, err := parseCancel([]string{"tombstone"})
	require.NoError(t, err)
	require.Equal(t, CancelResult{State: CancelTombstone}, res)

	res, err = parseCancel([]string{"inflight", "node-b"})
	require.NoError(t, err)
	require.Equal(t, CancelResult{State: CancelInFlight, Owner: "node-b"}, res)

	res, err = parseCancel([]string{"done"})
	require.NoError(t, err)
	require.Equal(t, CancelResult{State: CancelDone}, res)

	for _, parts := range [][]string{nil, {"unknown"}, {"inflight"}, {"inflight", "a", "b"}} {
		_, err = parseCancel(parts)
		require.ErrorIs(t, err, errMalformedReply, "%v", parts)
	}
}
