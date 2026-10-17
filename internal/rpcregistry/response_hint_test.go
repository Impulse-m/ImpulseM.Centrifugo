package rpcregistry

import (
	"math"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	testHintMaxBytes  int = 1 << 20
	testHintOverhead  int = 1024
	testHintFieldName int = 3
)

// testHintMethod returns a method with the hint on field 3 and the test limits.
func testHintMethod() *Method {
	return &Method{
		MaxResponseBytes: testHintMaxBytes,
		ResponseSizeHint: resolveResponseSizeHint(&configtypes.RPCResponseSizeHint{
			RequestField:  testHintFieldName,
			OverheadBytes: testHintOverhead,
		}),
	}
}

// testVarintField returns the encoding of a varint field.
func testVarintField(num protowire.Number, value uint64) []byte {
	out := protowire.AppendTag(nil, num, protowire.VarintType)
	return protowire.AppendVarint(out, value)
}

// testBytesField returns the encoding of a length-delimited field.
func testBytesField(num protowire.Number, value []byte) []byte {
	out := protowire.AppendTag(nil, num, protowire.BytesType)
	return protowire.AppendBytes(out, value)
}

func TestResponseBound(t *testing.T) {
	var negativeInt32 int32 = -1
	cases := []struct {
		name string
		data []byte
		want int
	}{
		{
			name: "hint as the only field",
			data: hintValue(64 << 10),
			want: 64<<10 + testHintOverhead,
		},
		{
			name: "hint as the first field",
			data: append(hintValue(64<<10), testVarintField(1, 7)...),
			want: 64<<10 + testHintOverhead,
		},
		{
			name: "hint as the third field after a string and a number",
			data: append(append(testBytesField(1, []byte("abc")), testVarintField(2, 9)...), hintValue(4096)...),
			want: 4096 + testHintOverhead,
		},
		{
			name: "zero value gives the full bound",
			data: hintValue(0),
			want: testHintMaxBytes,
		},
		{
			name: "field absent gives the full bound",
			data: testVarintField(1, 5),
			want: testHintMaxBytes,
		},
		{
			name: "empty request gives the full bound",
			data: nil,
			want: testHintMaxBytes,
		},
		{
			name: "one below max minus overhead adds the overhead",
			data: hintValue(uint64(testHintMaxBytes - testHintOverhead - 1)),
			want: testHintMaxBytes - 1,
		},
		{
			name: "exactly max minus overhead gives the full bound",
			data: hintValue(uint64(testHintMaxBytes - testHintOverhead)),
			want: testHintMaxBytes,
		},
		{
			name: "over max gives the full bound",
			data: hintValue(uint64(testHintMaxBytes)),
			want: testHintMaxBytes,
		},
		{
			name: "2^64-1 gives the full bound, not a wrapped one",
			data: hintValue(math.MaxUint64),
			want: testHintMaxBytes,
		},
		{
			name: "2^64-512 gives the full bound, not a wrapped one",
			data: hintValue(math.MaxUint64 - 511),
			want: testHintMaxBytes,
		},
		{
			name: "negative int32 is a 10-byte varint near 2^64 and gives the full bound",
			data: hintValue(uint64(negativeInt32)),
			want: testHintMaxBytes,
		},
		{
			name: "bytes field with the hinted number gives the full bound",
			data: testBytesField(protowire.Number(testHintFieldName), []byte{0x08, 0x01}),
			want: testHintMaxBytes,
		},
		{
			name: "bytes field before the hinted field is skipped by its length",
			data: append(testBytesField(1, []byte{0x18, 0x05}), hintValue(2048)...),
			want: 2048 + testHintOverhead,
		},
		{
			name: "truncated varint gives the full bound",
			data: protowire.AppendTag(nil, protowire.Number(testHintFieldName), protowire.VarintType),
			want: testHintMaxBytes,
		},
		{
			name: "truncated tag gives the full bound",
			data: []byte{0x80},
			want: testHintMaxBytes,
		},
		{
			name: "truncated field before the hinted one gives the full bound",
			data: append(testBytesField(1, []byte("abcdef"))[:3], hintValue(2048)...),
			want: testHintMaxBytes,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, testHintMethod().ResponseBound(tc.data))
		})
	}
}

func TestResponseBoundFieldAfterLimit(t *testing.T) {
	var data []byte
	for i := 0; i < maxResponseHintTags; i++ {
		data = append(data, testVarintField(1, 0)...)
	}
	data = append(data, hintValue(2048)...)
	require.Equal(t, testHintMaxBytes, testHintMethod().ResponseBound(data), "the hint after the tag limit is not read")

	var within []byte
	for i := 0; i < maxResponseHintTags-1; i++ {
		within = append(within, testVarintField(1, 0)...)
	}
	within = append(within, hintValue(2048)...)
	require.Equal(t, 2048+testHintOverhead, testHintMethod().ResponseBound(within), "the hint within the tag limit is read")
}

func TestResponseBoundLastOccurrenceWins(t *testing.T) {
	method := testHintMethod()
	data := append(hintValue(64<<10), hintValue(10_000_000)...)
	require.Equal(t, testHintMaxBytes, method.ResponseBound(data), "a repeated field keeps its last value, as the backend decodes it")

	data = append(hintValue(10_000_000), hintValue(2048)...)
	require.Equal(t, 2048+testHintOverhead, method.ResponseBound(data), "a repeated field keeps its last value, as the backend decodes it")
}

func TestResponseBoundDoesNotAllocate(t *testing.T) {
	method := testHintMethod()
	data := append(testBytesField(1, []byte("0123456789abcdef")), hintValue(4096)...)
	allocs := testing.AllocsPerRun(100, func() {
		_ = method.ResponseBound(data)
	})
	require.Zero(t, allocs)
}

func TestResponseBoundNilHint(t *testing.T) {
	method := &Method{MaxResponseBytes: testHintMaxBytes}
	require.Equal(t, testHintMaxBytes, method.ResponseBound(testVarintField(protowire.Number(testHintFieldName), 2048)))
}

func TestResolveResponseSizeHintDefaultOverhead(t *testing.T) {
	require.Nil(t, resolveResponseSizeHint(nil))
	resolved := resolveResponseSizeHint(&configtypes.RPCResponseSizeHint{RequestField: 2})
	require.Equal(t, configtypes.DefaultResponseSizeOverheadBytes, resolved.OverheadBytes)
}

func hintValue(value uint64) []byte {
	return testVarintField(protowire.Number(testHintFieldName), value)
}

func BenchmarkResponseBound(b *testing.B) {
	method := testHintMethod()
	data := append(append(testBytesField(1, []byte("0123456789abcdef")), testVarintField(2, 9)...), hintValue(4096)...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if method.ResponseBound(data) != 4096+testHintOverhead {
			b.Fatal("unexpected bound")
		}
	}
}
