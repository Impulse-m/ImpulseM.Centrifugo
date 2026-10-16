package rpcdedup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDoneNotificationRoundTrip(t *testing.T) {
	key := Key{Scope: "s:1f2e3d4c5b6a7988:9c0d1e2f", MsgID: 1887436800000001}
	data := EncodeDone(key)
	require.Equal(t, `{"s":"s:1f2e3d4c5b6a7988:9c0d1e2f","m":1887436800000001}`, string(data))

	decoded, err := DecodeDone(data)
	require.NoError(t, err)
	require.Equal(t, key, decoded)
}

func TestDoneNotificationMalformed(t *testing.T) {
	for _, data := range []string{
		``,
		`not json`,
		`{"s":"","m":1}`,
		`{"m":1}`,
		`{"s":"a","m":"x"}`,
		`[]`,
	} {
		_, err := DecodeDone([]byte(data))
		require.Error(t, err, data)
	}
}
