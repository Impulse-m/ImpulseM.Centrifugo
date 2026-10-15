package rpcdedup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCancelNotificationRoundTrip(t *testing.T) {
	key := Key{Scope: "s:1f2e3d4c5b6a7988:9c0d1e2f", MsgID: 1887436800000001}
	data := EncodeCancel(key)
	require.Equal(t, `{"s":"s:1f2e3d4c5b6a7988:9c0d1e2f","m":1887436800000001}`, string(data))

	decoded, err := DecodeCancel(data)
	require.NoError(t, err)
	require.Equal(t, key, decoded)
}

func TestCancelNotificationMalformed(t *testing.T) {
	for _, data := range []string{
		``,
		`not json`,
		`{"s":"","m":1}`,
		`{"m":1}`,
		`{"s":"a","m":"x"}`,
		`[]`,
	} {
		_, err := DecodeCancel([]byte(data))
		require.Error(t, err, data)
	}
}
