package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoundReaderBindReadUnbind(t *testing.T) {
	var reader boundReader[int]
	require.Equal(t, 0, reader.Read())

	reader.Bind(func() int {
		return 7
	})
	require.Equal(t, 7, reader.Read())

	reader.Bind(nil)
	require.Equal(t, 0, reader.Read())
}
