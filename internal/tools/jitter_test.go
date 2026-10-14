package tools

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHalfJitterWithinBounds(t *testing.T) {
	for i := 0; i < 1000; i++ {
		delay := HalfJitter(time.Second)
		require.GreaterOrEqual(t, delay, 500*time.Millisecond)
		require.LessOrEqual(t, delay, time.Second)
	}
}
