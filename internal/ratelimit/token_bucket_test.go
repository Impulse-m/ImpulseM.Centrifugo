package ratelimit

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGCRASaturatesOnOverflow(t *testing.T) {
	const emission int64 = 9e18
	const tolerance int64 = 9e18

	// The new TAT is 9e18-1 + 9e18, which is above math.MaxInt64, so it saturates instead of wrapping negative.
	newTAT, wait := gcra(emission, tolerance, 9e18-1, 1e18)
	require.Equal(t, int64(math.MaxInt64), newTAT)
	require.Positive(t, wait)
}
