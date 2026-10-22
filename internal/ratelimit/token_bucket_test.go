package ratelimit

import (
	"math"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
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

func TestCommandPlanRefusesOnSaturatedTAT(t *testing.T) {
	// One token per 9e18 ns: the first operation at now=1e18 is allowed and its new TAT, now + 9e18, saturates.
	p := newCommandPlan(configtypes.RateLimitClientCommand{
		Enabled: true,
		RateLimitOperations: configtypes.RateLimitOperations{
			Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Duration(9e18), 1)},
		},
	})
	slots := make([]int64, p.numSlots)
	const now int64 = 1e18
	require.Zero(t, p.allow(slots, now, OperationPublish, ""))
	// A TAT that wrapped negative would allow this second operation, so it must be refused.
	require.Positive(t, p.allow(slots, now, OperationPublish, ""))
	require.Equal(t, int64(math.MaxInt64), slots[0])
}
