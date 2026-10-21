package ratelimit

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// goldenTokenBucketOutcomes is the outcome of each of the 100 steps of the token bucket timeline of
// TestBucketCheckUnchangedGolden: A is allowed, R is refused. goldenTokenBucketWaits holds the wait in nanoseconds of
// every refused step, in order. Both were captured from the token bucket before the algorithms were added (the GCRA of
// Bucket.next), so the token bucket semantics must not move.
const goldenTokenBucketOutcomes = "" +
	"AAAAAAAAAA" +
	"AAAAAAARAA" +
	"ARAAARARAR" +
	"AARARAARAA" +
	"AAAAAAAAAA" +
	"AAAAAAAAAA" +
	"RARAAAAAAA" +
	"RAAARAAARR" +
	"AAAARRARAA" +
	"AAARAAAARA"

var goldenTokenBucketWaits = []int64{
	33890887, 84226882, 72879566, 58641462, 8772101, 90992484, 138609891, 38073022, 31418600, 31190406,
	16627620, 65642484, 108607501, 31742022, 126170226, 122858920, 67790029, 22026208, 12372318,
}

// TestBucketCheckUnchangedGolden replays the seeded token bucket timeline and checks each outcome and wait against the
// captured table. A change of the token bucket arithmetic shows up here.
func TestBucketCheckUnchangedGolden(t *testing.T) {
	require.Len(t, goldenTokenBucketOutcomes, 100)
	require.Len(t, goldenTokenBucketWaits, 19)
	algo := tokenBucket{}
	lim := algo.newLimit(time.Second, 5)
	st := make([]int64, algo.stateWords())
	rng := rand.New(rand.NewPCG(42, 99))
	now := int64(time.Hour)
	refused := 0
	for i := range len(goldenTokenBucketOutcomes) {
		now += rng.Int64N(int64(300 * time.Millisecond))
		wait := algo.wait(&lim, st, now)
		if goldenTokenBucketOutcomes[i] == 'A' {
			require.Zero(t, wait, "step %d", i)
			algo.take(&lim, st, now)
			continue
		}
		require.Equal(t, byte('R'), goldenTokenBucketOutcomes[i], "step %d", i)
		require.Equal(t, goldenTokenBucketWaits[refused], wait, "step %d", i)
		refused++
	}
	require.Equal(t, len(goldenTokenBucketWaits), refused)
}
