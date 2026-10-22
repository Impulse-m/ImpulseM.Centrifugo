package accessrevoke

import (
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/stretchr/testify/require"
)

// clampedCount returns the number of expire_at clamps counted for the source.
func clampedCount(t *testing.T, source string) float64 {
	t.Helper()
	return labeledCounterValue(t, "centrifugo_access_revoke_expire_at_clamped_total", "source", source)
}

// TestKafkaSourceClampsFromProcessingTime applies a block whose record is two hours old, with a far expire_at and a max
// TTL of one hour. The clamp counts from the processing time, so the block is applied with now plus the max TTL as its
// expiry. Counted from the record time, the clamp would give a time in the past and the block would be skipped as stale.
func TestKafkaSourceClampsFromProcessingTime(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	cfg := sourceTestConfig()
	cfg.AccessRevoke.MaxTTL = configtypes.Duration(time.Hour)
	source, store := newSourceTest(t, cfg, clock)
	clampsBefore := clampedCount(t, ClampSourceKafka)
	appliedBefore := appliedCount(t, KindBlockUser)
	staleBefore := skippedCount(t, skipStale)

	recordTime := clock.now.Add(-2 * time.Hour)
	farExpireAt := clock.now.Add(100 * 365 * 24 * time.Hour).Unix()
	source.handleRecord(testRecord("block_user", `{"user":"user-1","expire_at":`+strconv.FormatInt(farExpireAt, 10)+`}`, recordTime))

	require.Equal(t, clampsBefore+1, clampedCount(t, ClampSourceKafka))
	require.Equal(t, appliedBefore+1, appliedCount(t, KindBlockUser))
	require.Equal(t, staleBefore, skippedCount(t, skipStale))
	require.Equal(t, Blocked, userVerdict(store, "user-1"))

	clock.now = clock.now.Add(time.Hour - time.Second)
	require.Equal(t, Blocked, userVerdict(store, "user-1"))
	clock.now = clock.now.Add(2 * time.Second)
	require.Equal(t, Allowed, userVerdict(store, "user-1"))
}
