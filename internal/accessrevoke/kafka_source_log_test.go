package accessrevoke

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// TestSkipLogIsSampled skips 10000 records of an unknown method. Every skip is counted, but only one warning is logged
// within the skip log interval.
func TestSkipLogIsSampled(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	source, _ := newSourceTest(t, sourceTestConfig(), clock)
	var logged bytes.Buffer
	previous := log.Logger
	log.Logger = zerolog.New(&logged)
	t.Cleanup(func() { log.Logger = previous })

	before := skippedCount(t, skipUnknownMethod)
	recordTime := clock.now.Add(-time.Minute)
	for range 10000 {
		source.handleRecord(testRecord("publish", `{"user":"user-1"}`, recordTime))
	}

	require.Equal(t, before+10000, skippedCount(t, skipUnknownMethod))
	// The limiter reads the wall clock, so a run which takes longer than the skip log interval, as a race build on a slow
	// runner can, logs one more line. The sampling is what is tested: the lines stay at a few, not one per record.
	require.LessOrEqual(t, strings.Count(logged.String(), "\n"), 2)
}
