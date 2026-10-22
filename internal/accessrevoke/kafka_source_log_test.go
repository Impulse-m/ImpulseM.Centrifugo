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
	clock := &sourceTestClock{now: time.Now()}
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
	require.Equal(t, 1, strings.Count(logged.String(), "\n"))
}
