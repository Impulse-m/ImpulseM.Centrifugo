package proxy

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// The lines of plain Go errors are throttled across all targets. The line which comes after a throttled error reports
// how many errors it did not log, so a second target's failure in the same interval is counted, not dropped.
func TestNonStatusErrLogCountsSuppressedLines(t *testing.T) {
	var buf bytes.Buffer
	previousLogger := log.Logger
	previousLimiter := nonStatusErrLog
	t.Cleanup(func() {
		log.Logger = previousLogger
		nonStatusErrLog = previousLimiter
		nonStatusErrSuppressed.Store(0)
	})
	log.Logger = zerolog.New(&buf)
	nonStatusErrLog = tools.NewIntervalLimiter(time.Hour)
	nonStatusErrSuppressed.Store(0)

	failure := errors.New("dial tcp 10.0.0.5:9000: connection refused")
	for range 3 {
		_, _ = MapGRPCRegistryError(failure, nil, configtypes.RPCGRPCStatusToCodeTransforms{}, configtypes.RPCRegistryAppError{}, nil)
	}
	require.Equal(t, 1, bytes.Count(buf.Bytes(), []byte("non-status error")), "one line in the interval")

	nonStatusErrLog = tools.NewIntervalLimiter(0)
	_, _ = MapGRPCRegistryError(failure, nil, configtypes.RPCGRPCStatusToCodeTransforms{}, configtypes.RPCRegistryAppError{}, nil)
	require.Equal(t, 2, bytes.Count(buf.Bytes(), []byte("non-status error")))
	require.Contains(t, buf.String(), `"suppressed":2`)
}
