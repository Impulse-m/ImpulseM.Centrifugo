package ratelimit

import (
	"os"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
)

// TestMain sets up the metrics which the limiters update, for every test of the package.
func TestMain(m *testing.M) {
	_ = metrics.Init(metrics.Config{
		Registerer: prometheus.NewRegistry(),
	})
	os.Exit(m.Run())
}
