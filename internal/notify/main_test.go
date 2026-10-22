package notify

import (
	"os"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMain(m *testing.M) {
	_ = metrics.Init(metrics.Config{
		Registerer: prometheus.NewRegistry(),
	})
	os.Exit(m.Run())
}
