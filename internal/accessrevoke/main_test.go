package accessrevoke

import (
	"os"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

var testRegistry = prometheus.NewRegistry()

func TestMain(m *testing.M) {
	_ = metrics.Init(metrics.Config{
		Registerer: testRegistry,
	})
	os.Exit(m.Run())
}

// counterValue returns the current value of a counter (or gauge) metric of the test registry by name.
func counterValue(t *testing.T, name string) float64 {
	t.Helper()
	families, err := testRegistry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		metricsList := family.GetMetric()
		if len(metricsList) == 0 {
			return 0
		}
		if counter := metricsList[0].GetCounter(); counter != nil {
			return counter.GetValue()
		}
		return metricsList[0].GetGauge().GetValue()
	}
	return 0
}

// labeledCounterValue returns the value of the counter of the test registry which has the label value.
func labeledCounterValue(t *testing.T, name string, labelName string, labelValue string) float64 {
	t.Helper()
	families, err := testRegistry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() != labelName || label.GetValue() != labelValue {
					continue
				}
				if counter := metric.GetCounter(); counter != nil {
					return counter.GetValue()
				}
				return metric.GetGauge().GetValue()
			}
		}
	}
	return 0
}
