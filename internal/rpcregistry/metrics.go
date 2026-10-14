package rpcregistry

import (
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

// TargetMetrics holds the proxy metric children of one registry target. A nil value does nothing.
type TargetMetrics struct {
	children *metrics.ProxyCallChildren
}

// newTargetMetrics resolves the proxy metric children of target, with the grpc protocol and the rpc
// type. It returns nil when the proxy metrics are not initialised.
func newTargetMetrics(target string) *TargetMetrics {
	children := metrics.NewProxyCallChildren("grpc", "rpc", target)
	if children == nil {
		return nil
	}
	return &TargetMetrics{children: children}
}

// ObserveCall records the duration of one call to the target, and counts it as an error when failed.
func (m *TargetMetrics) ObserveCall(seconds float64, failed bool) {
	if m == nil {
		return
	}
	m.children.Duration.Observe(seconds)
	m.children.Summary.Observe(seconds)
	if failed {
		m.children.Errors.Inc()
	}
}

// IncInflight counts a call to the target which started.
func (m *TargetMetrics) IncInflight() {
	if m == nil {
		return
	}
	m.children.Inflight.Inc()
}

// DecInflight counts a call to the target which ended.
func (m *TargetMetrics) DecInflight() {
	if m == nil {
		return
	}
	m.children.Inflight.Dec()
}
