package metrics

// ExpiredRefusal counts one call of the method which was refused because its token had expired. The series of the
// method is created at its first refusal, so a method which is never refused has none.
func (m *RPCRegistryMethodMetrics) ExpiredRefusal(rpcMethod string) {
	if m == nil || RPCRegistryCallsTotal == nil {
		return
	}
	rpcRegistryDropped.count(rpcMethod, func() {
		RPCRegistryCallsTotal.WithLabelValues(rpcMethod, RPCRegistryResultExpired).Inc()
	})
}
