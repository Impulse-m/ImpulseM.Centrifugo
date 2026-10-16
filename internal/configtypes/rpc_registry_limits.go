package configtypes

import "time"

// Built-in limits. They apply when neither the method, its target nor the defaults set a value.
const (
	DefaultTimeout          = 5 * time.Second
	DefaultMaxRequestBytes  = 64 << 10
	DefaultMaxResponseBytes = 1 << 20
)

// DefaultResponseSizeOverheadBytes is the envelope added to a response size hint when overhead_bytes is zero.
const DefaultResponseSizeOverheadBytes = 1024

// ResolvedResponseSizeOverhead returns the overhead of a response size hint, with the default applied when
// overhead_bytes is zero. The registry and its validation both use it, so they cannot disagree.
func ResolvedResponseSizeOverhead(hint RPCResponseSizeHint) int {
	if hint.OverheadBytes == 0 {
		return DefaultResponseSizeOverheadBytes
	}
	return hint.OverheadBytes
}

// RPCMethodLimits are the effective limits of a registry method.
type RPCMethodLimits struct {
	Timeout          time.Duration
	MaxRequestBytes  int
	MaxResponseBytes int
}

// ResolveRPCMethodLimits returns the effective limits of method, with the precedence method, target, defaults and
// the built-in limits. The first value which is not zero wins.
func ResolveRPCMethodLimits(method RPCMethod, target RPCTarget, defaults RPCMethodDefaults) RPCMethodLimits {
	return RPCMethodLimits{
		Timeout: firstNonZero(
			method.Timeout.ToDuration(),
			target.Timeout.ToDuration(),
			defaults.Timeout.ToDuration(),
			DefaultTimeout,
		),
		MaxRequestBytes: firstNonZero(
			method.MaxRequestBytes,
			target.MaxRequestBytes,
			defaults.MaxRequestBytes,
			DefaultMaxRequestBytes,
		),
		MaxResponseBytes: firstNonZero(
			method.MaxResponseBytes,
			target.MaxResponseBytes,
			defaults.MaxResponseBytes,
			DefaultMaxResponseBytes,
		),
	}
}

// firstNonZero returns the first value which is not the zero value of its type.
func firstNonZero[T comparable](values ...T) T {
	var zero T
	for _, value := range values {
		if value != zero {
			return value
		}
	}
	return zero
}
