// Package rpcregistry holds the RPC method registry: the table which maps client RPC method
// names to gRPC methods of backend targets, and the reload which replaces the table.
package rpcregistry

import (
	"io"
	"slices"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

// AuthClass is the auth class a connection must have to call a registry method.
// The zero value is not a valid class.
type AuthClass uint8

const (
	AuthClassAnonymous AuthClass = iota + 1
	AuthClassPending2FA
	AuthClassAuthenticated
	AuthClassBot
)

// Admits reports whether a connection of class conn may call a method which requires the class a.
// The zero value admits nothing.
func (a AuthClass) Admits(conn AuthClass) bool {
	switch a {
	case AuthClassAnonymous:
		return true
	case AuthClassPending2FA:
		return conn == AuthClassPending2FA || conn == AuthClassAuthenticated || conn == AuthClassBot
	case AuthClassAuthenticated:
		return conn == AuthClassAuthenticated || conn == AuthClassBot
	case AuthClassBot:
		return conn == AuthClassBot
	}
	return false
}

// ConnectionAuthClass returns the auth class of a connection from the claim configured by
// rpc.registry.auth_class_claim and from the user of the connection. An empty claim gives
// anonymous without a user and authenticated with one. It reports false when the claim value
// does not agree with the user, or is unknown: such a connection must be rejected.
func ConnectionAuthClass(claim string, userID string) (AuthClass, bool) {
	if claim == "" {
		if userID == "" {
			return AuthClassAnonymous, true
		}
		return AuthClassAuthenticated, true
	}
	class, ok := ParseAuthClass(claim)
	if !ok {
		return 0, false
	}
	var agrees bool
	switch class {
	case AuthClassPending2FA:
		agrees = true
	case AuthClassAnonymous:
		agrees = userID == ""
	default:
		agrees = userID != ""
	}
	if !agrees {
		return 0, false
	}
	return class, true
}

// ParseAuthClass converts an auth class name from the config. It reports false for unknown names.
func ParseAuthClass(name string) (AuthClass, bool) {
	for _, entry := range authClassNames {
		if entry.name == name {
			return entry.class, true
		}
	}
	return 0, false
}

// Target is a gRPC backend. Config is its normalized config, and it never changes after build.
// A reload reuses a target whose normalized config is equal to the new one.
type Target struct {
	Config configtypes.RPCTarget
	// Metrics are the proxy metric children of the target, nil when the metrics are not initialised.
	Metrics *TargetMetrics
	// conn is the connection to the target, made by the dial hook. Nil when no hook is set.
	conn io.Closer
}

// Method is a registry entry with all defaults applied. It is immutable after build. A call
// keeps the Method it looked up, so an in-flight call is not affected by a reload.
type Method struct {
	Name       string
	GRPCMethod string
	Target     *Target
	// Metrics are the calls_total children of the method, nil when the metrics are not initialised.
	Metrics   *metrics.RPCRegistryMethodMetrics
	AuthClass AuthClass
	Timeout   time.Duration
	// MaxRequestBytes and MaxResponseBytes are the size limits of the call.
	MaxRequestBytes  int
	MaxResponseBytes int
	// ResponseSizeHint bounds the reply by a size the request names, nil when the method has none. Its overhead is
	// never zero: the default is applied at build.
	ResponseSizeHint *configtypes.RPCResponseSizeHint
	Idempotent       bool
	// Headers are per-call client headers forwarded to the target, sorted and lower-case.
	Headers []string
	// ErrorMetadata are backend trailer keys copied into the protocol error, sorted and lower-case.
	ErrorMetadata []string
}

// Conn returns the connection made for the target by the dial hook, or nil when there is none.
func (t *Target) Conn() io.Closer {
	return t.conn
}

// table is one complete registry. It is built once and published with an atomic swap.
type table struct {
	methods map[string]*Method
	targets map[string]*Target
	// enabled is the enabled flag of the config the table was built from.
	enabled bool
	// emulatedHeaders is the lower-case union of the emulated_headers names of all targets. It is empty when no
	// target forwards connect-time headers.
	emulatedHeaders map[string]struct{}
}

// Lookup returns the method registered under name, or nil. It does a single map read and does not allocate.
func (r *Registry) Lookup(name string) *Method {
	t := r.current.Load()
	if t == nil {
		return nil
	}
	return t.methods[name]
}

// Enabled reports whether the current table comes from an enabled config. A nil registry is disabled.
func (r *Registry) Enabled() bool {
	if r == nil {
		return false
	}
	t := r.current.Load()
	return t != nil && t.enabled
}

// UsesEmulatedHeaders reports whether a target of the enabled registry forwards connect-time
// headers, so the connection must keep them. A nil registry does not.
func (r *Registry) UsesEmulatedHeaders() bool {
	if r == nil {
		return false
	}
	t := r.current.Load()
	return t != nil && t.enabled && len(t.emulatedHeaders) > 0
}

// EmulatedHeaderAllowed reports whether the lower-case header name is listed in the emulated_headers
// of a target of the enabled registry. A nil registry does not list any name.
func (r *Registry) EmulatedHeaderAllowed(name string) bool {
	if r == nil {
		return false
	}
	t := r.current.Load()
	if t == nil || !t.enabled {
		return false
	}
	_, ok := t.emulatedHeaders[name]
	return ok
}

// normalizeTarget returns def with the defaults applied and the headers sorted. Two targets with
// equal normalized configs are interchangeable, which is what a reload compares.
func normalizeTarget(def configtypes.RPCTarget) configtypes.RPCTarget {
	normalized := def
	if normalized.LoadBalancing == "" {
		normalized.LoadBalancing = configtypes.RPCLoadBalancingRoundRobin
	}
	normalized.EmulatedHeaders = sortedCopy(def.EmulatedHeaders)
	return normalized
}

// sortedCopy returns a sorted copy of names, so the caller's slice is not changed.
func sortedCopy(names []string) []string {
	out := slices.Clone(names)
	slices.Sort(out)
	return out
}
