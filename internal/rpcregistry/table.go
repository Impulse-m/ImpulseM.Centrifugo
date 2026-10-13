// Package rpcregistry holds the RPC method registry: the table which maps client RPC method
// names to gRPC methods of backend targets, and the reload which replaces the table.
package rpcregistry

import (
	"crypto/sha256"
	"encoding/json"
	"io"
	"slices"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
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

// Built-in limits. They apply when neither the method, its target nor the defaults set a value.
const (
	DefaultTimeout          = 5 * time.Second
	DefaultMaxRequestBytes  = 64 << 10
	DefaultMaxResponseBytes = 1 << 20
)

// ParseAuthClass converts an auth class name from the config. It reports false for unknown names.
func ParseAuthClass(name string) (AuthClass, bool) {
	switch name {
	case configtypes.RPCAuthClassAnonymous:
		return AuthClassAnonymous, true
	case configtypes.RPCAuthClassPending2FA:
		return AuthClassPending2FA, true
	case configtypes.RPCAuthClassAuthenticated:
		return AuthClassAuthenticated, true
	case configtypes.RPCAuthClassBot:
		return AuthClassBot, true
	}
	return 0, false
}

// Target is a gRPC backend. Its fields are set at build time and never change afterwards.
// A target is reused across reloads while its normalized config is unchanged.
type Target struct {
	Name          string
	Endpoint      string
	LoadBalancing string
	// EmulatedHeaders are the connect-time headers forwarded to the target, sorted.
	EmulatedHeaders []string
	GRPC            configtypes.ProxyCommonGRPC
	// Timeout, MaxRequestBytes and MaxResponseBytes are the target-level values which methods
	// of this target inherit. Zero means not set.
	Timeout          time.Duration
	MaxRequestBytes  int
	MaxResponseBytes int

	hash [sha256.Size]byte
	// conn is the connection to the target, made by the dial hook. Nil when no hook is set.
	conn io.Closer
}

// Method is a registry entry with all defaults applied. It is immutable after build. A call
// keeps the Method it looked up, so an in-flight call is not affected by a reload.
type Method struct {
	Name       string
	GRPCMethod string
	Target     *Target
	AuthClass  AuthClass
	Timeout    time.Duration
	// MaxRequestBytes and MaxResponseBytes are the size limits of the call.
	MaxRequestBytes  int
	MaxResponseBytes int
	Idempotent       bool
	// Headers are per-call client headers forwarded to the target, sorted and lower-case.
	Headers []string
	// ErrorMetadata are backend trailer keys copied into the protocol error, sorted and lower-case.
	ErrorMetadata []string
}

// table is one complete registry. It is built once and published with an atomic swap.
type table struct {
	methods map[string]*Method
	targets map[string]*Target
}

// Lookup returns the method registered under name, or nil. It does a single map read and does not allocate.
func (r *Registry) Lookup(name string) *Method {
	t := r.current.Load()
	if t == nil {
		return nil
	}
	return t.methods[name]
}

// targetHash returns the hash of the normalized config of a target. Defaults applied by
// the config are part of the normalized form, and the order of headers does not matter.
func targetHash(def configtypes.RPCTarget) ([sha256.Size]byte, error) {
	normalized := def
	normalized.LoadBalancing = loadBalancingOrDefault(def.LoadBalancing)
	normalized.EmulatedHeaders = sortedCopy(def.EmulatedHeaders)
	data, err := json.Marshal(normalized)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(data), nil
}

func loadBalancingOrDefault(policy string) string {
	if policy == "" {
		return configtypes.RPCLoadBalancingRoundRobin
	}
	return policy
}

// sortedCopy returns a sorted copy of names, so the caller's slice is not changed.
func sortedCopy(names []string) []string {
	out := slices.Clone(names)
	slices.Sort(out)
	return out
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
