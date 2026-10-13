package rpcregistry

import (
	"fmt"
	"io"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// DialFunc makes the connection of a target. Prepare calls it for every target which is new or
// has changed. Nil means no connections are made.
type DialFunc func(target *Target) (io.Closer, error)

// Registry is the RPC method registry. Lookup may run concurrently with a reload. Prepare and
// apply must not run concurrently with each other: reloads run one at a time.
type Registry struct {
	current atomic.Pointer[table]
	dial    DialFunc
}

// New returns an empty registry. Prepare fills it.
func New() *Registry {
	r := &Registry{}
	r.current.Store(&table{
		methods: map[string]*Method{},
		targets: map[string]*Target{},
	})
	return r
}

// Prepare builds the table for cfg without changing the registry, and returns the function which
// publishes it. Apply cannot fail. A failed Prepare leaves the registry as it was. A disabled
// registry prepares an empty table, so applying it removes all methods.
func (r *Registry) Prepare(cfg configtypes.RPCRegistry) (func(), error) {
	apply, _, err := r.PrepareReload(cfg)
	return apply, err
}

// PrepareReload is Prepare which also returns discard. Discard closes the connections this
// prepare dialed, for a reload which fails after Prepare. It does nothing once apply ran, and
// apply does nothing once discard ran.
func (r *Registry) PrepareReload(cfg configtypes.RPCRegistry) (func(), func(), error) {
	var file configtypes.RPCRegistryFile
	if cfg.Enabled {
		resolved, err := config.ResolveRPCRegistry(cfg)
		if err != nil {
			return nil, nil, err
		}
		file = resolved
	}

	old := r.current.Load()
	next, removed, created, err := r.buildTable(file, old)
	if err != nil {
		return nil, nil, err
	}
	next.enabled = cfg.Enabled
	for _, target := range next.targets {
		if len(target.Config.EmulatedHeaders) > 0 {
			next.usesEmulatedHeaders = true
		}
	}
	drain := cfg.DrainTimeout.ToDuration()
	var settled atomic.Bool
	apply := func() {
		if !settled.CompareAndSwap(false, true) {
			return
		}
		r.current.Store(next)
		r.retire(removed, drain)
	}
	discard := func() {
		if !settled.CompareAndSwap(false, true) {
			return
		}
		closeTargets(created)
	}
	return apply, discard, nil
}

// closeTargets closes the connections of targets which have one.
func closeTargets(targets []*Target) {
	for _, target := range targets {
		if target.conn != nil {
			_ = target.conn.Close()
		}
	}
}

// buildTable builds the table for file. A target whose normalized config is unchanged since old is
// reused, with its connection. Targets of old which are not reused are returned as removed, and
// targets dialed by this call as created. On error, connections dialed so far are closed.
func (r *Registry) buildTable(file configtypes.RPCRegistryFile, old *table) (*table, []*Target, []*Target, error) {
	next := &table{
		methods: make(map[string]*Method, len(file.Methods)),
		targets: make(map[string]*Target, len(file.Targets)),
	}
	var created []*Target
	fail := func(err error) (*table, []*Target, []*Target, error) {
		closeTargets(created)
		return nil, nil, nil, err
	}

	for _, def := range file.Targets {
		normalized := normalizeTarget(def)
		prev, exists := old.targets[def.Name]
		if exists && reflect.DeepEqual(prev.Config, normalized) {
			next.targets[def.Name] = prev
			continue
		}
		target := &Target{Config: normalized}
		if r.dial != nil {
			conn, err := r.dial(target)
			if err != nil {
				return fail(fmt.Errorf("rpc registry target %q: %w", def.Name, err))
			}
			target.conn = conn
		}
		created = append(created, target)
		next.targets[def.Name] = target
	}

	defaults := file.Defaults
	for _, def := range file.Methods {
		target := next.targets[def.Target]
		authClassName := def.AuthClass
		if authClassName == "" {
			authClassName = defaults.AuthClass
		}
		authClass, ok := ParseAuthClass(authClassName)
		if !ok {
			return fail(fmt.Errorf("rpc registry method %q: unknown auth class %q", def.Method, authClassName))
		}
		idempotent := defaults.Idempotent
		if def.Idempotent != nil {
			idempotent = *def.Idempotent
		}
		next.methods[def.Method] = &Method{
			Name:       def.Method,
			GRPCMethod: def.GRPCMethod,
			Target:     target,
			AuthClass:  authClass,
			Timeout: firstNonZero(
				def.Timeout.ToDuration(),
				target.Config.Timeout.ToDuration(),
				defaults.Timeout.ToDuration(),
				DefaultTimeout,
			),
			MaxRequestBytes: firstNonZero(
				def.MaxRequestBytes,
				target.Config.MaxRequestBytes,
				defaults.MaxRequestBytes,
				DefaultMaxRequestBytes,
			),
			MaxResponseBytes: firstNonZero(
				def.MaxResponseBytes,
				target.Config.MaxResponseBytes,
				defaults.MaxResponseBytes,
				DefaultMaxResponseBytes,
			),
			Idempotent:    idempotent,
			Headers:       sortedCopy(def.Headers),
			ErrorMetadata: sortedCopy(def.ErrorMetadata),
		}
	}

	var removed []*Target
	for name, prev := range old.targets {
		if next.targets[name] != prev {
			removed = append(removed, prev)
		}
	}
	return next, removed, created, nil
}

// retire closes the connections of targets which left the table once the drain timeout has
// passed, so calls which still hold the old method can finish.
func (r *Registry) retire(removed []*Target, drain time.Duration) {
	for _, target := range removed {
		time.AfterFunc(drain, func() {
			closeTargets([]*Target{target})
		})
	}
}
