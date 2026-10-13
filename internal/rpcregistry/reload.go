package rpcregistry

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// DialFunc makes the connection of a target. Prepare calls it for every target which is new or
// has changed. Nil means no connections are made.
type DialFunc func(target *Target) (io.Closer, error)

// retiredTarget is a target which is no longer in the table. Its connection is closed after closeAt.
type retiredTarget struct {
	target  *Target
	closeAt time.Time
}

// Registry is the RPC method registry. Lookup may run concurrently with a reload. Prepare and
// apply must not run concurrently with each other: reloads run one at a time.
type Registry struct {
	current atomic.Pointer[table]
	dial    DialFunc

	retireMu sync.Mutex
	retired  []retiredTarget
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
	var file configtypes.RPCRegistryFile
	if cfg.Enabled {
		resolved, err := config.ResolveRPCRegistry(cfg)
		if err != nil {
			return nil, err
		}
		file = resolved
	}

	old := r.current.Load()
	next, removed, err := r.buildTable(file, old)
	if err != nil {
		return nil, err
	}
	drain := cfg.DrainTimeout.ToDuration()
	return func() {
		r.current.Store(next)
		r.retire(removed, drain)
	}, nil
}

// buildTable builds the table for file. A target whose normalized config is unchanged since old is
// reused, with its connection. Targets of old which are not reused are returned as removed.
// On error, connections dialed so far are closed.
func (r *Registry) buildTable(file configtypes.RPCRegistryFile, old *table) (*table, []*Target, error) {
	next := &table{
		methods: make(map[string]*Method, len(file.Methods)),
		targets: make(map[string]*Target, len(file.Targets)),
	}
	var created []*Target
	fail := func(err error) (*table, []*Target, error) {
		for _, target := range created {
			if target.conn != nil {
				_ = target.conn.Close()
			}
		}
		return nil, nil, err
	}

	for _, def := range file.Targets {
		hash, err := targetHash(def)
		if err != nil {
			return fail(fmt.Errorf("rpc registry target %q: %w", def.Name, err))
		}
		prev, exists := old.targets[def.Name]
		if exists && prev.hash == hash {
			next.targets[def.Name] = prev
			continue
		}
		target := &Target{
			Name:             def.Name,
			Endpoint:         def.Endpoint,
			LoadBalancing:    loadBalancingOrDefault(def.LoadBalancing),
			EmulatedHeaders:  sortedCopy(def.EmulatedHeaders),
			GRPC:             def.GRPC,
			Timeout:          def.Timeout.ToDuration(),
			MaxRequestBytes:  def.MaxRequestBytes,
			MaxResponseBytes: def.MaxResponseBytes,
			hash:             hash,
		}
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
				target.Timeout,
				defaults.Timeout.ToDuration(),
				DefaultTimeout,
			),
			MaxRequestBytes: firstNonZero(
				def.MaxRequestBytes,
				target.MaxRequestBytes,
				defaults.MaxRequestBytes,
				DefaultMaxRequestBytes,
			),
			MaxResponseBytes: firstNonZero(
				def.MaxResponseBytes,
				target.MaxResponseBytes,
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
	return next, removed, nil
}

// retire records targets which left the table. Their connections are to be closed once the drain
// timeout has passed. Closing is not done yet: it comes with the connection code.
func (r *Registry) retire(removed []*Target, drain time.Duration) {
	if len(removed) == 0 {
		return
	}
	closeAt := time.Now().Add(drain)
	r.retireMu.Lock()
	defer r.retireMu.Unlock()
	for _, target := range removed {
		r.retired = append(r.retired, retiredTarget{target: target, closeAt: closeAt})
	}
}

// retiredTargets returns the targets recorded as removed and not yet closed.
func (r *Registry) retiredTargets() []*Target {
	r.retireMu.Lock()
	defer r.retireMu.Unlock()
	out := make([]*Target, 0, len(r.retired))
	for _, item := range r.retired {
		out = append(out, item.target)
	}
	return out
}
