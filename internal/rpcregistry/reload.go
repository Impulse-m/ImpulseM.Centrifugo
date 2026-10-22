package rpcregistry

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/rs/zerolog/log"
)

// DialFunc makes the connection of a target. Prepare calls it for every target which is new or
// has changed. Nil means no connections are made.
type DialFunc func(target *Target) (io.Closer, error)

// Registry is the RPC method registry. Lookup may run concurrently with a reload. A reload is a
// prepare followed by an apply or a discard, and reloads run one at a time: reloadMu is held from
// the prepare until the apply or the discard, whether the reload comes from a signal or from the
// file poller.
type Registry struct {
	current atomic.Pointer[table]
	dial    DialFunc
	// admission is the admission of the calls, nil when its keys are off. It is built once, by the first Prepare, so
	// its budget and watermark are read at start, as the other keys of the dedup.
	admission     atomic.Pointer[Admission]
	admissionInit atomic.Bool
	// reloadMu serializes reloads. The fields below it are read and written only while it is held.
	reloadMu sync.Mutex
	// cfg is the registry config the running table was built from.
	cfg configtypes.RPCRegistry
	// source identifies the content of the registry file the running table was built from. It is
	// zero when the table does not come from a file.
	source fileSource
	// signalReloads and pollReloads count the reloads by trigger. Nil when the metrics are not initialised.
	signalReloads *metrics.RPCRegistryReloadMetrics
	pollReloads   *metrics.RPCRegistryReloadMetrics
}

// New returns an empty registry. Prepare fills it.
func New() *Registry {
	r := &Registry{
		signalReloads: metrics.NewRPCRegistryReloadMetrics(metrics.RPCRegistryReloadTriggerSignal),
		pollReloads:   metrics.NewRPCRegistryReloadMetrics(metrics.RPCRegistryReloadTriggerPoll),
	}
	r.current.Store(&table{
		methods: map[string]*Method{},
		targets: map[string]*Target{},
	})
	return r
}

// Prepare builds the table for cfg without changing the registry, and returns the function which
// publishes it. Apply cannot fail. A failed Prepare leaves the registry as it was. A disabled
// registry prepares an empty table, so applying it removes all methods. The returned function must
// be called, it releases the reload lock.
func (r *Registry) Prepare(cfg configtypes.RPCRegistry) (func(), error) {
	r.reloadMu.Lock()
	p, err := r.prepare(cfg, nil)
	if err != nil {
		r.reloadMu.Unlock()
		return nil, err
	}
	r.initAdmission(cfg)
	apply, _ := r.stage(p, nil)
	return apply, nil
}

// initAdmission builds the admission from cfg, on the first call only. A nil admission is kept when both its keys are
// off, so the admission path of such a registry is one nil check.
func (r *Registry) initAdmission(cfg configtypes.RPCRegistry) {
	if r.admissionInit.Swap(true) {
		return
	}
	r.admission.Store(NewAdmission(cfg, metrics.NewRPCRegistryAdmissionMetrics()))
}

// warnAdmissionKeys logs a reload which changes the admission keys. The admission is built once, at start, so the
// values in effect are kept until a restart, as the dedup keys are, and the rest of the reload applies. Before the
// first Prepare there is no admission, and nothing is logged.
func (r *Registry) warnAdmissionKeys(cfg configtypes.RPCRegistry) {
	if !r.admissionInit.Load() {
		return
	}
	var inEffectInflight int64
	var inEffectWatermark int64
	if admission := r.admission.Load(); admission != nil {
		inEffectInflight = admission.maxInflight
		inEffectWatermark = admission.watermark
	}
	if cfg.MaxInflightBytes == inEffectInflight && cfg.MemoryWatermarkBytes == inEffectWatermark {
		return
	}
	log.Warn().
		Int64("in_effect_max_inflight_bytes", inEffectInflight).
		Int64("in_effect_memory_watermark_bytes", inEffectWatermark).
		Int64("reloaded_max_inflight_bytes", cfg.MaxInflightBytes).
		Int64("reloaded_memory_watermark_bytes", cfg.MemoryWatermarkBytes).
		Msg("rpc registry: max_inflight_bytes and memory_watermark_bytes are read at start, the values in effect are kept until restart")
}

// Admission returns the admission of the calls, or nil when both its keys are off. A nil registry has none.
func (r *Registry) Admission() *Admission {
	if r == nil {
		return nil
	}
	return r.admission.Load()
}

// PrepareReload is Prepare which also returns discard, for a reload on SIGHUP. Discard closes the
// connections this prepare dialed, for a reload which fails after Prepare. Apply and discard each
// release the reload lock, and only the first call of either one has an effect. Until one of them
// runs, no other reload runs, so the caller must call one of them.
func (r *Registry) PrepareReload(cfg configtypes.RPCRegistry) (func(), func(), error) {
	r.reloadMu.Lock()
	p, err := r.prepare(cfg, nil)
	if err != nil {
		r.reloadMu.Unlock()
		r.signalReloads.Inc(false)
		return nil, nil, err
	}
	apply, discard := r.stage(p, r.signalReloads)
	return apply, discard, nil
}

// contentError marks an error of the registry entries themselves: the file or the inline config does
// not decode or validate. The same content fails the same way every time, unlike a dial error, so the
// poller does not retry the content which caused it.
type contentError struct {
	err error
}

// Error returns the message of the wrapped error.
func (e *contentError) Error() string {
	return e.err.Error()
}

// Unwrap returns the wrapped error.
func (e *contentError) Unwrap() error {
	return e.err
}

// isContentError reports whether err is, or wraps, a contentError.
func isContentError(err error) bool {
	var content *contentError
	return errors.As(err, &content)
}

// prepared is a table built by prepare, with what publishing it changes.
type prepared struct {
	cfg     configtypes.RPCRegistry
	next    *table
	source  fileSource
	removed []*Target
	created []*Target
	drain   time.Duration
}

// prepare builds the table for cfg. The caller holds reloadMu. A registry file is read into snap,
// or read here when snap is nil.
func (r *Registry) prepare(cfg configtypes.RPCRegistry, snap *fileSnapshot) (*prepared, error) {
	r.warnAdmissionKeys(cfg)
	var file configtypes.RPCRegistryFile
	var source fileSource
	switch {
	case cfg.Enabled && cfg.File != "":
		if snap == nil {
			loaded, err := snapshotFile(cfg.File, 0)
			if err != nil {
				return nil, err
			}
			snap = loaded
		}
		resolved, err := config.ResolveRPCRegistryContent(cfg, snap.data)
		if err != nil {
			return nil, &contentError{err: err}
		}
		file = resolved
		source = snap.fileSource
	case cfg.Enabled:
		resolved, err := config.ResolveRPCRegistry(cfg)
		if err != nil {
			return nil, &contentError{err: err}
		}
		file = resolved
	}

	old := r.current.Load()
	next, removed, created, err := r.buildTable(file, old)
	if err != nil {
		return nil, err
	}
	next.enabled = cfg.Enabled
	lists := make([][]string, 0, len(next.targets))
	for _, target := range next.targets {
		lists = append(lists, target.Config.EmulatedHeaders)
	}
	next.emulatedHeaders = config.EmulatedHeaderSet(lists...)
	return &prepared{
		cfg:     cfg,
		next:    next,
		source:  source,
		removed: removed,
		created: created,
		drain:   cfg.DrainTimeout.ToDuration(),
	}, nil
}

// stage returns the functions which publish p, and which drop it. The caller holds reloadMu. Apply
// publishes p, discard closes the connections p dialed, and each of them releases reloadMu. Only the
// first call of either one has an effect. reloads counts the outcome, nil counts nothing.
func (r *Registry) stage(p *prepared, reloads *metrics.RPCRegistryReloadMetrics) (func(), func()) {
	var settled atomic.Bool
	apply := func() {
		if !settled.CompareAndSwap(false, true) {
			return
		}
		old := r.current.Load()
		r.cfg = p.cfg
		r.source = p.source
		r.current.Store(p.next)
		r.retire(p.removed, p.drain)
		dropRemovedSeries(old, p.next)
		reloads.Inc(true)
		r.reloadMu.Unlock()
	}
	discard := func() {
		if !settled.CompareAndSwap(false, true) {
			return
		}
		closeTargets(p.created)
		reloads.Inc(false)
		r.reloadMu.Unlock()
	}
	return apply, discard
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
		target := &Target{
			Config:  normalized,
			Metrics: newTargetMetrics(def.Name),
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
	built := make([]string, 0, len(file.Methods))
	for _, def := range file.Methods {
		target := next.targets[def.Target]
		authClassName := def.AuthClass
		if authClassName == "" {
			authClassName = defaults.AuthClass
		}
		authClass, ok := ParseAuthClass(authClassName)
		if !ok {
			return fail(&contentError{err: fmt.Errorf("rpc registry method %q: unknown auth class %q", def.Method, authClassName)})
		}
		idempotent := defaults.Idempotent
		if def.Idempotent != nil {
			idempotent = *def.Idempotent
		}
		limits := configtypes.ResolveRPCMethodLimits(def, target.Config, defaults)
		next.methods[def.Method] = &Method{
			Name:             def.Method,
			GRPCMethod:       def.GRPCMethod,
			Target:           target,
			Metrics:          metrics.NewRPCRegistryMethodMetrics(def.Method),
			AuthClass:        authClass,
			Timeout:          limits.Timeout,
			MaxRequestBytes:  limits.MaxRequestBytes,
			MaxResponseBytes: limits.MaxResponseBytes,
			ResponseSizeHint: resolveResponseSizeHint(def.ResponseSizeHint),
			Idempotent:       idempotent,
			Headers:          sortedCopy(def.Headers),
			ErrorMetadata:    sortedCopy(def.ErrorMetadata),
		}
		built = append(built, def.Method)
	}
	metrics.ReviveRPCRegistryMethodSeries(built...)

	var removed []*Target
	for name, prev := range old.targets {
		if next.targets[name] != prev {
			removed = append(removed, prev)
		}
	}
	return next, removed, created, nil
}

// dropRemovedSeries deletes the metric series of the targets and methods which the new table drops.
// A name the new table keeps keeps its series, also when its target is replaced. Old is nil for the
// first table, which has nothing to drop.
func dropRemovedSeries(old *table, next *table) {
	if old == nil {
		return
	}
	for name := range old.targets {
		if _, ok := next.targets[name]; !ok {
			metrics.DropRPCRegistryTargetSeries(name)
		}
	}
	var removed []string
	for name := range old.methods {
		if _, ok := next.methods[name]; !ok {
			removed = append(removed, name)
		}
	}
	metrics.DropRPCRegistryMethodSeries(removed...)
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
