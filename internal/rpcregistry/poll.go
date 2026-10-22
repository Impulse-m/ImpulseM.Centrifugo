package rpcregistry

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/rs/zerolog/log"
)

// pollInterval returns the interval of the file poll for the running config. It is zero when the
// registry does not poll: it is disabled, it has no file, or poll_interval is not set.
func (r *Registry) pollInterval() time.Duration {
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()
	if !r.cfg.Enabled || r.cfg.File == "" {
		return 0
	}
	interval := r.cfg.PollInterval.ToDuration()
	if interval < 0 {
		return 0
	}
	return interval
}

// maxPollFileSize is the largest registry file a poll reads. A larger file is refused, and the current
// registry stays.
const maxPollFileSize = 16 << 20

// pollStartPhase returns the delay before the first check of a node: a random value within one
// interval. The nodes which share a registry file then do not check, and reload, on the same tick.
func pollStartPhase(interval time.Duration) time.Duration {
	return rand.N(interval) //nolint:gosec // Start phase of a poll, not a secret.
}

// poll checks the registry file every interval until ctx is done. The first check comes after a
// random start phase within one interval.
func (r *Registry) poll(ctx context.Context, interval time.Duration) {
	phase := time.NewTimer(pollStartPhase(interval))
	select {
	case <-ctx.Done():
		phase.Stop()
		return
	case <-phase.C:
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	p := &poller{registry: r}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.check()
		}
	}
}

// poller holds what the file poll remembers between checks. Only the poll goroutine uses it.
type poller struct {
	registry *Registry
	// lastErr is the last failure to stat or read the file which was logged, so a failure which
	// lasts is logged once.
	lastErr string
	// rejected is set when the content with the hash rejectedHash failed to load. That content is
	// not loaded again, so a broken file is reported once.
	rejected     bool
	rejectedHash [sha256.Size]byte
}

// check reloads the registry when the content of its file changed. A changed file is loaded like a
// reload on SIGHUP: it is applied as a whole, or the current registry stays. The file is read before
// reloadMu is taken, so a slow read does not hold up a reload, and the lock is taken again to apply it.
func (p *poller) check() {
	r := p.registry
	r.reloadMu.Lock()
	cfg := r.cfg
	source := r.source
	r.reloadMu.Unlock()
	if !cfg.Enabled || cfg.File == "" {
		return
	}
	path := cfg.File
	info, err := os.Stat(path)
	if err == nil && info.Size() == source.size && info.ModTime().Equal(source.modTime) {
		p.lastErr = ""
		return
	}
	if err == nil && info.Size() > maxPollFileSize {
		p.logFailure(fmt.Errorf("registry file of %d bytes is over the poll limit of %d bytes, keeping the current registry", info.Size(), maxPollFileSize))
		return
	}
	snap, err := snapshotFile(path)
	if err != nil {
		p.logFailure(err)
		return
	}
	r.reloadMu.Lock()
	if !r.cfg.Enabled || r.cfg.File != path {
		r.reloadMu.Unlock()
		return
	}
	cfg = r.cfg
	if snap.hash == r.source.hash {
		// A touch without a change of content. Remember the new stat, so the next check does not read
		// the file again.
		r.source = snap.fileSource
		r.reloadMu.Unlock()
		p.lastErr = ""
		return
	}
	if p.rejected && snap.hash == p.rejectedHash {
		r.reloadMu.Unlock()
		return
	}
	prepared, err := r.prepare(cfg, snap)
	if err != nil {
		r.reloadMu.Unlock()
		r.pollReloads.Inc(false)
		if !isContentError(err) {
			// A dial error, for example a certificate which is being rotated. The content is not
			// latched, so the next tick tries it again.
			p.logFailure(err)
			return
		}
		p.rejected = true
		p.rejectedHash = snap.hash
		p.lastErr = ""
		log.Error().Err(err).Msg("error reloading RPC registry file, keeping the current registry")
		return
	}
	p.rejected = false
	p.lastErr = ""
	apply, _ := r.stage(prepared, r.pollReloads)
	apply()
	log.Info().Str("file", cfg.File).Msg("RPC registry file reloaded")
}

// logFailure logs a failure of the poll which is retried on the next tick, once until the failure
// changes.
func (p *poller) logFailure(err error) {
	if err.Error() == p.lastErr {
		return
	}
	p.lastErr = err.Error()
	log.Error().Err(err).Msg("error reloading RPC registry file, keeping the current registry, retrying")
}
