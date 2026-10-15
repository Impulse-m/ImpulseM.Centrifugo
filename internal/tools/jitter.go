package tools

import (
	"math/rand/v2"
	"time"
)

// HalfJitter returns a random delay in [d/2, d], so that nodes started together do not retry in lockstep. It is
// the backoff of the retries of the Redis connectors and of the schema migrations.
func HalfJitter(d time.Duration) time.Duration {
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1)) //nolint:gosec // Jitter of retries, not a secret.
}
