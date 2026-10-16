package backoffpolicy

import (
	"time"

	"github.com/cenkalti/backoff/v5"
)

// randomizationFactor is the share of the current interval by which each delay is randomized either way, so that the
// retries of many calls do not come in step.
const randomizationFactor float64 = 0.2

// multiplier is the growth of the interval between two consecutive delays.
const multiplier float64 = 2

// Exponential returns the retry policy shared by the retries of the fork: the first delay is initialInterval, each
// next delay doubles up to maxInterval, and every delay is randomized by a fifth either way.
func Exponential(initialInterval time.Duration, maxInterval time.Duration) *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = initialInterval
	b.MaxInterval = maxInterval
	b.Multiplier = multiplier
	b.RandomizationFactor = randomizationFactor
	return b
}
