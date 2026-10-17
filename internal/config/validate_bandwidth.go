package config

import (
	"errors"
	"math"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// validateBandwidthOptions checks the channel options which reduce bandwidth: keep_latest_publication
// is useful only together with delta compression, and client_publish_debounce_interval is sent to
// clients as a whole number of milliseconds in an uint32 field.
func validateBandwidthOptions(c configtypes.ChannelOptions) error {
	if c.KeepLatestPublication && len(c.AllowedDeltaTypes) == 0 {
		return errors.New("keep_latest_publication requires allowed_delta_types")
	}
	debounce := c.ClientPublishDebounceInterval.ToDuration()
	if debounce < 0 {
		return errors.New("client_publish_debounce_interval must not be negative")
	}
	if debounce > 0 && (debounce < time.Millisecond || debounce%time.Millisecond != 0) {
		return errors.New("client_publish_debounce_interval must be a whole number of milliseconds, at least 1ms")
	}
	if debounce.Milliseconds() > math.MaxUint32 {
		return errors.New("client_publish_debounce_interval is too large")
	}
	return nil
}
