package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRebindExpiredBoundary checks the boundary second of an expiration, on the pure rule and on the clock.
func TestRebindExpiredBoundary(t *testing.T) {
	tests := []struct {
		name string
		exp  int64
		now  int64
		want bool
	}{
		{name: "negative is refused", exp: -1, now: 100, want: true},
		{name: "zero is disarmed", exp: 0, now: 100, want: false},
		{name: "expired before now", exp: 99, now: 100, want: true},
		{name: "expired at now", exp: 100, now: 100, want: true},
		{name: "live one second after now", exp: 101, now: 100, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tokenExpiredAt(tt.exp, tt.now))
		})
	}
}
