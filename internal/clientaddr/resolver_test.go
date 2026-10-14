package clientaddr

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestNewRejectsBadConfig(t *testing.T) {
	testCases := []struct {
		name string
		cfg  configtypes.ClientAddress
	}{
		{name: "bad CIDR prefix length", cfg: enabledConfig("10.0.0.0/33")},
		{name: "not an address", cfg: enabledConfig("not-an-ip")},
		{name: "IPv4-mapped range too wide", cfg: enabledConfig("::ffff:10.0.0.0/64")},
		{name: "empty header", cfg: configtypes.ClientAddress{Enabled: true}},
		{name: "header with a space", cfg: configtypes.ClientAddress{Enabled: true, Header: "x bad"}},
		{name: "header with a colon", cfg: configtypes.ClientAddress{Enabled: true, Header: "x:bad"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.cfg)
			require.Error(t, err)
		})
	}
}

func TestNewNormalizesRanges(t *testing.T) {
	r := mustResolver(t, enabledConfig("10.1.2.3/8", "192.0.2.7", "::ffff:198.51.100.0/120", "2001:db8::1"))
	require.Equal(t, []string{"10.0.0.0/8", "192.0.2.7/32", "198.51.100.0/24", "2001:db8::1/128"}, r.ranges)
}

func TestNewLowerCasesHeader(t *testing.T) {
	cfg := enabledConfig()
	cfg.Header = "X-Real-IP"
	r := mustResolver(t, cfg)
	require.Equal(t, "x-real-ip", r.header)
}
