package config

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func configWithClientAddress(modify func(ca *configtypes.ClientAddress)) Config {
	cfg := DefaultConfig()
	cfg.ClientAddress.Enabled = true
	cfg.ClientAddress.TrustedProxies = []string{"10.0.0.0/8"}
	modify(&cfg.ClientAddress)
	return cfg
}

func TestClientAddressDefaults(t *testing.T) {
	cfg := DefaultConfig()
	require.False(t, cfg.ClientAddress.Enabled)
	require.Empty(t, cfg.ClientAddress.TrustedProxies)
	require.Equal(t, "x-forwarded-for", cfg.ClientAddress.Header)
	require.False(t, cfg.ClientAddress.ProxyProtocol.Enabled)
	require.Equal(t, "reject", cfg.ClientAddress.ProxyProtocol.UntrustedPeers)
	require.Equal(t, 10*time.Second, cfg.ClientAddress.ProxyProtocol.ReadHeaderTimeout.ToDuration())
}

func TestValidateClientAddress(t *testing.T) {
	testCases := []struct {
		name    string
		modify  func(ca *configtypes.ClientAddress)
		wantErr string
	}{
		{
			name:   "valid",
			modify: func(ca *configtypes.ClientAddress) {},
		},
		{
			name: "valid proxy protocol",
			modify: func(ca *configtypes.ClientAddress) {
				ca.ProxyProtocol.Enabled = true
			},
		},
		{
			name: "bare address and CIDR entries",
			modify: func(ca *configtypes.ClientAddress) {
				ca.TrustedProxies = []string{"192.0.2.7", "2001:db8::/32"}
			},
		},
		{
			name: "bad CIDR",
			modify: func(ca *configtypes.ClientAddress) {
				ca.TrustedProxies = []string{"10.0.0.0/40"}
			},
			wantErr: "client_address: trusted_proxies[0]",
		},
		{
			name: "bad header",
			modify: func(ca *configtypes.ClientAddress) {
				ca.Header = "x bad"
			},
			wantErr: "client_address: invalid header",
		},
		{
			name: "proxy protocol without trusted proxies",
			modify: func(ca *configtypes.ClientAddress) {
				ca.TrustedProxies = nil
				ca.ProxyProtocol.Enabled = true
			},
			wantErr: "requires a non-empty client_address.trusted_proxies",
		},
		{
			name: "unknown untrusted_peers",
			modify: func(ca *configtypes.ClientAddress) {
				ca.ProxyProtocol.Enabled = true
				ca.ProxyProtocol.UntrustedPeers = "allow"
			},
			wantErr: "unknown client_address.proxy_protocol.untrusted_peers",
		},
		{
			name: "zero read header timeout",
			modify: func(ca *configtypes.ClientAddress) {
				ca.ProxyProtocol.Enabled = true
				ca.ProxyProtocol.ReadHeaderTimeout = 0
			},
			wantErr: "read_header_timeout must be greater than zero",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateClientAddress(configWithClientAddress(tc.modify))
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestValidateClientAddressDisabled(t *testing.T) {
	// A disabled section is not checked: a bad list is ignored at runtime too.
	cfg := DefaultConfig()
	cfg.ClientAddress.TrustedProxies = []string{"not-an-ip"}
	require.NoError(t, validateClientAddress(cfg))

	// PROXY protocol needs the section enabled, otherwise it would silently do nothing.
	cfg.ClientAddress.ProxyProtocol.Enabled = true
	require.ErrorContains(t, validateClientAddress(cfg), "requires client_address.enabled")
}
