package config

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// testAffinityHeader is the affinity header of the ring_hash cases.
const testAffinityHeader = "x-example-affinity"

// validRingHashRegistry returns the inline registry with its target on ring_hash, and a method which forwards the header.
func validRingHashRegistry() configtypes.RPCRegistry {
	reg := validInlineRegistry()
	reg.Targets[0].LoadBalancing = configtypes.RPCLoadBalancingRingHash
	reg.Targets[0].AffinityHeader = testAffinityHeader
	reg.Methods[0].Headers = []string{testAffinityHeader}
	return reg
}

func TestValidateRPCRegistryLimits(t *testing.T) {
	testCases := []struct {
		name    string
		modify  func(cfg *Config)
		wantErr string
	}{
		{
			name:   "defaults pass",
			modify: func(cfg *Config) {},
		},
		{
			name: "negative in-flight budget is refused",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.MaxInflightBytes = -1
			},
			wantErr: "rpc.registry.max_inflight_bytes: must not be negative",
		},
		{
			name: "negative memory watermark is refused",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.MemoryWatermarkBytes = -1
			},
			wantErr: "rpc.registry.memory_watermark_bytes: must not be negative",
		},
		{
			name: "method cost above the in-flight budget can never run",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.MaxInflightBytes = 1000
			},
			wantErr: `method "users.getUser": cost 1114112 bytes exceeds rpc.registry.max_inflight_bytes (1000): the method can never run`,
		},
		{
			name: "in-flight budget above the cost of every method passes",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.MaxInflightBytes = 2 << 20
				cfg.RPC.Registry.MemoryWatermarkBytes = 1 << 30
			},
		},
		{
			name: "ring_hash without affinity_header is refused",
			modify: func(cfg *Config) {
				cfg.RPC.Registry = validRingHashRegistry()
				cfg.RPC.Registry.Targets[0].AffinityHeader = ""
			},
			wantErr: `rpc registry target "users": ring_hash needs affinity_header`,
		},
		{
			name: "affinity_header without ring_hash is refused",
			modify: func(cfg *Config) {
				cfg.RPC.Registry = validRingHashRegistry()
				cfg.RPC.Registry.Targets[0].LoadBalancing = configtypes.RPCLoadBalancingRoundRobin
			},
			wantErr: `rpc registry target "users": affinity_header requires load_balancing: ring_hash`,
		},
		{
			name: "affinity_header must be lower-case",
			modify: func(cfg *Config) {
				cfg.RPC.Registry = validRingHashRegistry()
				cfg.RPC.Registry.Targets[0].AffinityHeader = "X-Example"
			},
			wantErr: `rpc registry target "users": affinity_header "X-Example" must match`,
		},
		{
			name: "reserved affinity_header is refused",
			modify: func(cfg *Config) {
				cfg.RPC.Registry = validRingHashRegistry()
				cfg.RPC.Registry.Targets[0].AffinityHeader = "authorization"
				cfg.RPC.Registry.Methods[0].Headers = []string{"authorization"}
			},
			wantErr: `rpc registry method "users.getUser": headers: header "authorization" is reserved`,
		},
		{
			name: "binary affinity_header is refused",
			modify: func(cfg *Config) {
				cfg.RPC.Registry = validRingHashRegistry()
				cfg.RPC.Registry.Targets[0].AffinityHeader = "x-example-bin"
				cfg.RPC.Registry.Methods[0].Headers = []string{"x-example-bin"}
			},
			wantErr: `rpc registry target "users": affinity_header "x-example-bin" is reserved, or ends with -bin`,
		},
		{
			name: "affinity_header which no method forwards is refused",
			modify: func(cfg *Config) {
				cfg.RPC.Registry = validRingHashRegistry()
				cfg.RPC.Registry.Methods[0].Headers = nil
			},
			wantErr: `rpc registry target "users": no method forwards affinity_header "x-example-affinity"`,
		},
		{
			name: "valid ring_hash passes",
			modify: func(cfg *Config) {
				cfg.RPC.Registry = validRingHashRegistry()
			},
		},
		{
			name: "response size hint field must be a valid field number",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Methods[0].ResponseSizeHint = &configtypes.RPCResponseSizeHint{RequestField: 0}
			},
			wantErr: `rpc registry method "users.getUser": response_size_hint.request_field must be from 1 to 536870911, got 0`,
		},
		{
			name: "response size hint field above the largest field number is refused",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Methods[0].ResponseSizeHint = &configtypes.RPCResponseSizeHint{RequestField: 536870912}
			},
			wantErr: "response_size_hint.request_field must be from 1 to 536870911, got 536870912",
		},
		{
			name: "negative response size hint overhead is refused",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Methods[0].ResponseSizeHint = &configtypes.RPCResponseSizeHint{RequestField: 3, OverheadBytes: -1}
			},
			wantErr: "response_size_hint.overhead_bytes must not be negative, got -1",
		},
		{
			name: "response size hint overhead at the response limit is refused",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Methods[0].MaxResponseBytes = 1000
				cfg.RPC.Registry.Methods[0].ResponseSizeHint = &configtypes.RPCResponseSizeHint{RequestField: 3, OverheadBytes: 1000}
			},
			wantErr: "response_size_hint.overhead_bytes 1000 must be less than max_response_bytes 1000",
		},
		{
			name: "default response size hint overhead is checked against the response limit",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Methods[0].MaxResponseBytes = 512
				cfg.RPC.Registry.Methods[0].ResponseSizeHint = &configtypes.RPCResponseSizeHint{RequestField: 3}
			},
			wantErr: "response_size_hint.overhead_bytes 1024 must be less than max_response_bytes 512",
		},
		{
			name: "valid response size hint passes",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Methods[0].ResponseSizeHint = &configtypes.RPCResponseSizeHint{RequestField: 3, OverheadBytes: 1024}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := configWithRegistry(validInlineRegistry())
			tc.modify(&cfg)
			err := validateRPCRegistry(cfg)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestValidateRPCRingHashEnvDisabled(t *testing.T) {
	cfg := configWithRegistry(validRingHashRegistry())
	t.Setenv(rpcRingHashSetRequestHashKeyEnv, "FALSE")
	err := validateRPCRegistry(cfg)
	require.ErrorContains(t, err, rpcRingHashSetRequestHashKeyEnv+" is false")

	t.Setenv(rpcRingHashSetRequestHashKeyEnv, "true")
	require.NoError(t, validateRPCRegistry(cfg))
}

func TestValidateRPCRingHashEnvIgnoredWithoutRingHash(t *testing.T) {
	cfg := configWithRegistry(validInlineRegistry())
	t.Setenv(rpcRingHashSetRequestHashKeyEnv, "false")
	require.NoError(t, validateRPCRegistry(cfg))
}

func TestValidateRPCRegistryLimitsDisabledRegistryIsNotChecked(t *testing.T) {
	cfg := configWithRegistry(validRingHashRegistry())
	cfg.RPC.Registry.Enabled = false
	cfg.RPC.Registry.MaxInflightBytes = -1
	cfg.RPC.Registry.Targets[0].AffinityHeader = ""
	require.NoError(t, validateRPCRegistry(cfg))
}

func TestRPCRegistryLimitWarnings(t *testing.T) {
	testCases := []struct {
		name         string
		modify       func(cfg *Config)
		wantWarnings []string
	}{
		{
			name:   "defaults give no warning",
			modify: func(cfg *Config) {},
		},
		{
			name: "websocket limit under a method request limit warns",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Methods[0].MaxRequestBytes = 131072
			},
			wantWarnings: []string{`method "users.getUser": max_request_bytes 131072 exceeds websocket.message_size_limit (65536)`},
		},
		{
			name: "disabled websocket names the webtransport limit only",
			modify: func(cfg *Config) {
				cfg.WebSocket.Disabled = true
				cfg.WebTransport.Enabled = true
				cfg.RPC.Registry.Methods[0].MaxRequestBytes = 131072
			},
			wantWarnings: []string{`exceeds webtransport.message_size_limit (65536)`},
		},
		{
			name: "http stream names the emulation limit once",
			modify: func(cfg *Config) {
				cfg.WebSocket.Disabled = true
				cfg.HTTPStream.Enabled = true
				cfg.RPC.Registry.Methods[0].MaxRequestBytes = 131072
			},
			wantWarnings: []string{`exceeds emulation.max_request_body_size (65536)`},
		},
		{
			name: "sse names the emulation limit once",
			modify: func(cfg *Config) {
				cfg.WebSocket.Disabled = true
				cfg.SSE.Enabled = true
				cfg.RPC.Registry.Methods[0].MaxRequestBytes = 131072
			},
			wantWarnings: []string{`exceeds emulation.max_request_body_size (65536)`},
		},
		{
			name: "http stream and sse together name the emulation limit once",
			modify: func(cfg *Config) {
				cfg.WebSocket.Disabled = true
				cfg.HTTPStream.Enabled = true
				cfg.SSE.Enabled = true
				cfg.RPC.Registry.Methods[0].MaxRequestBytes = 131072
			},
			wantWarnings: []string{`exceeds emulation.max_request_body_size (65536)`},
		},
		{
			name: "queue_max_size under the response limit warns for each method",
			modify: func(cfg *Config) {
				cfg.Client.QueueMaxSize = 1000
			},
			wantWarnings: []string{
				`method "users.getUser": max_response_bytes 1048576 exceeds client.queue_max_size (1000)`,
				`method "help.getConfig": max_response_bytes 1048576 exceeds client.queue_max_size (1000)`,
			},
		},
		{
			name: "queue_max_size zero gives no queue warning",
			modify: func(cfg *Config) {
				cfg.Client.QueueMaxSize = 0
			},
		},
		{
			name: "limits equal to the bounds give nothing",
			modify: func(cfg *Config) {
				cfg.WebSocket.MessageSizeLimit = 131072
				cfg.Client.QueueMaxSize = 1048576
				cfg.RPC.Registry.Methods[0].MaxRequestBytes = 131072
				cfg.RPC.Registry.Methods[0].MaxResponseBytes = 1048576
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := configWithRegistry(validInlineRegistry())
			tc.modify(&cfg)
			file, err := ResolveRPCRegistry(cfg.RPC.Registry)
			require.NoError(t, err)
			warnings := rpcRegistryLimitWarnings(cfg, file)
			require.Len(t, warnings, len(tc.wantWarnings))
			for i, want := range tc.wantWarnings {
				require.Contains(t, warnings[i], want)
			}
		})
	}
}
