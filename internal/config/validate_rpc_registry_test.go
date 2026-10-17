package config

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func validInlineRegistry() configtypes.RPCRegistry {
	return configtypes.RPCRegistry{
		Enabled: true,
		Defaults: configtypes.RPCMethodDefaults{
			AuthClass: configtypes.RPCAuthClassAuthenticated,
			Timeout:   configtypes.Duration(5 * time.Second),
		},
		Targets: []configtypes.RPCTarget{
			{
				Name:            "users",
				Endpoint:        "dns:///users.example.svc.cluster.local:9000",
				EmulatedHeaders: []string{"x-example-init-connection-bin"},
			},
		},
		Methods: []configtypes.RPCMethod{
			{
				Method:     "users.getUser",
				GRPCMethod: "/example.v1.UserService/GetUser",
				Target:     "users",
				Headers:    []string{"x-example-export-id"},
			},
			{
				Method:     "help.getConfig",
				GRPCMethod: "/example.v1.HelpService/GetConfig",
				Target:     "users",
				AuthClass:  configtypes.RPCAuthClassAnonymous,
			},
		},
	}
}

func configWithRegistry(reg configtypes.RPCRegistry) Config {
	cfg := DefaultConfig()
	cfg.RPC.Registry = reg
	return cfg
}

func TestValidateRPCRegistryRules(t *testing.T) {
	testCases := []struct {
		name    string
		modify  func(reg *configtypes.RPCRegistry)
		wantErr string
	}{
		{
			name:   "valid",
			modify: func(reg *configtypes.RPCRegistry) {},
		},
		{
			name: "poll interval must not be negative",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.PollInterval = configtypes.Duration(-time.Second)
			},
			wantErr: "rpc.registry.poll_interval: must not be negative",
		},
		{
			name: "pending max lifetime must not be negative",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.PendingMaxLifetime = configtypes.Duration(-time.Second)
			},
			wantErr: "rpc.registry.pending_2fa_max_lifetime: must not be negative",
		},
		{
			name: "method name must match pattern",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].Method = "bad method!"
			},
			wantErr: `method "bad method!": method: must match`,
		},
		{
			name: "method names must be unique",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[1].Method = "users.getUser"
			},
			wantErr: `method "users.getUser": method: duplicate method name`,
		},
		{
			name: "grpc method must be a full path",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].GRPCMethod = "example.v1.UserService/GetUser"
			},
			wantErr: `method "users.getUser": grpc_method: must match`,
		},
		{
			name: "method target must exist",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].Target = "missing"
			},
			wantErr: `method "users.getUser": target: unknown target "missing"`,
		},
		{
			name: "method auth class must be known",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].AuthClass = "root"
			},
			wantErr: `method "users.getUser": auth_class: unknown auth class "root"`,
		},
		{
			name: "defaults auth class must be known",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Defaults.AuthClass = "root"
			},
			wantErr: `rpc registry: defaults: auth_class: unknown auth class "root"`,
		},
		{
			name: "method must state class when defaults auth class is empty",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Defaults.AuthClass = ""
			},
			wantErr: `method "users.getUser": auth_class: must be set`,
		},
		{
			name: "method must state class when defaults auth class is anonymous",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Defaults.AuthClass = configtypes.RPCAuthClassAnonymous
			},
			wantErr: `method "users.getUser": auth_class: must be set`,
		},
		{
			name: "method inherits class from non-anonymous defaults",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.AuthClassClaim = "auth_class"
				reg.Defaults.AuthClass = configtypes.RPCAuthClassPending2FA
				reg.Methods[0].AuthClass = ""
			},
		},
		{
			name: "pending_2fa method is allowed when auth class claim is set",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.AuthClassClaim = "auth_class"
				reg.Methods[0].AuthClass = configtypes.RPCAuthClassPending2FA
			},
		},
		{
			name: "bot method is allowed when auth class claim is set",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.AuthClassClaim = "auth_class"
				reg.Methods[0].AuthClass = configtypes.RPCAuthClassBot
			},
		},
		{
			name: "auth class claim must match pattern",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.AuthClassClaim = "auth-class"
			},
			wantErr: `rpc.registry.auth_class_claim: invalid claim auth-class, must match`,
		},
		{
			name: "pending_2fa method needs auth class claim",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].AuthClass = configtypes.RPCAuthClassPending2FA
			},
			wantErr: `method "users.getUser": auth_class: "pending_2fa" requires rpc.registry.auth_class_claim to be set`,
		},
		{
			name: "bot method needs auth class claim",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].AuthClass = configtypes.RPCAuthClassBot
			},
			wantErr: `method "users.getUser": auth_class: "bot" requires rpc.registry.auth_class_claim to be set`,
		},
		{
			name: "bot defaults need auth class claim",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Defaults.AuthClass = configtypes.RPCAuthClassBot
			},
			wantErr: `rpc registry: defaults: auth_class: "bot" requires rpc.registry.auth_class_claim to be set`,
		},
		{
			name: "negative method timeout",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].Timeout = configtypes.Duration(-time.Second)
			},
			wantErr: `method "users.getUser": timeout: must not be negative`,
		},
		{
			name: "negative method max request bytes",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].MaxRequestBytes = -1
			},
			wantErr: `method "users.getUser": max_request_bytes: must not be negative`,
		},
		{
			name: "negative method max response bytes",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].MaxResponseBytes = -1
			},
			wantErr: `method "users.getUser": max_response_bytes: must not be negative`,
		},
		{
			name: "negative default timeout",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Defaults.Timeout = configtypes.Duration(-time.Second)
			},
			wantErr: `rpc registry: defaults: timeout: must not be negative`,
		},
		{
			name: "negative target max response bytes",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Targets[0].MaxResponseBytes = -1
			},
			wantErr: `target "users": max_response_bytes: must not be negative`,
		},
		{
			name: "load balancing must be known",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Targets[0].LoadBalancing = "random"
			},
			wantErr: `target "users": load_balancing: unknown policy "random"`,
		},
		{
			name: "method header must be lower-case",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].Headers = []string{"X-Example-Export-Id"}
			},
			wantErr: `method "users.getUser": headers: name "X-Example-Export-Id" must be lower-case`,
		},
		{
			name: "method header authorization is reserved",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].Headers = []string{"authorization"}
			},
			wantErr: `method "users.getUser": headers: header "authorization" is reserved`,
		},
		{
			name: "method header traceparent is reserved",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].Headers = []string{"traceparent"}
			},
			wantErr: `headers: header "traceparent" is reserved`,
		},
		{
			name: "method header x-forwarded-for is reserved",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].Headers = []string{"x-forwarded-for"}
			},
			wantErr: `headers: header "x-forwarded-for" is reserved`,
		},
		{
			name: "method header with centrifugo prefix is reserved",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].Headers = []string{"x-centrifugo-client"}
			},
			wantErr: `headers: header "x-centrifugo-client" is reserved`,
		},
		{
			name: "emulated header with centrifugo prefix is reserved",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Targets[0].EmulatedHeaders = []string{"x-centrifugo-msg-id"}
			},
			wantErr: `target "users": emulated_headers: header "x-centrifugo-msg-id" is reserved`,
		},
		{
			name: "emulated header must be lower-case",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Targets[0].EmulatedHeaders = []string{"X-Example-Init"}
			},
			wantErr: `target "users": emulated_headers: name "X-Example-Init" must be lower-case`,
		},
		{
			name: "error metadata must be lower-case",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Methods[0].ErrorMetadata = []string{"Example-Token"}
			},
			wantErr: `method "users.getUser": error_metadata: name "Example-Token" must be lower-case`,
		},
		{
			name: "target name must be set",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Targets[0].Name = ""
			},
			wantErr: `rpc registry targets[0]: name: must be set`,
		},
		{
			name: "target endpoint must be set",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Targets[0].Endpoint = ""
			},
			wantErr: `target "users": endpoint: must be set`,
		},
		{
			name: "target names must be unique",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.Targets = append(reg.Targets, reg.Targets[0])
			},
			wantErr: `target "users": name: duplicate target name`,
		},
		{
			name: "grpc status must be known",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.GRPCStatusToCodeTransforms = configtypes.RPCGRPCStatusToCodeTransforms{
					{Status: "NOPE", ToError: configtypes.TransformError{Code: 100}},
				}
			},
			wantErr: `grpc_status_to_code_transforms[0]: unknown gRPC status "NOPE"`,
		},
		{
			name: "grpc status transform needs a code",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.GRPCStatusToCodeTransforms = configtypes.RPCGRPCStatusToCodeTransforms{
					{Status: "UNAUTHENTICATED"},
				}
			},
			wantErr: `no error or disconnect code set in grpc_status_to_code_transforms[0]`,
		},
		{
			name: "grpc status transform takes error or disconnect, not both",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.GRPCStatusToCodeTransforms = configtypes.RPCGRPCStatusToCodeTransforms{
					{
						Status:       "UNAUTHENTICATED",
						ToError:      configtypes.TransformError{Code: 101},
						ToDisconnect: configtypes.TransformDisconnect{Code: 3000},
					},
				}
			},
			wantErr: `only error or disconnect code can be set in grpc_status_to_code_transforms[0], but not both`,
		},
		{
			name: "enabled registry needs entries",
			modify: func(reg *configtypes.RPCRegistry) {
				*reg = configtypes.RPCRegistry{Enabled: true}
			},
			wantErr: `rpc registry: enabled registry needs a file, or inline targets and methods`,
		},
		{
			name: "file cannot be combined with inline entries",
			modify: func(reg *configtypes.RPCRegistry) {
				reg.File = "testdata/rpc_registry.yaml"
			},
			wantErr: `rpc registry: file cannot be combined with inline defaults, targets or methods`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reg := validInlineRegistry()
			tc.modify(&reg)
			err := configWithRegistry(reg).Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestValidateRPCRegistryDisabledIsNotChecked(t *testing.T) {
	reg := validInlineRegistry()
	reg.Enabled = false
	reg.Methods[0].Target = "missing"
	require.NoError(t, configWithRegistry(reg).Validate())
}

func TestValidateRPCRegistryChecksFileWhenEnabled(t *testing.T) {
	reg := configtypes.RPCRegistry{Enabled: true, File: "testdata/rpc_registry.yaml", AuthClassClaim: "auth_class"}
	require.NoError(t, configWithRegistry(reg).Validate())
}

func TestValidateRPCRegistryFileVersion(t *testing.T) {
	path := writeRegistryFile(t, "registry.yaml", "version: 2\ntargets: []\nmethods: []\n")
	reg := configtypes.RPCRegistry{Enabled: true, File: path}
	err := configWithRegistry(reg).Validate()
	require.ErrorContains(t, err, "rpc registry: version must be 1, got 2")
}

func TestValidateRPCRegistryFileUnknownKeyFailsValidate(t *testing.T) {
	path := writeRegistryFile(t, "registry.yaml", "version: 1\nmethodz: []\n")
	reg := configtypes.RPCRegistry{Enabled: true, File: path}
	err := configWithRegistry(reg).Validate()
	require.ErrorContains(t, err, "methodz")
}

func TestValidateRPCRegistryFileNamesMethodOnError(t *testing.T) {
	path := writeRegistryFile(t, "registry.yaml", `version: 1
defaults:
  auth_class: authenticated
targets:
  - name: users
    endpoint: dns:///users:9000
methods:
  - method: users.getUser
    grpc_method: /example.v1.UserService/GetUser
    target: users
  - method: users.getOrder
    grpc_method: /example.v1.OrderService/GetOrder
    target: orders
`)
	reg := configtypes.RPCRegistry{Enabled: true, File: path}
	err := configWithRegistry(reg).Validate()
	require.ErrorContains(t, err, `method "users.getOrder": target: unknown target "orders"`)
}
