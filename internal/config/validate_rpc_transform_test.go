package config

import (
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestValidateRPCRegistryTransformAppFields(t *testing.T) {
	longType := strings.Repeat("A", 65)
	tests := []struct {
		name    string
		rule    configtypes.RPCGRPCStatusToCodeTransform
		wantErr string
	}{
		{
			name: "valid rule with both fields",
			rule: configtypes.RPCGRPCStatusToCodeTransform{
				Status: "UNAVAILABLE",
				ToError: configtypes.RPCTransformError{
					TransformError: configtypes.TransformError{Code: 100},
					AppCode:        503,
					AppType:        "BACKEND_UNAVAILABLE",
				},
			},
		},
		{
			name: "type must match the pattern",
			rule: configtypes.RPCGRPCStatusToCodeTransform{
				Status: "UNAVAILABLE",
				ToError: configtypes.RPCTransformError{
					TransformError: configtypes.TransformError{Code: 100},
					AppType:        "Backend down",
				},
			},
			wantErr: `grpc_status_to_code_transforms[0]: to_error.app_type must match ^[A-Z0-9_]{1,64}$, got "Backend down"`,
		},
		{
			name: "type is at most 64 characters",
			rule: configtypes.RPCGRPCStatusToCodeTransform{
				Status: "UNAVAILABLE",
				ToError: configtypes.RPCTransformError{
					TransformError: configtypes.TransformError{Code: 100},
					AppType:        longType,
				},
			},
			wantErr: `grpc_status_to_code_transforms[0]: to_error.app_type must match ^[A-Z0-9_]{1,64}$`,
		},
		{
			name: "app code needs an error code",
			rule: configtypes.RPCGRPCStatusToCodeTransform{
				Status:       "UNAVAILABLE",
				ToDisconnect: configtypes.TransformDisconnect{Code: 3000},
				ToError:      configtypes.RPCTransformError{AppCode: 503},
			},
			wantErr: `grpc_status_to_code_transforms[0]: to_error.app_code and to_error.app_type need to_error.code`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRPCRegistryTransforms(configtypes.RPCGRPCStatusToCodeTransforms{tt.rule})
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestRPCTransformAppFieldsDecode(t *testing.T) {
	jsonContent := `{"rpc": {"registry": {"grpc_status_to_code_transforms": [{"status": "UNAVAILABLE", "to_error": {"code": 100, "message": "backend unavailable", "temporary": true, "app_code": 503, "app_type": "BACKEND_UNAVAILABLE"}}]}}}`
	yamlContent := `rpc:
  registry:
    grpc_status_to_code_transforms:
      - status: UNAVAILABLE
        to_error:
          code: 100
          message: backend unavailable
          temporary: true
          app_code: 503
          app_type: BACKEND_UNAVAILABLE
`
	for _, tc := range []struct {
		name    string
		file    string
		content string
	}{
		{name: "json", file: "config.json", content: jsonContent},
		{name: "yaml", file: "config.yaml", content: yamlContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeRegistryFile(t, tc.file, tc.content)
			conf, meta, err := GetConfig(nil, path)
			require.NoError(t, err)
			require.Empty(t, meta.UnknownKeys)

			transforms := conf.RPC.Registry.GRPCStatusToCodeTransforms
			require.Len(t, transforms, 1)
			toError := transforms[0].ToError
			require.Equal(t, uint32(100), toError.Code)
			require.Equal(t, "backend unavailable", toError.Message)
			require.True(t, toError.Temporary)
			require.Equal(t, int32(503), toError.AppCode)
			require.Equal(t, "BACKEND_UNAVAILABLE", toError.AppType)
		})
	}
}
