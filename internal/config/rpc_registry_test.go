package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func writeRegistryFile(t *testing.T, name string, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestLoadRPCRegistryFileJSONAndYAMLAreEquivalent(t *testing.T) {
	fromJSON, err := LoadRPCRegistryFile("testdata/rpc_registry.json")
	require.NoError(t, err)
	fromYAML, err := LoadRPCRegistryFile("testdata/rpc_registry.yaml")
	require.NoError(t, err)

	require.Equal(t, fromJSON, fromYAML)
	require.Equal(t, 1, fromJSON.Version)
	require.Len(t, fromJSON.Targets, 2)
	require.Len(t, fromJSON.Methods, 6)
	require.Equal(t, 3*time.Second, fromJSON.Targets[0].Timeout.ToDuration())
	require.Equal(t, "centrifugo", fromJSON.Targets[0].GRPC.StaticMetadata["x-example-edge"])
	require.NotNil(t, fromJSON.Methods[0].Idempotent)
	require.True(t, *fromJSON.Methods[0].Idempotent)
	require.Nil(t, fromJSON.Methods[2].Idempotent)
}

func TestLoadRPCRegistryFileRejectsUnknownKeys(t *testing.T) {
	path := writeRegistryFile(t, "registry.json", `{
		"version": 1,
		"methodz": [],
		"targets": [{"name": "users", "endpoint": "dns:///users:9000", "gpc": {}}]
	}`)

	_, err := LoadRPCRegistryFile(path)
	require.ErrorContains(t, err, "unknown keys in rpc registry file")
	require.ErrorContains(t, err, "methodz")
	require.ErrorContains(t, err, "gpc")
}

func TestLoadRPCRegistryFileRejectsUnsupportedExtension(t *testing.T) {
	path := writeRegistryFile(t, "registry.txt", `{"version": 1}`)

	_, err := LoadRPCRegistryFile(path)
	require.ErrorContains(t, err, "extension must be .json, .yaml or .yml")
}

func TestLoadRPCRegistryFileMissing(t *testing.T) {
	_, err := LoadRPCRegistryFile(filepath.Join(t.TempDir(), "missing.yaml"))
	require.ErrorContains(t, err, "error reading rpc registry file")
}

func TestResolveRPCRegistryRejectsFileWithInlineEntries(t *testing.T) {
	reg := configtypes.RPCRegistry{
		File:    "testdata/rpc_registry.yaml",
		Targets: []configtypes.RPCTarget{{Name: "users"}},
	}

	_, err := ResolveRPCRegistry(reg)
	require.ErrorContains(t, err, "file cannot be combined with inline defaults, targets or methods")
}

func TestResolveRPCRegistryRequiresFileOrInlineEntries(t *testing.T) {
	_, err := ResolveRPCRegistry(configtypes.RPCRegistry{Enabled: true})
	require.ErrorContains(t, err, "needs a file, or inline targets and methods")
}

func TestResolveRPCRegistryFromFile(t *testing.T) {
	file, err := ResolveRPCRegistry(configtypes.RPCRegistry{Enabled: true, File: "testdata/rpc_registry.yaml"})
	require.NoError(t, err)
	require.Equal(t, 1, file.Version)
	require.Len(t, file.Methods, 6)
}

func TestResolveRPCRegistryInlineGetsVersion(t *testing.T) {
	reg := configtypes.RPCRegistry{
		Defaults: configtypes.RPCMethodDefaults{AuthClass: configtypes.RPCAuthClassAuthenticated},
		Targets:  []configtypes.RPCTarget{{Name: "users", Endpoint: "dns:///users:9000"}},
		Methods: []configtypes.RPCMethod{
			{Method: "users.getUser", GRPCMethod: "/example.v1.UserService/GetUser", Target: "users"},
		},
	}

	file, err := ResolveRPCRegistry(reg)
	require.NoError(t, err)
	require.Equal(t, 1, file.Version)
	require.Len(t, file.Methods, 1)
}

func TestRPCRegistryErrorDefaults(t *testing.T) {
	cfg := DefaultConfig()
	errs := cfg.RPC.Registry.Errors

	require.False(t, cfg.RPC.Registry.Enabled)
	require.Empty(t, cfg.RPC.Registry.Defaults.AuthClass)
	require.Equal(t, uint32(104), errs.UnknownMethod.Code)
	require.Equal(t, "method not found", errs.UnknownMethod.Message)
	require.Equal(t, uint32(103), errs.AuthClass.Code)
	require.Equal(t, "permission denied", errs.AuthClass.Message)
	require.Equal(t, uint32(106), errs.RequestTooLarge.Code)
	require.Equal(t, "limit exceeded", errs.RequestTooLarge.Message)
	require.Equal(t, uint32(107), errs.Cancelled.Code)
	require.Equal(t, "bad request", errs.Cancelled.Message)
	require.Zero(t, errs.UnknownMethod.AppCode)
	require.Empty(t, errs.UnknownMethod.AppType)
	require.Equal(t, "app-code", cfg.RPC.Registry.AppError.CodeKey)
	require.Equal(t, "app-type", cfg.RPC.Registry.AppError.TypeKey)
	require.Equal(t, 30*time.Second, cfg.RPC.Registry.DrainTimeout.ToDuration())
}
