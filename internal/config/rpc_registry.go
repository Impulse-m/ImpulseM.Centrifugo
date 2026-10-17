package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/spf13/viper"
)

// setRPCRegistryDefaults sets the defaults of the registry errors. They are set on viper
// rather than with default tags, because the four errors share one struct type and each
// needs its own code and message.
func setRPCRegistryDefaults(v *viper.Viper) {
	const prefix = "rpc.registry.errors."
	v.SetDefault(prefix+"unknown_method.code", 104)
	v.SetDefault(prefix+"unknown_method.message", "method not found")
	v.SetDefault(prefix+"auth_class.code", 103)
	v.SetDefault(prefix+"auth_class.message", "permission denied")
	v.SetDefault(prefix+"request_too_large.code", 106)
	v.SetDefault(prefix+"request_too_large.message", "limit exceeded")
	v.SetDefault(prefix+"cancelled.code", 107)
	v.SetDefault(prefix+"cancelled.message", "bad request")
	v.SetDefault(prefix+"msg_id_invalid.code", 107)
	v.SetDefault(prefix+"msg_id_invalid.message", "invalid msg_id")
	v.SetDefault(prefix+"msg_id_reused.code", 107)
	v.SetDefault(prefix+"msg_id_reused.message", "msg_id reused")
	v.SetDefault(prefix+"result_evicted.code", 110)
	v.SetDefault(prefix+"result_evicted.message", "result expired")
	v.SetDefault(prefix+"dedup_unavailable.code", 108)
	v.SetDefault(prefix+"dedup_unavailable.message", "dedup unavailable")
	v.SetDefault(prefix+"overloaded.code", centrifuge.ErrorTooManyRequests.Code)
	v.SetDefault(prefix+"overloaded.message", centrifuge.ErrorTooManyRequests.Message)
}

// The key and the environment variable of the wakeup switch of the dedup Redis.
const (
	rpcDedupWakeupSwitchPath   = "rpc.registry.dedup.redis.wakeup.enabled"            //nolint:gosec // A config key, not a credential.
	rpcDedupWakeupSwitchVar    = "CENTRIFUGO_RPC_REGISTRY_DEDUP_REDIS_WAKEUP_ENABLED" //nolint:gosec // An environment variable name, not a credential.
	rpcDedupMaxWaiterNodesPath = "rpc.registry.dedup.redis.wakeup.max_waiter_nodes"
	rpcDedupMaxWaiterNodesVar  = "CENTRIFUGO_RPC_REGISTRY_DEDUP_REDIS_WAKEUP_MAX_WAITER_NODES" //nolint:gosec // An environment variable name, not a credential.
	// defaultRPCDedupMaxWaiterNodes is the default of rpc.registry.dedup.redis.wakeup.max_waiter_nodes, which is the
	// default tag of configtypes.RPCDedupWakeup.
	defaultRPCDedupMaxWaiterNodes = 64
)

// fallbackRPCDedupWakeupCap replaces a waiter cap out of range when neither the file nor the environment sets the
// wakeup switch, so the wakeup is on by default only. The wakeup was off by default before, and a cap set for it was
// never checked, so a config which loaded before must keep loading. A cap above the maximum becomes the maximum, and a
// cap below the minimum becomes the default. The replacement is returned as a warning. An explicit switch keeps the
// cap, which is then checked by validation. A cap set by the environment overrides the file, so the file value is not
// read then.
func fallbackRPCDedupWakeupCap(v *viper.Viper, conf *Config) []string {
	if !conf.RPC.Registry.Dedup.Redis.Enabled {
		return nil
	}
	if v.InConfig(rpcDedupWakeupSwitchPath) {
		return nil
	}
	if _, set := os.LookupEnv(rpcDedupWakeupSwitchVar); set {
		return nil
	}
	wakeup := &conf.RPC.Registry.Dedup.Redis.Wakeup
	// A zero from the file is already the default by the default tag of the envconfig pass, so the file value is read
	// from viper to see it.
	offending := wakeup.MaxWaiterNodes
	_, capSetByEnv := os.LookupEnv(rpcDedupMaxWaiterNodesVar)
	if !capSetByEnv && v.InConfig(rpcDedupMaxWaiterNodesPath) && !isRPCDedupMaxWaiterNodesInRange(v.GetInt(rpcDedupMaxWaiterNodesPath)) {
		offending = v.GetInt(rpcDedupMaxWaiterNodesPath)
	}
	if isRPCDedupMaxWaiterNodesInRange(offending) {
		return nil
	}
	replacement := defaultRPCDedupMaxWaiterNodes
	if offending > maxRPCDedupMaxWaiterNodes {
		replacement = maxRPCDedupMaxWaiterNodes
	}
	warning := fmt.Sprintf("rpc.registry.dedup.redis.wakeup.max_waiter_nodes is %d, out of range, and is set to %d, as the wakeup is on by default; set rpc.registry.dedup.redis.wakeup.enabled to choose", offending, replacement)
	wakeup.MaxWaiterNodes = replacement
	return []string{warning}
}

// isRPCDedupMaxWaiterNodesInRange reports whether a waiter cap is within the bounds which validation accepts.
func isRPCDedupMaxWaiterNodesInRange(maxWaiterNodes int) bool {
	return maxWaiterNodes >= minRPCDedupMaxWaiterNodes && maxWaiterNodes <= maxRPCDedupMaxWaiterNodes
}

// LoadRPCRegistryFile reads the RPC registry file at path. The format is chosen by the
// extension: .json, .yaml or .yml. Unknown keys are an error. Entries are not validated
// here, see ResolveRPCRegistry.
func LoadRPCRegistryFile(path string) (configtypes.RPCRegistryFile, error) {
	var file configtypes.RPCRegistryFile
	_, err := rpcRegistryFormat(path)
	if err != nil {
		return file, err
	}
	data, err := os.ReadFile(path) //nolint:gosec // Path of the registry file set by the operator in the config.
	if err != nil {
		return file, fmt.Errorf("error reading rpc registry file %s: %w", path, err)
	}
	return DecodeRPCRegistryFile(path, data)
}

// DecodeRPCRegistryFile decodes data, the content of the RPC registry file at path. The format
// is chosen by the extension of path, as in LoadRPCRegistryFile.
func DecodeRPCRegistryFile(path string, data []byte) (configtypes.RPCRegistryFile, error) {
	var file configtypes.RPCRegistryFile
	format, err := rpcRegistryFormat(path)
	if err != nil {
		return file, err
	}

	v := viper.NewWithOptions(viper.WithDecodeHook(decodeHooks()))
	v.SetConfigType(format)
	err = v.ReadConfig(bytes.NewReader(data))
	if err != nil {
		return file, fmt.Errorf("error reading rpc registry file %s: %w", path, err)
	}
	err = v.Unmarshal(&file)
	if err != nil {
		return file, fmt.Errorf("error unmarshalling rpc registry file %s: %w", path, err)
	}
	unknownKeys := findUnknownKeys(v.AllSettings(), &file, "")
	if len(unknownKeys) > 0 {
		return file, fmt.Errorf("unknown keys in rpc registry file %s: %s", path, strings.Join(unknownKeys, ", "))
	}
	return file, nil
}

// rpcRegistryFormat returns the viper config type of the registry file at path, chosen by its extension.
func rpcRegistryFormat(path string) (string, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		return "json", nil
	case ".yaml", ".yml":
		return "yaml", nil
	}
	return "", fmt.Errorf("rpc registry file %s: extension must be .json, .yaml or .yml", path)
}

// ResolveRPCRegistry returns the registry entries of reg, read from the file or taken from the
// inline fields, and validates them. Inline entries get the only supported format version.
// It is the single source of registry entries for config validation and for the registry itself.
func ResolveRPCRegistry(reg configtypes.RPCRegistry) (configtypes.RPCRegistryFile, error) {
	return resolveRPCRegistry(reg, LoadRPCRegistryFile)
}

// ResolveRPCRegistryContent is ResolveRPCRegistry for a registry file whose content the caller has
// read already. content is the content of reg.File, and it is decoded instead of reading the file
// again, so the entries are the ones the caller holds.
func ResolveRPCRegistryContent(reg configtypes.RPCRegistry, content []byte) (configtypes.RPCRegistryFile, error) {
	return resolveRPCRegistry(reg, func(path string) (configtypes.RPCRegistryFile, error) {
		return DecodeRPCRegistryFile(path, content)
	})
}

// resolveRPCRegistry is ResolveRPCRegistry with the file loaded by loadFile.
func resolveRPCRegistry(
	reg configtypes.RPCRegistry,
	loadFile func(path string) (configtypes.RPCRegistryFile, error),
) (configtypes.RPCRegistryFile, error) {
	var file configtypes.RPCRegistryFile
	inline := reg.Defaults != (configtypes.RPCMethodDefaults{}) || len(reg.Targets) > 0 || len(reg.Methods) > 0
	if reg.File != "" && inline {
		return file, errors.New("rpc registry: file cannot be combined with inline defaults, targets or methods")
	}
	if reg.File != "" {
		loaded, err := loadFile(reg.File)
		if err != nil {
			return file, err
		}
		file = loaded
	} else if inline {
		file = configtypes.RPCRegistryFile{
			Version:  1,
			Defaults: reg.Defaults,
			Targets:  reg.Targets,
			Methods:  reg.Methods,
		}
	} else {
		return file, errors.New("rpc registry: enabled registry needs a file, or inline targets and methods")
	}
	err := validateRPCRegistryFile(file)
	if err != nil {
		return file, err
	}
	err = validateRPCAuthClassClaim(reg.AuthClassClaim, file)
	if err != nil {
		return file, err
	}
	err = validateRPCRegistryLimits(reg, file)
	if err != nil {
		return file, err
	}
	return file, nil
}
