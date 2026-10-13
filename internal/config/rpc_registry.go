package config

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

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
}

// LoadRPCRegistryFile reads the RPC registry file at path. The format is chosen by the
// extension: .json, .yaml or .yml. Unknown keys are an error. Entries are not validated
// here, see ResolveRPCRegistry.
func LoadRPCRegistryFile(path string) (configtypes.RPCRegistryFile, error) {
	var file configtypes.RPCRegistryFile
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json", ".yaml", ".yml":
	default:
		return file, fmt.Errorf("rpc registry file %s: extension must be .json, .yaml or .yml", path)
	}

	v := viper.NewWithOptions(viper.WithDecodeHook(decodeHooks()))
	v.SetConfigFile(path)
	err := v.ReadInConfig()
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

// ResolveRPCRegistry returns the registry entries of reg, read from the file or taken from the
// inline fields, and validates them. Inline entries get the only supported format version.
// It is the single source of registry entries for config validation and for the registry itself.
func ResolveRPCRegistry(reg configtypes.RPCRegistry) (configtypes.RPCRegistryFile, error) {
	var file configtypes.RPCRegistryFile
	inline := reg.Defaults != (configtypes.RPCMethodDefaults{}) || len(reg.Targets) > 0 || len(reg.Methods) > 0
	if reg.File != "" && inline {
		return file, errors.New("rpc registry: file cannot be combined with inline defaults, targets or methods")
	}
	if reg.File != "" {
		loaded, err := LoadRPCRegistryFile(reg.File)
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
	return file, nil
}
