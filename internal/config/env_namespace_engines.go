package config

import (
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/config/envconfig"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// processNamespaceEnginesEnv applies CENTRIFUGO_BROKERS_<NAME>_* and
// CENTRIFUGO_PRESENCE_MANAGERS_<NAME>_* environment variables to the named
// broker and presence manager entries, and registers the variables as known.
func processNamespaceEnginesEnv(conf *Config, knownEnvVars map[string]envconfig.VarInfo) error {
	err := processEntriesEnv(
		"CENTRIFUGO_BROKERS_", "brokers", conf.Brokers,
		func(entry *configtypes.NamedBroker) string { return entry.Name }, knownEnvVars,
	)
	if err != nil {
		return err
	}
	return processEntriesEnv(
		"CENTRIFUGO_PRESENCE_MANAGERS_", "presence managers", conf.PresenceManagers,
		func(entry *configtypes.NamedPresenceManager) string { return entry.Name }, knownEnvVars,
	)
}

// processEntriesEnv applies the environment variables named envPrefix followed by the entry name, in the env form,
// to each entry of a named list. kind names the list in the errors.
func processEntriesEnv[T any](
	envPrefix string,
	kind string,
	entries []T,
	nameOf func(entry *T) string,
	knownEnvVars map[string]envconfig.VarInfo,
) error {
	for i := range entries {
		varInfo, err := envconfig.Process(envPrefix+configtypes.NameForEnv(nameOf(&entries[i])), &entries[i])
		if err != nil {
			return fmt.Errorf("error processing env %s: %w", kind, err)
		}
		extendKnownEnvVars(knownEnvVars, varInfo)
	}
	return nil
}
