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
	for i, item := range conf.Brokers {
		varInfo, err := envconfig.Process("CENTRIFUGO_BROKERS_"+configtypes.NameForEnv(item.Name), &item)
		if err != nil {
			return fmt.Errorf("error processing env brokers: %w", err)
		}
		conf.Brokers[i] = item
		extendKnownEnvVars(knownEnvVars, varInfo)
	}

	for i, item := range conf.PresenceManagers {
		varInfo, err := envconfig.Process("CENTRIFUGO_PRESENCE_MANAGERS_"+configtypes.NameForEnv(item.Name), &item)
		if err != nil {
			return fmt.Errorf("error processing env presence managers: %w", err)
		}
		conf.PresenceManagers[i] = item
		extendKnownEnvVars(knownEnvVars, varInfo)
	}
	return nil
}
