package config

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/go-viper/mapstructure/v2"
)

// decodeHooks returns the viper decode hooks used for every config source: the main
// config file and the RPC registry file. Durations are read from strings like "5s".
func decodeHooks() mapstructure.DecodeHookFunc {
	return mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		configtypes.StringToDurationHookFunc(),
		configtypes.StringToPEMDataHookFunc(),
		configtypes.StringToMapStringStringHookFunc(),
		configtypes.StringToStringKeyValuesHookFunc(),
	)
}
