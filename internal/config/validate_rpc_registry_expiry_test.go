package config

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func TestValidateRPCRegistryRefuseExpiredTokens(t *testing.T) {
	const insecureErr = "rpc.registry.refuse_expired_tokens: can't be used with client.insecure"
	const proxyErr = "rpc.registry.refuse_expired_tokens: can't be used with client.proxy.refresh.enabled"
	const leadErr = "rpc.registry.refuse_expired_tokens: needs client.refresh_lead above zero, so that the refresh lands before the token expires"

	newCfg := func(enabled bool, switchOn bool) Config {
		reg := validInlineRegistry()
		reg.Enabled = enabled
		reg.RefuseExpiredTokens = switchOn
		return configWithRegistry(reg)
	}

	cfg := newCfg(true, true)
	cfg.Client.Insecure = true
	cfg.Client.RefreshLead = configtypes.Duration(60 * time.Second)
	require.EqualError(t, validateRPCRegistry(cfg), insecureErr)

	cfg = newCfg(true, true)
	cfg.Client.Proxy.Refresh.Enabled = true
	cfg.Client.RefreshLead = configtypes.Duration(60 * time.Second)
	require.EqualError(t, validateRPCRegistry(cfg), proxyErr)

	cfg = newCfg(true, true)
	require.EqualError(t, validateRPCRegistry(cfg), leadErr)

	cfg = newCfg(true, true)
	cfg.Client.RefreshLead = configtypes.Duration(60 * time.Second)
	require.NoError(t, validateRPCRegistry(cfg))

	cfg = newCfg(false, true)
	cfg.Client.Insecure = true
	require.NoError(t, validateRPCRegistry(cfg))

	cfg = newCfg(true, false)
	cfg.Client.Insecure = true
	require.NoError(t, validateRPCRegistry(cfg))
}

func TestRPCRegistryTokenExpiredDefaults(t *testing.T) {
	path := writeConfigFile(t, "config.json", "{}")
	conf, _, err := GetConfig(nil, path)
	require.NoError(t, err)
	require.Equal(t, configtypes.RPCRegistryError{
		Code:    centrifuge.ErrorTokenExpired.Code,
		Message: centrifuge.ErrorTokenExpired.Message,
	}, conf.RPC.Registry.Errors.TokenExpired)
	require.Equal(t, uint32(109), conf.RPC.Registry.Errors.TokenExpired.Code)
	require.Equal(t, "token expired", conf.RPC.Registry.Errors.TokenExpired.Message)
}
