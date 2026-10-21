package config

import (
	"errors"
)

// validateRefuseExpiredTokens checks rpc.registry.refuse_expired_tokens. The switch needs a token which is renewed by the
// client, and a refresh which lands before the token expires, so a client refresh lead above zero.
func validateRefuseExpiredTokens(c Config) error {
	if !c.RPC.Registry.RefuseExpiredTokens {
		return nil
	}
	if c.Client.Insecure {
		return errors.New("rpc.registry.refuse_expired_tokens: can't be used with client.insecure")
	}
	if c.Client.Proxy.Refresh.Enabled {
		return errors.New("rpc.registry.refuse_expired_tokens: can't be used with client.proxy.refresh.enabled")
	}
	if c.Client.RefreshLead.ToDuration() <= 0 {
		return errors.New("rpc.registry.refuse_expired_tokens: needs client.refresh_lead above zero, so that the refresh lands before the token expires")
	}
	return nil
}
