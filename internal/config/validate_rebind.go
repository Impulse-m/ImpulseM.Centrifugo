package config

import (
	"errors"
)

// validateRebind rejects rebind together with the refresh proxy. The refresh proxy answers refreshes
// without the token user, so the connection cannot be moved to the identity of the token.
func validateRebind(cfg Config) error {
	if cfg.Client.Rebind.Enabled && cfg.Client.Proxy.Refresh.Enabled {
		return errors.New("client.rebind.enabled can't be used with client.proxy.refresh.enabled")
	}
	return nil
}
