package config

import (
	"errors"
)

// validateRebind rejects rebind together with the refresh proxy. The refresh proxy answers refreshes
// without the token user, so the connection cannot be moved to the identity of the token. With rebind enabled it also
// rejects allow_downgrade with more than one command at a time per connection, as allow_downgrade is read only then.
// It does not close the server API window of a subscription which commits while a downgrade runs, see the known gaps
// of the rpcregistry README.
func validateRebind(cfg Config) error {
	if cfg.Client.Rebind.Enabled && cfg.Client.Proxy.Refresh.Enabled {
		return errors.New("client.rebind.enabled can't be used with client.proxy.refresh.enabled")
	}
	if cfg.Client.Rebind.Enabled && cfg.Client.Rebind.AllowDowngrade && cfg.Client.Concurrency > 1 {
		return errors.New("client.rebind.allow_downgrade can't be used with client.concurrency above 1")
	}
	return nil
}
