package config

import (
	"fmt"
	"time"
)

// validateClientRefreshLead checks client.refresh_lead. The ttl of a reply is in whole seconds, so the lead is too:
// a fraction would be cut by the connection, and a negative lead would be ignored.
func validateClientRefreshLead(c Config) error {
	lead := c.Client.RefreshLead.ToDuration()
	if lead < 0 || lead%time.Second != 0 {
		return fmt.Errorf("client.refresh_lead: must be zero or a whole number of seconds, got %s", lead)
	}
	return nil
}
