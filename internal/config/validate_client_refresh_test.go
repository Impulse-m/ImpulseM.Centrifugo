package config

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestValidateClientRefreshLead(t *testing.T) {
	tests := []struct {
		name    string
		lead    time.Duration
		wantErr string
	}{
		{name: "zero", lead: 0},
		{name: "whole seconds", lead: 60 * time.Second},
		{name: "fraction", lead: 1500 * time.Millisecond, wantErr: "client.refresh_lead: must be zero or a whole number of seconds, got 1.5s"},
		{name: "negative", lead: -time.Second, wantErr: "client.refresh_lead: must be zero or a whole number of seconds, got -1s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Client.RefreshLead = configtypes.Duration(tt.lead)
			err := validateClientRefreshLead(cfg)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestValidateClientRefreshLeadRunsWithoutRegistry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RPC.Registry.Enabled = false
	cfg.Client.RefreshLead = configtypes.Duration(-time.Second)
	require.ErrorContains(t, validateRPCRegistry(cfg), "client.refresh_lead: must be zero or a whole number of seconds")
}
