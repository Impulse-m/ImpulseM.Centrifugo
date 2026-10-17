package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateRebind(t *testing.T) {
	tests := []struct {
		name         string
		rebind       bool
		refreshProxy bool
		downgrade    bool
		concurrency  int
		wantErr      bool
	}{
		{name: "rebind off", rebind: false, refreshProxy: true, wantErr: false},
		{name: "rebind on without refresh proxy", rebind: true, refreshProxy: false, wantErr: false},
		{name: "rebind on with refresh proxy", rebind: true, refreshProxy: true, wantErr: true},
		{name: "downgrade with sequential commands", rebind: true, downgrade: true, concurrency: 0, wantErr: false},
		{name: "downgrade with one command at a time", rebind: true, downgrade: true, concurrency: 1, wantErr: false},
		{name: "downgrade with concurrency", rebind: true, downgrade: true, concurrency: 2, wantErr: true},
		{name: "downgrade off with concurrency", rebind: true, downgrade: false, concurrency: 4, wantErr: false},
		{name: "downgrade with concurrency and rebind off", rebind: false, downgrade: true, concurrency: 4, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Client.Rebind.Enabled = tt.rebind
			cfg.Client.Rebind.AllowDowngrade = tt.downgrade
			cfg.Client.Concurrency = tt.concurrency
			cfg.Client.Proxy.Refresh.Enabled = tt.refreshProxy
			err := validateRebind(cfg)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
