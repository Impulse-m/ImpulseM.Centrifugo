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
		wantErr      bool
	}{
		{name: "rebind off", rebind: false, refreshProxy: true, wantErr: false},
		{name: "rebind on without refresh proxy", rebind: true, refreshProxy: false, wantErr: false},
		{name: "rebind on with refresh proxy", rebind: true, refreshProxy: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Client.Rebind.Enabled = tt.rebind
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
