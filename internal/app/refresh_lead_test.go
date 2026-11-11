package app

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestCentrifugeNodeConfigRefreshLead(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Client.RefreshLead = configtypes.Duration(60 * time.Second)
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	require.Equal(t, 60*time.Second, testNodeConfig(cfgContainer).ClientRefreshLead)

	cfgContainer, err = config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	require.Zero(t, testNodeConfig(cfgContainer).ClientRefreshLead)
}
