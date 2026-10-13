package rpcregistry

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// Start returns a registry with cfg applied. It is the startup form of New, Prepare and apply.
func Start(cfg configtypes.RPCRegistry) (*Registry, error) {
	r := New()
	apply, err := r.Prepare(cfg)
	if err != nil {
		return nil, err
	}
	apply()
	return r, nil
}
