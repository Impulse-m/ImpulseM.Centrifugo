package rpcregistry

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// Start returns a registry with cfg applied. It is the startup form of New, Prepare and apply.
// dial makes the connections of targets, for this registry and for every later reload.
func Start(cfg configtypes.RPCRegistry, dial DialFunc) (*Registry, error) {
	r := New()
	r.dial = dial
	apply, err := r.Prepare(cfg)
	if err != nil {
		return nil, err
	}
	apply()
	return r, nil
}
