package client

import (
	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// newRPCDedupStore returns the store of exactly-once RPC, or nil when the dedup is off. The store is
// created once, so a change of the dedup config takes effect after a restart.
func newRPCDedupStore(cfgContainer *config.Container) rpcdedup.Store {
	if cfgContainer == nil {
		return nil
	}
	var dedup configtypes.RPCDedup = cfgContainer.ConfigRef().RPC.Registry.Dedup
	if !dedup.Enabled {
		return nil
	}
	var store *rpcdedup.Memory = rpcdedup.NewMemory(dedup.LocalMaxEntries, dedup.Window.ToDuration(), dedup.MaxReplySize)
	return store
}

// rpcDedupScope returns the dedup scope of a connection. A connection with a token session is scoped to
// the session of its user. Otherwise it is scoped to its install instance, stored at connect, and its user.
// The user is part of both, so an instance seen on one account cannot replay calls of another. A connection
// with neither has no scope, and its calls are not deduplicated.
func rpcDedupScope(c Client, storage map[string]any) string {
	var identity centrifuge.TokenIdentity = c.TokenIdentity()
	var userID string = c.UserID()
	if identity.Session != ([16]byte{}) {
		return rpcdedup.ScopeOf("s", userID, identity.Session[:])
	}
	var instance string
	instance, _ = storage[clientstorage.KeyInstance].(string)
	if instance != "" {
		return rpcdedup.ScopeOf("i", userID, []byte(instance))
	}
	return ""
}

// storeRPCInstance stores the install instance of a connection, when the dedup is on and the instance is
// valid. An invalid instance is dropped with a debug log, and the connection has no install scope.
func (h *Handler) storeRPCInstance(storage map[string]any, instance string) {
	if h.rpcDedup == nil || instance == "" {
		return
	}
	if !rpcdedup.ValidInstance(instance) {
		log.Debug().Msg("dropped invalid connect instance, the connection has no dedup scope from it")
		return
	}
	storage[clientstorage.KeyInstance] = instance
}
