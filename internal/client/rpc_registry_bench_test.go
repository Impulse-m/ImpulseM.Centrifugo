package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/centrifugal/centrifuge"
)

// benchRegistryCall runs b.N registry calls of users.get through OnRPC, each over the bufconn backend.
// setup returns the config which the handler runs with.
func benchRegistryCall(b *testing.B, setup func(cfg *config.Config)) {
	b.Helper()
	backend := newRegistryTestBackend(b)
	registry := startRegistryTestRegistry(b, registryTestRegistryConfig(), backend)
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = registryTestRegistryConfig()
	if setup != nil {
		setup(&cfg)
	}
	h, _ := newRegistryTestHandler(b, cfg, registry)
	client := newRegistryTestClient("c1", "42", nil, nil)
	event := centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := h.OnRPC(client, event, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRPCRegistryCallDedupOff is the registry hot path with dedup off and no msg_id, as before
// exactly-once RPC.
func BenchmarkRPCRegistryCallDedupOff(b *testing.B) {
	benchRegistryCall(b, nil)
}
