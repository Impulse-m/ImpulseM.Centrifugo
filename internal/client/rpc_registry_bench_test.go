package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

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

// BenchmarkRPCRegistryCallDedupOn runs b.N tracked calls of a non-idempotent method, each with its own msg_id, so
// each is a miss: one dedup begin and complete, and the connection storage acquired once for the tracking.
func BenchmarkRPCRegistryCallDedupOn(b *testing.B) {
	backend := newRegistryTestBackend(b)
	reg := dedupRegistryConfig()
	registry := startRegistryTestRegistry(b, reg, backend)
	cfg := config.DefaultConfig()
	cfg.Client.ReplyIncludeServerTime = true
	cfg.RPC.Registry = reg
	h, _ := newRegistryTestHandler(b, cfg, registry)
	client := newDedupClient("c1", "42", nil, testInstance)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := dedupCall(h, client, "users.get", dedupMsgID(uint64(i)&(1<<rpcdedup.MsgIDSeqBits-1)), "x"); err != nil {
			b.Fatal(err)
		}
	}
}
