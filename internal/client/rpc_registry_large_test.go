package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

// Sizes of the admission tests. A users.get call of data "slow" costs its 4 bytes and the 64 KiB response bound of
// its method, and a hinted blob.read of 4 KiB costs its request and 4 KiB plus 1 KiB of overhead.
const (
	testAdmissionHeldCost  int64  = 4 + 64<<10
	testAdmissionSmallRead int64  = 6000
	testAdmissionHintField uint32 = 3
)

// admissionBlobCost is the admission cost of blob.read without its hint: its request and response limits. The
// config validation refuses a budget under the cost of any method, so the budgets of the tests are at least this.
const admissionBlobCost int64 = 4096 + 64<<10

// admissionRegistryConfig returns the registry config of the admission tests, with a node budget of budget bytes. It
// adds a method blob.read whose reply is bounded by a response size hint on field 3.
func admissionRegistryConfig(budget int64) configtypes.RPCRegistry {
	reg := registryTestRegistryConfig()
	reg.Defaults.Timeout = configtypes.Duration(10 * time.Second)
	reg.MaxInflightBytes = budget
	reg.Errors.Overloaded = configtypes.RPCRegistryError{Code: 111, Message: "too many requests"}
	reg.Methods = append(reg.Methods, configtypes.RPCMethod{
		Method:           "blob.read",
		GRPCMethod:       "/example.v1.BlobService/Read",
		Target:           "backend",
		AuthClass:        configtypes.RPCAuthClassAnonymous,
		MaxRequestBytes:  4096,
		MaxResponseBytes: 64 << 10,
		ResponseSizeHint: &configtypes.RPCResponseSizeHint{RequestField: int(testAdmissionHintField), OverheadBytes: 1024},
	})
	return reg
}

// testHintedRequest returns a request whose field 3 holds hint, and whose field 1 holds payload.
func testHintedRequest(hint uint64, payload []byte) []byte {
	data := protowire.AppendTag(nil, protowire.Number(testAdmissionHintField), protowire.VarintType)
	data = protowire.AppendVarint(data, hint)
	data = protowire.AppendTag(data, 1, protowire.BytesType)
	return protowire.AppendBytes(data, payload)
}

// startHeldCall starts a users.get call which the backend holds until ctx is done, and returns when the backend has
// received it, so the budget it charges is in flight. The call ends with an error when ctx is cancelled.
func startHeldCall(t *testing.T, h *Handler, backend *tools.GRPCBytesBackend, ctx context.Context) <-chan error {
	t.Helper()
	held := newRegistryTestClient("c1", "42", ctx, nil)
	done := make(chan error, 1)
	go func() {
		_, err := h.OnRPC(held, centrifuge.RPCEvent{Method: "users.get", Data: []byte("slow")}, nil)
		done <- err
	}()
	require.Eventually(t, func() bool { return len(backend.Calls()) == 1 }, 5*time.Second, time.Millisecond)
	return done
}

func TestRegistryAdmissionRefusesWhenBudgetIsHeld(t *testing.T) {
	backend := newRegistryTestBackend(t)
	reg := admissionRegistryConfig(testAdmissionHeldCost + testAdmissionSmallRead)
	registry := startRegistryTestRegistry(t, reg, backend)
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = reg
	h, _ := newRegistryTestHandler(t, cfg, registry)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startHeldCall(t, h, backend, ctx)

	client := newRegistryTestClient("c2", "42", nil, nil)
	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	var protoErr *centrifuge.Error
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(111), protoErr.Code)
	require.True(t, protoErr.Temporary)
	require.Equal(t, rpcRetryLater, protoErr.Metadata[rpcRetryMetadataKey])
	require.Len(t, backend.Calls(), 1, "a refused call is not run")

	cancel()
	require.Error(t, <-done)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.NoError(t, err, "the budget is released when the held call ends")
}

func TestRegistryAdmissionReleasedAfterErrorReply(t *testing.T) {
	backend := newRegistryTestBackend(t)
	backend.FailMethod(registryTestBackendMethod, tools.GRPCBytesFailure{Err: status.Error(codes.Internal, "boom")})
	// Budget of one call at a time: two calls of users.get cost 2 * 65537 bytes, over the budget, so a budget which is not
	// released after the first failed call refuses the second.
	reg := admissionRegistryConfig(admissionBlobCost)
	registry := startRegistryTestRegistry(t, reg, backend)
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = reg
	h, _ := newRegistryTestHandler(t, cfg, registry)
	client := newRegistryTestClient("c1", "42", nil, nil)

	for i := 0; i < 2; i++ {
		_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
		var protoErr *centrifuge.Error
		require.ErrorAs(t, err, &protoErr)
		require.NotEqual(t, uint32(111), protoErr.Code, "call %d: the budget of the failed call is released", i)
	}
	require.Len(t, backend.Calls(), 2)
}

func TestRegistryResponseHintReservesTheHintedBound(t *testing.T) {
	backend := newRegistryTestBackend(t)
	reg := admissionRegistryConfig(testAdmissionHeldCost + testAdmissionSmallRead)
	registry := startRegistryTestRegistry(t, reg, backend)
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = reg
	h, _ := newRegistryTestHandler(t, cfg, registry)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startHeldCall(t, h, backend, ctx)

	client := newRegistryTestClient("c2", "42", nil, nil)
	hinted := testHintedRequest(4096, []byte("x"))
	reply, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "blob.read", Data: hinted}, nil)
	require.NoError(t, err, "a read which asks for 4 KiB reserves 5 KiB, so it fits beside the held call")
	require.Equal(t, hinted, reply.Data)

	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "blob.read", Data: []byte("x")}, nil)
	var protoErr *centrifuge.Error
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(111), protoErr.Code, "a read without the hint reserves the whole 1 MiB bound")

	cancel()
	require.Error(t, <-done)
}

func TestRegistryResponseHintBoundsTheReply(t *testing.T) {
	backend := newRegistryTestBackend(t)
	reg := admissionRegistryConfig(0)
	registry := startRegistryTestRegistry(t, reg, backend)
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = reg
	h, _ := newRegistryTestHandler(t, cfg, registry)
	client := newRegistryTestClient("c2", "42", nil, nil)
	payload := make([]byte, 2000)

	// The backend echoes the request, so a reply of about 2 KB is over the 1124 bytes the hint of 100 allows.
	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "blob.read", Data: testHintedRequest(100, payload)}, nil)
	var protoErr *centrifuge.Error
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(106), protoErr.Code, "a reply over the hinted bound fails as a reply over the method limit does")

	reply, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "blob.read", Data: testHintedRequest(4096, payload)}, nil)
	require.NoError(t, err)
	require.Equal(t, testHintedRequest(4096, payload), reply.Data)
}

// A refused call with a msg_id leaves no record of exactly-once RPC: its retransmit, once the budget is free, runs the
// backend once, and its answer is not a cached one. The refusal is counted in both admission metrics, and the in-flight
// gauge follows the charge of the held call.
func TestRegistryAdmissionRefusedMsgIDLeavesNoDedupRecord(t *testing.T) {
	vec := testDedupOutcomes(t)
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.Errors.Overloaded = configtypes.RPCRegistryError{Code: 111, Message: "too many requests"}
	// The held call of users.get costs its 4 byte request and its 64 KiB response bound. The budget leaves 100 bytes
	// beside it, so a second call of the method, whose cost is over 100 bytes, is refused while the first is held.
	heldCost := int64(len("gate") + 64<<10)
	reg.MaxInflightBytes = heldCost + 100
	h := newDedupHandler(t, reg, backend)

	overloadedBefore := testutil.ToFloat64(metrics.RPCRegistryCallsTotal.WithLabelValues("users.get", metrics.RPCRegistryResultOverloaded))
	inflightRefusalsBefore := testutil.ToFloat64(metrics.RPCRegistryAdmissionRefusalsTotal.WithLabelValues(metrics.RPCRegistryRefusedInflight))
	gaugeBefore := testutil.ToFloat64(metrics.RPCRegistryInflightBytes)

	held := newDedupClient("c1", "42", nil, testInstance)
	heldDone := dedupCallAsync(h, held, "users.get", dedupMsgID(1), "gate")
	backend.waitEntered(t)
	require.Equal(t, gaugeBefore+float64(heldCost), testutil.ToFloat64(metrics.RPCRegistryInflightBytes), "the gauge follows the held charge")

	client := newDedupClient("c2", "42", nil, testInstance)
	msgID := dedupMsgID(2)
	_, err := dedupCall(h, client, "users.get", msgID, "x")
	var protoErr *centrifuge.Error
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(111), protoErr.Code)
	require.True(t, protoErr.Temporary)
	require.Equal(t, rpcRetryLater, protoErr.Metadata[rpcRetryMetadataKey])
	require.Len(t, backend.Calls(), 1, "a refused call is not run")
	require.Equal(t, overloadedBefore+1, testutil.ToFloat64(metrics.RPCRegistryCallsTotal.WithLabelValues("users.get", metrics.RPCRegistryResultOverloaded)))
	require.Equal(t, inflightRefusalsBefore+1, testutil.ToFloat64(metrics.RPCRegistryAdmissionRefusalsTotal.WithLabelValues(metrics.RPCRegistryRefusedInflight)))

	backend.releaseGate()
	r := waitDedupResult(t, heldDone)
	require.NoError(t, r.err)
	require.Equal(t, gaugeBefore, testutil.ToFloat64(metrics.RPCRegistryInflightBytes), "the gauge returns to its value before the held call")

	// The retransmit carries the same msg_id. It finds no record, so it runs the backend once.
	reply, err := dedupCall(h, client, "users.get", msgID, "x")
	require.NoError(t, err)
	require.False(t, reply.Cached)
	require.Equal(t, []byte("reply:x"), reply.Data)
	require.Len(t, backend.Calls(), 2, "the held call and the retransmit each run the backend once")
	require.Equal(t, 0.0, testDedupCount(vec, "hit_done"))
	require.Equal(t, 2.0, testDedupCount(vec, "miss"), "only the held call and the retransmit reach the dedup store")
}

// A call refused by the memory watermark is counted under the memory reason, and under the overloaded result.
func TestRegistryAdmissionMemoryRefusalIsCounted(t *testing.T) {
	backend := newRegistryTestBackend(t)
	reg := registryTestRegistryConfig()
	reg.Errors.Overloaded = configtypes.RPCRegistryError{Code: 111, Message: "too many requests"}
	// The resident memory of the test process is over 1 MiB, so the first admission trips the watermark.
	reg.MemoryWatermarkBytes = 1 << 20
	registry := startRegistryTestRegistry(t, reg, backend)
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = reg
	h, _ := newRegistryTestHandler(t, cfg, registry)
	client := newRegistryTestClient("c1", "42", nil, nil)

	overloadedBefore := testutil.ToFloat64(metrics.RPCRegistryCallsTotal.WithLabelValues("users.get", metrics.RPCRegistryResultOverloaded))
	memoryRefusalsBefore := testutil.ToFloat64(metrics.RPCRegistryAdmissionRefusalsTotal.WithLabelValues(metrics.RPCRegistryRefusedMemory))
	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	var protoErr *centrifuge.Error
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(111), protoErr.Code)
	require.Empty(t, backend.Calls(), "a refused call is not run")
	require.Equal(t, overloadedBefore+1, testutil.ToFloat64(metrics.RPCRegistryCallsTotal.WithLabelValues("users.get", metrics.RPCRegistryResultOverloaded)))
	require.Equal(t, memoryRefusalsBefore+1, testutil.ToFloat64(metrics.RPCRegistryAdmissionRefusalsTotal.WithLabelValues(metrics.RPCRegistryRefusedMemory)))
}

func BenchmarkRegistryCallLarge(b *testing.B) {
	const size = 512 << 10
	backend := newRegistryTestBackend(b)
	reg := admissionRegistryConfig(16 << 20)
	reg.Methods[len(reg.Methods)-1].MaxRequestBytes = size + 64
	reg.Methods[len(reg.Methods)-1].MaxResponseBytes = size + 1024
	registry := startRegistryTestRegistry(b, reg, backend)
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = reg
	h, _ := newRegistryTestHandler(b, cfg, registry)
	client := newRegistryTestClient("c1", "42", nil, nil)
	data := testHintedRequest(size, make([]byte, size))

	b.ReportAllocs()
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "blob.read", Data: data}, nil); err != nil {
			b.Fatal(err)
		}
	}
}
