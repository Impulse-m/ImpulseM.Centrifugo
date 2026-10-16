package proxy

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/test/bufconn"
)

// testRingHashHeader is the affinity header of the ring_hash tests.
const testRingHashHeader = "x-example-affinity"

// testReplica is an in-process backend which records the affinity header of each call it receives.
type testReplica struct {
	listener *bufconn.Listener
	server   *grpc.Server

	mu sync.Mutex
	// affinity holds the affinity header of each call, empty when the call has none.
	affinity []string
}

// newTestReplica starts a replica which answers every call with its request bytes.
func newTestReplica(t *testing.T) *testReplica {
	t.Helper()
	replica := &testReplica{listener: bufconn.Listen(1 << 20)}
	replica.server = grpc.NewServer(
		grpc.ForceServerCodec(tools.GRPCBytesCodec{}),
		grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
			md, _ := metadata.FromIncomingContext(stream.Context())
			var in []byte
			err := stream.RecvMsg(&in)
			if err != nil {
				return err
			}
			var value string
			if values := md.Get(testRingHashHeader); len(values) > 0 {
				value = values[0]
			}
			replica.mu.Lock()
			replica.affinity = append(replica.affinity, value)
			replica.mu.Unlock()
			return stream.SendMsg(&in)
		}),
	)
	go func() {
		_ = replica.server.Serve(replica.listener)
	}()
	t.Cleanup(replica.server.Stop)
	return replica
}

func TestRegistryServiceConfig(t *testing.T) {
	require.Equal(t,
		`{"loadBalancingConfig":[{"round_robin":{}}]}`,
		registryServiceConfig(configtypes.RPCTarget{LoadBalancing: configtypes.RPCLoadBalancingRoundRobin}),
	)
	require.Equal(t,
		`{"loadBalancingConfig":[{"ring_hash_experimental":{"requestHashHeader":"x-example-affinity"}}]}`,
		registryServiceConfig(configtypes.RPCTarget{LoadBalancing: configtypes.RPCLoadBalancingRingHash, AffinityHeader: testRingHashHeader}),
	)
}

// TestRegistryRingHashKeepsAffinityOnOneReplica runs calls over three replicas with the ring_hash service config of a
// target. The calls of one affinity value all land on one replica, and calls without the header reach every replica.
func TestRegistryRingHashKeepsAffinityOnOneReplica(t *testing.T) {
	const scheme = "ringhashtest"
	replicas := map[string]*testReplica{}
	var addresses []resolver.Address
	for i := 0; i < 3; i++ {
		addr := fmt.Sprintf("replica-%d", i)
		replicas[addr] = newTestReplica(t)
		addresses = append(addresses, resolver.Address{Addr: addr})
	}
	builder := manual.NewBuilderWithScheme(scheme)
	builder.InitialState(resolver.State{Addresses: addresses})
	conn, err := grpc.NewClient(
		scheme+":///replicas",
		grpc.WithResolvers(builder),
		grpc.WithDefaultServiceConfig(registryServiceConfig(configtypes.RPCTarget{
			LoadBalancing:  configtypes.RPCLoadBalancingRingHash,
			AffinityHeader: testRingHashHeader,
		})),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return replicas[addr].listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	invoke := func(ctx context.Context) {
		var out []byte
		err := conn.Invoke(ctx, "/example.v1.BlobService/Read", []byte("x"), &out, grpc.ForceCodec(tools.GRPCBytesCodec{}))
		require.NoError(t, err)
	}
	for round := 0; round < 20; round++ {
		for value := 0; value < 50; value++ {
			invoke(metadata.AppendToOutgoingContext(context.Background(), testRingHashHeader, fmt.Sprintf("object-%d", value)))
		}
	}
	for i := 0; i < 300; i++ {
		invoke(context.Background())
	}

	owner := map[string]string{}
	for addr, replica := range replicas {
		replica.mu.Lock()
		withoutHeader := 0
		for _, value := range replica.affinity {
			if value == "" {
				withoutHeader++
				continue
			}
			if prev, found := owner[value]; found && prev != addr {
				t.Fatalf("affinity value %q reached %s and %s", value, prev, addr)
			}
			owner[value] = addr
		}
		replica.mu.Unlock()
		require.Positive(t, withoutHeader, "calls without the header reach replica %s", addr)
	}
	require.Len(t, owner, 50, "every affinity value reached a replica")
}
