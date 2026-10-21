package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/clientaddr"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/ringhash" // Registers ring_hash_experimental, the policy of the ring_hash targets.
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ErrRPCResponseTooLarge is returned by CallRegistryMethod when the target answers with a message
// over the response limit of the method.
var ErrRPCResponseTooLarge = errors.New("rpc registry response too large")

// ErrRPCTargetRequestTooLarge is returned by CallRegistryMethod when a grpc-go target refused the request with
// its own receive limit.
var ErrRPCTargetRequestTooLarge = errors.New("rpc registry target refused the request as too large")

// RegistryCall is one client call through the RPC method registry.
type RegistryCall struct {
	// ClientID is the client id, sent as x-centrifugo-client.
	ClientID string
	// Token is the connection token. It is forwarded as bearer authorization to methods which
	// are not anonymous. Empty when the connection has no token.
	Token string
	// Data is the request payload, passed to the target untouched.
	Data []byte
	// Headers are the per-call client headers, forwarded when the method allow-lists them.
	Headers map[string]string
	// MsgID is the client request id, sent as x-centrifugo-msg-id when not zero.
	MsgID uint64
	// Retransmit is set for a resend after a transport loss, sent as x-centrifugo-retransmit.
	Retransmit bool
	// WithoutUpdates is set when the client asks for the call without its updates, sent as
	// x-centrifugo-without-updates.
	WithoutUpdates bool
	// MaxResponseBytes is the response bound of this call, when not zero. It replaces the method's limit, and is the
	// bound the admission of the call reserved.
	MaxResponseBytes int
}

// DialRegistryTarget makes the gRPC connection of a registry target. It is the dial hook of the
// RPC method registry. gRPC connects lazily, so a dial fails only for an invalid endpoint.
func DialRegistryTarget(target *rpcregistry.Target) (io.Closer, error) {
	var p Config
	p.GRPC = target.Config.GRPC
	host, err := getGrpcHost(target.Config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("error getting grpc host: %w", err)
	}
	dialOpts, err := getDialOpts(target.Config.Name, p)
	if err != nil {
		return nil, fmt.Errorf("error creating GRPC dial options: %w", err)
	}
	dialOpts = append(
		dialOpts,
		grpc.WithDefaultServiceConfig(registryServiceConfig(target.Config)),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	conn, err := grpc.NewClient(host, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("error connecting to GRPC target: %w", err)
	}
	return conn, nil
}

// registryServiceConfig returns the gRPC service config which selects the load balancing policy of target. A ring_hash
// target hashes the value of its affinity header, which the config validation guarantees is set.
func registryServiceConfig(target configtypes.RPCTarget) string {
	if target.LoadBalancing == configtypes.RPCLoadBalancingRingHash {
		return fmt.Sprintf(`{"loadBalancingConfig":[{"ring_hash_experimental":{"requestHashHeader":%q}}]}`, target.AffinityHeader)
	}
	return fmt.Sprintf(`{"loadBalancingConfig":[{%q:{}}]}`, target.LoadBalancing)
}

// CallRegistryMethod calls the gRPC method of m with the payload of call. The context carries the
// client connection, which is the source of connect-time headers, and the call timeout is applied
// here. It returns the reply payload and the trailers. Errors are gRPC status errors, or
// ErrRPCResponseTooLarge when the reply is over the response limit, or ErrRPCTargetRequestTooLarge
// when the target refused the request as too large.
func CallRegistryMethod(ctx context.Context, m *rpcregistry.Method, call RegistryCall) ([]byte, metadata.MD, error) {
	conn, ok := m.Target.Conn().(*grpc.ClientConn)
	if !ok || conn == nil {
		return nil, nil, fmt.Errorf("rpc registry target %q has no connection", m.Target.Config.Name)
	}
	ctx, cancel := context.WithTimeout(ctx, m.Timeout)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, registryRequestMetadata(ctx, m, call))

	maxResponseBytes := m.MaxResponseBytes
	if call.MaxResponseBytes > 0 {
		maxResponseBytes = call.MaxResponseBytes
	}
	var out []byte
	var trailer metadata.MD
	err := conn.Invoke(
		ctx,
		m.GRPCMethod,
		call.Data,
		&out,
		grpc.ForceCodec(bytesCodec{}),
		grpc.MaxCallRecvMsgSize(maxResponseBytes),
		grpc.Trailer(&trailer),
	)
	if err != nil {
		tooLarge, reply := registryMessageTooLarge(err, len(call.Data), maxResponseBytes, m.Target.Config.GRPC.Compression)
		switch {
		case reply:
			return nil, trailer, ErrRPCResponseTooLarge
		case tooLarge:
			return nil, trailer, ErrRPCTargetRequestTooLarge
		}
		return nil, trailer, err
	}
	return out, trailer, nil
}

// registryMessageTooLarge classifies a grpc-go "larger than max" error of a call whose request has requestLen
// bytes, whose reply bound is bound, and whose target is compressed or not. tooLarge reports that the error is
// one of these. reply reports that the reply was over bound: the message names bound, or maxInt, which grpc-go
// uses on 32-bit builds. Otherwise a grpc-go target refused the request. A compressed target reports the
// compressed size, which the request size cannot match, so it always counts as a refused request.
func registryMessageTooLarge(err error, requestLen int, bound int, compressed bool) (tooLarge bool, reply bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted || !strings.Contains(st.Message(), "larger than max") {
		return false, false
	}
	if compressed || reportedSize(st.Message()) == requestLen {
		return true, false
	}
	msg := strings.TrimSuffix(st.Message(), ")")
	named := strings.HasSuffix(msg, " "+strconv.Itoa(bound)) || strings.HasSuffix(msg, " "+strconv.Itoa(math.MaxInt))
	return true, named
}

// reportedSize returns the size in the first parenthesis of a grpc-go message, as in "(2000 vs. 1024)". It
// returns -1 when the message has none.
func reportedSize(message string) int {
	_, after, found := strings.Cut(message, "(")
	if !found {
		return -1
	}
	fields := strings.Fields(after)
	if len(fields) == 0 {
		return -1
	}
	size, err := strconv.Atoi(fields[0])
	if err != nil {
		return -1
	}
	return size
}

// registryRequestMetadata builds the outgoing metadata of a registry call. Sources later in the
// list win: static metadata of the target, connect-time headers, per-call headers, then the values
// the registry sets itself.
func registryRequestMetadata(ctx context.Context, m *rpcregistry.Method, call RegistryCall) metadata.MD {
	target := m.Target.Config
	md := requestMetadata(ctx, nil, nil, target.EmulatedHeaders, target.GRPC.StaticMetadata)

	// m.Headers never names a reserved header: validation rejects them. The reserved values below
	// are written after the client headers, so they win if a client value ever gets through.
	setListedHeaders(md, call.Headers, m.Headers, nil)

	if m.AuthClass != rpcregistry.AuthClassAnonymous && call.Token != "" {
		md.Set("authorization", "Bearer "+call.Token)
	}
	md.Set(configtypes.RPCRegistryReservedHeaderPrefix+"client", call.ClientID)
	if call.MsgID != 0 {
		md.Set(configtypes.RPCRegistryReservedHeaderPrefix+"msg-id", strconv.FormatUint(call.MsgID, 10))
	}
	if call.Retransmit {
		md.Set(configtypes.RPCRegistryReservedHeaderPrefix+"retransmit", "1")
	}
	if call.WithoutUpdates {
		md.Set(configtypes.RPCRegistryReservedHeaderPrefix+"without-updates", "1")
	}
	// Written last and omitted when unknown, even if a source carried one: a backend which reads
	// this header would otherwise take the balancer for the client. An absent header is detectable,
	// a wrong one is not.
	if addr, ok := clientaddr.FromContext(ctx); ok {
		md.Set("x-forwarded-for", addr.String())
	} else {
		md.Delete("x-forwarded-for")
	}
	return md
}

// setListedHeaders sets on md every header whose lower-case name is in allowed. It reports whether
// a header was skipped which is listed in other instead. requestMetadata passes the transport
// allow-list as other, to hint at a migration.
func setListedHeaders(md metadata.MD, headers map[string]string, allowed []string, other []string) bool {
	var skipped bool
	for k, v := range headers {
		lk := strings.ToLower(k)
		if slices.Contains(allowed, lk) {
			md.Set(k, v)
		} else if slices.Contains(other, lk) {
			skipped = true
		}
	}
	return skipped
}
