package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ErrRPCResponseTooLarge is returned by CallRegistryMethod when the target answers with a message
// over the response limit of the method.
var ErrRPCResponseTooLarge = errors.New("rpc registry response too large")

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
		grpc.WithDefaultServiceConfig(registryServiceConfig(target.Config.LoadBalancing)),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	conn, err := grpc.NewClient(host, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("error connecting to GRPC target: %w", err)
	}
	return conn, nil
}

// registryServiceConfig returns the gRPC service config which selects the load balancing policy.
func registryServiceConfig(policy string) string {
	return fmt.Sprintf(`{"loadBalancingConfig":[{%q:{}}]}`, policy)
}

// CallRegistryMethod calls the gRPC method of m with the payload of call. The context carries the
// client connection, which is the source of connect-time headers, and the call timeout is applied
// here. It returns the reply payload and the trailers. Errors are gRPC status errors, or
// ErrRPCResponseTooLarge when the reply is over the response limit.
func CallRegistryMethod(ctx context.Context, m *rpcregistry.Method, call RegistryCall) ([]byte, metadata.MD, error) {
	conn, ok := m.Target.Conn().(*grpc.ClientConn)
	if !ok || conn == nil {
		return nil, nil, fmt.Errorf("rpc registry target %q has no connection", m.Target.Config.Name)
	}
	ctx, cancel := context.WithTimeout(ctx, m.Timeout)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, registryRequestMetadata(ctx, m, call))

	var out []byte
	var trailer metadata.MD
	err := conn.Invoke(
		ctx,
		m.GRPCMethod,
		call.Data,
		&out,
		grpc.ForceCodec(bytesCodec{}),
		grpc.MaxCallRecvMsgSize(m.MaxResponseBytes),
		grpc.Trailer(&trailer),
	)
	if err != nil {
		if isRegistryResponseTooLarge(err) {
			return nil, trailer, ErrRPCResponseTooLarge
		}
		return nil, trailer, err
	}
	return out, trailer, nil
}

// isRegistryResponseTooLarge reports whether err is the gRPC error for a message over the receive limit.
func isRegistryResponseTooLarge(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.ResourceExhausted && strings.Contains(st.Message(), "larger than max")
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
