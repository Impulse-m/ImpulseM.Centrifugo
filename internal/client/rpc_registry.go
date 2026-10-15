package client

import (
	"context"
	"errors"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"

	"github.com/centrifugal/centrifuge"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// registryUnknownMethod is the rpc_method label of calls to names which are not in the registry.
const registryUnknownMethod = "unknown"

// SetRPCRegistry sets the RPC method registry consulted for client RPC calls. Must be called before Setup.
func (h *Handler) SetRPCRegistry(registry *rpcregistry.Registry) {
	h.rpcRegistry = registry
}

// onRPCRegistry answers a client RPC through the RPC method registry. handled is false when the
// call goes on to the namespace proxies as before.
func (h *Handler) onRPCRegistry(c rebindableClient, e centrifuge.RPCEvent, rpcProxyHandler proxy.RPCHandlerFunc) (centrifuge.RPCReply, bool, error) {
	if !h.rpcRegistry.Enabled() {
		// OnRPC is registered for the registry too, so a disabled registry must answer
		// as it did when OnRPC was not set: the default protocol error when nothing
		// else handles RPC. Otherwise the call goes on as before.
		if rpcProxyHandler == nil && len(h.rpcExtension) == 0 {
			return centrifuge.RPCReply{}, true, centrifuge.ErrorNotAvailable
		}
		return centrifuge.RPCReply{}, false, nil
	}
	var track *rpcTrack
	// The identity of the call is read once here, so its dedup scope, its token and its class come from the same identity.
	caller := registryConnection(c, h.registryConfig().AuthClassClaim)
	if h.rpcDedup != nil {
		track = h.rpcTrackCall(c, caller, e.Call)
		h.rpcAck(track, e.Call.Ack)
		if e.Call.Cancel != 0 {
			// A cancel command names no method, and is answered before the lookup. With the dedup off it is an
			// ordinary call, as it was before exactly-once RPC.
			reply, err := h.cancelRPCRegistry(track, e.Call.Cancel)
			return reply, true, err
		}
		runTestHook(&testHookRPCTracked, c)
	}
	m := h.rpcRegistry.Lookup(e.Method)
	if m == nil {
		if rpcProxyHandler != nil && h.namespaceRPCProxyEnabled(e.Method) {
			return centrifuge.RPCReply{}, false, nil
		}
		unknownErr := registryError(h.registryErrors().UnknownMethod)
		metrics.IncRPCRegistryCall(registryUnknownMethod, metrics.RPCRegistryResultUnknown)
		return centrifuge.RPCReply{}, true, registryFailure(registryUnknownMethod, metrics.RPCRegistryResultUnknown, unknownErr)
	}
	reply, err := h.callRPCRegistry(c, e, m, caller, track)
	return reply, true, err
}

// namespaceRPCProxyEnabled reports whether a namespace RPC proxy handles method. A config error is
// reported as handled, so the namespace path reports it as before.
func (h *Handler) namespaceRPCProxyEnabled(method string) bool {
	rpcOpts, found, err := h.cfgContainer.RpcOptions(method)
	if err != nil {
		return true
	}
	return found && rpcOpts.ProxyEnabled
}

// registryConfig returns the RPC method registry config of the running configuration.
func (h *Handler) registryConfig() configtypes.RPCRegistry {
	return h.cfgContainer.ConfigRef().RPC.Registry
}

// registryErrors returns the protocol errors configured for registry-originated failures.
func (h *Handler) registryErrors() configtypes.RPCRegistryErrors {
	return h.registryConfig().Errors
}

// registryConnectFlags returns the connect reply flags advertising the RPC options capability.
// Core applies a flag only where the client advertised it, so an enabled registry is announced
// to every client and clients without the capability simply ignore it.
func (h *Handler) registryConnectFlags() int64 {
	if !h.rpcRegistry.Enabled() {
		return 0
	}
	return centrifuge.ConnectionFlagRPCOptions
}

// callRPCRegistry checks the connection against the method, and calls its gRPC target. caller is the identity the call
// runs with, which track was made under too. A call with a msg_id is tracked when the dedup is on, see callRPCTracked.
// track is nil when the dedup is off.
func (h *Handler) callRPCRegistry(
	c rebindableClient,
	e centrifuge.RPCEvent,
	m *rpcregistry.Method,
	caller registryCaller,
	track *rpcTrack,
) (centrifuge.RPCReply, error) {
	cfg := h.registryConfig()
	// The class of the connection decides whether it may call the method, see AuthClass.Admits.
	if !m.AuthClass.Admits(caller.class) {
		authErr := registryError(cfg.Errors.AuthClass)
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultDenied, authErr)
	}
	if len(e.Data) > m.MaxRequestBytes {
		sizeErr := registryError(cfg.Errors.RequestTooLarge)
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, sizeErr)
	}
	if e.Call.Retransmit {
		m.Metrics.IncRetransmit()
	}
	if track != nil && e.Call.MsgID != 0 {
		return h.callRPCTracked(c, e, m, cfg, caller.token, track)
	}
	return h.invokeRPCRegistry(c.Context(), c, e, m, cfg, caller.token)
}

// invokeRPCRegistry calls the gRPC target of m for the client, on ctx, and maps a failure to the error the client
// receives. The context of the call is the client context, unless a tracked call gave its own.
func (h *Handler) invokeRPCRegistry(ctx context.Context, c Client, e centrifuge.RPCEvent, m *rpcregistry.Method, cfg configtypes.RPCRegistry, connToken string) (centrifuge.RPCReply, error) {
	var token string
	if m.AuthClass != rpcregistry.AuthClassAnonymous {
		token = connToken
	}
	started := time.Now()
	m.Target.Metrics.IncInflight()
	defer m.Target.Metrics.DecInflight()
	data, trailers, err := proxy.CallRegistryMethod(ctx, m, proxy.RegistryCall{
		ClientID:       c.ID(),
		Token:          token,
		Data:           e.Data,
		Headers:        e.Call.Headers,
		MsgID:          e.Call.MsgID,
		Retransmit:     e.Call.Retransmit,
		WithoutUpdates: e.Call.WithoutUpdates,
	})
	// The context decides whether the call was cancelled. A CANCELLED status sent by the
	// backend on a live connection is an ordinary failure of the target. A call cancelled by the
	// client is not observed in the proxy metrics, as the proxy handler does for a closed client.
	cancelled := errors.Is(ctx.Err(), context.Canceled)
	if !cancelled {
		m.Target.Metrics.ObserveCall(time.Since(started).Seconds(), err != nil)
	}
	if err != nil {
		return centrifuge.RPCReply{}, registryMethodFailure(m, registryCallResult(err, cancelled), registryCallError(err, trailers, m, cfg, cancelled))
	}
	m.Metrics.IncCall(metrics.RPCRegistryResultOK)
	return centrifuge.RPCReply{Data: data}, nil
}

// registryCallError maps the error of a failed registry call to the error the client receives. It
// is the one place where the mapping lives. A response over the limit and a cancelled call come from
// the registry config. Any other error goes through the gRPC status mapping, which fills the
// application fields from the trailers, and may give a disconnect.
func registryCallError(err error, trailers metadata.MD, m *rpcregistry.Method, cfg configtypes.RPCRegistry, cancelled bool) error {
	if errors.Is(err, proxy.ErrRPCResponseTooLarge) {
		return registryError(cfg.Errors.RequestTooLarge)
	}
	if cancelled {
		return registryError(cfg.Errors.Cancelled)
	}
	protoErr, disconnect := proxy.MapGRPCRegistryError(err, trailers, cfg.GRPCStatusToCodeTransforms, cfg.AppError, m.ErrorMetadata)
	if disconnect != nil {
		return disconnect
	}
	return protoErr
}

// registryCallResult returns the result label of a failed registry call. cancelled tells that the
// client went away during the call.
func registryCallResult(err error, cancelled bool) string {
	switch {
	case cancelled:
		return metrics.RPCRegistryResultCancelled
	case status.Code(err) == codes.DeadlineExceeded:
		return metrics.RPCRegistryResultTimeout
	default:
		return metrics.RPCRegistryResultError
	}
}

// registryMethodFailure counts a failed call of m with its result, and returns err.
func registryMethodFailure(m *rpcregistry.Method, result string, err error) error {
	m.Metrics.IncCall(result)
	return registryFailure(m.Name, result, err)
}

// registryFailure counts the application error of a failed registry call, and returns err. The call
// counter is counted by the caller. A cancelled call has no application error to count.
func registryFailure(method string, result string, err error) error {
	if result == metrics.RPCRegistryResultCancelled {
		return err
	}
	var protoErr *centrifuge.Error
	var appType string
	if errors.As(err, &protoErr) {
		appType = protoErr.AppType
	}
	metrics.IncRPCRegistryError(method, appType)
	return err
}

// registryError returns the protocol error configured for a registry-originated failure.
func registryError(e configtypes.RPCRegistryError) error {
	return registryErrorValue(e)
}

// registryErrorValue is registryError with the concrete protocol error, for the dedup records.
func registryErrorValue(e configtypes.RPCRegistryError) *centrifuge.Error {
	return &centrifuge.Error{
		Code:    e.Code,
		Message: e.Message,
		AppCode: e.AppCode,
		AppType: e.AppType,
	}
}

// registryCaller is the identity a connection calls the registry with. It is read once for a call, from the record of the
// connection, so the token, the class and the dedup scope of the call are those of one identity.
type registryCaller struct {
	token    string
	class    rpcregistry.AuthClass
	userID   string
	identity centrifuge.TokenIdentity
	// recorded is false for a connection without a record, whose token identity is the one Core holds. Core's identity is
	// read only when the dedup scope needs it, see rpcTrackCall.
	recorded bool
}

// registryConnection reads the identity a connection calls the registry with, and its effective auth class under claim,
// the claim configured now. A connection without a record has no token, and its class is derived as effectiveClass does.
func registryConnection(c rebindableClient, claim string) registryCaller {
	rec := connStateOf(c)
	if rec == nil {
		return registryCaller{
			class:  effectiveClass(&connIdentity{}, claim, c.UserID()),
			userID: c.UserID(),
		}
	}
	id := rec.identity.Load()
	return registryCaller{
		token:    id.token,
		class:    effectiveClass(id, claim, c.UserID()),
		userID:   id.tokenUser,
		identity: id.identity,
		recorded: true,
	}
}

// authClassName returns the name of the auth class reported to the client. It is empty when the registry is
// disabled, as the auth class has no meaning for the connection then.
func (h *Handler) authClassName(class rpcregistry.AuthClass) string {
	if !h.rpcRegistry.Enabled() {
		return ""
	}
	return class.String()
}
