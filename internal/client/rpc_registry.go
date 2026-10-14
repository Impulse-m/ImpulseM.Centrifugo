package client

import (
	"context"
	"errors"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
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

// registryProxyProtocol is the protocol label of the proxy metrics of registry targets.
const registryProxyProtocol = "grpc"

// registryProxyType is the type label of the proxy metrics of registry targets.
const registryProxyType = "rpc"

// SetRPCRegistry sets the RPC method registry consulted for client RPC calls. Must be called before Setup.
func (h *Handler) SetRPCRegistry(registry *rpcregistry.Registry) {
	h.rpcRegistry = registry
}

// onRPCRegistry answers a client RPC through the RPC method registry. handled is false when the
// call goes on to the namespace proxies as before.
func (h *Handler) onRPCRegistry(c Client, e centrifuge.RPCEvent, rpcProxyHandler proxy.RPCHandlerFunc) (centrifuge.RPCReply, bool, error) {
	if !h.rpcRegistry.Enabled() {
		// OnRPC is registered for the registry too, so a disabled registry must answer
		// as it did when OnRPC was not set: the default protocol error when nothing
		// else handles RPC. Otherwise the call goes on as before.
		if rpcProxyHandler == nil && len(h.rpcExtension) == 0 {
			return centrifuge.RPCReply{}, true, centrifuge.ErrorNotAvailable
		}
		return centrifuge.RPCReply{}, false, nil
	}
	m := h.rpcRegistry.Lookup(e.Method)
	if m == nil {
		if rpcProxyHandler != nil && h.namespaceRPCProxyEnabled(e.Method) {
			return centrifuge.RPCReply{}, false, nil
		}
		unknownErr := registryError(h.registryErrors().UnknownMethod)
		return centrifuge.RPCReply{}, true, registryFailure(registryUnknownMethod, metrics.RPCRegistryResultUnknown, unknownErr)
	}
	reply, err := h.callRPCRegistry(c, e, m)
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

// callRPCRegistry checks the connection against the method and calls its gRPC target.
func (h *Handler) callRPCRegistry(c Client, e centrifuge.RPCEvent, m *rpcregistry.Method) (centrifuge.RPCReply, error) {
	cfg := h.registryConfig()
	// Anonymous methods are open to everyone. pending_2fa and bot methods are checked
	// like authenticated ones for now: the class claims that tell them apart come with
	// the auth upgrade work, so they are not invented here.
	if m.AuthClass != rpcregistry.AuthClassAnonymous && c.UserID() == "" {
		authErr := registryError(cfg.Errors.AuthClass)
		return centrifuge.RPCReply{}, registryFailure(m.Name, metrics.RPCRegistryResultDenied, authErr)
	}
	if len(e.Data) > m.MaxRequestBytes {
		sizeErr := registryError(cfg.Errors.RequestTooLarge)
		return centrifuge.RPCReply{}, registryFailure(m.Name, metrics.RPCRegistryResultError, sizeErr)
	}
	var token string
	if m.AuthClass != rpcregistry.AuthClassAnonymous {
		token = connectionToken(c)
	}
	started := time.Now()
	data, trailers, err := proxy.CallRegistryMethod(c.Context(), m, proxy.RegistryCall{
		ClientID: c.ID(),
		Token:    token,
		Data:     e.Data,
		Headers:  e.Call.Headers,
		MsgID:    e.Call.MsgID,
	})
	// The client context decides whether the call was cancelled. A CANCELLED status sent by the
	// backend on a live connection is an ordinary failure of the target.
	cancelled := errors.Is(c.Context().Err(), context.Canceled)
	observeRegistryTarget(m.Target.Config.Name, time.Since(started).Seconds(), err, cancelled)
	if err != nil {
		return centrifuge.RPCReply{}, registryFailure(m.Name, registryCallResult(err, cancelled), registryCallError(err, trailers, m, cfg, cancelled))
	}
	metrics.IncRPCRegistryCall(m.Name, metrics.RPCRegistryResultOK)
	return centrifuge.RPCReply{Data: data}, nil
}

// observeRegistryTarget records the duration of a call to a registry target in the proxy metrics,
// with the target as the name label. A cancelled call is not counted as a proxy error: the client
// went away, the target did not fail.
func observeRegistryTarget(target string, seconds float64, err error, cancelled bool) {
	metrics.ProxyCallDurationHistogram.WithLabelValues(registryProxyProtocol, registryProxyType, target).Observe(seconds)
	if err != nil && !cancelled {
		metrics.ProxyCallErrorCount.WithLabelValues(registryProxyProtocol, registryProxyType, target).Inc()
	}
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

// registryFailure counts a registry call which ends with err, and returns err. A cancelled call is
// counted in the call counter only, as the proxy error counter does not count it.
func registryFailure(method string, result string, err error) error {
	metrics.IncRPCRegistryCall(method, result)
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
	return &centrifuge.Error{
		Code:    e.Code,
		Message: e.Message,
		AppCode: e.AppCode,
		AppType: e.AppType,
	}
}

// storeRegistryToken stores the verified connection token for the registry, when the registry is enabled.
func (h *Handler) storeRegistryToken(storage map[string]any, token string) {
	if h.rpcRegistry.Enabled() {
		storage[clientstorage.KeyToken] = token
	}
}

// replaceRegistryToken replaces the connection token of the client with the one from a refresh.
func (h *Handler) replaceRegistryToken(c Client, token string) {
	if !h.rpcRegistry.Enabled() {
		return
	}
	storage, release := c.AcquireStorage()
	defer release(storage)
	storage[clientstorage.KeyToken] = token
}

// connectionToken returns the connection token stored for the client, or empty when there is none.
func connectionToken(c Client) string {
	storage, release := c.AcquireStorage()
	defer release(storage)
	token, _ := storage[clientstorage.KeyToken].(string)
	return token
}
