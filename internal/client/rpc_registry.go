package client

import (
	"context"
	"errors"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
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
		metrics.IncRPCRegistryCall(registryUnknownMethod, metrics.RPCRegistryResultUnknown)
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

// registryConnectFlags returns the connect reply flags advertising the RPC options capability.
// Core applies a flag only where the client advertised it, so an enabled registry is announced
// to every client and clients without the capability simply ignore it.
func (h *Handler) registryConnectFlags() int64 {
	if !h.rpcRegistry.Enabled() {
		return 0
	}
	return centrifuge.ConnectionFlagRPCOptions
}

// callRPCRegistry checks the connection against the method and calls its gRPC target.
func (h *Handler) callRPCRegistry(c Client, e centrifuge.RPCEvent, m *rpcregistry.Method) (centrifuge.RPCReply, error) {
	cfg := h.registryConfig()
	connToken, connClass := registryConnection(c, cfg.AuthClassClaim)
	// The class of the connection decides whether it may call the method, see AuthClass.Admits.
	if !m.AuthClass.Admits(connClass) {
		authErr := registryError(cfg.Errors.AuthClass)
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultDenied, authErr)
	}
	if len(e.Data) > m.MaxRequestBytes {
		sizeErr := registryError(cfg.Errors.RequestTooLarge)
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, sizeErr)
	}
	var token string
	if m.AuthClass != rpcregistry.AuthClassAnonymous {
		token = connToken
	}
	started := time.Now()
	m.Target.Metrics.IncInflight()
	defer m.Target.Metrics.DecInflight()
	data, trailers, err := proxy.CallRegistryMethod(c.Context(), m, proxy.RegistryCall{
		ClientID: c.ID(),
		Token:    token,
		Data:     e.Data,
		Headers:  e.Call.Headers,
		MsgID:    e.Call.MsgID,
	})
	// The client context decides whether the call was cancelled. A CANCELLED status sent by the
	// backend on a live connection is an ordinary failure of the target. A call cancelled by the
	// client is not observed in the proxy metrics, as the proxy handler does for a closed client.
	cancelled := errors.Is(c.Context().Err(), context.Canceled)
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
	return &centrifuge.Error{
		Code:    e.Code,
		Message: e.Message,
		AppCode: e.AppCode,
		AppType: e.AppType,
	}
}

// registryAuthClass is the auth class stored for a connection at connect or refresh, with the name of
// the claim it was derived under.
type registryAuthClass struct {
	class rpcregistry.AuthClass
	claim string
}

// storeConnectClass stores the verified connection token and its auth class for the registry, when the registry is
// enabled, and returns the auth class of the token. It reports false when the auth class of the token is invalid for
// its user, and the connection must be rejected.
func (h *Handler) storeConnectClass(
	storage map[string]any,
	token string,
	verified jwtverify.ConnectToken,
) (rpcregistry.AuthClass, bool) {
	authClass, ok := rpcregistry.ConnectionAuthClass(verified.AuthClass, verified.UserID)
	if !ok {
		return 0, false
	}
	storage[clientstorage.KeyTokenUser] = verified.UserID
	h.setRegistryStorage(storage, token, authClass)
	return authClass, true
}

// setRegistryStorage stores the connection token when the registry is enabled, and the auth class when it is kept.
func (h *Handler) setRegistryStorage(storage map[string]any, token string, authClass rpcregistry.AuthClass) {
	if h.rpcRegistry.Enabled() {
		storage[clientstorage.KeyToken] = token
	}
	if h.keepsAuthClass() {
		storage[clientstorage.KeyAuthClass] = registryAuthClass{class: authClass, claim: h.registryConfig().AuthClassClaim}
	}
}

// keepsAuthClass reports whether the auth class of a connection is stored. It is stored for the registry, and for
// rebind, which needs the class of the connection to check a transition even when the registry is disabled.
func (h *Handler) keepsAuthClass() bool {
	return h.rpcRegistry.Enabled() || h.cfgContainer.ConfigRef().Client.Rebind.Enabled
}

// replaceRegistryToken replaces the connection token and auth class of the client with the ones of a
// refresh. The caller has validated the class of the token.
func (h *Handler) replaceRegistryToken(c Client, token string, authClass rpcregistry.AuthClass) {
	if !h.keepsAuthClass() {
		return
	}
	storage, release := c.AcquireStorage()
	defer release(storage)
	h.setRegistryStorage(storage, token, authClass)
}

// registryConnection returns the connection token stored for the client, and the effective auth class of
// the client for the registry, with claim the current auth class claim. The storage is acquired once.
//   - A stored class derived under claim is the class of the connection.
//   - A stored class derived under another claim is anonymous, until a refresh derives it again.
//   - Without a stored class, the class is taken from the user without a claim, and is anonymous when a
//     claim is configured: a connection without a token cannot prove a class then.
func registryConnection(c Client, claim string) (string, rpcregistry.AuthClass) {
	userID := c.UserID()
	storage, release := c.AcquireStorage()
	defer release(storage)
	token, _ := storage[clientstorage.KeyToken].(string)
	return token, authClassOf(storage, claim, userID)
}

// hasStoredAuthClass reports whether the client has an auth class stored under claim, which a connection
// established by a token has. A connection made through a connect proxy has none.
func hasStoredAuthClass(c Client, claim string) bool {
	storage, release := c.AcquireStorage()
	defer release(storage)
	stored, ok := storage[clientstorage.KeyAuthClass].(registryAuthClass)
	return ok && stored.claim == claim
}

// authClassOf returns the auth class of a connection with the storage of the connection, its user and the
// claim, as registryConnection describes it.
func authClassOf(storage map[string]any, claim string, userID string) rpcregistry.AuthClass {
	stored, ok := storage[clientstorage.KeyAuthClass].(registryAuthClass)
	if ok {
		if stored.claim == claim {
			return stored.class
		}
		return rpcregistry.AuthClassAnonymous
	}
	if claim != "" {
		return rpcregistry.AuthClassAnonymous
	}
	class, _ := rpcregistry.ConnectionAuthClass("", userID)
	return class
}

// authClassName returns the name of the auth class reported to the client. It is empty when the registry is
// disabled, as the auth class has no meaning for the connection then.
func (h *Handler) authClassName(class rpcregistry.AuthClass) string {
	if !h.rpcRegistry.Enabled() {
		return ""
	}
	return class.String()
}
