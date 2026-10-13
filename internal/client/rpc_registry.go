package client

import (
	"errors"

	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"

	"github.com/centrifugal/centrifuge"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// registryBackendErrorCode is the protocol error code of a failed call to a registry target.
// The full status mapping comes with the error work.
const registryBackendErrorCode = 100

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
		return centrifuge.RPCReply{}, true, registryError(h.registryErrors().UnknownMethod)
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

// registryErrors returns the protocol errors configured for registry-originated failures.
func (h *Handler) registryErrors() configtypes.RPCRegistryErrors {
	return h.cfgContainer.ConfigRef().RPC.Registry.Errors
}

// callRPCRegistry checks the connection against the method and calls its gRPC target.
func (h *Handler) callRPCRegistry(c Client, e centrifuge.RPCEvent, m *rpcregistry.Method) (centrifuge.RPCReply, error) {
	errs := h.registryErrors()
	// Anonymous methods are open to everyone. pending_2fa and bot methods are checked
	// like authenticated ones for now: the class claims that tell them apart come with
	// the auth upgrade work, so they are not invented here.
	if m.AuthClass != rpcregistry.AuthClassAnonymous && c.UserID() == "" {
		return centrifuge.RPCReply{}, registryError(errs.AuthClass)
	}
	if len(e.Data) > m.MaxRequestBytes {
		return centrifuge.RPCReply{}, registryError(errs.RequestTooLarge)
	}
	var token string
	if m.AuthClass != rpcregistry.AuthClassAnonymous {
		token = connectionToken(c)
	}
	data, _, err := proxy.CallRegistryMethod(c.Context(), m, proxy.RegistryCall{
		ClientID: c.ID(),
		Token:    token,
		Data:     e.Data,
		Headers:  e.Call.Headers,
		MsgID:    e.Call.MsgID,
	})
	if err != nil {
		return centrifuge.RPCReply{}, registryCallError(err, errs)
	}
	return centrifuge.RPCReply{Data: data}, nil
}

// registryCallError maps an error of a registry call to a protocol error. This is the one place
// where the mapping lives, so the full status table can replace it.
func registryCallError(err error, errs configtypes.RPCRegistryErrors) error {
	if errors.Is(err, proxy.ErrRPCResponseTooLarge) {
		return registryError(errs.RequestTooLarge)
	}
	st := status.Convert(err)
	temporary := st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded
	return &centrifuge.Error{
		Code:      registryBackendErrorCode,
		Message:   st.Message(),
		Temporary: temporary,
	}
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
