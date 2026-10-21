package client

import (
	"sync"
	"sync/atomic"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/ratelimit"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"

	"github.com/centrifugal/centrifuge"
)

// storageKeyConnState carries the record of a connection from OnClientConnecting to OnConnect. OnConnect removes it.
const storageKeyConnState = "conn_state"

// rebindableClient is a Client whose connection state and channels the handler changes: the rebind of a refresh and
// the session index and rate limit state of a connection. Core's client satisfies it. A Client without these methods
// is only used by requests which do not change the connection.
type rebindableClient interface {
	Client
	Disconnect(disconnect ...centrifuge.Disconnect)
	BeginDisconnect(disconnect centrifuge.Disconnect) bool
	Closing() bool
	ExtState() any
	SetExtState(v any)
	OnDisconnectIfConnected(h centrifuge.DisconnectHandler) bool
	Rebind(opts centrifuge.RebindOptions) error
	Subscribe(channel string, opts ...centrifuge.SubscribeOption) error
	ChannelsWithContext() map[string]centrifuge.ChannelContext
}

// connState is the record the handler keeps for a connection, in its ext state. It exists only when a feature needs
// it, see Handler.needsConnState. Its fields are set once at connect; only the identity changes, through its pointer.
// The identity is written by connect, by a same-identity refresh, by a rebind, and by a refresh proxy. A refresh proxy
// writes a copy of the identity which keeps the origin, the account, the class and the token of the connection, and
// moves only its token identity, see moveProxyIdentity. The moves of a refresh proxy hold moveMu across the load, the
// store and the session index sync, so two of them of one connection apply in order.
type connState struct {
	track      accessrevoke.Tracked
	limits     *ratelimit.ConnState
	identity   atomic.Pointer[connIdentity]
	refreshing atomic.Bool
	moveMu     sync.Mutex
}

// connIdentity is the identity of a connection: what its token proves and the user it is bound to. It is immutable
// once stored. Every path which gives a connection an identity builds one with nextIdentity.
type connIdentity struct {
	origin    connOrigin
	class     rpcregistry.AuthClass
	claim     string
	token     string
	tokenUser string
	account   string
	identity  centrifuge.TokenIdentity
}

// connOrigin tells how a connection was made. It never changes.
type connOrigin uint8

const (
	// originAnonymous is a connection with no token and no connect proxy.
	originAnonymous connOrigin = iota
	// originToken is a connection made with a connect token.
	originToken
	// originProxy is a connection made through a connect proxy.
	originProxy
)

// connStateOf returns the record of the connection, nil when the connection has none.
func connStateOf(c rebindableClient) *connState {
	st, _ := c.ExtState().(*connState)
	return st
}

// rateLimitStateOf returns the rate limit state of the connection, nil when the connection has none.
func rateLimitStateOf(c rebindableClient) *ratelimit.ConnState {
	st := connStateOf(c)
	if st == nil {
		return nil
	}
	return st.limits
}

// AccessIdentity implements accessrevoke.Identified. It returns the user and the token identity of the connection, which
// the revoker, the recheck and the catch-up sweep check it against.
func (s *connState) AccessIdentity() (string, centrifuge.TokenIdentity) {
	id := s.identity.Load()
	if id == nil {
		return "", centrifuge.TokenIdentity{}
	}
	return id.tokenUser, id.identity
}

// needsConnState reports whether a connection with userID and identity gets a record. A feature which reads the record
// needs one: rebind, the registry, the auth class claim, or a user or client limiter. The access store needs one for a
// connection with no user whose token has a session, see storeNeedsConnState. OnClientConnecting decides with it, from
// the credentials of the connection. OnConnect re-decides with clientNeedsConnState, which mirrors connectRecord, see
// trackConnect.
func (h *Handler) needsConnState(cfg *config.Config, userID string, identity centrifuge.TokenIdentity) bool {
	return h.configNeedsConnState(cfg) || h.storeNeedsConnState(userID, identity)
}

// configNeedsConnState reports whether a feature of the configuration needs a record for every connection.
func (h *Handler) configNeedsConnState(cfg *config.Config) bool {
	return cfg.Client.Rebind.Enabled ||
		cfg.RPC.Registry.AuthClassClaim != "" ||
		h.rpcRegistry.Enabled() ||
		h.userLimiter != nil ||
		h.clientLimiter != nil
}

// clientNeedsConnState reports whether a connecting client has a record, as needsConnState decided at connect. It reads
// the user and the token identity Core holds, which are the ones the connect credentials gave. The identity is read under
// the client's lock only for a connection with no user while the access store is set, so a connection with a user under
// the default config is decided without it. It mirrors connectRecord, which reads the same facts from the credentials,
// because Core copies credentials.TokenIdentity into the client at connect.
func (h *Handler) clientNeedsConnState(cfg *config.Config, client *centrifuge.Client) bool {
	if h.configNeedsConnState(cfg) {
		return true
	}
	if h.accessStore == nil || client.UserID() != "" {
		return false
	}
	return h.storeNeedsConnState("", client.TokenIdentity())
}

// adoptClientRecord adopts the record of a connecting client into its ext state, when clientNeedsConnState says the
// client has one, and returns it. Without one nothing is read from storage. The record is a single assignment, so the
// closures of trackConnect capture it by value and it does not escape on the default path.
func (h *Handler) adoptClientRecord(client *centrifuge.Client) *connState {
	if !h.clientNeedsConnState(h.cfgContainer.ConfigRef(), client) {
		return nil
	}
	return adoptConnState(client)
}

// storeNeedsConnState reports whether the access store needs a record for a connection with userID and identity: a
// connection with no user whose token has a session. The session index holds only such connections. A connection with
// no user and no session at connect has none; a refresh which gives it a session indexes it by its sessionTrack, see
// session_track.go.
func (h *Handler) storeNeedsConnState(userID string, identity centrifuge.TokenIdentity) bool {
	return h.accessStore != nil && userID == "" && identity.Session != ([16]byte{})
}

// keptToken returns the token a connection keeps for the RPC method registry. The token is kept only when the registry
// is enabled, as nothing else reads it.
func (h *Handler) keptToken(token string) string {
	if h.rpcRegistry.Enabled() {
		return token
	}
	return ""
}

// replyIdentityOn reports whether the connect and refresh replies carry the user and the auth class of the connection.
// Only the features which act on the identity turn it on, so the replies of a default connection stay as upstream
// sends them. Each of these features also has a record, see needsConnState.
func (h *Handler) replyIdentityOn(cfg *config.Config) bool {
	return cfg.Client.Rebind.Enabled || h.rpcRegistry.Enabled() || cfg.RPC.Registry.AuthClassClaim != ""
}

// setCoreCredentials sets the user and the info Core gets for a connection of class made with credentials. A pending
// connection is anonymous to Core and carries no info: the token of an upgrade sets the info.
func setCoreCredentials(credentials *centrifuge.Credentials, class rpcregistry.AuthClass, userID string) {
	credentials.UserID = coreUserID(class, userID)
	if class == rpcregistry.AuthClassPending2FA {
		credentials.Info = nil
	}
}

// recordIdentityOf returns the user and the token identity of a connection held in its record. ok is false when the
// connection has no record.
func recordIdentityOf(c Client) (userID string, identity centrifuge.TokenIdentity, ok bool) {
	rc, isRebindable := c.(rebindableClient)
	if !isRebindable {
		return "", centrifuge.TokenIdentity{}, false
	}
	rec := connStateOf(rc)
	if rec == nil {
		return "", centrifuge.TokenIdentity{}, false
	}
	userID, identity = rec.AccessIdentity()
	return userID, identity, true
}

// tokenUserOf returns the user the token of a connection was issued for. Core sees no user for a pending connection, so
// the record is read first. Without a record it is the user Core holds.
func tokenUserOf(c Client) string {
	if userID, _, ok := recordIdentityOf(c); ok {
		return userID
	}
	return c.UserID()
}

// connectOriginOf returns how a connection is made: with a connect token, through the connect proxy, or anonymously.
func connectOriginOf(token string, proxied bool) connOrigin {
	if token != "" {
		return originToken
	}
	if proxied {
		return originProxy
	}
	return originAnonymous
}

// connectRecord puts the record of a connecting client into storage, when needsConnState asks for one, and returns it.
// The origin token takes its identity from verified, the connect token. The origin proxy takes its user from credentials
// and has no token.
func (h *Handler) connectRecord(
	cfg *config.Config,
	storage map[string]any,
	origin connOrigin,
	token string,
	verified jwtverify.ConnectToken,
	credentials *centrifuge.Credentials,
) *connState {
	var userID string
	var identity centrifuge.TokenIdentity
	if credentials != nil {
		userID = credentials.UserID
		identity = credentials.TokenIdentity
	}
	if !h.needsConnState(cfg, userID, identity) {
		return nil
	}
	rec := &connState{}
	if h.clientLimiter != nil {
		rec.limits = h.clientLimiter.NewConnState()
	}
	var class rpcregistry.AuthClass
	var claim string
	switch origin {
	case originToken:
		class, _ = rpcregistry.ConnectionAuthClass(verified.AuthClass, verified.UserID)
		claim = cfg.RPC.Registry.AuthClassClaim
	case originProxy:
		if credentials != nil {
			verified.UserID = credentials.UserID
		}
	}
	rec.identity.Store(nextIdentity(nil, origin, class, claim, h.keptToken(token), verified))
	storage[storageKeyConnState] = rec
	return rec
}

// adoptConnState moves the record of a connecting client from its storage into its ext state. Core applied the storage
// of the connect reply before OnConnect. Without a record nothing is touched.
func adoptConnState(client *centrifuge.Client) *connState {
	storage, release := client.AcquireStorage()
	rec, _ := storage[storageKeyConnState].(*connState)
	if rec == nil {
		release(storage)
		return nil
	}
	delete(storage, storageKeyConnState)
	if len(storage) == 0 {
		release(nil) // the map was made for the hop only; Core recreates one on demand
	} else {
		release(storage)
	}
	client.SetExtState(rec)
	return rec
}

// nextIdentity builds the identity of a connection from the token it holds now. prev is nil at connect. The account is
// the user of the token when it has one, else the account held before, so a logout keeps the account and a later token
// of another user is refused by the account check. The origin never changes.
func nextIdentity(
	prev *connIdentity,
	origin connOrigin,
	class rpcregistry.AuthClass,
	claim string,
	token string,
	verified jwtverify.ConnectToken,
) *connIdentity {
	next := &connIdentity{
		origin:    origin,
		class:     class,
		claim:     claim,
		token:     token,
		tokenUser: verified.UserID,
		account:   verified.UserID,
		identity:  tokenIdentityOf(verified),
	}
	if prev != nil {
		next.origin = prev.origin
		if next.account == "" {
			next.account = prev.account
		}
	}
	return next
}

// movedIdentity returns the identity a refresh proxy moves a connection to: a copy of prev with identity as its token
// identity. The user, the account, the origin, the class and the token stay as prev holds them, see moveProxyIdentity.
// It lives next to nextIdentity, so the rules which keep those fields are in one place.
func movedIdentity(prev *connIdentity, identity centrifuge.TokenIdentity) *connIdentity {
	next := *prev
	next.identity = identity
	return &next
}

// tokenIdentityOf returns the identity of a connection which holds the verified token. Every path which gives a
// connection the identity of its token builds it here, so the reply to the client and the record agree.
func tokenIdentityOf(token jwtverify.ConnectToken) centrifuge.TokenIdentity {
	return accessrevoke.Identity(token.Session, token.Generation, token.IssuedAt)
}

// effectiveClass returns the auth class of a connection for the registry under claim, the claim configured now. A class
// derived from a token holds only under the claim it was derived under, and is anonymous under another one. Without a
// token-derived class the class is anonymous when a claim is configured, else the class of the Core user. A connection
// which an anonymous origin rebound to a token has the class of that token, so the class decides, not the origin.
func effectiveClass(id *connIdentity, claim string, coreUser string) rpcregistry.AuthClass {
	if id.class != 0 {
		if id.claim == claim {
			return id.class
		}
		return rpcregistry.AuthClassAnonymous
	}
	if claim != "" {
		return rpcregistry.AuthClassAnonymous
	}
	class, _ := rpcregistry.ConnectionAuthClass("", coreUser)
	return class
}

// coreUserID returns the user Core sees for a connection of class with userID. A pending_2fa connection keeps
// its user for the registry, but is anonymous to Core: it gets no personal channel, no user channels and no
// entry in the user index of the hub. Any other class gives its user.
func coreUserID(class rpcregistry.AuthClass, userID string) string {
	if class == rpcregistry.AuthClassPending2FA {
		return ""
	}
	return userID
}

// setReplyIdentity sets the user and the auth class which a connect reply reports, when replyIdentityOn, from the record
// of the connection. The user is the one of the token or of the connect proxy, which Core may not see for a pending
// connection. Each feature which turns the identity on has a record, see needsConnState.
func (h *Handler) setReplyIdentity(cfg *config.Config, reply *centrifuge.ConnectReply, rec *connState) {
	if rec == nil || !h.replyIdentityOn(cfg) {
		return
	}
	id := rec.identity.Load()
	class := id.class
	if id.origin != originToken {
		class = effectiveClass(id, h.registryConfig().AuthClassClaim, id.tokenUser)
	}
	reply.User = id.tokenUser
	reply.AuthClass = h.authClassName(class)
}
