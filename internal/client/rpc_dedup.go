package client

import (
	"context"
	"errors"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// rpcInflightKey is the connection storage key of the in-flight map of exactly-once RPC.
const rpcInflightKey = "rpc_inflight"

// rpcScopeKey is the connection storage key of the dedup scope of the connection. It is computed at connect, and
// again at refresh, from the user and the identity of the connection, and each call reads it.
const rpcScopeKey = "rpc_scope"

// rpcRetryMetadataKey is the metadata key of a protocol error which tells the client that its call was not executed
// and may be sent again. The server sets it itself: the x-centrifugo- prefix is reserved, and no allow-list may name
// a key with it, so a backend cannot set it.
const rpcRetryMetadataKey = "x-centrifugo-retry"

// The values of rpcRetryMetadataKey. rpcRetryNewMsgID means the msg_id of the call was refused: the call is sent
// again with a new msg_id, after the client re-syncs its clock. rpcRetryLater means the msg_id is still good: the
// same call is sent again after a delay.
const (
	rpcRetryNewMsgID = "new_msg_id"
	rpcRetryLater    = "later"
)

// rpcDedupState is the exactly-once RPC state of a handler: the store, the metrics of its outcomes, and the
// window and the skew of the msg_id check. The window and the skew are captured here, with the store, since the
// store keeps its records for the window: a reload of them must not widen the msg_ids which are accepted.
// joined, when set, is called each time a duplicate joins a call in flight on this node, before it waits. Tests set
// it to learn of a join. afterBackend, when set, is called by the owner of a dedup call once its backend returned,
// before the record is kept. Tests set it to fail after the backend committed.
type rpcDedupState struct {
	store        rpcdedup.Store
	metrics      *metrics.RPCRegistryDedupMetrics
	window       time.Duration
	maxSkew      time.Duration
	anonymous    bool
	joined       func()
	afterBackend func()
}

// newRPCDedupState returns the exactly-once RPC state of the config, or nil when the dedup is off. The state is
// created once, so a change of the dedup config takes effect after a restart.
func newRPCDedupState(cfgContainer *config.Container) *rpcDedupState {
	if cfgContainer == nil {
		return nil
	}
	dedup := cfgContainer.ConfigRef().RPC.Registry.Dedup
	if !dedup.Enabled {
		return nil
	}
	dedupMetrics := metrics.NewRPCRegistryDedupMetrics()
	store := rpcdedup.NewMemory(rpcdedup.MemoryOptions{
		MaxEntries:          dedup.LocalMaxEntries,
		MaxBytes:            int64(dedup.LocalMaxBytes),
		AnonymousMaxEntries: dedup.LocalAnonymousMaxEntries,
		AnonymousMaxBytes:   int64(dedup.LocalAnonymousMaxBytes),
		MaxRecordsPerScope:  dedup.MaxRecordsPerScope,
		ScopeReserve:        dedup.ScopeReserve,
		ScopeReserveBytes:   int64(dedup.ScopeReserveBytes),
		MaxScopesPerUser:    dedup.MaxScopesPerUser,
		TTL:                 rpcdedup.RecordTTL(dedup.Window.ToDuration(), dedup.MaxSkew.ToDuration()),
		MaxReplyBytes:       dedup.MaxReplySize,
		Observer:            dedupMetrics,
	})
	return &rpcDedupState{
		store:     store,
		metrics:   dedupMetrics,
		window:    dedup.Window.ToDuration(),
		maxSkew:   dedup.MaxSkew.ToDuration(),
		anonymous: dedup.LocalAnonymousMaxEntries > 0,
	}
}

// rpcDedupScope returns the dedup scope of a connection, from its user, its token identity and its storage. A
// connection with a user is scoped to its token session, or else to its install instance, and the user is part of
// both, so an instance seen on one account cannot replay calls of another. An anonymous connection has no user, and
// is scoped to its install instance only. A connection with neither has no scope, and its calls are not
// deduplicated. The caller reads the user and the identity before acquiring the storage, which the client accessors
// may lock.
func rpcDedupScope(userID string, identity centrifuge.TokenIdentity, storage map[string]any) string {
	var instance string
	instance, _ = storage[clientstorage.KeyInstance].(string)
	if userID == "" {
		if instance == "" {
			return ""
		}
		return rpcdedup.ScopeOf("a", "", []byte(instance))
	}
	if identity.Session != ([16]byte{}) {
		return rpcdedup.ScopeOf("s", userID, identity.Session[:])
	}
	if instance != "" {
		return rpcdedup.ScopeOf("i", userID, []byte(instance))
	}
	return ""
}

// rpcConnectionScope returns the dedup scope of a connection. An anonymous connection has one only when the store
// keeps the scopes of anonymous connections.
func (h *Handler) rpcConnectionScope(userID string, identity centrifuge.TokenIdentity, storage map[string]any) string {
	if userID == "" && !h.rpcDedup.anonymous {
		return ""
	}
	return rpcDedupScope(userID, identity, storage)
}

// storeRPCConnection stores the install instance of a connection, when the dedup is on and the instance is valid,
// and the dedup scope of the connection, computed from its credentials. An invalid instance is dropped with a debug
// log, which never shows the value.
func (h *Handler) storeRPCConnection(storage map[string]any, instance string, credentials *centrifuge.Credentials) {
	if h.rpcDedup == nil {
		return
	}
	if instance != "" {
		if rpcdedup.ValidInstance(instance) {
			storage[clientstorage.KeyInstance] = instance
		} else {
			log.Debug().Msg("dropped invalid connect instance, the connection has no dedup scope from it")
		}
	}
	var userID string
	var identity centrifuge.TokenIdentity
	if credentials != nil {
		userID = credentials.UserID
		identity = credentials.TokenIdentity
	}
	storage[rpcScopeKey] = h.rpcConnectionScope(userID, identity, storage)
}

// refreshRPCScope computes the dedup scope of a connection again after its token is refreshed, since the session
// of the identity may change. The user is the same, since a refresh of another user is refused.
func (h *Handler) refreshRPCScope(c Client, identity centrifuge.TokenIdentity) {
	if h.rpcDedup == nil {
		return
	}
	userID := c.UserID()
	storage, release := c.AcquireStorage()
	defer release(storage)
	storage[rpcScopeKey] = h.rpcConnectionScope(userID, identity, storage)
}

// rpcLimit returns the concurrency limit of an OnRPC call. With the dedup on, a cancel command does not wait for
// the concurrency semaphore: it would queue behind the calls it cancels.
func (h *Handler) rpcLimit(event centrifuge.RPCEvent, concurrency int) int {
	if h.rpcDedup != nil && event.Call.Cancel != 0 {
		return 0
	}
	return concurrency
}

// rpcTrack is the dedup state of a call which carries a msg_id, a cancel or acks: the scope the call runs in, and
// the calls with a msg_id which run on its connection.
type rpcTrack struct {
	scope    string
	inflight *rpcdedup.InFlight
}

// rpcTrackCall returns the dedup state of a call. It is nil when the dedup is off, or the call carries nothing to
// track. The connection storage is acquired once, to create the in-flight map and to read the scope. The user and
// the identity are read only when a connection has no scope stored yet.
func (h *Handler) rpcTrackCall(c Client, call centrifuge.RPCCallOptions) *rpcTrack {
	if h.rpcDedup == nil {
		return nil
	}
	if call.MsgID == 0 && call.Cancel == 0 && len(call.Ack) == 0 {
		return nil
	}
	storage, release := c.AcquireStorage()
	var inflight *rpcdedup.InFlight
	inflight, _ = storage[rpcInflightKey].(*rpcdedup.InFlight)
	if inflight == nil {
		inflight = rpcdedup.NewInFlight()
		storage[rpcInflightKey] = inflight
	}
	scope, found := storage[rpcScopeKey].(string)
	release(storage)
	if !found {
		scope = h.storeMissingRPCScope(c)
	}
	return &rpcTrack{scope: scope, inflight: inflight}
}

// storeMissingRPCScope computes the dedup scope of a connection which has none stored, and stores it. The user and
// the identity are read before the storage is acquired, which the client accessors may lock. A scope stored
// meanwhile, by a refresh, is kept.
func (h *Handler) storeMissingRPCScope(c Client) string {
	userID := c.UserID()
	identity := c.TokenIdentity()
	storage, release := c.AcquireStorage()
	defer release(storage)
	stored, found := storage[rpcScopeKey].(string)
	if found {
		return stored
	}
	scope := h.rpcConnectionScope(userID, identity, storage)
	storage[rpcScopeKey] = scope
	return scope
}

// rpcAck forgets the done records which a call acknowledges: the client has their replies.
func (h *Handler) rpcAck(track *rpcTrack, acks []uint64) {
	if track == nil || track.scope == "" || len(acks) == 0 {
		return
	}
	if err := h.rpcDedup.store.Ack(track.scope, acks); err != nil {
		log.Error().Err(err).Msg("error acknowledging exactly-once RPC records")
	}
	h.observeDedupStore()
}

// cancelRPCRegistry cancels the call msgID of the connection and of its scope. The cancel command has no reply
// of its own, so it answers an empty reply. A msg_id outside the window is refused, and nothing is recorded for it.
// A cancel of a scope which the store does not admit records nothing, and is counted as refused.
func (h *Handler) cancelRPCRegistry(track *rpcTrack, msgID uint64) (centrifuge.RPCReply, error) {
	cfg := h.registryConfig()
	if !rpcdedup.MsgIDInWindow(msgID, time.Now(), h.rpcDedup.window, h.rpcDedup.maxSkew) {
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupMsgIDInvalid)
		return centrifuge.RPCReply{}, registryError(cfg.Errors.MsgIDInvalid)
	}
	h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupCancelled)
	track.inflight.Cancel(msgID)
	if track.scope != "" {
		failure := registryErrorValue(cfg.Errors.Cancelled)
		if err := h.rpcDedup.store.Cancel(rpcdedup.Key{Scope: track.scope, MsgID: msgID}, failure); err != nil {
			h.storeCancelFailure(err)
		}
		h.observeDedupStore()
	}
	return centrifuge.RPCReply{}, nil
}

// callRPCTracked runs a call which carries a msg_id, with the dedup on. The msg_id is checked first. The call can
// be cancelled by a cancel command of its msg_id, on its connection or in its scope. A non-idempotent call in a
// scope is recorded, and its context is not cancelled by a disconnect, so its backend commit runs on. Any other
// call is cancelled by a disconnect, as it always was.
func (h *Handler) callRPCTracked(c Client, e centrifuge.RPCEvent, m *rpcregistry.Method, cfg configtypes.RPCRegistry, connToken string, track *rpcTrack) (centrifuge.RPCReply, error) {
	msgID := e.Call.MsgID
	if !rpcdedup.MsgIDInWindow(msgID, time.Now(), h.rpcDedup.window, h.rpcDedup.maxSkew) {
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupMsgIDInvalid)
		// A retransmit may be a call which ran, and whose record is outside the window: its msg_id is refused, and
		// the client must not send the call again with a new one. Only a call refused on its first transmission gets
		// the signal.
		retry := rpcRetryNewMsgID
		if e.Call.Retransmit {
			retry = ""
		}
		msgIDErr := registryRetryError(cfg.Errors.MsgIDInvalid, false, retry)
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, msgIDErr)
	}
	if m.Idempotent || track.scope == "" {
		if !m.Idempotent {
			h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupNoScope)
		}
		ctx, cancel := context.WithCancel(c.Context())
		defer cancel()
		release := track.inflight.Add(msgID, cancel)
		defer release()
		return h.invokeRPCRegistry(ctx, c, e, m, cfg, connToken)
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(c.Context()))
	defer cancel()
	return h.callRPCRecorded(ctx, cancel, c, e, m, cfg, connToken, track)
}

// callRPCRecorded runs a non-idempotent call in a scope through the dedup store. The call is a miss, a duplicate
// of a done call, or a duplicate of a call in flight on this node. A duplicate with another fingerprint is refused.
// Only the owner of a miss is registered in the in-flight map of its connection: a duplicate joins the call of the
// owner, and a cancel of the msg_id reaches the owner, which then answers the duplicate too.
func (h *Handler) callRPCRecorded(ctx context.Context, cancel context.CancelFunc, c Client, e centrifuge.RPCEvent, m *rpcregistry.Method, cfg configtypes.RPCRegistry, connToken string, track *rpcTrack) (centrifuge.RPCReply, error) {
	key := rpcdedup.Key{Scope: track.scope, MsgID: e.Call.MsgID}
	flight := rpcdedup.NewFlightWithFingerprint(rpcdedup.Fingerprint(e.Method, e.Data))
	lookup, err := h.rpcDedup.store.Begin(key, flight)
	if err != nil {
		return h.rpcStoreFailure(ctx, cancel, c, e, m, cfg, connToken, track, err)
	}
	switch lookup.Outcome {
	case rpcdedup.OutcomeDone:
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupHitDone)
		return h.rpcRecordedReply(lookup.Record, cfg)
	case rpcdedup.OutcomeInFlight:
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupHitInFlightLocal)
		if h.rpcDedup.joined != nil {
			h.rpcDedup.joined()
		}
		return h.rpcJoin(c, m, cfg, lookup.Flight)
	case rpcdedup.OutcomeMismatch:
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupMsgIDReused)
		reusedErr := registryError(cfg.Errors.MsgIDReused)
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, reusedErr)
	}
	h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupMiss)
	flight.Attach(cancel)
	release := track.inflight.Add(e.Call.MsgID, cancel)
	defer release()
	// The owner always completes its flight, so its duplicates never wait on a call which ended without an answer.
	// A panic in the call leaves the flight abandoned, with no record: the call may have run, so the duplicates get
	// the error without the retry signal.
	completed := false
	defer func() {
		if !completed {
			h.completeRPCRecord(key, flight, rpcdedup.Record{Abandoned: true})
		}
	}()
	reply, callErr := h.invokeRPCRegistry(ctx, c, e, m, cfg, connToken)
	if h.rpcDedup.afterBackend != nil {
		h.rpcDedup.afterBackend()
	}
	completed = true
	h.completeRPCRecord(key, flight, rpcRecordOf(reply, callErr))
	return reply, callErr
}

// completeRPCRecord keeps the record of the call which flight owns, and wakes its duplicates.
func (h *Handler) completeRPCRecord(key rpcdedup.Key, flight *rpcdedup.Flight, rec rpcdedup.Record) {
	if err := h.rpcDedup.store.Complete(key, flight, rec); err != nil {
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupStoreError)
		log.Error().Err(err).Msg("error keeping exactly-once RPC record")
	}
	h.observeDedupStore()
}

// rpcStoreFailure handles a failed Begin. A refusal of the scope is counted by its reason, and any other failure
// is counted as a store error and logged. With fail_open the call runs without dedup, and stays in the in-flight
// map of its connection, so a cancel reaches it. A call with the msg_id of a call which runs on the connection is
// not run again: it is answered with the temporary dedup_unavailable error, as the store would not know it. With
// fail_closed the call is answered with the same error, which carries the later retry signal, and it is not run.
func (h *Handler) rpcStoreFailure(ctx context.Context, cancel context.CancelFunc, c Client, e centrifuge.RPCEvent, m *rpcregistry.Method, cfg configtypes.RPCRegistry, connToken string, track *rpcTrack, storeErr error) (centrifuge.RPCReply, error) {
	var refused *rpcdedup.RefusedError
	if errors.As(storeErr, &refused) {
		h.countRefused(refused)
	} else {
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupStoreError)
		log.Error().Err(storeErr).Str("method", m.Name).Msg("exactly-once RPC store failed")
	}
	if cfg.Dedup.OnStoreError == configtypes.RPCDedupFailOpen {
		release, admitted := track.inflight.TryAdd(e.Call.MsgID, cancel)
		if !admitted {
			return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, registryRetryError(cfg.Errors.DedupUnavailable, true, ""))
		}
		defer release()
		return h.invokeRPCRegistry(ctx, c, e, m, cfg, connToken)
	}
	return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, registryRetryError(cfg.Errors.DedupUnavailable, true, rpcRetryLater))
}

// storeCancelFailure handles a failed Cancel of the store. A refusal is counted and records nothing. Any other failure
// is logged.
func (h *Handler) storeCancelFailure(err error) {
	var refused *rpcdedup.RefusedError
	if errors.As(err, &refused) {
		h.countRefused(refused)
		return
	}
	log.Error().Err(err).Msg("error cancelling exactly-once RPC call")
}

// countRefused counts a scope which the store refused, by the limit which refused it.
func (h *Handler) countRefused(refused *rpcdedup.RefusedError) {
	if refused.Refusal == rpcdedup.RefusalUser {
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupRefusedUser)
		return
	}
	h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupRefusedPool)
}

// observeDedupStore sets the gauges of the store: its records, its admitted scopes and its bytes, per pool.
func (h *Handler) observeDedupStore() {
	h.rpcDedup.metrics.SetStore(h.rpcDedup.store.Stats())
}

// rpcJoin waits for the call which a duplicate joined, and answers the duplicate with its record. The wait ends
// when the call is done, when the connection of the duplicate closes, or at the method timeout. A cancel of the
// duplicate's msg_id does not end the wait: the store cancels the call of its owner, which then completes, and the
// duplicate is answered with the owner's record. A closed duplicate frees its slot at once. A duplicate is not
// registered in the in-flight map of its connection.
func (h *Handler) rpcJoin(c Client, m *rpcregistry.Method, cfg configtypes.RPCRegistry, flight *rpcdedup.Flight) (centrifuge.RPCReply, error) {
	started := time.Now()
	timer := time.NewTimer(m.Timeout)
	defer timer.Stop()
	select {
	case <-flight.Done():
	case <-c.Context().Done():
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultCancelled, registryError(cfg.Errors.Cancelled))
	case <-timer.C:
		timeoutErr := registryRetryError(cfg.Errors.DedupUnavailable, true, rpcRetryLater)
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultTimeout, timeoutErr)
	}
	h.rpcDedup.metrics.ObserveWait(time.Since(started).Seconds())
	rec := flight.Record()
	if rec.Abandoned {
		// The call did not answer, and was not kept. It may have run, so no retry signal is sent: the app decides.
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, registryRetryError(cfg.Errors.DedupUnavailable, true, ""))
	}
	return h.rpcRecordedReply(rec, cfg)
}

// rpcRecordedReply answers a duplicate from the record of its call. A reply which was not kept is evicted, and
// the call is not run again.
func (h *Handler) rpcRecordedReply(rec rpcdedup.Record, cfg configtypes.RPCRegistry) (centrifuge.RPCReply, error) {
	if rec.Uncached {
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupEvicted)
		return centrifuge.RPCReply{}, registryError(cfg.Errors.ResultEvicted)
	}
	if rec.Err != nil {
		return centrifuge.RPCReply{Cached: true}, rec.Err
	}
	return centrifuge.RPCReply{Data: rec.Reply, Cached: true}, nil
}

// registryRetryError is the protocol error of a dedup failure, sent as temporary or not. A non-empty retry is the
// value of the retry signal in its metadata: the client sends the call again after it. An empty retry gives no
// signal, for a call which runs on its connection, which a resend would run again. An error which carries the signal
// is never recorded: it is answered before Begin, by a failed Begin, or to a duplicate, and a record only holds the
// outcome of a call which ran. So a retransmit never replays the signal.
func registryRetryError(e configtypes.RPCRegistryError, temporary bool, retry string) *centrifuge.Error {
	protoErr := registryErrorValue(e)
	protoErr.Temporary = temporary
	if retry != "" {
		protoErr.Metadata = map[string]string{rpcRetryMetadataKey: retry}
	}
	return protoErr
}

// rpcRecordOf returns the final record of a call which ran: its reply, or its error. A disconnect is final too:
// the backend answered, and a retransmit must not run the call again. The record holds the disconnect as a
// protocol error, with its code and reason.
func rpcRecordOf(reply centrifuge.RPCReply, err error) rpcdedup.Record {
	if err == nil {
		return rpcdedup.Record{Reply: reply.Data}
	}
	var protoErr *centrifuge.Error
	if errors.As(err, &protoErr) {
		return rpcdedup.Record{Err: protoErr}
	}
	var disconnect *centrifuge.Disconnect
	if errors.As(err, &disconnect) {
		return rpcdedup.Record{Err: &centrifuge.Error{Code: disconnect.Code, Message: disconnect.Reason}}
	}
	return rpcdedup.Record{Err: centrifuge.ErrorInternal}
}
