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

// rpcDedupState is the exactly-once RPC state of a handler: the store and the metrics of its outcomes. A handler
// has one when the dedup is on, and none otherwise.
type rpcDedupState struct {
	store   rpcdedup.Store
	metrics *metrics.RPCRegistryDedupMetrics
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
	store := rpcdedup.NewMemory(rpcdedup.MemoryOptions{
		MaxEntries:         dedup.LocalMaxEntries,
		MaxBytes:           int64(dedup.LocalMaxBytes),
		MaxRecordsPerScope: dedup.MaxRecordsPerScope,
		TTL:                dedup.Window.ToDuration(),
		MaxReplyBytes:      dedup.MaxReplySize,
	})
	return &rpcDedupState{store: store, metrics: metrics.NewRPCRegistryDedupMetrics()}
}

// rpcDedupScope returns the dedup scope of a connection, from its user, its token identity and its storage.
// A connection with a token session is scoped to the session of its user. Otherwise it is scoped to its install
// instance, stored at connect, and its user. The user is part of both, so an instance seen on one account cannot
// replay calls of another. A connection with neither has no scope, and its calls are not deduplicated. The
// caller reads the user and the identity before acquiring the storage, which the client accessors may lock.
func rpcDedupScope(userID string, identity centrifuge.TokenIdentity, storage map[string]any) string {
	if identity.Session != ([16]byte{}) {
		return rpcdedup.ScopeOf("s", userID, identity.Session[:])
	}
	var instance string
	instance, _ = storage[clientstorage.KeyInstance].(string)
	if instance != "" {
		return rpcdedup.ScopeOf("i", userID, []byte(instance))
	}
	return ""
}

// storeRPCInstance stores the install instance of a connection, when the dedup is on and the instance is
// valid. An invalid instance is dropped with a debug log, which never shows the value.
func (h *Handler) storeRPCInstance(storage map[string]any, instance string) {
	if h.rpcDedup == nil || instance == "" {
		return
	}
	if !rpcdedup.ValidInstance(instance) {
		log.Debug().Msg("dropped invalid connect instance, the connection has no dedup scope from it")
		return
	}
	storage[clientstorage.KeyInstance] = instance
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

// rpcTrackCall returns the dedup state of a call. It is nil when the dedup is off, or the call carries nothing
// to track. The connection storage is acquired once, to create the in-flight map and read the scope.
func (h *Handler) rpcTrackCall(c Client, call centrifuge.RPCCallOptions) *rpcTrack {
	if h.rpcDedup == nil {
		return nil
	}
	if call.MsgID == 0 && call.Cancel == 0 && len(call.Ack) == 0 {
		return nil
	}
	userID := c.UserID()
	identity := c.TokenIdentity()
	var storage map[string]any
	var release func(map[string]any)
	storage, release = c.AcquireStorage()
	defer release(storage)
	var inflight *rpcdedup.InFlight
	inflight, _ = storage[rpcInflightKey].(*rpcdedup.InFlight)
	if inflight == nil {
		inflight = rpcdedup.NewInFlight()
		storage[rpcInflightKey] = inflight
	}
	return &rpcTrack{scope: rpcDedupScope(userID, identity, storage), inflight: inflight}
}

// rpcAck forgets the done records which a call acknowledges: the client has their replies.
func (h *Handler) rpcAck(track *rpcTrack, acks []uint64) {
	if track == nil || track.scope == "" || len(acks) == 0 {
		return
	}
	if err := h.rpcDedup.store.Ack(track.scope, acks); err != nil {
		log.Error().Err(err).Msg("error acknowledging exactly-once RPC records")
	}
	h.rpcDedup.metrics.SetEntries(h.rpcDedup.store.Len())
}

// cancelRPCRegistry cancels the call msgID of the connection and of its scope. The cancel command has no reply
// of its own, so it answers an empty reply. A msg_id outside the window is refused, and nothing is recorded for it.
func (h *Handler) cancelRPCRegistry(track *rpcTrack, msgID uint64) (centrifuge.RPCReply, error) {
	cfg := h.registryConfig()
	if !rpcdedup.MsgIDInWindow(msgID, time.Now(), cfg.Dedup.Window.ToDuration(), cfg.Dedup.MaxSkew.ToDuration()) {
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupMsgIDInvalid)
		return centrifuge.RPCReply{}, registryError(cfg.Errors.MsgIDInvalid)
	}
	h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupCancelled)
	track.inflight.Cancel(msgID)
	if track.scope != "" {
		failure := registryErrorValue(cfg.Errors.Cancelled)
		if err := h.rpcDedup.store.Cancel(rpcdedup.Key{Scope: track.scope, MsgID: msgID}, failure); err != nil {
			log.Error().Err(err).Msg("error cancelling exactly-once RPC call")
		}
		h.rpcDedup.metrics.SetEntries(h.rpcDedup.store.Len())
	}
	return centrifuge.RPCReply{}, nil
}

// callRPCTracked runs a call which carries a msg_id, with the dedup on. The msg_id is checked first. The call can
// be cancelled by a cancel command of its msg_id, on its connection or in its scope. A non-idempotent call in a
// scope is recorded, and its context is not cancelled by a disconnect, so its backend commit runs on. Any other
// call is cancelled by a disconnect, as it always was.
func (h *Handler) callRPCTracked(c Client, e centrifuge.RPCEvent, m *rpcregistry.Method, cfg configtypes.RPCRegistry, connToken string, track *rpcTrack) (centrifuge.RPCReply, error) {
	msgID := e.Call.MsgID
	if !rpcdedup.MsgIDInWindow(msgID, time.Now(), cfg.Dedup.Window.ToDuration(), cfg.Dedup.MaxSkew.ToDuration()) {
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupMsgIDInvalid)
		msgIDErr := registryError(cfg.Errors.MsgIDInvalid)
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
	release := track.inflight.Add(msgID, cancel)
	defer release()
	return h.callRPCRecorded(ctx, cancel, c, e, m, cfg, connToken, track.scope)
}

// callRPCRecorded runs a non-idempotent call in a scope through the dedup store. The call is a miss, a duplicate
// of a done call, or a duplicate of a call in flight on this node. A duplicate with another fingerprint is refused.
func (h *Handler) callRPCRecorded(ctx context.Context, cancel context.CancelFunc, c Client, e centrifuge.RPCEvent, m *rpcregistry.Method, cfg configtypes.RPCRegistry, connToken string, scope string) (centrifuge.RPCReply, error) {
	key := rpcdedup.Key{Scope: scope, MsgID: e.Call.MsgID}
	flight := rpcdedup.NewFlightWithFingerprint(rpcdedup.Fingerprint(e.Method, e.Data))
	lookup, err := h.rpcDedup.store.Begin(key, flight)
	if err != nil {
		return h.rpcStoreFailure(ctx, c, e, m, cfg, connToken, err)
	}
	switch lookup.Outcome {
	case rpcdedup.OutcomeDone:
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupHitDone)
		return h.rpcRecordedReply(lookup.Record, cfg)
	case rpcdedup.OutcomeInFlight:
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupHitInFlightLocal)
		return h.rpcJoin(ctx, c, m, cfg, lookup.Flight)
	case rpcdedup.OutcomeMismatch:
		h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupMsgIDReused)
		reusedErr := registryError(cfg.Errors.MsgIDReused)
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, reusedErr)
	}
	h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupMiss)
	flight.Attach(cancel)
	// The owner always completes its flight, so its duplicates never wait on a call which ended without an answer.
	// A panic in the call leaves the flight abandoned, and the duplicates retry.
	completed := false
	defer func() {
		if !completed {
			h.completeRPCRecord(key, flight, rpcdedup.Record{Abandoned: true})
		}
	}()
	reply, callErr := h.invokeRPCRegistry(ctx, c, e, m, cfg, connToken)
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
	h.rpcDedup.metrics.SetEntries(h.rpcDedup.store.Len())
}

// rpcStoreFailure handles a failed Begin. With fail_open the call runs without dedup. With fail_closed it is
// answered with the temporary dedup_unavailable error, which clients retry.
func (h *Handler) rpcStoreFailure(ctx context.Context, c Client, e centrifuge.RPCEvent, m *rpcregistry.Method, cfg configtypes.RPCRegistry, connToken string, storeErr error) (centrifuge.RPCReply, error) {
	h.rpcDedup.metrics.Inc(metrics.RPCRegistryDedupStoreError)
	log.Error().Err(storeErr).Str("method", m.Name).Msg("exactly-once RPC store failed")
	if cfg.Dedup.OnStoreError == configtypes.RPCDedupFailOpen {
		return h.invokeRPCRegistry(ctx, c, e, m, cfg, connToken)
	}
	return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, registryTemporaryError(cfg.Errors.DedupUnavailable))
}

// rpcJoin waits for the call which a duplicate joined, and answers the duplicate with its record. The wait ends
// when the call is done, when the duplicate is cancelled, when its connection closes, or at the method timeout.
// A closed duplicate frees its slot at once.
func (h *Handler) rpcJoin(ctx context.Context, c Client, m *rpcregistry.Method, cfg configtypes.RPCRegistry, flight *rpcdedup.Flight) (centrifuge.RPCReply, error) {
	started := time.Now()
	timer := time.NewTimer(m.Timeout)
	defer timer.Stop()
	select {
	case <-flight.Done():
	case <-ctx.Done():
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultCancelled, registryError(cfg.Errors.Cancelled))
	case <-c.Context().Done():
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultCancelled, registryError(cfg.Errors.Cancelled))
	case <-timer.C:
		timeoutErr := registryTemporaryError(cfg.Errors.DedupUnavailable)
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultTimeout, timeoutErr)
	}
	h.rpcDedup.metrics.ObserveWait(time.Since(started).Seconds())
	rec := flight.Record()
	if rec.Abandoned {
		// The call did not answer, and was not kept: the duplicate retries.
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, registryTemporaryError(cfg.Errors.DedupUnavailable))
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

// registryTemporaryError returns the protocol error configured for a failure which clients retry. It is sent as
// temporary.
func registryTemporaryError(e configtypes.RPCRegistryError) *centrifuge.Error {
	protoErr := registryErrorValue(e)
	protoErr.Temporary = true
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
