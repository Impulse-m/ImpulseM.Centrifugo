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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newRPCDedupStore returns the store of exactly-once RPC, or nil when the dedup is off. The store is
// created once, so a change of the dedup config takes effect after a restart.
func newRPCDedupStore(cfgContainer *config.Container) rpcdedup.Store {
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
	return store
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

// rpcTrack is the dedup state of a call which carries a msg_id, a cancel or acks: the scope the call runs in,
// and the calls with a msg_id which run on its connection.
type rpcTrack struct {
	scope    string
	inflight *rpcdedup.InFlight
}

// rpcTrackCall returns the dedup state of a call. It is nil when the dedup is off, or the call carries
// nothing to track. The connection storage is acquired once, to create the in-flight map and read the scope.
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
	inflight, _ = storage[clientstorage.KeyRPCInflight].(*rpcdedup.InFlight)
	if inflight == nil {
		inflight = rpcdedup.NewInFlight()
		storage[clientstorage.KeyRPCInflight] = inflight
	}
	return &rpcTrack{scope: rpcDedupScope(userID, identity, storage), inflight: inflight}
}

// rpcAck forgets the done records which a call acknowledges: the client has their replies.
func (h *Handler) rpcAck(track *rpcTrack, acks []uint64) {
	if track == nil || track.scope == "" || len(acks) == 0 {
		return
	}
	if err := h.rpcDedup.Ack(track.scope, acks); err != nil {
		log.Error().Err(err).Msg("error acknowledging exactly-once RPC records")
	}
	h.rpcDedupMetrics.SetEntries(h.rpcDedup.Len())
}

// cancelRPCRegistry cancels the call msgID of the connection and of its scope. The cancel command has no
// reply of its own, so it answers an empty reply. With the dedup off there is no call to cancel.
func (h *Handler) cancelRPCRegistry(track *rpcTrack, msgID uint64) centrifuge.RPCReply {
	if track == nil {
		return centrifuge.RPCReply{}
	}
	h.rpcDedupMetrics.Inc(metrics.RPCRegistryDedupCancelled)
	track.inflight.Cancel(msgID)
	if track.scope != "" {
		failure := registryErrorValue(h.registryErrors().Cancelled)
		if err := h.rpcDedup.Cancel(rpcdedup.Key{Scope: track.scope, MsgID: msgID}, failure); err != nil {
			log.Error().Err(err).Msg("error cancelling exactly-once RPC call")
		}
		h.rpcDedupMetrics.SetEntries(h.rpcDedup.Len())
	}
	return centrifuge.RPCReply{}
}

// callRPCTracked runs a call which carries a msg_id, with the dedup on. The msg_id is checked first. The call
// is cancelled by a cancel command of its msg_id on its connection, or in its scope. A non-idempotent call is
// not cancelled by a disconnect, so its backend commit runs on. A non-idempotent call in a scope is recorded,
// and its duplicates are answered from the record.
func (h *Handler) callRPCTracked(c Client, e centrifuge.RPCEvent, m *rpcregistry.Method, cfg configtypes.RPCRegistry, connToken string, track *rpcTrack) (centrifuge.RPCReply, error) {
	msgID := e.Call.MsgID
	if !rpcdedup.MsgIDInWindow(msgID, time.Now(), cfg.Dedup.Window.ToDuration(), cfg.Dedup.MaxSkew.ToDuration()) {
		h.rpcDedupMetrics.Inc(metrics.RPCRegistryDedupMsgIDInvalid)
		msgIDErr := registryError(cfg.Errors.MsgIDInvalid)
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, msgIDErr)
	}
	base := c.Context()
	if !m.Idempotent {
		base = context.WithoutCancel(base)
	}
	var ctx context.Context
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(base)
	defer cancel()
	release := track.inflight.Add(msgID, cancel)
	defer release()

	if m.Idempotent {
		return h.invokeRPCRegistry(ctx, c, e, m, cfg, connToken)
	}
	if track.scope == "" {
		h.rpcDedupMetrics.Inc(metrics.RPCRegistryDedupNoScope)
		return h.invokeRPCRegistry(ctx, c, e, m, cfg, connToken)
	}
	return h.callRPCRecorded(ctx, cancel, c, e, m, cfg, connToken, track.scope)
}

// callRPCRecorded runs a non-idempotent call in a scope through the dedup store. The call is a miss, a duplicate
// of a done call, or a duplicate of a call in flight on this node. A duplicate with another fingerprint is refused.
func (h *Handler) callRPCRecorded(ctx context.Context, cancel context.CancelFunc, c Client, e centrifuge.RPCEvent, m *rpcregistry.Method, cfg configtypes.RPCRegistry, connToken string, scope string) (centrifuge.RPCReply, error) {
	key := rpcdedup.Key{Scope: scope, MsgID: e.Call.MsgID}
	flight := rpcdedup.NewFlightWithFingerprint(rpcdedup.Fingerprint(e.Method, e.Data))
	lookup, err := h.rpcDedup.Begin(key, flight)
	if err != nil {
		return h.rpcStoreFailure(ctx, c, e, m, cfg, connToken, err)
	}
	switch lookup.Outcome {
	case rpcdedup.OutcomeDone:
		h.rpcDedupMetrics.Inc(metrics.RPCRegistryDedupHitDone)
		return h.rpcRecordedReply(lookup.Record, cfg)
	case rpcdedup.OutcomeInFlight:
		h.rpcDedupMetrics.Inc(metrics.RPCRegistryDedupHitInFlightLocal)
		return h.rpcJoin(ctx, m, cfg, lookup.Flight)
	case rpcdedup.OutcomeMismatch:
		h.rpcDedupMetrics.Inc(metrics.RPCRegistryDedupMsgIDReused)
		reusedErr := registryError(cfg.Errors.MsgIDReused)
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, reusedErr)
	}
	h.rpcDedupMetrics.Inc(metrics.RPCRegistryDedupMiss)
	flight.Attach(cancel)
	reply, callErr := h.invokeRPCRegistry(ctx, c, e, m, cfg, connToken)
	if storeErr := h.rpcDedup.Complete(key, flight, rpcRecordOf(reply, callErr)); storeErr != nil {
		h.rpcDedupMetrics.Inc(metrics.RPCRegistryDedupStoreError)
		log.Error().Err(storeErr).Str("method", m.Name).Msg("error keeping exactly-once RPC record")
	}
	h.rpcDedupMetrics.SetEntries(h.rpcDedup.Len())
	return reply, callErr
}

// rpcStoreFailure handles a failed Begin. With fail_open the call runs without dedup. With fail_closed it is
// answered with the temporary dedup_unavailable error, which clients retry.
func (h *Handler) rpcStoreFailure(ctx context.Context, c Client, e centrifuge.RPCEvent, m *rpcregistry.Method, cfg configtypes.RPCRegistry, connToken string, storeErr error) (centrifuge.RPCReply, error) {
	h.rpcDedupMetrics.Inc(metrics.RPCRegistryDedupStoreError)
	log.Error().Err(storeErr).Str("method", m.Name).Msg("exactly-once RPC store failed")
	if cfg.Dedup.OnStoreError == configtypes.RPCDedupFailOpen {
		return h.invokeRPCRegistry(ctx, c, e, m, cfg, connToken)
	}
	unavailable := registryErrorValue(cfg.Errors.DedupUnavailable)
	unavailable.Temporary = true
	return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, unavailable)
}

// rpcJoin waits for the call which a duplicate joined, and answers the duplicate with its record. The wait is
// bounded by the method timeout and by the connection of the duplicate.
func (h *Handler) rpcJoin(ctx context.Context, m *rpcregistry.Method, cfg configtypes.RPCRegistry, flight *rpcdedup.Flight) (centrifuge.RPCReply, error) {
	started := time.Now()
	timer := time.NewTimer(m.Timeout)
	defer timer.Stop()
	select {
	case <-flight.Done():
	case <-ctx.Done():
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultCancelled, registryError(cfg.Errors.Cancelled))
	case <-timer.C:
		timeoutErr := registryCallError(status.Error(codes.DeadlineExceeded, "duplicate call still in flight"), nil, m, cfg, false)
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultTimeout, timeoutErr)
	}
	h.rpcDedupMetrics.ObserveWait(time.Since(started).Seconds())
	rec := flight.Record()
	if rec.Abandoned {
		// The call ended with a disconnect, and was not kept: the duplicate retries.
		unavailable := registryErrorValue(cfg.Errors.DedupUnavailable)
		unavailable.Temporary = true
		return centrifuge.RPCReply{}, registryMethodFailure(m, metrics.RPCRegistryResultError, unavailable)
	}
	return h.rpcRecordedReply(rec, cfg)
}

// rpcRecordedReply answers a duplicate from the record of its call. A reply which was not kept is evicted, and
// the call is not run again.
func (h *Handler) rpcRecordedReply(rec rpcdedup.Record, cfg configtypes.RPCRegistry) (centrifuge.RPCReply, error) {
	if rec.Uncached {
		h.rpcDedupMetrics.Inc(metrics.RPCRegistryDedupEvicted)
		return centrifuge.RPCReply{}, registryError(cfg.Errors.ResultEvicted)
	}
	if rec.Err != nil {
		return centrifuge.RPCReply{Cached: true}, rec.Err
	}
	return centrifuge.RPCReply{Data: rec.Reply, Cached: true}, nil
}

// rpcRecordOf returns the record of a call which ran: its reply, or its protocol error. A call which ended
// with a disconnect gave no answer, so nothing is kept for it and its retransmit runs again.
func rpcRecordOf(reply centrifuge.RPCReply, err error) rpcdedup.Record {
	if err == nil {
		return rpcdedup.Record{Reply: reply.Data}
	}
	var protoErr *centrifuge.Error
	if errors.As(err, &protoErr) {
		return rpcdedup.Record{Err: protoErr}
	}
	return rpcdedup.Record{Abandoned: true}
}
