package api

import (
	"context"
	"errors"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/rs/zerolog/log"
)

// BlockUser blocks user on all nodes until expire_at. Live connections of the user are disconnected
// with 4500 code and the user can't connect or refresh while the block is active.
func (h *Executor) BlockUser(_ context.Context, cmd *BlockUserRequest) *BlockUserResponse {
	defer metrics.ObserveAPICommand(time.Now(), h.config.Protocol, "block_user")

	resp := &BlockUserResponse{}

	cfg := h.cfgContainer.Config()
	if !cfg.UserBlock.Enabled {
		resp.Error = ErrorNotAvailable
		return resp
	}

	notification, respErr := resolveAccessRevoke(accessrevoke.Operation{
		Kind:     accessrevoke.KindBlockUser,
		User:     cmd.User,
		ExpireAt: cmd.ExpireAt,
	}, cfg.AccessRevoke.DefaultTTL)
	if respErr != nil {
		resp.Error = respErr
		return resp
	}
	resp.Error = h.applyAccessRevoke(notification)
	if resp.Error != nil {
		return resp
	}
	resp.Result = &BlockUserResult{}
	return resp
}

// UnblockUser removes the block of user on all nodes. Connections are not affected.
func (h *Executor) UnblockUser(_ context.Context, cmd *UnblockUserRequest) *UnblockUserResponse {
	defer metrics.ObserveAPICommand(time.Now(), h.config.Protocol, "unblock_user")

	resp := &UnblockUserResponse{}

	cfg := h.cfgContainer.Config()
	if !cfg.UserBlock.Enabled {
		resp.Error = ErrorNotAvailable
		return resp
	}

	notification, respErr := resolveAccessRevoke(accessrevoke.Operation{
		Kind: accessrevoke.KindUnblockUser,
		User: cmd.User,
	}, cfg.AccessRevoke.DefaultTTL)
	if respErr != nil {
		resp.Error = respErr
		return resp
	}
	resp.Error = h.applyAccessRevoke(notification)
	if resp.Error != nil {
		return resp
	}
	resp.Result = &UnblockUserResult{}
	return resp
}

// RevokeToken revokes session (uid claim of connection tokens) on all nodes. Connections of user
// with this session are disconnected when user is set. Without user the node also sweeps its
// local connections in the background and disconnects those which carry this session.
func (h *Executor) RevokeToken(_ context.Context, cmd *RevokeTokenRequest) *RevokeTokenResponse {
	defer metrics.ObserveAPICommand(time.Now(), h.config.Protocol, "revoke_token")

	resp := &RevokeTokenResponse{}

	cfg := h.cfgContainer.Config()
	if !cfg.TokenRevoke.Enabled {
		resp.Error = ErrorNotAvailable
		return resp
	}
	notification, respErr := resolveAccessRevoke(accessrevoke.Operation{
		Kind:       accessrevoke.KindRevokeToken,
		User:       cmd.User,
		Session:    cmd.Uid,
		RevokedAt:  cmd.RevokedAt,
		ExpireAt:   cmd.ExpireAt,
		Disconnect: accessrevoke.FromAPIDisconnect(cmd.Disconnect),
	}, cfg.AccessRevoke.DefaultTTL)
	if respErr != nil {
		resp.Error = respErr
		return resp
	}
	resp.Error = h.applyAccessRevoke(notification)
	if resp.Error != nil {
		return resp
	}
	resp.Result = &RevokeTokenResult{}
	return resp
}

// InvalidateUserTokens invalidates tokens of user on all nodes. Connections of user with rejected
// tokens are disconnected.
func (h *Executor) InvalidateUserTokens(_ context.Context, cmd *InvalidateUserTokensRequest) *InvalidateUserTokensResponse {
	defer metrics.ObserveAPICommand(time.Now(), h.config.Protocol, "invalidate_user_tokens")

	resp := &InvalidateUserTokensResponse{}

	cfg := h.cfgContainer.Config()
	if !cfg.UserTokensInvalidate.Enabled {
		resp.Error = ErrorNotAvailable
		return resp
	}
	notification, respErr := resolveAccessRevoke(accessrevoke.Operation{
		Kind:         accessrevoke.KindInvalidateUserTokens,
		User:         cmd.User,
		Channel:      cmd.Channel,
		Generation:   cmd.Generation,
		IssuedBefore: cmd.IssuedBefore,
		RevokedAt:    cmd.RevokedAt,
		ExpireAt:     cmd.ExpireAt,
	}, cfg.AccessRevoke.DefaultTTL)
	if respErr != nil {
		resp.Error = respErr
		return resp
	}
	resp.Error = h.applyAccessRevoke(notification)
	if resp.Error != nil {
		return resp
	}
	resp.Result = &InvalidateUserTokensResult{}
	return resp
}

// applyAccessRevoke applies the notification on this node and then sends it to other nodes. The
// rate limit is checked first, so ErrorTooManyRequests is returned before anything is applied and
// the whole call can be retried.
func (h *Executor) applyAccessRevoke(notification accessrevoke.Notification) *Error {
	if h.config.AccessRevoker == nil || h.config.AccessNotifier == nil {
		return ErrorNotAvailable
	}
	if !h.config.AccessNotifier.Allow() {
		return ErrorTooManyRequests
	}
	err := h.config.AccessRevoker.Apply(notification)
	if err != nil {
		log.Error().Err(err).Str("kind", notification.Kind).Msg("error applying access revocation")
		return ErrorInternal
	}
	err = h.config.AccessNotifier.Notify(notification)
	if err != nil {
		log.Error().Err(err).Str("kind", notification.Kind).Msg("error notifying nodes about access revocation")
		return ErrorInternal
	}
	return nil
}

// resolveAccessRevoke resolves the operation into a Notification using the current time. Validation
// failures are mapped to API errors: unsupported operations to ErrorNotAvailable, others to ErrorBadRequest.
func resolveAccessRevoke(operation accessrevoke.Operation, defaultTTL configtypes.Duration) (accessrevoke.Notification, *Error) {
	now := time.Now()
	// Expired operation has no effect, so it is rejected here. Kafka source skips such records itself.
	if operation.ExpireAt != 0 && operation.ExpireAt <= now.Unix() {
		log.Error().Int64("expire_at", operation.ExpireAt).Str("kind", operation.Kind).Msg("bad access revocation request: expire_at is in the past")
		return accessrevoke.Notification{}, ErrorBadRequest
	}
	notification, err := accessrevoke.Resolve(operation, now, defaultTTL.ToDuration())
	if err != nil {
		log.Error().Err(err).Str("kind", operation.Kind).Msg("bad access revocation request")
		var validationErr *accessrevoke.ValidationError
		if errors.As(err, &validationErr) && validationErr.NotSupported {
			return accessrevoke.Notification{}, ErrorNotAvailable
		}
		return accessrevoke.Notification{}, ErrorBadRequest
	}
	return notification, nil
}

// dispatchAccessRevoke handles access revocation commands of asynchronous consumers. handled is false
// for methods which are not access revocation ones. Temporary errors are returned for a retry, other
// errors are logged and skipped.
func (d *Dispatcher) dispatchAccessRevoke(ctx context.Context, method string, payload []byte) (bool, error) {
	var err error
	switch method {
	case "block_user":
		_, err = d.handler.handleBlockUser(ctx, payload)
	case "unblock_user":
		_, err = d.handler.handleUnblockUser(ctx, payload)
	case "revoke_token":
		// An invalid disconnect must not lose the revocation, so it is dropped from the payload first.
		_, err = d.handler.handleRevokeToken(ctx, accessrevoke.SanitizeRevokeTokenBody(payload))
	case "invalidate_user_tokens":
		_, err = d.handler.handleInvalidateUserTokens(ctx, payload)
	default:
		return false, nil
	}
	if err == nil {
		return true, nil
	}
	var apiError *Error
	if errors.As(err, &apiError) && apiError.Temporary {
		return true, err
	}
	logNonRetryableConsumingError(err, method)
	return true, nil
}
