package api

import (
	"context"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
)

// pushNotEnabledError returns the answer of every push notification method
// while the subsystem is not available. A method implemented by a later change
// replaces its stub and keeps this answer when push notifications are off.
// Each call returns a new value, so a caller can never change a shared answer.
func pushNotEnabledError() *Error {
	return &Error{
		Code:    ErrorNotAvailable.Code,
		Message: "push notifications are not enabled",
	}
}

// DeviceRegister registers a device for push notifications.
func (h *Executor) DeviceRegister(_ context.Context, _ *DeviceRegisterRequest) *DeviceRegisterResponse {
	return &DeviceRegisterResponse{
		Error: pushNotEnabledError(),
	}
}

// DeviceUpdate updates registered devices.
func (h *Executor) DeviceUpdate(_ context.Context, _ *DeviceUpdateRequest) *DeviceUpdateResponse {
	return &DeviceUpdateResponse{
		Error: pushNotEnabledError(),
	}
}

// DeviceRemove removes registered devices.
func (h *Executor) DeviceRemove(_ context.Context, _ *DeviceRemoveRequest) *DeviceRemoveResponse {
	return &DeviceRemoveResponse{
		Error: pushNotEnabledError(),
	}
}

// DeviceList lists registered devices.
func (h *Executor) DeviceList(_ context.Context, _ *DeviceListRequest) *DeviceListResponse {
	return &DeviceListResponse{
		Error: pushNotEnabledError(),
	}
}

// DeviceTopicUpdate changes the topics of a device.
func (h *Executor) DeviceTopicUpdate(_ context.Context, _ *DeviceTopicUpdateRequest) *DeviceTopicUpdateResponse {
	return &DeviceTopicUpdateResponse{
		Error: pushNotEnabledError(),
	}
}

// DeviceTopicList lists device topic subscriptions.
func (h *Executor) DeviceTopicList(_ context.Context, _ *DeviceTopicListRequest) *DeviceTopicListResponse {
	return &DeviceTopicListResponse{
		Error: pushNotEnabledError(),
	}
}

// UserTopicUpdate changes the topics of a user.
func (h *Executor) UserTopicUpdate(_ context.Context, _ *UserTopicUpdateRequest) *UserTopicUpdateResponse {
	return &UserTopicUpdateResponse{
		Error: pushNotEnabledError(),
	}
}

// UserTopicList lists user topic subscriptions.
func (h *Executor) UserTopicList(_ context.Context, _ *UserTopicListRequest) *UserTopicListResponse {
	return &UserTopicListResponse{
		Error: pushNotEnabledError(),
	}
}

// SendPushNotification sends push notifications to explicit provider tokens, inline, and answers one raw result per
// recipient. Without a direct send mode on this node the answer is the not-enabled error, and the mode answers its own
// refusals, such as a node that is not a sender.
func (h *Executor) SendPushNotification(ctx context.Context, req *SendPushNotificationRequest) *SendPushNotificationResponse {
	if h.config.Push == nil {
		return &SendPushNotificationResponse{
			Error: pushNotEnabledError(),
		}
	}
	return h.config.Push.Send(ctx, req)
}

// UpdatePushStatus saves the status of a push notification.
func (h *Executor) UpdatePushStatus(_ context.Context, _ *UpdatePushStatusRequest) *UpdatePushStatusResponse {
	return &UpdatePushStatusResponse{
		Error: pushNotEnabledError(),
	}
}

// CancelPush cancels a delayed push notification.
func (h *Executor) CancelPush(_ context.Context, _ *CancelPushRequest) *CancelPushResponse {
	return &CancelPushResponse{
		Error: pushNotEnabledError(),
	}
}
