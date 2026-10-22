package api

import (
	"context"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
)

// newPushNotEnabledError is the answer of every push notification method while
// the subsystem is not available. A method implemented by a later change
// replaces its stub and keeps this answer when push notifications are off.
// Each call returns a new value, so no response shares an Error with another.
func newPushNotEnabledError() *Error {
	return &Error{
		Code:    ErrorNotAvailable.Code,
		Message: "push notifications are not enabled",
	}
}

// DeviceRegister registers a device for push notifications.
func (h *Executor) DeviceRegister(_ context.Context, _ *DeviceRegisterRequest) *DeviceRegisterResponse {
	return &DeviceRegisterResponse{
		Error: newPushNotEnabledError(),
	}
}

// DeviceUpdate updates registered devices.
func (h *Executor) DeviceUpdate(_ context.Context, _ *DeviceUpdateRequest) *DeviceUpdateResponse {
	return &DeviceUpdateResponse{
		Error: newPushNotEnabledError(),
	}
}

// DeviceRemove removes registered devices.
func (h *Executor) DeviceRemove(_ context.Context, _ *DeviceRemoveRequest) *DeviceRemoveResponse {
	return &DeviceRemoveResponse{
		Error: newPushNotEnabledError(),
	}
}

// DeviceList lists registered devices.
func (h *Executor) DeviceList(_ context.Context, _ *DeviceListRequest) *DeviceListResponse {
	return &DeviceListResponse{
		Error: newPushNotEnabledError(),
	}
}

// DeviceTopicUpdate changes the topics of a device.
func (h *Executor) DeviceTopicUpdate(_ context.Context, _ *DeviceTopicUpdateRequest) *DeviceTopicUpdateResponse {
	return &DeviceTopicUpdateResponse{
		Error: newPushNotEnabledError(),
	}
}

// DeviceTopicList lists device topic subscriptions.
func (h *Executor) DeviceTopicList(_ context.Context, _ *DeviceTopicListRequest) *DeviceTopicListResponse {
	return &DeviceTopicListResponse{
		Error: newPushNotEnabledError(),
	}
}

// UserTopicUpdate changes the topics of a user.
func (h *Executor) UserTopicUpdate(_ context.Context, _ *UserTopicUpdateRequest) *UserTopicUpdateResponse {
	return &UserTopicUpdateResponse{
		Error: newPushNotEnabledError(),
	}
}

// UserTopicList lists user topic subscriptions.
func (h *Executor) UserTopicList(_ context.Context, _ *UserTopicListRequest) *UserTopicListResponse {
	return &UserTopicListResponse{
		Error: newPushNotEnabledError(),
	}
}

// SendPushNotification sends a push notification.
func (h *Executor) SendPushNotification(_ context.Context, _ *SendPushNotificationRequest) *SendPushNotificationResponse {
	return &SendPushNotificationResponse{
		Error: newPushNotEnabledError(),
	}
}

// UpdatePushStatus saves the status of a push notification.
func (h *Executor) UpdatePushStatus(_ context.Context, _ *UpdatePushStatusRequest) *UpdatePushStatusResponse {
	return &UpdatePushStatusResponse{
		Error: newPushNotEnabledError(),
	}
}

// CancelPush cancels a delayed push notification.
func (h *Executor) CancelPush(_ context.Context, _ *CancelPushRequest) *CancelPushResponse {
	return &CancelPushResponse{
		Error: newPushNotEnabledError(),
	}
}
