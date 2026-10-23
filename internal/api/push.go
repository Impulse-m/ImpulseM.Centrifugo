package api

import (
	"context"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push"
)

// notAvailableInBuild returns the answer of a push method that this build does not implement. It is given while push
// notifications are on, because the method exists in the build but is not built.
func notAvailableInBuild() *Error {
	return &Error{
		Code:    ErrorNotAvailable.Code,
		Message: "not available in this build",
	}
}

// unbuiltPushError returns the answer of a push method that this build does not implement. With push notifications off
// the answer is the not-enabled error; with them on, it is the not-available error of the build.
func (h *Executor) unbuiltPushError() *Error {
	if h.config.Push == nil {
		return push.NotEnabledError()
	}
	return notAvailableInBuild()
}

// DeviceRegister registers a device for push notifications.
func (h *Executor) DeviceRegister(_ context.Context, _ *DeviceRegisterRequest) *DeviceRegisterResponse {
	return &DeviceRegisterResponse{
		Error: h.unbuiltPushError(),
	}
}

// DeviceUpdate updates registered devices.
func (h *Executor) DeviceUpdate(_ context.Context, _ *DeviceUpdateRequest) *DeviceUpdateResponse {
	return &DeviceUpdateResponse{
		Error: h.unbuiltPushError(),
	}
}

// DeviceRemove removes registered devices.
func (h *Executor) DeviceRemove(_ context.Context, _ *DeviceRemoveRequest) *DeviceRemoveResponse {
	return &DeviceRemoveResponse{
		Error: h.unbuiltPushError(),
	}
}

// DeviceList lists registered devices.
func (h *Executor) DeviceList(_ context.Context, _ *DeviceListRequest) *DeviceListResponse {
	return &DeviceListResponse{
		Error: h.unbuiltPushError(),
	}
}

// DeviceTopicUpdate changes the topics of a device.
func (h *Executor) DeviceTopicUpdate(_ context.Context, _ *DeviceTopicUpdateRequest) *DeviceTopicUpdateResponse {
	return &DeviceTopicUpdateResponse{
		Error: h.unbuiltPushError(),
	}
}

// DeviceTopicList lists device topic subscriptions.
func (h *Executor) DeviceTopicList(_ context.Context, _ *DeviceTopicListRequest) *DeviceTopicListResponse {
	return &DeviceTopicListResponse{
		Error: h.unbuiltPushError(),
	}
}

// UserTopicUpdate changes the topics of a user.
func (h *Executor) UserTopicUpdate(_ context.Context, _ *UserTopicUpdateRequest) *UserTopicUpdateResponse {
	return &UserTopicUpdateResponse{
		Error: h.unbuiltPushError(),
	}
}

// UserTopicList lists user topic subscriptions.
func (h *Executor) UserTopicList(_ context.Context, _ *UserTopicListRequest) *UserTopicListResponse {
	return &UserTopicListResponse{
		Error: h.unbuiltPushError(),
	}
}

// SendPushNotification sends push notifications to explicit provider tokens, inline, and answers one raw result per
// recipient. Without a direct send mode on this node the answer is the not-enabled error, and the mode answers its own
// refusals, such as a node that is not a sender.
func (h *Executor) SendPushNotification(ctx context.Context, req *SendPushNotificationRequest) *SendPushNotificationResponse {
	if h.config.Push == nil {
		return &SendPushNotificationResponse{
			Error: push.NotEnabledError(),
		}
	}
	return h.config.Push.Send(ctx, req)
}

// UpdatePushStatus saves the status of a push notification.
func (h *Executor) UpdatePushStatus(_ context.Context, _ *UpdatePushStatusRequest) *UpdatePushStatusResponse {
	return &UpdatePushStatusResponse{
		Error: h.unbuiltPushError(),
	}
}

// CancelPush cancels a delayed push notification.
func (h *Executor) CancelPush(_ context.Context, _ *CancelPushRequest) *CancelPushResponse {
	return &CancelPushResponse{
		Error: h.unbuiltPushError(),
	}
}
