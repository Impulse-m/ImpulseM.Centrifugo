package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

// pushMaxBodyBytes caps the body of a push notification request read before
// the generated handler runs. The cap is fixed while push notifications are
// not available, so a request that is refused anyway is not read in full.
const pushMaxBodyBytes = 1 << 20

// errPushNotEnabled is the answer of every push notification method while
// the subsystem is not available. A method implemented by a later change
// replaces its stub and keeps this answer when push notifications are off.
var errPushNotEnabled = &Error{
	Code:    ErrorNotAvailable.Code,
	Message: "push notifications are not enabled",
}

// pushErrorBody is the JSON body of a push notification answer that carries
// only an error, the same shape as the response messages of the methods.
type pushErrorBody struct {
	Error *Error `json:"error,omitempty"`
}

// withPushBodyLimit reads the request body up to pushMaxBodyBytes before the
// generated handler reads it. A longer body answers 107 and is not read
// further.
func (s *Handler) withPushBodyLimit(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, pushMaxBodyBytes))
		if err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				s.writePushBodyTooLarge(method, r, w)
				return
			}
			s.handleReadDataError(r, w, err)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(data))
		next(w, r)
	}
}

func (s *Handler) writePushBodyTooLarge(method string, r *http.Request, w http.ResponseWriter) {
	apiErr := &Error{
		Code:    ErrorBadRequest.Code,
		Message: "request too large",
	}
	metrics.IncAPIError(s.api.config.Protocol, method, apiErr.Code)
	if s.useTransportErrorMode(r) {
		data, _ := EncodeError(apiErr)
		s.writeJsonCustomStatus(w, MapErrorToHTTPCode(apiErr), data)
		return
	}
	data, _ := json.Marshal(&pushErrorBody{Error: apiErr})
	s.writeJson(w, data)
}

// DeviceRegister registers a device for push notifications.
func (h *Executor) DeviceRegister(_ context.Context, _ *DeviceRegisterRequest) *DeviceRegisterResponse {
	return &DeviceRegisterResponse{
		Error: errPushNotEnabled,
	}
}

// DeviceUpdate updates registered devices.
func (h *Executor) DeviceUpdate(_ context.Context, _ *DeviceUpdateRequest) *DeviceUpdateResponse {
	return &DeviceUpdateResponse{
		Error: errPushNotEnabled,
	}
}

// DeviceRemove removes registered devices.
func (h *Executor) DeviceRemove(_ context.Context, _ *DeviceRemoveRequest) *DeviceRemoveResponse {
	return &DeviceRemoveResponse{
		Error: errPushNotEnabled,
	}
}

// DeviceList lists registered devices.
func (h *Executor) DeviceList(_ context.Context, _ *DeviceListRequest) *DeviceListResponse {
	return &DeviceListResponse{
		Error: errPushNotEnabled,
	}
}

// DeviceTopicUpdate changes the topics of a device.
func (h *Executor) DeviceTopicUpdate(_ context.Context, _ *DeviceTopicUpdateRequest) *DeviceTopicUpdateResponse {
	return &DeviceTopicUpdateResponse{
		Error: errPushNotEnabled,
	}
}

// DeviceTopicList lists device topic subscriptions.
func (h *Executor) DeviceTopicList(_ context.Context, _ *DeviceTopicListRequest) *DeviceTopicListResponse {
	return &DeviceTopicListResponse{
		Error: errPushNotEnabled,
	}
}

// UserTopicUpdate changes the topics of a user.
func (h *Executor) UserTopicUpdate(_ context.Context, _ *UserTopicUpdateRequest) *UserTopicUpdateResponse {
	return &UserTopicUpdateResponse{
		Error: errPushNotEnabled,
	}
}

// UserTopicList lists user topic subscriptions.
func (h *Executor) UserTopicList(_ context.Context, _ *UserTopicListRequest) *UserTopicListResponse {
	return &UserTopicListResponse{
		Error: errPushNotEnabled,
	}
}

// SendPushNotification sends a push notification.
func (h *Executor) SendPushNotification(_ context.Context, _ *SendPushNotificationRequest) *SendPushNotificationResponse {
	return &SendPushNotificationResponse{
		Error: errPushNotEnabled,
	}
}

// UpdatePushStatus saves the status of a push notification.
func (h *Executor) UpdatePushStatus(_ context.Context, _ *UpdatePushStatusRequest) *UpdatePushStatusResponse {
	return &UpdatePushStatusResponse{
		Error: errPushNotEnabled,
	}
}

// CancelPush cancels a delayed push notification.
func (h *Executor) CancelPush(_ context.Context, _ *CancelPushRequest) *CancelPushResponse {
	return &CancelPushResponse{
		Error: errPushNotEnabled,
	}
}
