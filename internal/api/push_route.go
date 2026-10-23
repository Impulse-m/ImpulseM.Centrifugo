package api

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"time"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/push"
)

// handlePushSendRoute serves /send_push_notification. With push notifications off, the generated handler answers the
// not-enabled error. A node that is not a sender answers the sender-only error at once, before it reads the body, as
// the first step of the send checks. On a sender node the call slot is taken before the body is read, so the bodies of
// the calls in flight are bounded by the slots. The body is read through a cap of max_request_bytes, and the recipient
// count is checked on the raw body before the generated decoder allocates the entries. A body over the cap is refused
// with the bad request error, not with a server error.
func (s *Handler) handlePushSendRoute(w http.ResponseWriter, r *http.Request) {
	// The call starts with its request: the deadline of timeout_ms counts the time the body takes to arrive.
	started := time.Now()
	sender := s.api.config.Push
	if sender == nil {
		s.handleSendPushNotification(w, r)
		return
	}

	if !sender.IsSender() {
		s.writePushSendRefusal(w, r, push.SenderOnlyError())
		return
	}

	release, busy := sender.Admit()
	if busy != nil {
		s.writePushSendRefusal(w, r, busy)
		return
	}
	defer release()

	data, ok := s.readCappedBody(w, r, sender)
	if !ok {
		return
	}

	refusal := sender.CheckRecipientCount(data)
	if refusal != nil {
		s.writePushSendRefusal(w, r, refusal)
		return
	}

	r.Body = io.NopCloser(bytes.NewReader(data))
	// The write deadline is the deadline of the call, which the send sets once the request is decoded. The call deadline
	// counts from started, so the write deadline covers the body upload, the inline run and the response. A writer without
	// deadline control ignores the error, and the server write timeout then applies.
	ctx := push.WithWriteDeadline(r.Context(), func(deadline time.Time) {
		_ = http.NewResponseController(w).SetWriteDeadline(deadline)
	})
	r = r.WithContext(push.WithAdmission(ctx, started))
	s.handleSendPushNotification(w, r)
}

// readCappedBody reads the body of r through the cap of max_request_bytes. It reports false when it has written the
// refusal: the body is over the cap, or it cannot be read.
func (s *Handler) readCappedBody(w http.ResponseWriter, r *http.Request, sender *push.Sender) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, sender.MaxRequestBytes())
	data, err := io.ReadAll(r.Body)
	if err == nil {
		return data, true
	}

	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		s.writePushSendRefusal(w, r, &Error{
			Code:    ErrorBadRequest.Code,
			Message: "request too large",
		})
		return nil, false
	}
	s.handleReadDataError(r, w, err)
	return nil, false
}

// writePushSendRefusal answers a route refusal the way the generic handler of the send method answers its error: in the
// transport error mode, the Error JSON with the HTTP status of its code; otherwise HTTP 200 with the error in the body of
// the method response. The route refusal is counted as a refused call, because it never reaches the send path that counts
// it.
func (s *Handler) writePushSendRefusal(w http.ResponseWriter, r *http.Request, refusal *Error) {
	metrics.IncAPIError(s.api.config.Protocol, "send_push_notification", refusal.Code)
	push.CountRefusedRequest()

	if s.useTransportErrorMode(r) {
		data, err := EncodeError(refusal)
		if err != nil {
			s.handleMarshalError(r, w, err)
			return
		}
		s.writeJsonCustomStatus(w, MapErrorToHTTPCode(refusal), data)
		return
	}

	data, err := responseEncoder.EncodeSendPushNotification(&SendPushNotificationResponse{Error: refusal})
	if err != nil {
		metrics.IncAPIErrorStringCode(s.api.config.Protocol, "send_push_notification", "marshal")
		s.handleMarshalError(r, w, err)
		return
	}
	s.writeJson(w, data)
}
