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
// not-enabled error. With them on, the body is read through a cap of max_request_bytes on every node, so a node that is
// not a sender refuses an oversized body before it answers the sender-only error. On a sender node the call slot is taken
// before the body is read, and the recipient count is checked on the raw body before the generated decoder allocates the
// entries. A body over the cap is refused with the bad request error, not with a server error.
func (s *Handler) handlePushSendRoute(w http.ResponseWriter, r *http.Request) {
	sender := s.api.config.Push
	if sender == nil {
		s.handleSendPushNotification(w, r)
		return
	}

	if !sender.IsSender() {
		data, ok := s.readCappedBody(w, r, sender)
		if !ok {
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(data))
		s.handleSendPushNotification(w, r)
		return
	}

	release, busy := sender.Admit()
	if busy != nil {
		s.writePushSendRefusal(w, r, busy)
		return
	}
	defer release()

	// The inline run ends within the bound, so the write deadline is set before the body is read. A writer without
	// deadline control ignores the error, and the server write timeout then applies.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(sender.WriteBound()))

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
	r = r.WithContext(push.WithAdmission(r.Context()))
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

// writePushSendRefusal writes refusal as the Error JSON of the send method, with the HTTP status of its code. The route
// refusal is counted as a refused call, because it never reaches the send path that counts it.
func (s *Handler) writePushSendRefusal(w http.ResponseWriter, r *http.Request, refusal *Error) {
	metrics.IncAPIError(s.api.config.Protocol, "send_push_notification", refusal.Code)
	push.CountRefusedRequest()

	data, err := EncodeError(refusal)
	if err != nil {
		s.handleMarshalError(r, w, err)
		return
	}
	s.writeJsonCustomStatus(w, MapErrorToHTTPCode(refusal), data)
}
