package api

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"time"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

// handlePushSendRoute serves /send_push_notification. On a sender node the body is read through a cap of
// max_request_bytes, and the recipient count is checked on the raw body before the generated decoder allocates the
// entries. A body over the cap is refused with the bad request error, not with a server error. Any other node answers
// through the generated handler, which answers the not-enabled or the sender-only error.
func (s *Handler) handlePushSendRoute(w http.ResponseWriter, r *http.Request) {
	sender := s.api.config.Push
	if !sender.IsSender() {
		s.handleSendPushNotification(w, r)
		return
	}

	// The inline run ends within the bound, so the write deadline is set before the body is read. A writer without
	// deadline control ignores the error, and the server write timeout then applies.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(sender.WriteBound()))

	r.Body = http.MaxBytesReader(w, r.Body, sender.MaxRequestBytes())
	data, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			s.writePushSendRefusal(w, r, &Error{
				Code:    ErrorBadRequest.Code,
				Message: "request too large",
			})
			return
		}
		s.handleReadDataError(r, w, err)
		return
	}

	refusal := sender.CheckRecipientCount(data)
	if refusal != nil {
		s.writePushSendRefusal(w, r, refusal)
		return
	}

	r.Body = io.NopCloser(bytes.NewReader(data))
	s.handleSendPushNotification(w, r)
}

// writePushSendRefusal writes refusal as the Error JSON of the send method, with the HTTP status of its code.
func (s *Handler) writePushSendRefusal(w http.ResponseWriter, r *http.Request, refusal *Error) {
	metrics.IncAPIError(s.api.config.Protocol, "send_push_notification", refusal.Code)

	data, err := EncodeError(refusal)
	if err != nil {
		s.handleMarshalError(r, w, err)
		return
	}
	s.writeJsonCustomStatus(w, MapErrorToHTTPCode(refusal), data)
}
