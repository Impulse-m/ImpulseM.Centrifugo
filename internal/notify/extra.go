package notify

import (
	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// mergeHandlers adds the extra handlers to handlers, and returns handlers. An extra handler of an op which handlers
// already has is skipped and logged, so that the registered handler stays.
func mergeHandlers(handlers map[string]centrifuge.NotificationHandler, extra map[string]centrifuge.NotificationHandler) map[string]centrifuge.NotificationHandler {
	for op, handler := range extra {
		if _, found := handlers[op]; found {
			log.Error().Str("op", op).Msg("notification handler is already registered, the extra handler is skipped")
			continue
		}
		handlers[op] = handler
	}
	return handlers
}
