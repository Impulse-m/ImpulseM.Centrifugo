package proxy

import (
	"context"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
)

// CacheEmptyProxy allows to send cache empty requests to the application backend.
type CacheEmptyProxy interface {
	ProxyNotifyCacheEmpty(context.Context, *proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error)
	// Protocol for metrics and logging.
	Protocol() string
	// Timeout of a single cache empty call.
	Timeout() time.Duration
}
