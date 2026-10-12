package health

import (
	"net/http"

	"github.com/centrifugal/centrifuge"
)

// Config of health check handler.
type Config struct {
	// Ready reports whether the node is ready to serve. Nil means always ready.
	Ready func() bool
}

// Handler handles health endpoint. It is a readiness probe, not a liveness probe: a node which is not
// ready answers 503 so that load balancers stop routing to it, and it must not be restarted for that.
type Handler struct {
	node   *centrifuge.Node
	config Config
}

// NewHandler creates new Handler.
func NewHandler(n *centrifuge.Node, c Config) *Handler {
	h := &Handler{
		node:   n,
		config: c,
	}
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if h.config.Ready != nil && !h.config.Ready() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"ready":false}`))
		return
	}
	_, _ = w.Write([]byte(`{}`))
}
