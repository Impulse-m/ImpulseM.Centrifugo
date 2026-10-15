package notify

import (
	"testing"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func TestMergeHandlersKeepsRegisteredHandler(t *testing.T) {
	var called string
	handlers := map[string]centrifuge.NotificationHandler{
		"a": func(centrifuge.NotificationEvent) { called = "registered" },
	}
	extra := map[string]centrifuge.NotificationHandler{
		"a": func(centrifuge.NotificationEvent) { called = "extra" },
		"b": func(centrifuge.NotificationEvent) {},
	}

	merged := mergeHandlers(handlers, extra)
	require.Len(t, merged, 2)
	require.Contains(t, merged, "b")
	merged["a"](centrifuge.NotificationEvent{})
	require.Equal(t, "registered", called)
}
