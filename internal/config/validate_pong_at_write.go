package config

import (
	"fmt"
)

// validatePongAtWrite checks client.pong_timeout_at_write. The pong timeout starts when a ping is written, so a write
// that never returns would never time out a connection: the WebSocket write timeout must be positive. A negative
// pong_timeout turns pong checks off, so the key does nothing then and is not checked.
func validatePongAtWrite(c Config) error {
	if !c.Client.PongTimeoutAtWrite || c.Client.PongTimeout.ToDuration() < 0 {
		return nil
	}
	writeTimeout := c.WebSocket.WriteTimeout.ToDuration()
	if writeTimeout <= 0 {
		return fmt.Errorf("client.pong_timeout_at_write: websocket.write_timeout must be positive when it is on, got %s", writeTimeout)
	}
	return nil
}
