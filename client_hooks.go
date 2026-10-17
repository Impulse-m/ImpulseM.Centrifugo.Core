package centrifuge

// OnDisconnectIfConnected sets the DisconnectHandler of a connected client and reports whether it was set. It reports
// false for a client which is closed or not yet connected: close reads the handler in the critical section which marks
// the client closed and runs it only for a connected client, so a handler set here always runs when the client closes,
// and a caller which gets false knows that nothing it attaches to the connection will be released by a handler.
// It replaces the DisconnectHandler which is set at the time, including one set by OnDisconnect.
// Must not be called while c.mu is held.
func (c *Client) OnDisconnectIfConnected(h DisconnectHandler) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status != statusConnected {
		return false
	}
	c.eventHub.disconnectHandler = h
	return true
}

// BeginDisconnect starts closing the client with disconnect, as Disconnect does, and marks it closing before it returns,
// so Closing reports true from then on although the close itself runs in its own goroutine. It reports false and starts
// nothing when a close of the client has begun already: that close completes with its own disconnect, as a second
// Disconnect would. A client which is still connecting is closed once its connect completes, as with Disconnect.
func (c *Client) BeginDisconnect(disconnect Disconnect) bool {
	if c.closing.Swap(true) {
		return false
	}
	go func() {
		_ = c.close(disconnect)
	}()
	return true
}

// Closing reports whether a close of the client has begun: by BeginDisconnect, by the goroutine of Disconnect, or by the
// transport. It stays true once the client is closed. A client which reports false has no close begun: it is new,
// connecting or connected.
func (c *Client) Closing() bool {
	return c.closing.Load()
}
