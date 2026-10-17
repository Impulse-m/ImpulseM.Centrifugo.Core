package centrifuge

// OnDisconnectIfConnected sets the DisconnectHandler of a connected client and reports whether it was set. It reports
// false for a client which is closed or not yet connected: close reads the handler in the critical section which marks
// the client closed and runs it only for a connected client, so a handler set here always runs when the client closes,
// and a caller which gets false knows that nothing it attaches to the connection will be released by a handler.
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
