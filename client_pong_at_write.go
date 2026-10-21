package centrifuge

import (
	"time"

	"github.com/centrifugal/protocol"
)

// afterPingWriteFn returns the hook the writer calls, with no writer lock held, after a drain
// that wrote a ping. It returns nil when pong-at-write is off, so the writer does nothing extra.
func (c *Client) afterPingWriteFn() func() {
	if !c.pongAtWriteOn {
		return nil
	}
	return c.armPongAfterWrite
}

// sendPingAtWrite is sendPing with pong-at-write on. The pong timeout is not armed here: it is
// armed by armPongAfterWrite once the ping is written. A ping is not queued while an earlier one
// is unanswered or still queued, so a pong always pairs with one ping (see HandleCommand).
func (c *Client) sendPingAtWrite() {
	c.mu.Lock()
	if c.lastPing > 0 || c.pingQueued {
		c.addPingUpdate(false, true)
		c.mu.Unlock()
		return
	}
	c.lastPing = time.Now().UnixNano()
	c.pingQueued = true
	// Clear a deadline armed for an earlier ping, so it cannot disconnect a client
	// for this ping before the ping is written.
	c.nextPong = 0
	c.mu.Unlock()
	_ = c.writeEncodedPushData(getPingData(false, c.transport.Protocol()), "", "", protocol.FrameTypeServerPing, ChannelBatchConfig{})
	if c.node.logEnabled(LogLevelTrace) {
		c.traceOutReply(emptyReply)
	}
	c.mu.Lock()
	c.addPingUpdate(false, true)
	c.mu.Unlock()
}

// armPongAfterWrite is the writer hook. It runs after the drain that wrote the ping, so the
// pong deadline starts at the write.
func (c *Client) armPongAfterWrite() {
	c.mu.Lock()
	c.pingQueued = false
	if c.status == statusClosed {
		c.mu.Unlock()
		return
	}
	c.nextPong = time.Now().Add(c.pongTimeout).UnixNano()
	c.scheduleNextTimer()
	c.mu.Unlock()
}
