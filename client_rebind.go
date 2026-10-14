package centrifuge

import (
	"errors"
	"time"
)

// ErrRebindClosed is returned by Client.Rebind when the connection is closed.
var ErrRebindClosed = errors.New("client closed")

// RebindOptions describe the identity a live connection takes over in Client.Rebind.
type RebindOptions struct {
	// UserID is the new user of the connection, empty for anonymous.
	UserID string
	// Info replaces the connection info. Nil keeps the current info.
	Info []byte
	// ExpireAt is the new expiration in unix seconds. Zero disarms expiration.
	ExpireAt int64
	// TokenIdentity is the identity of the token the connection holds now.
	TokenIdentity TokenIdentity
}

// Rebind moves a live connection to another user without reconnecting. It emits
// no join or leave events and does not touch channels: the caller decides which
// channels to keep. Presence is rewritten with the new user on the next tick.
func (c *Client) Rebind(opts RebindOptions) error {
	// Lock order: connectMu, then mu. Same as close and connect.
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status == statusClosed {
		return ErrRebindClosed
	}
	oldUser := c.user
	c.storeUser(opts.UserID)
	if opts.Info != nil {
		c.info = opts.Info
	}
	c.tokenIdentity = opts.TokenIdentity
	now := time.Now()
	nowUnix := now.Unix()
	c.exp = min(opts.ExpireAt, nowUnix+maxTTLSeconds)
	if c.exp == 0 {
		c.nextExpire = 0
	} else {
		ttl := time.Duration(c.exp-nowUnix) * time.Second
		c.addExpireUpdate(ttl+c.node.config.ClientExpiredCloseDelay, false)
	}
	// Presence entries carry the user, so the next tick rewrites them.
	c.nextPresence = now.UnixNano()
	c.scheduleNextTimer()
	if c.authenticated {
		c.node.hub.rebind(c, oldUser, opts.UserID)
	}
	return nil
}
