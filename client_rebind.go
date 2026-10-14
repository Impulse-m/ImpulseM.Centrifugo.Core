package centrifuge

import (
	"errors"
	"time"
)

// ErrRebindClosed is returned by Client.Rebind when the connection is closed.
var ErrRebindClosed = errors.New("client closed")

// ErrRebindExpired is returned by Client.Rebind when ExpireAt is negative or
// not in the future. Zero is not an error: it disarms expiration.
var ErrRebindExpired = errors.New("rebind expiration must be zero or in the future")

// ErrRebindUserLimit is returned by Client.Rebind when the new user already has
// UserConnectionLimit connections on the node.
var ErrRebindUserLimit = errors.New("user connection limit reached")

// RebindOptions describe the identity a live connection takes over in Client.Rebind.
type RebindOptions struct {
	// UserID is the new user of the connection, empty for anonymous.
	UserID string
	// ExpireAt is the new expiration in unix seconds. Zero disarms expiration.
	ExpireAt int64
	// TokenIdentity is the identity of the token the connection holds now.
	TokenIdentity TokenIdentity
}

// rebindPresence is a subscribed channel whose presence moves on rebind.
type rebindPresence struct {
	channel  string
	ctx      ChannelContext
	connInfo []byte
}

// Rebind moves a live connection to another user without reconnecting.
//
// Channels are not touched: the caller decides which channels to keep. Join was
// published with the old user and stays so. Leave and publication ClientInfo
// carry the new user from the rebind on. Presence is rewritten synchronously
// before Rebind returns: node presence entries are moved from the old user to
// the new one, and map client presence entries are removed and added again. Map
// user-presence keys of the old user are not removed; they expire by TTL.
//
// The user index of the hub moves with the connection. connectionsInflight is
// unchanged. Expiration is re-armed from ExpireAt, and ExpireAt zero disarms it.
func (c *Client) Rebind(opts RebindOptions) error {
	nowUnix := time.Now().Unix()
	if opts.ExpireAt < 0 || (opts.ExpireAt > 0 && opts.ExpireAt <= nowUnix) {
		return ErrRebindExpired
	}
	limit := c.node.config.UserConnectionLimit
	if limit > 0 && opts.UserID != "" && opts.UserID != c.UserID() && len(c.node.hub.UserConnections(opts.UserID)) >= limit {
		return ErrRebindUserLimit
	}

	// Lock order: connectMu, then mu. Presence is written after mu is released,
	// still under connectMu, so close cannot overlap it.
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		return ErrRebindClosed
	}
	oldUser := c.user
	c.storeUser(opts.UserID)
	c.tokenIdentity = opts.TokenIdentity
	c.exp = min(opts.ExpireAt, nowUnix+maxTTLSeconds)
	if c.exp == 0 {
		c.nextExpire = 0
	} else {
		ttl := time.Duration(c.exp-nowUnix) * time.Second
		if c.clientSideRefresh {
			ttl += c.node.config.ClientExpiredCloseDelay
		}
		c.addExpireUpdate(ttl, false)
	}
	c.scheduleNextTimer()
	var presences []rebindPresence
	for channel, chCtx := range c.channels {
		if !channelHasFlag(chCtx.flags, flagSubscribed) {
			continue
		}
		if !channelHasFlag(chCtx.flags, flagEmitPresence|flagMapClientPresence|flagMapUserPresence) {
			continue
		}
		presences = append(presences, rebindPresence{channel: channel, ctx: chCtx, connInfo: c.info})
	}
	if c.authenticated {
		c.node.hub.rebind(c, oldUser, opts.UserID)
	}
	c.mu.Unlock()

	for _, p := range presences {
		c.rewritePresence(p, oldUser)
	}
	return nil
}

// rewritePresence moves the presence of one channel from oldUser to the user
// the connection has now. connectMu must be held, c.mu must not be: presence
// calls go to the PresenceManager and the map broker.
func (c *Client) rewritePresence(p rebindPresence, oldUser string) {
	newUser := c.UserID()
	if channelHasFlag(p.ctx.flags, flagEmitPresence) {
		if err := c.node.removePresence(p.channel, c.uid, oldUser); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error removing channel presence on rebind", map[string]any{"channel": p.channel, "client": c.uid, "error": err.Error()}))
		}
		info := &ClientInfo{ClientID: c.uid, UserID: newUser, ConnInfo: p.connInfo, ChanInfo: p.ctx.info}
		if err := c.node.addPresence(p.channel, c.uid, info); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding channel presence on rebind", map[string]any{"channel": p.channel, "client": c.uid, "error": err.Error()}))
		}
	}
	if p.ctx.mapClientPresenceChannel != "" {
		_, err := c.node.MapRemove(presenceContext(c.ctx), p.ctx.mapClientPresenceChannel, c.uid, MapRemoveOptions{})
		if err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error removing map client presence on rebind", map[string]any{"channel": p.channel, "client": c.uid, "error": err.Error()}))
		}
		info := &ClientInfo{ClientID: c.uid, UserID: newUser, ConnInfo: p.connInfo, ChanInfo: p.ctx.info}
		if err := c.addMapClientPresence(p.ctx.mapClientPresenceChannel, info); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding map client presence on rebind", map[string]any{"channel": p.channel, "client": c.uid, "error": err.Error()}))
		}
	}
	if p.ctx.mapUserPresenceChannel != "" {
		if err := c.addMapUserPresence(p.ctx.mapUserPresenceChannel); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding map user presence on rebind", map[string]any{"channel": p.channel, "client": c.uid, "error": err.Error()}))
		}
	}
}
