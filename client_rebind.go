package centrifuge

import (
	"errors"
	"sync/atomic"
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

// testHookRebindBeforePresence, if set, runs in rewritePresence before a
// channel's presence moves. Tests use it to run an Unsubscribe at that point.
var testHookRebindBeforePresence atomic.Pointer[func(channel string)]

// RebindOptions describe the identity a live connection takes over in Client.Rebind.
type RebindOptions struct {
	// UserID is the new user of the connection, empty for anonymous.
	UserID string
	// ExpireAt is the new expiration in unix seconds. Zero disarms expiration.
	ExpireAt int64
	// TokenIdentity is the identity of the token the connection holds now.
	TokenIdentity TokenIdentity
	// Info is the new connection info, sent as ConnInfo in presence, join, leave
	// and publications. It always replaces the old info: nil clears it.
	Info []byte
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
// carry the new user and the new info from the rebind on. Presence is rewritten synchronously
// before Rebind returns: node presence entries are moved from the old user to
// the new one, and map client presence entries are removed and added again. Map
// user-presence keys of the old user are not removed; they expire by TTL.
//
// Map client presence emits a leave and then a join for the connection on
// rebind, because its entry is removed and added again. A channel unsubscribed
// while its presence moves is not left with a stale entry.
//
// Rebind holds connectMu while it rewrites presence, so a concurrent close of the
// same connection waits for that I/O, which is bounded by the presence backend
// timeouts. Rebind is rare (a login or a logout), so this is accepted.
//
// The user-connection limit is checked before the locks are taken, so concurrent
// rebinds of different connections to the same full user can exceed the limit
// by the number of racing rebinds.
//
// The user index of the hub moves with the connection. connectionsInflight is
// unchanged. Expiration is re-armed from ExpireAt, and ExpireAt zero disarms it.
func (c *Client) Rebind(opts RebindOptions) error {
	nowUnix := time.Now().Unix()
	if opts.ExpireAt < 0 || (opts.ExpireAt > 0 && opts.ExpireAt <= nowUnix) {
		return ErrRebindExpired
	}
	limit := c.node.config.UserConnectionLimit
	if limit > 0 && opts.UserID != "" && opts.UserID != c.UserID() && c.node.hub.numUserConnections(opts.UserID) >= limit {
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
	c.info = opts.Info
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

// connInfo returns the connection info under the read lock, because Rebind and
// refresh may replace it on a live client. Lock must not be held outside.
func (c *Client) connInfo() []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.info
}

// storeUser sets the user and its lock-free mirror read by UserID. Lock must be
// held outside.
func (c *Client) storeUser(user string) {
	c.user = user
	c.userAtomic.Store(&user)
}

// rewritePresence moves the presence of one channel from oldUser to the user
// the connection has now. connectMu must be held, c.mu must not be: presence
// calls go to the PresenceManager and the map broker.
//
// Unsubscribe deletes the channel under c.mu before it removes presence, and it
// reads the user lock-free, so it removes either the old or the new user. The
// recheck after the add covers the case where it ran before the add: if the
// channel is no longer subscribed, the entry just added is removed again.
func (c *Client) rewritePresence(p rebindPresence, oldUser string) {
	newUser := c.UserID()
	if f := testHookRebindBeforePresence.Load(); f != nil {
		(*f)(p.channel)
	}
	c.removeChannelPresence(p, oldUser)
	if _, err := c.updateChannelPresence(p.channel, p.ctx); err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error updating presence for channel on rebind", map[string]any{
			"channel": p.channel, "user": c.UserID(), "client": c.uid, "error": err.Error()}))
	}
	if c.isSubscribed(p.channel) {
		return
	}
	// Unsubscribed while the presence moved: undo what was just added.
	c.removeChannelPresence(p, newUser)
}

// removeChannelPresence removes the node presence of the connection on the
// channel for user, and its map client presence. Map user presence is not
// removed: like unsubscribe, it expires by TTL.
func (c *Client) removeChannelPresence(p rebindPresence, user string) {
	if channelHasFlag(p.ctx.flags, flagEmitPresence) {
		if err := c.node.removePresence(p.channel, c.uid, user); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error removing channel presence on rebind", map[string]any{"channel": p.channel, "client": c.uid, "error": err.Error()}))
		}
	}
	if p.ctx.mapClientPresenceChannel != "" {
		_, err := c.node.MapRemove(presenceContext(c.ctx), p.ctx.mapClientPresenceChannel, c.uid, MapRemoveOptions{})
		if err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error removing map client presence on rebind", map[string]any{"channel": p.channel, "client": c.uid, "error": err.Error()}))
		}
	}
}

// isSubscribed reports whether the channel is in c.channels with the subscribed flag.
func (c *Client) isSubscribed(channel string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	chCtx, ok := c.channels[channel]
	return ok && channelHasFlag(chCtx.flags, flagSubscribed)
}
