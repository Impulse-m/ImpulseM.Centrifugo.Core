package centrifuge

import (
	"sync/atomic"
)

// clientExt holds the state of the fork features that most connections do not set: the token identity and the
// extension state. It is allocated when one of them is set first, so a connection without them holds one pointer.
// The token identity is guarded by Client.mu, and the extension state is atomic.
type clientExt struct {
	tokenIdentity TokenIdentity
	extState      atomic.Pointer[extStateBox]
}

// extension returns the state of the fork features of the connection, allocating it when create is set. Setting
// the state twice at once keeps one of the two allocations.
func (c *Client) extension(create bool) *clientExt {
	if e := c.ext.Load(); e != nil || !create {
		return e
	}
	e := &clientExt{}
	if c.ext.CompareAndSwap(nil, e) {
		return e
	}
	return c.ext.Load()
}

// setTokenIdentity replaces the token identity of the connection. Lock must be held outside. A zero identity on a
// connection without the state allocates nothing.
func (c *Client) setTokenIdentity(identity TokenIdentity) {
	if identity == (TokenIdentity{}) && c.ext.Load() == nil {
		return
	}
	c.extension(true).tokenIdentity = identity
}
