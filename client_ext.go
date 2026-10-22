package centrifuge

import (
	"sync/atomic"
)

// clientExt holds the state of the fork features that most connections do not set: the token identity and the
// extension state. It is allocated when one of them is set first, so a connection without them holds one pointer.
// The trade-off: a connection with a token identity pays one more allocation, 48 bytes in the size class of clientExt,
// which is 16 bytes more than holding the identity inline in Client, and the 40 bytes of a connection without them are
// saved. BenchmarkClientExt measures both cases. The token identity is guarded by Client.mu, and the extension state is
// atomic.
type clientExt struct {
	tokenIdentity TokenIdentity
	extState      atomic.Pointer[extStateBox]
}

// extension returns the state of the fork features of the connection, allocating it when it is not set yet.
// Setting the state twice at once keeps one of the two allocations.
func (c *Client) extension() *clientExt {
	if e := c.ext.Load(); e != nil {
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
	c.extension().tokenIdentity = identity
}

// tokenIdentityValue returns the token identity of the connection, which is zero when no state is allocated. Lock must
// be held outside.
func (c *Client) tokenIdentityValue() TokenIdentity {
	if e := c.ext.Load(); e != nil {
		return e.tokenIdentity
	}
	return TokenIdentity{}
}

// extStateBox wraps the value stored with SetExtState, so that it can be swapped
// atomically regardless of the dynamic type of the value.
type extStateBox struct {
	v any
}

// ExtState returns the value previously set with SetExtState, or nil if no value
// has been set. It is two atomic loads and is safe to call from any goroutine.
// Intended for applications to attach cheap per-connection state (instead of an
// AcquireStorage map entry). A connection that never sets it holds only the nil
// pointer to its state: the first SetExtState of the connection allocates the state,
// and each SetExtState allocates a value box. The recommended pattern is to
// call SetExtState from the transport reader goroutine, for example in OnCommandRead
// for the connect command, and read it later from any goroutine.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) ExtState() any {
	e := c.ext.Load()
	if e == nil {
		return nil
	}
	box := e.extState.Load()
	if box == nil {
		return nil
	}
	return box.v
}

// SetExtState attaches an arbitrary value to the connection. Values set this way are
// not visible in the connection storage map and are not affected by AcquireStorage.
// Setting is safe from any goroutine; the last Set wins.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) SetExtState(v any) {
	c.extension().extState.Store(&extStateBox{v: v})
}
