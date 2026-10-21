package centrifuge

import (
	"time"
)

// replyTTL returns the ttl a connect or refresh reply carries for a connection which expires in ttl seconds. With
// a refresh lead set, the client is told to refresh that much earlier, but never earlier than half of ttl, so a
// token close to its expiry, or a token source which hands back the same token, does not make it refresh in a loop.
func replyTTL(lead time.Duration, ttl int64) uint32 {
	leadSeconds := int64(lead / time.Second)
	if leadSeconds > 0 {
		ttl -= min(leadSeconds, ttl/2)
	}
	return uint32(ttl)
}
