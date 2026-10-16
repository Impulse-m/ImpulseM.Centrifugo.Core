package centrifuge

// historyRefusedAtCallback decides in the history callback whether a history
// reply must be refused for a server tags filter, the second half of the check
// in handleHistory. The application decides on the history after that check,
// so the subscription may have changed meanwhile. presentAtCheck and genAtCheck
// are what handleHistory saw on entry, under the same lock.
//
// The history is served only in two cases: the channel has a committed
// subscription without a server tags filter, or the channel had no subscription
// at any moment since entry. The second needs the channel absent from both
// maps on entry and at the callback, and the subscription generation counter
// unchanged: every entry is created with a fresh generation taken in the same
// critical section as its insert, so no entry was created in between. Anything
// else is refused, and the client may retry.
//
// It takes c.mu itself and must be called without it held.
func (c *Client) historyRefusedAtCallback(channel string, presentAtCheck bool, genAtCheck uint64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	channelContext, subscribed := c.channels[channel]
	if subscribed {
		if !channelHasFlag(channelContext.flags, flagSubscribed) {
			// A subscribe is in flight, it is not committed yet.
			return true
		}
		return channelHasFlag(channelContext.flags, flagServerTagsFilter)
	}
	if _, loading := c.mapSubscribing[channel]; loading {
		return true
	}
	return presentAtCheck || c.subGenCounter.Load() != genAtCheck
}
