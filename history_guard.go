package centrifuge

// historyCheckAtEntry is the check in handleHistory, before the application
// decides on the history. It reads the subscription state under one c.mu.RLock:
// refused is true for a subscription with a server tags filter; present is true
// when the channel has an entry in c.channels (a reservation or a committed
// subscription) or in c.mapSubscribing; gen is the subscription generation
// counter. The callback gets present and gen back in historyRefusedAtCallback.
func (c *Client) historyCheckAtEntry(channel string) (refused bool, present bool, gen uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	channelContext, hasEntry := c.channels[channel]
	_, loading := c.mapSubscribing[channel]
	refused = hasEntry && channelHasFlag(channelContext.flags, flagServerTagsFilter)
	present = hasEntry || loading
	gen = c.subGenCounter.Load()
	return refused, present, gen
}

// historyRefusedAtCallback decides in the history callback whether a history
// reply must be refused for a server tags filter, the second half of the check
// in historyCheckAtEntry. The application decides on the history after that
// check, so the subscription may have changed meanwhile. presentAtCheck and
// genAtCheck are what historyCheckAtEntry returned on entry.
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
