package centrifuge

import (
	"time"
)

// mediumPositionState is the shared position sync state of a channelMedium. It is guarded by channelMedium.mu
// and is only used when the medium has SharedPositionSync enabled.
type mediumPositionState struct {
	// cachedTop is the last stream top fetched by a position check, cachedAt is the time (Unix Nanoseconds)
	// of that fetch. cachedAt is zero until the first fetch.
	cachedTop StreamPosition
	cachedAt  int64
	// lastPosition is a StreamPosition up to which every local subscriber has been updated: it is set after
	// a non-marker publication is handed to the Node. Only valid when hasLast is true.
	lastPosition StreamPosition
	hasLast      bool
	// suspectTop is a stream top seen by the previous check while the Node looked behind the stream. Only
	// valid when hasSuspect is true.
	suspectTop StreamPosition
	hasSuspect bool
	// regressTop is a stream top seen while the stream top was behind lastPosition in the same epoch, and a
	// client was ahead of it. Only valid when hasRegress is true. A second consecutive fetch with the same
	// regression confirms it (broker lost the stream tail, epoch kept).
	regressTop StreamPosition
	hasRegress bool
}

// notePublicationDelivered records a publication that the Node has handed to all local subscribers of the
// channel. It is called after handlePublication returns, so lastPosition never runs ahead of what subscribers
// received.
func (c *channelMedium) notePublicationDelivered(isInsufficientState bool, sp StreamPosition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if isInsufficientState {
		c.position.hasSuspect = false
		return
	}
	// Monotone within an epoch: a publication that was still in flight when a marker moved lastPosition
	// to the stream top must not move it back.
	c.advanceLastPositionLocked(sp)
	if c.position.hasSuspect && sp.Epoch == c.position.suspectTop.Epoch && sp.Offset >= c.position.suspectTop.Offset {
		c.position.hasSuspect = false
	}
}

// CheckPosition checks position of a single client against the channel stream top.
//
// The stream top is fetched at most once per checkDelay for the whole channel. Other calls within the
// interval use the cached top. checked is false only for calls made before the first fetch of the channel.
// When checked is true and err is nil, validPosition tells whether the client may keep its position.
//
// Node state is evaluated once per interval, on the call that fetches the top:
//   - the node is behind when the stream top is ahead of lastPosition (or of the client position when nothing
//     was broadcast yet). The first sighting only marks the node as suspect: a publication may still be in
//     flight between the stream and the Node. If the node is still behind one full checkDelay later,
//     insufficient state is broadcast to all channel subscribers on this node.
//   - an invalid client is marked individually only when the node delivered something it lacks, or when its
//     epoch differs from the stream top and the node is not suspect. Otherwise the client is behind only
//     because the node is, and the node-wide marker covers it.
//
// posTime is the client's positionCheckTime in Unix seconds: a client ahead of the stream top is only
// invalid when the top was fetched after the client was last checked or delivered to.
func (c *channelMedium) CheckPosition(historyMetaTTL time.Duration, clientPosition StreamPosition, checkDelay time.Duration, posTime int64) (checked bool, validPosition bool, err error) {
	nowUnixNano := c.nowFn().UnixNano()
	c.mu.Lock()
	needFetch := c.claimFetchLocked(nowUnixNano, checkDelay)
	c.mu.Unlock()

	var fetched StreamPosition
	if needFetch {
		fetched, err = c.streamTopWithRetry(historyMetaTTL)
		if err != nil {
			// Interval is consumed, position will be checked again on the next interval.
			return true, false, err
		}
	}

	c.mu.Lock()
	top, ok := c.cachedTopLocked(fetched, needFetch, nowUnixNano)
	if !ok {
		// Nothing fetched for this channel yet, the check happens on the next interval.
		c.mu.Unlock()
		return false, true, nil
	}
	sameEpoch := clientPosition.Epoch == top.Epoch
	if sameEpoch && clientPosition.Offset == top.Offset {
		c.mu.Unlock()
		return true, true, nil
	}
	if sameEpoch && clientPosition.Offset > top.Offset {
		valid, markerSent := c.checkAheadLocked(clientPosition, top, needFetch, posTime)
		c.mu.Unlock()
		if markerSent {
			c.broadcastInsufficientState()
		}
		return true, valid, nil
	}
	if c.checkNodeBehindLocked(clientPosition, top, needFetch) {
		c.mu.Unlock()
		c.broadcastInsufficientState()
		return true, false, nil
	}
	valid := c.clientValidLocked(clientPosition, top)
	c.mu.Unlock()
	return true, valid, nil
}

// claimFetchLocked reports whether the current check fetches the stream top. A fetch consumes the check
// interval. Must be called with c.mu held.
func (c *channelMedium) claimFetchLocked(nowUnixNano int64, checkDelay time.Duration) bool {
	needFetch := nowUnixNano-c.positionCheckTime >= checkDelay.Nanoseconds()
	if needFetch {
		c.positionCheckTime = nowUnixNano
	}
	return needFetch
}

// cachedTopLocked returns the stream top for the current check: the freshly fetched one when needFetch is set
// (and stores it in the cache), otherwise the cached one. ok is false when nothing was fetched for the channel
// yet. Must be called with c.mu held.
func (c *channelMedium) cachedTopLocked(fetched StreamPosition, needFetch bool, fetchedAt int64) (top StreamPosition, ok bool) {
	if needFetch {
		c.position.cachedTop = fetched
		c.position.cachedAt = fetchedAt
		return fetched, true
	}
	if c.position.cachedAt == 0 {
		return StreamPosition{}, false
	}
	return c.position.cachedTop, true
}

// checkNodeBehindLocked evaluates whether the Node is behind the stream top, for a client that is not ahead of
// the top in the same epoch. Nothing changes unless the check fetched the top. Must be called with c.mu held.
// Returns true when the Node is confirmed behind: lastPosition has then been moved to the stream top and the
// caller must broadcast insufficient state after releasing c.mu.
func (c *channelMedium) checkNodeBehindLocked(clientPosition StreamPosition, streamTop StreamPosition, needFetch bool) bool {
	if !needFetch {
		return false
	}
	// Floor is what the Node is known to have delivered. Without a broadcast the client position is used.
	floor := clientPosition
	if c.position.hasLast {
		floor = c.position.lastPosition
	}
	behind := streamTop.Epoch != floor.Epoch || streamTop.Offset > floor.Offset
	if !behind {
		c.position.hasSuspect = false
		return false
	}
	if c.position.hasSuspect && (floor.Epoch != c.position.suspectTop.Epoch || floor.Offset < c.position.suspectTop.Offset) {
		// Still behind one full interval after the first sighting: the Node is behind the stream.
		c.position.hasSuspect = false
		// Subscribers are about to resync from the stream top, so the Node is considered up to it.
		// Without this the same gap would be re-confirmed on every interval while the channel is quiet.
		c.advanceLastPositionLocked(streamTop)
		return true
	}
	c.position.suspectTop = streamTop
	c.position.hasSuspect = true
	return false
}

// clientValidLocked is the client verdict for a client behind the stream top, when the Node is not confirmed
// behind. Must be called with c.mu held.
func (c *channelMedium) clientValidLocked(clientPosition StreamPosition, streamTop StreamPosition) bool {
	behindDelivered := c.clientDeliveredLocked(clientPosition, false)
	staleEpoch := clientPosition.Epoch != streamTop.Epoch && !c.position.hasSuspect
	return !behindDelivered && !staleEpoch
}

// checkAheadLocked resolves a client which is ahead of the stream top in the same epoch. Must be called with
// c.mu held. Returns whether the client is valid and whether an insufficient state marker must be broadcast
// (after c.mu is released).
//
// Channel level (R1), once per actual fetch: the Node is behind the stream top when the top is behind
// lastPosition in the same epoch. The first sighting only marks it suspect. A second consecutive fetch with
// the same top confirms it: lastPosition moves to the top and the marker is broadcast.
//
// Client level (R2): a client at or behind what the Node delivered is valid, as R1 owns that case. Otherwise
// it is valid only when the top was not fetched after the client was last checked or delivered to.
func (c *channelMedium) checkAheadLocked(clientPosition, streamTop StreamPosition, needFetch bool, posTime int64) (bool, bool) {
	if needFetch {
		brokerBehindNode := c.position.hasLast && streamTop.Epoch == c.position.lastPosition.Epoch && streamTop.Offset < c.position.lastPosition.Offset
		if !brokerBehindNode {
			c.position.hasRegress = false
		} else if c.position.hasRegress && c.position.regressTop.Epoch == streamTop.Epoch {
			c.position.hasRegress = false
			c.setLastPositionLocked(streamTop)
			return false, true
		} else {
			c.position.regressTop = streamTop
			c.position.hasRegress = true
		}
	}
	if c.clientDeliveredLocked(clientPosition, true) {
		return true, false
	}
	fetchedAfterClient := c.position.cachedAt >= (posTime+1)*int64(time.Second)
	return !fetchedAfterClient, false
}

// clientDeliveredLocked reports whether the client is behind what the Node has delivered, in the epoch of
// lastPosition. The client must be strictly behind lastPosition, or at or behind it when includeEqual is set.
// Must be called with c.mu held.
func (c *channelMedium) clientDeliveredLocked(clientPosition StreamPosition, includeEqual bool) bool {
	if !c.position.hasLast || clientPosition.Epoch != c.position.lastPosition.Epoch {
		return false
	}
	if includeEqual {
		return clientPosition.Offset <= c.position.lastPosition.Offset
	}
	return clientPosition.Offset < c.position.lastPosition.Offset
}

// advanceLastPositionLocked moves lastPosition forward to sp: when nothing is known yet, when the epoch changed,
// or when sp is ahead in the same epoch. Must be called with c.mu held.
func (c *channelMedium) advanceLastPositionLocked(sp StreamPosition) {
	if !c.position.hasLast || sp.Epoch != c.position.lastPosition.Epoch || sp.Offset > c.position.lastPosition.Offset {
		c.setLastPositionLocked(sp)
	}
}

// setLastPositionLocked sets lastPosition. Must be called with c.mu held. Regression state is only meaningful
// within one epoch, so a change of epoch clears it.
func (c *channelMedium) setLastPositionLocked(sp StreamPosition) {
	if c.position.hasLast && sp.Epoch != c.position.lastPosition.Epoch {
		c.position.hasRegress = false
	}
	c.position.lastPosition = sp
	c.position.hasLast = true
}

// streamTopWithRetry fetches stream top of the channel (map stream top for map mediums), retrying once on error.
func (c *channelMedium) streamTopWithRetry(historyMetaTTL time.Duration) (StreamPosition, error) {
	streamTop, err := c.fetchStreamTop(historyMetaTTL)
	if err != nil {
		streamTop, err = c.fetchStreamTop(historyMetaTTL)
	}
	return streamTop, err
}

func (c *channelMedium) fetchStreamTop(historyMetaTTL time.Duration) (StreamPosition, error) {
	if c.isMap {
		return c.node.mapStreamTop(c.channel)
	}
	return c.node.streamTop(c.channel, historyMetaTTL)
}
