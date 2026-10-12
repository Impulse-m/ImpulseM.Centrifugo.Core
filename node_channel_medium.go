package centrifuge

// ensureChannelMedium creates the channel medium of ch when the channel medium layer is enabled for the
// channel and no medium exists for it yet. It returns the medium it created, or nil when nothing was created.
// Called with subLock(ch) held.
func (n *Node) ensureChannelMedium(ch string, isMap bool) (*channelMedium, error) {
	if n.config.GetChannelMediumOptions == nil {
		return nil, nil
	}
	mediumOptions := n.config.GetChannelMediumOptions(ch)
	if !mediumOptions.isMediumEnabled() {
		return nil, nil
	}
	// Medium may still exist here if its teardown is pending in subDissolver. Keep it: it has been
	// receiving all channel publications so far, replacing it would leak the old medium.
	// Mediums of ch are stored and deleted only under subLock(ch), held here, so the check
	// below cannot race with another creation for the same channel.
	mediumMu := n.mediumLock(ch)
	mediumMu.Lock()
	_, exists := n.mediumShard(ch)[ch]
	mediumMu.Unlock()
	if exists {
		return nil, nil
	}
	medium, err := newChannelMedium(ch, n, mediumOptions)
	if err != nil {
		return nil, err
	}
	medium.isMap = isMap
	medium.nowFn = n.nowTimeGetter
	mediumMu.Lock()
	n.mediumShard(ch)[ch] = medium
	mediumMu.Unlock()
	n.metrics.incChannelMediums()
	return medium, nil
}

// dropCreatedMedium tears down the medium created by a subscribe that failed to
// complete. A medium that was already there before (kept for a pending teardown)
// is left alone. Called with subLock(ch) held. Nil medium is a no-op.
func (n *Node) dropCreatedMedium(ch string, medium *channelMedium) {
	if medium == nil {
		return
	}
	mediumMu := n.mediumLock(ch)
	mediumMu.Lock()
	defer mediumMu.Unlock()
	if n.mediumShard(ch)[ch] != medium {
		return
	}
	medium.close()
	delete(n.mediumShard(ch), ch)
	n.metrics.decChannelMediums()
}
