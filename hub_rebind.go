package centrifuge

// rebind moves an authenticated connection from the oldUser index entry to the
// newUser one. The two users may hash to different shards: then both shard locks
// are taken in index order, so two concurrent rebinds cannot deadlock. The
// caller holds c.mu. connectionsInflight is untouched: the connection stays one
// connection.
func (h *Hub) rebind(c *Client, oldUser, newUser string) {
	uid := c.ID()
	oldIdx := index(oldUser, numHubShards)
	newIdx := index(newUser, numHubShards)
	if oldIdx == newIdx {
		shard := h.connShards[oldIdx]
		shard.mu.Lock()
		shard.dropUser(uid, oldUser)
		shard.addUser(uid, newUser)
		shard.mu.Unlock()
		return
	}
	lowIdx := min(oldIdx, newIdx)
	highIdx := max(oldIdx, newIdx)
	h.connShards[lowIdx].mu.Lock()
	h.connShards[highIdx].mu.Lock()
	oldShard := h.connShards[oldIdx]
	newShard := h.connShards[newIdx]
	delete(oldShard.clients, uid)
	oldShard.dropUser(uid, oldUser)
	newShard.clients[uid] = c
	newShard.addUser(uid, newUser)
	h.connShards[highIdx].mu.Unlock()
	h.connShards[lowIdx].mu.Unlock()
}

// dropUser removes uid from the user index entry of user. Lock must be held.
func (h *connShard) dropUser(uid string, user string) {
	userConns, ok := h.users[user]
	if !ok {
		return
	}
	delete(userConns, uid)
	if len(userConns) == 0 {
		delete(h.users, user)
	}
}

// addUser adds uid to the user index entry of user. Lock must be held.
func (h *connShard) addUser(uid string, user string) {
	if _, ok := h.users[user]; !ok {
		h.users[user] = make(map[string]struct{})
	}
	h.users[user][uid] = struct{}{}
}
