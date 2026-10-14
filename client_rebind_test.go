package centrifuge

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rebindPresenceRecorder records the users of the presence entries written and
// removed by the node.
type rebindPresenceRecorder struct {
	*TestPresenceManager
	mu      sync.Mutex
	added   string
	removed string
}

func (r *rebindPresenceRecorder) AddPresence(_ string, _ string, info *ClientInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.added = info.UserID
	return nil
}

func (r *rebindPresenceRecorder) RemovePresence(_ string, _ string, userID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed = userID
	return nil
}

func (r *rebindPresenceRecorder) users() (string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.added, r.removed
}

func TestClientRebindClosed(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "old")
	require.NoError(t, client.close(DisconnectForceNoReconnect))

	err := client.Rebind(RebindOptions{UserID: "new"})
	require.ErrorIs(t, err, ErrRebindClosed)
	require.Equal(t, "old", client.UserID())
}

func TestClientRebindExpireAt(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "old")

	ttl := time.Hour
	expireAt := time.Now().Add(ttl).Unix()
	require.NoError(t, client.Rebind(RebindOptions{UserID: "new", ExpireAt: expireAt}))

	client.mu.RLock()
	require.Equal(t, expireAt, client.exp)
	// Not a client-side refresh connection: no expiration delay is added.
	require.InDelta(t, float64(time.Now().Add(ttl).UnixNano()), float64(client.nextExpire), float64(2*time.Second))
	client.mu.RUnlock()

	require.NoError(t, client.Rebind(RebindOptions{UserID: "new", ExpireAt: 0}))

	client.mu.RLock()
	require.Equal(t, int64(0), client.exp)
	require.Equal(t, int64(0), client.nextExpire)
	client.mu.RUnlock()
}

func TestClientRebindExpireAtClientSideRefresh(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "old")
	client.mu.Lock()
	client.clientSideRefresh = true
	client.mu.Unlock()

	ttl := time.Hour
	expireAt := time.Now().Add(ttl).Unix()
	require.NoError(t, client.Rebind(RebindOptions{UserID: "new", ExpireAt: expireAt}))

	delay := node.config.ClientExpiredCloseDelay
	require.Greater(t, delay, time.Duration(0))
	client.mu.RLock()
	require.InDelta(t, float64(time.Now().Add(ttl+delay).UnixNano()), float64(client.nextExpire), float64(2*time.Second))
	client.mu.RUnlock()
}

func TestClientRebindExpiredRejected(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "old")

	require.ErrorIs(t, client.Rebind(RebindOptions{UserID: "new", ExpireAt: -1}), ErrRebindExpired)
	past := time.Now().Add(-time.Minute).Unix()
	require.ErrorIs(t, client.Rebind(RebindOptions{UserID: "new", ExpireAt: past}), ErrRebindExpired)

	// Rejected rebinds change nothing.
	require.Equal(t, "old", client.UserID())
	require.Len(t, node.hub.UserConnections("old"), 1)
}

func TestClientRebindTokenIdentity(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "old")
	require.NoError(t, client.Rebind(RebindOptions{
		UserID:        "new",
		TokenIdentity: TokenIdentity{Generation: 7},
	}))
	client.mu.RLock()
	require.Equal(t, uint64(7), client.tokenIdentity.Generation)
	client.mu.RUnlock()

	require.NoError(t, client.Rebind(RebindOptions{UserID: "new"}))
	client.mu.RLock()
	require.Equal(t, uint64(0), client.tokenIdentity.Generation)
	client.mu.RUnlock()
}

func TestClientRebindUserLimit(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.config.UserConnectionLimit = 1

	a := newTestConnectedClientV2(t, node, "a")
	b := newTestConnectedClientV2(t, node, "b")

	// "a" already has its one connection: b cannot take the user.
	require.ErrorIs(t, b.Rebind(RebindOptions{UserID: "a"}), ErrRebindUserLimit)
	require.Equal(t, "b", b.UserID())
	require.Len(t, node.hub.UserConnections("a"), 1)

	// Rebinding to the user the connection already has is not a new connection.
	require.NoError(t, a.Rebind(RebindOptions{UserID: "a"}))
	// Anonymous connections are not limited.
	require.NoError(t, b.Rebind(RebindOptions{UserID: ""}))
	require.NoError(t, b.Rebind(RebindOptions{UserID: "c"}))
}

func TestClientRebindPresenceSynchronous(t *testing.T) {
	recorder := &rebindPresenceRecorder{TestPresenceManager: NewTestPresenceManager()}
	node := nodeWithPresenceManager(recorder)
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "old")
	client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
		cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
	})
	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")
	added, _ := recorder.users()
	require.Equal(t, "old", added)

	require.NoError(t, client.Rebind(RebindOptions{UserID: "new"}))
	// No presence tick: presence is rewritten before Rebind returns.
	added, removed := recorder.users()
	require.Equal(t, "new", added)
	require.Equal(t, "old", removed)
}

// TestClientRebindRaces runs Rebind concurrently with UserID readers, the keyed
// hub user removal (which calls UserID under its own lock), leave and subscribe
// on a second channel, a presence tick and close. It must finish with no race
// and no deadlock, and leave the hub empty.
func TestClientRebindRaces(t *testing.T) {
	node := nodeWithPresenceManager(NewTestPresenceManager())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "a")
	client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
		cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true, EmitJoinLeave: true}}, nil)
	})
	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	keyed := newKeyedHub()
	keyed.addSubscriber("key", client)

	rebindResults := make(chan error, 200)
	halfway := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if i == 100 {
				close(halfway)
			}
			user := "a"
			if i%2 == 1 {
				user = "b"
			}
			var expireAt int64
			if i%3 == 0 {
				expireAt = time.Now().Add(time.Hour).Unix()
			}
			rebindResults <- client.Rebind(RebindOptions{UserID: user, ExpireAt: expireAt})
		}
	}()

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				user := client.UserID()
				if user != "a" && user != "b" && user != "" {
					t.Errorf("unexpected user %q", user)
					return
				}
			}
		}()
	}

	// Alternates the users the keyed hub removes, so its delete path runs while
	// the connection changes user.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			keyed.addSubscriber("key", client)
			user := "a"
			if i%2 == 1 {
				user = "b"
			}
			keyed.removeSubscribersForUsers("key", []string{user}, nil)
		}
	}()

	// Matches no subscriber, so the loop only reads users under the hub lock.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			keyed.removeSubscribersForUsers("key", []string{"nobody"}, nil)
		}
	}()

	// Subscribe and leave on a second channel with presence.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = client.Subscribe("test2", WithEmitPresence(true))
			client.Unsubscribe("test2")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		client.Unsubscribe("test")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			client.updatePresence()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// Close while the rebinds are still running, not before them.
		<-halfway
		_ = client.close(DisconnectForceNoReconnect)
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock: Rebind racing with readers, keyed hub, leave, presence or close did not finish")
	}

	close(rebindResults)
	for err := range rebindResults {
		if err != nil && !errors.Is(err, ErrRebindClosed) {
			t.Fatalf("unexpected rebind error: %v", err)
		}
	}

	require.Equal(t, 0, node.hub.NumClients())
	require.Equal(t, 0, node.hub.NumUsers())
	for _, shard := range node.hub.connShards {
		shard.mu.RLock()
		require.Empty(t, shard.clients)
		require.Empty(t, shard.users)
		shard.mu.RUnlock()
	}
}

// TestClientRebindCrossShardOpposite moves two connections across hub shards in
// opposite directions at the same time: A from shard s1 to s2 while B moves from
// s2 to s1. Both must end indexed once, under their final user.
func TestClientRebindCrossShardOpposite(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	u1 := "opposite-1"
	u2 := findUserInShard(u1, false)
	u1b := findUserInShard(u1, true)
	u2b := findUserInShard(u2, true)
	require.NotEqual(t, index(u1, numHubShards), index(u2, numHubShards))

	a := newTestConnectedClientV2(t, node, u1)
	b := newTestConnectedClientV2(t, node, u2b)

	const rounds = 100
	var wg sync.WaitGroup
	errs := make(chan error, 2*rounds)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			next := u2
			if i%2 == 1 {
				next = u1
			}
			errs <- a.Rebind(RebindOptions{UserID: next})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			next := u1b
			if i%2 == 1 {
				next = u2b
			}
			errs <- b.Rebind(RebindOptions{UserID: next})
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock: opposite cross-shard rebinds did not finish")
	}
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	// The last round (odd index) moves A to u1 and B to u2b.
	require.Equal(t, u1, a.UserID())
	require.Equal(t, u2b, b.UserID())
	require.Len(t, node.hub.UserConnections(u1), 1)
	require.Len(t, node.hub.UserConnections(u2b), 1)
	require.Len(t, node.hub.UserConnections(u2), 0)
	require.Len(t, node.hub.UserConnections(u1b), 0)
	require.Equal(t, 2, node.hub.NumClients())
	require.Equal(t, 2, node.hub.NumUsers())

	// Each connection is in exactly one shard, the one of its final user.
	for _, c := range []*Client{a, b} {
		found := 0
		for i, shard := range node.hub.connShards {
			shard.mu.RLock()
			_, ok := shard.clients[c.ID()]
			shard.mu.RUnlock()
			if ok {
				found++
				require.Equal(t, index(c.UserID(), numHubShards), i)
			}
		}
		require.Equal(t, 1, found)
	}
}
