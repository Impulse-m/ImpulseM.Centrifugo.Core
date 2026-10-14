package centrifuge

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// rebindPresenceRecorder records the users of the presence entries written and
// removed by the node.
type rebindPresenceRecorder struct {
	*TestPresenceManager
	mu        sync.Mutex
	added     string
	addedInfo []byte
	removed   string
}

func (r *rebindPresenceRecorder) AddPresence(_ string, _ string, info *ClientInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.added = info.UserID
	r.addedInfo = info.ConnInfo
	return nil
}

func (r *rebindPresenceRecorder) info() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.addedInfo
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

// rebindInfoBroker keeps the ClientInfo of the last publication and of the last
// leave the node sent. The memory broker does the work.
type rebindInfoBroker struct {
	*MemoryBroker
	mu        sync.Mutex
	published *ClientInfo
	left      *ClientInfo
}

func (b *rebindInfoBroker) Publish(ch string, data []byte, opts PublishOptions) (PublishResult, error) {
	b.mu.Lock()
	b.published = opts.ClientInfo
	b.mu.Unlock()
	return b.MemoryBroker.Publish(ch, data, opts)
}

func (b *rebindInfoBroker) PublishLeave(ch string, info *ClientInfo) error {
	b.mu.Lock()
	b.left = info
	b.mu.Unlock()
	return b.MemoryBroker.PublishLeave(ch, info)
}

func (b *rebindInfoBroker) lastPublished() *ClientInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.published
}

func (b *rebindInfoBroker) lastLeft() *ClientInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.left
}

func newRebindInfoNode(t *testing.T, presenceManager PresenceManager) (*Node, *rebindInfoBroker) {
	node, err := New(Config{})
	require.NoError(t, err)
	memory, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	broker := &rebindInfoBroker{MemoryBroker: memory}
	node.SetBroker(broker)
	node.SetPresenceManager(presenceManager)
	require.NoError(t, node.Run())
	return node, broker
}

// publishInfo publishes from the client to the channel and returns the
// ClientInfo the node published with.
func publishInfo(t *testing.T, client *Client, broker *rebindInfoBroker) *ClientInfo {
	rwWrapper := testReplyWriterWrapper()
	err := client.handlePublish(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{}`),
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	return broker.lastPublished()
}

// TestRebindReplacesInfo checks that a Rebind replaces the connection info in
// the presence, the publications and the leave of the connection. Nil clears it.
func TestRebindReplacesInfo(t *testing.T) {
	recorder := &rebindPresenceRecorder{TestPresenceManager: NewTestPresenceManager()}
	node, broker := newRebindInfoNode(t, recorder)
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "old")
	client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
		cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true, EmitJoinLeave: true}}, nil)
	})
	client.OnPublish(func(event PublishEvent, cb PublishCallback) {
		cb(PublishReply{Options: PublishOptions{ClientInfo: event.ClientInfo}}, nil)
	})
	connectClientV2(t, client)
	client.mu.Lock()
	client.info = []byte(`{"name":"old"}`)
	client.mu.Unlock()
	subscribeClientV2(t, client, "test")
	require.Equal(t, []byte(`{"name":"old"}`), recorder.info())
	require.Equal(t, []byte(`{"name":"old"}`), publishInfo(t, client, broker).ConnInfo)

	// A token rebind without expiration shows its info in presence and publications.
	require.NoError(t, client.Rebind(RebindOptions{UserID: "new", Info: []byte(`{"name":"new"}`)}))
	require.Equal(t, []byte(`{"name":"new"}`), recorder.info())
	require.Equal(t, []byte(`{"name":"new"}`), publishInfo(t, client, broker).ConnInfo)

	// An expiring rebind replaces the info too.
	expireAt := time.Now().Add(time.Hour).Unix()
	require.NoError(t, client.Rebind(RebindOptions{UserID: "new", ExpireAt: expireAt, Info: []byte(`{"name":"expiring"}`)}))
	require.Equal(t, []byte(`{"name":"expiring"}`), recorder.info())
	require.Equal(t, []byte(`{"name":"expiring"}`), publishInfo(t, client, broker).ConnInfo)

	// The leave carries the user and the info the connection has when it leaves.
	client.Unsubscribe("test")
	require.Equal(t, "new", broker.lastLeft().UserID)
	require.Equal(t, []byte(`{"name":"expiring"}`), broker.lastLeft().ConnInfo)

	// Nil clears the info of an anonymous connection in presence, publications and leave.
	subscribeClientV2(t, client, "test")
	require.NoError(t, client.Rebind(RebindOptions{UserID: ""}))
	require.Nil(t, recorder.info())
	require.Nil(t, publishInfo(t, client, broker).ConnInfo)
	client.Unsubscribe("test")
	require.Equal(t, "", broker.lastLeft().UserID)
	require.Nil(t, broker.lastLeft().ConnInfo)
}

// heldPresenceRecorder holds the first AddPresence of the node until release is
// closed, and keeps the presence entry of each client.
type heldPresenceRecorder struct {
	*TestPresenceManager
	mu      sync.Mutex
	hold    bool
	entered chan struct{}
	release chan struct{}
	entries map[string]*ClientInfo
}

func (r *heldPresenceRecorder) AddPresence(_ string, clientID string, info *ClientInfo) error {
	r.mu.Lock()
	hold := r.hold
	r.hold = false
	r.mu.Unlock()
	if hold {
		close(r.entered)
		<-r.release
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[clientID] = info
	return nil
}

func (r *heldPresenceRecorder) RemovePresence(_ string, clientID string, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, clientID)
	return nil
}

func (r *heldPresenceRecorder) entry(clientID string) *ClientInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.entries[clientID]
}

// TestRebindDuringSubscribePresenceAdd rebinds a connection while its subscribe
// adds presence, before the channel is committed. The presence the subscribe
// adds carries the identity read before the rebind, so the entry is moved to the
// new identity after the commit.
func TestRebindDuringSubscribePresenceAdd(t *testing.T) {
	recorder := &heldPresenceRecorder{
		TestPresenceManager: NewTestPresenceManager(),
		hold:                true,
		entered:             make(chan struct{}),
		release:             make(chan struct{}),
		entries:             map[string]*ClientInfo{},
	}
	node := nodeWithPresenceManager(recorder)
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "old")
	client.mu.Lock()
	client.info = []byte(`{"name":"old"}`)
	client.mu.Unlock()

	subscribed := make(chan error, 1)
	go func() {
		subscribed <- client.Subscribe("test", WithEmitPresence(true))
	}()
	<-recorder.entered
	require.NoError(t, client.Rebind(RebindOptions{UserID: "new", Info: []byte(`{"name":"new"}`)}))
	close(recorder.release)
	require.NoError(t, <-subscribed)

	entry := recorder.entry(client.uid)
	require.NotNil(t, entry)
	require.Equal(t, "new", entry.UserID)
	require.Equal(t, []byte(`{"name":"new"}`), entry.ConnInfo)
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

// newMemoryPresenceNode returns a node with the in-memory presence manager, so
// the presence entries of a channel can be inspected directly.
func newMemoryPresenceNode(t *testing.T) *Node {
	node, err := New(Config{LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	pm, err := NewMemoryPresenceManager(node, MemoryPresenceManagerConfig{})
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	require.NoError(t, node.Run())
	return node
}

// requireNoPresence waits until the channel has no memory presence entry. A
// stale entry never goes away, so the wait only ends early on the correct state.
func requireNoPresence(t *testing.T, node *Node, channel string) {
	t.Helper()
	require.Eventually(t, func() bool {
		res, err := node.Presence(channel)
		return err == nil && len(res.Presence) == 0
	}, 5*time.Second, 5*time.Millisecond)
}

// TestClientRebindUnsubscribeDuringPresenceMove runs an Unsubscribe of the channel
// at the point where Rebind starts moving its presence, so the unsubscribe removes
// presence before the new user's entry is written. Afterwards the channel has no
// presence entry, and the client is not subscribed.
func TestClientRebindUnsubscribeDuringPresenceMove(t *testing.T) {
	node := newMemoryPresenceNode(t)
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "old")
	client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
		cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
	})
	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")
	res, err := node.Presence("test")
	require.NoError(t, err)
	require.Len(t, res.Presence, 1)

	var once sync.Once
	hook := func(channel string) {
		once.Do(func() { client.Unsubscribe(channel) })
	}
	testHookRebindBeforePresence.Store(&hook)
	t.Cleanup(func() { testHookRebindBeforePresence.Store(nil) })

	require.NoError(t, client.Rebind(RebindOptions{UserID: "new"}))
	testHookRebindBeforePresence.Store(nil)

	require.False(t, client.isSubscribed("test"))
	requireNoPresence(t, node, "test")
}

// TestClientRebindSubscribeUnsubscribeLoop races Rebind with repeated server-side
// subscribe and leave of a channel with presence. Once the loop is over, the
// channel has no presence entry.
func TestClientRebindSubscribeUnsubscribeLoop(t *testing.T) {
	node := newMemoryPresenceNode(t)
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "a")
	connectClientV2(t, client)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = client.Subscribe("loop", WithEmitPresence(true))
			client.Unsubscribe("loop")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			user := "a"
			if i%2 == 1 {
				user = "b"
			}
			if err := client.Rebind(RebindOptions{UserID: user}); err != nil {
				t.Errorf("unexpected rebind error: %v", err)
				return
			}
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
		t.Fatal("deadlock: Rebind racing with subscribe and leave did not finish")
	}

	require.False(t, client.isSubscribed("loop"))
	requireNoPresence(t, node, "loop")
}

// unsubscribeOnAddPresence runs an Unsubscribe of the channel from inside the
// first AddPresence call, after updateChannelPresence has checked the channel is
// still subscribed and before the entry is written.
type unsubscribeOnAddPresence struct {
	*MemoryPresenceManager
	client *Client
	armed  atomic.Bool
}

func (p *unsubscribeOnAddPresence) AddPresence(ch string, clientID string, info *ClientInfo) error {
	if p.armed.CompareAndSwap(true, false) {
		p.client.Unsubscribe(ch)
	}
	return p.MemoryPresenceManager.AddPresence(ch, clientID, info)
}

// TestClientRebindUnsubscribeInsideAddPresence covers the window the subscription
// recheck exists for: an unsubscribe lands between the membership check and the
// add of the moved presence. The entry written after the unsubscribe must be
// removed again, so the channel ends with no presence entry.
func TestClientRebindUnsubscribeInsideAddPresence(t *testing.T) {
	node, err := New(Config{LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	mem, err := NewMemoryPresenceManager(node, MemoryPresenceManagerConfig{})
	require.NoError(t, err)
	pm := &unsubscribeOnAddPresence{MemoryPresenceManager: mem}
	node.SetPresenceManager(pm)
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "old")
	client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
		cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
	})
	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")
	pm.client = client
	pm.armed.Store(true)

	require.NoError(t, client.Rebind(RebindOptions{UserID: "new"}))
	require.False(t, pm.armed.Load(), "the unsubscribe hook must have run during Rebind")

	require.False(t, client.isSubscribed("test"))
	requireNoPresence(t, node, "test")
}
