package centrifuge

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rebindPresenceRecorder records the user of the last presence entry added.
type rebindPresenceRecorder struct {
	*TestPresenceManager
	mu       sync.Mutex
	lastUser string
}

func (r *rebindPresenceRecorder) AddPresence(_ string, _ string, info *ClientInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastUser = info.UserID
	return nil
}

func (r *rebindPresenceRecorder) user() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastUser
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

	expireAt := time.Now().Add(time.Hour).Unix()
	require.NoError(t, client.Rebind(RebindOptions{UserID: "new", ExpireAt: expireAt}))

	client.mu.RLock()
	require.Equal(t, expireAt, client.exp)
	require.Greater(t, client.nextExpire, time.Now().UnixNano())
	client.mu.RUnlock()

	require.NoError(t, client.Rebind(RebindOptions{UserID: "new", ExpireAt: 0}))

	client.mu.RLock()
	require.Equal(t, int64(0), client.exp)
	require.Equal(t, int64(0), client.nextExpire)
	client.mu.RUnlock()
}

func TestClientRebindInfoAndTokenIdentity(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "old")
	info := []byte(`{"name":"a"}`)
	require.NoError(t, client.Rebind(RebindOptions{
		UserID:        "new",
		Info:          info,
		TokenIdentity: TokenIdentity{Generation: 7},
	}))
	require.Equal(t, info, client.Info())
	client.mu.RLock()
	require.Equal(t, uint64(7), client.tokenIdentity.Generation)
	client.mu.RUnlock()

	// Nil info keeps the current one.
	require.NoError(t, client.Rebind(RebindOptions{UserID: "new"}))
	require.Equal(t, info, client.Info())
	client.mu.RLock()
	require.Equal(t, uint64(0), client.tokenIdentity.Generation)
	client.mu.RUnlock()
}

func TestClientRebindPresenceAfterTick(t *testing.T) {
	recorder := &rebindPresenceRecorder{TestPresenceManager: NewTestPresenceManager()}
	node := nodeWithPresenceManager(recorder)
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "old")
	client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
		cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
	})
	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")
	require.Equal(t, "old", recorder.user())

	require.NoError(t, client.Rebind(RebindOptions{UserID: "new"}))
	client.updatePresence()
	require.Equal(t, "new", recorder.user())
}

// TestClientRebindRaces runs Rebind concurrently with UserID readers, the keyed
// hub user removal (which calls UserID under its own lock), a leave, a presence
// tick and close. It must finish with no race and no deadlock.
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			// Matches no subscriber, so the loop only reads users under the hub lock.
			keyed.removeSubscribersForUsers("key", []string{"nobody"}, nil)
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
}
