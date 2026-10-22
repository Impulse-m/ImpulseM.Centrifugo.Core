package centrifuge

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientExtStateDefaultNil(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "42")
	require.Nil(t, client.ExtState())
}

func TestClientExtStateSetGetAcrossGoroutines(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "42")

	type state struct {
		counter int
	}

	setDone := make(chan struct{})
	go func() {
		client.SetExtState(&state{counter: 7})
		close(setDone)
	}()
	<-setDone

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.Equal(t, &state{counter: 7}, client.ExtState())
		}()
	}
	wg.Wait()
}

func TestClientExtStateConcurrentSetAndGet(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "42")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 1000 {
			if i%2 == 0 {
				client.SetExtState(i)
			} else {
				client.SetExtState("value")
			}
		}
	}()
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				_ = client.ExtState()
			}
		}()
	}
	wg.Wait()
	require.NotNil(t, client.ExtState())
}

func TestNodeOnCommandReadStarted(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Credentials: &Credentials{
				UserID: "12",
			},
		}, nil
	})

	startedCh := make(chan time.Time, 2)

	node.OnCommandRead(func(client *Client, event CommandReadEvent) error {
		if event.Command.Connect != nil || event.Command.Subscribe != nil {
			startedCh <- event.Started
		}
		return nil
	})

	client := newTestClientV2(t, node, "42")

	before := time.Now()
	client.HandleCommand(&protocol.Command{
		Id: 1,
		Connect: &protocol.ConnectRequest{
			Token: "123",
		},
	}, 0)
	select {
	case started := <-startedCh:
		require.False(t, started.IsZero())
		require.False(t, started.Before(before))
	case <-time.After(time.Second):
		require.Fail(t, "timeout connect")
	}

	before = time.Now()
	client.HandleCommand(&protocol.Command{
		Id: 2,
		Subscribe: &protocol.SubscribeRequest{
			Channel: "channel",
		},
	}, 0)
	select {
	case started := <-startedCh:
		require.False(t, started.IsZero())
		require.False(t, started.Before(before))
	case <-time.After(time.Second):
		require.Fail(t, "timeout subscribe")
	}
}

func TestStoreUserSameUserNoAlloc(t *testing.T) {
	client := &Client{}
	client.storeUser("user-42")
	allocs := testing.AllocsPerRun(100, func() {
		client.storeUser("user-42")
	})
	require.Zero(t, allocs)
	require.Equal(t, "user-42", client.UserID())
}

// TestClientExtAllocatedOnlyWhenSet checks that a connection without a token identity or extension state holds no
// fork state, and that the first non-zero identity allocates it.
func TestClientExtAllocatedOnlyWhenSet(t *testing.T) {
	client := &Client{}
	client.storeUser("user-42")
	client.mu.Lock()
	client.setTokenIdentity(TokenIdentity{})
	client.mu.Unlock()
	require.Nil(t, client.ext.Load())
	require.Nil(t, client.ExtState())

	client.mu.Lock()
	client.setTokenIdentity(TokenIdentity{Generation: 1})
	client.mu.Unlock()
	require.NotNil(t, client.ext.Load())
	require.Equal(t, uint64(1), client.TokenIdentity().Generation)
}

// TestClientSizeWithoutForkState checks the size of Client on 64-bit platforms. The token identity and the extension
// state live in one pointer, so the fields of the fork features add nothing to a connection that does not set them.
// A change of the size is a change of the memory of every connection and must be made on purpose.
func TestClientSizeWithoutForkState(t *testing.T) {
	t.Parallel()
	if strconv.IntSize != 64 {
		t.Skip("the size is checked on 64-bit platforms only")
	}
	require.Equal(t, uintptr(552), unsafe.Sizeof(Client{}))
}
