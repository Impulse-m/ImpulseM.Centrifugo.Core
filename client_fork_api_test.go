package centrifuge

import (
	"context"
	"sync"
	"testing"
	"time"

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