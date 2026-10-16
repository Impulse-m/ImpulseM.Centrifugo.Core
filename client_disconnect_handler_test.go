package centrifuge

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientOnDisconnectRacesClose(t *testing.T) {
	for i := 0; i < 100; i++ {
		node := defaultNodeNoHandlers()
		client := newTestConnectedClientV2(t, node, "42")

		var calls atomic.Int32
		var closeErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			client.OnDisconnect(func(DisconnectEvent) {
				calls.Add(1)
			})
		}()
		go func() {
			defer wg.Done()
			closeErr = client.close(DisconnectForceNoReconnect)
		}()
		wg.Wait()

		require.NoError(t, closeErr)
		require.LessOrEqual(t, calls.Load(), int32(1))
		require.NoError(t, node.Shutdown(context.Background()))
	}
}

func TestClientOnDisconnectAfterCloseNotCalled(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "42")
	require.NoError(t, client.close(DisconnectForceNoReconnect))

	var calls atomic.Int32
	client.OnDisconnect(func(DisconnectEvent) {
		calls.Add(1)
	})
	require.Equal(t, int32(0), calls.Load())
}

func TestClientOnDisconnectFromConnectHandler(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	var calls atomic.Int32
	node.OnConnect(func(client *Client) {
		client.OnDisconnect(func(DisconnectEvent) {
			calls.Add(1)
		})
	})

	client := newTestConnectedClientV2(t, node, "42")
	require.NoError(t, client.close(DisconnectForceNoReconnect))
	require.Equal(t, int32(1), calls.Load())
}

func TestClientOnDisconnectNoAlloc(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "42")
	handler := func(DisconnectEvent) {}
	allocs := testing.AllocsPerRun(1000, func() {
		client.OnDisconnect(handler)
	})
	require.Zero(t, allocs)
}
