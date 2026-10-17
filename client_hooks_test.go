package centrifuge

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnDisconnectIfConnectedRunsOnLaterClose(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "42")

	var calls atomic.Int32
	installed := client.OnDisconnectIfConnected(func(DisconnectEvent) {
		calls.Add(1)
	})
	require.True(t, installed)
	require.Equal(t, int32(0), calls.Load())

	require.NoError(t, client.close(DisconnectForceNoReconnect))
	require.Equal(t, int32(1), calls.Load())
}

func TestOnDisconnectIfConnectedClosedClientReturnsFalse(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "42")
	require.NoError(t, client.close(DisconnectForceNoReconnect))

	var calls atomic.Int32
	installed := client.OnDisconnectIfConnected(func(DisconnectEvent) {
		calls.Add(1)
	})
	require.False(t, installed)
	require.NoError(t, client.close(DisconnectForceNoReconnect))
	require.Equal(t, int32(0), calls.Load())
}

func TestOnDisconnectIfConnectedRacesCloseNeverLosesHandler(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	for i := 0; i < 1000; i++ {
		client := newTestConnectedClientV2(t, node, "42")

		var calls atomic.Int32
		var installed bool
		var closeErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			installed = client.OnDisconnectIfConnected(func(DisconnectEvent) {
				calls.Add(1)
			})
		}()
		go func() {
			defer wg.Done()
			closeErr = client.close(DisconnectForceNoReconnect)
		}()
		wg.Wait()

		require.NoError(t, closeErr)
		if installed {
			require.Equal(t, int32(1), calls.Load(), "iteration %d: handler installed but not run on close", i)
		} else {
			require.Equal(t, int32(0), calls.Load(), "iteration %d: handler ran though install reported closed", i)
		}
	}
}
