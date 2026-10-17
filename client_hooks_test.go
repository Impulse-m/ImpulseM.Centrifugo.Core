package centrifuge

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestBeginDisconnectMarksClosingBeforeClose(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "42")

	var calls atomic.Int32
	require.True(t, client.OnDisconnectIfConnected(func(DisconnectEvent) {
		calls.Add(1)
	}))

	// connectMu is taken by close before anything else, so the close
	// which BeginDisconnect starts stays parked until the test releases it.
	client.connectMu.Lock()
	begun := client.BeginDisconnect(DisconnectForceNoReconnect)
	closing := client.Closing()
	inHub := len(node.hub.UserConnections("42"))
	client.connectMu.Unlock()

	require.True(t, begun)
	require.True(t, closing)
	require.Equal(t, 1, inHub)
	require.Equal(t, int32(0), calls.Load())

	require.Eventually(t, func() bool {
		return len(node.hub.UserConnections("42")) == 0 && calls.Load() == 1
	}, 5*time.Second, time.Millisecond)
}

func TestBeginDisconnectOnClosingClientStartsNothing(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "42")

	var calls atomic.Int32
	var code atomic.Uint32
	require.True(t, client.OnDisconnectIfConnected(func(event DisconnectEvent) {
		calls.Add(1)
		code.Store(event.Disconnect.Code)
	}))

	client.connectMu.Lock()
	first := client.BeginDisconnect(DisconnectForceNoReconnect)
	second := client.BeginDisconnect(DisconnectConnectionLimit)
	client.connectMu.Unlock()

	require.True(t, first)
	require.False(t, second)

	require.Eventually(t, func() bool {
		return calls.Load() == 1
	}, 5*time.Second, time.Millisecond)

	third := client.BeginDisconnect(DisconnectShutdown)
	require.False(t, third)
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, DisconnectForceNoReconnect.Code, code.Load())
}

func TestClosingAfterDisconnect(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "42")
	require.False(t, client.Closing())

	client.Disconnect(DisconnectForceNoReconnect)

	require.Eventually(t, client.Closing, 5*time.Second, time.Millisecond)
	require.False(t, client.BeginDisconnect(DisconnectForceNoReconnect))
	require.Eventually(t, func() bool {
		return len(node.hub.UserConnections("42")) == 0
	}, 5*time.Second, time.Millisecond)
}

func TestBeginDisconnectConcurrent(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "42")

	var calls atomic.Int32
	require.True(t, client.OnDisconnectIfConnected(func(DisconnectEvent) {
		calls.Add(1)
	}))

	const goroutines = 64
	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if client.BeginDisconnect(DisconnectForceNoReconnect) {
				wins.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	require.Equal(t, int32(1), wins.Load())
	require.Eventually(t, func() bool {
		return calls.Load() == 1
	}, 5*time.Second, time.Millisecond)
}

func TestBeginDisconnectNotConnectedClient(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "42")

	var calls atomic.Int32
	client.OnDisconnect(func(DisconnectEvent) {
		calls.Add(1)
	})

	require.True(t, client.BeginDisconnect(DisconnectForceNoReconnect))
	require.True(t, client.Closing())
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.status == statusClosed
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, int32(0), calls.Load())
}
