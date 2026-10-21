package centrifuge

import (
	"errors"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifuge/internal/queue"
)

var (
	pongAtWritePingItem queue.Item = queue.Item{Data: []byte("{}"), FrameType: protocol.FrameTypeServerPing}
	pongAtWriteDataItem queue.Item = queue.Item{Data: []byte("data"), FrameType: protocol.FrameTypePushMessage}
)

// newPongAtWriteTestWriter returns a writer whose transport calls return writeErr. The hook is
// the one under test.
func newPongAtWriteTestWriter(writeErr error, hook func()) *writer {
	return newWriter(writerConfig{
		WriteFn: func(_ queue.Item) error {
			return writeErr
		},
		WriteManyFn: func(_ ...queue.Item) error {
			return writeErr
		},
		AfterPingWriteFn: hook,
	}, 0)
}

// drainOnce queues items and runs one drain. The drain never waits on a timer: the frame is
// filled, so the WriteDelay path writes at once and the timer pool is not used.
func drainOnce(t *testing.T, w *writer, maxMessagesInFrame int, writeDelay time.Duration, items ...queue.Item) bool {
	t.Helper()
	for _, item := range items {
		require.True(t, w.messages.Add(item))
	}
	return w.waitSendMessage(maxMessagesInFrame, writeDelay, 0)
}

func TestWriterAfterPingWriteFnCalledOnPing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		items              []queue.Item
		maxMessagesInFrame int
		writeDelay         time.Duration
	}{
		{
			name:               "lone ping",
			items:              []queue.Item{pongAtWritePingItem},
			maxMessagesInFrame: 16,
		},
		{
			name:               "ping with data",
			items:              []queue.Item{pongAtWriteDataItem, pongAtWritePingItem},
			maxMessagesInFrame: 16,
		},
		{
			name:               "lone ping, write delay",
			items:              []queue.Item{pongAtWritePingItem},
			maxMessagesInFrame: 1,
			writeDelay:         time.Millisecond,
		},
		{
			name:               "ping with data, write delay",
			items:              []queue.Item{pongAtWriteDataItem, pongAtWritePingItem},
			maxMessagesInFrame: 2,
			writeDelay:         time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var calls int
			w := newPongAtWriteTestWriter(nil, func() {
				calls++
			})
			require.True(t, drainOnce(t, w, tt.maxMessagesInFrame, tt.writeDelay, tt.items...))
			require.Equal(t, 1, calls)
		})
	}
}

func TestWriterAfterPingWriteFnNotCalledWithoutPing(t *testing.T) {
	t.Parallel()
	var calls int
	w := newPongAtWriteTestWriter(nil, func() {
		calls++
	})
	require.True(t, drainOnce(t, w, 16, 0, pongAtWriteDataItem, pongAtWriteDataItem))
	require.Zero(t, calls)
}

func TestWriterAfterPingWriteFnNotCalledOnError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		items []queue.Item
	}{
		{
			name:  "lone ping",
			items: []queue.Item{pongAtWritePingItem},
		},
		{
			name:  "ping with data",
			items: []queue.Item{pongAtWriteDataItem, pongAtWritePingItem},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var calls int
			w := newPongAtWriteTestWriter(errors.New("write failed"), func() {
				calls++
			})
			require.False(t, drainOnce(t, w, 16, 0, tt.items...))
			require.Zero(t, calls)
		})
	}
}

func TestWriterAfterPingWriteFnRunsUnlocked(t *testing.T) {
	t.Parallel()
	var locked bool
	var w *writer
	w = newPongAtWriteTestWriter(nil, func() {
		locked = w.mu.TryLock()
		if locked {
			w.mu.Unlock()
		}
	})
	require.True(t, drainOnce(t, w, 16, 0, pongAtWritePingItem))
	require.True(t, locked, "writer lock is held inside the hook")
}

func TestWriterAfterPingWriteFnNilIsNoop(t *testing.T) {
	t.Parallel()
	var writes int
	w := newWriter(writerConfig{
		WriteFn: func(_ queue.Item) error {
			writes++
			return nil
		},
		WriteManyFn: func(_ ...queue.Item) error {
			writes++
			return nil
		},
	}, 0)
	require.True(t, drainOnce(t, w, 16, 0, pongAtWriteDataItem, pongAtWritePingItem))
	require.Equal(t, 1, writes)
	require.False(t, pingWrittenIn(nil, []queue.Item{pongAtWritePingItem}))
}
