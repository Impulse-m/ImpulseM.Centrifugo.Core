package centrifuge

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Helper function to create a channelMedium with options.
func setupChannelMedium(t testing.TB, options ChannelMediumOptions, node nodeSubset) *channelMedium {
	t.Helper()
	channel := "testChannel"
	cache, err := newChannelMedium(channel, node, options)
	if err != nil {
		require.NoError(t, err)
	}
	return cache
}

type mockNode struct {
	handlePublicationFunc func(channel string, sp StreamPosition, pub, prevPub, localPrevPub *Publication) error
	streamTopFunc         func(ch string, historyMetaTTL time.Duration) (StreamPosition, error)
}

func (m *mockNode) handlePublication(channel string, sp StreamPosition, pub, prevPub, localPrevPub *Publication) error {
	if m.handlePublicationFunc != nil {
		return m.handlePublicationFunc(channel, sp, pub, prevPub, localPrevPub)
	}
	return nil
}

func (m *mockNode) streamTop(ch string, historyMetaTTL time.Duration) (StreamPosition, error) {
	if m.streamTopFunc != nil {
		return m.streamTopFunc(ch, historyMetaTTL)
	}
	return StreamPosition{}, nil
}

func TestChannelMediumHandlePublication(t *testing.T) {
	var testCases = []struct {
		numPublications int
		options         ChannelMediumOptions
	}{
		{
			numPublications: 10,
			options: ChannelMediumOptions{
				enableQueue:           false,
				KeepLatestPublication: false,
			},
		},
		{
			numPublications: 10,
			options: ChannelMediumOptions{
				enableQueue:           true,
				KeepLatestPublication: false,
			},
		},
		{
			numPublications: 1,
			options: ChannelMediumOptions{
				enableQueue:           true,
				KeepLatestPublication: false,
				broadcastDelay:        10 * time.Millisecond,
			},
		},
		{
			numPublications: 1,
			options: ChannelMediumOptions{
				enableQueue:           true,
				KeepLatestPublication: true,
				broadcastDelay:        10 * time.Millisecond,
			},
		},
	}

	for i, tt := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			numPublications := tt.numPublications

			doneCh := make(chan struct{}, numPublications)

			cache := setupChannelMedium(t, tt.options, &mockNode{
				handlePublicationFunc: func(channel string, sp StreamPosition, pub, prevPub, localPrevPub *Publication) error {
					doneCh <- struct{}{}
					return nil
				},
			})

			pub := &Publication{Data: []byte("test data")}
			sp := StreamPosition{Offset: 1}

			for i := 0; i < numPublications; i++ {
				cache.broadcastPublication(pub, sp, false, nil)
			}

			for i := 0; i < numPublications; i++ {
				select {
				case <-doneCh:
				case <-time.After(5 * time.Second):
					require.Fail(t, "handlePublicationFunc was not called")
				}
			}
		})
	}
}

func TestChannelMediumInsufficientState(t *testing.T) {
	options := ChannelMediumOptions{
		enableQueue:           true,
		KeepLatestPublication: true,
	}
	doneCh := make(chan struct{})
	medium := setupChannelMedium(t, options, &mockNode{
		handlePublicationFunc: func(channel string, sp StreamPosition, pub, prevPub, localPrevPub *Publication) error {
			require.Equal(t, uint64(math.MaxUint64), pub.Offset)
			require.Equal(t, uint64(math.MaxUint64), sp.Offset)
			close(doneCh)
			return nil
		},
	})

	// Simulate the behavior when the state is marked as insufficient
	medium.broadcastInsufficientState()

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "handlePublicationFunc was not called")
	}
}

func TestChannelMediumPositionSync(t *testing.T) {
	options := ChannelMediumOptions{
		SharedPositionSync: true,
	}
	doneCh := make(chan struct{})
	var closeOnce sync.Once
	medium := setupChannelMedium(t, options, &mockNode{
		streamTopFunc: func(ch string, historyMetaTTL time.Duration) (StreamPosition, error) {
			closeOnce.Do(func() {
				close(doneCh)
			})
			return StreamPosition{}, nil
		},
	})
	originalGetter := channelMediumTimeNow
	channelMediumTimeNow = func() time.Time {
		return time.Now().Add(time.Hour)
	}
	medium.CheckPosition(time.Second, StreamPosition{Offset: 1, Epoch: "test"}, time.Second)
	channelMediumTimeNow = originalGetter
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "historyFunc was not called")
	}
}

func TestChannelMediumPositionSyncRetry(t *testing.T) {
	options := ChannelMediumOptions{
		SharedPositionSync: true,
	}
	doneCh := make(chan struct{})
	var closeOnce sync.Once
	numCalls := 0
	medium := setupChannelMedium(t, options, &mockNode{
		streamTopFunc: func(ch string, historyMetaTTL time.Duration) (StreamPosition, error) {
			if numCalls == 0 {
				numCalls++
				return StreamPosition{}, errors.New("boom")
			}
			closeOnce.Do(func() {
				close(doneCh)
			})
			return StreamPosition{}, nil
		},
	})
	originalGetter := channelMediumTimeNow
	channelMediumTimeNow = func() time.Time {
		return time.Now().Add(time.Hour)
	}
	medium.CheckPosition(time.Second, StreamPosition{Offset: 1, Epoch: "test"}, time.Second)
	channelMediumTimeNow = originalGetter
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "streamTopLatestPubFunc was not called")
	}
}

func TestChannelMediumCheckPositionDecision(t *testing.T) {
	base := time.Now()
	clock := base
	originalGetter := channelMediumTimeNow
	channelMediumTimeNow = func() time.Time {
		return clock
	}
	t.Cleanup(func() {
		channelMediumTimeNow = originalGetter
	})

	var testCases = []struct {
		name           string
		broadcast      bool
		broadcastPos   StreamPosition
		streamTop      StreamPosition
		clientPosition StreamPosition
		expectValid    bool
		expectMarker   bool
	}{
		{
			name:           "nothing broadcast yet, client behind",
			broadcast:      false,
			streamTop:      StreamPosition{Offset: 5, Epoch: "e"},
			clientPosition: StreamPosition{Offset: 3, Epoch: "e"},
			expectValid:    false,
			expectMarker:   false,
		},
		{
			name:           "node in sync, only client behind",
			broadcast:      true,
			broadcastPos:   StreamPosition{Offset: 5, Epoch: "e"},
			streamTop:      StreamPosition{Offset: 5, Epoch: "e"},
			clientPosition: StreamPosition{Offset: 3, Epoch: "e"},
			expectValid:    false,
			expectMarker:   false,
		},
		{
			name:           "medium behind stream",
			broadcast:      true,
			broadcastPos:   StreamPosition{Offset: 4, Epoch: "e"},
			streamTop:      StreamPosition{Offset: 5, Epoch: "e"},
			clientPosition: StreamPosition{Offset: 4, Epoch: "e"},
			expectValid:    false,
			expectMarker:   true,
		},
		{
			name:           "epoch changed after last broadcast",
			broadcast:      true,
			broadcastPos:   StreamPosition{Offset: 5, Epoch: "e1"},
			streamTop:      StreamPosition{Offset: 5, Epoch: "e2"},
			clientPosition: StreamPosition{Offset: 5, Epoch: "e1"},
			expectValid:    false,
			expectMarker:   true,
		},
		{
			name:           "client up to date, medium behind stream",
			broadcast:      true,
			broadcastPos:   StreamPosition{Offset: 4, Epoch: "e"},
			streamTop:      StreamPosition{Offset: 5, Epoch: "e"},
			clientPosition: StreamPosition{Offset: 5, Epoch: "e"},
			expectValid:    true,
			expectMarker:   false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			markers := 0
			node := &mockNode{
				handlePublicationFunc: func(_ string, _ StreamPosition, pub, _, _ *Publication) error {
					if pub.Offset == math.MaxUint64 {
						markers++
					}
					return nil
				},
				streamTopFunc: func(_ string, _ time.Duration) (StreamPosition, error) {
					return tt.streamTop, nil
				},
			}
			clock = base
			medium := setupChannelMedium(t, ChannelMediumOptions{SharedPositionSync: true}, node)
			if tt.broadcast {
				medium.broadcastPublication(&Publication{Offset: tt.broadcastPos.Offset}, tt.broadcastPos, false, nil)
			}
			clock = base.Add(time.Hour)
			checked, validPosition, err := medium.CheckPosition(time.Second, tt.clientPosition, time.Second)
			require.NoError(t, err)
			require.True(t, checked)
			require.Equal(t, tt.expectValid, validPosition)
			require.Equal(t, tt.expectMarker, markers > 0)
		})
	}
}
