package centrifuge

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const positionTestEpoch = "epoch"

func testPos(offset uint64) StreamPosition {
	return StreamPosition{Offset: offset, Epoch: positionTestEpoch}
}

// positionTestMedium drives a channel medium with a fake Node: the stream top is set by the test, every
// insufficient state marker is counted, and time moves only when tick is called.
type positionTestMedium struct {
	clock     *testClock
	top       StreamPosition
	topCalls  int
	markers   int
	onPublish func()
	medium    *channelMedium
}

func newPositionTestMedium(t *testing.T) *positionTestMedium {
	t.Helper()
	p := &positionTestMedium{clock: newTestClock(time.Now())}
	node := &mockNode{
		handlePublicationFunc: func(_ string, _ StreamPosition, pub, _, _ *Publication) error {
			if pub.Offset == math.MaxUint64 {
				p.markers++
				return nil
			}
			if p.onPublish != nil {
				p.onPublish()
			}
			return nil
		},
		streamTopFunc: func(_ string, _ time.Duration) (StreamPosition, error) {
			p.topCalls++
			return p.top, nil
		},
	}
	p.medium = setupChannelMedium(t, ChannelMediumOptions{SharedPositionSync: true}, node)
	p.medium.nowFn = p.clock.now
	// The medium stamped its last check with the real clock on creation, slightly after p.clock was taken.
	// Restamp it with the test clock, so one tick is exactly one check interval.
	p.medium.mu.Lock()
	p.medium.positionCheckTime = p.clock.now().UnixNano()
	p.medium.mu.Unlock()
	return p
}

// publish delivers publication at position sp from the broker to the medium.
func (p *positionTestMedium) publish(sp StreamPosition) {
	p.medium.broadcastPublication(&Publication{Offset: sp.Offset}, sp, false, nil)
}

// tick moves time forward by one position check interval (1 second in these tests).
func (p *positionTestMedium) tick() {
	p.clock.advance(time.Second)
}

// check runs CheckPosition for a client at position client, stamped with the current clock second, and
// returns its validPosition.
func (p *positionTestMedium) check(t *testing.T, client StreamPosition) bool {
	t.Helper()
	return p.checkAt(t, client, p.clock.now().Unix())
}

// checkAt runs CheckPosition for a client whose positionCheckTime is posTime (Unix seconds).
func (p *positionTestMedium) checkAt(t *testing.T, client StreamPosition, posTime int64) bool {
	t.Helper()
	checked, validPosition, err := p.medium.CheckPosition(0, client, time.Second, posTime)
	require.NoError(t, err)
	require.True(t, checked)
	return validPosition
}

func (p *positionTestMedium) hasRegress() bool {
	p.medium.mu.Lock()
	defer p.medium.mu.Unlock()
	return p.medium.position.hasRegress
}

func (p *positionTestMedium) lastPos() StreamPosition {
	p.medium.mu.Lock()
	defer p.medium.mu.Unlock()
	return p.medium.position.lastPosition
}

func (p *positionTestMedium) hasSuspect() bool {
	p.medium.mu.Lock()
	defer p.medium.mu.Unlock()
	return p.medium.position.hasSuspect
}

func TestSharedPositionSyncLaggingClientDetectedWithinOwnInterval(t *testing.T) {
	p := newPositionTestMedium(t)
	p.publish(testPos(6))
	p.top = testPos(6)
	p.tick()

	// 19 clients are at the stream top, the lagging one checks last in the same interval.
	for i := 0; i < 19; i++ {
		require.True(t, p.check(t, testPos(6)))
	}
	require.False(t, p.check(t, testPos(5)))
	require.Equal(t, 1, p.topCalls)
	require.Equal(t, 0, p.markers)
}

func TestSharedPositionSyncPublishRaceNoMarker(t *testing.T) {
	p := newPositionTestMedium(t)
	p.publish(testPos(6))
	p.top = testPos(6)
	p.tick()
	require.True(t, p.check(t, testPos(6)))

	// Publication 7 is in the stream, but not yet delivered to the medium: node becomes suspect.
	p.top = testPos(7)
	p.tick()
	require.True(t, p.check(t, testPos(6)))
	require.True(t, p.hasSuspect())

	// Publication arrives before the next interval: suspicion is cleared, no marker.
	p.publish(testPos(7))
	require.False(t, p.hasSuspect())
	p.tick()
	require.True(t, p.check(t, testPos(7)))
	require.False(t, p.hasSuspect())
	require.Equal(t, 0, p.markers)
}

func TestSharedPositionSyncMediumBehindConfirmedAfterTwoIntervals(t *testing.T) {
	p := newPositionTestMedium(t)
	p.publish(testPos(6))
	p.top = testPos(7)

	p.tick()
	require.True(t, p.check(t, testPos(6)))
	require.Equal(t, 0, p.markers)

	p.tick()
	require.False(t, p.check(t, testPos(6)))
	require.Equal(t, 1, p.markers)
}

func TestSharedPositionSyncNoMarkerLoopAfterRecovery(t *testing.T) {
	p := newPositionTestMedium(t)
	p.publish(testPos(6))
	p.top = testPos(7)
	p.tick()
	require.True(t, p.check(t, testPos(6)))
	p.tick()
	require.False(t, p.check(t, testPos(6)))
	require.Equal(t, 1, p.markers)

	// Subscribers resubscribe and recover to the stream top, no publication reaches the medium.
	for i := 0; i < 3; i++ {
		p.tick()
		for j := 0; j < 3; j++ {
			require.True(t, p.check(t, testPos(7)))
		}
	}
	require.Equal(t, 1, p.markers)
}

func TestChannelMediumLastPositionMonotoneAfterMarker(t *testing.T) {
	p := newPositionTestMedium(t)
	p.publish(testPos(6))
	p.top = testPos(9)
	p.tick()
	require.True(t, p.check(t, testPos(6)))
	p.tick()
	require.False(t, p.check(t, testPos(6)))
	require.Equal(t, 1, p.markers)

	lastOffset := func() uint64 {
		p.medium.mu.Lock()
		defer p.medium.mu.Unlock()
		return p.medium.position.lastPosition.Offset
	}
	// A publication still in flight when the marker fired must not move the Node position back.
	p.publish(testPos(8))
	require.Equal(t, uint64(9), lastOffset())
	// A newer publication still advances it.
	p.publish(testPos(10))
	require.Equal(t, uint64(10), lastOffset())
}

func TestChannelMediumLastPositionSetAfterBroadcast(t *testing.T) {
	p := newPositionTestMedium(t)
	p.publish(testPos(6))
	p.top = testPos(7)
	p.tick()
	require.True(t, p.check(t, testPos(7)))

	// While publication 7 is being handed to the Node, a client at 6 checks its position. Every local
	// subscriber has not received 7 yet, so the client is not behind because of the node.
	called := false
	valid := false
	p.onPublish = func() {
		if !called {
			called = true
			valid = p.check(t, testPos(6))
		}
	}
	p.publish(testPos(7))
	p.onPublish = nil
	require.True(t, called)
	require.True(t, valid)
}

func TestSharedPositionSyncClientAheadOfCachedTopIsValid(t *testing.T) {
	p := newPositionTestMedium(t)
	p.publish(testPos(6))
	p.top = testPos(7)
	p.tick()
	require.True(t, p.check(t, testPos(7)))
	calls := p.topCalls

	// Client recovered to a newer position than the cached top, within the same interval.
	require.True(t, p.check(t, testPos(8)))
	require.Equal(t, calls, p.topCalls)

	// Next interval: the top is fetched after the client was checked, and it is still 7 while the node is at 6.
	// The client is not delivered by the node, so it is invalid. The node is not confirmed behind (the top is
	// not ahead of lastPosition), so no marker is sent.
	stamp := p.clock.now().Unix()
	p.tick()
	require.False(t, p.checkAt(t, testPos(8), stamp))
	require.Equal(t, 0, p.markers)
}

// A same-epoch broker regression (the stream tail is lost, the epoch is kept) must be caught without a
// publication: a client ahead of the re-issued top is invalid once the regression is confirmed.
func TestSharedPositionSyncBrokerRegressionConfirmedAfterTwoIntervals(t *testing.T) {
	p := newPositionTestMedium(t)
	p.publish(testPos(105))
	p.top = testPos(105)
	p.tick()
	require.True(t, p.check(t, testPos(105)))

	// Broker re-issued the tail: the top is 103 while the node delivered 105. First sighting is suspect only.
	p.top = testPos(103)
	p.tick()
	require.True(t, p.check(t, testPos(105)))
	require.Equal(t, 0, p.markers)
	require.True(t, p.hasRegress())

	// Second consecutive fetch with the same regression confirms it: the marker is sent.
	p.tick()
	require.False(t, p.check(t, testPos(105)))
	require.Equal(t, 1, p.markers)
	require.Equal(t, testPos(103), p.lastPos())

	// The re-issued 104 is delivered after the marker: it does not trigger another marker.
	p.publish(testPos(104))
	require.Equal(t, 1, p.markers)
}

func TestSharedPositionSyncClientAheadDeliveredByNodeIsValid(t *testing.T) {
	p := newPositionTestMedium(t)
	p.publish(testPos(6))
	p.publish(testPos(8))
	p.top = testPos(7)
	stamp := p.clock.now().Unix()
	p.tick()

	// The node delivered 8 already, so a client at 8 is valid even though the top was fetched after it.
	require.True(t, p.checkAt(t, testPos(8), stamp))
	require.Equal(t, 0, p.markers)
}

func TestSharedPositionSyncNothingBroadcastYetUsesClientFloor(t *testing.T) {
	p := newPositionTestMedium(t)
	p.top = testPos(7)
	p.tick()

	// Nothing was broadcast yet: the client position is the floor. First sighting only makes the node suspect.
	require.True(t, p.check(t, testPos(5)))
	require.True(t, p.hasSuspect())
	require.Equal(t, 0, p.markers)

	p.tick()
	require.False(t, p.check(t, testPos(5)))
	require.Equal(t, 1, p.markers)
}

func TestChannelMediumClientOnStaleEpochMarkedWhenNodeInSync(t *testing.T) {
	p := newPositionTestMedium(t)
	p.publish(testPos(5))
	p.top = testPos(5)
	p.tick()
	require.True(t, p.check(t, testPos(5)))

	// Stream was reset to a new epoch, the node has received it.
	newEpoch := StreamPosition{Offset: 6, Epoch: "epoch2"}
	p.publish(newEpoch)
	p.top = newEpoch
	p.tick()

	// Client on the old epoch is stale individually, the node is not behind.
	require.False(t, p.check(t, testPos(5)))
	require.Equal(t, 0, p.markers)
}

func TestChannelMediumCheckPositionDecision(t *testing.T) {
	base := time.Now()
	clock := base

	var testCases = []struct {
		name           string
		broadcast      bool
		broadcastPos   StreamPosition
		streamTop      StreamPosition
		clientPosition StreamPosition
		// secondInterval makes the row run one more check on the next interval: the marker is expected
		// on the second check only.
		secondInterval bool
		expectValid    bool
		expectMarker   bool
	}{
		{
			name:           "nothing broadcast yet, client behind",
			broadcast:      false,
			streamTop:      StreamPosition{Offset: 5, Epoch: "e"},
			clientPosition: StreamPosition{Offset: 3, Epoch: "e"},
			secondInterval: true,
			expectValid:    false,
			expectMarker:   true,
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
			secondInterval: true,
			expectValid:    false,
			expectMarker:   true,
		},
		{
			name:           "epoch changed after last broadcast",
			broadcast:      true,
			broadcastPos:   StreamPosition{Offset: 5, Epoch: "e1"},
			streamTop:      StreamPosition{Offset: 5, Epoch: "e2"},
			clientPosition: StreamPosition{Offset: 5, Epoch: "e1"},
			secondInterval: true,
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
			medium.nowFn = func() time.Time { return clock }
			if tt.broadcast {
				medium.broadcastPublication(&Publication{Offset: tt.broadcastPos.Offset}, tt.broadcastPos, false, nil)
			}
			checks := 1
			if tt.secondInterval {
				checks = 2
			}
			for i := 1; i <= checks; i++ {
				clock = base.Add(time.Duration(i) * time.Hour)
				checked, validPosition, err := medium.CheckPosition(time.Second, tt.clientPosition, time.Second, 0)
				require.NoError(t, err)
				require.True(t, checked)
				if i == checks {
					require.Equal(t, tt.expectValid, validPosition)
					require.Equal(t, tt.expectMarker, markers > 0)
				} else {
					// First sighting of a behind node: the client is not marked, no marker yet.
					require.True(t, validPosition)
					require.Equal(t, 0, markers)
				}
			}
		})
	}
}
