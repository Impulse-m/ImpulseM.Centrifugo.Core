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
	clock     time.Time
	top       StreamPosition
	topCalls  int
	markers   int
	onPublish func()
	medium    *channelMedium
}

func newPositionTestMedium(t *testing.T) *positionTestMedium {
	t.Helper()
	p := &positionTestMedium{clock: time.Now()}
	originalGetter := channelMediumTimeNow
	channelMediumTimeNow = func() time.Time {
		return p.clock
	}
	t.Cleanup(func() {
		channelMediumTimeNow = originalGetter
	})
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
	return p
}

// publish delivers publication at position sp from the broker to the medium.
func (p *positionTestMedium) publish(sp StreamPosition) {
	p.medium.broadcastPublication(&Publication{Offset: sp.Offset}, sp, false, nil)
}

// tick moves time forward by one position check interval (1 second in these tests).
func (p *positionTestMedium) tick() {
	p.clock = p.clock.Add(time.Second)
}

// check runs CheckPosition for a client at position client and returns its validPosition.
func (p *positionTestMedium) check(t *testing.T, client StreamPosition) bool {
	t.Helper()
	checked, validPosition, err := p.medium.CheckPosition(0, client, time.Second)
	require.NoError(t, err)
	require.True(t, checked)
	return validPosition
}

func (p *positionTestMedium) hasSuspect() bool {
	p.medium.mu.Lock()
	defer p.medium.mu.Unlock()
	return p.medium.hasSuspect
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
		return p.medium.lastPosition.Offset
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
