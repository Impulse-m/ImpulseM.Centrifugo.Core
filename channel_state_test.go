//go:build integration

package centrifuge

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestChannelStateNode creates a node with a Redis broker, so that the nodes of one
// prefix see each other in the node registry, and a channel state presence manager.
func newTestChannelStateNode(tb testing.TB, prefix string, state RedisChannelStateConfig) (*Node, *RedisPresenceManager) {
	tb.Helper()
	node := testNode(tb)
	s, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(tb, err)
	broker, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix:   prefix,
		UseLists: true,
		Shards:   []*RedisShard{s},
	})
	require.NoError(tb, err)
	node.SetBroker(broker)
	pm := newTestRedisPresenceStateManagerWithShards(tb, node, prefix, state, 0, false, []*RedisShard{s})
	return node, pm
}

// testStatePrefixes numbers the prefixes of the channel state tests.
var testStatePrefixes atomic.Uint64

// getUniqueStatePrefix returns a prefix no other channel state test uses. getUniquePrefix seeds
// its letters from the clock, so calls at the same moment (parallel tests) can return one prefix,
// and two nodes on one prefix share their partition leases.
func getUniqueStatePrefix() string {
	return getUniquePrefix() + "-" + strconv.FormatUint(testStatePrefixes.Add(1), 10)
}

func testOwnedPartitions(tb testing.TB, node *Node) int {
	tb.Helper()
	return int(testGaugeValue(tb, node.metrics.channelStatePartitionsOwned))
}

func testStreamLength(tb testing.TB, pm *RedisPresenceManager, p int) int64 {
	tb.Helper()
	client := pm.shards[0].client
	length, err := client.Do(context.Background(), client.B().Xlen().Key(string(pm.state.partitionKey(p, "stream"))).Build()).AsInt64()
	require.NoError(tb, err)
	return length
}

func TestRedisChannelStateRetryDelivery(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	collector.setFailures(2)
	node := testNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testChannelStateConfig(collector), 0, false, 6379)

	require.NoError(t, pm.AddPresence("retry", "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return len(collector.snapshot()) == 1
	}, 10*time.Second, 10*time.Millisecond)
	time.Sleep(3 * testChannelStateVacatedDelay)

	attempts := collector.attemptTimes()
	require.Len(t, attempts, 3)
	require.GreaterOrEqual(t, attempts[1].Sub(attempts[0]), 100*time.Millisecond)
	require.GreaterOrEqual(t, attempts[2].Sub(attempts[1]), 200*time.Millisecond)
	require.Equal(t, 2.0, testCounterValue(t, node.metrics.channelStateDeliveryErrors))
	require.Len(t, collector.snapshot(), 1)
}

func TestRedisChannelStateStreamCap(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	collector.setFailAll(true)
	state := testChannelStateConfig(collector)
	// One partition, so the five channels share the stream cap of three events.
	state.NumPartitions = 1
	state.MaxStreamLength = 3
	node := testNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), state, 0, false, 6379)

	channels := []string{"cap-1", "cap-2", "cap-3", "cap-4", "cap-5"}
	for _, ch := range channels {
		require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	}
	require.Equal(t, 2.0, testCounterVecValue(t, node.metrics.channelStateEventsDropped, "occupied"))
	require.Equal(t, int64(3), testStreamLength(t, pm, 0))

	collector.setFailAll(false)
	require.Eventually(t, func() bool {
		return len(collector.snapshot()) == 3
	}, 20*time.Second, 10*time.Millisecond)
	events := collector.snapshot()
	require.Equal(t, "cap-1", events[0].Channel)
	require.Equal(t, "cap-2", events[1].Channel)
	require.Equal(t, "cap-3", events[2].Channel)
	require.Eventually(t, func() bool {
		return testStreamLength(t, pm, 0) == 0
	}, 5*time.Second, 10*time.Millisecond)
}

func TestRedisChannelStateTwoNodes(t *testing.T) {
	t.Parallel()
	prefix := getUniqueStatePrefix()
	collector := &testChannelStateCollector{}
	state := testChannelStateConfig(collector)
	state.NumPartitions = 8

	node1, pm1 := newTestChannelStateNode(t, prefix, state)
	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, node1) == 8
	}, 3*time.Second, 10*time.Millisecond)

	node2, _ := newTestChannelStateNode(t, prefix, state)
	// Each node owns 4 partitions within 3 lease intervals.
	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, node1) == 4 && testOwnedPartitions(t, node2) == 4
	}, 3*testChannelStateLeaseTTL, 10*time.Millisecond)

	const numChannels = 50
	for i := 0; i < numChannels; i++ {
		require.NoError(t, pm1.AddPresence("two-nodes-"+strconv.Itoa(i), "client-1", &ClientInfo{}))
	}
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == numChannels
	}, 10*time.Second, 10*time.Millisecond)
	time.Sleep(3 * testChannelStateVacatedDelay)

	// Each channel is delivered exactly once in total, by either node.
	seen := make(map[string]int, numChannels)
	for _, e := range eventsOfType(collector.snapshot(), ChannelStateOccupied) {
		seen[e.Channel]++
	}
	require.Len(t, seen, numChannels)
	for ch, count := range seen {
		require.Equal(t, 1, count, ch)
	}

	// Closing node 2 moves its partitions to node 1.
	require.NoError(t, node2.Shutdown(context.Background()))
	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, node1) == 8
	}, 2*testChannelStateLeaseTTL+time.Second, 10*time.Millisecond)

	require.NoError(t, pm1.AddPresence("after-close", "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		for _, e := range eventsOfType(collector.snapshot(), ChannelStateOccupied) {
			if e.Channel == "after-close" {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond)
}

func TestRedisChannelStateClose(t *testing.T) {
	collector := &testChannelStateCollector{}
	node := testNode(t)
	s, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(t, err)
	t.Cleanup(s.Close)
	require.NoError(t, node.Run())

	state := testChannelStateConfig(collector)
	pm, err := NewRedisPresenceManager(node, RedisPresenceManagerConfig{
		Prefix:       getUniqueStatePrefix(),
		Shards:       []*RedisShard{s},
		ChannelState: &state,
	})
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	require.NoError(t, pm.AddPresence("close", "client-1", &ClientInfo{}))

	leaseKeys := func() int {
		client := pm.shards[0].client
		keys, err := client.Do(context.Background(), client.B().Keys().Pattern(pm.config.Prefix+".channel_state.*").Build()).AsStrSlice()
		require.NoError(t, err)
		count := 0
		for _, key := range keys {
			if strings.HasSuffix(key, ".lease") {
				count++
			}
		}
		return count
	}
	// The lease loop and one consumer per partition: the worker goroutines.
	require.Eventually(t, func() bool {
		return leaseKeys() == channelStateDefaultNumPartitions && countChannelStateGoroutines() == 1+channelStateDefaultNumPartitions
	}, 3*time.Second, 10*time.Millisecond)

	require.NoError(t, pm.Close(context.Background()))
	require.Equal(t, 0, leaseKeys())
	// The pool of the Redis client keeps its own goroutines, so only the worker goroutines are counted.
	require.Eventually(t, func() bool {
		return countChannelStateGoroutines() == 0
	}, time.Second, 10*time.Millisecond)
}

// countChannelStateGoroutines counts the goroutines that run the channel state worker.
func countChannelStateGoroutines() int {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, goroutine := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(goroutine, "(*channelStateWorker).") {
			count++
		}
	}
	return count
}
