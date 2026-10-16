//go:build integration

package centrifuge

import (
	"context"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// testStateNode creates a node with a registry of its own for its metrics, so that the channel state
// metrics of the tests, which run in parallel, do not share the default registry.
func testStateNode(tb testing.TB) *Node {
	tb.Helper()
	node, err := New(Config{
		LogLevel:   LogLevelDebug,
		LogHandler: func(entry LogEntry) {},
		Metrics: MetricsConfig{
			RegistererGatherer: prometheus.NewRegistry(),
		},
	})
	require.NoError(tb, err)
	return node
}

// newTestChannelStateNode creates a node with one Redis shard and a channel state presence manager.
func newTestChannelStateNode(tb testing.TB, prefix string, state RedisChannelStateConfig) (*Node, *RedisPresenceManager) {
	tb.Helper()
	node, pm, _ := newTestChannelStateNodeWithShards(tb, prefix, state, 1)
	return node, pm
}

// newTestChannelStateNodeWithShards creates a node with numShards Redis shards on the test server and a
// channel state presence manager on them. The shards are returned, so that a test can close them to simulate
// an outage.
func newTestChannelStateNodeWithShards(tb testing.TB, prefix string, state RedisChannelStateConfig, numShards int) (*Node, *RedisPresenceManager, []*RedisShard) {
	tb.Helper()
	confs := make([]RedisShardConfig, 0, numShards)
	for range numShards {
		confs = append(confs, testSingleRedisConf(6379))
	}
	return newTestChannelStateNodeWithConfs(tb, prefix, state, confs)
}

// newTestChannelStateNodeWithConfs creates a node with one Redis shard per config and a channel state presence
// manager on them. The node has a Redis broker because Node.Run registers its broker event handler.
func newTestChannelStateNodeWithConfs(tb testing.TB, prefix string, state RedisChannelStateConfig, confs []RedisShardConfig) (*Node, *RedisPresenceManager, []*RedisShard) {
	tb.Helper()
	node := testStateNode(tb)
	shards := make([]*RedisShard, 0, len(confs))
	for _, conf := range confs {
		s, err := NewRedisShard(node, conf)
		require.NoError(tb, err)
		shards = append(shards, s)
	}
	broker, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix:   prefix,
		UseLists: true,
		Shards:   shards,
	})
	require.NoError(tb, err)
	node.SetBroker(broker)
	pm := newTestRedisPresenceStateManagerWithShards(tb, node, prefix, state, 0, false, shards)
	return node, pm, shards
}

// testStatePrefixes numbers the prefixes of the channel state tests.
var testStatePrefixes atomic.Uint64

// getUniqueStatePrefix returns a prefix no other channel state test uses. getUniquePrefix seeds
// its letters from the clock, so calls at the same moment (parallel tests) can return one prefix,
// and two nodes on one prefix share their partition leases.
func getUniqueStatePrefix() string {
	return getUniquePrefix() + "-" + strconv.FormatUint(testStatePrefixes.Add(1), 10)
}

func testOwnedPartitions(tb testing.TB, pm *RedisPresenceManager) int {
	tb.Helper()
	return int(metricValue(tb, pm.state.metrics.partitionsOwned))
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
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testChannelStateConfig(collector), 0, false, 6379)

	require.NoError(t, pm.AddPresence("retry", "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return len(collector.snapshot()) == 1
	}, 10*time.Second, 10*time.Millisecond)
	requireSettled(t, func() int { return len(collector.attemptTimes()) }, testChannelStateQuietWindow)

	attempts := collector.attemptTimes()
	require.Len(t, attempts, 3)
	require.GreaterOrEqual(t, attempts[1].Sub(attempts[0]), 100*time.Millisecond)
	require.GreaterOrEqual(t, attempts[2].Sub(attempts[1]), 200*time.Millisecond)
	require.Equal(t, 2.0, counterValue(t, pm.state.metrics.deliveryErrors))
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
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), state, 0, false, 6379)

	channels := []string{"cap-1", "cap-2", "cap-3", "cap-4", "cap-5"}
	for _, ch := range channels {
		require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	}
	require.Equal(t, 2.0, metricValue(t, pm.state.metrics.eventsDropped.WithLabelValues("occupied")))
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

	_, pm1 := newTestChannelStateNode(t, prefix, state)
	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, pm1) == 8
	}, 3*time.Second, 10*time.Millisecond)

	node2, pm2 := newTestChannelStateNode(t, prefix, state)
	// Each node owns 4 partitions within 3 lease intervals.
	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, pm1) == 4 && testOwnedPartitions(t, pm2) == 4
	}, 3*testChannelStateLeaseTTL, 10*time.Millisecond)

	const numChannels = 50
	for i := 0; i < numChannels; i++ {
		require.NoError(t, pm1.AddPresence("two-nodes-"+strconv.Itoa(i), "client-1", &ClientInfo{}))
	}
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == numChannels
	}, 10*time.Second, 10*time.Millisecond)
	requireSettled(t, func() int { return len(collector.snapshot()) }, testChannelStateQuietWindow)

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
		return testOwnedPartitions(t, pm1) == 8
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

// TestRedisChannelStateCrashedWorkerPartitionsMove closes the shard of a worker without a clean shutdown:
// it neither renews its leases nor refreshes its heartbeat entry, so they expire. The survivor takes
// every partition, and every partition delivers the events of its channels.
func TestRedisChannelStateCrashedWorkerPartitionsMove(t *testing.T) {
	t.Parallel()
	prefix := getUniqueStatePrefix()
	collector := &testChannelStateCollector{}
	state := testChannelStateConfig(collector)
	state.NumPartitions = 8

	_, pm1, _ := newTestChannelStateNodeWithShards(t, prefix, state, 1)
	_, pm2, shards2 := newTestChannelStateNodeWithShards(t, prefix, state, 1)
	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, pm1) == 4 && testOwnedPartitions(t, pm2) == 4
	}, 3*testChannelStateLeaseTTL, 10*time.Millisecond)

	shards2[0].Close()
	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, pm1) == 8
	}, 2*testChannelStateLeaseTTL+time.Second, 10*time.Millisecond)

	const numChannels = 16
	for i := 0; i < numChannels; i++ {
		require.NoError(t, pm1.AddPresence("crashed-"+strconv.Itoa(i), "client-1", &ClientInfo{}))
	}
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == numChannels
	}, 10*time.Second, 10*time.Millisecond)
}

// TestRedisChannelStateShardZeroOutage runs two nodes with two shards each, on one Redis server. Node 1
// loses shard 0, which keeps the worker set, and node 2 crashes. Node 1 cannot count the workers, so it
// keeps its partitions and takes the free ones: the partitions of shard 1 that node 2 held must not wait
// for shard 0 to recover.
func TestRedisChannelStateShardZeroOutage(t *testing.T) {
	t.Parallel()
	prefix := getUniqueStatePrefix()
	collector := &testChannelStateCollector{}
	state := testChannelStateConfig(collector)
	state.NumPartitions = 8

	_, pm1, shards1 := newTestChannelStateNodeWithShards(t, prefix, state, 2)
	_, pm2, shards2 := newTestChannelStateNodeWithShards(t, prefix, state, 2)
	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, pm1) == 4 && testOwnedPartitions(t, pm2) == 4
	}, 3*testChannelStateLeaseTTL, 10*time.Millisecond)

	var shard1Partitions int
	for p := range state.NumPartitions {
		if pm1.state.partitionShards[p] == 1 {
			shard1Partitions++
		}
	}
	require.Positive(t, shard1Partitions)
	require.Less(t, shard1Partitions, state.NumPartitions)

	shards1[0].Close()
	shards2[0].Close()
	shards2[1].Close()
	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, pm1) == shard1Partitions
	}, 2*testChannelStateLeaseTTL+time.Second, 10*time.Millisecond)

	// One channel of every partition of shard 1: each of the partitions must deliver its event.
	covered := make(map[int]bool, shard1Partitions)
	var channels []string
	for i := 0; len(covered) < shard1Partitions; i++ {
		ch := "shard-zero-" + strconv.Itoa(i)
		p := pm1.state.partition(ch)
		if covered[p] || pm1.state.partitionShards[p] != 1 {
			continue
		}
		covered[p] = true
		channels = append(channels, ch)
		require.NoError(t, pm1.AddPresence(ch, "client-1", &ClientInfo{}))
	}
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == len(channels)
	}, 10*time.Second, 10*time.Millisecond)
}

// TestRedisChannelStateHungShard pauses the first shard behind a gate that accepts connections and holds their
// traffic, so that its calls wait until they time out. The partitions of the second shard keep their leases and
// keep delivering while the first shard hangs. The partitions of the first shard are given up.
func TestRedisChannelStateHungShard(t *testing.T) {
	t.Parallel()
	prefix := getUniqueStatePrefix()
	collector := &testChannelStateCollector{}
	state := testChannelStateConfig(collector)
	state.NumPartitions = 8

	gate := startTestTCPGate(t, "127.0.0.1:6379")
	gatedConf := testSingleRedisConf(6379)
	gatedConf.Address = gate.addr()
	_, pm, _ := newTestChannelStateNodeWithConfs(t, prefix, state, []RedisShardConfig{gatedConf, testSingleRedisConf(6379)})
	// Registered after the node, so it runs first: the node shuts down on a shard that answers.
	t.Cleanup(gate.resume)
	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, pm) == state.NumPartitions
	}, 3*testChannelStateLeaseTTL, 10*time.Millisecond)

	var shard1Partitions []int
	for p := range state.NumPartitions {
		if pm.state.partitionShards[p] == 1 {
			shard1Partitions = append(shard1Partitions, p)
		}
	}
	require.NotEmpty(t, shard1Partitions)
	require.Less(t, len(shard1Partitions), state.NumPartitions)

	gate.pause()
	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, pm) == len(shard1Partitions)
	}, 2*testChannelStateLeaseTTL+time.Second, 10*time.Millisecond)
	// The healthy shard keeps the leases of its partitions while the first shard hangs.
	require.Never(t, func() bool {
		return testOwnedPartitions(t, pm) != len(shard1Partitions)
	}, 3*testChannelStateLeaseTTL, 10*time.Millisecond)

	// One channel of every partition of the second shard: each of the partitions must deliver its event.
	covered := make(map[int]bool, len(shard1Partitions))
	var channels []string
	for i := 0; len(covered) < len(shard1Partitions); i++ {
		ch := "hung-shard-" + strconv.Itoa(i)
		p := pm.state.partition(ch)
		if covered[p] || pm.state.partitionShards[p] != 1 {
			continue
		}
		covered[p] = true
		channels = append(channels, ch)
		require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	}
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == len(channels)
	}, 10*time.Second, 10*time.Millisecond)
}

// testTCPGatePollInterval is how often a paused gate checks whether it is resumed.
const testTCPGatePollInterval = 5 * time.Millisecond

// testTCPGate forwards TCP connections to upstream until it is paused. A paused gate keeps the connections open
// and holds their traffic, so that a client waits for replies that do not come: a shard that hangs. Unlike
// startTestTCPProxy, which breaks the connections when it stops, the gate leaves the client waiting.
type testTCPGate struct {
	ln     net.Listener
	paused atomic.Bool
	closed atomic.Bool
}

// startTestTCPGate listens on a free local port and forwards the connections to upstream.
func startTestTCPGate(tb testing.TB, upstream string) *testTCPGate {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	g := &testTCPGate{ln: ln}
	tb.Cleanup(g.close)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", upstream)
			if err != nil {
				_ = c.Close()
				continue
			}
			go g.copyChunks(up, c)
			go g.copyChunks(c, up)
		}
	}()
	return g
}

// copyChunks copies the traffic from src to dst. A chunk read while the gate is paused waits until the gate is
// resumed or closed.
func (g *testTCPGate) copyChunks(dst net.Conn, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			for g.paused.Load() && !g.closed.Load() {
				time.Sleep(testTCPGatePollInterval)
			}
			if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = dst.Close()
	_ = src.Close()
}

func (g *testTCPGate) addr() string { return g.ln.Addr().String() }

func (g *testTCPGate) pause() { g.paused.Store(true) }

func (g *testTCPGate) resume() { g.paused.Store(false) }

func (g *testTCPGate) close() {
	g.closed.Store(true)
	_ = g.ln.Close()
}

// TestRedisChannelStateMixedNodes runs a node with channel state and a node without it on one prefix.
// Only the node with channel state is a worker, so it takes every partition, and every partition has
// a consumer for the channels added to it.
func TestRedisChannelStateMixedNodes(t *testing.T) {
	t.Parallel()
	prefix := getUniqueStatePrefix()
	collector := &testChannelStateCollector{}
	state := testChannelStateConfig(collector)
	state.NumPartitions = 8

	_, pm1 := newTestChannelStateNode(t, prefix, state)

	plain := testStateNode(t)
	s, err := NewRedisShard(plain, testSingleRedisConf(6379))
	require.NoError(t, err)
	t.Cleanup(s.Close)
	broker, err := NewRedisBroker(plain, RedisBrokerConfig{
		Prefix:   prefix,
		UseLists: true,
		Shards:   []*RedisShard{s},
	})
	require.NoError(t, err)
	plain.SetBroker(broker)
	plainPM, err := NewRedisPresenceManager(plain, RedisPresenceManagerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s},
	})
	require.NoError(t, err)
	plain.SetPresenceManager(plainPM)
	require.NoError(t, plain.Run())
	t.Cleanup(func() {
		_ = plain.Shutdown(context.Background())
	})

	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, pm1) == 8
	}, 3*testChannelStateLeaseTTL, 10*time.Millisecond)

	// One channel of every partition: each of the partitions must deliver its event.
	covered := make(map[int]bool, 8)
	var channels []string
	for i := 0; len(covered) < 8; i++ {
		ch := "mixed-" + strconv.Itoa(i)
		p := pm1.state.partition(ch)
		if covered[p] {
			continue
		}
		covered[p] = true
		channels = append(channels, ch)
		require.NoError(t, pm1.AddPresence(ch, "client-1", &ClientInfo{}))
	}
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == len(channels)
	}, 10*time.Second, 10*time.Millisecond)
}

func TestRedisChannelStateClose(t *testing.T) {
	collector := &testChannelStateCollector{}
	node := testStateNode(t)
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
	// Consumers stopped by Close are not Redis errors.
	require.Equal(t, 0.0, counterValue(t, pm.state.metrics.sweepErrors))
	// The pool of the Redis client keeps its own goroutines, so only the worker goroutines are counted.
	require.Eventually(t, func() bool {
		return countChannelStateGoroutines() == 0
	}, time.Second, 10*time.Millisecond)
}

// testBlockingHandler is a channel state handler whose calls block until unblock is called. It ignores its
// context, like a handler that does not honour it. It counts the calls and the most calls that ran at once.
type testBlockingHandler struct {
	calls    atomic.Int32
	inFlight atomic.Int32
	peak     atomic.Int32
	release  chan struct{}
	once     sync.Once
}

func newTestBlockingHandler() *testBlockingHandler {
	return &testBlockingHandler{release: make(chan struct{})}
}

func (h *testBlockingHandler) handle(_ context.Context, _ []ChannelStateEvent) error {
	h.calls.Add(1)
	running := h.inFlight.Add(1)
	defer h.inFlight.Add(-1)
	for {
		peak := h.peak.Load()
		if running <= peak || h.peak.CompareAndSwap(peak, running) {
			break
		}
	}
	<-h.release
	return nil
}

// unblock lets the blocked calls return. It may be called more than once.
func (h *testBlockingHandler) unblock() {
	h.once.Do(func() {
		close(h.release)
	})
}

// testBlockingChannelStateConfig returns the config of a single partition whose events go to handler.
func testBlockingChannelStateConfig(handler *testBlockingHandler) RedisChannelStateConfig {
	return RedisChannelStateConfig{
		Enabled: func(_ string) bool {
			return true
		},
		Handler:           handler.handle,
		NumPartitions:     1,
		SweepInterval:     testChannelStateSweepInterval,
		VacatedEventDelay: testChannelStateVacatedDelay,
		LeaseTTL:          testChannelStateLeaseTTL,
	}
}

// TestRedisChannelStateLostLeaseKeepsOneConsumer deletes the lease of a partition while its handler call is
// blocked. The node gives the partition up, and it must not start a second consumer for the partition while the
// first one still runs, even though the lease is free in Redis.
func TestRedisChannelStateLostLeaseKeepsOneConsumer(t *testing.T) {
	t.Parallel()
	handler := newTestBlockingHandler()
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testBlockingChannelStateConfig(handler), 0, false, 6379)
	t.Cleanup(handler.unblock)
	require.NoError(t, pm.AddPresence("lost-lease", "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return handler.inFlight.Load() == 1
	}, 3*testChannelStateLeaseTTL, 5*time.Millisecond)

	// The lease is gone from Redis while the handler still runs, as when another owner takes it.
	client := pm.shards[0].client
	require.NoError(t, client.Do(context.Background(), client.B().Del().Key(string(pm.state.partitionKey(0, "lease"))).Build()).Error())

	// The lease loop gives the partition up on its next renewal. No second call may start while the first one is
	// blocked, for several lease intervals.
	require.Never(t, func() bool {
		return handler.calls.Load() > 1
	}, 4*testChannelStateLeaseTTL, 5*time.Millisecond)
	require.Equal(t, int32(1), handler.peak.Load())
	require.Equal(t, 0, testOwnedPartitions(t, pm))
}

// TestRedisChannelStateCloseReleasesWhileConsumerHangs closes the presence manager while a handler call ignores
// its context. Close releases the leases and removes the membership before it waits for the consumer, so the
// leases are gone when Close returns with the context error.
func TestRedisChannelStateCloseReleasesWhileConsumerHangs(t *testing.T) {
	t.Parallel()
	handler := newTestBlockingHandler()
	node := testStateNode(t)
	prefix := getUniqueStatePrefix()
	pm := newTestRedisPresenceStateManager(t, node, prefix, testBlockingChannelStateConfig(handler), 0, false, 6379)
	t.Cleanup(handler.unblock)
	require.NoError(t, pm.AddPresence("hung-close", "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return handler.inFlight.Load() == 1
	}, 3*testChannelStateLeaseTTL, 5*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, pm.Close(ctx), context.DeadlineExceeded)

	client := pm.shards[0].client
	keys, err := client.Do(context.Background(), client.B().Keys().Pattern(prefix+".channel_state.*").Build()).AsStrSlice()
	require.NoError(t, err)
	for _, key := range keys {
		require.False(t, strings.HasSuffix(key, ".lease"), "lease held after Close: %s", key)
	}
	members, err := client.Do(context.Background(), client.B().Zcard().Key(pm.state.workersKey).Build()).AsInt64()
	require.NoError(t, err)
	require.Equal(t, int64(0), members)
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
