//go:build integration

package centrifuge

import (
	"context"
	"errors"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/rueidis"
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

// countingShardClient records the time of each command that the channel state worker sends to its shard: the GETs of
// the lease keys, which are the probes for a free partition, and the ZADDs of the workers set, which are the
// heartbeats. A test counts the events of a window by their times, so a late read of the counts cannot add an event
// that happened after the window.
type countingShardClient struct {
	rueidis.Client
	mu          sync.Mutex
	leaseGets   []time.Time
	workerZadds []time.Time
	// flapping makes every other heartbeat fail. The failing heartbeats are sent to wrongTypeKey, a string key,
	// which Redis refuses for a sorted set command. heartbeatFails is the state of the flapping.
	flapping       bool
	heartbeatFails bool
	wrongTypeKey   string
}

// DoMulti records the commands of the pipeline and sends them on.
func (c *countingShardClient) DoMulti(ctx context.Context, multi ...rueidis.Completed) []rueidis.RedisResult {
	for i := range multi {
		words := multi[i].Commands()
		if len(words) < 2 {
			continue
		}
		switch {
		case words[0] == "GET" && strings.HasSuffix(words[1], ".lease"):
			c.record(&c.leaseGets)
		case words[0] == "ZADD" && strings.HasSuffix(words[1], ".channel_state.workers"):
			c.record(&c.workerZadds)
			if c.nextHeartbeatFails() {
				return c.Client.DoMulti(ctx, c.failingCommands(len(multi))...)
			}
		}
	}
	return c.Client.DoMulti(ctx, multi...)
}

// flapHeartbeats makes every other heartbeat from now on fail, with the wrong type error of wrongTypeKey.
func (c *countingShardClient) flapHeartbeats(wrongTypeKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flapping = true
	c.wrongTypeKey = wrongTypeKey
}

// nextHeartbeatFails reports whether the heartbeat that is sent now fails. It toggles the flapping state.
func (c *countingShardClient) nextHeartbeatFails() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.flapping {
		return false
	}
	c.heartbeatFails = !c.heartbeatFails
	return c.heartbeatFails
}

// failingCommands returns count sorted set commands on the wrong type key, which Redis refuses.
func (c *countingShardClient) failingCommands(count int) []rueidis.Completed {
	c.mu.Lock()
	key := c.wrongTypeKey
	c.mu.Unlock()
	cmds := make([]rueidis.Completed, 0, count)
	for range count {
		cmds = append(cmds, c.B().Zadd().Key(key).ScoreMember().ScoreMember(0, "failing").Build())
	}
	return cmds
}

// record appends the current time to the events.
func (c *countingShardClient) record(events *[]time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	*events = append(*events, time.Now())
}

// countBetween returns how many of the events happened at or after start and before end.
func (c *countingShardClient) countBetween(events *[]time.Time, start time.Time, end time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, at := range *events {
		if !at.Before(start) && at.Before(end) {
			count++
		}
	}
	return count
}

// newCountingChannelStateNode creates a node with one Redis shard whose client is a countingShardClient, and a
// channel state presence manager on the shard. The client is set before the broker and the manager are created, so
// the workers never read the client of the shard while it is replaced.
func newCountingChannelStateNode(tb testing.TB, prefix string, state RedisChannelStateConfig) (*RedisPresenceManager, *countingShardClient) {
	tb.Helper()
	node := testStateNode(tb)
	shard, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(tb, err)
	counting := &countingShardClient{Client: shard.client}
	shard.client = counting
	broker, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix:   prefix,
		UseLists: true,
		Shards:   []*RedisShard{shard},
	})
	require.NoError(tb, err)
	node.SetBroker(broker)
	pm := newTestRedisPresenceStateManagerWithShards(tb, node, prefix, state, 0, false, []*RedisShard{shard})
	return pm, counting
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

// TestRedisChannelStateVacatedSurvivesStreamCap runs the check script on a stream at its cap, with a vacated
// event due. The event is kept pending, and its pending entry stays, moved one sweep interval ahead, so that a later
// sweep sends the event once the stream has room. The script runs on keys of the test, so the partition consumers do
// not touch them.
func TestRedisChannelStateVacatedSurvivesStreamCap(t *testing.T) {
	t.Parallel()
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testChannelStateConfig(&testChannelStateCollector{}), 0, false, 6379)
	client := pm.shards[0].client
	ctx := context.Background()

	stream := getUniqueStatePrefix() + ".stream"
	pending := getUniqueStatePrefix() + ".pending"
	expire := getUniqueStatePrefix() + ".expire"
	set := getUniqueStatePrefix() + ".set"
	hash := getUniqueStatePrefix() + ".hash"
	const channel = "vac-1"
	for range 3 {
		require.NoError(t, client.Do(ctx, client.B().Xadd().Key(stream).Id("*").FieldValue().FieldValue("c", "occupied").FieldValue("t", "occupied").FieldValue("ms", "1").Build()).Error())
	}
	nowMs := time.Now().UnixMilli()
	require.NoError(t, client.Do(ctx, client.B().Zadd().Key(pending).ScoreMember().ScoreMember(float64(nowMs-1), channel).Build()).Error())

	const sweepIntervalMs = 1000
	checkArgs := func(now int64) []string {
		return []string{channel, strconv.FormatInt(now, 10), strconv.FormatInt(now/1000, 10), strconv.FormatInt(now, 10), "0", "3", "0", strconv.Itoa(channelStateExpiredBatch), strconv.Itoa(sweepIntervalMs)}
	}
	keptKey := getUniqueStatePrefix() + ".kept"
	keys := []string{set, hash, stream, pending, expire, keptKey}
	kept, err := pm.state.checkScript.Exec(ctx, client, keys, checkArgs(nowMs)).AsInt64()
	require.NoError(t, err)
	require.Equal(t, int64(channelStateKeptPending), kept)
	// The kept event is moved one sweep interval ahead, so the sweep does not find it due again before then.
	score, err := client.Do(ctx, client.B().Zscore().Key(pending).Member(channel).Build()).AsFloat64()
	require.NoError(t, err, "the pending entry of the kept vacated event must stay")
	require.Equal(t, float64(nowMs+sweepIntervalMs), score)

	require.NoError(t, client.Do(ctx, client.B().Xtrim().Key(stream).Maxlen().Threshold("0").Build()).Error())
	sent, err := pm.state.checkScript.Exec(ctx, client, keys, checkArgs(nowMs+sweepIntervalMs)).AsInt64()
	require.NoError(t, err)
	require.Equal(t, int64(1), sent)
	length, err := client.Do(ctx, client.B().Xlen().Key(stream).Build()).AsInt64()
	require.NoError(t, err)
	require.Equal(t, int64(1), length)
}

// TestRedisChannelStateKeptVacatedBacksOff holds the stream of a partition at its cap and sweeps one kept vacated
// channel over 64 sweep intervals. Each keep doubles the delay of the next check, up to 16 intervals, so the checks
// grow logarithmically and not once per interval. Once the stream has room, the next due check sends the event and
// drops the attempt count of the channel.
func TestRedisChannelStateKeptVacatedBacksOff(t *testing.T) {
	t.Parallel()
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testChannelStateConfig(&testChannelStateCollector{}), 0, false, 6379)
	client := pm.shards[0].client
	ctx := context.Background()

	stream := getUniqueStatePrefix() + ".stream"
	pending := getUniqueStatePrefix() + ".pending"
	expire := getUniqueStatePrefix() + ".expire"
	set := getUniqueStatePrefix() + ".set"
	hash := getUniqueStatePrefix() + ".hash"
	kept := getUniqueStatePrefix() + ".kept"
	const channel = "vac-backoff"
	for range 3 {
		require.NoError(t, client.Do(ctx, client.B().Xadd().Key(stream).Id("*").FieldValue().FieldValue("c", "occupied").FieldValue("t", "occupied").FieldValue("ms", "1").Build()).Error())
	}
	const sweepIntervalMs = 1000
	const intervals = 64
	startMs := time.Now().UnixMilli()
	require.NoError(t, client.Do(ctx, client.B().Zadd().Key(pending).ScoreMember().ScoreMember(float64(startMs-1), channel).Build()).Error())

	checkArgs := func(now int64) []string {
		return []string{channel, strconv.FormatInt(now, 10), strconv.FormatInt(now/1000, 10), strconv.FormatInt(now, 10), "0", "3", "0", strconv.Itoa(channelStateExpiredBatch), strconv.Itoa(sweepIntervalMs)}
	}
	keys := []string{set, hash, stream, pending, expire, kept}
	checks := 0
	for i := range intervals {
		now := startMs + int64(i)*sweepIntervalMs
		score, err := client.Do(ctx, client.B().Zscore().Key(pending).Member(channel).Build()).AsFloat64()
		require.NoError(t, err)
		if score > float64(now) {
			continue
		}
		result, err := pm.state.checkScript.Exec(ctx, client, keys, checkArgs(now)).AsInt64()
		require.NoError(t, err)
		require.Equal(t, int64(channelStateKeptPending), result)
		checks++
	}
	require.LessOrEqual(t, checks, 10, "a kept vacated channel must be checked logarithmically, not once per interval")

	require.NoError(t, client.Do(ctx, client.B().Xtrim().Key(stream).Maxlen().Threshold("0").Build()).Error())
	score, err := client.Do(ctx, client.B().Zscore().Key(pending).Member(channel).Build()).AsFloat64()
	require.NoError(t, err)
	sent, err := pm.state.checkScript.Exec(ctx, client, keys, checkArgs(int64(score))).AsInt64()
	require.NoError(t, err)
	require.Equal(t, int64(1), sent)
	attempts, err := client.Do(ctx, client.B().Hexists().Key(kept).Field(channel).Build()).AsBool()
	require.NoError(t, err)
	require.False(t, attempts, "the attempt count of a delivered vacated event must be dropped")
}

// TestRedisChannelStateSweepStopsAtFullStream leaves 150 due vacated channels pending in one partition whose stream
// is full, and whose handler never runs, so the stream stays full. One sweep checks every page of them: each kept
// event moves one sweep interval ahead, so no channel is due after the sweep and the sweep does not stop at the
// first page.
func TestRedisChannelStateSweepStopsAtFullStream(t *testing.T) {
	t.Parallel()
	state := testChannelStateConfig(&testChannelStateCollector{})
	state.Handler = func(_ context.Context, _ []ChannelStateEvent) error {
		return errors.New("backend unavailable")
	}
	state.NumPartitions = 1
	state.MaxStreamLength = 1
	// The first lease cycle comes one LeaseTTL/5 after the start. The partition is leased by another owner right
	// after the start, so the node runs no consumer of its own and the sweep below is the only one.
	state.LeaseTTL = time.Minute
	node := testStateNode(t)
	shard, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(t, err)
	pm := newTestRedisPresenceStateManagerWithShards(t, node, getUniqueStatePrefix(), state, 0, false, []*RedisShard{shard})
	client := pm.shards[0].client
	ctx := context.Background()
	held, err := client.Do(ctx, client.B().Set().Key(string(pm.state.partitionKey(0, "lease"))).Value("other-owner").Nx().PxMilliseconds(60000).Build()).ToString()
	require.NoError(t, err)
	require.Equal(t, "OK", held)
	pending := string(pm.state.partitionKey(0, "pending"))

	// One event fills the stream of the cap, and nothing delivers it.
	require.NoError(t, client.Do(ctx, client.B().Xadd().Key(string(pm.state.partitionKey(0, "stream"))).Id("*").FieldValue().FieldValue("c", "full").FieldValue("t", "occupied").FieldValue("ms", "1").Build()).Error())
	require.Equal(t, int64(1), testStreamLength(t, pm, 0), "the stream must be full before the sweep")

	const dueChannels = 150
	nowMs := time.Now().UnixMilli()
	for i := range dueChannels {
		require.NoError(t, client.Do(ctx, client.B().Zadd().Key(pending).ScoreMember().ScoreMember(float64(nowMs-1000), "sweep-cap-"+strconv.Itoa(i)).Build()).Error())
	}

	err = pm.state.worker.sweep(ctx, pm.state.shardOf(0), 0)
	require.NoError(t, err)
	due, err := client.Do(ctx, client.B().Zcount().Key(pending).Min("-inf").Max(strconv.FormatInt(nowMs, 10)).Build()).AsInt64()
	require.NoError(t, err)
	require.Equal(t, int64(0), due, "every due channel is checked in one sweep, and the kept ones are due again after the interval")
	left, err := client.Do(ctx, client.B().Zcard().Key(pending).Build()).AsInt64()
	require.NoError(t, err)
	require.Equal(t, int64(dueChannels), left, "the kept channels stay pending, one interval ahead")
	require.Equal(t, 0.0, metricValue(t, pm.state.metrics.eventsDropped.WithLabelValues(string(ChannelStateVacated))), "a vacated event kept pending is not a drop")
}

// TestRedisChannelStateKeptPageKeepsExpireWork leaves 100 due vacated channels in one partition whose stream is full,
// so their events are kept, and one channel whose presence expired. The sweep reads the pending and the expire sets in
// the same page, so the expired channel is checked in the step with the kept events, and its presence is resolved there.
// The channel is on the first page of the expire set, so this test does not reach a later page. The sweep past kept
// events is covered by TestRedisChannelStateSweepStopsAtFullStream.
func TestRedisChannelStateKeptPageKeepsExpireWork(t *testing.T) {
	t.Parallel()
	state := testChannelStateConfig(&testChannelStateCollector{})
	state.Handler = func(_ context.Context, _ []ChannelStateEvent) error {
		return errors.New("backend unavailable")
	}
	state.NumPartitions = 1
	state.MaxStreamLength = 1
	// The partition is leased by another owner for the whole test, so the node runs no consumer of its own.
	state.LeaseTTL = time.Minute
	node := testStateNode(t)
	shard, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(t, err)
	pm := newTestRedisPresenceStateManagerWithShards(t, node, getUniqueStatePrefix(), state, 0, false, []*RedisShard{shard})
	client := pm.shards[0].client
	ctx := context.Background()
	held, err := client.Do(ctx, client.B().Set().Key(string(pm.state.partitionKey(0, "lease"))).Value("other-owner").Nx().PxMilliseconds(60000).Build()).ToString()
	require.NoError(t, err)
	require.Equal(t, "OK", held)
	pending := string(pm.state.partitionKey(0, "pending"))
	expire := string(pm.state.partitionKey(0, "expire"))

	require.NoError(t, client.Do(ctx, client.B().Xadd().Key(string(pm.state.partitionKey(0, "stream"))).Id("*").FieldValue().FieldValue("c", "full").FieldValue("t", "occupied").FieldValue("ms", "1").Build()).Error())
	require.Equal(t, int64(1), testStreamLength(t, pm, 0), "the stream must be full before the sweep")

	nowMs := time.Now().UnixMilli()
	for i := range channelStateSweepPageSize {
		require.NoError(t, client.Do(ctx, client.B().Zadd().Key(pending).ScoreMember().ScoreMember(float64(nowMs-1000), "a-kept-"+strconv.Itoa(i)).Build()).Error())
	}
	require.NoError(t, client.Do(ctx, client.B().Zadd().Key(expire).ScoreMember().ScoreMember(float64(nowMs-1000), "z-behind").Build()).Error())

	require.NoError(t, pm.state.worker.sweep(ctx, pm.state.shardOf(0), 0))
	_, err = client.Do(ctx, client.B().Zscore().Key(pending).Member("z-behind").Build()).AsFloat64()
	require.NoError(t, err, "the channel behind the kept events must be swept in the same step")
	_, err = client.Do(ctx, client.B().Zscore().Key(expire).Member("z-behind").Build()).AsFloat64()
	require.True(t, rueidis.IsRedisNil(err), "the expired presence of the channel behind the kept events must be resolved")
}

// TestRedisChannelStateFailingDeliveryReturnsToSweep runs one consume step of a partition whose handler fails. The
// batch stays in the stream and is delivered again, but the step returns after the retry delay, so the consumer goes
// back to the sweep of the partition on its next tick. A step that retried in place would never sweep again.
func TestRedisChannelStateFailingDeliveryReturnsToSweep(t *testing.T) {
	t.Parallel()
	state := testChannelStateConfig(&testChannelStateCollector{})
	state.Handler = func(_ context.Context, _ []ChannelStateEvent) error {
		return errors.New("backend unavailable")
	}
	state.NumPartitions = 1
	// The first lease cycle comes one LeaseTTL/5 after the start, so a long LeaseTTL leaves the lease below
	// room to be taken before it.
	state.LeaseTTL = time.Minute
	node := testStateNode(t)
	shard, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(t, err)
	pm := newTestRedisPresenceStateManagerWithShards(t, node, getUniqueStatePrefix(), state, 0, false, []*RedisShard{shard})

	// The partition is leased by another owner before the first lease cycle of the node, so the node runs no consumer
	// of its own and the consume step below is the only one.
	client := pm.shards[0].client
	held, err := client.Do(context.Background(), client.B().Set().Key(string(pm.state.partitionKey(0, "lease"))).Value("other-owner").Nx().PxMilliseconds(60000).Build()).ToString()
	require.NoError(t, err)
	require.Equal(t, "OK", held)
	require.NoError(t, pm.AddPresence("poison", "client-1", &ClientInfo{}))
	part := &channelStatePartition{p: 0, shard: pm.shards[0]}
	part.extendLease(time.Now(), time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan bool, 1)
	var backoff channelStateBackoff
	step := func() bool {
		go func() {
			returned <- pm.state.worker.consumeOnce(ctx, pm.shards[0], 0, part, &backoff)
		}()
		select {
		case more := <-returned:
			return more
		case <-time.After(5 * time.Second):
			t.Fatal("the consume step retried the failing batch in place and did not return to the sweep")
			return false
		}
	}
	require.True(t, step())
	require.Equal(t, int64(1), testStreamLength(t, pm, 0), "the failed event must stay in the stream")

	// A channel that becomes due after the first step is sent by the sweep of the second step, which runs before the
	// batch is delivered again.
	require.NoError(t, client.Do(ctx, client.B().Zadd().Key(string(pm.state.partitionKey(0, "pending"))).ScoreMember().ScoreMember(float64(time.Now().UnixMilli()-1000), "due-later").Build()).Error())
	require.True(t, step())
	require.Equal(t, int64(2), testStreamLength(t, pm, 0), "the sweep of the second step must run before the failing batch is delivered again")
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
	// Each node owns 4 partitions. Node 1 gives up one partition per renewal of LeaseTTL/5, so it frees its last one
	// within one LeaseTTL of the start of node 2. Node 2 probes for a free partition with a delay of at most LeaseTTL,
	// since the delay is capped at LeaseTTL, so it takes its last partition within 2 LeaseTTL. The bound adds one
	// LeaseTTL for the scheduling of the test under -race.
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

	// The survivor takes the partitions once the lease of the crashed worker has expired. Its probe of a held
	// partition backs off up to LeaseTTL. The worst case is about 2 LeaseTTL: the lease of the crashed worker expires up
	// to LeaseTTL after its last renewal, and the probe delay of the survivor can add up to another LeaseTTL. The bound
	// below, 3 LeaseTTL and one second, covers that worst case.
	shards2[0].Close()
	require.Eventually(t, func() bool {
		return testOwnedPartitions(t, pm1) == 8
	}, 3*testChannelStateLeaseTTL+time.Second, 10*time.Millisecond)

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

// TestRedisChannelStateTinyTimingsAreRaised sets a SweepInterval and a LeaseTTL below their floors. The sweep is raised
// to 100ms and the lease is raised to 500ms, so the lease loop runs at most every 100ms.
func TestRedisChannelStateTinyTimingsAreRaised(t *testing.T) {
	t.Parallel()
	node := testStateNode(t)
	config := testChannelStateConfig(&testChannelStateCollector{})
	config.SweepInterval = time.Millisecond
	config.LeaseTTL = 5 * time.Millisecond
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), config, 0, false, 6379)
	require.Equal(t, 100*time.Millisecond, pm.state.sweepInterval)
	require.Equal(t, 500*time.Millisecond, pm.state.leaseTTL)
}

// TestRedisChannelStateIdleWorkerProbeBackoff holds the only partition of the prefix with a lease of another owner.
// The node owns nothing and wants its share, so it probes the lease key on each tick. Over ten ticks it must not
// probe on every tick: the probes back off while every probe finds the partition held.
func TestRedisChannelStateIdleWorkerProbeBackoff(t *testing.T) {
	t.Parallel()
	prefix := getUniqueStatePrefix()
	state := testChannelStateConfig(&testChannelStateCollector{})
	state.NumPartitions = 1
	state.LeaseTTL = 2 * time.Second
	pm, counting := newCountingChannelStateNode(t, prefix, state)

	client := pm.shards[0].client
	held, err := client.Do(context.Background(), client.B().Set().Key(string(pm.state.partitionKey(0, "lease"))).Value("other-owner").Nx().PxMilliseconds(60000).Build()).ToString()
	require.NoError(t, err)
	require.Equal(t, "OK", held)

	// Ten lease ticks: a tick is a fifth of LeaseTTL, which is 0.4 seconds here. The delay between probes doubles from one
	// tick and is capped at LeaseTTL, and the probes land on the ticks, so the spacing is quantized and not exactly the
	// doubling. The window is 2 * LeaseTTL = 4 seconds and is measured from the start of the test. The probes are counted
	// by their send times inside the window, so the bound does not depend on when the test reads the count. Without the
	// backoff the node probes about ten times, which the bound refuses. With the schedule above the probes fall about
	// 0.4, 0.8, 1.6 and 3.2 seconds after the first tick, so the window holds three or four probes. The bound of six is
	// that schedule plus a margin of two probes for the phase of the ticks against the window start. The lower bound waits
	// for the first probe, so a
	// counter that stops counting fails the test. The counter belongs to the shard client of this test, so no other test
	// adds to it.
	start := time.Now()
	end := start.Add(2 * state.LeaseTTL)
	require.Eventually(t, func() bool {
		return counting.countBetween(&counting.leaseGets, start, time.Now().Add(time.Hour)) > 0
	}, 10*state.LeaseTTL, 10*time.Millisecond)
	require.Never(t, func() bool {
		return counting.countBetween(&counting.leaseGets, start, end) > 6
	}, time.Until(end), 10*time.Millisecond)
	require.LessOrEqual(t, counting.countBetween(&counting.leaseGets, start, end), 6)
	require.Equal(t, 0, testOwnedPartitions(t, pm))
}

// TestRedisChannelStateFlappingHeartbeatKeepsProbeBackoff makes every other heartbeat of an idle worker fail. The
// share is the one of the last heartbeat that succeeded, so a failed heartbeat must not reset the probe delay: the
// probes keep backing off, and the window holds about four probes. Each probe reads the leases of both partitions,
// so four probes are eight reads. The bound is those eight reads plus a margin of two probes, four reads.
func TestRedisChannelStateFlappingHeartbeatKeepsProbeBackoff(t *testing.T) {
	t.Parallel()
	prefix := getUniqueStatePrefix()
	state := testChannelStateConfig(&testChannelStateCollector{})
	state.NumPartitions = 2
	state.LeaseTTL = 2 * time.Second
	pm, counting := newCountingChannelStateNode(t, prefix, state)

	client := pm.shards[0].client
	ctx := context.Background()
	// Another worker counts in the workers set, so the share of this node is one of the two partitions.
	require.NoError(t, client.Do(ctx, client.B().Zadd().Key(pm.state.workersKey).ScoreMember().ScoreMember(float64(time.Now().Add(time.Hour).UnixMilli()), "other-worker").Build()).Error())
	for p := range state.NumPartitions {
		held, err := client.Do(ctx, client.B().Set().Key(string(pm.state.partitionKey(p, "lease"))).Value("other-owner").Nx().PxMilliseconds(60000).Build()).ToString()
		require.NoError(t, err)
		require.Equal(t, "OK", held)
	}
	wrongTypeKey := prefix + ".string"
	require.NoError(t, client.Do(ctx, client.B().Set().Key(wrongTypeKey).Value("string").Build()).Error())
	counting.flapHeartbeats(wrongTypeKey)

	// The window is two LeaseTTL. The lower bound waits for the first probe of the window, so a counter that stops
	// counting fails the test.
	start := time.Now()
	end := start.Add(2 * state.LeaseTTL)
	require.Eventually(t, func() bool {
		return counting.countBetween(&counting.leaseGets, start, time.Now().Add(time.Hour)) > 0
	}, 10*state.LeaseTTL, 10*time.Millisecond)
	require.Never(t, func() bool {
		return counting.countBetween(&counting.leaseGets, start, end) > 12
	}, time.Until(end), 10*time.Millisecond)
	require.LessOrEqual(t, counting.countBetween(&counting.leaseGets, start, end), 12)
	require.Equal(t, 0, testOwnedPartitions(t, pm))
}

// TestRedisChannelStateHeartbeatRate checks that a worker writes its heartbeat to the workers set of the shard
// less often than on every lease tick. The set is only a count for the share, and LeaseTTL keeps the entries alive.
func TestRedisChannelStateHeartbeatRate(t *testing.T) {
	t.Parallel()
	prefix := getUniqueStatePrefix()
	state := testChannelStateConfig(&testChannelStateCollector{})
	state.NumPartitions = 1
	state.LeaseTTL = 2 * time.Second
	pm, counting := newCountingChannelStateNode(t, prefix, state)
	waitForChannelStatePartitions(t, pm, 1)

	// The window is two LeaseTTL, which is ten lease ticks: one heartbeat per tick would write ten times. Two heartbeats
	// are at least LeaseTTL/3 apart, so the window holds at most seven of them. The heartbeats are counted by their send
	// times inside the window, so the upper bound does not depend on when the test reads the count. The lower bound waits
	// for the first heartbeat of the window. The counter belongs to the shard client of this test, so no other test adds
	// to it.
	start := time.Now()
	end := start.Add(2 * state.LeaseTTL)
	require.Eventually(t, func() bool {
		return counting.countBetween(&counting.workerZadds, start, time.Now().Add(time.Hour)) > 0
	}, 10*state.LeaseTTL, 10*time.Millisecond)
	require.Never(t, func() bool {
		return counting.countBetween(&counting.workerZadds, start, end) > 7
	}, time.Until(end), 10*time.Millisecond)
	require.LessOrEqual(t, counting.countBetween(&counting.workerZadds, start, end), 7)
	require.Equal(t, 1, testOwnedPartitions(t, pm))
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
