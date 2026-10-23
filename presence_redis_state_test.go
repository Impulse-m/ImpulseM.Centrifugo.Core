//go:build integration

package centrifuge

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

const (
	testChannelStateSweepInterval = 50 * time.Millisecond
	testChannelStateVacatedDelay  = 200 * time.Millisecond
	testChannelStateLeaseTTL      = 500 * time.Millisecond
)

// testChannelStateCollector is a ChannelStateHandler that keeps the delivered events.
type testChannelStateCollector struct {
	mu       sync.Mutex
	events   []ChannelStateEvent
	attempts []time.Time
	failures int
	failAll  bool
}

func (c *testChannelStateCollector) handle(_ context.Context, events []ChannelStateEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempts = append(c.attempts, time.Now())
	if c.failAll {
		return errors.New("backend unavailable")
	}
	if c.failures > 0 {
		c.failures--
		return errors.New("backend unavailable")
	}
	c.events = append(c.events, events...)
	return nil
}

func (c *testChannelStateCollector) setFailures(failures int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures = failures
}

func (c *testChannelStateCollector) setFailAll(failAll bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failAll = failAll
}

func (c *testChannelStateCollector) snapshot() []ChannelStateEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ChannelStateEvent(nil), c.events...)
}

func (c *testChannelStateCollector) attemptTimes() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.attempts...)
}

// testChannelStateQuietWindow is how long a test watches that no extra channel state event arrives.
const testChannelStateQuietWindow = 3 * testChannelStateVacatedDelay

// requireSettled fails when count changes within the window, so that a test checks that no extra event
// arrives after the expected ones. The check ends at the first change.
func requireSettled(tb testing.TB, count func() int, window time.Duration) {
	tb.Helper()
	before := count()
	require.Never(tb, func() bool {
		return count() != before
	}, window, 10*time.Millisecond)
}

// eventsOfType returns the events of the channel state event type, in order.
func eventsOfType(events []ChannelStateEvent, eventType ChannelStateEventType) []ChannelStateEvent {
	var res []ChannelStateEvent
	for _, e := range events {
		if e.Type == eventType {
			res = append(res, e)
		}
	}
	return res
}

func testChannelStateConfig(collector *testChannelStateCollector) RedisChannelStateConfig {
	return RedisChannelStateConfig{
		Enabled: func(_ string) bool {
			return true
		},
		Handler:           collector.handle,
		SweepInterval:     testChannelStateSweepInterval,
		VacatedEventDelay: testChannelStateVacatedDelay,
		LeaseTTL:          testChannelStateLeaseTTL,
	}
}

// newTestRedisPresenceStateManager creates a presence manager with channel state on a single Redis shard.
// The node is shut down when the test ends.
func newTestRedisPresenceStateManager(
	tb testing.TB,
	n *Node,
	prefix string,
	state RedisChannelStateConfig,
	presenceTTL time.Duration,
	hashFieldTTL bool,
	port int,
) *RedisPresenceManager {
	tb.Helper()
	s, err := NewRedisShard(n, testSingleRedisConf(port))
	require.NoError(tb, err)
	tb.Cleanup(s.Close)
	pm := newTestRedisPresenceStateManagerWithShards(tb, n, prefix, state, presenceTTL, hashFieldTTL, []*RedisShard{s})
	waitForChannelStatePartitions(tb, pm, state.NumPartitions)
	return pm
}

// waitForChannelStatePartitions waits until the node consumes every partition, so that the
// timing of the tests starts after the first lease cycle, where the partitions are taken.
func waitForChannelStatePartitions(tb testing.TB, pm *RedisPresenceManager, numPartitions int) {
	tb.Helper()
	if numPartitions <= 0 {
		numPartitions = channelStateDefaultNumPartitions
	}
	require.Eventually(tb, func() bool {
		return testOwnedPartitions(tb, pm) == numPartitions
	}, 5*time.Second, 5*time.Millisecond)
}

func newTestRedisPresenceStateManagerWithShards(
	tb testing.TB,
	n *Node,
	prefix string,
	state RedisChannelStateConfig,
	presenceTTL time.Duration,
	hashFieldTTL bool,
	shards []*RedisShard,
) *RedisPresenceManager {
	tb.Helper()
	pm, err := NewRedisPresenceManager(n, RedisPresenceManagerConfig{
		Prefix:          prefix,
		PresenceTTL:     presenceTTL,
		Shards:          shards,
		UseHashFieldTTL: hashFieldTTL,
		ChannelState:    &state,
	})
	require.NoError(tb, err)
	n.SetPresenceManager(pm)
	require.NoError(tb, n.Run())
	tb.Cleanup(func() {
		_ = n.Shutdown(context.Background())
		for _, s := range shards {
			s.Close()
		}
	})
	return pm
}

// testOccupiedVacated adds a client, removes it and checks the occupied event and the
// vacated event after the delay. It runs on Redis with the given hash field TTL mode.
func testOccupiedVacated(t *testing.T, port int, hashFieldTTL bool) {
	collector := &testChannelStateCollector{}
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testChannelStateConfig(collector), 0, hashFieldTTL, port)

	const ch = "occupied-vacated"
	require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == 1
	}, 5*time.Second, 10*time.Millisecond)

	removeAt := time.Now()
	require.NoError(t, pm.RemovePresence(ch, "client-1", ""))
	requireSettled(t, func() int { return len(collector.snapshot()) }, testChannelStateVacatedDelay/2)
	require.Empty(t, eventsOfType(collector.snapshot(), ChannelStateVacated))

	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateVacated)) == 1
	}, 5*time.Second, 10*time.Millisecond)
	vacated := eventsOfType(collector.snapshot(), ChannelStateVacated)[0]
	require.Equal(t, ch, vacated.Channel)
	require.GreaterOrEqual(t, vacated.Time.Sub(removeAt), testChannelStateVacatedDelay-time.Millisecond)
}

func TestRedisChannelStateOccupiedOnce(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testChannelStateConfig(collector), 0, false, 6379)

	const ch = "occupied-once"
	addAt := time.Now()
	require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	require.NoError(t, pm.AddPresence(ch, "client-2", &ClientInfo{}))
	require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))

	require.Eventually(t, func() bool {
		return len(collector.snapshot()) >= 1
	}, 5*time.Second, 10*time.Millisecond)
	requireSettled(t, func() int { return len(collector.snapshot()) }, testChannelStateQuietWindow)

	events := collector.snapshot()
	require.Len(t, events, 1)
	require.Equal(t, ChannelStateOccupied, events[0].Type)
	require.Equal(t, ch, events[0].Channel)
	require.WithinDuration(t, addAt, events[0].Time, time.Second)
}

func TestRedisChannelStateVacatedAfterDelay(t *testing.T) {
	t.Parallel()
	testOccupiedVacated(t, 6379, false)
}

func TestRedisChannelStateResubscribeCancelsVacated(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testChannelStateConfig(collector), 0, false, 6379)

	const ch = "resubscribe"
	require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return len(collector.snapshot()) == 1
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, pm.RemovePresence(ch, "client-1", ""))
	require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	requireSettled(t, func() int { return len(collector.snapshot()) }, testChannelStateQuietWindow)

	events := collector.snapshot()
	require.Len(t, eventsOfType(events, ChannelStateOccupied), 1)
	require.Empty(t, eventsOfType(events, ChannelStateVacated))

	require.NoError(t, pm.RemovePresence(ch, "client-1", ""))
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateVacated)) == 1
	}, 5*time.Second, 10*time.Millisecond)
}

func TestRedisChannelStateNegativeDelayHasNoGrace(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	state := testChannelStateConfig(collector)
	state.VacatedEventDelay = -1
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), state, 0, false, 6379)
	require.Equal(t, time.Duration(0), pm.state.vacatedEventDelay)

	const ch = "no-grace"
	require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == 1
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, pm.RemovePresence(ch, "client-1", ""))
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateVacated)) == 1
	}, time.Second, 10*time.Millisecond)
}

func TestRedisChannelStateZeroDelayIsDefault(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	state := testChannelStateConfig(collector)
	state.VacatedEventDelay = 0
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), state, 0, false, 6379)
	require.Equal(t, channelStateDefaultVacatedEventDelay, pm.state.vacatedEventDelay)
}

func TestRedisChannelStateOrderPerChannel(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testChannelStateConfig(collector), 0, false, 6379)

	const ch = "order"
	require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return len(collector.snapshot()) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, pm.RemovePresence(ch, "client-1", ""))
	require.Eventually(t, func() bool {
		return len(collector.snapshot()) == 2
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return len(collector.snapshot()) == 3
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, pm.RemovePresence(ch, "client-1", ""))
	require.Eventually(t, func() bool {
		return len(collector.snapshot()) == 4
	}, 5*time.Second, 10*time.Millisecond)

	events := collector.snapshot()
	require.Equal(t, []ChannelStateEventType{
		ChannelStateOccupied, ChannelStateVacated, ChannelStateOccupied, ChannelStateVacated,
	}, []ChannelStateEventType{events[0].Type, events[1].Type, events[2].Type, events[3].Type})
	for _, e := range events {
		require.Equal(t, ch, e.Channel)
	}
}

func TestRedisChannelStateExpiredPresence(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	node := testStateNode(t)
	const presenceTTL = time.Second
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testChannelStateConfig(collector), presenceTTL, false, 6379)

	const ch = "expired"
	addAt := time.Now()
	require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == 1
	}, 5*time.Second, 10*time.Millisecond)

	// No refresh: the presence expires and no vacated event is sent before its TTL.
	requireSettled(t, func() int { return len(collector.snapshot()) }, presenceTTL/2)
	require.Empty(t, eventsOfType(collector.snapshot(), ChannelStateVacated))

	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateVacated)) == 1
	}, 5*time.Second, 10*time.Millisecond)
	vacated := eventsOfType(collector.snapshot(), ChannelStateVacated)[0]
	require.GreaterOrEqual(t, vacated.Time.Sub(addAt), presenceTTL-time.Millisecond)
}

// TestRedisChannelStateStaleEntriesDoNotKeepOccupied keeps a stale entry in the hash: client-a
// expires without a refresh while client-c keeps the hash of the channel alive. The hash key
// has the presence TTL, so a stale entry survives only next to a live client. When client-c
// leaves, the remove cleans the stale entry and arms the vacated event at once.
func TestRedisChannelStateStaleEntriesDoNotKeepOccupied(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	node := testStateNode(t)
	const presenceTTL = time.Second
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testChannelStateConfig(collector), presenceTTL, false, 6379)

	const ch = "stale"
	require.NoError(t, pm.AddPresence(ch, "client-a", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == 1
	}, 5*time.Second, 10*time.Millisecond)

	deadline := time.Now().Add(presenceTTL + presenceTTL/2)
	for time.Now().Before(deadline) {
		require.NoError(t, pm.AddPresence(ch, "client-c", &ClientInfo{}))
		time.Sleep(presenceTTL / 4)
	}
	require.Len(t, eventsOfType(collector.snapshot(), ChannelStateOccupied), 1)
	require.Empty(t, eventsOfType(collector.snapshot(), ChannelStateVacated))

	removeAt := time.Now()
	require.NoError(t, pm.RemovePresence(ch, "client-c", ""))
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateVacated)) == 1
	}, 5*time.Second, 10*time.Millisecond)
	vacated := eventsOfType(collector.snapshot(), ChannelStateVacated)[0]
	require.GreaterOrEqual(t, vacated.Time.Sub(removeAt), testChannelStateVacatedDelay-time.Millisecond)

	// The refreshes of client-c and the stale entry did not send a second occupied event.
	require.Len(t, eventsOfType(collector.snapshot(), ChannelStateOccupied), 1)
}

// TestRedisChannelStateAddAfterPresenceKeyExpired adds a client after the hash of the channel
// expired on Redis, before the sweep sent the vacated event: the channel is still occupied for
// the backend, so the add must not send occupied again.
func TestRedisChannelStateAddAfterPresenceKeyExpired(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	node := testStateNode(t)
	const presenceTTL = time.Second
	state := testChannelStateConfig(collector)
	// The sweep runs once in the window of the test, after the second add.
	state.SweepInterval = 2 * time.Second
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), state, presenceTTL, false, 6379)

	const ch = "late-add"
	require.NoError(t, pm.AddPresence(ch, "client-a", &ClientInfo{}))
	time.Sleep(presenceTTL + presenceTTL/3)
	require.NoError(t, pm.AddPresence(ch, "client-b", &ClientInfo{}))

	require.Eventually(t, func() bool {
		return len(collector.snapshot()) >= 1
	}, 10*time.Second, 10*time.Millisecond)
	requireSettled(t, func() int { return len(collector.snapshot()) }, state.SweepInterval)

	events := collector.snapshot()
	require.Len(t, eventsOfType(events, ChannelStateOccupied), 1)
	require.Empty(t, eventsOfType(events, ChannelStateVacated))
}

// TestRedisChannelStateRepeatedRemoveSendsOneVacated removes the same client twice: only the first
// remove, the one that found the client, arms the vacated event.
func TestRedisChannelStateRepeatedRemoveSendsOneVacated(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testChannelStateConfig(collector), 0, false, 6379)

	const ch = "repeated-remove"
	require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == 1
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, pm.RemovePresence(ch, "client-1", ""))
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateVacated)) == 1
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, pm.RemovePresence(ch, "client-1", ""))
	requireSettled(t, func() int { return len(collector.snapshot()) }, testChannelStateQuietWindow)
	require.Len(t, eventsOfType(collector.snapshot(), ChannelStateVacated), 1)
}

// TestRedisChannelStateDueInBothSetsSendsOneVacated makes a channel due in the pending and the expire
// sets at once, as after a sweep gap longer than the presence TTL. The sweep lists the channel twice,
// and the vacated event must still be sent once.
func TestRedisChannelStateDueInBothSetsSendsOneVacated(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), testChannelStateConfig(collector), 0, false, 6379)

	const ch = "due-in-both-sets"
	require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, pm.RemovePresence(ch, "client-1", ""))

	// Both sets hold the channel with a score in the past, so the next sweep lists it in both.
	p := pm.state.partition(ch)
	client := pm.state.shardOf(p).client
	script := rueidis.NewLuaScript(`redis.call("zadd", KEYS[1], 1, ARGV[1]); redis.call("zadd", KEYS[2], 1, ARGV[1]); return 1`)
	require.NoError(t, script.Exec(
		context.Background(),
		client,
		[]string{string(pm.state.partitionKey(p, "pending")), string(pm.state.partitionKey(p, "expire"))},
		[]string{ch},
	).Error())

	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateVacated)) == 1
	}, 5*time.Second, 10*time.Millisecond)
	requireSettled(t, func() int { return len(collector.snapshot()) }, testChannelStateQuietWindow)
	require.Len(t, eventsOfType(collector.snapshot(), ChannelStateVacated), 1)
}

// TestRedisChannelStateOverlappingSweepKeepsOccupied runs two sweeps of one partition that overlap, as when a
// consumer still sweeps after its lease was released. The first sweep lists the due channel, the second one
// sends its vacated event, and the first one checks its stale list afterwards. A client that subscribes next
// must still send occupied, so the stale check must not arm a vacated event again.
func TestRedisChannelStateOverlappingSweepKeepsOccupied(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	node := testStateNode(t)
	state := testChannelStateConfig(collector)
	// A negative delay makes the channel due at once after its last client left.
	state.VacatedEventDelay = -1
	// The consumers start on the first lease cycle, one LeaseTTL/5 after the start. The writes of the test
	// finish well before that, so the first read of a consumer delivers all of them, in order.
	state.LeaseTTL = 5 * time.Second
	state.SweepInterval = time.Hour
	s, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(t, err)
	t.Cleanup(s.Close)
	pm := newTestRedisPresenceStateManagerWithShards(t, node, getUniqueStatePrefix(), state, 0, false, []*RedisShard{s})

	const ch = "overlapping-sweep"
	require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	require.NoError(t, pm.RemovePresence(ch, "client-1", ""))

	p := pm.state.partition(ch)
	sh := pm.state.shardOf(p)
	ctx := context.Background()
	nowMs := time.Now().UnixMilli()
	stale, _, err := pm.state.worker.duePage(ctx, sh, p, nowMs)
	require.NoError(t, err)
	require.Equal(t, []string{ch}, stale)

	require.NoError(t, pm.state.worker.sweep(ctx, sh, p))
	require.NoError(t, pm.state.checkChannels(ctx, sh, p, stale, nowMs))
	require.NoError(t, pm.AddPresence(ch, "client-2", &ClientInfo{}))

	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == 2
	}, 10*time.Second, 10*time.Millisecond)
	requireSettled(t, func() int { return len(collector.snapshot()) }, testChannelStateQuietWindow)

	events := collector.snapshot()
	require.Len(t, eventsOfType(events, ChannelStateOccupied), 2)
	require.Len(t, eventsOfType(events, ChannelStateVacated), 1)
}

func TestRedisChannelStateHashFieldTTL(t *testing.T) {
	t.Parallel()
	if !supportsHashFieldTTL(t, 6379) {
		t.Skip("hash field TTL requires Redis 7.4 or later")
	}
	testOccupiedVacated(t, 6379, true)
}

func TestRedisChannelStateDisabledChannelKeysUnchanged(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	state := testChannelStateConfig(collector)
	state.Enabled = func(channel string) bool {
		return strings.HasPrefix(channel, "state:")
	}
	node := testStateNode(t)
	prefix := getUniqueStatePrefix()
	pm := newTestRedisPresenceStateManager(t, node, prefix, state, 0, false, 6379)

	require.NoError(t, pm.AddPresence("plain", "client-1", &ClientInfo{}))
	// The partition leases of the worker are not channel keys: only the presence keys are compared.
	keys, err := pm.shards[0].client.Do(context.Background(), pm.shards[0].client.B().Keys().Pattern(prefix+".presence.*").Build()).AsStrSlice()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		prefix + ".presence.data.plain",
		prefix + ".presence.expire.plain",
	}, keys)

	require.NoError(t, pm.AddPresence("state:channel", "client-1", &ClientInfo{}))
	require.Eventually(t, func() bool {
		return len(collector.snapshot()) == 1
	}, 5*time.Second, 10*time.Millisecond)
	stateKeys, err := pm.shards[0].client.Do(context.Background(), pm.shards[0].client.B().Keys().Pattern(prefix+"*").Build()).AsStrSlice()
	require.NoError(t, err)
	for _, key := range stateKeys {
		if strings.Contains(key, "state:channel") {
			require.Contains(t, key, "{cs")
		}
	}

	// A presence manager without ChannelState keeps no state at all.
	node2 := testNode(t)
	pm2 := NewTestRedisPresenceManagerWithPrefix(t, node2, getUniqueStatePrefix(), false, false, 6379)
	require.Nil(t, pm2.state)
	require.NoError(t, pm2.AddPresence("plain", "client-1", &ClientInfo{}))
}

// TestRedisChannelStateTaggedChannelDoesNotShareKeys names channels without channel state with the
// partition tag of a channel with channel state, and with the key infix of channel state. Their presence
// must not change the occupied and vacated events or the presence list of the state channel.
func TestRedisChannelStateTaggedChannelDoesNotShareKeys(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	const ch = "state:shared"
	state := testChannelStateConfig(collector)
	state.Enabled = func(channel string) bool {
		return channel == ch
	}
	node := testStateNode(t)
	pm := newTestRedisPresenceStateManager(t, node, getUniqueStatePrefix(), state, 0, false, 6379)

	p := strconv.Itoa(pm.state.partition(ch))
	for _, plain := range []string{"{cs" + p + "}." + ch, "channel_state.{cs" + p + "}.presence.data." + ch} {
		require.NoError(t, pm.AddPresence(plain, "attacker", &ClientInfo{}))
	}
	require.NoError(t, pm.AddPresence(ch, "victim", &ClientInfo{}))

	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == 1
	}, 5*time.Second, 10*time.Millisecond)

	presence, err := pm.Presence(ch)
	require.NoError(t, err)
	require.Len(t, presence, 1)
	require.Contains(t, presence, "victim")

	require.NoError(t, pm.RemovePresence(ch, "victim", ""))
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateVacated)) == 1
	}, 5*time.Second, 10*time.Millisecond)

	events := collector.snapshot()
	require.Len(t, eventsOfType(events, ChannelStateOccupied), 1)
	require.Equal(t, ch, eventsOfType(events, ChannelStateOccupied)[0].Channel)
	require.Equal(t, ch, eventsOfType(events, ChannelStateVacated)[0].Channel)
}

// TestRedisChannelStateSharedRegistry runs two nodes with channel state on one registry. The second
// presence manager reuses the collectors of the first, so both count into the same series.
func TestRedisChannelStateSharedRegistry(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	newNode := func() *Node {
		node, err := New(Config{
			LogLevel:   LogLevelDebug,
			LogHandler: func(entry LogEntry) {},
			Metrics: MetricsConfig{
				RegistererGatherer: registry,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = node.Shutdown(context.Background())
		})
		return node
	}
	collector := &testChannelStateCollector{}
	state := testChannelStateConfig(collector)
	prefix := getUniqueStatePrefix()

	node1 := newNode()
	s1, err := NewRedisShard(node1, testSingleRedisConf(6379))
	require.NoError(t, err)
	t.Cleanup(s1.Close)
	pm1 := newTestRedisPresenceStateManagerWithShards(t, node1, prefix, state, 0, false, []*RedisShard{s1})

	node2 := newNode()
	s2, err := NewRedisShard(node2, testSingleRedisConf(6379))
	require.NoError(t, err)
	t.Cleanup(s2.Close)
	pm2 := newTestRedisPresenceStateManagerWithShards(t, node2, prefix, state, 0, false, []*RedisShard{s2})

	require.Same(t, pm1.state.metrics.eventsEmitted, pm2.state.metrics.eventsEmitted)
	require.Same(t, pm1.state.metrics.partitionsOwned, pm2.state.metrics.partitionsOwned)

	const ch = "shared-registry"
	require.NoError(t, pm2.AddPresence(ch, "client-1", &ClientInfo{}))
	require.Equal(t, 1.0, metricValue(t, pm1.state.metrics.eventsEmitted.WithLabelValues("occupied")))
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == 1
	}, 5*time.Second, 10*time.Millisecond)
}

// luaNonEmptyLines returns the trimmed non-empty lines of a script.
func luaNonEmptyLines(script string) []string {
	var lines []string
	for _, line := range strings.Split(script, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}

// luaScriptBody returns the non-empty lines of a script after its header, which ends with the last
// "-- ARGV[" line.
func luaScriptBody(source string) []string {
	lines := strings.Split(source, "\n")
	start := 0
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "-- ARGV[") {
			start = i + 1
		}
	}
	return luaNonEmptyLines(strings.Join(lines[start:], "\n"))
}

// containsLuaLines reports whether the needle lines are a run of consecutive non-empty lines of the script.
func containsLuaLines(script string, needle []string) bool {
	lines := luaNonEmptyLines(script)
	for i := 0; i+len(needle) <= len(lines); i++ {
		if slices.Equal(lines[i:i+len(needle)], needle) {
			return true
		}
	}
	return false
}

// luaExpiredCleanup returns the expired entries cleanup of a script: the lines from its local expired
// statement to the end of the if that removes the entries. The expiry argument is written as ARGV[now], so
// that the copies of the cleanup in other scripts compare equal.
func luaExpiredCleanup(source string, expiryArg string) []string {
	var block []string
	depth := 0
	inside := false
	for _, line := range luaNonEmptyLines(source) {
		if !inside {
			if !strings.HasPrefix(line, `local expired = redis.call("zrangebyscore"`) {
				continue
			}
			inside = true
		}
		block = append(block, strings.ReplaceAll(line, expiryArg, "ARGV[now]"))
		switch {
		case strings.HasSuffix(line, " then"), strings.HasSuffix(line, " do"):
			depth++
		case line == "end":
			depth--
		}
		if depth == 0 && line == "end" {
			break
		}
	}
	return block
}

// TestRedisChannelStateScriptsKeepPresenceBodies fails when the upstream presence scripts change and the
// channel state scripts keep the old body: the state scripts embed the bodies of the upstream scripts, and
// the cleanup of the expired entries of presence_get.lua.
func TestRedisChannelStateScriptsKeepPresenceBodies(t *testing.T) {
	require.True(t, containsLuaLines(presenceStateAddScriptSource, luaScriptBody(addPresenceScriptSource)),
		"presence_state_add.lua does not contain the body of presence_add.lua")
	require.True(t, containsLuaLines(presenceStateRemScriptSource, luaScriptBody(remPresenceScriptSource)),
		"presence_state_rem.lua does not contain the body of presence_rem.lua")

	cleanup := luaExpiredCleanup(presenceScriptSource, "ARGV[1]")
	require.NotEmpty(t, cleanup, "presence_get.lua has no cleanup of the expired entries")
	require.Equal(t, cleanup, luaExpiredCleanup(presenceStateCheckScriptSource, "ARGV[3]"),
		"presence_state_check.lua does not keep the cleanup of presence_get.lua")
	require.Equal(t, cleanup, luaExpiredCleanup(presenceStateRemScriptSource, "ARGV[6]"),
		"presence_state_rem.lua does not keep the cleanup of presence_get.lua")
}

func TestRedisPresenceManagerStateOffNoWorker(t *testing.T) {
	node := testStateNode(t)
	s, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(t, err)
	defer s.Close()

	// Other tests run their workers in parallel, so the worker goroutines are counted, not all goroutines.
	before := countChannelStateGoroutines()
	pm, err := NewRedisPresenceManager(node, RedisPresenceManagerConfig{
		Prefix: getUniqueStatePrefix(),
		Shards: []*RedisShard{s},
	})
	require.NoError(t, err)
	require.LessOrEqual(t, countChannelStateGoroutines(), before)
	require.Nil(t, pm.state)
	require.NoError(t, pm.Close(context.Background()))
}

func TestRedisChannelStateCluster(t *testing.T) {
	t.Parallel()
	collector := &testChannelStateCollector{}
	node := testStateNode(t)
	s, err := NewRedisShard(node, RedisShardConfig{
		ClusterAddresses: []string{"localhost:7001", "localhost:7002", "localhost:7003"},
		IOTimeout:        10 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(s.Close)
	pm := newTestRedisPresenceStateManagerWithShards(t, node, getUniqueStatePrefix(), testChannelStateConfig(collector), 0, false, []*RedisShard{s})
	waitForChannelStatePartitions(t, pm, 0)

	channels := []string{"cluster-a", "cluster-b", "cluster-c", "cluster-d", "cluster-e"}
	for _, ch := range channels {
		require.NoError(t, pm.AddPresence(ch, "client-1", &ClientInfo{}))
	}
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateOccupied)) == len(channels)
	}, 5*time.Second, 10*time.Millisecond)
	for _, ch := range channels {
		require.NoError(t, pm.RemovePresence(ch, "client-1", ""))
	}
	require.Eventually(t, func() bool {
		return len(eventsOfType(collector.snapshot(), ChannelStateVacated)) == len(channels)
	}, 5*time.Second, 10*time.Millisecond)
}

// BenchmarkRedisPresenceStateAdd measures AddPresence of a channel with channel state enabled.
func BenchmarkRedisPresenceStateAdd(b *testing.B) {
	collector := &testChannelStateCollector{}
	node := benchNode(b)
	pm := newTestRedisPresenceStateManager(b, node, getUniqueStatePrefix(), testChannelStateConfig(collector), 0, false, 6379)
	b.SetParallelism(getBenchParallelism())
	_ = pm.AddPresence("channel", "uid", &ClientInfo{})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := pm.AddPresence("channel", "uid", &ClientInfo{})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRedisPresenceStateRemove measures RemovePresence of a channel with channel state enabled.
func BenchmarkRedisPresenceStateRemove(b *testing.B) {
	collector := &testChannelStateCollector{}
	node := benchNode(b)
	pm := newTestRedisPresenceStateManager(b, node, getUniqueStatePrefix(), testChannelStateConfig(collector), 0, false, 6379)
	b.SetParallelism(getBenchParallelism())
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			uid := "uid" + strconv.Itoa(i)
			i++
			_ = pm.AddPresence("channel", uid, &ClientInfo{})
			err := pm.RemovePresence("channel", uid, "")
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
