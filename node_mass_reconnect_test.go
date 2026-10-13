package centrifuge

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	massReconnectChannels    int           = 10
	massReconnectSubscribers int           = 200
	massReconnectPrePubs     int           = 3
	massReconnectOutagePubs  int           = 2
	massReconnectCheckDelay  time.Duration = time.Minute
	// massReconnectBrokerLatency must exceed the time a slow CI runner needs to connect a client, otherwise
	// recovery calls rarely overlap and the singleflight bound becomes timing dependent.
	massReconnectBrokerLatency time.Duration = 20 * time.Millisecond
	// massReconnectClockMargin is added on top of the check delay when advancing the injected clock.
	// Client positionCheckTime is recorded from wall clock time, so a margin is needed to make the
	// client-side check independent of whether the burst crossed a wall clock second boundary.
	massReconnectClockMargin time.Duration = 30 * time.Second
	// massReconnectTopCallsPerChannel bounds stream top requests during one position check round
	// when shared position sync is enabled: one per channel plus one confirmation round.
	massReconnectTopCallsPerChannel int64 = 2
)

// massReconnectCounts splits History calls of the Node by shape.
type massReconnectCounts struct {
	recovery  atomic.Int64
	streamTop atomic.Int64
	other     atomic.Int64
}

func (c *massReconnectCounts) reset() {
	c.recovery.Store(0)
	c.streamTop.Store(0)
	c.other.Store(0)
}

// massReconnectBroker wraps MemoryBroker and counts History calls. Subscribe and Publish pass through.
// Each History call sleeps for massReconnectBrokerLatency to emulate broker round trip, so that
// concurrent identical calls can overlap the way they do with a real broker.
type massReconnectBroker struct {
	*MemoryBroker
	counts *massReconnectCounts
}

func (b *massReconnectBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	switch {
	case opts.Filter.Since != nil:
		b.counts.recovery.Add(1)
	case opts.Filter.Limit == 0:
		b.counts.streamTop.Add(1)
	default:
		b.counts.other.Add(1)
	}
	time.Sleep(massReconnectBrokerLatency)
	return b.MemoryBroker.History(ch, opts)
}

type massReconnectMode struct {
	name               string
	singleFlight       bool
	sharedPositionSync bool
}

type massReconnectResult struct {
	mode           massReconnectMode
	recoveryBurst  int64
	streamTopBurst int64
	streamTopCheck int64
	otherCalls     int64
	recovered      int64
}

// runConcurrently calls fn for indexes [0, count) in separate goroutines and returns joined errors.
func runConcurrently(count int, fn func(i int) error) error {
	errs := make([]error, count)
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = fn(i)
		}(i)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// massReconnectConnect creates and connects a client without using require, so it is safe to call
// from goroutines.
func massReconnectConnect(node *Node, userID string) (*Client, error) {
	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	transport.setProtocolVersion(ProtocolVersion2)
	newCtx := SetCredentials(ctx, &Credentials{UserID: userID})
	client, err := newClient(newCtx, node, transport)
	if err != nil {
		return nil, err
	}
	rw := testReplyWriterWrapper()
	err = client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw)
	if err != nil {
		return nil, err
	}
	if len(rw.replies) == 0 || rw.replies[0].Error != nil {
		return nil, fmt.Errorf("connect failed: %v", rw.replies)
	}
	client.triggerConnect()
	client.scheduleOnConnectTimers()
	return client, nil
}

// massReconnectSubscribe subscribes client to ch, optionally recovering from pos.
func massReconnectSubscribe(client *Client, ch string, pos StreamPosition, withRecovery bool) (*protocol.SubscribeResult, error) {
	rw := testReplyWriterWrapper()
	req := &protocol.SubscribeRequest{Channel: ch}
	if withRecovery {
		req.Recover = true
		req.Offset = pos.Offset
		req.Epoch = pos.Epoch
	}
	err := client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	if err != nil {
		return nil, err
	}
	if len(rw.replies) == 0 || rw.replies[0].Error != nil {
		return nil, fmt.Errorf("subscribe to %s failed: %v", ch, rw.replies)
	}
	return rw.replies[0].Subscribe, nil
}

// runMassReconnectScenario runs: connect and subscribe all clients, publish, outage (all clients
// disconnect while more publications arrive), concurrent reconnect with recovery from the last seen
// position, then a periodic position check round for all clients after the check delay.
func runMassReconnectScenario(t *testing.T, mode massReconnectMode) massReconnectResult {
	t.Helper()
	start := time.Now()
	clock := newTestClock(start)
	config := Config{
		ClientChannelPositionCheckDelay: massReconnectCheckDelay,
		UseSingleFlight:                 mode.singleFlight,
		Metrics: MetricsConfig{
			RegistererGatherer: prometheus.NewRegistry(),
		},
	}
	if mode.sharedPositionSync {
		config.GetChannelMediumOptions = func(_ string) ChannelMediumOptions {
			return ChannelMediumOptions{SharedPositionSync: true}
		}
	}
	node, err := New(config)
	require.NoError(t, err)
	node.nowTimeGetter = clock.now

	counts := &massReconnectCounts{}
	memoryBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	node.SetBroker(&massReconnectBroker{MemoryBroker: memoryBroker, counts: counts})
	require.NoError(t, node.Run())
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	// lost counts unexpected disconnects and unsubscribes. It is only armed for the check round,
	// so that outage disconnects are not counted.
	var lost atomic.Int64
	var watching atomic.Bool
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnableRecovery: true, EnablePositioning: true}}, nil)
		})
		client.OnDisconnect(func(_ DisconnectEvent) {
			if watching.Load() {
				lost.Add(1)
			}
		})
		client.OnUnsubscribe(func(_ UnsubscribeEvent) {
			if watching.Load() {
				lost.Add(1)
			}
		})
	})

	channels := make([]string, massReconnectChannels)
	for i := range channels {
		channels[i] = "mass_reconnect_" + strconv.Itoa(i)
	}
	total := massReconnectChannels * massReconnectSubscribers
	channelOf := func(i int) string {
		return channels[i/massReconnectSubscribers]
	}
	userOf := func(i int) string {
		return strconv.Itoa(i)
	}

	// Connect and subscribe all clients.
	sessions := make([]*Client, total)
	require.NoError(t, runConcurrently(total, func(i int) error {
		client, err := massReconnectConnect(node, userOf(i))
		if err != nil {
			return err
		}
		sessions[i] = client
		_, err = massReconnectSubscribe(client, channelOf(i), StreamPosition{}, false)
		return err
	}))

	// Publications before the outage. Every client has seen up to the last one of them.
	lastPos := make(map[string]StreamPosition, len(channels))
	for _, ch := range channels {
		for j := 0; j < massReconnectPrePubs; j++ {
			res, err := node.Publish(ch, []byte(`{}`), WithHistory(100, time.Minute))
			require.NoError(t, err)
			lastPos[ch] = res.StreamPosition
		}
	}

	// Outage: all clients lose their connection, publications keep flowing.
	for _, client := range sessions {
		_ = client.close(DisconnectConnectionClosed)
	}
	for _, ch := range channels {
		for j := 0; j < massReconnectOutagePubs; j++ {
			_, err := node.Publish(ch, []byte(`{}`), WithHistory(100, time.Minute))
			require.NoError(t, err)
		}
	}
	require.Eventually(t, func() bool {
		for _, ch := range channels {
			if node.hub.NumSubscribers(ch) != 0 {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)

	// Mass reconnect: all clients reconnect and resubscribe concurrently with recovery.
	counts.reset()
	var recovered atomic.Int64
	require.NoError(t, runConcurrently(total, func(i int) error {
		client, err := massReconnectConnect(node, userOf(i))
		if err != nil {
			return err
		}
		ch := channelOf(i)
		res, err := massReconnectSubscribe(client, ch, lastPos[ch], true)
		if err != nil {
			return err
		}
		sessions[i] = client
		if res.Recovered && len(res.Publications) == massReconnectOutagePubs {
			recovered.Add(1)
		}
		return nil
	}))
	recoveryBurst := counts.recovery.Load()
	streamTopBurst := counts.streamTop.Load()
	otherBurst := counts.other.Load()

	// Periodic position check: advance time past the check delay and run presence update for
	// every client, which runs the position check for each of its channels.
	counts.reset()
	clock.set(start.Add(massReconnectCheckDelay + massReconnectClockMargin))
	watching.Store(true)
	var wg sync.WaitGroup
	wg.Add(total)
	for _, client := range sessions {
		go func(client *Client) {
			defer wg.Done()
			client.updatePresence()
		}(client)
	}
	wg.Wait()
	streamTopCheck := counts.streamTop.Load()
	otherCheck := counts.other.Load()

	// Insufficient state may be handled asynchronously, so give it a chance to show up.
	require.Never(t, func() bool {
		return lost.Load() > 0
	}, 200*time.Millisecond, 20*time.Millisecond)
	for i, client := range sessions {
		require.True(t, client.IsSubscribed(channelOf(i)), "client %d lost subscription", i)
	}
	require.Equal(t, int64(total), recovered.Load(), "all clients must recover missed publications")
	require.Zero(t, lost.Load(), "no client may be disconnected or unsubscribed in the check round")

	result := massReconnectResult{
		mode:           mode,
		recoveryBurst:  recoveryBurst,
		streamTopBurst: streamTopBurst,
		streamTopCheck: streamTopCheck,
		otherCalls:     otherBurst + otherCheck,
		recovered:      recovered.Load(),
	}

	if !mode.singleFlight && !mode.sharedPositionSync {
		// Baseline: every client recovers and checks its position on its own.
		require.Equal(t, int64(total), recoveryBurst)
		require.Equal(t, int64(total), streamTopCheck)
	}
	if mode.singleFlight {
		// Identical concurrent recovery calls are merged. Merging depends on overlap timing, so the bound
		// is a 10x reduction from the baseline (one call per client) rather than an exact count.
		require.Less(t, recoveryBurst, int64(total)/10)
	}
	if mode.sharedPositionSync {
		// One stream top request per channel per check interval.
		require.LessOrEqual(t, streamTopCheck, int64(massReconnectChannels)*massReconnectTopCallsPerChannel)
	}
	return result
}

func massReconnectTable(results []massReconnectResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-22s %8s %9s %14s %16s %17s %8s %10s\n",
		"mode", "clients", "channels", "recovery(burst)", "top(burst)", "top(pos check)", "other", "recovered")
	for _, r := range results {
		fmt.Fprintf(&b, "%-22s %8d %9d %14d %16d %17d %8d %10d\n",
			r.mode.name,
			massReconnectChannels*massReconnectSubscribers,
			massReconnectChannels,
			r.recoveryBurst,
			r.streamTopBurst,
			r.streamTopCheck,
			r.otherCalls,
			r.recovered,
		)
	}
	return b.String()
}

func TestMassReconnectAfterOutage(t *testing.T) {
	modes := []massReconnectMode{
		{name: "baseline"},
		{name: "singleflight", singleFlight: true},
		{name: "shared position sync", sharedPositionSync: true},
		{name: "both", singleFlight: true, sharedPositionSync: true},
	}
	results := make([]massReconnectResult, len(modes))
	for i, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			results[i] = runMassReconnectScenario(t, mode)
		})
	}
	t.Logf("\n%s", massReconnectTable(results))
}
