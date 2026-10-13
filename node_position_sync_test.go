package centrifuge

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

const positionSyncTestCheckDelay = time.Minute

// metricValue returns the current value of a counter or a gauge, read the way upstream tests read metrics.
func metricValue(t testing.TB, metric prometheus.Metric) float64 {
	t.Helper()
	var pb dto.Metric
	require.NoError(t, metric.Write(&pb))
	if pb.GetCounter() != nil {
		return pb.GetCounter().GetValue()
	}
	return pb.GetGauge().GetValue()
}

// positionSyncTestStream is a stream top controlled by a test, served by TestBroker history.
type positionSyncTestStream struct {
	mu      sync.Mutex
	top     StreamPosition
	err     error
	calls   int32
	history func(_ string, _ HistoryOptions) ([]*Publication, StreamPosition, error)
}

func newPositionSyncTestStream(top StreamPosition) *positionSyncTestStream {
	s := &positionSyncTestStream{top: top}
	s.history = func(_ string, _ HistoryOptions) ([]*Publication, StreamPosition, error) {
		atomic.AddInt32(&s.calls, 1)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.err != nil {
			return nil, StreamPosition{}, s.err
		}
		return nil, s.top, nil
	}
	return s
}

func (s *positionSyncTestStream) setTop(top StreamPosition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.top = top
}

func (s *positionSyncTestStream) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *positionSyncTestStream) historyCalls() int32 {
	return atomic.LoadInt32(&s.calls)
}

// newPositionSyncTestNode returns a node with SharedPositionSync enabled for all channels. Channel medium
// clock is controlled by the returned setter so that position check intervals can be advanced in tests.
func newPositionSyncTestNode(t *testing.T, stream *positionSyncTestStream) (*Node, func(time.Time)) {
	t.Helper()
	clock := time.Now()
	node, err := New(Config{
		ClientChannelPositionCheckDelay: positionSyncTestCheckDelay,
		GetChannelMediumOptions: func(_ string) ChannelMediumOptions {
			return ChannelMediumOptions{SharedPositionSync: true}
		},
		Metrics: MetricsConfig{
			RegistererGatherer: prometheus.NewRegistry(),
		},
	})
	require.NoError(t, err)
	// Channel mediums take their clock from the node.
	node.nowTimeGetter = func() time.Time {
		return clock
	}
	broker := NewTestBroker()
	broker.historyFunc = stream.history
	node.SetBroker(broker)
	require.NoError(t, node.Run())
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnablePositioning: true}}, nil)
		})
	})
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})
	setClock := func(now time.Time) {
		clock = now
	}
	return node, setClock
}

// subscribePositionSyncTestClients connects and subscribes n positioned clients to ch.
func subscribePositionSyncTestClients(t *testing.T, node *Node, ch string, n int) []*Client {
	t.Helper()
	clients := make([]*Client, 0, n)
	for i := 0; i < n; i++ {
		client := newTestConnectedClientV2(t, node, strconv.Itoa(i))
		result := subscribeClientV2(t, client, ch)
		require.True(t, result.Positioned)
		clients = append(clients, client)
	}
	return clients
}

func positionSyncTestIsSubscribed(client *Client, ch string) bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	_, ok := client.channels[ch]
	return ok
}

func TestSharedPositionSyncOneStreamTopRequestPerInterval(t *testing.T) {
	stream := newPositionSyncTestStream(StreamPosition{Offset: 5, Epoch: "epoch"})
	node, setClock := newPositionSyncTestNode(t, stream)
	base := time.Now()
	setClock(base)

	const ch = "shared_check_channel"
	const numSubscribers = 20
	clients := subscribePositionSyncTestClients(t, node, ch, numSubscribers)
	require.Equal(t, float64(1), metricValue(t, node.metrics.channelMediumsGauge))

	clientPosition := StreamPosition{Offset: 5, Epoch: "epoch"}

	// First interval: every subscriber checks its position, but only one stream top request is made.
	setClock(base.Add(positionSyncTestCheckDelay + time.Second))
	before := stream.historyCalls()
	for _, client := range clients {
		valid, err := node.checkPosition(ch, clientPosition, 0, false, 0)
		require.NoError(t, err)
		require.True(t, valid)
		require.True(t, positionSyncTestIsSubscribed(client, ch))
	}
	require.Equal(t, int32(1), stream.historyCalls()-before)

	// Next interval: again one request for all subscribers.
	setClock(base.Add(2*positionSyncTestCheckDelay + 2*time.Second))
	before = stream.historyCalls()
	for range clients {
		valid, err := node.checkPosition(ch, clientPosition, 0, false, 0)
		require.NoError(t, err)
		require.True(t, valid)
	}
	require.Equal(t, int32(1), stream.historyCalls()-before)
}

func TestSharedPositionSyncSingleLaggingClientDoesNotAffectOthers(t *testing.T) {
	stream := newPositionSyncTestStream(StreamPosition{Offset: 5, Epoch: "epoch"})
	node, setClock := newPositionSyncTestNode(t, stream)
	base := time.Now()
	setClock(base)

	const ch = "lagging_client_channel"
	clients := subscribePositionSyncTestClients(t, node, ch, 3)
	lagging := clients[2]
	others := clients[:2]

	// Publication 6 reaches the medium and all subscribers, so the node is in sync at offset 6.
	stream.setTop(StreamPosition{Offset: 6, Epoch: "epoch"})
	require.NoError(t, node.HandlePublication(ch, &Publication{Offset: 6, Data: []byte("{}")}, StreamPosition{Offset: 6, Epoch: "epoch"}, false, nil))

	// Lagging client missed publication 6: its position is still 5.
	lagging.mu.Lock()
	chCtx := lagging.channels[ch]
	chCtx.streamPosition = StreamPosition{Offset: 5, Epoch: "epoch"}
	lagging.channels[ch] = chCtx
	lagging.mu.Unlock()

	setClock(base.Add(positionSyncTestCheckDelay + time.Second))
	valid, err := node.checkPosition(ch, StreamPosition{Offset: 5, Epoch: "epoch"}, 0, false, 0)
	require.NoError(t, err)
	require.False(t, valid)

	// Only the lagging client is marked. Subscribers at the stream top must stay subscribed.
	for _, other := range others {
		require.Never(t, func() bool {
			return !positionSyncTestIsSubscribed(other, ch)
		}, 200*time.Millisecond, 20*time.Millisecond)
	}
}

func TestSharedPositionSyncMediumCreateTeardownConcurrent(t *testing.T) {
	stream := newPositionSyncTestStream(StreamPosition{Offset: 5, Epoch: "epoch"})
	node, setClock := newPositionSyncTestNode(t, stream)
	setClock(time.Now())

	const ch = "concurrent_medium_channel"
	const numWorkers = 8
	const numIterations = 20

	clients := make([]*Client, numWorkers)
	for i := range clients {
		clients[i] = newTestClient(t, node, strconv.Itoa(i))
	}

	// Sample the gauge during the run: a channel must never have more than one medium.
	var maxMediums atomic.Int64
	stopSampling := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		for {
			select {
			case <-stopSampling:
				return
			default:
			}
			if v := int64(metricValue(t, node.metrics.channelMediumsGauge)); v > maxMediums.Load() {
				maxMediums.Store(v)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	errCh := make(chan error, numWorkers*numIterations*3)
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(client *Client) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				subGen := uint64(j + 1)
				_, err := node.addSubscription(ch, subInfo{client: client, subGen: subGen})
				if err != nil {
					errCh <- err
					return
				}
				// Publications and position checks race with medium creation and teardown.
				_ = node.HandlePublication(ch, &Publication{Offset: uint64(j + 1), Data: []byte("{}")}, StreamPosition{Offset: uint64(j + 1), Epoch: "epoch"}, false, nil)
				_, err = node.checkPosition(ch, StreamPosition{Offset: 5, Epoch: "epoch"}, 0, false, 0)
				if err != nil {
					errCh <- err
					return
				}
				err = node.removeSubscription(ch, client, subGen)
				if err != nil {
					errCh <- err
					return
				}
			}
		}(clients[i])
	}
	wg.Wait()
	close(stopSampling)
	<-samplerDone
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	require.LessOrEqual(t, maxMediums.Load(), int64(1))

	// Last subscriber left the channel, medium must be torn down once the dissolver runs.
	require.Eventually(t, func() bool {
		return metricValue(t, node.metrics.channelMediumsGauge) == 0
	}, 15*time.Second, 50*time.Millisecond)
}

func TestSharedPositionSyncBrokerErrorCountedAsError(t *testing.T) {
	stream := newPositionSyncTestStream(StreamPosition{Offset: 5, Epoch: "epoch"})
	node, setClock := newPositionSyncTestNode(t, stream)
	base := time.Now()
	setClock(base)

	const ch = "error_check_channel"
	subscribePositionSyncTestClients(t, node, ch, 1)

	stream.setErr(errSingleFlightBoom)
	setClock(base.Add(positionSyncTestCheckDelay + time.Second))
	valid, err := node.checkPosition(ch, StreamPosition{Offset: 5, Epoch: "epoch"}, 0, false, 0)
	require.ErrorIs(t, err, errSingleFlightBoom)
	require.False(t, valid)
	require.Equal(t, float64(1), metricValue(t, node.metrics.positionChecksCount.WithLabelValues("error", "true")))

	// The interval is still consumed: no broker request until the next interval.
	before := stream.historyCalls()
	valid, err = node.checkPosition(ch, StreamPosition{Offset: 5, Epoch: "epoch"}, 0, false, 0)
	require.NoError(t, err)
	require.True(t, valid)
	require.Equal(t, before, stream.historyCalls())
	require.Equal(t, float64(1), metricValue(t, node.metrics.positionChecksCount.WithLabelValues("error", "true")))
}

// TestNodeCheckPositionMapWithMediumTwoIntervalSemantics covers a map channel with shared position sync: a client
// ahead of the stream top with its check time before and after the top fetch, the first and the confirming
// sighting of a mismatched epoch, and lastPosition after the mismatch is confirmed.
func TestNodeCheckPositionMapWithMediumTwoIntervalSemantics(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:   MapModeRecoverable,
			KeyTTL: 60 * time.Second,
		}
	}
	node.config.GetChannelMediumOptions = func(channel string) ChannelMediumOptions {
		return ChannelMediumOptions{
			SharedPositionSync: true,
		}
	}
	node.config.ClientChannelPositionCheckDelay = 0

	mapBroker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	node.SetMapBroker(mapBroker)

	ctx := context.Background()
	channel := "test_map_medium_semantics"

	// Publish data to create stream position.
	result, err := mapBroker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"test": 1}`),
	})
	require.NoError(t, err)
	streamPos := result.Position

	// Create a subscription to trigger medium creation with isMap=true.
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:              SubscriptionTypeMap,
					EnablePositioning: true,
					HistoryMetaTTL:    200 * time.Second,
				},
			}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Correct position — valid.
	isValid, err := node.checkPosition(channel, streamPos, 200*time.Second, true, 0)
	require.NoError(t, err)
	require.True(t, isValid)

	// Client one position ahead of the top, checked in the same second as the fetch: valid.
	isValid, err = node.checkPosition(channel, StreamPosition{
		Offset: streamPos.Offset + 1,
		Epoch:  streamPos.Epoch,
	}, 200*time.Second, true, time.Now().Unix())
	require.NoError(t, err)
	require.True(t, isValid)

	// Same client, checked two seconds before the fetch: invalid.
	isValid, err = node.checkPosition(channel, StreamPosition{
		Offset: streamPos.Offset + 1,
		Epoch:  streamPos.Epoch,
	}, 200*time.Second, true, time.Now().Unix()-2)
	require.NoError(t, err)
	require.False(t, isValid)

	// Wrong epoch. The first sighting only marks the node suspect, so the client is valid.
	isValid, err = node.checkPosition(channel, StreamPosition{
		Offset: streamPos.Offset,
		Epoch:  "wrong",
	}, 200*time.Second, true, 0)
	require.NoError(t, err)
	require.True(t, isValid)

	// The mismatch persists on the second check: the client is invalid and insufficient state is broadcast.
	isValid, err = node.checkPosition(channel, StreamPosition{
		Offset: streamPos.Offset,
		Epoch:  "wrong",
	}, 200*time.Second, true, 0)
	require.NoError(t, err)
	require.False(t, isValid)

	// The confirmed mismatch moved the medium's lastPosition to the stream top.
	mediumMu := node.mediumLock(channel)
	mediumMu.Lock()
	medium := node.mediumShard(channel)[channel]
	mediumMu.Unlock()
	require.NotNil(t, medium)
	medium.mu.Lock()
	lastPosition := medium.position.lastPosition
	medium.mu.Unlock()
	require.Equal(t, streamPos, lastPosition)
}
