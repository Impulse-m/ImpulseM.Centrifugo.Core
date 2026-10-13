package centrifuge

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const singleFlightConcurrentCalls = 50

var errSingleFlightBoom = errors.New("boom")

type singleFlightBlockingPresenceManager struct {
	*TestPresenceManager
	releaseCh          chan struct{}
	presenceCalls      int32
	presenceStatsCalls int32
}

func (m *singleFlightBlockingPresenceManager) Presence(_ string) (map[string]*ClientInfo, error) {
	atomic.AddInt32(&m.presenceCalls, 1)
	<-m.releaseCh
	return map[string]*ClientInfo{}, nil
}

func (m *singleFlightBlockingPresenceManager) PresenceStats(_ string) (PresenceStats, error) {
	atomic.AddInt32(&m.presenceStatsCalls, 1)
	<-m.releaseCh
	return PresenceStats{NumClients: 1}, nil
}

func newSingleFlightTestNode(t *testing.T) *Node {
	t.Helper()
	node, err := New(Config{
		UseSingleFlight: true,
		Metrics: MetricsConfig{
			RegistererGatherer: prometheus.NewRegistry(),
		},
	})
	require.NoError(t, err)
	require.NoError(t, node.Run())
	return node
}

// newBlockingHistoryBroker returns a broker whose History blocks until releaseCh is closed.
func newBlockingHistoryBroker(releaseCh chan struct{}, calls *int32, err error) *TestBroker {
	broker := NewTestBroker()
	broker.historyFunc = func(_ string, _ HistoryOptions) ([]*Publication, StreamPosition, error) {
		atomic.AddInt32(calls, 1)
		<-releaseCh
		if err != nil {
			return nil, StreamPosition{}, err
		}
		return []*Publication{{Offset: 1, Data: []byte("{}")}}, StreamPosition{Offset: 1, Epoch: "epoch"}, nil
	}
	return broker
}

// callConcurrently starts count goroutines calling fn, waits until all of them are
// running, gives them time to block on the same in-flight call, then calls release.
func callConcurrently(count int, release func(), fn func(i int)) {
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(count)
	done.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer done.Done()
			ready.Done()
			fn(i)
		}(i)
	}
	ready.Wait()
	time.Sleep(100 * time.Millisecond)
	release()
	done.Wait()
}

func TestSingleFlightHistoryMergesConcurrentCalls(t *testing.T) {
	releaseCh := make(chan struct{})
	var calls int32
	node := newSingleFlightTestNode(t)
	node.SetBroker(newBlockingHistoryBroker(releaseCh, &calls, nil))
	defer func() { _ = node.Shutdown(t.Context()) }()

	results := make([]HistoryResult, singleFlightConcurrentCalls)
	errs := make([]error, singleFlightConcurrentCalls)
	callConcurrently(singleFlightConcurrentCalls, func() { close(releaseCh) }, func(i int) {
		results[i], errs[i] = node.History("singleflight-history-merge", WithHistoryFilter(HistoryFilter{Limit: 10}))
	})

	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
	for i := 0; i < singleFlightConcurrentCalls; i++ {
		require.NoError(t, errs[i])
		require.Len(t, results[i].Publications, 1)
		require.Equal(t, results[0].Publications[0], results[i].Publications[0])
		require.Equal(t, results[0].StreamPosition, results[i].StreamPosition)
	}
	require.Greater(t, metricValue(t, node.metrics.singleflightMergedCount.WithLabelValues("history")), float64(0))
}

func TestSingleFlightHistoryDoesNotMergeDifferentOptions(t *testing.T) {
	releaseCh := make(chan struct{})
	var calls int32
	node := newSingleFlightTestNode(t)
	node.SetBroker(newBlockingHistoryBroker(releaseCh, &calls, nil))
	defer func() { _ = node.Shutdown(t.Context()) }()

	optionsByIndex := []HistoryOption{
		WithHistoryFilter(HistoryFilter{Limit: 1}),
		WithHistoryFilter(HistoryFilter{Limit: 2}),
		WithHistoryFilter(HistoryFilter{Limit: 1, Since: &StreamPosition{Offset: 1, Epoch: "epoch"}}),
	}
	callConcurrently(len(optionsByIndex), func() { close(releaseCh) }, func(i int) {
		_, _ = node.History("singleflight-history-options", optionsByIndex[i])
	})

	require.Equal(t, int32(len(optionsByIndex)), atomic.LoadInt32(&calls))
	require.Equal(t, float64(0), metricValue(t, node.metrics.singleflightMergedCount.WithLabelValues("history")))
}

func TestSingleFlightHistoryDeliversErrorToAllWaiters(t *testing.T) {
	releaseCh := make(chan struct{})
	var calls int32
	node := newSingleFlightTestNode(t)
	node.SetBroker(newBlockingHistoryBroker(releaseCh, &calls, errSingleFlightBoom))
	defer func() { _ = node.Shutdown(t.Context()) }()

	errs := make([]error, singleFlightConcurrentCalls)
	callConcurrently(singleFlightConcurrentCalls, func() { close(releaseCh) }, func(i int) {
		_, errs[i] = node.History("singleflight-history-error", WithHistoryFilter(HistoryFilter{Limit: 10}))
	})

	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
	for i := 0; i < singleFlightConcurrentCalls; i++ {
		require.ErrorIs(t, errs[i], errSingleFlightBoom)
	}
}

func TestSingleFlightPresenceMergesConcurrentCalls(t *testing.T) {
	releaseCh := make(chan struct{})
	pm := &singleFlightBlockingPresenceManager{
		TestPresenceManager: NewTestPresenceManager(),
		releaseCh:           releaseCh,
	}
	node := newSingleFlightTestNode(t)
	node.SetPresenceManager(pm)
	defer func() { _ = node.Shutdown(t.Context()) }()

	errs := make([]error, singleFlightConcurrentCalls)
	callConcurrently(singleFlightConcurrentCalls, func() { close(releaseCh) }, func(i int) {
		_, errs[i] = node.Presence("singleflight-presence")
	})

	require.Equal(t, int32(1), atomic.LoadInt32(&pm.presenceCalls))
	for i := 0; i < singleFlightConcurrentCalls; i++ {
		require.NoError(t, errs[i])
	}
	require.Greater(t, metricValue(t, node.metrics.singleflightMergedCount.WithLabelValues("presence")), float64(0))
}

func TestSingleFlightPresenceStatsMergesConcurrentCalls(t *testing.T) {
	releaseCh := make(chan struct{})
	pm := &singleFlightBlockingPresenceManager{
		TestPresenceManager: NewTestPresenceManager(),
		releaseCh:           releaseCh,
	}
	node := newSingleFlightTestNode(t)
	node.SetPresenceManager(pm)
	defer func() { _ = node.Shutdown(t.Context()) }()

	results := make([]PresenceStatsResult, singleFlightConcurrentCalls)
	errs := make([]error, singleFlightConcurrentCalls)
	callConcurrently(singleFlightConcurrentCalls, func() { close(releaseCh) }, func(i int) {
		results[i], errs[i] = node.PresenceStats("singleflight-presence-stats")
	})

	require.Equal(t, int32(1), atomic.LoadInt32(&pm.presenceStatsCalls))
	for i := 0; i < singleFlightConcurrentCalls; i++ {
		require.NoError(t, errs[i])
		require.Equal(t, 1, results[i].NumClients)
	}
	require.Greater(t, metricValue(t, node.metrics.singleflightMergedCount.WithLabelValues("presence_stats")), float64(0))
}

func TestSingleFlightNotSharedBetweenNodes(t *testing.T) {
	releaseCh := make(chan struct{})
	var callsA, callsB int32
	nodeA := newSingleFlightTestNode(t)
	nodeA.SetBroker(newBlockingHistoryBroker(releaseCh, &callsA, nil))
	defer func() { _ = nodeA.Shutdown(t.Context()) }()
	nodeB := newSingleFlightTestNode(t)
	nodeB.SetBroker(newBlockingHistoryBroker(releaseCh, &callsB, nil))
	defer func() { _ = nodeB.Shutdown(t.Context()) }()

	callConcurrently(singleFlightConcurrentCalls, func() { close(releaseCh) }, func(i int) {
		node := nodeA
		if i%2 == 1 {
			node = nodeB
		}
		_, err := node.History("singleflight-history-nodes", WithHistoryFilter(HistoryFilter{Limit: 10}))
		require.NoError(t, err)
	})

	require.Equal(t, int32(1), atomic.LoadInt32(&callsA))
	require.Equal(t, int32(1), atomic.LoadInt32(&callsB))
}
