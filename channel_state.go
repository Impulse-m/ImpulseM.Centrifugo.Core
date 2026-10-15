package centrifuge

import (
	"context"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/redis/rueidis"
)

const (
	channelStateMinBackoff = 100 * time.Millisecond
	channelStateMaxBackoff = 20 * time.Second
)

// channelStateWorker holds the leases of the channel state partitions of a node and
// runs one consumer goroutine per partition the node owns.
type channelStateWorker struct {
	state     *presenceState
	node      *Node
	leaseTick time.Duration
	closeCh   chan struct{}
	closeOnce sync.Once
	stopped   chan struct{}
	// owned is only accessed by the lease loop goroutine.
	owned map[int]*channelStatePartition
}

// channelStatePartition is a partition the node owns: its consumer stops on cancel and
// closes done when it has stopped.
type channelStatePartition struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// channelStateBackoff doubles the delay between attempts of a failing step, from 100ms up to 20s.
type channelStateBackoff struct {
	delay time.Duration
}

func newChannelStateWorker(state *presenceState, node *Node) *channelStateWorker {
	return &channelStateWorker{
		state:     state,
		node:      node,
		leaseTick: state.leaseTTL / 5,
		closeCh:   make(chan struct{}),
		stopped:   make(chan struct{}),
		owned:     make(map[int]*channelStatePartition),
	}
}

// stop asks the worker to release its leases and stop. It does not wait.
func (w *channelStateWorker) stop() {
	w.closeOnce.Do(func() {
		close(w.closeCh)
	})
}

// run is the lease loop. It runs until stop is called.
func (w *channelStateWorker) run() {
	defer close(w.stopped)

	ticker := time.NewTicker(w.leaseTick)
	defer ticker.Stop()

	for {
		select {
		case <-w.closeCh:
			w.releaseAll()
			return
		case <-ticker.C:
			w.leaseCycle()
		}
	}
}

// leaseCycle renews the owned leases, gives up one partition when the node owns more
// than its share, and takes free partitions while it owns less.
func (w *channelStateWorker) leaseCycle() {
	ctx, cancel := context.WithTimeout(context.Background(), w.state.leaseTTL)
	defer cancel()

	w.renewOwned(ctx)

	share := w.share()
	if len(w.owned) > share {
		for p, part := range w.owned {
			w.releasePartition(ctx, p, part)
			break
		}
	} else if len(w.owned) < share {
		w.acquire(ctx, share)
	}
	w.state.metrics.setPartitionsOwned(len(w.owned))
}

// renewOwned renews the leases of the owned partitions, one pipeline per shard, so that the
// renewals of a node take one round trip however many partitions it owns. A partition whose
// lease is not renewed is stopped: another node may own it now.
func (w *channelStateWorker) renewOwned(ctx context.Context) {
	byShard := make(map[*RedisShard][]int)
	for p := range w.owned {
		sh := w.state.shardOf(p)
		byShard[sh] = append(byShard[sh], p)
	}

	ttl := strconv.FormatInt(w.state.leaseTTL.Milliseconds(), 10)
	var lost []int
	for sh, partitions := range byShard {
		execs := make([]rueidis.LuaExec, 0, len(partitions))
		for _, p := range partitions {
			execs = append(execs, rueidis.LuaExec{
				Keys: []string{w.leaseKey(p)},
				Args: []string{w.node.uid, ttl},
			})
		}
		results := w.state.renewScript.ExecMulti(ctx, sh.client, execs...)
		for i, res := range results {
			renewed, err := res.AsInt64()
			if err != nil {
				w.state.metrics.incSweepErrors()
				w.logError(err, "channel state lease renew failed", partitions[i])
			}
			if err != nil || renewed != 1 {
				lost = append(lost, partitions[i])
			}
		}
	}
	for _, p := range lost {
		w.stopPartition(p, w.owned[p])
	}
}

// releaseAll stops every owned partition and releases its lease. It runs once, on stop.
func (w *channelStateWorker) releaseAll() {
	ctx, cancel := context.WithTimeout(context.Background(), w.state.leaseTTL)
	defer cancel()

	for p, part := range w.owned {
		w.releasePartition(ctx, p, part)
	}
	w.state.metrics.setPartitionsOwned(len(w.owned))
}

// share is the number of partitions this node takes: an even split by the nodes known
// in the registry. The registry is a heuristic for fairness only, the lease is the guard.
func (w *channelStateWorker) share() int {
	nodes := len(w.node.nodes.list())
	if nodes < 1 {
		nodes = 1
	}
	return (w.state.numPartitions + nodes - 1) / nodes
}

// acquire takes free partitions until the node owns its share. The lease attempts of a shard
// go out in one pipeline, in random order, so a node takes its partitions in one round trip per
// shard. A lease taken beyond the share is released at once.
func (w *channelStateWorker) acquire(ctx context.Context, share int) {
	free := make(map[*RedisShard][]int)
	for _, p := range rand.Perm(w.state.numPartitions) { //nolint:gosec // Partition order is only a fairness hint; not security-sensitive.
		if _, ok := w.owned[p]; ok {
			continue
		}
		sh := w.state.shardOf(p)
		free[sh] = append(free[sh], p)
	}

	var taken []int
	var errPartition int
	var firstErr error
	for sh, partitions := range free {
		cmds := make([]rueidis.Completed, 0, len(partitions))
		for _, p := range partitions {
			cmds = append(cmds, sh.client.B().Set().Key(w.leaseKey(p)).Value(w.node.uid).Nx().PxMilliseconds(w.state.leaseTTL.Milliseconds()).Build())
		}
		for i, res := range sh.client.DoMulti(ctx, cmds...) {
			err := res.Error()
			if rueidis.IsRedisNil(err) {
				// Another node holds the lease.
				continue
			}
			if err != nil {
				w.state.metrics.incSweepErrors()
				if firstErr == nil {
					firstErr = err
					errPartition = partitions[i]
				}
				continue
			}
			taken = append(taken, partitions[i])
		}
	}
	if firstErr != nil {
		w.logError(firstErr, "channel state lease acquire failed", errPartition)
	}

	for _, p := range taken {
		if len(w.owned) >= share {
			w.releaseTaken(ctx, p)
			continue
		}
		w.startPartition(p)
	}
}

// releaseTaken gives back a lease the node took but does not need.
func (w *channelStateWorker) releaseTaken(ctx context.Context, p int) {
	if err := w.releaseLease(ctx, p); err != nil {
		w.state.metrics.incSweepErrors()
		w.logError(err, "channel state lease release failed", p)
	}
}

func (w *channelStateWorker) startPartition(p int) {
	ctx, cancel := context.WithCancel(context.Background())
	part := &channelStatePartition{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	w.owned[p] = part
	go w.consume(ctx, p, part.done)
}

// stopPartition cancels the consumer of the partition and waits until it has stopped.
// The lease is not released: it is lost or it is released by the caller.
func (w *channelStateWorker) stopPartition(p int, part *channelStatePartition) {
	part.cancel()
	<-part.done
	delete(w.owned, p)
}

// releasePartition stops the consumer of the partition, so no two nodes consume it at
// once, and then releases its lease.
func (w *channelStateWorker) releasePartition(ctx context.Context, p int, part *channelStatePartition) {
	w.stopPartition(p, part)
	if err := w.releaseLease(ctx, p); err != nil {
		w.state.metrics.incSweepErrors()
		w.logError(err, "channel state lease release failed", p)
	}
}

// consume sweeps the partition and delivers its events on every tick until ctx is done.
func (w *channelStateWorker) consume(ctx context.Context, p int, done chan struct{}) {
	defer close(done)

	sh := w.state.shardOf(p)
	ticker := time.NewTicker(w.state.sweepInterval)
	defer ticker.Stop()

	var backoff channelStateBackoff
	for {
		if !w.consumeOnce(ctx, sh, p, &backoff) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// consumeOnce runs the sweep and delivers the stream of the partition until it is empty.
// It returns false when ctx is done.
func (w *channelStateWorker) consumeOnce(ctx context.Context, sh *RedisShard, p int, backoff *channelStateBackoff) bool {
	if err := w.sweep(ctx, sh, p); err != nil {
		w.state.metrics.incSweepErrors()
		w.logError(err, "channel state sweep failed", p)
		return w.wait(ctx, backoff)
	}
	for {
		events, ids, err := w.readBatch(ctx, sh, p)
		if err != nil {
			w.state.metrics.incSweepErrors()
			w.logError(err, "channel state read failed", p)
			return w.wait(ctx, backoff)
		}
		if len(ids) == 0 {
			return ctx.Err() == nil
		}
		if err := w.state.handler(ctx, events); err != nil {
			// The events stay in the stream, so the same batch is delivered again.
			w.state.metrics.incDeliveryErrors()
			w.logError(err, "channel state delivery failed", p)
			if !w.wait(ctx, backoff) {
				return false
			}
			continue
		}
		w.state.metrics.addDelivered(len(events))
		backoff.reset()
		if err := w.ack(ctx, sh, p, ids); err != nil {
			// The batch is still in the stream: a repeated delivery is possible, by design.
			w.state.metrics.incSweepErrors()
			w.logError(err, "channel state ack failed", p)
			if !w.wait(ctx, backoff) {
				return false
			}
		}
	}
}

// sweep moves the channels that are due in the partition to their next state: a
// vacated event when the delay has passed, or an armed delay when the presence expired.
func (w *channelStateWorker) sweep(ctx context.Context, sh *RedisShard, p int) error {
	nowMs := time.Now().UnixMilli()
	for page := 0; page < channelStateSweepMaxPages; page++ {
		channels, full, err := w.duePage(ctx, sh, p, nowMs)
		if err != nil {
			return err
		}
		if len(channels) > 0 {
			if err := w.state.checkChannels(ctx, sh, p, channels, nowMs); err != nil {
				return err
			}
		}
		if !full {
			return nil
		}
	}
	return nil
}

// duePage returns one page of the channels due in the pending and expire sets of the
// partition, and whether any of the pages was full.
func (w *channelStateWorker) duePage(ctx context.Context, sh *RedisShard, p int, nowMs int64) ([]string, bool, error) {
	nowStr := strconv.FormatInt(nowMs, 10)
	pendingKey := string(w.state.partitionKey(p, "pending"))
	expireKey := string(w.state.partitionKey(p, "expire"))
	results := sh.client.DoMulti(
		ctx,
		sh.client.B().Zrangebyscore().Key(pendingKey).Min("-inf").Max(nowStr).Limit(0, channelStateSweepPageSize).Build(),
		sh.client.B().Zrangebyscore().Key(expireKey).Min("-inf").Max(nowStr).Limit(0, channelStateSweepPageSize).Build(),
	)
	pending, err := results[0].AsStrSlice()
	if err != nil {
		return nil, false, err
	}
	expired, err := results[1].AsStrSlice()
	if err != nil {
		return nil, false, err
	}
	full := len(pending) >= channelStateSweepPageSize || len(expired) >= channelStateSweepPageSize
	channels := make([]string, 0, len(pending)+len(expired))
	channels = append(channels, pending...)
	channels = append(channels, expired...)
	return channels, full, nil
}

// readBatch reads up to BatchSize events of the stream of the partition, oldest first.
func (w *channelStateWorker) readBatch(ctx context.Context, sh *RedisShard, p int) ([]ChannelStateEvent, []string, error) {
	stream := string(w.state.partitionKey(p, "stream"))
	entries, err := sh.client.Do(
		ctx,
		sh.client.B().Xrange().Key(stream).Start("-").End("+").Count(int64(w.state.batchSize)).Build(),
	).AsXRange()
	if err != nil {
		return nil, nil, err
	}
	events := make([]ChannelStateEvent, 0, len(entries))
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ms, err := strconv.ParseInt(entry.FieldValues["ms"], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		events = append(events, ChannelStateEvent{
			Channel: entry.FieldValues["c"],
			Type:    ChannelStateEventType(entry.FieldValues["t"]),
			Time:    time.UnixMilli(ms),
		})
		ids = append(ids, entry.ID)
	}
	return events, ids, nil
}

// ack removes the delivered events from the stream of the partition.
func (w *channelStateWorker) ack(ctx context.Context, sh *RedisShard, p int, ids []string) error {
	stream := string(w.state.partitionKey(p, "stream"))
	return sh.client.Do(ctx, sh.client.B().Xdel().Key(stream).Id(ids...).Build()).Error()
}

func (w *channelStateWorker) leaseKey(p int) string {
	return string(w.state.partitionKey(p, "lease"))
}

func (w *channelStateWorker) releaseLease(ctx context.Context, p int) error {
	sh := w.state.shardOf(p)
	_, err := w.state.releaseScript.Exec(
		ctx,
		sh.client,
		[]string{w.leaseKey(p)},
		[]string{w.node.uid},
	).AsInt64()
	return err
}

// wait sleeps the next backoff delay. It returns false when ctx is done first.
func (w *channelStateWorker) wait(ctx context.Context, backoff *channelStateBackoff) bool {
	timer := time.NewTimer(backoff.next())
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *channelStateWorker) logError(err error, message string, p int) {
	w.node.logger.log(newErrorLogEntry(err, message, map[string]any{"partition": p}))
}

// next returns the delay before the next attempt and doubles the one after it.
func (b *channelStateBackoff) next() time.Duration {
	if b.delay == 0 {
		b.delay = channelStateMinBackoff
	} else {
		b.delay *= 2
		if b.delay > channelStateMaxBackoff {
			b.delay = channelStateMaxBackoff
		}
	}
	return b.delay
}

func (b *channelStateBackoff) reset() {
	b.delay = 0
}
