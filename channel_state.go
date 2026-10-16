package centrifuge

import (
	"context"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
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
	// validUntil is the wall clock time in Unix nanoseconds until which the lease is known
	// to be held: the start of the last renewal (or of the acquisition) plus LeaseTTL. The
	// wall clock keeps running while the process is suspended, so a resumed consumer sees
	// an expired lease before it delivers anything. A backward step of the wall clock (NTP)
	// works the other way: Redis expires the lease on its own clock, but this node still
	// believes it is held until the step has passed. The node may then deliver the partition
	// for about the size of the step while another node holds it. Delivery is at least once,
	// so those events may be delivered twice.
	validUntil atomic.Int64
}

// leaseHeld reports whether the lease is known to be held at this moment.
func (p *channelStatePartition) leaseHeld() bool {
	return time.Now().UnixNano() < p.validUntil.Load()
}

// extendLease records that the lease was taken or renewed at since.
func (p *channelStatePartition) extendLease(since time.Time, leaseTTL time.Duration) {
	p.validUntil.Store(since.Add(leaseTTL).UnixNano())
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
			w.leave()
			return
		case <-ticker.C:
			w.leaseCycle()
		}
	}
}

// leaseCycle renews the owned leases, publishes this worker, and rebalances the partitions by the
// number of workers the heartbeat counts.
func (w *channelStateWorker) leaseCycle() {
	ctx, cancel := context.WithTimeout(context.Background(), w.state.leaseTTL)
	defer cancel()

	w.renewOwned(ctx)

	workers, err := w.heartbeat(ctx)
	if err != nil {
		// Without the count the share is unknown, so the partitions are kept until the next cycle.
		w.state.metrics.incSweepErrors()
		w.node.logger.log(newErrorLogEntry(err, "channel state worker heartbeat failed", nil))
	} else {
		w.rebalance(ctx, w.share(workers))
	}
	w.state.metrics.setPartitionsOwned(len(w.owned))
}

// rebalance gives up one partition when the node owns more than its share, and takes free partitions
// while it owns less.
func (w *channelStateWorker) rebalance(ctx context.Context, share int) {
	if len(w.owned) > share {
		for p, part := range w.owned {
			w.releasePartition(p, part)
			break
		}
	} else if len(w.owned) < share {
		w.acquire(ctx, share-len(w.owned))
	}
}

// renewOwned renews the leases of the owned partitions, one pipeline per shard, so that the
// renewals of a node take one round trip however many partitions it owns. A partition whose
// lease is not renewed is stopped: another node may own it now. A partition whose renewal
// failed is stopped and its lease released, because the renewal may have reached Redis.
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
		start := time.Now()
		results := w.state.renewScript.ExecMulti(ctx, sh.client, execs...)
		var failed []int
		for i, res := range results {
			p := partitions[i]
			renewed, err := res.AsInt64()
			switch {
			case err != nil:
				w.state.metrics.incSweepErrors()
				w.logError(err, "channel state lease renew failed", p)
				failed = append(failed, p)
			case renewed != 1:
				lost = append(lost, p)
			default:
				w.owned[p].extendLease(start, w.state.leaseTTL)
			}
		}
		for _, p := range failed {
			w.stopPartition(p, w.owned[p])
		}
		w.releaseLeases(sh, failed)
	}
	for _, p := range lost {
		w.stopPartition(p, w.owned[p])
	}
}

// releaseAll stops every owned partition and releases its lease. It runs once, on stop.
func (w *channelStateWorker) releaseAll() {
	partitions := make([]int, 0, len(w.owned))
	for p, part := range w.owned {
		w.stopPartition(p, part)
		partitions = append(partitions, p)
	}
	w.releasePartitions(partitions)
	w.state.metrics.setPartitionsOwned(len(w.owned))
}

// share is the number of partitions this node takes: an even split by the live workers of the
// prefix, as counted by the heartbeat. The count is a heuristic for fairness only, the lease is
// the guard. A worker that crashed drops out of the count LeaseTTL after its last heartbeat.
func (w *channelStateWorker) share(workers int) int {
	if workers < 1 {
		workers = 1
	}
	return (w.state.numPartitions + workers - 1) / workers
}

// heartbeat publishes this worker in the set of workers of the prefix, removes the entries that
// have expired, and returns how many workers the set holds, this one included. An entry expires
// LeaseTTL after its heartbeat unless the next heartbeat renews it. The set lives on the first
// shard, so all nodes count the same set. The set has no TTL of its own: every heartbeat trims it.
func (w *channelStateWorker) heartbeat(ctx context.Context) (int, error) {
	sh := w.state.workersShard
	key := w.state.workersKey
	nowMs := time.Now().UnixMilli()
	nowStr := strconv.FormatInt(nowMs, 10)
	expireAt := float64(nowMs + w.state.leaseTTL.Milliseconds())
	results := sh.client.DoMulti(
		ctx,
		sh.client.B().Zadd().Key(key).ScoreMember().ScoreMember(expireAt, w.node.uid).Build(),
		sh.client.B().Zremrangebyscore().Key(key).Min("-inf").Max(nowStr).Build(),
		sh.client.B().Zcount().Key(key).Min(nowStr).Max("+inf").Build(),
	)
	for _, res := range results {
		if err := res.Error(); err != nil {
			return 0, err
		}
	}
	count, err := results[2].AsInt64()
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

// leave removes this worker from the set of workers, so the other workers take its partitions
// without waiting for its entry to expire. It runs when the worker stops.
func (w *channelStateWorker) leave() {
	ctx, cancel := context.WithTimeout(context.Background(), w.state.leaseTTL)
	defer cancel()

	sh := w.state.workersShard
	if err := sh.client.Do(ctx, sh.client.B().Zrem().Key(w.state.workersKey).Member(w.node.uid).Build()).Error(); err != nil {
		w.state.metrics.incSweepErrors()
		w.node.logger.log(newErrorLogEntry(err, "channel state worker leave failed", nil))
	}
}

// acquire takes up to need free partitions, in random order, and starts a consumer for each. Only as many
// partitions are attempted as the node needs, so it takes no lease beyond its share. The lease attempts of
// a shard go out in one pipeline. A lease attempt whose reply failed may have taken the lease, so those
// leases are released too.
func (w *channelStateWorker) acquire(ctx context.Context, need int) {
	free := make([]int, 0, w.state.numPartitions-len(w.owned))
	for _, p := range rand.Perm(w.state.numPartitions) { //nolint:gosec // Partition order is only a fairness hint; not security-sensitive.
		if _, ok := w.owned[p]; !ok {
			free = append(free, p)
		}
	}
	if len(free) > need {
		free = free[:need]
	}
	byShard := make(map[*RedisShard][]int)
	for _, p := range free {
		sh := w.state.shardOf(p)
		byShard[sh] = append(byShard[sh], p)
	}

	start := time.Now()
	var firstErr error
	var errPartition int
	for sh, partitions := range byShard {
		cmds := make([]rueidis.Completed, 0, len(partitions))
		for _, p := range partitions {
			cmds = append(cmds, sh.client.B().Set().Key(w.leaseKey(p)).Value(w.node.uid).Nx().PxMilliseconds(w.state.leaseTTL.Milliseconds()).Build())
		}
		var failed []int
		for i, res := range sh.client.DoMulti(ctx, cmds...) {
			err := res.Error()
			switch {
			case rueidis.IsRedisNil(err):
				// Another node holds the lease.
			case err != nil:
				w.state.metrics.incSweepErrors()
				if firstErr == nil {
					firstErr = err
					errPartition = partitions[i]
				}
				failed = append(failed, partitions[i])
			default:
				w.startPartition(partitions[i], start)
			}
		}
		w.releaseLeases(sh, failed)
	}
	if firstErr != nil {
		w.logError(firstErr, "channel state lease acquire failed", errPartition)
	}
}

// startPartition starts the consumer of a partition whose lease was taken at since.
func (w *channelStateWorker) startPartition(p int, since time.Time) {
	ctx, cancel := context.WithCancel(context.Background())
	part := &channelStatePartition{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	part.extendLease(since, w.state.leaseTTL)
	w.owned[p] = part
	go w.consume(ctx, p, part)
}

// stopPartition cancels the consumer of the partition and waits until it has stopped.
// The lease is not released: it is lost or it is released by the caller.
func (w *channelStateWorker) stopPartition(p int, part *channelStatePartition) {
	part.cancel()
	<-part.done
	delete(w.owned, p)
}

// releasePartition stops the consumer of the partition and then releases its lease. Delivery is
// at least once: a stalled owner may overlap with the node that takes the lease after it, until
// its own lease check stops its consumer.
func (w *channelStateWorker) releasePartition(p int, part *channelStatePartition) {
	w.stopPartition(p, part)
	w.releaseLeases(w.state.shardOf(p), []int{p})
}

// consume sweeps the partition and delivers its events on every tick until ctx is done. A tick
// that finds the lease expired does nothing: the events are delivered by the node that holds it.
func (w *channelStateWorker) consume(ctx context.Context, p int, part *channelStatePartition) {
	defer close(part.done)

	sh := w.state.shardOf(p)
	ticker := time.NewTicker(w.state.sweepInterval)
	defer ticker.Stop()

	var backoff channelStateBackoff
	for {
		if part.leaseHeld() && !w.consumeOnce(ctx, sh, p, part, &backoff) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// consumeOnce runs the sweep and delivers the stream of the partition until it is empty or the
// lease is lost. It returns false when ctx is done.
func (w *channelStateWorker) consumeOnce(ctx context.Context, sh *RedisShard, p int, part *channelStatePartition, backoff *channelStateBackoff) bool {
	if err := w.sweep(ctx, sh, p); err != nil {
		if ctx.Err() != nil {
			return false
		}
		w.state.metrics.incSweepErrors()
		w.logError(err, "channel state sweep failed", p)
		return w.wait(ctx, backoff)
	}
	backoff.reset()
	for {
		events, ids, err := w.readBatch(ctx, sh, p)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			w.state.metrics.incSweepErrors()
			w.logError(err, "channel state read failed", p)
			return w.wait(ctx, backoff)
		}
		if len(ids) == 0 {
			backoff.reset()
			return ctx.Err() == nil
		}
		if len(events) > 0 {
			if !part.leaseHeld() {
				// The lease expired while the batch was read: another node may deliver it now.
				return true
			}
			if err := w.state.handler(ctx, events); err != nil {
				if ctx.Err() != nil {
					// Stopped while the handler ran: the events stay in the stream.
					return false
				}
				// The events stay in the stream, so the same batch is delivered again.
				w.state.metrics.incDeliveryErrors()
				w.logError(err, "channel state delivery failed", p)
				if !w.wait(ctx, backoff) {
					return false
				}
				continue
			}
			w.state.metrics.addDelivered(len(events))
		}
		backoff.reset()
		if err := w.ack(ctx, sh, p, ids); err != nil {
			if ctx.Err() != nil {
				return false
			}
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
// partition, each channel once, and whether any of the pages was full.
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
	// A channel can be due in both sets. Its second check would arm a new vacated event after the
	// first one has sent it, so each channel is checked once.
	slices.Sort(channels)
	return slices.Compact(channels), full, nil
}

// readBatch reads up to BatchSize entries of the stream of the partition, oldest first. An entry
// whose time does not parse is skipped, but its id is returned so that it is acknowledged.
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
		ids = append(ids, entry.ID)
		ms, err := strconv.ParseInt(entry.FieldValues["ms"], 10, 64)
		if err != nil {
			w.logError(err, "channel state entry skipped: time does not parse", p)
			continue
		}
		events = append(events, ChannelStateEvent{
			Channel: entry.FieldValues["c"],
			Type:    ChannelStateEventType(entry.FieldValues["t"]),
			Time:    time.UnixMilli(ms),
		})
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

// releasePartitions releases the leases of the partitions, one pipeline per shard.
func (w *channelStateWorker) releasePartitions(partitions []int) {
	byShard := make(map[*RedisShard][]int)
	for _, p := range partitions {
		sh := w.state.shardOf(p)
		byShard[sh] = append(byShard[sh], p)
	}
	for sh, shardPartitions := range byShard {
		w.releaseLeases(sh, shardPartitions)
	}
}

// releaseLeases releases the leases of the partitions of one shard in one pipeline. The release
// runs on its own context, because the lease cycle context may be the reason of the failure.
// A lease that is not held by this node is left alone by the release script.
func (w *channelStateWorker) releaseLeases(sh *RedisShard, partitions []int) {
	if len(partitions) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.state.leaseTTL)
	defer cancel()

	execs := make([]rueidis.LuaExec, 0, len(partitions))
	for _, p := range partitions {
		execs = append(execs, rueidis.LuaExec{
			Keys: []string{w.leaseKey(p)},
			Args: []string{w.node.uid},
		})
	}
	for i, res := range w.state.releaseScript.ExecMulti(ctx, sh.client, execs...) {
		if err := res.Error(); err != nil {
			w.state.metrics.incSweepErrors()
			w.logError(err, "channel state lease release failed", partitions[i]) //nolint:gosec // ExecMulti returns one result per exec, one exec per partition.
		}
	}
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
