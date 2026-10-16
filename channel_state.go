package centrifuge

import (
	"context"
	"errors"
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
	// channelStateLeaseRenewals is how many times a lease is renewed within its TTL.
	channelStateLeaseRenewals = 5
	// channelStateCallDivisor sets the timeout of a Redis call of the lease loop: a third of LeaseTTL.
	channelStateCallDivisor = 3
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
	// heartbeats tracks the heartbeat calls that may still reach Redis, so that the membership
	// is removed after the last of them.
	heartbeats sync.WaitGroup
	// background tracks the work beside the lease loop: the consumers, the retirement of partitions and
	// the release of the leases of an acquisition that the lease loop gave up. Shutdown waits for it, and
	// Close bounds that wait with its context.
	background sync.WaitGroup
	// owned is only accessed by the lease loop goroutine.
	owned map[int]*channelStatePartition
	// retiring holds the partitions whose consumers are stopping. They are not free until their
	// consumers have stopped, so two consumers never run one partition. Only the lease loop goroutine
	// accesses it.
	retiring map[int]*channelStatePartition
}

// channelStatePartition is a partition the node holds a lease on, and the consumer of the partition.
// The token is the value the acquisition wrote to the lease key, so a renewal or a release touches the
// lease only while it is still the one of that acquisition. The consumer stops on cancel and closes
// done when it has stopped.
type channelStatePartition struct {
	p      int
	shard  *RedisShard
	token  string
	cancel context.CancelFunc
	done   chan struct{}
	// release says whether the lease is released once the consumer has stopped. It is set when the
	// partition is given up, and the lease loop goroutine reads it.
	release bool
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

// stop cancels the consumer of the partition and waits until it has stopped. It does not change the owned set.
func (p *channelStatePartition) stop() {
	p.cancel()
	<-p.done
}

// channelStateBackoff doubles the delay between attempts of a failing step, from 100ms up to 20s.
type channelStateBackoff struct {
	delay time.Duration
}

func newChannelStateWorker(state *presenceState, node *Node) *channelStateWorker {
	return &channelStateWorker{
		state:     state,
		node:      node,
		leaseTick: state.leaseTTL / channelStateLeaseRenewals,
		closeCh:   make(chan struct{}),
		stopped:   make(chan struct{}),
		owned:     make(map[int]*channelStatePartition),
		retiring:  make(map[int]*channelStatePartition),
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
			w.shutdown()
			return
		case <-ticker.C:
			w.leaseCycle()
		}
	}
}

// leaseCycle renews the owned leases, publishes this worker, and rebalances the partitions by the number of
// workers the heartbeat counts. The renewals and the heartbeat run together, and the cycle waits for their
// replies for at most callTimeout. A shard that does not answer in time is given up, so it cannot delay the
// renewals of the others. The acquisitions wait for at most callTimeout too: a cycle lasts at most two call
// timeouts, two thirds of LeaseTTL at the most, however long a shard takes to answer.
func (w *channelStateWorker) leaseCycle() {
	ctx, cancel := w.callContext()
	defer cancel()
	w.heartbeats.Add(1)
	heartbeat := startCall(func() (int, error) {
		defer w.heartbeats.Done()
		count, err := w.heartbeat()
		select {
		case <-w.closeCh:
			// The worker closed while the heartbeat was in flight, so its entry may have been written after leave
			// removed it. Leaving again removes the entry, since this call ran after its write.
			w.leave()
		default:
		}
		return count, err
	})

	w.renewOwned(ctx.Done())
	hb, _ := awaitCall(heartbeat, ctx.Done())

	if hb.err != nil {
		// Without the count the share is unknown. The worker keeps the partitions it owns and takes the free
		// ones, so the partitions of a crashed worker are still consumed on the healthy shards. The leases keep
		// one consumer per partition. The worker may hold more than its share until the count is known again.
		w.state.metrics.incSweepErrors()
		w.node.logger.log(newErrorLogEntry(hb.err, "channel state worker heartbeat failed", nil))
		w.rebalance(w.state.numPartitions)
	} else {
		w.rebalance(w.share(hb.value))
	}
	w.state.metrics.setPartitionsOwned(len(w.owned))
}

// callTimeout is how long one Redis call of the lease loop may take: a third of LeaseTTL. A shard that does not
// answer within it is given up for the cycle, so the other shards keep their leases.
func (w *channelStateWorker) callTimeout() time.Duration {
	return w.state.leaseTTL / channelStateCallDivisor
}

// callContext returns the context of one Redis call of the lease loop, which gives up after callTimeout.
func (w *channelStateWorker) callContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), w.callTimeout())
}

// errCallTimeout is the outcome of a call of the lease loop that did not return in time.
var errCallTimeout = errors.New("channel state call did not return in time")

// callResult is the outcome of a call of the lease loop that runs on its own goroutine.
type callResult[T any] struct {
	value T
	err   error
}

// startCall runs fn on its own goroutine and returns the channel of its outcome. The channel is buffered, so
// a call that returns after the lease loop stopped waiting for it does not block.
func startCall[T any](fn func() (T, error)) <-chan callResult[T] {
	ch := make(chan callResult[T], 1)
	go func() {
		value, err := fn()
		ch <- callResult[T]{value: value, err: err}
	}()
	return ch
}

// awaitCall waits for the outcome of a call until expired is closed. A result that is already there wins over
// the expiry. ok is false when the call did not return in time: rueidis queues a call behind a connection that
// is being dialed, and that wait does not end with the context of the call, so the lease loop does not wait for
// a call without a bound of its own.
func awaitCall[T any](ch <-chan callResult[T], expired <-chan struct{}) (res callResult[T], ok bool) {
	select {
	case res = <-ch:
		return res, true
	default:
	}
	select {
	case res = <-ch:
		return res, true
	case <-expired:
		select {
		case res = <-ch:
			return res, true
		default:
			return callResult[T]{err: errCallTimeout}, false
		}
	}
}

// waitWithin waits until the goroutines of wg are done or d has passed. A call whose connection is being dialed
// does not end with its context, so the waits of the shutdown are bounded here. A wait that gives up leaves its
// goroutine behind until the work of wg is done.
func waitWithin(wg *sync.WaitGroup, d time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}

// rebalance gives up one partition when the node owns more than its share, and takes free partitions
// while it owns less.
func (w *channelStateWorker) rebalance(share int) {
	if len(w.owned) > share {
		for _, part := range w.owned {
			w.giveUp([]*channelStatePartition{part}, true)
			break
		}
	} else if len(w.owned) < share {
		w.acquire(share - len(w.owned))
	}
}

// renewResult is the outcome of the renewals of one shard.
type renewResult struct {
	// lost are the partitions whose lease another owner holds, or whose lease has expired.
	lost []*channelStatePartition
	// failed are the partitions whose renewal failed.
	failed []*channelStatePartition
}

// renewOwned renews the leases of the owned partitions, one pipeline per shard. The shards run in parallel, and
// the loop waits for each of them until expired: a shard that does not answer in time is given up, so it does
// not delay the renewals of the others. A partition whose lease is lost is given up without a release, because
// another owner may hold it now. A partition whose renewal failed, or whose shard was given up, is given up and
// its lease released after its consumer has stopped, because the renewal may have reached Redis.
func (w *channelStateWorker) renewOwned(expired <-chan struct{}) {
	owned := make([]*channelStatePartition, 0, len(w.owned))
	for _, part := range w.owned {
		owned = append(owned, part)
	}
	ttl := strconv.FormatInt(w.state.leaseTTL.Milliseconds(), 10)

	type renewal struct {
		parts []*channelStatePartition
		ch    <-chan callResult[renewResult]
	}
	byShard := groupByShard(owned, partitionShard)
	renewals := make([]renewal, 0, len(byShard))
	for sh, parts := range byShard {
		renewals = append(renewals, renewal{
			parts: parts,
			ch: startCall(func() (renewResult, error) {
				return w.renewShard(sh, parts, ttl), nil
			}),
		})
	}
	for _, r := range renewals {
		res, ok := awaitCall(r.ch, expired)
		result := res.value
		if !ok {
			// The renewals of the shard may still reach Redis, so its partitions are released as well.
			result = renewResult{failed: r.parts}
		}
		w.giveUp(result.lost, false)
		w.giveUp(result.failed, true)
	}
}

// renewShard renews the leases of the partitions of one shard in one pipeline and returns the outcome of each.
// It does not touch the owned set, so it may run beside the renewals of other shards.
func (w *channelStateWorker) renewShard(sh *RedisShard, parts []*channelStatePartition, ttl string) renewResult {
	ctx, cancel := w.callContext()
	defer cancel()

	execs := make([]rueidis.LuaExec, 0, len(parts))
	for _, part := range parts {
		execs = append(execs, rueidis.LuaExec{
			Keys: []string{w.leaseKey(part.p)},
			Args: []string{part.token, ttl},
		})
	}
	start := time.Now()
	var res renewResult
	var errs callErrors
	for i, reply := range w.state.renewScript.ExecMulti(ctx, sh.client, execs...) {
		part := parts[i]
		renewed, err := reply.AsInt64()
		switch {
		case err != nil:
			errs.add(err, part.p)
			res.failed = append(res.failed, part)
		case renewed != 1:
			res.lost = append(res.lost, part)
		default:
			part.extendLease(start, w.state.leaseTTL)
		}
	}
	w.reportCallErrors("channel state lease renew failed", errs)
	return res
}

// giveUp removes the partitions from the owned set and retires them. release says whether their leases are
// released once their consumers have stopped.
func (w *channelStateWorker) giveUp(parts []*channelStatePartition, release bool) {
	if len(parts) == 0 {
		return
	}
	for _, part := range parts {
		delete(w.owned, part.p)
	}
	w.retire(parts, release)
}

// retire stops the consumers of the partitions in the background and, when release is set, releases their
// leases after the consumers have stopped. The partitions stay in the retiring set until their consumers have
// stopped, so acquire does not start a second consumer for them. The lease loop never waits for a consumer: a
// consumer may be stuck in a Redis call that ignores its context, such as the connection setup of rueidis to a
// shard that does not answer. A renewal that was given up may still reach Redis after the release, so the lease
// can outlive its consumer by up to LeaseTTL.
func (w *channelStateWorker) retire(parts []*channelStatePartition, release bool) {
	for _, part := range parts {
		part.release = release
		w.retiring[part.p] = part
	}
	w.background.Add(1)
	go func() {
		defer w.background.Done()
		for _, part := range parts {
			part.stop()
		}
		if release {
			w.releaseParts(parts)
		}
	}()
}

// shutdown stops the worker when Close is called. It cancels the consumers and releases the leases of the owned
// partitions, and of the retiring ones whose release is pending, before it waits for any consumer, so a consumer
// stuck in a call that ignores its context does not hold the leases of the others. The releases and the membership
// removal are bounded by the call timeout. The wait for the consumers and the background work comes last and has
// no bound of its own: Close bounds it with its context. A consumer that runs past the release of its lease may
// deliver one more batch, which at-least-once delivery allows.
func (w *channelStateWorker) shutdown() {
	released := make([]*channelStatePartition, 0, len(w.owned)+len(w.retiring))
	for _, part := range w.owned {
		part.cancel()
		released = append(released, part)
	}
	for _, part := range w.retiring {
		part.cancel()
		if part.release {
			released = append(released, part)
		}
	}
	w.releaseAtShutdown(released)
	waitWithin(&w.heartbeats, w.callTimeout())
	w.leave()
	w.background.Wait()
	clear(w.owned)
	w.state.metrics.setPartitionsOwned(0)
}

// releaseAtShutdown releases the leases of the partitions, one pipeline per shard, with the shards in parallel.
// The wait is bounded by the call timeout.
func (w *channelStateWorker) releaseAtShutdown(parts []*channelStatePartition) {
	var releases sync.WaitGroup
	for sh, leases := range groupByShard(parts, partitionShard) {
		releases.Add(1)
		go func(sh *RedisShard, leases []*channelStatePartition) {
			defer releases.Done()
			w.releaseLeases(sh, leases)
		}(sh, leases)
	}
	waitWithin(&releases, w.callTimeout())
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
func (w *channelStateWorker) heartbeat() (int, error) {
	ctx, cancel := w.callContext()
	defer cancel()

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
// without waiting for its entry to expire. It runs when the worker stops, after its last heartbeat.
// The call is bounded by the call timeout, because a call that waits for a connection being dialed
// does not end with its context.
func (w *channelStateWorker) leave() {
	ctx, cancel := w.callContext()
	defer cancel()

	sh := w.state.workersShard
	leaving := startCall(func() (struct{}, error) {
		return struct{}{}, sh.client.Do(ctx, sh.client.B().Zrem().Key(w.state.workersKey).Member(w.node.uid).Build()).Error()
	})
	res, _ := awaitCall(leaving, ctx.Done())
	if res.err != nil {
		w.state.metrics.incSweepErrors()
		w.node.logger.log(newErrorLogEntry(res.err, "channel state worker leave failed", nil))
	}
}

// acquire takes up to need free partitions, in random order, and starts a consumer for each. A partition is free
// when this node holds no lease on it, its consumer, if it was retiring, has stopped, and its lease key is not held
// in Redis. The candidates are probed before any lease is attempted, so the attempts go to the partitions that are
// free and not to a random pick that another worker holds. The shards run in parallel, and each shard attempts at
// most the slots left of need. The loop waits for each shard for at most callTimeout: a shard that is given up
// releases the leases it takes when it returns. A node whose worker count is unknown takes every free partition it
// can (see leaseCycle), so it may exceed its share until the count is back.
func (w *channelStateWorker) acquire(need int) {
	w.forgetStopped()
	candidates := make([]int, 0, w.state.numPartitions-len(w.owned))
	for _, p := range rand.Perm(w.state.numPartitions) { //nolint:gosec // Partition order is only a fairness hint; not security-sensitive.
		_, owned := w.owned[p]
		_, retiring := w.retiring[p]
		if !owned && !retiring {
			candidates = append(candidates, p)
		}
	}
	ctx, cancel := w.callContext()
	defer cancel()
	var remaining atomic.Int64
	remaining.Store(int64(need))
	byShard := groupByShard(candidates, w.state.shardOf)
	attempts := make([]<-chan callResult[[]*channelStatePartition], 0, len(byShard))
	for sh, partitions := range byShard {
		attempts = append(attempts, startCall(func() ([]*channelStatePartition, error) {
			return w.acquireShard(sh, partitions, &remaining), nil
		}))
	}
	for _, ch := range attempts {
		res, ok := awaitCall(ch, ctx.Done())
		if !ok {
			w.releaseWhenReturned(ch)
			continue
		}
		for _, part := range res.value {
			w.startPartition(part)
		}
	}
}

// forgetStopped removes the partitions whose consumers have stopped from the retiring set.
func (w *channelStateWorker) forgetStopped() {
	for p, part := range w.retiring {
		select {
		case <-part.done:
			delete(w.retiring, p)
		default:
		}
	}
}

// releaseWhenReturned releases the leases that an acquisition takes when it returns, for an acquisition that the
// lease loop has given up. The release runs on a goroutine of its own.
func (w *channelStateWorker) releaseWhenReturned(ch <-chan callResult[[]*channelStatePartition]) {
	w.background.Add(1)
	go func() {
		defer w.background.Done()
		res := <-ch
		w.releaseParts(res.value)
	}()
}

// acquireShard probes the lease keys of the partitions of one shard in one pipeline, then attempts the leases of
// the free ones in one pipeline, on its own timeout. It attempts no more partitions than the slots it claims from
// remaining, which holds the partitions the node still needs in total. Every attempt writes its own token. It
// releases the leases of the attempts whose reply failed, since they may have been taken, and returns the leases it
// took. It starts no consumer: the caller does, on the lease loop goroutine.
func (w *channelStateWorker) acquireShard(sh *RedisShard, partitions []int, remaining *atomic.Int64) []*channelStatePartition {
	ctx, cancel := w.callContext()
	defer cancel()

	var errs callErrors
	free := w.freePartitions(ctx, sh, partitions, &errs)
	free = free[:claimSlots(remaining, len(free))]

	attempts := make([]*channelStatePartition, 0, len(free))
	cmds := make([]rueidis.Completed, 0, len(free))
	for _, p := range free {
		attempt := &channelStatePartition{p: p, shard: sh, token: w.newLeaseToken()}
		attempts = append(attempts, attempt)
		cmds = append(cmds, sh.client.B().Set().Key(w.leaseKey(p)).Value(attempt.token).Nx().PxMilliseconds(w.state.leaseTTL.Milliseconds()).Build())
	}
	since := time.Now()
	var taken []*channelStatePartition
	var failed []*channelStatePartition
	for i, res := range sh.client.DoMulti(ctx, cmds...) {
		attempt := attempts[i]
		err := res.Error()
		switch {
		case rueidis.IsRedisNil(err):
			// Another node holds the lease.
		case err != nil:
			errs.add(err, attempt.p)
			failed = append(failed, attempt)
		default:
			attempt.extendLease(since, w.state.leaseTTL)
			taken = append(taken, attempt)
		}
	}
	w.releaseParts(failed)
	w.reportCallErrors("channel state lease acquire failed", errs)
	return taken
}

// freePartitions returns the partitions of one shard whose lease key is not held, in the order given. The keys are
// read in one pipeline. A key whose read failed is not free, and its error is added to errs.
func (w *channelStateWorker) freePartitions(ctx context.Context, sh *RedisShard, partitions []int, errs *callErrors) []int {
	cmds := make([]rueidis.Completed, 0, len(partitions))
	for _, p := range partitions {
		cmds = append(cmds, sh.client.B().Get().Key(w.leaseKey(p)).Build())
	}
	free := make([]int, 0, len(partitions))
	for i, res := range sh.client.DoMulti(ctx, cmds...) {
		err := res.Error()
		switch {
		case rueidis.IsRedisNil(err):
			free = append(free, partitions[i])
		case err != nil:
			errs.add(err, partitions[i])
		}
	}
	return free
}

// claimSlots takes up to want of the slots left in remaining and returns how many it took. The shards claim their
// slots in parallel, so the claim is atomic: the attempts of all shards together never exceed what the node needs.
func claimSlots(remaining *atomic.Int64, want int) int {
	for {
		left := remaining.Load()
		if left <= 0 {
			return 0
		}
		take := min(left, int64(want))
		if remaining.CompareAndSwap(left, left-take) {
			return int(take)
		}
	}
}

// newLeaseToken returns the value an acquisition writes to the lease key: the node ID and a random part, so that
// a renewal or a release only touches the acquisition it was made for.
func (w *channelStateWorker) newLeaseToken() string {
	return w.node.uid + "." + strconv.FormatUint(rand.Uint64(), 16) //nolint:gosec // The token tells acquisitions apart; it is not a secret.
}

// startPartition starts the consumer of a partition whose lease was taken.
func (w *channelStateWorker) startPartition(part *channelStatePartition) {
	ctx, cancel := context.WithCancel(context.Background())
	part.cancel = cancel
	part.done = make(chan struct{})
	w.owned[part.p] = part
	w.background.Add(1)
	go func() {
		defer w.background.Done()
		w.consume(ctx, part)
	}()
}

// consume sweeps the partition and delivers its events on every tick until ctx is done. A tick
// that finds the lease expired does nothing: the events are delivered by the node that holds it.
func (w *channelStateWorker) consume(ctx context.Context, part *channelStatePartition) {
	defer close(part.done)

	sh := part.shard
	ticker := time.NewTicker(w.state.sweepInterval)
	defer ticker.Stop()

	var backoff channelStateBackoff
	for {
		if part.leaseHeld() && !w.consumeOnce(ctx, sh, part.p, part, &backoff) {
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
		} else {
			// The backoff resets only once the batch is delivered and acknowledged, so a failure of any step
			// of the sweep, the read, the delivery or the ack keeps the delay growing.
			backoff.reset()
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

// groupByShard groups the items by the Redis shard that shardOf returns for each of them.
func groupByShard[T any](items []T, shardOf func(T) *RedisShard) map[*RedisShard][]T {
	byShard := make(map[*RedisShard][]T)
	for _, item := range items {
		sh := shardOf(item)
		byShard[sh] = append(byShard[sh], item)
	}
	return byShard
}

// partitionShard returns the Redis shard that keeps the lease of the partition.
func partitionShard(part *channelStatePartition) *RedisShard {
	return part.shard
}

// releaseParts releases the leases of the partitions, one pipeline per shard.
func (w *channelStateWorker) releaseParts(parts []*channelStatePartition) {
	for sh, leases := range groupByShard(parts, partitionShard) {
		w.releaseLeases(sh, leases)
	}
}

// releaseLeases releases the leases of the partitions of one shard in one pipeline. The release runs on
// its own timeout, because the failure it follows may be a shard that does not answer. A lease that another
// owner holds, or that another acquisition took, is left alone by the release script.
func (w *channelStateWorker) releaseLeases(sh *RedisShard, parts []*channelStatePartition) {
	if len(parts) == 0 {
		return
	}
	ctx, cancel := w.callContext()
	defer cancel()

	execs := make([]rueidis.LuaExec, 0, len(parts))
	for _, part := range parts {
		execs = append(execs, rueidis.LuaExec{
			Keys: []string{w.leaseKey(part.p)},
			Args: []string{part.token},
		})
	}
	var errs callErrors
	for i, res := range w.state.releaseScript.ExecMulti(ctx, sh.client, execs...) {
		errs.add(res.Error(), parts[i].p) //nolint:gosec // ExecMulti returns one result per exec, one exec per partition.
	}
	w.reportCallErrors("channel state lease release failed", errs)
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

// callErrors collects the errors of the commands of one call to a shard. The call counts as one sweep
// error and logs its first error, with the partition of that error and how many commands failed.
type callErrors struct {
	first     error
	partition int
	failed    int
}

// add records the outcome of the command of the partition. A nil error is not a failure.
func (e *callErrors) add(err error, p int) {
	if err == nil {
		return
	}
	if e.first == nil {
		e.first = err
		e.partition = p
	}
	e.failed++
}

// reportCallErrors counts one sweep error for the call and logs its first error, if it failed.
func (w *channelStateWorker) reportCallErrors(message string, errs callErrors) {
	if errs.first == nil {
		return
	}
	w.state.metrics.incSweepErrors()
	w.node.logger.log(newErrorLogEntry(errs.first, message, map[string]any{"partition": errs.partition, "failed": errs.failed}))
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
