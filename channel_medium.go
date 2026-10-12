package centrifuge

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/timers"
)

// ChannelMediumOptions configures an optional channel medium layer in Centrifuge.
// Channel medium layer is an optional per-channel intermediary between Broker PUB/SUB and Client
// connections. This intermediary layer may be used for various per-channel tweaks and optimizations.
// Channel medium comes with memory overhead depending on ChannelMediumOptions. At the same time, it
// can provide significant benefits in terms of overall system efficiency and flexibility.
type ChannelMediumOptions struct {
	// KeepLatestPublication enables keeping latest publication which was broadcasted to channel subscribers on
	// this Node in the channel medium layer. This is helpful for supporting deltas in at most once scenario.
	KeepLatestPublication bool

	// SharedPositionSync when true delegates connection position checks to the channel medium. In that case
	// check is only performed no more often than once in Config.ClientChannelPositionCheckDelay thus reducing
	// the load on broker in cases when channel has many subscribers. When message loss is detected medium layer
	// tells caller about this and also marks all channel subscribers with insufficient state flag. By default,
	// medium is not used for sync – in that case each individual connection syncs position independently.
	SharedPositionSync bool

	// EnableQueue for incoming publications. This can be useful to reduce PUB/SUB message processing time
	// (as we put it into a single medium layer queue instead of each individual connection queue), reduce
	// channel broadcast contention (when one channel waits for broadcast of another channel to finish),
	// and also opens a road for broadcast tweaks – such as BroadcastDelay and delta between several
	// publications (deltas require both BroadcastDelay and KeepLatestPublication to be enabled). This costs
	// additional goroutine.
	// EXPERIMENTAL: the writer goroutine is not stopped by close() – known goroutine leak, not fixed yet.
	enableQueue bool
	// QueueMaxSize is a maximum size of the queue used in channel medium (in bytes). If zero, 16MB default
	// is used. If max size reached, new publications will be dropped.
	queueMaxSize int

	// BroadcastDelay controls the delay before Publication broadcast. On time tick Centrifugo broadcasts
	// only the latest publication in the channel if any. Useful to reduce/smooth the number of messages sent
	// to clients when publication contains the entire state. If zero, all publications will be sent to clients
	// without delay logic involved on channel medium level. BroadcastDelay option requires (!) EnableQueue to be
	// enabled, as we can not afford delays during broadcast from the PUB/SUB layer. BroadcastDelay must not be
	// used in channels with positioning/recovery on since it skips publications.
	broadcastDelay time.Duration
}

func (o ChannelMediumOptions) isMediumEnabled() bool {
	return o.SharedPositionSync || o.KeepLatestPublication || o.enableQueue || o.broadcastDelay > 0
}

// Keep global to save 8 byte per-channel. Must be only changed by tests.
var channelMediumTimeNow = time.Now

// channelMedium is initialized when first subscriber comes into channel, and dropped as soon as last
// subscriber leaves the channel on the Node.
type channelMedium struct {
	channel string
	node    nodeSubset
	options ChannelMediumOptions

	mu      sync.RWMutex
	closeCh chan struct{}
	// optional queue for publications.
	messages *publicationQueue
	// We must synchronize broadcast method between general publications and insufficient state notifications.
	// Only used when queue is disabled.
	broadcastMu sync.Mutex
	// latestPublication is a publication last sent to connections on this Node.
	latestPublication *Publication
	// positionCheckTime is a time (Unix Nanoseconds) when last position check was performed.
	positionCheckTime int64
	// cachedTop is the last stream top fetched by a position check, cachedAt is the time (Unix Nanoseconds)
	// of that fetch. cachedAt is zero until the first fetch.
	cachedTop StreamPosition
	cachedAt  int64
	// lastPosition is a StreamPosition up to which every local subscriber has been updated: it is set after
	// a non-marker publication is handed to the Node. Only valid when hasLast is true.
	lastPosition StreamPosition
	hasLast      bool
	// suspectTop is a stream top seen by the previous check while the Node looked behind the stream. Only
	// valid when hasSuspect is true.
	suspectTop StreamPosition
	hasSuspect bool
}

type nodeSubset interface {
	handlePublication(ch string, sp StreamPosition, pub, prevPub *Publication, localPrevPub *Publication) error
	streamTop(ch string, historyMetaTTL time.Duration) (StreamPosition, error)
}

func newChannelMedium(channel string, node nodeSubset, options ChannelMediumOptions) (*channelMedium, error) {
	if options.broadcastDelay > 0 && !options.enableQueue {
		return nil, errors.New("broadcast delay can only be used with queue enabled")
	}
	c := &channelMedium{
		channel:           channel,
		node:              node,
		options:           options,
		closeCh:           make(chan struct{}),
		positionCheckTime: channelMediumTimeNow().UnixNano(),
	}
	if options.enableQueue {
		c.messages = newPublicationQueue(2)
		go c.writer()
	}
	return c, nil
}

type queuedPub struct {
	pub                 *Publication
	sp                  StreamPosition
	prevPub             *Publication
	delta               bool
	isInsufficientState bool
}

const defaultChannelLayerQueueMaxSize = 16 * 1024 * 1024

func (c *channelMedium) broadcastPublication(pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) {
	bp := queuedPub{pub: pub, sp: sp, prevPub: prevPub, delta: delta}
	c.mu.Lock()
	c.positionCheckTime = channelMediumTimeNow().UnixNano()
	c.mu.Unlock()

	if c.options.enableQueue {
		queueMaxSize := defaultChannelLayerQueueMaxSize
		if c.options.queueMaxSize > 0 {
			queueMaxSize = c.options.queueMaxSize
		}
		if c.messages.Size() > queueMaxSize {
			return
		}
		c.messages.Add(queuedPublication{Publication: bp})
	} else {
		c.broadcastMu.Lock()
		defer c.broadcastMu.Unlock()
		c.broadcast(bp)
	}
}

func (c *channelMedium) broadcastInsufficientState() {
	bp := queuedPub{prevPub: nil, isInsufficientState: true}
	c.mu.Lock()
	c.positionCheckTime = channelMediumTimeNow().UnixNano()
	c.mu.Unlock()
	if c.options.enableQueue {
		// TODO: possibly support c.messages.dropQueued() for this path ?
		c.messages.Add(queuedPublication{Publication: bp})
	} else {
		c.broadcastMu.Lock()
		defer c.broadcastMu.Unlock()
		c.broadcast(bp)
	}
}

func (c *channelMedium) broadcast(qp queuedPub) {
	pubToBroadcast := qp.pub
	spToBroadcast := qp.sp
	if qp.isInsufficientState {
		// using math.MaxUint64 as a special offset to trigger insufficient state.
		pubToBroadcast = &Publication{Offset: math.MaxUint64}
		spToBroadcast.Offset = math.MaxUint64
	}

	prevPub := qp.prevPub
	var localPrevPub *Publication
	useLocalLatestPub := c.options.KeepLatestPublication && !qp.isInsufficientState
	if useLocalLatestPub && qp.delta {
		localPrevPub = c.latestPublication
	}
	if c.options.broadcastDelay > 0 && !c.options.KeepLatestPublication {
		prevPub = nil
	}
	if qp.isInsufficientState {
		prevPub = nil
	}
	_ = c.node.handlePublication(c.channel, spToBroadcast, pubToBroadcast, prevPub, localPrevPub)
	// Only after handlePublication returns every local subscriber has been updated, so lastPosition is set here.
	c.mu.Lock()
	if qp.isInsufficientState {
		c.hasSuspect = false
	} else {
		// Monotone within an epoch: a publication that was still in flight when a marker moved lastPosition
		// to the stream top must not move it back.
		if !c.hasLast || spToBroadcast.Epoch != c.lastPosition.Epoch || spToBroadcast.Offset > c.lastPosition.Offset {
			c.lastPosition = spToBroadcast
			c.hasLast = true
		}
		if c.hasSuspect && spToBroadcast.Epoch == c.suspectTop.Epoch && spToBroadcast.Offset >= c.suspectTop.Offset {
			c.hasSuspect = false
		}
	}
	c.mu.Unlock()
	if useLocalLatestPub {
		c.latestPublication = qp.pub
	}
}

func (c *channelMedium) writer() {
	for {
		if ok := c.waitSendPub(c.options.broadcastDelay); !ok {
			return
		}
	}
}

func (c *channelMedium) waitSendPub(delay time.Duration) bool {
	// Wait for message from the queue.
	ok := c.messages.Wait()
	if !ok {
		return false
	}

	if delay > 0 {
		tm := timers.AcquireTimer(delay)
		select {
		case <-tm.C:
		case <-c.closeCh:
			timers.ReleaseTimer(tm)
			return false
		}
		timers.ReleaseTimer(tm)
	}

	msg, ok := c.messages.Remove()
	if !ok {
		return !c.messages.Closed()
	}
	if delay == 0 || msg.Publication.isInsufficientState {
		c.broadcast(msg.Publication)
		return true
	}
	messageCount := c.messages.Len()
	for messageCount > 0 {
		messageCount--
		var ok bool
		msg, ok = c.messages.Remove()
		if !ok {
			if c.messages.Closed() {
				return false
			}
			break
		}
		if msg.Publication.isInsufficientState {
			break
		}
	}
	c.broadcast(msg.Publication)
	return true
}

// CheckPosition checks position of a single client against the channel stream top.
//
// The stream top is fetched at most once per checkDelay for the whole channel. Other calls within the
// interval use the cached top. checked is false only for calls made before the first fetch of the channel.
// When checked is true and err is nil, validPosition tells whether the client may keep its position.
//
// Node state is evaluated once per interval, on the call that fetches the top:
//   - the node is behind when the stream top is ahead of lastPosition (or of the client position when nothing
//     was broadcast yet). The first sighting only marks the node as suspect: a publication may still be in
//     flight between the stream and the Node. If the node is still behind one full checkDelay later,
//     insufficient state is broadcast to all channel subscribers on this node.
//   - an invalid client is marked individually only when the node delivered something it lacks, or when its
//     epoch differs from the stream top and the node is not suspect. Otherwise the client is behind only
//     because the node is, and the node-wide marker covers it.
func (c *channelMedium) CheckPosition(historyMetaTTL time.Duration, clientPosition StreamPosition, checkDelay time.Duration) (checked bool, validPosition bool, err error) {
	nowUnixNano := channelMediumTimeNow().UnixNano()
	c.mu.Lock()
	needFetch := nowUnixNano-c.positionCheckTime >= checkDelay.Nanoseconds()
	if needFetch {
		c.positionCheckTime = nowUnixNano
	}
	c.mu.Unlock()

	var streamTop StreamPosition
	if needFetch {
		streamTop, err = c.streamTopWithRetry(historyMetaTTL)
		if err != nil {
			// Interval is consumed, position will be checked again on the next interval.
			return true, false, err
		}
	}

	c.mu.Lock()
	if needFetch {
		c.cachedTop = streamTop
		c.cachedAt = nowUnixNano
	} else {
		if c.cachedAt == 0 {
			// Nothing fetched for this channel yet, the check happens on the next interval.
			c.mu.Unlock()
			return false, true, nil
		}
		streamTop = c.cachedTop
	}
	if clientPosition.Epoch == streamTop.Epoch && clientPosition.Offset >= streamTop.Offset {
		c.mu.Unlock()
		return true, true, nil
	}

	// Floor is what the Node is known to have delivered. Without a broadcast the client position is used.
	floor := clientPosition
	if c.hasLast {
		floor = c.lastPosition
	}
	if needFetch {
		behind := streamTop.Epoch != floor.Epoch || streamTop.Offset > floor.Offset
		if !behind {
			c.hasSuspect = false
		} else if c.hasSuspect && (floor.Epoch != c.suspectTop.Epoch || floor.Offset < c.suspectTop.Offset) {
			// Still behind one full interval after the first sighting: the Node is behind the stream.
			c.hasSuspect = false
			// Subscribers are about to resync from the stream top, so the Node is considered up to it.
			// Without this the same gap would be re-confirmed on every interval while the channel is quiet.
			if !c.hasLast || streamTop.Epoch != c.lastPosition.Epoch || streamTop.Offset > c.lastPosition.Offset {
				c.lastPosition = streamTop
				c.hasLast = true
			}
			c.mu.Unlock()
			c.broadcastInsufficientState()
			return true, false, nil
		} else {
			c.suspectTop = streamTop
			c.hasSuspect = true
		}
	}

	// Client is behind the stream top, but the Node is not confirmed behind.
	behindDelivered := c.hasLast && clientPosition.Epoch == c.lastPosition.Epoch && clientPosition.Offset < c.lastPosition.Offset
	staleEpoch := clientPosition.Epoch != streamTop.Epoch && !c.hasSuspect
	c.mu.Unlock()
	if behindDelivered || staleEpoch {
		return true, false, nil
	}
	return true, true, nil
}

// streamTopWithRetry fetches stream top, retrying once on error.
func (c *channelMedium) streamTopWithRetry(historyMetaTTL time.Duration) (StreamPosition, error) {
	streamTop, err := c.node.streamTop(c.channel, historyMetaTTL)
	if err != nil {
		streamTop, err = c.node.streamTop(c.channel, historyMetaTTL)
	}
	return streamTop, err
}

// close only closes closeCh and is called when the medium is removed from the Node. A publication that still
// reaches a closed medium (it was fetched before the removal) is benign: it is broadcast to a hub that has no
// subscribers for the channel anymore.
func (c *channelMedium) close() {
	close(c.closeCh)
}

type queuedPublication struct {
	Publication queuedPub
}

// publicationQueue is an unbounded queue of queuedPublication.
// The queue is goroutine safe.
// Inspired by http://blog.dubbelboer.com/2015/04/25/go-faster-queue.html (MIT)
type publicationQueue struct {
	mu      sync.RWMutex
	cond    *sync.Cond
	nodes   []queuedPublication
	head    int
	tail    int
	cnt     int
	size    int
	closed  bool
	initCap int
}

// newPublicationQueue returns a new queuedPublication queue with initial capacity.
func newPublicationQueue(initialCapacity int) *publicationQueue {
	sq := &publicationQueue{
		initCap: initialCapacity,
		nodes:   make([]queuedPublication, initialCapacity),
	}
	sq.cond = sync.NewCond(&sq.mu)
	return sq
}

// Mutex must be held when calling.
func (q *publicationQueue) resize(n int) {
	nodes := make([]queuedPublication, n)
	if q.head < q.tail {
		copy(nodes, q.nodes[q.head:q.tail])
	} else {
		copy(nodes, q.nodes[q.head:])
		copy(nodes[len(q.nodes)-q.head:], q.nodes[:q.tail])
	}

	q.tail = q.cnt % n
	q.head = 0
	q.nodes = nodes
}

// Add an queuedPublication to the back of the queue
// will return false if the queue is closed.
// In that case the queuedPublication is dropped.
func (q *publicationQueue) Add(i queuedPublication) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	if q.cnt == len(q.nodes) {
		// Also tested a growth rate of 1.5, see: http://stackoverflow.com/questions/2269063/buffer-growth-strategy
		// In Go this resulted in a higher memory usage.
		q.resize(q.cnt * 2)
	}
	q.nodes[q.tail] = i
	q.tail = (q.tail + 1) % len(q.nodes)
	if i.Publication.pub != nil {
		q.size += len(i.Publication.pub.Data)
	}
	q.cnt++
	q.cond.Signal()
	q.mu.Unlock()
	return true
}

// Close the queue and discard all entries in the queue
// all goroutines in wait() will return
func (q *publicationQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cnt = 0
	q.nodes = nil
	q.size = 0
	q.cond.Broadcast()
}

// Closed returns true if the queue has been closed
// The call cannot guarantee that the queue hasn't been
// closed while the function returns, so only "true" has a definite meaning.
func (q *publicationQueue) Closed() bool {
	q.mu.RLock()
	c := q.closed
	q.mu.RUnlock()
	return c
}

// Wait for a message to be added.
// If there are items on the queue will return immediately.
// Will return false if the queue is closed.
// Otherwise, returns true.
func (q *publicationQueue) Wait() bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	if q.cnt != 0 {
		q.mu.Unlock()
		return true
	}
	q.cond.Wait()
	q.mu.Unlock()
	return true
}

// Remove will remove an queuedPublication from the queue.
// If false is returned, it either means 1) there were no items on the queue
// or 2) the queue is closed.
func (q *publicationQueue) Remove() (queuedPublication, bool) {
	q.mu.Lock()
	if q.cnt == 0 {
		q.mu.Unlock()
		return queuedPublication{}, false
	}
	i := q.nodes[q.head]
	q.head = (q.head + 1) % len(q.nodes)
	q.cnt--
	if i.Publication.pub != nil {
		q.size -= len(i.Publication.pub.Data)
	}

	if n := len(q.nodes) / 2; n >= q.initCap && q.cnt <= n {
		q.resize(n)
	}

	q.mu.Unlock()
	return i, true
}

// Len returns the current length of the queue.
func (q *publicationQueue) Len() int {
	q.mu.RLock()
	l := q.cnt
	q.mu.RUnlock()
	return l
}

// Size returns the current size of the queue.
func (q *publicationQueue) Size() int {
	q.mu.RLock()
	s := q.size
	q.mu.RUnlock()
	return s
}
