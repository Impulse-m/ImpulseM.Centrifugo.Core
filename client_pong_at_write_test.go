package centrifuge

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifuge/internal/queue"
)

// gatedTransport is a bidirectional test transport. A ping waits for one token from gate, so a
// test decides when the writer may write pings: admit adds tokens, and releaseAll lets
// everything through. With gateAll every message waits, not only pings. A write of several
// messages takes one token per waiting message. The embedded testTransport supplies the other
// methods; its Write and WriteMany are not called, so its sink and closed behaviour do not apply.
// The counters and the callback use the embedded mutex.
type gatedTransport struct {
	*testTransport
	gate    chan struct{}
	gateAll bool
	// onPingWritten, when set, runs inside a ping write once its token is taken, before the write returns.
	onPingWritten func()
	pingAttempts  int
	pingWrites    int
}

// pingCounts is how many ping messages reached the gate and how many passed it.
type pingCounts struct {
	attempts int
	writes   int
}

func newGatedTransport(cancelFn func(), pingInterval, pongTimeout time.Duration) *gatedTransport {
	tt := newTestTransport(cancelFn)
	tt.setPing(pingInterval, pongTimeout)
	return &gatedTransport{
		testTransport: tt,
		gate:          make(chan struct{}, 256),
	}
}

// admit lets n messages through.
func (t *gatedTransport) admit(n int) {
	for range n {
		t.gate <- struct{}{}
	}
}

// releaseAll lets every blocked and future message through. It is called once, before the
// client closes, because a write blocked in the gate holds the writer lock.
func (t *gatedTransport) releaseAll() {
	close(t.gate)
}

// counts returns the ping counters.
func (t *gatedTransport) counts() pingCounts {
	t.mu.Lock()
	defer t.mu.Unlock()
	return pingCounts{attempts: t.pingAttempts, writes: t.pingWrites}
}

func (t *gatedTransport) take(data []byte) {
	ping := bytes.Equal(data, jsonPingReply)
	if ping {
		t.mu.Lock()
		t.pingAttempts++
		t.mu.Unlock()
	}
	if ping || t.gateAll {
		<-t.gate
	}
	if ping {
		t.mu.Lock()
		t.pingWrites++
		t.mu.Unlock()
	}
}

// setOnPingWritten sets the callback that runs inside each ping write.
func (t *gatedTransport) setOnPingWritten(fn func()) {
	t.mu.Lock()
	t.onPingWritten = fn
	t.mu.Unlock()
}

func (t *gatedTransport) pingWritten(data []byte) {
	if !bytes.Equal(data, jsonPingReply) {
		return
	}
	t.mu.Lock()
	fn := t.onPingWritten
	t.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func (t *gatedTransport) Write(data []byte) error {
	t.take(data)
	t.pingWritten(data)
	return nil
}

func (t *gatedTransport) WriteMany(messages ...[]byte) error {
	for _, data := range messages {
		t.take(data)
		t.pingWritten(data)
	}
	return nil
}

// pongAtWriteSetup configures a pongAtWriteFixture.
type pongAtWriteSetup struct {
	pongAtWrite  bool
	uni          bool
	gateAll      bool
	pingInterval time.Duration
	pongTimeout  time.Duration
	queueMaxSize int
	connecting   bool
}

// pongAtWriteFixture is a connected client on a gatedTransport. Its own connect-time ping timer
// is removed, so the test controls every ping. Build it and call shutdown inside a synctest bubble.
type pongAtWriteFixture struct {
	node           *Node
	client         *Client
	transport      *gatedTransport
	disconnectCode atomic.Uint32
}

// pongState is the client fields the tests check, read under the client lock.
type pongState struct {
	nextPong   int64
	lastPing   int64
	pingQueued bool
}

func newPongAtWriteFixture(t *testing.T, setup pongAtWriteSetup) *pongAtWriteFixture {
	f := &pongAtWriteFixture{
		node: defaultNodeNoHandlers(),
	}
	f.node.config.ClientPongAtWrite = setup.pongAtWrite
	if setup.queueMaxSize > 0 {
		f.node.config.ClientQueueMaxSize = setup.queueMaxSize
	}
	f.node.OnConnect(func(client *Client) {
		client.OnDisconnect(func(event DisconnectEvent) {
			f.disconnectCode.Store(event.Disconnect.Code)
		})
	})
	if setup.connecting {
		f.node.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
			return ConnectReply{}, nil
		})
	}
	var ctx context.Context
	var cancelFn context.CancelFunc
	ctx, cancelFn = context.WithCancel(context.Background())
	f.transport = newGatedTransport(cancelFn, setup.pingInterval, setup.pongTimeout)
	f.transport.setUnidirectional(setup.uni)
	f.transport.gateAll = setup.gateAll
	f.client = newTestClientCustomTransport(t, ctx, f.node, f.transport, "42")
	connectClientV2(t, f.client)
	f.client.mu.Lock()
	f.client.nextPing = 0
	f.client.scheduleNextTimer()
	f.client.mu.Unlock()
	return f
}

func (f *pongAtWriteFixture) shutdown() {
	f.transport.releaseAll()
	_ = f.client.close(DisconnectForceNoReconnect)
	_ = f.node.Shutdown(context.Background())
}

// deliverPong sends the pong for the outstanding ping, the way the reader does: an empty
// command with no id and no send is a pong.
func (f *pongAtWriteFixture) deliverPong() bool {
	return f.client.HandleCommand(&protocol.Command{}, 0)
}

func (f *pongAtWriteFixture) state() pongState {
	f.client.mu.Lock()
	defer f.client.mu.Unlock()
	return pongState{
		nextPong:   f.client.nextPong,
		lastPing:   f.client.lastPing,
		pingQueued: f.client.pingQueued,
	}
}

// advance moves the fake clock by d and lets the timers it wakes run to rest.
func advance(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}

func TestPongAtWriteOffArmsAtEnqueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newPongAtWriteFixture(t, pongAtWriteSetup{
			pingInterval: 25 * time.Second,
			pongTimeout:  8 * time.Second,
		})
		defer f.shutdown()

		enqueuedAt := time.Now()
		f.client.sendPing()
		st := f.state()
		require.Equal(t, enqueuedAt.Add(8*time.Second).UnixNano(), st.nextPong)
	})
}

func TestPongAtWriteOnNotArmedWhileQueued(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newPongAtWriteFixture(t, pongAtWriteSetup{
			pongAtWrite:  true,
			pingInterval: 25 * time.Second,
			pongTimeout:  8 * time.Second,
		})
		defer f.shutdown()

		f.client.sendPing()
		advance(10 * 8 * time.Second)
		st := f.state()
		require.Zero(t, st.nextPong)
		require.NotZero(t, st.lastPing)
		require.Zero(t, f.disconnectCode.Load())
	})
}

func TestPongAtWriteArmedAtWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newPongAtWriteFixture(t, pongAtWriteSetup{
			pongAtWrite:  true,
			pingInterval: 25 * time.Second,
			pongTimeout:  8 * time.Second,
		})
		defer f.shutdown()
		require.True(t, f.client.pongAtWriteOn)

		// The callback runs inside the write, before the hook. No fake time passes between
		// the two, so it reads the same instant the hook arms from.
		var writtenAt atomic.Int64
		f.transport.setOnPingWritten(func() {
			writtenAt.Store(time.Now().UnixNano())
		})
		enqueuedAt := time.Now().UnixNano()
		f.client.sendPing()
		advance(24 * time.Second)
		f.transport.admit(1)
		synctest.Wait()

		written := writtenAt.Load()
		require.Equal(t, int64(24*time.Second), written-enqueuedAt)
		st := f.state()
		require.Equal(t, written+int64(8*time.Second), st.nextPong)
	})
}

func TestPongAtWriteNoPongDisconnects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newPongAtWriteFixture(t, pongAtWriteSetup{
			pongAtWrite:  true,
			pingInterval: 25 * time.Second,
			pongTimeout:  8 * time.Second,
		})
		defer f.shutdown()

		// The writer config holds the hook only when the feature is on, and the
		// connect path must have wired it.
		require.NotNil(t, f.client.messageWriter.config.AfterPingWriteFn)
		f.client.sendPing()
		f.transport.admit(1)
		synctest.Wait()
		advance(8*time.Second + time.Nanosecond)
		require.Equal(t, DisconnectNoPong.Code, f.disconnectCode.Load())
	})
}

func TestPongAtWritePongInTimeKeepsConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newPongAtWriteFixture(t, pongAtWriteSetup{
			pongAtWrite:  true,
			pingInterval: 25 * time.Second,
			pongTimeout:  8 * time.Second,
		})
		defer f.shutdown()

		f.client.sendPing()
		f.transport.admit(1)
		synctest.Wait()
		advance(8*time.Second - time.Millisecond)
		require.True(t, f.deliverPong())
		advance(8 * time.Second)
		require.Zero(t, f.disconnectCode.Load())
	})
}

func TestPongAtWriteSkipsWhileUnanswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newPongAtWriteFixture(t, pongAtWriteSetup{
			pongAtWrite:  true,
			pingInterval: 25 * time.Second,
			pongTimeout:  8 * time.Second,
		})
		defer f.shutdown()

		f.client.sendPing()
		advance(25 * time.Second)
		advance(25 * time.Second)
		advance(25 * time.Second)
		f.transport.admit(1)
		synctest.Wait()

		counts := f.transport.counts()
		require.Equal(t, 1, counts.attempts)
		require.Equal(t, 1, counts.writes)
	})
}

func TestPongAtWriteStaleDeadlineNoFalseNoPong(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newPongAtWriteFixture(t, pongAtWriteSetup{
			pongAtWrite:  true,
			pingInterval: 25 * time.Second,
			pongTimeout:  8 * time.Second,
		})
		defer f.shutdown()

		f.client.sendPing()
		advance(20 * time.Second)
		f.transport.admit(1)
		synctest.Wait()
		// The hook armed the deadline for ping N at t=28s.
		advance(100 * time.Millisecond)
		require.True(t, f.deliverPong())
		// The tick at t=25s enqueues ping N+1, and that write stays blocked. Run on to t=40s.
		advance(19900 * time.Millisecond)

		st := f.state()
		require.Zero(t, st.nextPong)
		require.True(t, st.pingQueued)
		require.Zero(t, f.disconnectCode.Load())
		counts := f.transport.counts()
		require.Equal(t, 2, counts.attempts)
		require.Equal(t, 1, counts.writes)
	})
}

func TestPongAtWritePongBetweenWriteAndHook(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newPongAtWriteFixture(t, pongAtWriteSetup{
			pongAtWrite:  true,
			pingInterval: 25 * time.Second,
			pongTimeout:  8 * time.Second,
		})
		defer f.shutdown()

		var pongDelivered atomic.Bool
		// The pong and a tick both arrive inside the write, before the hook runs.
		f.transport.setOnPingWritten(func() {
			pongDelivered.Store(f.deliverPong())
			f.client.sendPing()
		})
		f.client.sendPing()
		f.transport.admit(1)
		synctest.Wait()
		require.True(t, pongDelivered.Load())
		counts := f.transport.counts()
		require.Equal(t, 1, counts.attempts)
		require.Equal(t, 1, counts.writes)

		// The deadline armed by the hook is cleared by the pong at its check.
		advance(26 * time.Second)
		require.Zero(t, f.disconnectCode.Load())
		st := f.state()
		require.Zero(t, st.nextPong)
	})
}

func TestPongAtWriteInertWhenNotApplicable(t *testing.T) {
	tests := []struct {
		name  string
		setup pongAtWriteSetup
	}{
		{
			name: "unidirectional transport",
			setup: pongAtWriteSetup{
				pongAtWrite:  true,
				uni:          true,
				pingInterval: 25 * time.Second,
				pongTimeout:  8 * time.Second,
			},
		},
		{
			name: "pong timeout disabled",
			setup: pongAtWriteSetup{
				pongAtWrite:  true,
				pingInterval: 25 * time.Second,
				pongTimeout:  -1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newPongAtWriteFixture(t, tt.setup)
				defer f.shutdown()

				f.transport.admit(3)
				f.client.sendPing()
				advance(25 * time.Second)
				advance(25 * time.Second)
				synctest.Wait()

				counts := f.transport.counts()
				require.Equal(t, 3, counts.writes)
				st := f.state()
				require.Zero(t, st.nextPong)
			})
		})
	}
}

func TestPongAtWriteClosedClientDoesNotArm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newPongAtWriteFixture(t, pongAtWriteSetup{
			pongAtWrite:  true,
			pingInterval: 25 * time.Second,
			pongTimeout:  8 * time.Second,
		})
		defer f.shutdown()

		_ = f.client.close(DisconnectForceNoReconnect)
		f.client.armPongAfterWrite()
		st := f.state()
		require.Zero(t, st.nextPong)
		require.False(t, st.pingQueued)
	})
}

func TestPongAtWriteArmedWithConnectingHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// OnConnecting returns an empty reply, so connect runs the handler branch. The
		// fixture has OnConnect only otherwise, which takes the else branch.
		f := newPongAtWriteFixture(t, pongAtWriteSetup{
			pongAtWrite:  true,
			connecting:   true,
			pingInterval: 25 * time.Second,
			pongTimeout:  8 * time.Second,
		})
		defer f.shutdown()
		require.True(t, f.client.pongAtWriteOn)

		var writtenAt atomic.Int64
		f.transport.setOnPingWritten(func() {
			writtenAt.Store(time.Now().UnixNano())
		})
		f.client.sendPing()
		f.transport.admit(1)
		synctest.Wait()

		st := f.state()
		require.Equal(t, writtenAt.Load()+int64(8*time.Second), st.nextPong)
	})
}

func TestPongAtWriteSlowDrainNoNoPong(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newPongAtWriteFixture(t, pongAtWriteSetup{
			pongAtWrite:  true,
			gateAll:      true,
			pingInterval: 60 * time.Second,
			pongTimeout:  time.Second,
			queueMaxSize: 3 << 20,
		})
		defer f.shutdown()

		// 2 MiB of replies ahead of the ping, in 64 KiB messages. The transport admits one
		// message per fake second, so the drain takes about 32 s, far longer than pongTimeout.
		for range 32 {
			require.Nil(t, f.client.messageWriter.enqueue(queue.Item{
				Data:      make([]byte, 64<<10),
				FrameType: protocol.FrameTypePushMessage,
			}))
		}
		f.client.sendPing()

		// 33 admitted messages are needed: 32 replies and the ping. The bound stops the
		// loop if the ping is never written.
		var counts pingCounts
		for range 64 {
			counts = f.transport.counts()
			if counts.writes > 0 {
				break
			}
			st := f.state()
			require.Zero(t, st.nextPong, "pong armed before the ping was written")
			require.Zero(t, f.disconnectCode.Load(), "disconnect while the queue drains")
			advance(time.Second)
			f.transport.admit(1)
			synctest.Wait()
		}
		require.NotZero(t, counts.writes, "the ping was never written")
		st := f.state()
		require.NotZero(t, st.nextPong)
		require.Zero(t, f.disconnectCode.Load())
	})
}
