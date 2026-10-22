package centrifuge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelStateShare(t *testing.T) {
	t.Parallel()
	w := &channelStateWorker{state: &presenceState{numPartitions: 8}}
	tests := []struct {
		name    string
		workers int
		want    int
	}{
		{name: "no count takes every partition", workers: 0, want: 8},
		{name: "one worker takes every partition", workers: 1, want: 8},
		{name: "two workers split evenly", workers: 2, want: 4},
		{name: "three workers round up", workers: 3, want: 3},
		{name: "more workers than partitions take one each", workers: 20, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, w.share(tt.workers))
		})
	}
}

func TestPubSubProbeRestartBackoff(t *testing.T) {
	t.Parallel()
	interval := time.Second
	want := []time.Duration{
		0,
		0,
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		10 * time.Second,
		10 * time.Second,
	}
	for restarts, delay := range want {
		require.Equal(t, delay, pubSubProbeRestartBackoff(interval, restarts), "restarts %d", restarts)
	}
	require.Equal(t, 10*time.Second, pubSubProbeRestartBackoff(interval, 1000000))
	require.Equal(t, time.Duration(0), pubSubProbeRestartBackoff(0, 5))
}

func TestChannelStateBackoff(t *testing.T) {
	t.Parallel()
	var backoff channelStateBackoff
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		6400 * time.Millisecond,
		12800 * time.Millisecond,
		20 * time.Second,
		20 * time.Second,
	}
	for i, delay := range want {
		require.Equal(t, delay, backoff.next(), "attempt %d", i)
	}

	backoff.reset()
	require.Equal(t, 100*time.Millisecond, backoff.next())
	require.Equal(t, 200*time.Millisecond, backoff.next())
}

// TestChannelStateProbeBackoffBounds checks the probe backoff of a worker: it starts at one lease tick and doubles up
// to LeaseTTL, which is the bound the probes of an idle worker take.
func TestChannelStateProbeBackoffBounds(t *testing.T) {
	t.Parallel()
	w := newChannelStateWorker(&presenceState{leaseTTL: 5 * time.Second}, &Node{})
	want := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		5 * time.Second,
		5 * time.Second,
	}
	for i, delay := range want {
		require.Equal(t, delay, w.probe.next(), "probe %d", i)
	}
}
