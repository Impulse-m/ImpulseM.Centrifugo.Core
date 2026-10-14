package centrifuge

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// findUserInShard returns a user ID whose hub shard is equal to, or different
// from, the shard of base.
func findUserInShard(base string, sameShard bool) string {
	baseIdx := index(base, numHubShards)
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("user-%d", i)
		if (index(candidate, numHubShards) == baseIdx) == sameShard && candidate != base {
			return candidate
		}
	}
}

func TestHubRebindSameShard(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	base := "base-user"
	target := findUserInShard(base, true)
	require.Equal(t, index(base, numHubShards), index(target, numHubShards))

	client := newTestConnectedClientV2(t, node, base)
	hub := node.hub
	require.Len(t, hub.UserConnections(base), 1)

	require.NoError(t, client.Rebind(RebindOptions{UserID: target}))

	require.Len(t, hub.UserConnections(base), 0)
	require.Len(t, hub.UserConnections(target), 1)
	require.Contains(t, hub.UserConnections(target), client.ID())
	require.Equal(t, 1, hub.NumClients())
	require.Equal(t, 1, hub.NumUsers())
}

func TestHubRebindCrossShard(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	base := "base-user"
	target := findUserInShard(base, false)
	require.NotEqual(t, index(base, numHubShards), index(target, numHubShards))

	client := newTestConnectedClientV2(t, node, base)
	hub := node.hub
	require.Len(t, hub.UserConnections(base), 1)

	require.NoError(t, client.Rebind(RebindOptions{UserID: target}))

	require.Len(t, hub.UserConnections(base), 0)
	require.Len(t, hub.UserConnections(target), 1)
	require.Contains(t, hub.UserConnections(target), client.ID())
	// The client moved shards: the old shard no longer holds it, the new one does.
	require.NotContains(t, hub.connShards[index(base, numHubShards)].clients, client.ID())
	require.Contains(t, hub.connShards[index(target, numHubShards)].clients, client.ID())
	require.Equal(t, 1, hub.NumClients())
	require.Equal(t, 1, hub.NumUsers())

	// Moving back restores the original index entry.
	require.NoError(t, client.Rebind(RebindOptions{UserID: base}))
	require.Len(t, hub.UserConnections(base), 1)
	require.Len(t, hub.UserConnections(target), 0)
	require.Equal(t, 1, hub.NumClients())
}
