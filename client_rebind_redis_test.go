//go:build integration

package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// requireRedisPresenceStats checks clients and unique users of a channel.
func requireRedisPresenceStats(t *testing.T, pm *RedisPresenceManager, channel string, clients int, users int) {
	t.Helper()
	stats, err := pm.PresenceStats(channel)
	require.NoError(t, err)
	require.Equal(t, clients, stats.NumClients)
	require.Equal(t, users, stats.NumUsers)
}

func TestClientRebindRedisPresence(t *testing.T) {
	t.Parallel()
	for _, tt := range excludeClusterPresenceTests(redisPresenceTests) {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)
			pm := newTestRedisPresenceManager(t, node, false, true, false, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisPresenceManager(pm)

			newPresenceClient := func(user string) *Client {
				client := newTestClientV2(t, node, user)
				client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
					cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
				})
				connectClientV2(t, client)
				return client
			}

			old := newPresenceClient("old")
			subscribeClientV2(t, old, "test")
			other := newPresenceClient("other")
			subscribeClientV2(t, other, "test")
			requireRedisPresenceStats(t, pm, "test", 2, 2)

			require.NoError(t, old.Rebind(RebindOptions{UserID: "new"}))
			// Presence is rewritten before Rebind returns: the old user is gone and
			// the new one is counted, so the unique users stay at 2.
			requireRedisPresenceStats(t, pm, "test", 2, 2)
			presence, err := pm.Presence("test")
			require.NoError(t, err)
			require.Len(t, presence, 2)
			require.Equal(t, "new", presence[old.ID()].UserID)

			// A second connection of the new user joins and then leaves. The new
			// user still has the rebound connection, so it must stay counted.
			second := newPresenceClient("new")
			subscribeClientV2(t, second, "test")
			requireRedisPresenceStats(t, pm, "test", 3, 2)
			second.Unsubscribe("test")
			require.Eventually(t, func() bool {
				stats, err := pm.PresenceStats("test")
				return err == nil && stats.NumClients == 2
			}, 5*time.Second, 10*time.Millisecond)
			requireRedisPresenceStats(t, pm, "test", 2, 2)
		})
	}
}
