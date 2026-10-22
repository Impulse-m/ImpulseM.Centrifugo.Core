package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClientExtAllocatedOnlyWhenSet checks that a connection without a token identity or extension state holds no
// fork state, and that the first non-zero identity allocates it.
func TestClientExtAllocatedOnlyWhenSet(t *testing.T) {
	client := &Client{}
	client.storeUser("user-42")
	client.mu.Lock()
	client.setTokenIdentity(TokenIdentity{})
	client.mu.Unlock()
	require.Nil(t, client.ext.Load())
	require.Nil(t, client.ExtState())

	client.mu.Lock()
	client.setTokenIdentity(TokenIdentity{Generation: 1})
	client.mu.Unlock()
	require.NotNil(t, client.ext.Load())
	require.Equal(t, uint64(1), client.TokenIdentity().Generation)
}

// BenchmarkClientExt measures a new connection with and without a token identity. A connection without one allocates
// no fork state, and a connection with one allocates the clientExt struct once.
func BenchmarkClientExt(b *testing.B) {
	identity := TokenIdentity{Generation: 1}
	b.Run("NoIdentity", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			client := &Client{}
			client.mu.Lock()
			client.setTokenIdentity(TokenIdentity{})
			client.mu.Unlock()
		}
	})
	b.Run("TokenIdentity", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			client := &Client{}
			client.mu.Lock()
			client.setTokenIdentity(identity)
			client.mu.Unlock()
		}
	})
}
