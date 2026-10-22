package filter

import (
	"fmt"
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// notChain returns a NOT chain of the given number of NOT nodes over one leaf.
func notChain(depth int) *protocol.FilterNode {
	node := &protocol.FilterNode{Cmp: CompareEQ, Key: "env", Val: "prod"}
	for range depth {
		node = &protocol.FilterNode{Op: OpNot, Nodes: []*protocol.FilterNode{node}}
	}
	return node
}

// flatAnd returns an AND node over count-1 leaves, so the filter has count nodes and depth 2.
func flatAnd(count int) *protocol.FilterNode {
	children := make([]*protocol.FilterNode, 0, count-1)
	for i := 1; i < count; i++ {
		children = append(children, &protocol.FilterNode{Cmp: CompareEQ, Key: "env", Val: "v" + fmt.Sprint(i)})
	}
	return &protocol.FilterNode{Op: OpAnd, Nodes: children}
}

func TestValidateClientRejectsLargeFilters(t *testing.T) {
	t.Parallel()
	const maxNodes = 256
	const maxValues = 1024
	require.NoError(t, ValidateClient(&protocol.FilterNode{Cmp: CompareEQ, Key: "env", Val: "prod"}, maxNodes, maxValues))
	require.NoError(t, ValidateClient(flatAnd(maxNodes), maxNodes, maxValues))
	require.Error(t, ValidateClient(flatAnd(maxNodes+1), maxNodes, maxValues))

	vals := make([]string, maxValues+1)
	for i := range vals {
		vals[i] = "v" + fmt.Sprint(i)
	}
	require.NoError(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals[:maxValues]}, maxNodes, maxValues))
	require.Error(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals}, maxNodes, maxValues))
}

// TestValidateClientZeroMeansDefault checks that a zero limit is the default of 128 nodes and 128 values, and
// that a negative limit turns the node or value cap off.
func TestValidateClientZeroMeansDefault(t *testing.T) {
	t.Parallel()
	require.Equal(t, 128, DefaultClientFilterMaxNodes)
	require.Equal(t, 128, DefaultClientFilterMaxValues)
	require.NoError(t, ValidateClient(flatAnd(DefaultClientFilterMaxNodes), 0, 0))
	require.Error(t, ValidateClient(flatAnd(DefaultClientFilterMaxNodes+1), 0, 0))

	vals := make([]string, DefaultClientFilterMaxValues+1)
	for i := range vals {
		vals[i] = "v" + fmt.Sprint(i)
	}
	require.NoError(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals[:DefaultClientFilterMaxValues]}, 0, 0))
	require.Error(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals}, 0, 0))
}

// TestValidateClientNegativeDisablesCaps checks that a negative limit accepts a filter of any node or value count.
func TestValidateClientNegativeDisablesCaps(t *testing.T) {
	t.Parallel()
	require.NoError(t, ValidateClient(flatAnd(1000), -1, -1))

	vals := make([]string, 2000)
	for i := range vals {
		vals[i] = "v" + fmt.Sprint(i)
	}
	require.NoError(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals}, -1, -1))
	require.NoError(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals}, 0, -1))
}

// TestValidateClientDepthLimit checks that a client filter nested deeper than 16 levels is refused whatever the
// node and value limits are, and that a filter at the depth is accepted.
func TestValidateClientDepthLimit(t *testing.T) {
	t.Parallel()
	require.NoError(t, ValidateClient(notChain(15), -1, -1))
	require.Error(t, ValidateClient(notChain(16), -1, -1))
	require.Error(t, ValidateClient(notChain(16), 0, 0))
}

// TestValidateClientLimitsAreIndependent checks that each limit applies on its own when the other one is not set.
func TestValidateClientLimitsAreIndependent(t *testing.T) {
	t.Parallel()
	require.Error(t, ValidateClient(notChain(10), 5, 0))
	require.NoError(t, ValidateClient(notChain(10), 0, 1))

	vals := make([]string, 10)
	for i := range vals {
		vals[i] = "v" + fmt.Sprint(i)
	}
	require.Error(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals}, 0, 5))
	require.NoError(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals}, 5, 0))
}

// TestValidateKeepsServerFilterLimits checks that Validate itself keeps no node limit on the filters the
// application sets on the server.
func TestValidateKeepsServerFilterLimits(t *testing.T) {
	t.Parallel()
	require.NoError(t, Validate(notChain(256)))
}
