package filter

import (
	"fmt"

	"github.com/centrifugal/protocol"
)

// Default limits of a client filter. They apply when a limit is zero.
const (
	DefaultClientFilterMaxNodes  = 128
	DefaultClientFilterMaxValues = 128
)

// maxClientFilterDepth is the deepest nesting of a client filter, the root being level 1. It is not configurable.
const maxClientFilterDepth = 16

// ValidateClient is Validate with the limits on the size of a filter that a client sends: at most maxNodes nodes
// of its tree and at most maxValues values of its comparisons. A zero limit means the default, and a negative limit
// turns that cap off. The depth of the tree is capped at maxClientFilterDepth either way. The filters the
// application sets on the server are checked by Validate only. The size is counted by a second walk of the tree.
func ValidateClient(f *protocol.FilterNode, maxNodes int, maxValues int) error {
	if err := Validate(f); err != nil {
		return err
	}
	if maxNodes == 0 {
		maxNodes = DefaultClientFilterMaxNodes
	}
	if maxValues == 0 {
		maxValues = DefaultClientFilterMaxValues
	}
	var nodes, values int
	if err := countFilter(f, 1, &nodes, &values); err != nil {
		return err
	}
	if maxNodes > 0 && nodes > maxNodes {
		return fmt.Errorf("filter has more than %d nodes", maxNodes)
	}
	if maxValues > 0 && values > maxValues {
		return fmt.Errorf("filter has more than %d values", maxValues)
	}
	return nil
}

// countFilter counts the nodes of the tree and the values of its comparisons, and refuses a tree nested deeper than
// maxClientFilterDepth. The depth is the level of f in the tree, the root being level 1. The tree is valid, see Validate.
func countFilter(f *protocol.FilterNode, depth int, nodes *int, values *int) error {
	if depth > maxClientFilterDepth {
		return fmt.Errorf("filter is nested more than %d levels deep", maxClientFilterDepth)
	}
	*nodes++
	*values += len(f.Vals)
	for _, child := range f.Nodes {
		if err := countFilter(child, depth+1, nodes, values); err != nil {
			return err
		}
	}
	return nil
}
