package filter

import (
	"errors"
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
// in the whole tree and at most maxValues values in each comparison. A zero limit means the default, and a negative
// limit turns that cap off. The depth of the tree is capped at maxClientFilterDepth either way. The size is checked
// before Validate walks the tree, so the caps bound the work done on a filter from a client. The filters the
// application sets on the server are checked by Validate only.
func ValidateClient(f *protocol.FilterNode, maxNodes int, maxValues int) error {
	if maxNodes == 0 {
		maxNodes = DefaultClientFilterMaxNodes
	}
	if maxValues == 0 {
		maxValues = DefaultClientFilterMaxValues
	}
	var nodes int
	if err := countFilter(f, 1, maxNodes, maxValues, &nodes); err != nil {
		return err
	}
	return Validate(f)
}

// countFilter walks the tree and refuses it when it is nested deeper than maxClientFilterDepth, has more than maxNodes
// nodes, or has a comparison with more than maxValues values. A negative limit is not checked. The walk stops at the
// first node over a node limit. The depth is the level of f in the tree, the root being level 1. A nil node is refused
// here, before Validate, so the walk never dereferences it.
func countFilter(f *protocol.FilterNode, depth int, maxNodes int, maxValues int, nodes *int) error {
	if depth > maxClientFilterDepth {
		return fmt.Errorf("filter is nested more than %d levels deep", maxClientFilterDepth)
	}
	if f == nil {
		return errors.New("filter node must not be nil")
	}
	*nodes++
	if maxNodes > 0 && *nodes > maxNodes {
		return fmt.Errorf("filter has more than %d nodes", maxNodes)
	}
	if maxValues > 0 && len(f.Vals) > maxValues {
		return fmt.Errorf("filter has more than %d values in a comparison", maxValues)
	}
	for _, child := range f.Nodes {
		if err := countFilter(child, depth+1, maxNodes, maxValues, nodes); err != nil {
			return err
		}
	}
	return nil
}
