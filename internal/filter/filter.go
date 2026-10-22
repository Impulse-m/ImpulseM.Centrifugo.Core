package filter

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/centrifugal/centrifuge/internal/bpool"

	"github.com/centrifugal/protocol"
	"github.com/quagmt/udecimal"
)

// Node operations.
const (
	OpLeaf = "" // leaf node
	OpAnd  = "and"
	OpOr   = "or"
	OpNot  = "not"
)

// Leaf comparison operators.
const (
	CompareEQ         = "eq"
	CompareNotEQ      = "neq"
	CompareIn         = "in"
	CompareNotIn      = "nin"
	CompareExists     = "ex"
	CompareNotExists  = "nex"
	CompareStartsWith = "sw"
	CompareEndsWith   = "ew"
	CompareContains   = "ct"
	CompareGT         = "gt"
	CompareGTE        = "gte"
	CompareLT         = "lt"
	CompareLTE        = "lte"
)

// Match checks if the provided tags match the filter.
func Match(f *protocol.FilterNode, tags map[string]string) (bool, error) {
	if f == nil {
		// Validate refuses nil nodes; never panic on a filter which skipped it.
		return false, errors.New("filter node must not be nil")
	}
	switch f.Op {
	case OpLeaf:
		val, ok := tags[f.Key]
		switch f.Cmp {
		case CompareEQ:
			return ok && val == f.Val, nil
		case CompareNotEQ:
			return !ok || val != f.Val, nil
		case CompareIn:
			return slices.Contains(f.Vals, val), nil
		case CompareNotIn:
			return !slices.Contains(f.Vals, val), nil
		case CompareExists:
			return ok, nil
		case CompareNotExists:
			return !ok, nil
		case CompareStartsWith:
			return ok && strings.HasPrefix(val, f.Val), nil
		case CompareEndsWith:
			return ok && strings.HasSuffix(val, f.Val), nil
		case CompareContains:
			return ok && strings.Contains(val, f.Val), nil

		// numeric comparisons unified
		case CompareGT, CompareGTE, CompareLT, CompareLTE:
			if !ok {
				return false, nil
			}
			v, err := udecimal.Parse(val)
			if err != nil {
				return false, nil
			}
			cmp, err := udecimal.Parse(f.Val)
			if err != nil {
				return false, nil
			}
			switch f.Cmp {
			case CompareGT:
				return v.Cmp(cmp) > 0, nil
			case CompareGTE:
				return v.Cmp(cmp) >= 0, nil
			case CompareLT:
				return v.Cmp(cmp) < 0, nil
			case CompareLTE:
				return v.Cmp(cmp) <= 0, nil
			}
		default:
			return false, fmt.Errorf("invalid Compare value: %s", f.Cmp)
		}

	case OpAnd:
		for _, c := range f.Nodes {
			match, err := Match(c, tags)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
		return true, nil

	case OpOr:
		for _, c := range f.Nodes {
			match, err := Match(c, tags)
			if err != nil {
				return false, err
			}
			if match {
				return true, nil
			}
		}
		return false, nil

	case OpNot:
		if len(f.Nodes) != 1 {
			return false, errors.New("NOT must have exactly one child")
		}
		match, err := Match(f.Nodes[0], tags)
		if err != nil {
			return false, err
		}
		return !match, nil
	default:
	}
	return false, fmt.Errorf("invalid filter op: %s", f.Op)
}

// Validate ensures the filter tree is well-formed.
// Called at subscription time.
func Validate(f *protocol.FilterNode) error {
	if f == nil {
		return errors.New("filter node must not be nil")
	}
	switch f.Op {
	case OpLeaf:
		// Leaf must have a comparison operator.
		if f.Cmp == "" {
			return errors.New("leaf node must have cmp set")
		}

		switch f.Cmp {
		case CompareEQ, CompareNotEQ,
			CompareStartsWith, CompareEndsWith, CompareContains,
			CompareGT, CompareGTE, CompareLT, CompareLTE:
			if f.Val == "" {
				return fmt.Errorf("%s comparison requires Val", f.Cmp)
			}
			if len(f.Vals) > 0 {
				return fmt.Errorf("%s comparison must not use Vals", f.Cmp)
			}

		case CompareIn, CompareNotIn:
			if len(f.Vals) == 0 {
				return fmt.Errorf("%s comparison requires non-empty Vals", f.Cmp)
			}
			if f.Val != "" {
				return fmt.Errorf("%s comparison must not use Val", f.Cmp)
			}

		case CompareExists, CompareNotExists:
			if f.Val != "" || len(f.Vals) > 0 {
				return fmt.Errorf("%s comparison must not use Val or Vals", f.Cmp)
			}

		default:
			return fmt.Errorf("unknown comparison operator: %s", f.Cmp)
		}

		// All leafs must have a key except exists/nex
		if f.Key == "" &&
			f.Cmp != CompareExists &&
			f.Cmp != CompareNotExists {
			return errors.New("leaf node requires key")
		}

	case OpAnd, OpOr:
		if len(f.Nodes) == 0 {
			return fmt.Errorf("%s node must have at least one child", f.Op)
		}
		for _, c := range f.Nodes {
			if err := Validate(c); err != nil {
				return err
			}
		}

	case OpNot:
		if len(f.Nodes) != 1 {
			return errors.New("not node must have exactly one child")
		}
		return Validate(f.Nodes[0])

	default:
		return fmt.Errorf("invalid op: %s", f.Op)
	}
	return nil
}

// Limits of a filter that a client sends: the nodes of its tree and the values of its comparisons. The filter is
// matched for every publication of every subscriber, so a client may not send one that costs more than these.
const (
	MaxClientNodes  = 256
	MaxClientValues = 1024
)

// ValidateClient is Validate with the limits of MaxClientNodes and MaxClientValues. It checks the filters that
// clients send. The filters the application sets on the server are checked by Validate only.
func ValidateClient(f *protocol.FilterNode) error {
	if err := Validate(f); err != nil {
		return err
	}
	var nodes, values int
	countFilter(f, &nodes, &values)
	if nodes > MaxClientNodes {
		return fmt.Errorf("filter has more than %d nodes", MaxClientNodes)
	}
	if values > MaxClientValues {
		return fmt.Errorf("filter has more than %d values", MaxClientValues)
	}
	return nil
}

// countFilter counts the nodes of the tree and the values of its comparisons. The tree is valid, see Validate.
func countFilter(f *protocol.FilterNode, nodes *int, values *int) {
	*nodes++
	*values += len(f.Vals)
	for _, child := range f.Nodes {
		countFilter(child, nodes, values)
	}
}

// Hash computes filter hash.
func Hash(f *protocol.FilterNode) [32]byte {
	bb := bpool.GetByteBuffer(f.SizeCF())
	defer bpool.PutByteBuffer(bb)
	n, _ := f.MarshalToCF(bb.B)    // get canonical hash.
	return sha256.Sum256(bb.B[:n]) // SHA-256 hash.
}
