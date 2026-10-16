package centrifuge

import (
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestValidateFilterNode(t *testing.T) {
	tests := []struct {
		name    string
		node    *FilterNode
		wantErr bool
	}{
		{
			name:    "nil node",
			node:    nil,
			wantErr: true,
		},
		{
			name: "valid leaf",
			node: &protocol.FilterNode{Key: "a", Cmp: "eq", Val: "x"},
		},
		{
			name:    "in without vals",
			node:    &protocol.FilterNode{Key: "a", Cmp: "in"},
			wantErr: true,
		},
		{
			name:    "unknown cmp",
			node:    &protocol.FilterNode{Key: "a", Cmp: "equals", Val: "x"},
			wantErr: true,
		},
		{
			name: "not with two children",
			node: &protocol.FilterNode{
				Op: "not",
				Nodes: []*protocol.FilterNode{
					{Key: "a", Cmp: "eq", Val: "x"},
					{Key: "b", Cmp: "eq", Val: "y"},
				},
			},
			wantErr: true,
		},
		{
			name: "nested valid and/or tree",
			node: &protocol.FilterNode{
				Op: "and",
				Nodes: []*protocol.FilterNode{
					{Key: "a", Cmp: "eq", Val: "x"},
					{
						Op: "or",
						Nodes: []*protocol.FilterNode{
							{Key: "b", Cmp: "in", Vals: []string{"1", "2"}},
							{
								Op:    "not",
								Nodes: []*protocol.FilterNode{{Key: "c", Cmp: "neq", Val: "z"}},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateFilterNode(tt.node)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
