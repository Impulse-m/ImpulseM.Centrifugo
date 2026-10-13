package proxyproto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterNodeFromProto(t *testing.T) {
	require.Nil(t, FilterNodeFromProto(nil))

	in := &FilterNode{
		Op: "and",
		Nodes: []*FilterNode{
			{Key: "type", Cmp: "eq", Val: "a"},
			{Op: "not", Nodes: []*FilterNode{{Key: "tier", Cmp: "in", Vals: []string{"x", "y"}}}},
		},
	}
	out := FilterNodeFromProto(in)
	require.Equal(t, "and", out.Op)
	require.Len(t, out.Nodes, 2)
	require.Equal(t, "type", out.Nodes[0].Key)
	require.Equal(t, "eq", out.Nodes[0].Cmp)
	require.Equal(t, "a", out.Nodes[0].Val)
	require.Nil(t, out.Nodes[0].Nodes)
	require.Equal(t, "not", out.Nodes[1].Op)
	require.Len(t, out.Nodes[1].Nodes, 1)
	require.Equal(t, []string{"x", "y"}, out.Nodes[1].Nodes[0].Vals)
}

func TestSubscribeOptionsServerTagsFilterJSON(t *testing.T) {
	var opts SubscribeOptions
	err := json.Unmarshal([]byte(`{"server_tags_filter": {"op": "or", "nodes": [{"key": "k", "cmp": "eq", "val": "v"}]}}`), &opts)
	require.NoError(t, err)
	require.Equal(t, "or", opts.ServerTagsFilter.Op)
	require.Equal(t, "k", opts.ServerTagsFilter.Nodes[0].Key)
}
