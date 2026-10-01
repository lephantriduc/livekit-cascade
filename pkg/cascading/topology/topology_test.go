package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMesh(t *testing.T) {
	assert.Equal(t, []string{"b", "c"}, (Mesh{}).Neighbors("a", []string{"a", "b", "c"}))
}

func TestHubSpoke(t *testing.T) {
	all := []string{"hub", "a", "b"}
	assert.Equal(t, []string{"a", "b"}, (HubSpoke{HubID: "hub"}).Neighbors("hub", all))
	assert.Equal(t, []string{"hub"}, (HubSpoke{HubID: "hub"}).Neighbors("a", all))
}

func TestRing4Nodes(t *testing.T) {
	ring := Ring{}
	all := []string{"a", "b", "c", "d"}
	assert.Equal(t, []string{"d", "b"}, ring.Neighbors("a", all))
	assert.Equal(t, []string{"a", "c"}, ring.Neighbors("b", all))
}

func TestLine(t *testing.T) {
	line := Line{}
	all := []string{"a", "b", "c", "d"}
	assert.Equal(t, []string{"b"}, line.Neighbors("a", all))
	assert.Equal(t, []string{"b"}, line.Neighbors("c", []string{"a", "b", "c"}))
	assert.Equal(t, []string{"c"}, line.Neighbors("d", all))
}

func TestCustom(t *testing.T) {
	custom := Custom{Edges: map[string][]string{
		"a": {"b", "c", "c", "missing", "a"},
	}}
	assert.Equal(t, []string{"b", "c"}, custom.Neighbors("a", []string{"a", "b", "c"}))
}
