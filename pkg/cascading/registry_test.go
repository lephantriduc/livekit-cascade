package cascading

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStaticRegistry(t *testing.T) {
	reg := NewStaticRegistry([]*SFUPeerInfo{
		{NodeID: "sfu-a", Addr: "sfu-a:7880"},
		{NodeID: "sfu-b", Addr: "sfu-b:7880"},
	})

	peers, err := reg.Discover(context.Background())
	require.NoError(t, err)
	require.Len(t, peers, 2)
	assert.Equal(t, "sfu-a", peers[0].NodeID)
}
