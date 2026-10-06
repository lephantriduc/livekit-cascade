package service

import (
	"testing"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNewCascadeRuntime(t *testing.T) {
	runtime, err := newCascadeRuntime(&config.Config{
		Cascade: config.CascadeConfig{
			Enabled:  true,
			SelfID:   "sfu-a",
			Topology: "mesh",
			Peers: []config.PeerConfig{
				{ID: "sfu-a", Addr: "sfu-a:7880"},
				{ID: "sfu-b", Addr: "sfu-b:7880"},
				{ID: "sfu-c", Addr: "sfu-c:7880"},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, runtime)
	require.Equal(t, []string{"sfu-b", "sfu-c"}, runtime.neighbors)
	require.Equal(t, []string{}, runtime.linkManager.AllIDs())
}

func TestNewCascadeRuntimeValidation(t *testing.T) {
	_, err := newCascadeRuntime(&config.Config{
		Cascade: config.CascadeConfig{
			Enabled:  true,
			SelfID:   "sfu-a",
			Topology: "unsupported",
		},
	})
	require.EqualError(t, err, "cascade.peers must not be empty when cascade is enabled")

	_, err = newCascadeRuntime(&config.Config{
		Cascade: config.CascadeConfig{
			Enabled:  true,
			SelfID:   "sfu-a",
			Topology: "unsupported",
			Peers:    []config.PeerConfig{{ID: "sfu-b", Addr: "sfu-b:7880"}},
		},
	})
	require.EqualError(t, err, "unsupported cascade topology \"unsupported\"")
}
