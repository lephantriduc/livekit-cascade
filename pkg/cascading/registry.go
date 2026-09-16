package cascading

import "context"

type PeerEvent struct {
	NodeID string
	Joined bool
}

type PeerRegistry interface {
	Discover(ctx context.Context) ([]*SFUPeerInfo, error)
	Watch(ctx context.Context) (<-chan PeerEvent, error)
}

// Reads peer registry from startup (Hardcoded for demo)
type StaticPeerRegistry struct {
	peers []*SFUPeerInfo
}

func NewStaticRegistry(peers []*SFUPeerInfo) *StaticPeerRegistry {
	return &StaticPeerRegistry{peers: peers}
}

// Get all peers/neighbors of a node
func (reg *StaticPeerRegistry) Discover(ctx context.Context) ([]*SFUPeerInfo, error) {
	return reg.peers, nil
}

// Watch for joining/leaving node
// However, we are using a static list, so no such events...for now
func (reg *StaticPeerRegistry) Watch(ctx context.Context) (<-chan PeerEvent, error) {
	ch := make(chan PeerEvent)
	close(ch)
	return ch, nil
}
