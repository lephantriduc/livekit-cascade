package cascading

import (
	"encoding/json"
	"sync"

	"github.com/livekit/protocol/livekit"
)

// Manager coordinates cascade announcements. Media-track attachment is kept
// behind the callback because it belongs to the room's track lifecycle.
type Manager struct {
	selfID string
	gossip GossipEngine

	mu      sync.RWMutex
	onTrack func(TrackPublishedGossip)
}

func NewManager(selfID string, gossip GossipEngine) *Manager {
	m := &Manager{selfID: selfID, gossip: gossip}
	if gossip != nil {
		gossip.OnReceive(func(_ string, payload []byte) {
			var msg TrackPublishedGossip
			if json.Unmarshal(payload, &msg) == nil {
				m.OnGossipTrackPublished(msg)
			}
		})
	}
	return m
}

func (m *Manager) OnTrackPublished(fn func(TrackPublishedGossip)) {
	m.mu.Lock()
	m.onTrack = fn
	m.mu.Unlock()
}

func (m *Manager) AnnounceTrackPublished(track *livekit.TrackInfo) error {
	if m.gossip == nil || track == nil {
		return nil
	}
	payload, err := json.Marshal(TrackPublishedGossip{
		OriginSFUID: m.selfID,
		Track:       track,
	})
	if err != nil {
		return err
	}
	return m.gossip.Broadcast(payload)
}

func (m *Manager) OnGossipTrackPublished(msg TrackPublishedGossip) {
	if msg.OriginSFUID == m.selfID || msg.OriginSFUID == msg.DestSFUID || msg.Track == nil {
		return
	}
	m.mu.RLock()
	onTrack := m.onTrack
	m.mu.RUnlock()
	if onTrack != nil {
		onTrack(msg)
	}
}
