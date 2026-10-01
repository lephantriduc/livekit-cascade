package cascading

import (
	"sync"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
)

type GossipEngine interface {
	Broadcast(payload []byte) error
	OnReceive(func(senderID string, payload []byte))
}

type gossipBase struct {
	mu   sync.Mutex
	seen *lru.Cache[string, struct{}]
}

func newGossipBase() gossipBase {
	cache, _ := lru.New[string, struct{}](10_000)
	return gossipBase{seen: cache}
}

func (g *gossipBase) isSeen(msgID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.seen.Get(msgID)
	return ok
}

func (g *gossipBase) markSeen(msgID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.seen.Add(msgID, struct{}{})
}

type MeshGossipEngine struct {
	gossipBase
	selfID string
	links  *LinkManager

	mu     sync.RWMutex
	onRecv func(senderID string, payload []byte)
}

func NewMeshGossipEngine(selfID string, links *LinkManager) *MeshGossipEngine {
	m := &MeshGossipEngine{
		gossipBase: newGossipBase(),
		selfID:     selfID,
		links:      links,
	}
	links.OnLink(func(link *SFULink) {
		link.OnGossip(func(msg GossipMsg) {
			m.HandleIncoming(link.RemoteID, msg)
		})
	})
	return m
}

func (m *MeshGossipEngine) OnReceive(fn func(senderID string, payload []byte)) {
	m.mu.Lock()
	m.onRecv = fn
	m.mu.Unlock()
}

func (m *MeshGossipEngine) Broadcast(payload []byte) error {
	msg := GossipMsg{
		MsgID:    uuid.NewString(),
		SenderID: m.selfID,
		Payload:  append([]byte(nil), payload...),
	}
	m.markSeen(msg.MsgID)
	var firstErr error
	for _, link := range m.links.All() {
		if err := link.SendGossip(msg); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *MeshGossipEngine) HandleIncoming(senderID string, msg GossipMsg) {
	if m.isSeen(msg.MsgID) {
		return
	}
	m.markSeen(msg.MsgID)

	m.mu.RLock()
	onRecv := m.onRecv
	m.mu.RUnlock()
	if onRecv != nil {
		onRecv(senderID, msg.Payload)
	}
	for _, link := range m.links.All() {
		if link.RemoteID != senderID {
			_ = link.SendGossip(msg)
		}
	}
}
