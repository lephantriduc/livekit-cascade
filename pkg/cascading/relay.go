package cascading

import (
	"fmt"
	"sync"

	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/protocol/livekit"
)

// RelayManager owns the relay sender for each track and destination peer.
type RelayManager struct {
	selfID string
	links  *LinkManager

	mu     sync.Mutex
	relays map[string]map[string]*CascadeSender
}

func NewRelayManager(selfID string, links *LinkManager) *RelayManager {
	return &RelayManager{
		selfID: selfID,
		links:  links,
		relays: make(map[string]map[string]*CascadeSender),
	}
}

func (rm *RelayManager) RelayTrack(trackID livekit.TrackID, dstPeerID string, localReceiver sfu.TrackReceiver) error {
	link := rm.links.GetLink(dstPeerID)
	if link == nil {
		return fmt.Errorf("no link to peer %s", dstPeerID)
	}

	trackLocal, rtpSender, err := link.AddTrack(localReceiver.Codec(), trackID)
	if err != nil {
		return fmt.Errorf("add relay track: %w", err)
	}
	sender := &CascadeSender{
		trackID:    trackID,
		peerID:     dstPeerID,
		link:       link,
		trackLocal: trackLocal,
		rtpSender:  rtpSender,
	}
	if err := localReceiver.AddDownTrack(sender); err != nil {
		_ = link.RemoveTrack(rtpSender)
		return fmt.Errorf("add down track: %w", err)
	}

	rm.mu.Lock()
	if rm.relays[string(trackID)] == nil {
		rm.relays[string(trackID)] = make(map[string]*CascadeSender)
	}
	previous := rm.relays[string(trackID)][dstPeerID]
	rm.relays[string(trackID)][dstPeerID] = sender
	rm.mu.Unlock()
	if previous != nil {
		previous.Close()
	}
	return nil
}

func (rm *RelayManager) StopRelay(trackID livekit.TrackID, dstPeerID string) {
	rm.mu.Lock()
	peers := rm.relays[string(trackID)]
	sender := peers[dstPeerID]
	delete(peers, dstPeerID)
	if len(peers) == 0 {
		delete(rm.relays, string(trackID))
	}
	rm.mu.Unlock()
	if sender != nil {
		sender.Close()
	}
}

func (rm *RelayManager) ActiveCount() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	count := 0
	for _, peers := range rm.relays {
		count += len(peers)
	}
	return count
}
