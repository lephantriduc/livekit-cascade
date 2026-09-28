package cascading

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"github.com/pion/webrtc/v4"
)

// Every node has a LinkManager
// It maintains a map of neighbor -> SFULink
type LinkManager struct {
	selfID    string
	signaling SignalingTransport

	mu    sync.RWMutex
	links map[string]*SFULink
}

func NewLinkManager(selfID string, signaling SignalingTransport) *LinkManager {
	return &LinkManager{
		selfID:    selfID,
		signaling: signaling,
		links:     make(map[string]*SFULink),
	}
}

// GetLink safely returns an existing SFULink by remote node ID.
func (lm *LinkManager) GetLink(nodeID string) *SFULink {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.links[nodeID]
}

// If lower ID -> offer, higher ID -> answerer
func (lm *LinkManager) EnsureLink(ctx context.Context, peer *SFUPeerInfo) (*SFULink, error) {
	lm.mu.RLock()
	if link, ok := lm.links[peer.NodeID]; ok {
		lm.mu.RUnlock()
		return link, nil
	}
	lm.mu.RUnlock()

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return nil, fmt.Errorf("creating peer connection: %w", err)
	}

	link := &SFULink{
		LocalID:  lm.selfID,
		RemoteID: peer.NodeID,
		pc:       pc,
	}

	lm.mu.Lock()
	lm.links[peer.NodeID] = link
	lm.mu.Unlock()

	if lm.selfID < peer.NodeID {
		err := lm.setUpAsOfferer(ctx, link, peer)
		if err != nil {
			lm.mu.Lock()
			delete(lm.links, peer.NodeID)
			lm.mu.Unlock()
			pc.Close()
			return nil, err
		}
	}

	return link, nil
}

func (lm *LinkManager) setUpAsOfferer(ctx context.Context, link *SFULink, peer *SFUPeerInfo) error {
	pc := link.pc

	if err := link.InitControlChannel(); err != nil {
		return fmt.Errorf("init control channel: %w", err)
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("create offer: %w", err)
	}

	if err = pc.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("set local description: %w", err)
	}

	<-webrtc.GatheringCompletePromise(pc)

	// Send pc.LocalDescription().SDP with gathered ICE candidates (not raw offer.SDP)
	answerSDP, err := lm.signaling.SendOffer(ctx, lm.selfID, peer.NodeID, peer.Addr, pc.LocalDescription().SDP)
	if err != nil {
		return fmt.Errorf("send offer to %s: %w", peer.NodeID, err)
	}

	err = pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answerSDP,
	})
	if err != nil {
		return fmt.Errorf("set remote description: %w", err)
	}

	return nil
}

// Register endpoint cascade/signal to answer incoming offers
func (lm *LinkManager) RegisterHTTPHandler(mux *http.ServeMux) {
	mux.HandleFunc("/cascade/signal", func(w http.ResponseWriter, r *http.Request) {
		offer, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		offererID := r.Header.Get("Node-ID")
		if offererID == "" {
			http.Error(w, "empty offerer id", http.StatusBadRequest)
			return
		}

		answerSDP, err := lm.handleOffer(r.Context(), offererID, string(offer))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/sdp")
		w.Write([]byte(answerSDP))
	})
}

func (lm *LinkManager) handleOffer(ctx context.Context, offererID string, offerSDP string) (string, error) {
	lm.mu.RLock()
	link, ok := lm.links[offererID]
	lm.mu.RUnlock()
	if !ok {
		pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			return "", err
		}
		link = &SFULink{
			LocalID:  lm.selfID,
			RemoteID: offererID,
			pc:       pc,
		}
		lm.mu.Lock()
		lm.links[offererID] = link
		lm.mu.Unlock()
	}

	if err := link.InitControlChannel(); err != nil {
		return "", err
	}

	pc := link.pc

	slog.Info(fmt.Sprintf("Setting remote (%v) description", offererID))
	err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	})
	if err != nil {
		return "", err
	}
	slog.Info(fmt.Sprintf("Set remote (%v) description successfully!", offererID))

	answerSDP, err := pc.CreateAnswer(nil)
	if err != nil {
		return "", err
	}

	if err = pc.SetLocalDescription(answerSDP); err != nil {
		return "", err
	}

	<-webrtc.GatheringCompletePromise(pc)
	return pc.LocalDescription().SDP, nil
}
