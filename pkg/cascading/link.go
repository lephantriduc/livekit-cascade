package cascading

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/livekit/protocol/livekit"
	"github.com/pion/webrtc/v4"
)

// A wrapper around webrtc.PeerConnection
type SFULink struct {
	LocalID  string
	RemoteID string
	Addr     string

	pc          *webrtc.PeerConnection
	controlChan *webrtc.DataChannel

	mu        sync.RWMutex
	onControl func(CascadeControlMsg)
	onGossip  func(GossipMsg)
}

func (link *SFULink) SendControl(msg CascadeControlMsg) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if link.controlChan == nil {
		return fmt.Errorf("control channel not initialized")
	}

	return link.controlChan.Send(data)
}

func (link *SFULink) SendGossip(msg GossipMsg) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return link.SendControl(CascadeControlMsg{
		Type:    MsgGossip,
		Payload: payload,
		Version: 1,
	})
}

func (link *SFULink) AddTrack(codec webrtc.RTPCodecParameters, trackID livekit.TrackID) (*webrtc.TrackLocalStaticRTP, *webrtc.RTPSender, error) {
	track, err := webrtc.NewTrackLocalStaticRTP(codec.RTPCodecCapability, string(trackID), link.LocalID)
	if err != nil {
		return nil, nil, err
	}
	rtpSender, err := link.pc.AddTrack(track)
	if err != nil {
		return nil, nil, err
	}
	return track, rtpSender, nil
}

func (link *SFULink) RemoveTrack(sender *webrtc.RTPSender) error {
	if sender == nil {
		return nil
	}
	return link.pc.RemoveTrack(sender)
}

// OnControl safely registers the message callback
func (link *SFULink) OnControl(handler func(CascadeControlMsg)) {
	link.mu.Lock()
	defer link.mu.Unlock()
	link.onControl = handler
}

func (link *SFULink) OnGossip(handler func(GossipMsg)) {
	link.mu.Lock()
	defer link.mu.Unlock()
	link.onGossip = handler
}

func (link *SFULink) InitControlChannel() error {
	id := uint16(0)
	dc, err := link.pc.CreateDataChannel("control", &webrtc.DataChannelInit{
		Negotiated: new(true),
		Ordered:    new(true),
		ID:         &id,
	})
	if err != nil {
		return err
	}

	link.controlChan = dc
	link.controlChan.OnMessage(func(msg webrtc.DataChannelMessage) {
		var ctrl CascadeControlMsg
		if err := json.Unmarshal(msg.Data, &ctrl); err != nil {
			return
		}
		if ctrl.Type == MsgGossip {
			var gossip GossipMsg
			if err := json.Unmarshal(ctrl.Payload, &gossip); err != nil {
				return
			}
			link.mu.RLock()
			handler := link.onGossip
			link.mu.RUnlock()
			if handler != nil {
				handler(gossip)
			}
			return
		}

		link.mu.RLock()
		handler := link.onControl
		link.mu.RUnlock()

		if handler != nil {
			handler(ctrl)
		}
	})

	return nil
}

func (link *SFULink) Close() error {
	return link.pc.Close()
}
