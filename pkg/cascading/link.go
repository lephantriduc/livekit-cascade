package cascading

import (
	"encoding/json"
	"fmt"
	"sync"

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

// OnControl safely registers the message callback
func (link *SFULink) OnControl(handler func(CascadeControlMsg)) {
	link.mu.Lock()
	defer link.mu.Unlock()
	link.onControl = handler
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
