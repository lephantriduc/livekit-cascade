package cascading

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/pion/webrtc/v4"
)

// A wrapper around webrtc.PeerConnection
type SFULink struct {
	LocalID  string
	RemoteID string
	Addr     string

	conn        *webrtc.PeerConnection
	controlChan *webrtc.DataChannel

	onControl func(CascadeControlMsg)
}

func (link *SFULink) SendControl(msg CascadeControlMsg) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return link.controlChan.Send(data)
}

func (link *SFULink) InitControlChannel() error {
	slog.Info(fmt.Sprintf("Init control channel, local: %v, remote: %v", link.LocalID, link.RemoteID))
	id := uint16(0)
	dc, err := link.conn.CreateDataChannel("control", &webrtc.DataChannelInit{
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
		if err := json.Unmarshal(msg.Data, &ctrl); err == nil && link.onControl != nil {
			link.onControl(ctrl)
		}
	})

	return nil
}

func (link *SFULink) close() error {
	return link.conn.Close()
}
