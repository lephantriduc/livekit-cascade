package cascading

import (
	"testing"
	"time"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

func TestCascadeSenderWritesRTP(t *testing.T) {
	pcA, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pcA.Close()
	pcB, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pcB.Close()

	localTrack, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeVP8,
		ClockRate: 90000,
	}, "video", "pion")
	require.NoError(t, err)
	rtpSender, err := pcA.AddTrack(localTrack)
	require.NoError(t, err)
	require.NotNil(t, rtpSender)

	received := make(chan []byte, 1)
	pcB.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		go func() {
			packet, _, err := track.ReadRTP()
			if err == nil {
				rawPacket, err := packet.Marshal()
				if err == nil {
					received <- rawPacket
				}
			}
		}()
	})

	connected := make(chan struct{})
	pcA.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			select {
			case <-connected:
			default:
				close(connected)
			}
		}
	})

	offer, err := pcA.CreateOffer(nil)
	require.NoError(t, err)
	require.NoError(t, pcA.SetLocalDescription(offer))
	<-webrtc.GatheringCompletePromise(pcA)
	require.NoError(t, pcB.SetRemoteDescription(*pcA.LocalDescription()))

	answer, err := pcB.CreateAnswer(nil)
	require.NoError(t, err)
	require.NoError(t, pcB.SetLocalDescription(answer))
	<-webrtc.GatheringCompletePromise(pcB)
	require.NoError(t, pcA.SetRemoteDescription(*pcB.LocalDescription()))

	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("peer connections did not become connected")
	}

	packet := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    96,
			SequenceNumber: 42,
			Timestamp:      90000,
			SSRC:           1234,
		},
		Payload: []byte("cascade RTP payload"),
	}
	rawPacket, err := packet.Marshal()
	require.NoError(t, err)

	sender := &CascadeSender{trackLocal: localTrack}
	require.Equal(t, int32(len(rawPacket)), sender.WriteRTP(&buffer.ExtPacket{RawPacket: rawPacket}, 0))

	select {
	case got := <-received:
		var receivedPacket rtp.Packet
		require.NoError(t, receivedPacket.Unmarshal(got))
		require.Equal(t, packet.Payload, receivedPacket.Payload)
		require.Equal(t, packet.PayloadType, receivedPacket.PayloadType)
		require.Equal(t, packet.SequenceNumber, receivedPacket.SequenceNumber)
		require.Equal(t, packet.Timestamp, receivedPacket.Timestamp)
	case <-time.After(5 * time.Second):
		t.Fatal("RTP packet did not arrive at the receiving peer")
	}
}
