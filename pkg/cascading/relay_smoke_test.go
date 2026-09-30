package cascading

import (
	"testing"
	"time"

	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/protocol/livekit"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

type relayProbe struct {
	sfu.TrackSender
	packets chan []byte
}

func (p *relayProbe) SubscriberID() livekit.ParticipantID { return "relay-probe" }

func (p *relayProbe) UpTrackMaxPublishedLayerChange(int32) {}

func (p *relayProbe) UpTrackMaxTemporalLayerSeenChange(int32) {}

func (p *relayProbe) WriteRTP(packet *buffer.ExtPacket, _ int32) int32 {
	copyOfPacket := append([]byte(nil), packet.RawPacket...)
	p.packets <- copyOfPacket
	return int32(len(copyOfPacket))
}

func TestRelayRTPEndToEnd(t *testing.T) {
	pcA, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pcA.Close()
	pcB, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pcB.Close()

	localTrack, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeVP8,
		ClockRate: 90000,
	}, "track", "cascade")
	require.NoError(t, err)
	_, err = pcA.AddTrack(localTrack)
	require.NoError(t, err)

	received := make(chan []byte, 1)
	receiverReady := make(chan *CascadeReceiver, 1)
	pcB.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		receiver := NewCascadeReceiver(track, &livekit.TrackInfo{
			Sid:  "track",
			Type: livekit.TrackType_VIDEO,
		}, nil)
		probe := &relayProbe{packets: received}
		require.NoError(t, receiver.AddDownTrack(probe))
		receiverReady <- receiver
		go receiver.Start()
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
	case <-receiverReady:
	case <-time.After(5 * time.Second):
		t.Fatal("receiver track was not created")
	}

	select {
	case got := <-received:
		var receivedPacket rtp.Packet
		require.NoError(t, receivedPacket.Unmarshal(got))
		require.Equal(t, packet.Payload, receivedPacket.Payload)
		require.Equal(t, packet.PayloadType, receivedPacket.PayloadType)
		require.Equal(t, packet.SequenceNumber, receivedPacket.SequenceNumber)
		require.Equal(t, packet.Timestamp, receivedPacket.Timestamp)
	case <-time.After(5 * time.Second):
		t.Fatal("RTP packet did not arrive at CascadeReceiver")
	}
}
