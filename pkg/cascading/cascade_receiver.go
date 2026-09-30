package cascading

import (
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils/mono"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type CascadeReceiver struct {
	*sfu.ReceiverBase
	track     *webrtc.TrackRemote
	trackInfo *livekit.TrackInfo

	onPLI func(layer int32)
}

func NewCascadeReceiver(track *webrtc.TrackRemote, trackInfo *livekit.TrackInfo, onPLI func(layer int32)) *CascadeReceiver {
	codec := track.Codec()
	r := &CascadeReceiver{
		track:     track,
		trackInfo: trackInfo,
		onPLI:     onPLI,
	}
	r.ReceiverBase = sfu.NewReceiverBase(sfu.ReceiverBaseParams{
		TrackID:       livekit.TrackID(track.ID()),
		StreamID:      track.StreamID(),
		Kind:          track.Kind(),
		Codec:         codec,
		Logger:        logger.GetLogger(),
		IsSelfClosing: true,
		OnNewBufferNeeded: func(int32, *livekit.TrackInfo) (buffer.BufferProvider, error) {
			return newCascadeBuffer(track, codec), nil
		},
	}, trackInfo, sfu.ReceiverCodecStateNormal)

	buff := newCascadeBuffer(track, codec)
	r.AddBuffer(buff, 0)
	r.StartBuffer(buff, 0)
	return r
}

func (r *CascadeReceiver) Start() {
	r.readLoop()
}

func (r *CascadeReceiver) readLoop() {
	buf := make([]byte, 1500)
	rtpPacket := &rtp.Packet{}
	for {
		n, _, err := r.track.Read(buf)
		if err != nil {
			return
		}
		if err := rtpPacket.Unmarshal(buf[:n]); err != nil {
			continue
		}

		incoming := r.GetAllBuffers()[0]
		cascadeBuffer, ok := incoming.(*buffer.Buffer)
		if !ok {
			return
		}
		_, _ = cascadeBuffer.HandleIncomingPacket(buf[:n], rtpPacket, mono.UnixNano(), false, false, nil, 0)
	}
}

func (r *CascadeReceiver) SendPLI(layer int32, force bool) {
	if r.onPLI != nil {
		r.onPLI(layer)
	}
}

func newCascadeBuffer(track *webrtc.TrackRemote, codec webrtc.RTPCodecParameters) *buffer.Buffer {
	buff := buffer.NewBuffer(uint32(track.SSRC()), buffer.InitPacketBufferSizeVideo, buffer.InitPacketBufferSizeAudio)
	_ = buff.Bind(
		webrtc.RTPParameters{Codecs: []webrtc.RTPCodecParameters{codec}},
		codec.RTPCodecCapability,
		0,
	)
	return buff
}
