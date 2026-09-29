package cascading

import (
	"github.com/livekit/livekit-server/pkg/sfu"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/protocol/livekit"
	"github.com/pion/webrtc/v4"
)

// CascadeSender implements TrackSender in pkg/sfu/interfaces.go
// It acts like a DownTrack that routes the corresponding track to the subscriber.
// Except that it doesn't manage the dynamic resolution switching based on
// subscriber's internet speed.
// (Since the media is relayed between servers, so decent bandwidth and internet
// speed is presumed ;-) )
type CascadeSender struct {
	trackID    livekit.TrackID
	peerID     string // The remote SFU's ID
	link       *SFULink
	trackLocal *webrtc.TrackLocalStaticRTP
}

// Write an RTP Packet to CascadeSender's RTPSender
func (sender *CascadeSender) WriteRTP(p *buffer.ExtPacket, layer int32) int32 {
	n, err := sender.trackLocal.Write(p.RawPacket)
	if err != nil {
		return 0
	}
	return int32(n)
}

func (sender *CascadeSender) SubscriberID() livekit.ParticipantID {
	return livekit.ParticipantID(sender.peerID)
}

func (sender *CascadeSender) UpTrackLayersChange()                                                {}
func (sender *CascadeSender) UpTrackBitrateAvailabilityChange()                                   {}
func (sender *CascadeSender) UpTrackMaxPublishedLayerChange(maxPublishedLayer int32)              {}
func (sender *CascadeSender) UpTrackMaxTemporalLayerSeenChange(maxTemporalLayerSeen int32)        {}
func (sender *CascadeSender) UpTrackBitrateReport(availableLayers []int32, bitrates sfu.Bitrates) {}
func (sender *CascadeSender) Close()                                                              {}
func (sender *CascadeSender) IsClosed() bool                                                      { return false }
func (sender *CascadeSender) ID() string                                                          { return "" }
func (sender *CascadeSender) Resync()                                                             {}
func (sender *CascadeSender) SetReceiver(CascadeSender)                                           {}
func (sender *CascadeSender) ReceiverRestart(CascadeSender)                                       {}
