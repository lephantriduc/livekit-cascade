package prometheus

import "github.com/prometheus/client_golang/prometheus"

var (
	CascadeRelayTracksActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "cascade_relay_tracks_active", Help: "Active relayed tracks"},
		[]string{"from_sfu", "to_sfu"},
	)
	CascadeGossipSentTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "cascade_gossip_sent_total", Help: "Gossip messages sent"},
		[]string{"peer"},
	)
	CascadeLinkRTT = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cascade_link_rtt_ms",
			Help:    "Round-trip time to peer SFU in milliseconds",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500},
		},
		[]string{"peer"},
	)
	CascadeElectionsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "cascade_elections_total", Help: "Hub elections triggered"},
	)
)

func initCascadeStats() {
	prometheus.MustRegister(
		CascadeRelayTracksActive,
		CascadeGossipSentTotal,
		CascadeLinkRTT,
		CascadeElectionsTotal,
	)
}
