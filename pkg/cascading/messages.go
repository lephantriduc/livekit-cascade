package cascading

type SFUPeerInfo struct {
	NodeID string
	IP     string
	Addr   string             // host:port
	PingMs map[string]float64 // Stores ping to nearby neighbors
}

type TrackRelayInfo struct {
	TrackID       string
	ParticipantID string
}

type CascadeControlMsgType string

const (
	MsgRelayTrack          CascadeControlMsgType = "RELAY_TRACK"
	MsgStopRelay           CascadeControlMsgType = "STOP_RELAY"
	MsgSubscriptionRequest CascadeControlMsgType = "SUBSCRIPTION_REQUEST"
)

type CascadeControlMsg struct {
	Type    CascadeControlMsgType
	Payload []byte // JSON-encoded
	Version int
}

// For gossiping
type GossipMsg struct {
	MsgID    string
	SenderID string
	TTL      int
	Payload  []byte
}

// Plumtree
type IHaveMsg struct{ MsgID string }
type GraftMsg struct{ MsgID string }
type PruneMsg struct{ MsgID string }

// Bully algorithm
type ElectionMsg struct {
	SenderID string
	Score    float64
	Epoch    int64
}

type AnswerMsg struct {
	SenderID string
}

type CoordinatorMsg struct {
}
