package cascading

import (
	"sync"
	"time"
)

const electionAnswerTimeout = 3 * time.Second

type EpochStore interface {
	NextEpoch() (int64, error)
	CurrentEpoch() int64
}

type ElectionTransport interface {
	SendElection(peerID string, msg ElectionMsg) error
	SendAnswer(peerID string, msg AnswerMsg) error
	BroadcastCoordinator(msg CoordinatorMsg) error
}

type BullyElection struct {
	selfID    string
	score     float64
	peers     []string
	transport ElectionTransport
	epochs    EpochStore

	mu          sync.Mutex
	epoch       int64
	coordinator string
	election    bool
	answers     chan struct{}
	randBackoff func() time.Duration
}

func NewBullyElection(selfID string, score float64, peers []string, transport ElectionTransport, epochs EpochStore) *BullyElection {
	return &BullyElection{
		selfID:    selfID,
		score:     score,
		peers:     append([]string(nil), peers...),
		transport: transport,
		epochs:    epochs,
		epoch:     epochs.CurrentEpoch(),
		randBackoff: func() time.Duration {
			return time.Duration(time.Now().UnixNano() % int64(2*time.Second+1))
		},
	}
}

func (b *BullyElection) Coordinator() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.coordinator
}

func (b *BullyElection) StartElection() {
	go func() {
		time.Sleep(b.randBackoff())
		b.startElection()
	}()
}

func (b *BullyElection) startElection() {
	epoch, err := b.epochs.NextEpoch()
	if err != nil {
		return
	}

	b.mu.Lock()
	if b.election {
		b.mu.Unlock()
		return
	}
	b.election = true
	b.epoch = epoch
	b.answers = make(chan struct{}, 1)
	answers := b.answers
	b.mu.Unlock()

	msg := ElectionMsg{SenderID: b.selfID, Score: b.score, Epoch: epoch}
	for _, peerID := range b.peers {
		if err := b.transport.SendElection(peerID, msg); err != nil {
			continue
		}
	}

	timer := time.NewTimer(electionAnswerTimeout)
	defer timer.Stop()
	select {
	case <-answers:
	case <-timer.C:
		b.declareCoordinator(epoch)
	}

	b.mu.Lock()
	b.election = false
	b.mu.Unlock()
}

func (b *BullyElection) HandleElection(senderID string, msg ElectionMsg) {
	b.mu.Lock()
	if msg.Epoch < b.epoch {
		b.mu.Unlock()
		return
	}
	if msg.Epoch > b.epoch {
		b.epoch = msg.Epoch
	}
	b.mu.Unlock()

	if msg.Score < b.score {
		_ = b.transport.SendAnswer(senderID, AnswerMsg{SenderID: b.selfID})
		b.StartElection()
	}
}

func (b *BullyElection) HandleAnswer(msg AnswerMsg) {
	b.mu.Lock()
	answers := b.answers
	b.mu.Unlock()
	if answers != nil {
		select {
		case answers <- struct{}{}:
		default:
		}
	}
}

func (b *BullyElection) HandleCoordinator(msg CoordinatorMsg) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if msg.Epoch < b.epoch {
		return
	}
	b.epoch = msg.Epoch
	b.coordinator = msg.HubID
	b.election = false
}

func (b *BullyElection) declareCoordinator(epoch int64) {
	if err := b.transport.BroadcastCoordinator(CoordinatorMsg{HubID: b.selfID, Epoch: epoch}); err != nil {
		return
	}
	b.mu.Lock()
	b.coordinator = b.selfID
	b.epoch = epoch
	b.mu.Unlock()
}

// Score favors high bandwidth and penalizes latency and CPU load.
func Score(pingMs, cpuLoad, availableBandwidth float64) float64 {
	if pingMs < 0 || cpuLoad < 0 || availableBandwidth <= 0 {
		return 0
	}
	return availableBandwidth / (1 + pingMs + 100*cpuLoad)
}
