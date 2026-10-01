package cascading

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type electionTransport struct {
	elections    []string
	answers      []string
	coordinators []CoordinatorMsg
}

func (t *electionTransport) SendElection(peerID string, _ ElectionMsg) error {
	t.elections = append(t.elections, peerID)
	return nil
}
func (t *electionTransport) SendAnswer(peerID string, _ AnswerMsg) error {
	t.answers = append(t.answers, peerID)
	return nil
}
func (t *electionTransport) BroadcastCoordinator(msg CoordinatorMsg) error {
	t.coordinators = append(t.coordinators, msg)
	return nil
}

type epochStore struct{ epoch int64 }

func (s *epochStore) CurrentEpoch() int64 { return s.epoch }

func (s *epochStore) NextEpoch() (int64, error) {
	s.epoch++
	return s.epoch, nil
}

func TestBullyHigherScoreAnswersAndStarts(t *testing.T) {
	transport := &electionTransport{}
	b := NewBullyElection("b", 2, []string{"a"}, transport, &epochStore{})
	b.randBackoff = func() time.Duration { return 0 }

	b.HandleElection("a", ElectionMsg{SenderID: "a", Score: 1, Epoch: 1})
	require.Eventually(t, func() bool {
		return len(transport.elections) == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"a"}, transport.answers)
	require.Equal(t, []string{"a"}, transport.elections)
}

func TestCoordinatorRejectsStaleEpoch(t *testing.T) {
	b := NewBullyElection("b", 2, nil, &electionTransport{}, &epochStore{epoch: 3})
	b.HandleCoordinator(CoordinatorMsg{HubID: "old", Epoch: 2})
	require.Empty(t, b.Coordinator())
	b.HandleCoordinator(CoordinatorMsg{HubID: "new", Epoch: 4})
	require.Equal(t, "new", b.Coordinator())
}

func TestScore(t *testing.T) {
	require.Greater(t, Score(10, 0.2, 1000), Score(50, 0.8, 1000))
	require.Zero(t, Score(-1, 0, 100))
}
