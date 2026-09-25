package cascading

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fmt"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTwoLinksExchangeControl(t *testing.T) {
	slog.Info("Start testing")

	sig := NewHTTPSignaling()
	lmA := NewLinkManager("sfu-a", sig)
	lmB := NewLinkManager("sfu-b", sig)

	mux := http.NewServeMux()
	lmB.RegisterHTTPHandler(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// try to connect
	peerB := &SFUPeerInfo{
		NodeID: "sfu-b",
		Addr:   strings.TrimPrefix(srv.URL, "http://"),
	}
	link, err := lmA.EnsureLink(context.Background(), peerB)
	require.NoError(t, err)

	received := make(chan CascadeControlMsg, 1)
	link.onControl = func(msg CascadeControlMsg) {
		fmt.Printf("Got control: %v!\n", msg)
		received <- msg
	}

	opened := make(chan struct{})
	link.controlChan.OnOpen(func() {
		close(opened)
	})

	fmt.Printf("Channel state: %v\n", link.controlChan.ReadyState())
	select {
	case <-opened:
		slog.Info(fmt.Sprintf("Channel state: %v", link.controlChan.ReadyState()))
		err = link.SendControl(CascadeControlMsg{Type: MsgRelayTrack})
		require.NoError(t, err)
		slog.Info("Sending test control message")
	case <-time.After(3 * time.Second):
		t.Fatal("Data channel never opened")
	}

	select {
	case msg := <-received:
		assert.Equal(t, MsgRelayTrack, msg.Type)
	case <-time.After(5 * time.Second):
		slog.Error("Huhu")
		t.Fatal("Control msg never arrived")
	}
}
