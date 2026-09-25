package cascading

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	// "github.com/pion/webrtc/v4"
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

	// Connect node A to node B
	peerB := &SFUPeerInfo{
		NodeID: "sfu-b",
		Addr:   strings.TrimPrefix(srv.URL, "http://"),
	}
	linkA, err := lmA.EnsureLink(context.Background(), peerB)
	require.NoError(t, err)

	// Node B's link was created during the HTTP signaling exchange
	// linkB := lmB.GetLink("sfu-a")
	// require.NotNil(t, linkB, "Link on Node B must exist")
	peerA := &SFUPeerInfo{
		NodeID: "sfu-a",
		Addr:   strings.TrimPrefix(srv.URL, "http://"),
	}
	linkB, err := lmB.EnsureLink(context.Background(), peerA)
	require.NoError(t, err)

	// Messages sent by Node A are received by Node B
	receivedAtB := make(chan CascadeControlMsg, 1)
	linkB.OnControl(func(msg CascadeControlMsg) {
		fmt.Printf("Node B got control: %v!\n", msg)
		receivedAtB <- msg
	})

	// Wait until Node A's data channel is open
	openedA := make(chan struct{})
	linkA.controlChan.OnOpen(func() {
		close(openedA)
	})
	/*
		if linkA.controlChan.ReadyState() == webrtc.DataChannelStateOpen {
			close(openedA)
		} else {
			linkA.controlChan.OnOpen(func() {
				close(openedA)
			})
		}
	*/

	select {
	case <-openedA:
		slog.Info(fmt.Sprintf("Channel state: %v", linkA.controlChan.ReadyState()))
		err = linkA.SendControl(CascadeControlMsg{Type: MsgRelayTrack})
		require.NoError(t, err)
		slog.Info("Sending test control message from Node A to Node B")
	case <-time.After(3 * time.Second):
		t.Fatal("Data channel never opened on Node A")
	}

	// Verify Node B received the control message
	select {
	case msg := <-receivedAtB:
		assert.Equal(t, MsgRelayTrack, msg.Type)
	case <-time.After(5 * time.Second):
		t.Fatal("Control msg never arrived at Node B")
	}
}
