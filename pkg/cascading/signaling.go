package cascading

import (
	"context"
	"io"
	"net/http"
	"strings"
)

// Exchanges SDP offer/answer between 2 SFU nodes
type SignalingTransport interface {
	SendOffer(ctx context.Context, srcID string, dstID string, dstAddr string, offerSDP string) (answerSDP string, err error)
}

type HTTPSignaling struct {
	client *http.Client
}

func NewHTTPSignaling() *HTTPSignaling {
	return &HTTPSignaling{client: &http.Client{}}
}

func (h *HTTPSignaling) SendOffer(ctx context.Context, srcID string, dstID string, dstAddr string, offerSDP string) (string, error) {
	req, err := http.NewRequest(
		"POST",
		"http://"+dstAddr+"/cascade/signal",
		strings.NewReader(offerSDP),
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/sdp")
	req.Header.Set("Node-ID", srcID)

	resp, err := h.client.Do(req)
	if err != nil {
		return "", err
	}
	answer, _ := io.ReadAll(resp.Body)
	return string(answer), nil
}
