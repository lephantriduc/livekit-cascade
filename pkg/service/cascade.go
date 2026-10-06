package service

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/livekit/livekit-server/pkg/cascading"
	"github.com/livekit/livekit-server/pkg/cascading/topology"
	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/protocol/logger"
)

type cascadeRuntime struct {
	linkManager  *cascading.LinkManager
	relayManager *cascading.RelayManager
	manager      *cascading.Manager
	peers        map[string]*cascading.SFUPeerInfo
	neighbors    []string
}

func newCascadeRuntime(conf *config.Config) (*cascadeRuntime, error) {
	if !conf.Cascade.Enabled {
		return nil, nil
	}

	selfID := conf.Cascade.SelfID
	if selfID == "" {
		return nil, fmt.Errorf("cascade.self_id is required when cascade is enabled")
	}
	if len(conf.Cascade.Peers) == 0 {
		return nil, fmt.Errorf("cascade.peers must not be empty when cascade is enabled")
	}

	allIDs := make([]string, 0, len(conf.Cascade.Peers)+1)
	allIDs = append(allIDs, selfID)
	peers := make(map[string]*cascading.SFUPeerInfo, len(conf.Cascade.Peers))
	for _, peer := range conf.Cascade.Peers {
		if peer.ID == "" || peer.Addr == "" {
			return nil, fmt.Errorf("cascade peers require both id and addr")
		}
		if peer.ID == selfID {
			continue
		}
		if _, ok := peers[peer.ID]; ok {
			return nil, fmt.Errorf("duplicate cascade peer %q", peer.ID)
		}
		peers[peer.ID] = &cascading.SFUPeerInfo{NodeID: peer.ID, Addr: peer.Addr}
		allIDs = append(allIDs, peer.ID)
	}

	strategy, err := cascadeTopology(conf.Cascade.Topology, conf.Cascade.HubID)
	if err != nil {
		return nil, err
	}

	links := cascading.NewLinkManager(selfID, cascading.NewHTTPSignaling())
	gossip := cascading.NewMeshGossipEngine(selfID, links)
	return &cascadeRuntime{
		linkManager:  links,
		relayManager: cascading.NewRelayManager(selfID, links),
		manager:      cascading.NewManager(selfID, gossip),
		peers:        peers,
		neighbors:    strategy.Neighbors(selfID, allIDs),
	}, nil
}

func cascadeTopology(name, hubID string) (topology.Strategy, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "mesh":
		return topology.Mesh{}, nil
	case "hub-spoke", "hub_spoke":
		if hubID == "" {
			return nil, fmt.Errorf("cascade.hub_id is required for hub-spoke topology")
		}
		return topology.HubSpoke{HubID: hubID}, nil
	case "ring":
		return topology.Ring{}, nil
	case "line":
		return topology.Line{}, nil
	default:
		return nil, fmt.Errorf("unsupported cascade topology %q", name)
	}
}

func (c *cascadeRuntime) registerHTTPHandlers(mux *http.ServeMux) {
	c.linkManager.RegisterHTTPHandler(mux)
}

func (c *cascadeRuntime) start() {
	for _, peerID := range c.neighbors {
		peer := c.peers[peerID]
		if peer == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := c.linkManager.EnsureLink(ctx, peer)
		cancel()
		if err != nil {
			logger.Warnw("cascade link setup failed", err, "peer", peerID)
			continue
		}
		logger.Infow("cascade link established", "local", c.linkManager.SelfID(), "remote", peerID)
	}
}

func (c *cascadeRuntime) close() {
	for _, link := range c.linkManager.All() {
		if err := link.Close(); err != nil {
			logger.Warnw("failed to close cascade link", err, "peer", link.RemoteID)
		}
	}
}
