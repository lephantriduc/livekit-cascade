package topology

// Strategy selects the peers a node should connect to.
type Strategy interface {
	Neighbors(selfID string, allNodes []string) []string
}

type Mesh struct{}

func (Mesh) Neighbors(selfID string, allNodes []string) []string {
	return excludingSelf(selfID, allNodes)
}

type HubSpoke struct {
	HubID string
}

func (h HubSpoke) Neighbors(selfID string, allNodes []string) []string {
	if selfID == h.HubID {
		return excludingSelf(selfID, allNodes)
	}
	for _, node := range allNodes {
		if node == h.HubID {
			return []string{h.HubID}
		}
	}
	return nil
}

type Ring struct{}

func (Ring) Neighbors(selfID string, allNodes []string) []string {
	index := indexOf(selfID, allNodes)
	if index < 0 || len(allNodes) < 2 {
		return nil
	}
	return []string{
		allNodes[(index+len(allNodes)-1)%len(allNodes)],
		allNodes[(index+1)%len(allNodes)],
	}
}

type Line struct{}

func (Line) Neighbors(selfID string, allNodes []string) []string {
	index := indexOf(selfID, allNodes)
	if index < 0 {
		return nil
	}
	neighbors := make([]string, 0, 2)
	if index > 0 {
		neighbors = append(neighbors, allNodes[index-1])
	}
	if index+1 < len(allNodes) {
		neighbors = append(neighbors, allNodes[index+1])
	}
	return neighbors
}

type Custom struct {
	Edges map[string][]string
}

func (c Custom) Neighbors(selfID string, allNodes []string) []string {
	neighbors := c.Edges[selfID]
	if len(neighbors) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(allNodes))
	for _, node := range allNodes {
		allowed[node] = struct{}{}
	}
	result := make([]string, 0, len(neighbors))
	seen := make(map[string]struct{}, len(neighbors))
	for _, neighbor := range neighbors {
		if neighbor == selfID {
			continue
		}
		if _, ok := allowed[neighbor]; !ok {
			continue
		}
		if _, ok := seen[neighbor]; ok {
			continue
		}
		seen[neighbor] = struct{}{}
		result = append(result, neighbor)
	}
	return result
}

func excludingSelf(selfID string, allNodes []string) []string {
	neighbors := make([]string, 0, len(allNodes))
	for _, node := range allNodes {
		if node != selfID {
			neighbors = append(neighbors, node)
		}
	}
	return neighbors
}

func indexOf(node string, allNodes []string) int {
	for index, candidate := range allNodes {
		if candidate == node {
			return index
		}
	}
	return -1
}
