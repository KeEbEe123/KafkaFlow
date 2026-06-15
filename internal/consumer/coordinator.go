package consumer

import (
	"log"
	"sync"
	"time"
)

// Coordinator manages all consumer groups and drives rebalancing.
type Coordinator struct {
	groups map[string]*Group
	mu     sync.RWMutex

	// topicPartitionsFn returns number of partitions for a topic
	topicPartitionsFn func(topic string) int32

	// rebalance event callback for WebSocket broadcast
	OnRebalance func(groupID string, generation int32, assignment map[string][]int32)
}

func NewCoordinator(topicPartitionsFn func(string) int32) *Coordinator {
	c := &Coordinator{
		groups:            make(map[string]*Group),
		topicPartitionsFn: topicPartitionsFn,
	}
	return c
}

func (c *Coordinator) GetOrCreateGroup(id string, sessionTimeoutMs int) *Group {
	c.mu.Lock()
	defer c.mu.Unlock()
	if g, ok := c.groups[id]; ok {
		return g
	}
	timeout := time.Duration(sessionTimeoutMs) * time.Millisecond
	g := NewGroup(id, timeout)
	c.groups[id] = g
	go c.watchGroup(g)
	return g
}

func (c *Coordinator) GetGroup(id string) (*Group, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	g, ok := c.groups[id]
	return g, ok
}

func (c *Coordinator) SetTopicPartitions(topic string, count int32) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, g := range c.groups {
		g.SetTopicPartitionCount(topic, count)
	}
}

func (c *Coordinator) ListGroups() []GroupSummary {
	c.mu.RLock()
	defer c.mu.RUnlock()
	summaries := make([]GroupSummary, 0, len(c.groups))
	for _, g := range c.groups {
		summaries = append(summaries, GroupSummary{
			ID:         g.ID,
			State:      g.State().String(),
			Generation: g.Generation(),
			Members:    g.Members(),
		})
	}
	return summaries
}

type GroupSummary struct {
	ID         string
	State      string
	Generation int32
	Members    []string
}

func (gs GroupState) String() string {
	switch gs {
	case Empty:
		return "Empty"
	case PreRebalance:
		return "PreRebalance"
	case CompletingRebalance:
		return "CompletingRebalance"
	case Stable:
		return "Stable"
	}
	return "Unknown"
}

func (c *Coordinator) watchGroup(g *Group) {
	for range g.WaitRebalance() {
		// Update partition counts from broker
		g.mu.Lock()
		for topic := range g.topicPartitions {
			count := c.topicPartitionsFn(topic)
			if count > 0 {
				g.topicPartitions[topic] = count
			}
		}
		g.mu.Unlock()

		assignment := g.Rebalance()
		gen := g.Generation()
		log.Printf("coordinator: rebalanced group=%s gen=%d members=%d", g.ID, gen, len(assignment))

		if c.OnRebalance != nil {
			c.OnRebalance(g.ID, gen, assignment)
		}
	}
}

