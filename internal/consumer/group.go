package consumer

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type GroupState int32

const (
	Empty               GroupState = 0
	PreRebalance        GroupState = 1
	CompletingRebalance GroupState = 2
	Stable              GroupState = 3
)

type Member struct {
	ID            string
	Topics        []string
	LastHeartbeat time.Time
	Assignment    []int32 // assigned partition IDs (single-topic for simplicity)
}

type Group struct {
	ID         string
	state      GroupState
	generation int32
	members    map[string]*Member
	assignment map[string][]int32 // memberID -> partitions

	// partition count per topic (injected by coordinator)
	topicPartitions map[string]int32

	sessionTimeout time.Duration
	mu             sync.RWMutex
	rebalanceCh    chan struct{}

	// committed offsets: topic/partition -> offset
	offsets   map[string]int64
	offsetsMu sync.RWMutex
}

func NewGroup(id string, sessionTimeout time.Duration) *Group {
	g := &Group{
		ID:              id,
		members:         make(map[string]*Member),
		assignment:      make(map[string][]int32),
		topicPartitions: make(map[string]int32),
		offsets:         make(map[string]int64),
		sessionTimeout:  sessionTimeout,
		rebalanceCh:     make(chan struct{}, 1),
	}
	atomic.StoreInt32((*int32)(&g.state), int32(Empty))
	go g.heartbeatWatcher()
	return g
}

func (g *Group) Join(memberID string, topics []string) (int32, []string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	isNew := false
	if _, ok := g.members[memberID]; !ok {
		isNew = true
	}
	g.members[memberID] = &Member{
		ID:            memberID,
		Topics:        topics,
		LastHeartbeat: time.Now(),
	}
	// Register topics
	for _, t := range topics {
		if _, ok := g.topicPartitions[t]; !ok {
			g.topicPartitions[t] = 1 // default; coordinator sets real count
		}
	}

	gen := atomic.LoadInt32(&g.generation)
	memberList := g.memberIDs()

	if isNew || atomic.LoadInt32((*int32)(&g.state)) != int32(Stable) {
		atomic.StoreInt32((*int32)(&g.state), int32(PreRebalance))
		select {
		case g.rebalanceCh <- struct{}{}:
		default:
		}
	}
	return gen, memberList, atomic.LoadInt32((*int32)(&g.state)) == int32(Stable)
}

func (g *Group) Leave(memberID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.members, memberID)
	delete(g.assignment, memberID)
	if len(g.members) > 0 {
		atomic.StoreInt32((*int32)(&g.state), int32(PreRebalance))
		select {
		case g.rebalanceCh <- struct{}{}:
		default:
		}
	} else {
		atomic.StoreInt32((*int32)(&g.state), int32(Empty))
	}
}

func (g *Group) Heartbeat(memberID string, generation int32) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	m, ok := g.members[memberID]
	if !ok {
		return false, fmt.Errorf("unknown member: %s", memberID)
	}
	m.LastHeartbeat = time.Now()
	rebalanceNeeded := atomic.LoadInt32((*int32)(&g.state)) != int32(Stable)
	return rebalanceNeeded, nil
}

func (g *Group) Sync(memberID string, generation int32) ([]int32, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if atomic.LoadInt32(&g.generation) != generation {
		return nil, fmt.Errorf("generation mismatch")
	}
	parts, ok := g.assignment[memberID]
	if !ok {
		return nil, fmt.Errorf("no assignment for member %s", memberID)
	}
	return parts, nil
}

func (g *Group) CommitOffset(topic string, partition int32, offset int64) {
	key := fmt.Sprintf("%s/%d", topic, partition)
	g.offsetsMu.Lock()
	g.offsets[key] = offset
	g.offsetsMu.Unlock()
}

func (g *Group) FetchOffset(topic string, partition int32) int64 {
	key := fmt.Sprintf("%s/%d", topic, partition)
	g.offsetsMu.RLock()
	defer g.offsetsMu.RUnlock()
	return g.offsets[key]
}

func (g *Group) SetTopicPartitionCount(topic string, count int32) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.topicPartitions[topic] = count
}

// Rebalance computes and applies a new round-robin assignment.
func (g *Group) Rebalance() map[string][]int32 {
	g.mu.Lock()
	defer g.mu.Unlock()

	atomic.StoreInt32((*int32)(&g.state), int32(CompletingRebalance))
	gen := atomic.AddInt32(&g.generation, 1)

	// Collect all partitions across subscribed topics
	type partKey struct{ topic string; part int32 }
	var allParts []partKey
	for topic, count := range g.topicPartitions {
		for i := int32(0); i < count; i++ {
			allParts = append(allParts, partKey{topic, i})
		}
	}
	sort.Slice(allParts, func(i, j int) bool {
		if allParts[i].topic != allParts[j].topic {
			return allParts[i].topic < allParts[j].topic
		}
		return allParts[i].part < allParts[j].part
	})

	members := g.memberIDs()
	sort.Strings(members)

	newAssignment := make(map[string][]int32)
	for _, m := range members {
		newAssignment[m] = nil
	}
	for i, pk := range allParts {
		_ = pk // single-topic simplification: just use partition IDs
		if len(members) == 0 {
			break
		}
		m := members[i%len(members)]
		newAssignment[m] = append(newAssignment[m], allParts[i].part)
	}

	g.assignment = newAssignment
	atomic.StoreInt32((*int32)(&g.state), int32(Stable))
	_ = gen
	return newAssignment
}

func (g *Group) WaitRebalance() <-chan struct{} {
	return g.rebalanceCh
}

func (g *Group) State() GroupState {
	return GroupState(atomic.LoadInt32((*int32)(&g.state)))
}

func (g *Group) Generation() int32 {
	return atomic.LoadInt32(&g.generation)
}

func (g *Group) Members() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.memberIDs()
}

func (g *Group) memberIDs() []string {
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	return ids
}

func (g *Group) heartbeatWatcher() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		g.mu.Lock()
		expired := false
		for id, m := range g.members {
			if time.Since(m.LastHeartbeat) > g.sessionTimeout {
				delete(g.members, id)
				delete(g.assignment, id)
				expired = true
			}
		}
		if expired && len(g.members) > 0 {
			atomic.StoreInt32((*int32)(&g.state), int32(PreRebalance))
			select {
			case g.rebalanceCh <- struct{}{}:
			default:
			}
		}
		g.mu.Unlock()
	}
}
