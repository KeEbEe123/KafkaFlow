package raft

import "sync"

// FSM is the state machine applied when Raft commits an entry.
type FSM interface {
	Apply(entry LogEntry)
}

// GroupState holds the current state of all consumer groups.
type GroupState struct {
	Groups map[string]*GroupInfo
	mu     sync.RWMutex
}

type GroupInfo struct {
	ID         string
	Generation int32
	Members    map[string]MemberInfo // memberID -> info
	Assignment map[string][]int32    // memberID -> partitions
}

type MemberInfo struct {
	ID     string
	Topics []string
}

func NewGroupState() *GroupState {
	return &GroupState{Groups: make(map[string]*GroupInfo)}
}

// Apply implements FSM. Called on commit.
func (gs *GroupState) Apply(entry LogEntry) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	switch entry.Type {
	case EntryJoinGroup:
		cmd, err := UnmarshalJoinCmd(entry.Data)
		if err != nil {
			return
		}
		g := gs.getOrCreate(cmd.GroupID)
		g.Members[cmd.MemberID] = MemberInfo{ID: cmd.MemberID, Topics: cmd.Topics}

	case EntryLeaveGroup:
		cmd, err := UnmarshalLeaveCmd(entry.Data)
		if err != nil {
			return
		}
		if g, ok := gs.Groups[cmd.GroupID]; ok {
			delete(g.Members, cmd.MemberID)
			delete(g.Assignment, cmd.MemberID)
		}

	case EntryAssignGroup:
		cmd, err := UnmarshalAssignCmd(entry.Data)
		if err != nil {
			return
		}
		g := gs.getOrCreate(cmd.GroupID)
		g.Generation = cmd.Generation
		g.Assignment = cmd.Assignment
	}
}

func (gs *GroupState) GetGroup(id string) (*GroupInfo, bool) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	g, ok := gs.Groups[id]
	return g, ok
}

func (gs *GroupState) getOrCreate(id string) *GroupInfo {
	if g, ok := gs.Groups[id]; ok {
		return g
	}
	g := &GroupInfo{
		ID:         id,
		Members:    make(map[string]MemberInfo),
		Assignment: make(map[string][]int32),
	}
	gs.Groups[id] = g
	return g
}
