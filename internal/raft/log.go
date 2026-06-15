package raft

import (
	"encoding/json"
	"sync"
)

type EntryType string

const (
	EntryJoinGroup   EntryType = "join_group"
	EntryLeaveGroup  EntryType = "leave_group"
	EntryAssignGroup EntryType = "assign_group"
	EntryNoop        EntryType = "noop"
)

type LogEntry struct {
	Term  int64     `json:"term"`
	Index int64     `json:"index"`
	Type  EntryType `json:"type"`
	Data  []byte    `json:"data"`
}

type RaftLog struct {
	entries []LogEntry
	mu      sync.RWMutex
}

func newRaftLog() *RaftLog {
	// sentinel entry at index 0
	return &RaftLog{
		entries: []LogEntry{{Term: 0, Index: 0, Type: EntryNoop}},
	}
}

func (l *RaftLog) Append(entry LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry.Index = int64(len(l.entries))
	l.entries = append(l.entries, entry)
}

func (l *RaftLog) AppendEntries(prevIndex int64, entries []LogEntry) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if prevIndex >= int64(len(l.entries)) {
		return false
	}
	// truncate conflicting entries
	l.entries = l.entries[:prevIndex+1]
	for i, e := range entries {
		e.Index = prevIndex + int64(i) + 1
		l.entries = append(l.entries, e)
	}
	return true
}

func (l *RaftLog) Entry(index int64) (LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index < 0 || index >= int64(len(l.entries)) {
		return LogEntry{}, false
	}
	return l.entries[index], true
}

func (l *RaftLog) LastIndex() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return int64(len(l.entries) - 1)
}

func (l *RaftLog) LastTerm() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Term
}

func (l *RaftLog) EntriesFrom(index int64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index >= int64(len(l.entries)) {
		return nil
	}
	result := make([]LogEntry, len(l.entries)-int(index))
	copy(result, l.entries[index:])
	return result
}

// Command helpers for group coordination
type JoinGroupCmd struct {
	GroupID  string `json:"group_id"`
	MemberID string `json:"member_id"`
	Topics   []string `json:"topics"`
}

type LeaveGroupCmd struct {
	GroupID  string `json:"group_id"`
	MemberID string `json:"member_id"`
}

type AssignGroupCmd struct {
	GroupID    string              `json:"group_id"`
	Generation int32               `json:"generation"`
	Assignment map[string][]int32  `json:"assignment"` // memberID -> partitions
}

func MarshalCmd(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func UnmarshalJoinCmd(data []byte) (*JoinGroupCmd, error) {
	var cmd JoinGroupCmd
	return &cmd, json.Unmarshal(data, &cmd)
}

func UnmarshalLeaveCmd(data []byte) (*LeaveGroupCmd, error) {
	var cmd LeaveGroupCmd
	return &cmd, json.Unmarshal(data, &cmd)
}

func UnmarshalAssignCmd(data []byte) (*AssignGroupCmd, error) {
	var cmd AssignGroupCmd
	return &cmd, json.Unmarshal(data, &cmd)
}
