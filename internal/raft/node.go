package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/kafkaflow/kafkaflow/internal/gen/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeState int32

const (
	Follower  NodeState = 0
	Candidate NodeState = 1
	Leader    NodeState = 2
)

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

type Peer struct {
	ID   int32
	Addr string
	conn *grpc.ClientConn
	cli  pb.RaftServiceClient
}

type Node struct {
	id          int32
	state       int32 // NodeState, atomic
	currentTerm int64 // atomic
	votedFor    int32 // atomic, -1 = none
	leaderID    int32 // atomic

	log         *RaftLog
	commitIndex int64 // atomic
	lastApplied int64

	peers map[int32]*Peer
	fsm   FSM

	// leader volatile state
	nextIndex  map[int32]int64
	matchIndex map[int32]int64
	leaderMu   sync.Mutex

	resetElectionCh chan struct{}
	proposeCh       chan LogEntry
	commitCh        chan LogEntry
	stopCh          chan struct{}

	mu sync.RWMutex
}

func NewNode(id int32, peerAddrs map[int32]string, fsm FSM) (*Node, error) {
	n := &Node{
		id:              id,
		log:             newRaftLog(),
		peers:           make(map[int32]*Peer),
		fsm:             fsm,
		nextIndex:       make(map[int32]int64),
		matchIndex:      make(map[int32]int64),
		resetElectionCh: make(chan struct{}, 1),
		proposeCh:       make(chan LogEntry, 256),
		commitCh:        make(chan LogEntry, 256),
		stopCh:          make(chan struct{}),
	}
	atomic.StoreInt32(&n.votedFor, -1)

	for peerID, addr := range peerAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		n.peers[peerID] = &Peer{
			ID:   peerID,
			Addr: addr,
			conn: conn,
			cli:  pb.NewRaftServiceClient(conn),
		}
	}
	return n, nil
}

func (n *Node) Start() {
	go n.applyLoop()
	go n.electionLoop()
}

func (n *Node) Stop() {
	close(n.stopCh)
	for _, p := range n.peers {
		p.conn.Close()
	}
}

func (n *Node) Propose(entryType EntryType, data []byte) error {
	entry := LogEntry{
		Term: atomic.LoadInt64(&n.currentTerm),
		Type: entryType,
		Data: data,
	}
	select {
	case n.proposeCh <- entry:
		return nil
	case <-n.stopCh:
		return context.Canceled
	}
}

func (n *Node) IsLeader() bool {
	return NodeState(atomic.LoadInt32(&n.state)) == Leader
}

func (n *Node) LeaderID() int32 {
	return atomic.LoadInt32(&n.leaderID)
}

func (n *Node) electionLoop() {
	for {
		timeout := electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
		timer := time.NewTimer(timeout)
		select {
		case <-n.stopCh:
			timer.Stop()
			return
		case <-n.resetElectionCh:
			timer.Stop()
		case <-timer.C:
			if NodeState(atomic.LoadInt32(&n.state)) != Leader {
				n.startElection()
			}
		case entry := <-n.proposeCh:
			if n.IsLeader() {
				n.log.Append(entry)
				n.replicateToAll()
			}
		}
	}
}

func (n *Node) startElection() {
	term := atomic.AddInt64(&n.currentTerm, 1)
	atomic.StoreInt32(&n.state, int32(Candidate))
	atomic.StoreInt32(&n.votedFor, n.id)
	log.Printf("raft[%d]: starting election term=%d", n.id, term)

	votes := int32(1) // vote for self
	majority := int32(len(n.peers)/2 + 1)

	var wg sync.WaitGroup
	var mu sync.Mutex
	wonElection := false

	for _, peer := range n.peers {
		wg.Add(1)
		go func(p *Peer) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			resp, err := p.cli.RequestVote(ctx, &pb.RequestVoteRequest{
				Term:        term,
				CandidateId: n.id,
				LastLogIndex: n.log.LastIndex(),
				LastLogTerm:  n.log.LastTerm(),
			})
			if err != nil {
				return
			}
			if resp.Term > term {
				n.becomeFollower(resp.Term)
				return
			}
			if resp.VoteGranted {
				mu.Lock()
				votes++
				if !wonElection && votes >= majority {
					wonElection = true
					mu.Unlock()
					n.becomeLeader(term)
					return
				}
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()
}

func (n *Node) becomeLeader(term int64) {
	atomic.StoreInt32(&n.state, int32(Leader))
	atomic.StoreInt32(&n.leaderID, n.id)
	log.Printf("raft[%d]: became leader term=%d", n.id, term)

	n.leaderMu.Lock()
	lastIdx := n.log.LastIndex()
	for peerID := range n.peers {
		n.nextIndex[peerID] = lastIdx + 1
		n.matchIndex[peerID] = 0
	}
	n.leaderMu.Unlock()

	go n.heartbeatLoop()
}

func (n *Node) becomeFollower(term int64) {
	atomic.StoreInt64(&n.currentTerm, term)
	atomic.StoreInt32(&n.state, int32(Follower))
	atomic.StoreInt32(&n.votedFor, -1)
	select {
	case n.resetElectionCh <- struct{}{}:
	default:
	}
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			if !n.IsLeader() {
				return
			}
			n.replicateToAll()
		}
	}
}

func (n *Node) replicateToAll() {
	for _, peer := range n.peers {
		go n.replicateToPeer(peer)
	}
}

func (n *Node) replicateToPeer(p *Peer) {
	n.leaderMu.Lock()
	nextIdx := n.nextIndex[p.ID]
	n.leaderMu.Unlock()

	prevIdx := nextIdx - 1
	prevEntry, _ := n.log.Entry(prevIdx)
	entries := n.log.EntriesFrom(nextIdx)

	pbEntries := make([]*pb.LogEntry, len(entries))
	for i, e := range entries {
		pbEntries[i] = &pb.LogEntry{
			Term:  e.Term,
			Index: e.Index,
			Type:  string(e.Type),
			Data:  e.Data,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	resp, err := p.cli.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         atomic.LoadInt64(&n.currentTerm),
		LeaderId:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevEntry.Term,
		Entries:      pbEntries,
		LeaderCommit: atomic.LoadInt64(&n.commitIndex),
	})
	if err != nil {
		return
	}
	if resp.Term > atomic.LoadInt64(&n.currentTerm) {
		n.becomeFollower(resp.Term)
		return
	}
	if resp.Success && len(entries) > 0 {
		n.leaderMu.Lock()
		n.nextIndex[p.ID] = n.log.LastIndex() + 1
		n.matchIndex[p.ID] = n.log.LastIndex()
		n.leaderMu.Unlock()
		n.maybeCommit()
	} else if !resp.Success {
		n.leaderMu.Lock()
		if n.nextIndex[p.ID] > 1 {
			n.nextIndex[p.ID]--
		}
		n.leaderMu.Unlock()
	}
}

func (n *Node) maybeCommit() {
	n.leaderMu.Lock()
	defer n.leaderMu.Unlock()

	majority := int64(len(n.peers)/2 + 1)
	lastIdx := n.log.LastIndex()
	for idx := lastIdx; idx > atomic.LoadInt64(&n.commitIndex); idx-- {
		entry, ok := n.log.Entry(idx)
		if !ok || entry.Term != atomic.LoadInt64(&n.currentTerm) {
			continue
		}
		count := int64(1) // self
		for _, m := range n.matchIndex {
			if m >= idx {
				count++
			}
		}
		if count >= majority {
			atomic.StoreInt64(&n.commitIndex, idx)
			break
		}
	}
}

func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
			commit := atomic.LoadInt64(&n.commitIndex)
			if n.lastApplied < commit {
				n.lastApplied++
				entry, ok := n.log.Entry(n.lastApplied)
				if ok {
					n.fsm.Apply(entry)
					select {
					case n.commitCh <- entry:
					default:
					}
				}
			} else {
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
}

// RequestVote handles incoming vote requests (gRPC server handler).
func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	currentTerm := atomic.LoadInt64(&n.currentTerm)
	if req.Term > currentTerm {
		n.becomeFollower(req.Term)
		currentTerm = req.Term
	}
	votedFor := atomic.LoadInt32(&n.votedFor)
	lastIdx := n.log.LastIndex()
	lastTerm := n.log.LastTerm()

	grant := req.Term >= currentTerm &&
		(votedFor == -1 || votedFor == req.CandidateId) &&
		(req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx))

	if grant {
		atomic.StoreInt32(&n.votedFor, req.CandidateId)
		select {
		case n.resetElectionCh <- struct{}{}:
		default:
		}
	}
	return &pb.RequestVoteResponse{Term: currentTerm, VoteGranted: grant}, nil
}

// AppendEntries handles incoming log replication (gRPC server handler).
func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	currentTerm := atomic.LoadInt64(&n.currentTerm)
	if req.Term < currentTerm {
		return &pb.AppendEntriesResponse{Term: currentTerm, Success: false}, nil
	}
	if req.Term > currentTerm {
		n.becomeFollower(req.Term)
	}
	atomic.StoreInt32(&n.leaderID, req.LeaderId)
	select {
	case n.resetElectionCh <- struct{}{}:
	default:
	}

	entries := make([]LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = LogEntry{Term: e.Term, Index: e.Index, Type: EntryType(e.Type), Data: e.Data}
	}
	ok := n.log.AppendEntries(req.PrevLogIndex, entries)
	if ok && req.LeaderCommit > atomic.LoadInt64(&n.commitIndex) {
		newCommit := req.LeaderCommit
		if last := n.log.LastIndex(); last < newCommit {
			newCommit = last
		}
		atomic.StoreInt64(&n.commitIndex, newCommit)
	}
	return &pb.AppendEntriesResponse{Term: currentTerm, Success: ok}, nil
}
