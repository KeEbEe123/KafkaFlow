package transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/kafkaflow/kafkaflow/internal/broker"
	"github.com/kafkaflow/kafkaflow/internal/consumer"
	pb "github.com/kafkaflow/kafkaflow/internal/gen/proto"
	"github.com/kafkaflow/kafkaflow/internal/metrics"
	"github.com/kafkaflow/kafkaflow/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// GRPCServer wires gRPC services to broker, consumer coordinator, and Raft node.
type GRPCServer struct {
	broker      *broker.Broker
	coord       *consumer.Coordinator
	raftNode    *raft.Node
	metrics     *metrics.Collector
	wsHub       *WSHub
	server      *grpc.Server

	pb.UnimplementedBrokerServiceServer
	pb.UnimplementedConsumerServiceServer
	pb.UnimplementedAdminServiceServer
	pb.UnimplementedRaftServiceServer
}

func NewGRPCServer(
	b *broker.Broker,
	coord *consumer.Coordinator,
	n *raft.Node,
	m *metrics.Collector,
	hub *WSHub,
) *GRPCServer {
	s := &GRPCServer{
		broker:  b,
		coord:   coord,
		raftNode: n,
		metrics: m,
		wsHub:   hub,
	}
	srv := grpc.NewServer(
		grpc.MaxConcurrentStreams(1000),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
	)
	pb.RegisterBrokerServiceServer(srv, s)
	pb.RegisterConsumerServiceServer(srv, s)
	pb.RegisterAdminServiceServer(srv, s)
	pb.RegisterRaftServiceServer(srv, s)
	s.server = srv
	return s
}

func (s *GRPCServer) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("grpc server listening on %s", addr)
	return s.server.Serve(lis)
}

func (s *GRPCServer) GracefulStop() {
	s.server.GracefulStop()
}

// ── BrokerService ──────────────────────────────────────────────────────────

func (s *GRPCServer) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	start := time.Now()
	offset, err := s.broker.Publish(req.Topic, req.Partition, req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	latNs := time.Since(start).Nanoseconds()
	s.metrics.RecordPublish(int64(len(req.Value)), latNs)
	s.wsHub.Broadcast("partition.offset", map[string]interface{}{
		"topic":     req.Topic,
		"partition": req.Partition,
		"offset":    offset,
	})
	return &pb.PublishResponse{
		Offset:    offset,
		Timestamp: time.Now().UnixMilli(),
		Partition: req.Partition,
	}, nil
}

func (s *GRPCServer) FetchStream(req *pb.FetchRequest, stream pb.BrokerService_FetchStreamServer) error {
	maxWait := time.Duration(req.MaxWaitMs) * time.Millisecond
	if maxWait == 0 {
		maxWait = 500 * time.Millisecond
	}
	maxBytes := int(req.MaxBytes)
	if maxBytes == 0 {
		maxBytes = 1 * 1024 * 1024
	}
	deadline := time.Now().Add(maxWait)
	fromOffset := req.Offset

	for time.Now().Before(deadline) {
		msgs, hwm, err := s.broker.Fetch(req.Topic, req.Partition, fromOffset, maxBytes)
		if err != nil {
			return err
		}
		if len(msgs) > 0 {
			pbMsgs := make([]*pb.Message, len(msgs))
			for i, m := range msgs {
				pbMsgs[i] = &pb.Message{
					Offset:    m.Offset,
					Timestamp: m.Timestamp,
					Key:       m.Key,
					Value:     m.Value,
				}
			}
			if err := stream.Send(&pb.FetchResponse{Messages: pbMsgs, HighWatermark: hwm}); err != nil {
				return err
			}
			fromOffset = msgs[len(msgs)-1].Offset + 1
			s.metrics.RecordFetch(int64(maxBytes))
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	// No messages — return empty with hwm
	_, hwm, err := s.broker.Fetch(req.Topic, req.Partition, fromOffset, 0)
	if err != nil {
		return nil
	}
	return stream.Send(&pb.FetchResponse{HighWatermark: hwm})
}

func (s *GRPCServer) GetMetadata(ctx context.Context, req *pb.MetadataRequest) (*pb.MetadataResponse, error) {
	topics := req.Topics
	if len(topics) == 0 {
		topics = s.broker.ListTopics()
	}
	var topicMetas []*pb.TopicMetadata
	for _, t := range topics {
		infos, err := s.broker.GetPartitionInfo(t)
		if err != nil {
			continue
		}
		parts := make([]*pb.PartitionMetadata, len(infos))
		for i, info := range infos {
			parts[i] = &pb.PartitionMetadata{
				Id:            info.ID,
				Leader:        info.LeaderID,
				HighWatermark: info.HWM,
			}
		}
		topicMetas = append(topicMetas, &pb.TopicMetadata{Name: t, Partitions: parts})
	}
	return &pb.MetadataResponse{
		Topics:  topicMetas,
		Brokers: []*pb.BrokerInfo{{Id: s.broker.ID, Addr: ""}},
	}, nil
}

// ── AdminService ───────────────────────────────────────────────────────────

func (s *GRPCServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if err := s.broker.CreateTopic(req.Name, req.Partitions); err != nil {
		return &pb.CreateTopicResponse{Success: false, Error: err.Error()}, nil
	}
	s.coord.SetTopicPartitions(req.Name, req.Partitions)
	s.wsHub.Broadcast("topic.created", map[string]interface{}{
		"topic":      req.Name,
		"partitions": req.Partitions,
	})
	return &pb.CreateTopicResponse{Success: true}, nil
}

func (s *GRPCServer) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	if err := s.broker.DeleteTopic(req.Name); err != nil {
		return &pb.DeleteTopicResponse{Success: false, Error: err.Error()}, nil
	}
	return &pb.DeleteTopicResponse{Success: true}, nil
}

func (s *GRPCServer) GetBrokerStats(ctx context.Context, req *pb.GetBrokerStatsRequest) (*pb.BrokerStats, error) {
	snap := s.metrics.Snapshot()
	bStats := s.broker.Stats()
	return &pb.BrokerStats{
		BrokerId:           bStats.ID,
		MessagesIn:         bStats.MessagesIn,
		MessagesOut:        bStats.MessagesOut,
		BytesIn:            bStats.BytesIn,
		BytesOut:           bStats.BytesOut,
		MessagesPerSec:     snap.MessagesPerSec,
		AvgLatencyMs:       snap.AvgLatencyMs,
		P99LatencyMs:       snap.P99LatencyMs,
		TopicCount:         bStats.TopicCount,
		PartitionCount:     bStats.PartitionCount,
		ConsumerGroupCount: int32(len(s.coord.ListGroups())),
	}, nil
}

func (s *GRPCServer) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	return &pb.ListTopicsResponse{Topics: s.broker.ListTopics()}, nil
}

// ── ConsumerService ────────────────────────────────────────────────────────

func (s *GRPCServer) JoinGroup(ctx context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	g := s.coord.GetOrCreateGroup(req.GroupId, int(req.SessionTimeoutMs))
	gen, members, _ := g.Join(req.MemberId, req.Topics)
	s.wsHub.Broadcast("group.member_joined", map[string]interface{}{
		"group_id":  req.GroupId,
		"member_id": req.MemberId,
	})
	return &pb.JoinGroupResponse{
		GroupId:    req.GroupId,
		MemberId:   req.MemberId,
		Generation: gen,
		Members:    members,
	}, nil
}

func (s *GRPCServer) SyncGroup(ctx context.Context, req *pb.SyncGroupRequest) (*pb.SyncGroupResponse, error) {
	g, ok := s.coord.GetGroup(req.GroupId)
	if !ok {
		return nil, fmt.Errorf("group %s not found", req.GroupId)
	}
	parts, err := g.Sync(req.MemberId, req.Generation)
	if err != nil {
		return &pb.SyncGroupResponse{Error: err.Error()}, nil
	}
	return &pb.SyncGroupResponse{AssignedPartitions: parts}, nil
}

func (s *GRPCServer) LeaveGroup(ctx context.Context, req *pb.LeaveGroupRequest) (*pb.LeaveGroupResponse, error) {
	g, ok := s.coord.GetGroup(req.GroupId)
	if !ok {
		return &pb.LeaveGroupResponse{}, nil
	}
	g.Leave(req.MemberId)
	s.wsHub.Broadcast("group.member_left", map[string]interface{}{
		"group_id":  req.GroupId,
		"member_id": req.MemberId,
	})
	return &pb.LeaveGroupResponse{}, nil
}

func (s *GRPCServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	g, ok := s.coord.GetGroup(req.GroupId)
	if !ok {
		return &pb.HeartbeatResponse{Error: "group not found"}, nil
	}
	rebalance, err := g.Heartbeat(req.MemberId, req.Generation)
	if err != nil {
		return &pb.HeartbeatResponse{Error: err.Error()}, nil
	}
	return &pb.HeartbeatResponse{RebalanceNeeded: rebalance}, nil
}

func (s *GRPCServer) CommitOffsets(ctx context.Context, req *pb.CommitOffsetsRequest) (*pb.CommitOffsetsResponse, error) {
	g, ok := s.coord.GetGroup(req.GroupId)
	if !ok {
		return &pb.CommitOffsetsResponse{Error: "group not found"}, nil
	}
	for _, o := range req.Offsets {
		g.CommitOffset(o.Topic, o.Partition, o.Offset)
	}
	return &pb.CommitOffsetsResponse{}, nil
}

func (s *GRPCServer) FetchOffsets(ctx context.Context, req *pb.FetchOffsetsRequest) (*pb.FetchOffsetsResponse, error) {
	g, ok := s.coord.GetGroup(req.GroupId)
	if !ok {
		return nil, fmt.Errorf("group not found")
	}
	var offsets []*pb.OffsetInfo
	for _, t := range req.Topics {
		infos, err := s.broker.GetPartitionInfo(t)
		if err != nil {
			continue
		}
		for _, info := range infos {
			committed := g.FetchOffset(t, info.ID)
			lag := info.HWM - committed
			if lag < 0 {
				lag = 0
			}
			offsets = append(offsets, &pb.OffsetInfo{
				Topic:     t,
				Partition: info.ID,
				Offset:    committed,
				Lag:       lag,
			})
		}
	}
	return &pb.FetchOffsetsResponse{Offsets: offsets}, nil
}

// ── RaftService ────────────────────────────────────────────────────────────

func (s *GRPCServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if s.raftNode == nil {
		return &pb.RequestVoteResponse{}, nil
	}
	return s.raftNode.RequestVote(ctx, req)
}

func (s *GRPCServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if s.raftNode == nil {
		return &pb.AppendEntriesResponse{}, nil
	}
	return s.raftNode.AppendEntries(ctx, req)
}
