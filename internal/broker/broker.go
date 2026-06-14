package broker

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type Broker struct {
	ID         int32
	cfg        *Config
	partitions map[string]map[int32]*Partition // topic -> partID -> partition
	mu         sync.RWMutex

	shutdownCh chan struct{}
	doneCh     chan struct{}

	EventCh chan Event
}

type Event struct {
	Type    string
	Topic   string
	Partition int32
	Offset  int64
	HWM     int64
	Extra   map[string]interface{}
}

func New(cfg *Config) (*Broker, error) {
	b := &Broker{
		ID:         cfg.Broker.ID,
		cfg:        cfg,
		partitions: make(map[string]map[int32]*Partition),
		shutdownCh: make(chan struct{}),
		doneCh:     make(chan struct{}),
		EventCh:    make(chan Event, 4096),
	}
	return b, nil
}

func (b *Broker) CreateTopic(topic string, numPartitions int32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.partitions[topic]; exists {
		return fmt.Errorf("topic %s already exists", topic)
	}
	b.partitions[topic] = make(map[int32]*Partition)
	for i := int32(0); i < numPartitions; i++ {
		p, err := NewPartition(i, topic, b.cfg.Storage.DataDir, b.cfg.Storage.SegmentMaxBytes)
		if err != nil {
			return fmt.Errorf("create partition %d: %w", i, err)
		}
		p.LeaderID = b.ID
		b.partitions[topic][i] = p
	}
	log.Printf("broker %d: created topic %s with %d partitions", b.ID, topic, numPartitions)
	return nil
}

func (b *Broker) DeleteTopic(topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	parts, ok := b.partitions[topic]
	if !ok {
		return fmt.Errorf("topic %s not found", topic)
	}
	for _, p := range parts {
		p.Close()
	}
	delete(b.partitions, topic)
	return nil
}

func (b *Broker) Publish(topic string, partition int32, key, value []byte) (int64, error) {
	p, err := b.getPartition(topic, partition)
	if err != nil {
		return 0, err
	}
	offset, err := p.Append(key, value)
	if err != nil {
		return 0, err
	}
	select {
	case b.EventCh <- Event{Type: "message.published", Topic: topic, Partition: partition, Offset: offset, HWM: p.HWM()}:
	default:
	}
	return offset, nil
}

func (b *Broker) Fetch(topic string, partition int32, fromOffset int64, maxBytes int) ([]*Message, int64, error) {
	p, err := b.getPartition(topic, partition)
	if err != nil {
		return nil, 0, err
	}
	msgs, hwm := p.Fetch(fromOffset, maxBytes)
	return msgs, hwm, nil
}

func (b *Broker) GetPartitionInfo(topic string) ([]PartitionInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	parts, ok := b.partitions[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	var infos []PartitionInfo
	for id, p := range parts {
		infos = append(infos, PartitionInfo{
			ID:       id,
			Topic:    topic,
			LeaderID: p.LeaderID,
			HWM:      p.HWM(),
		})
	}
	return infos, nil
}

type PartitionInfo struct {
	ID       int32
	Topic    string
	LeaderID int32
	HWM      int64
}

func (b *Broker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	topics := make([]string, 0, len(b.partitions))
	for t := range b.partitions {
		topics = append(topics, t)
	}
	return topics
}

func (b *Broker) Stats() BrokerStats {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var totalMsgs, totalBytes int64
	partCount := int32(0)
	for _, parts := range b.partitions {
		for _, p := range parts {
			totalMsgs += p.MsgCount()
			totalBytes += p.BytesCount()
			partCount++
		}
	}
	return BrokerStats{
		ID:             b.ID,
		MessagesIn:     totalMsgs,
		BytesIn:        totalBytes,
		TopicCount:     int32(len(b.partitions)),
		PartitionCount: partCount,
	}
}

type BrokerStats struct {
	ID             int32
	MessagesIn     int64
	BytesIn        int64
	MessagesOut    int64
	BytesOut       int64
	TopicCount     int32
	PartitionCount int32
}

func (b *Broker) Shutdown(ctx context.Context) error {
	close(b.shutdownCh)
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, parts := range b.partitions {
		for _, p := range parts {
			p.Close()
		}
	}
	close(b.doneCh)
	return nil
}

func (b *Broker) getPartition(topic string, partition int32) (*Partition, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	parts, ok := b.partitions[topic]
	if !ok {
		return nil, fmt.Errorf("unknown topic: %s", topic)
	}
	p, ok := parts[partition]
	if !ok {
		return nil, fmt.Errorf("unknown partition: %s/%d", topic, partition)
	}
	return p, nil
}
