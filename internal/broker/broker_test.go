package broker

import (
	"os"
	"testing"
)

func TestPartitionAppendFetch(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartition(0, "test-topic", dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewPartition: %v", err)
	}
	defer p.Close()

	for i := 0; i < 100; i++ {
		_, err := p.Append([]byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// allow batch flush
	msgs, hwm := p.Fetch(0, 1024*1024)
	if len(msgs) == 0 {
		t.Fatalf("expected messages, got 0; hwm=%d", hwm)
	}
	if hwm != 100 {
		t.Errorf("expected hwm=100, got %d", hwm)
	}
}

func TestBrokerCreateAndPublish(t *testing.T) {
	dir, _ := os.MkdirTemp("", "broker-test-*")
	defer os.RemoveAll(dir)

	cfg := DefaultConfig(1, ":19092", ":19093", ":19095")
	cfg.Storage.DataDir = dir
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := b.CreateTopic("events", 3); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	offset, err := b.Publish("events", 0, []byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if offset < 0 {
		t.Errorf("bad offset: %d", offset)
	}

	msgs, hwm, err := b.Fetch("events", 0, 0, 1024*1024)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) == 0 {
		t.Errorf("expected 1 message, hwm=%d", hwm)
	}
}

func TestBrokerListTopics(t *testing.T) {
	dir, _ := os.MkdirTemp("", "broker-list-*")
	defer os.RemoveAll(dir)

	cfg := DefaultConfig(1, ":0", ":0", ":0")
	cfg.Storage.DataDir = dir
	b, _ := New(cfg)

	b.CreateTopic("a", 1)
	b.CreateTopic("b", 2)

	topics := b.ListTopics()
	if len(topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(topics))
	}
}
