package consumer

import (
	"testing"
	"time"
)

func TestGroupJoinLeave(t *testing.T) {
	g := NewGroup("g1", 30*time.Second)

	gen, members, _ := g.Join("m1", []string{"topic-a"})
	if gen != 0 {
		t.Errorf("expected gen=0, got %d", gen)
	}
	if len(members) != 1 {
		t.Errorf("expected 1 member, got %d", len(members))
	}

	g.Join("m2", []string{"topic-a"})
	if len(g.Members()) != 2 {
		t.Errorf("expected 2 members, got %d", len(g.Members()))
	}

	g.Leave("m1")
	if len(g.Members()) != 1 {
		t.Errorf("expected 1 member after leave, got %d", len(g.Members()))
	}
}

func TestGroupRebalance(t *testing.T) {
	g := NewGroup("g2", 30*time.Second)
	g.SetTopicPartitionCount("orders", 4)
	g.Join("m1", []string{"orders"})
	g.Join("m2", []string{"orders"})

	assignment := g.Rebalance()

	if len(assignment) != 2 {
		t.Fatalf("expected assignment for 2 members, got %d", len(assignment))
	}
	total := 0
	for _, parts := range assignment {
		total += len(parts)
	}
	if total != 4 {
		t.Errorf("expected 4 total partitions assigned, got %d", total)
	}
}

func TestGroupOffsetCommit(t *testing.T) {
	g := NewGroup("g3", 30*time.Second)
	g.CommitOffset("orders", 0, 42)
	if off := g.FetchOffset("orders", 0); off != 42 {
		t.Errorf("expected offset 42, got %d", off)
	}
}
