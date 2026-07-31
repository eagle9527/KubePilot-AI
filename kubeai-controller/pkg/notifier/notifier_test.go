package notifier

import (
	"context"
	"sync"
	"testing"
	"time"
)

type countingNotifier struct {
	mu    sync.Mutex
	count int
	last  *Message
}

func (n *countingNotifier) Name() string { return "counting" }

func (n *countingNotifier) Send(ctx context.Context, msg *Message) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.count++
	copied := *msg
	n.last = &copied
	return nil
}

func (n *countingNotifier) HealthCheck(ctx context.Context) error { return nil }

func (n *countingNotifier) Count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.count
}

func (n *countingNotifier) Last() *Message {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.last == nil {
		return nil
	}
	copied := *n.last
	return &copied
}

func TestManager_SuppressionCooldownAndDigest(t *testing.T) {
	m := NewManager(&Filters{
		MinLevel:       Warning,
		CooldownPeriod: 5 * time.Second,
		MaxPerMinute:   100,
		DigestInterval: 1 * time.Second,
		DigestMinCount: 2,
		DigestMaxItems: 10,
	})
	n := &countingNotifier{}
	m.Register("counting", n)

	msg := &Message{
		Title: "Kubernetes 异常: MemoryPressure",
		Level: Warning,
		ResourceInfo: ResourceInfo{
			Cluster:   "kind",
			Namespace: "default",
			Kind:      "Node",
			Name:      "node-1",
		},
		Timestamp: time.Now(),
	}

	if err := m.Send(context.Background(), msg); err != nil {
		t.Fatalf("send1 err: %v", err)
	}
	if err := m.Send(context.Background(), msg); err != nil {
		t.Fatalf("send2 err: %v", err)
	}
	if err := m.Send(context.Background(), msg); err != nil {
		t.Fatalf("send3 err: %v", err)
	}

	if got := n.Count(); got != 1 {
		t.Fatalf("expected only 1 real send within cooldown, got %d", got)
	}

	if err := m.flushDigest(context.Background()); err != nil {
		t.Fatalf("flushDigest err: %v", err)
	}

	if got := n.Count(); got != 2 {
		t.Fatalf("expected digest to be sent once, total=2, got %d", got)
	}
	last := n.Last()
	if last == nil || last.Title != "KubePilot AI 告警收敛摘要" {
		t.Fatalf("expected last message to be digest, got %+v", last)
	}
}

func TestManager_RateLimit(t *testing.T) {
	m := NewManager(&Filters{
		MinLevel:       Warning,
		CooldownPeriod: 0,
		MaxPerMinute:   1,
		DigestInterval: 1 * time.Second,
		DigestMinCount: 1,
		DigestMaxItems: 10,
	})
	n := &countingNotifier{}
	m.Register("counting", n)

	msg1 := &Message{
		Title: "A",
		Level: Warning,
		ResourceInfo: ResourceInfo{
			Cluster:   "kind",
			Namespace: "default",
			Kind:      "Pod",
			Name:      "p1",
		},
		Timestamp: time.Now(),
	}
	msg2 := &Message{
		Title: "B",
		Level: Warning,
		ResourceInfo: ResourceInfo{
			Cluster:   "kind",
			Namespace: "default",
			Kind:      "Pod",
			Name:      "p2",
		},
		Timestamp: time.Now(),
	}

	_ = m.Send(context.Background(), msg1)
	_ = m.Send(context.Background(), msg2)

	if got := n.Count(); got != 1 {
		t.Fatalf("expected only 1 real send due to rate limit, got %d", got)
	}
}
