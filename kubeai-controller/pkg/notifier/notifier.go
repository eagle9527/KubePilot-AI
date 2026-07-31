// Package notifier 实现了多种通知渠道
// 支持企业微信、钉钉、Slack 和通用 Webhook
package notifier

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

type NoiseDecision string

const (
	NoiseDecisionSend       NoiseDecision = "send"
	NoiseDecisionSuppressed NoiseDecision = "suppressed"
)

// Message 是通知消息的结构
type Message struct {
	Title          string
	Content        string
	Level          MessageLevel
	ResourceInfo   ResourceInfo
	AnalysisResult string
	Suggestions    []string
	Timestamp      time.Time
	ForceSend      bool
}

// MessageLevel 是消息级别
type MessageLevel string

const (
	Critical MessageLevel = "Critical"
	High     MessageLevel = "High"
	Warning  MessageLevel = "Warning"
	Info     MessageLevel = "Info"
	Success  MessageLevel = "Success"
)

// ResourceInfo 是资源信息
type ResourceInfo struct {
	Kind      string
	Name      string
	Namespace string
	Cluster   string
}

// Notifier 是通知器的接口
type Notifier interface {
	Name() string
	Send(ctx context.Context, msg *Message) error
	HealthCheck(ctx context.Context) error
}

// Manager 管理多个通知器
type Manager struct {
	notifiers map[string]Notifier
	filters   *Filters
	lastSent  map[string]time.Time
	suppCount map[string]int
	suppStats map[string]*suppressionStats
	lastFlush time.Time
	window    []time.Time
	mu        sync.RWMutex
}

type suppressionStats struct {
	Count    int
	FirstAt  time.Time
	LastAt   time.Time
	Title    string
	Level    MessageLevel
	Resource ResourceInfo
}

// Filters 是通知过滤规则
type Filters struct {
	MinLevel          MessageLevel
	IncludeNamespaces []string
	ExcludeNamespaces []string
	IncludeResources  []string
	ExcludeResources  []string
	CooldownPeriod    time.Duration
	MaxPerMinute      int
	DigestInterval    time.Duration
	DigestMinCount    int
	DigestMaxItems    int
}

// NewManager 创建一个新的通知管理器
func NewManager(filters *Filters) *Manager {
	if filters == nil {
		filters = &Filters{
			MinLevel:       Warning,
			CooldownPeriod: 5 * time.Minute,
		}
	}
	if filters.DigestInterval <= 0 {
		if filters.CooldownPeriod > 0 {
			filters.DigestInterval = filters.CooldownPeriod
		} else {
			filters.DigestInterval = 1 * time.Minute
		}
	}
	if filters.DigestMinCount <= 0 {
		filters.DigestMinCount = 3
	}
	if filters.DigestMaxItems <= 0 {
		filters.DigestMaxItems = 10
	}
	return &Manager{
		notifiers: make(map[string]Notifier),
		filters:   filters,
		lastSent:  make(map[string]time.Time),
		suppCount: make(map[string]int),
		suppStats: make(map[string]*suppressionStats),
	}
}

// Register 注册一个通知器
func (m *Manager) Register(name string, notifier Notifier) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notifiers[name] = notifier
}

// Send 发送通知到所有已注册的通知器
func (m *Manager) Send(ctx context.Context, msg *Message) error {
	l := log.FromContext(ctx).WithName("notifier")

	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	decision, key, reason := m.decideAndRecord(msg)
	if decision == NoiseDecisionSuppressed {
		l.Info("notification suppressed",
			"reason", reason,
			"title", msg.Title,
			"level", msg.Level,
			"kind", msg.ResourceInfo.Kind,
			"namespace", msg.ResourceInfo.Namespace,
			"name", msg.ResourceInfo.Name,
		)
		return nil
	}

	m.mu.Lock()
	notifiers := make(map[string]Notifier, len(m.notifiers))
	for name, n := range m.notifiers {
		notifiers[name] = n
	}
	suppressed := m.suppCount[key]
	m.suppCount[key] = 0
	m.mu.Unlock()

	if len(notifiers) == 0 {
		l.Error(fmt.Errorf("no notifiers registered"), "notification send failed",
			"title", msg.Title,
			"level", msg.Level,
			"kind", msg.ResourceInfo.Kind,
			"namespace", msg.ResourceInfo.Namespace,
			"name", msg.ResourceInfo.Name,
		)
		return fmt.Errorf("no notifiers registered")
	}

	if suppressed > 0 {
		msg.Title = fmt.Sprintf("%s (已收敛%d次)", msg.Title, suppressed)
	}

	var errs []error
	for name, notifier := range notifiers {
		if err := notifier.Send(ctx, msg); err != nil {
			l.Error(err, "notification send failed",
				"notifier", name,
				"title", msg.Title,
				"level", msg.Level,
				"kind", msg.ResourceInfo.Kind,
				"namespace", msg.ResourceInfo.Namespace,
				"name", msg.ResourceInfo.Name,
			)
			errs = append(errs, fmt.Errorf("failed to send via %s: %w", name, err))
			continue
		}
		l.Info("notification sent",
			"notifier", name,
			"title", msg.Title,
			"level", msg.Level,
			"kind", msg.ResourceInfo.Kind,
			"namespace", msg.ResourceInfo.Namespace,
			"name", msg.ResourceInfo.Name,
		)
	}

	if len(errs) > 0 {
		return fmt.Errorf("notification errors: %v", errs)
	}
	return nil
}

func (m *Manager) Start(ctx context.Context) {
	interval := m.filters.DigestInterval
	if interval <= 0 {
		interval = 1 * time.Minute
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = m.flushDigest(ctx)
			}
		}
	}()
}

func (m *Manager) flushDigest(ctx context.Context) error {
	now := time.Now()

	m.mu.Lock()
	if !m.lastFlush.IsZero() && now.Sub(m.lastFlush) < m.filters.DigestInterval/2 {
		m.mu.Unlock()
		return nil
	}
	m.lastFlush = now

	items := make([]*suppressionStats, 0, len(m.suppStats))
	for _, st := range m.suppStats {
		if st.Count >= m.filters.DigestMinCount {
			items = append(items, st)
		}
	}
	if len(items) == 0 {
		m.mu.Unlock()
		return nil
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Count > items[j].Count
	})
	if len(items) > m.filters.DigestMaxItems {
		items = items[:m.filters.DigestMaxItems]
	}

	notifiers := make(map[string]Notifier, len(m.notifiers))
	for name, n := range m.notifiers {
		notifiers[name] = n
	}

	for k, st := range m.suppStats {
		if st.Count >= m.filters.DigestMinCount {
			delete(m.suppStats, k)
		}
	}
	m.mu.Unlock()

	level := Warning
	for _, it := range items {
		if it.Level == Critical {
			level = Critical
			break
		}
	}

	var b strings.Builder
	b.WriteString("告警收敛摘要（自动汇总）\n\n")
	b.WriteString(fmt.Sprintf("时间：%s\n\n", now.Format("2006-01-02 15:04:05")))
	for i, it := range items {
		b.WriteString(fmt.Sprintf("%d) %s\n", i+1, it.Title))
		b.WriteString(fmt.Sprintf("   资源：%s/%s/%s\n", it.Resource.Namespace, it.Resource.Kind, it.Resource.Name))
		b.WriteString(fmt.Sprintf("   收敛：%d 次（%s ~ %s）\n\n", it.Count, it.FirstAt.Format("15:04:05"), it.LastAt.Format("15:04:05")))
	}

	digest := &Message{
		Title:          "KubePilot AI 告警收敛摘要",
		Level:          level,
		AnalysisResult: b.String(),
		Timestamp:      now,
		ResourceInfo: ResourceInfo{
			Kind:      "AIIncident",
			Name:      "digest",
			Namespace: "kubeai-system",
		},
	}

	var errs []error
	for name, n := range notifiers {
		if err := n.Send(ctx, digest); err != nil {
			errs = append(errs, fmt.Errorf("failed to send digest via %s: %w", name, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("digest notification errors: %v", errs)
	}
	return nil
}

func (m *Manager) decideAndRecord(msg *Message) (NoiseDecision, string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !msg.ForceSend && !m.levelAllowed(msg.Level) {
		return NoiseDecisionSuppressed, "", "levelFiltered"
	}
	if !m.namespaceAllowed(msg.ResourceInfo.Namespace) {
		return NoiseDecisionSuppressed, "", "namespaceFiltered"
	}
	if !m.resourceAllowed(msg.ResourceInfo.Kind) {
		return NoiseDecisionSuppressed, "", "resourceFiltered"
	}

	key := m.dedupKey(msg)
	now := msg.Timestamp

	if m.filters.CooldownPeriod > 0 {
		if last, ok := m.lastSent[key]; ok && now.Sub(last) < m.filters.CooldownPeriod {
			m.suppCount[key]++
			m.recordSuppression(key, msg)
			return NoiseDecisionSuppressed, key, "cooldown"
		}
	}

	if m.filters.MaxPerMinute > 0 {
		cutoff := now.Add(-1 * time.Minute)
		n := 0
		for _, t := range m.window {
			if t.After(cutoff) {
				m.window[n] = t
				n++
			}
		}
		m.window = m.window[:n]
		if len(m.window) >= m.filters.MaxPerMinute {
			m.suppCount[key]++
			m.recordSuppression(key, msg)
			return NoiseDecisionSuppressed, key, "rateLimit"
		}
		m.window = append(m.window, now)
	}

	m.lastSent[key] = now
	return NoiseDecisionSend, key, ""
}

func (m *Manager) recordSuppression(key string, msg *Message) {
	st, ok := m.suppStats[key]
	if !ok {
		st = &suppressionStats{
			Title:    msg.Title,
			Level:    msg.Level,
			Resource: msg.ResourceInfo,
			FirstAt:  msg.Timestamp,
		}
		m.suppStats[key] = st
	}
	st.Count++
	st.LastAt = msg.Timestamp
	if st.FirstAt.IsZero() {
		st.FirstAt = msg.Timestamp
	}
	if st.Title == "" {
		st.Title = msg.Title
	}
	if st.Level == "" {
		st.Level = msg.Level
	}
}

func (m *Manager) dedupKey(msg *Message) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", msg.ResourceInfo.Cluster, msg.ResourceInfo.Namespace, msg.ResourceInfo.Kind, msg.ResourceInfo.Name, msg.Title)
}

func (m *Manager) levelAllowed(level MessageLevel) bool {
	levels := map[MessageLevel]int{
		Critical: 5,
		High:     4,
		Warning:  3,
		Info:     2,
		Success:  1,
	}
	min, ok := levels[m.filters.MinLevel]
	if !ok {
		min = 3
	}
	cur, ok := levels[level]
	if !ok {
		cur = 0
	}
	return cur >= min
}

func (m *Manager) namespaceAllowed(ns string) bool {
	for _, v := range m.filters.ExcludeNamespaces {
		if v == ns {
			return false
		}
	}
	if len(m.filters.IncludeNamespaces) == 0 {
		return true
	}
	for _, v := range m.filters.IncludeNamespaces {
		if v == ns {
			return true
		}
	}
	return false
}

func (m *Manager) resourceAllowed(kind string) bool {
	for _, v := range m.filters.ExcludeResources {
		if v == kind {
			return false
		}
	}
	if len(m.filters.IncludeResources) == 0 {
		return true
	}
	for _, v := range m.filters.IncludeResources {
		if v == kind {
			return true
		}
	}
	return false
}
