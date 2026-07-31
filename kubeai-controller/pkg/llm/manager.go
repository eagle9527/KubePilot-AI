package llm

import (
	"context"
	"fmt"
	"sync"
)

// Manager 管理多个 LLM Provider
type Manager struct {
	providers map[string]Provider
	defaultProvider string
	mu        sync.RWMutex
}

// NewManager 创建一个新的 Provider Manager
func NewManager() *Manager {
	return &Manager{
		providers: make(map[string]Provider),
	}
}

// RegisterProvider 注册一个 Provider
func (m *Manager) RegisterProvider(name string, provider Provider) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.providers[name] = provider
}

// SetDefaultProvider 设置默认 Provider
func (m *Manager) SetDefaultProvider(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.providers[name]; !exists {
		return fmt.Errorf("provider %s not found", name)
	}

	m.defaultProvider = name
	return nil
}

// GetProvider 获取指定名称的 Provider
func (m *Manager) GetProvider(name string) (Provider, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if name == "" {
		name = m.defaultProvider
	}

	provider, exists := m.providers[name]
	if !exists {
		return nil, fmt.Errorf("provider %s not found", name)
	}

	return provider, nil
}

// GetDefaultProvider 获取默认 Provider
func (m *Manager) GetDefaultProvider() (Provider, error) {
	return m.GetProvider("")
}

// Analyze 使用指定的 Provider 进行分析
func (m *Manager) Analyze(ctx context.Context, providerName string, req *AnalysisRequest) (*AnalysisResult, error) {
	provider, err := m.GetProvider(providerName)
	if err != nil {
		return nil, err
	}

	return provider.Analyze(ctx, req)
}

// HealthCheck 检查指定 Provider 的健康状态
func (m *Manager) HealthCheck(ctx context.Context, providerName string) error {
	provider, err := m.GetProvider(providerName)
	if err != nil {
		return err
	}

	return provider.HealthCheck(ctx)
}

// HealthCheckAll 检查所有 Provider 的健康状态
func (m *Manager) HealthCheckAll(ctx context.Context) map[string]error {
	m.mu.RLock()
	providers := make(map[string]Provider)
	for name, p := range m.providers {
		providers[name] = p
	}
	m.mu.RUnlock()

	results := make(map[string]error)
	for name, provider := range providers {
		results[name] = provider.HealthCheck(ctx)
	}

	return results
}

// ListProviders 列出所有已注册的 Provider 名称
func (m *Manager) ListProviders() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.providers))
	for name := range m.providers {
		names = append(names, name)
	}

	return names
}

// InitializeFromConfig 从配置初始化 Manager
func (m *Manager) InitializeFromConfig(configs map[string]*Config, defaultProvider string) error {
	for name, cfg := range configs {
		provider, err := NewProvider(cfg)
		if err != nil {
			return fmt.Errorf("failed to create provider %s: %w", name, err)
		}

		m.RegisterProvider(name, provider)
	}

	if defaultProvider != "" {
		if err := m.SetDefaultProvider(defaultProvider); err != nil {
			return err
		}
	}

	return nil
}
