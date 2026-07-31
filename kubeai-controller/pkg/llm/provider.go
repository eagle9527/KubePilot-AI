// Package llm 定义了 LLM Provider 的接口和通用类型
// 用于统一调用不同的大模型服务
package llm

import (
	"context"
	"fmt"
)

// AnalysisRequest 是调用 LLM 进行分析的请求
type AnalysisRequest struct {
	// Resource 是发生异常的资源信息
	ResourceType string `json:"resourceType"`
	ResourceName string `json:"resourceName"`
	Namespace    string `json:"namespace"`

	// Event 是触发事件的信息
	EventType string `json:"eventType"`
	Message   string `json:"message"`
	Severity  string `json:"severity"`

	// Context 是上下文信息
	Logs    []string          `json:"logs,omitempty"`
	Metrics map[string]string `json:"metrics,omitempty"`
	Events  []string          `json:"events,omitempty"`
}

// AnalysisResult 是 LLM 分析的结果
type AnalysisResult struct {
	// Level 是风险级别: Critical, Warning, Info, Success
	Level string `json:"level"`

	// Reason 是问题原因分析
	Reason string `json:"reason"`

	// Confidence 是置信度 (0-1)
	Confidence float64 `json:"confidence"`

	// Suggestions 是修复建议列表
	Suggestions []string `json:"suggestions"`

	// AutoFixable 是否可以自动修复
	AutoFixable bool `json:"autoFixable,omitempty"`

	// FixCommand 是自动修复命令（如果可修复）
	FixCommand string `json:"fixCommand,omitempty"`

	// RawResponse 是 LLM 的原始响应
	RawResponse string `json:"rawResponse,omitempty"`
}

// Provider 是 LLM Provider 的接口
type Provider interface {
	// Analyze 调用 LLM 分析异常事件
	Analyze(ctx context.Context, req *AnalysisRequest) (*AnalysisResult, error)

	// Name 返回 Provider 的名称
	Name() string

	// HealthCheck 检查 Provider 是否可用
	HealthCheck(ctx context.Context) error
}

// Config 是 LLM Provider 的配置
type Config struct {
	// Provider 类型: openai, deepseek, qwen, claude, ollama
	Provider string `json:"provider"`

	// APIKey 是 API 密钥
	APIKey string `json:"apiKey"`

	// BaseURL 是 API 基础 URL（可选，用于私有化部署）
	BaseURL string `json:"baseURL,omitempty"`

	// Model 是模型名称
	Model string `json:"model"`

	// Temperature 控制输出的随机性 (0-2)
	Temperature float32 `json:"temperature,omitempty"`

	// MaxTokens 是最大输出 token 数
	MaxTokens int `json:"maxTokens,omitempty"`

	// Timeout 是请求超时时间（秒）
	Timeout int `json:"timeout,omitempty"`
}

// NewProvider 根据配置创建对应的 Provider
func NewProvider(cfg *Config) (Provider, error) {
	switch cfg.Provider {
	case "openai", "gpt":
		return NewOpenAIProvider(cfg), nil
	case "deepseek":
		return NewDeepSeekProvider(cfg), nil
	case "qwen", "tongyi":
		return NewQwenProvider(cfg), nil
	case "claude":
		return NewClaudeProvider(cfg), nil
	case "ollama":
		return NewOllamaProvider(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported LLM provider: %s", cfg.Provider)
	}
}
