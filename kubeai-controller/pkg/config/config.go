package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kubepilot-ai/kubeai-controller/pkg/llm"
)

// ControllerConfig 是 Controller 的全局配置
type ControllerConfig struct {
	// LLM 相关配置
	LLM LLMConfig `json:"llm"`

	// 通知相关配置
	Notification NotificationConfig `json:"notification"`

	// 自动修复相关配置
	AutoRemediation RemediationConfig `json:"autoRemediation"`

	// 巡检相关配置
	Inspection InspectionConfig `json:"inspection"`

	// Controller 通用配置
	Controller GeneralConfig `json:"controller"`
}

// LLMConfig 是 LLM 相关配置
type LLMConfig struct {
	// Providers 是 LLM Provider 配置列表
	Providers map[string]*llm.Config `json:"providers"`

	// DefaultProvider 是默认使用的 Provider
	DefaultProvider string `json:"defaultProvider"`

	// RetryCount 是调用 LLM 失败时的重试次数
	RetryCount int `json:"retryCount"`

	// RetryDelay 是重试间隔
	RetryDelay time.Duration `json:"retryDelay"`

	// Timeout 是 LLM 调用超时时间
	Timeout time.Duration `json:"timeout"`
}

// NotificationConfig 是通知相关配置
type NotificationConfig struct {
	// WeChat 是企业微信配置
	WeChat WeChatConfig `json:"wechat"`

	// DingTalk 是钉钉配置
	DingTalk DingTalkConfig `json:"dingtalk"`

	// Slack 是 Slack 配置
	Slack SlackConfig `json:"slack"`

	// Webhook 是通用 Webhook 配置
	Webhook WebhookConfig `json:"webhook"`

	// Enabled 是否启用通知
	Enabled bool `json:"enabled"`

	// CooldownPeriod 是通知冷却期（相同事件再次通知的间隔）
	CooldownPeriod time.Duration `json:"cooldownPeriod"`

	// Filter 是通知过滤规则
	Filter NotificationFilter `json:"filter"`
}

// WeChatConfig 是企业微信配置
type WeChatConfig struct {
	// Enabled 是否启用
	Enabled bool `json:"enabled"`

	// WebhookURL 是企业微信机器人 Webhook URL
	WebhookURL string `json:"webhookURL"`

	// Key 是企业微信机器人 Key（从 Webhook URL 中提取）
	Key string `json:"key"`

	// MentionedList 是 @提及的用户列表
	MentionedList []string `json:"mentionedList"`

	// MentionedMobileList 是 @提及的手机号列表
	MentionedMobileList []string `json:"mentionedMobileList"`
}

// DingTalkConfig 是钉钉配置
type DingTalkConfig struct {
	// Enabled 是否启用
	Enabled bool `json:"enabled"`

	// WebhookURL 是钉钉机器人 Webhook URL
	WebhookURL string `json:"webhookURL"`

	// AccessToken 是钉钉机器人 Access Token
	AccessToken string `json:"accessToken"`

	// Secret 是钉钉机器人密钥（用于签名）
	Secret string `json:"secret"`

	// AtMobiles 是 @提及的手机号列表
	AtMobiles []string `json:"atMobiles"`

	// IsAtAll 是否 @所有人
	IsAtAll bool `json:"isAtAll"`
}

// SlackConfig 是 Slack 配置
type SlackConfig struct {
	// Enabled 是否启用
	Enabled bool `json:"enabled"`

	// WebhookURL 是 Slack Webhook URL
	WebhookURL string `json:"webhookURL"`

	// Channel 是消息发送的频道
	Channel string `json:"channel"`

	// Username 是发送者用户名
	Username string `json:"username"`

	// IconEmoji 是发送者图标
	IconEmoji string `json:"iconEmoji"`
}

// WebhookConfig 是通用 Webhook 配置
type WebhookConfig struct {
	// Enabled 是否启用
	Enabled bool `json:"enabled"`

	// URL 是 Webhook 地址
	URL string `json:"url"`

	// Method 是请求方法（GET/POST）
	Method string `json:"method"`

	// Headers 是自定义请求头
	Headers map[string]string `json:"headers"`

	// Timeout 是请求超时时间
	Timeout time.Duration `json:"timeout"`
}

// NotificationFilter 是通知过滤规则
type NotificationFilter struct {
	// MinLevel 是最低通知级别（低于此级别不通知）
	MinLevel string `json:"minLevel"`

	// IncludeNamespaces 是包含的命名空间列表（为空表示包含所有）
	IncludeNamespaces []string `json:"includeNamespaces"`

	// ExcludeNamespaces 是排除的命名空间列表
	ExcludeNamespaces []string `json:"excludeNamespaces"`

	// IncludeResources 是包含的资源类型列表
	IncludeResources []string `json:"includeResources"`

	// ExcludeResources 是排除的资源类型列表
	ExcludeResources []string `json:"excludeResources"`
}

// RemediationConfig 是自动修复相关配置
type RemediationConfig struct {
	// Enabled 是否启用自动修复
	Enabled bool `json:"enabled"`

	// Level 是自动修复级别
	// Level 1: 只分析不修复
	// Level 2: 生成修复方案但需人工确认
	// Level 3: 自动执行修复
	Level int `json:"level"`

	// ApprovalTimeout 是等待审批的超时时间
	ApprovalTimeout time.Duration `json:"approvalTimeout"`

	// DryRun 是否只模拟执行不实际修改
	DryRun bool `json:"dryRun"`

	// ExcludedNamespaces 是排除自动修复的命名空间
	ExcludedNamespaces []string `json:"excludedNamespaces"`

	// AllowedOperations 是允许执行的修复操作类型
	AllowedOperations []string `json:"allowedOperations"`
}

// InspectionConfig 是巡检相关配置
type InspectionConfig struct {
	// Enabled 是否启用自动巡检
	Enabled bool `json:"enabled"`

	// Schedule 是巡检 Cron 表达式
	Schedule string `json:"schedule"`

	// Timeout 是单次巡检的超时时间
	Timeout time.Duration `json:"timeout"`

	// Resources 是巡检的资源类型
	Resources []string `json:"resources"`

	// NotifyOnSuccess 是否成功时也发送通知
	NotifyOnSuccess bool `json:"notifyOnSuccess"`

	// ReportRetention 是报告保留数量
	ReportRetention int `json:"reportRetention"`
}

// GeneralConfig 是 Controller 通用配置
type GeneralConfig struct {
	// Namespace 是 Controller 部署的命名空间
	Namespace string `json:"namespace"`

	// Workers 是并发工作线程数
	Workers int `json:"workers"`

	// ResyncPeriod 是重新同步周期
	ResyncPeriod time.Duration `json:"resyncPeriod"`

	// QueueSize 是工作队列大小
	QueueSize int `json:"queueSize"`

	// MetricsPort 是监控端口
	MetricsPort int `json:"metricsPort"`

	// HealthPort 是健康检查端口
	HealthPort int `json:"healthPort"`

	// LogLevel 是日志级别
	LogLevel string `json:"logLevel"`
}

// LoadConfig 从环境变量加载配置
func LoadConfig() (*ControllerConfig, error) {
	config := &ControllerConfig{
		LLM: LLMConfig{
			Providers:  make(map[string]*llm.Config),
			RetryCount: 3,
			RetryDelay: 5 * time.Second,
			Timeout:    60 * time.Second,
		},
		Notification: NotificationConfig{
			CooldownPeriod: 5 * time.Minute,
		},
		AutoRemediation: RemediationConfig{
			Level:           1,
			ApprovalTimeout: 30 * time.Minute,
		},
		Inspection: InspectionConfig{
			Enabled:         false,
			Schedule:        "0 2 * * *",
			Timeout:         30 * time.Minute,
			Resources:       []string{"pods", "nodes", "deployments", "services"},
			ReportRetention: 30,
		},
		Controller: GeneralConfig{
			Namespace:    getEnv("POD_NAMESPACE", "kubeai-system"),
			Workers:      getEnvInt("WORKERS", 5),
			ResyncPeriod: getEnvDuration("RESYNC_PERIOD", 10*time.Minute),
			QueueSize:    getEnvInt("QUEUE_SIZE", 100),
			MetricsPort:  getEnvInt("METRICS_PORT", 8080),
			HealthPort:   getEnvInt("HEALTH_PORT", 8081),
			LogLevel:     getEnv("LOG_LEVEL", "info"),
		},
	}

	// 加载默认 Provider
	config.LLM.DefaultProvider = getEnv("LLM_DEFAULT_PROVIDER", "deepseek")

	// 加载 DeepSeek 配置
	if apiKey := getEnv("DEEPSEEK_API_KEY", ""); apiKey != "" {
		config.LLM.Providers["deepseek"] = &llm.Config{
			Provider:    "deepseek",
			APIKey:      apiKey,
			BaseURL:     getEnv("DEEPSEEK_BASE_URL", ""),
			Model:       getEnv("DEEPSEEK_MODEL", "deepseek-chat"),
			Temperature: float32(getEnvFloat("LLM_TEMPERATURE", 0.3)),
			MaxTokens:   getEnvInt("LLM_MAX_TOKENS", 2000),
			Timeout:     getEnvInt("LLM_TIMEOUT", 60),
		}
	}

	// 加载 OpenAI 配置
	if apiKey := getEnv("OPENAI_API_KEY", ""); apiKey != "" {
		config.LLM.Providers["openai"] = &llm.Config{
			Provider:    "openai",
			APIKey:      apiKey,
			BaseURL:     getEnv("OPENAI_BASE_URL", ""),
			Model:       getEnv("OPENAI_MODEL", "gpt-4o"),
			Temperature: float32(getEnvFloat("LLM_TEMPERATURE", 0.3)),
			MaxTokens:   getEnvInt("LLM_MAX_TOKENS", 2000),
			Timeout:     getEnvInt("LLM_TIMEOUT", 60),
		}
	}

	// 加载通义千问配置
	if apiKey := getEnv("QWEN_API_KEY", ""); apiKey != "" {
		config.LLM.Providers["qwen"] = &llm.Config{
			Provider:    "qwen",
			APIKey:      apiKey,
			BaseURL:     getEnv("QWEN_BASE_URL", ""),
			Model:       getEnv("QWEN_MODEL", "qwen-turbo"),
			Temperature: float32(getEnvFloat("LLM_TEMPERATURE", 0.3)),
			MaxTokens:   getEnvInt("LLM_MAX_TOKENS", 2000),
			Timeout:     getEnvInt("LLM_TIMEOUT", 60),
		}
	}

	// 加载 Ollama 配置（本地模型）
	if baseURL := getEnv("OLLAMA_BASE_URL", ""); baseURL != "" {
		config.LLM.Providers["ollama"] = &llm.Config{
			Provider:    "ollama",
			APIKey:      "", // Ollama 通常不需要 API Key
			BaseURL:     baseURL,
			Model:       getEnv("OLLAMA_MODEL", "llama2"),
			Temperature: float32(getEnvFloat("LLM_TEMPERATURE", 0.3)),
			MaxTokens:   getEnvInt("LLM_MAX_TOKENS", 2000),
			Timeout:     getEnvInt("OLLAMA_TIMEOUT", 120),
		}
	}

	// 加载通知配置
	config.Notification.Enabled = getEnvBool("NOTIFICATION_ENABLED", true)
	config.Notification.CooldownPeriod = getEnvDuration("NOTIFICATION_COOLDOWN", 5*time.Minute)

	// 企业微信配置
	config.Notification.WeChat.Enabled = getEnvBool("WECHAT_ENABLED", false)
	config.Notification.WeChat.WebhookURL = getEnv("WECHAT_WEBHOOK_URL", "")
	config.Notification.WeChat.Key = getEnv("WECHAT_KEY", "")
	if mentionedList := getEnv("WECHAT_MENTIONED_LIST", ""); mentionedList != "" {
		config.Notification.WeChat.MentionedList = strings.Split(mentionedList, ",")
	}
	if mentionedMobileList := getEnv("WECHAT_MENTIONED_MOBILE_LIST", ""); mentionedMobileList != "" {
		config.Notification.WeChat.MentionedMobileList = strings.Split(mentionedMobileList, ",")
	}

	// 钉钉配置
	config.Notification.DingTalk.Enabled = getEnvBool("DINGTALK_ENABLED", false)
	config.Notification.DingTalk.WebhookURL = getEnv("DINGTALK_WEBHOOK_URL", "")
	config.Notification.DingTalk.AccessToken = getEnv("DINGTALK_ACCESS_TOKEN", "")
	config.Notification.DingTalk.Secret = getEnv("DINGTALK_SECRET", "")
	config.Notification.DingTalk.IsAtAll = getEnvBool("DINGTALK_IS_AT_ALL", false)

	// 自动修复配置
	config.AutoRemediation.Enabled = getEnvBool("AUTO_REMEDIATION_ENABLED", false)
	config.AutoRemediation.Level = getEnvInt("AUTO_REMEDIATION_LEVEL", 1)
	config.AutoRemediation.DryRun = getEnvBool("AUTO_REMEDIATION_DRY_RUN", true)
	config.AutoRemediation.ApprovalTimeout = getEnvDuration("AUTO_REMEDIATION_APPROVAL_TIMEOUT", 30*time.Minute)

	// 巡检配置
	config.Inspection.Enabled = getEnvBool("INSPECTION_ENABLED", false)
	config.Inspection.Schedule = getEnv("INSPECTION_SCHEDULE", "0 2 * * *")
	config.Inspection.Timeout = getEnvDuration("INSPECTION_TIMEOUT", 30*time.Minute)
	config.Inspection.NotifyOnSuccess = getEnvBool("INSPECTION_NOTIFY_ON_SUCCESS", false)

	// 通知过滤配置
	config.Notification.Filter.MinLevel = getEnv("NOTIFICATION_MIN_LEVEL", "Warning")
	if nsList := getEnv("NOTIFICATION_INCLUDE_NAMESPACES", ""); nsList != "" {
		config.Notification.Filter.IncludeNamespaces = strings.Split(nsList, ",")
	}
	if nsList := getEnv("NOTIFICATION_EXCLUDE_NAMESPACES", ""); nsList != "" {
		config.Notification.Filter.ExcludeNamespaces = strings.Split(nsList, ",")
	}

	return config, nil
}

// 辅助函数

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}
