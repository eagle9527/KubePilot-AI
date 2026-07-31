package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	defaultClaudeBaseURL = "https://api.anthropic.com"
	defaultClaudeModel   = "claude-3-sonnet-20240229"
)

// ClaudeProvider 实现了 Claude API 的调用
type ClaudeProvider struct {
	config *Config
	client *http.Client
}

// ClaudeRequest 是 Claude API 的请求结构
type ClaudeRequest struct {
	Model     string          `json:"model"`
	MaxTokens int             `json:"max_tokens"`
	Messages  []ClaudeMessage `json:"messages"`
	System    string          `json:"system,omitempty"`
	Temperature *float32    `json:"temperature,omitempty"`
}

// ClaudeMessage 是 Claude 的消息结构
type ClaudeMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ClaudeResponse 是 Claude API 的响应结构
type ClaudeResponse struct {
	ID           string `json:"id"`
	Type         string `json:"type"`
	Role         string `json:"role"`
	Model        string `json:"model"`
	Content      []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	StopReason   string `json:"stop_reason"`
	StopSequence string `json:"stop_sequence"`
	Usage        struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
	Error *ClaudeError `json:"error,omitempty"`
}

// ClaudeError 是 Claude API 的错误结构
type ClaudeError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

func (e *ClaudeError) Error() string {
	return fmt.Sprintf("Claude API error: %s - %s", e.Type, e.Message)
}

// NewClaudeProvider 创建 Claude Provider 实例
func NewClaudeProvider(config *Config) *ClaudeProvider {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 60
	}

	return &ClaudeProvider{
		config: config,
		client: &http.Client{
			Timeout: time.Duration(timeout) * time.Second,
		},
	}
}

// Name 返回 Provider 名称
func (p *ClaudeProvider) Name() string {
	return "claude"
}

// Analyze 调用 Claude API 分析异常事件
func (p *ClaudeProvider) Analyze(ctx context.Context, req *AnalysisRequest) (*AnalysisResult, error) {
	// 构建系统提示词
	systemPrompt := `你是 KubePilot AI，一名专业的 Kubernetes SRE 专家，擅长分析集群异常事件。

你的任务是分析 Kubernetes 异常事件，并提供结构化的诊断结果。

分析维度：
1. 根因分析 - 确定问题的根本原因
2. 风险级别 - Critical/Warning/Info/Success
3. 置信度 - 0-1 之间的小数
4. 修复建议 - 具体的 actionable 建议
5. 是否可自动修复

输出必须是有效的 JSON 格式：
{
  "level": "Warning",
  "reason": "详细的根因分析...",
  "confidence": 0.92,
  "suggestions": ["建议1", "建议2"],
  "autoFixable": false,
  "fixCommand": ""
}

注意：
- JSON 必须有效，不要添加 markdown 代码块标记
- 分析要简洁但专业
- 对于不确定的信息，降低置信度
- 建议要具体可操作`;

	// 构建用户提示词
	userPrompt := fmt.Sprintf(`请分析以下 Kubernetes 异常事件：

【资源信息】
- 类型: %s
- 名称: %s
- 命名空间: %s

【事件信息】
- 类型: %s
- 严重程度: %s
- 消息: %s

【上下文信息】
日志片段:
%s

指标数据:
%s

相关事件:
%s

请提供结构化的分析结果。`,
		req.ResourceType, req.ResourceName, req.Namespace,
		req.EventType, req.Severity, req.Message,
		formatLogs(req.Logs),
		formatMetrics(req.Metrics),
		formatEvents(req.Events))

	// 构建 Claude 请求
	model := p.config.Model
	if model == "" {
		model = defaultClaudeModel
	}

	temp := p.config.Temperature
	if temp == 0 {
		temp = 0.3
	}

	maxTokens := p.config.MaxTokens
	if maxTokens == 0 {
		maxTokens = 2000
	}

	claudeReq := ClaudeRequest{
		Model:     model,
		MaxTokens: maxTokens,
		Messages: []ClaudeMessage{
			{Role: "user", Content: userPrompt},
		},
		System:      systemPrompt,
		Temperature: &temp,
	}

	// 发送请求
	baseURL := p.config.BaseURL
	if baseURL == "" {
		baseURL = defaultClaudeBaseURL
	}

	reqBody, err := json.Marshal(claudeReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Claude request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/v1/messages", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", p.config.APIKey)
	httpReq.Header.Set("anthropic-version", "2023-06-01")

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to Claude: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// 解析响应
	var claudeResp ClaudeResponse
	if err := json.Unmarshal(body, &claudeResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Claude response: %w", err)
	}

	// 检查 API 错误
	if claudeResp.Error != nil {
		return nil, claudeResp.Error
	}

	if len(claudeResp.Content) == 0 {
		return nil, fmt.Errorf("no content in Claude response")
	}

	// 解析 LLM 返回的 JSON
	content := claudeResp.Content[0].Text

	var result AnalysisResult
	if err := parseLLMJSONResponse(content, &result); err != nil {
		// 如果解析失败，使用原始内容作为 reason
		result = AnalysisResult{
			Level:       "Warning",
			Reason:      content,
			Confidence:  0.5,
			Suggestions: []string{"请人工检查详细日志"},
			RawResponse: content,
		}
	}

	result.RawResponse = content
	return &result, nil
}

// HealthCheck 检查 Provider 是否可用
func (p *ClaudeProvider) HealthCheck(ctx context.Context) error {
	baseURL := p.config.BaseURL
	if baseURL == "" {
		baseURL = defaultClaudeBaseURL
	}

	req, err := http.NewRequestWithContext(ctx, "GET", baseURL+"/v1/models", nil)
	if err != nil {
		return fmt.Errorf("failed to create health check request: %w", err)
	}

	req.Header.Set("x-api-key", p.config.APIKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed with status: %d", resp.StatusCode)
	}

	return nil
}
