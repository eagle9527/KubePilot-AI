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
	defaultOpenAIBaseURL = "https://api.openai.com/v1"
	defaultOpenAIModel   = "gpt-4o"
)

// OpenAIProvider 实现了 OpenAI API 的调用
type OpenAIProvider struct {
	config *Config
	client *http.Client
}

// OpenAIRequest 是 OpenAI API 的请求结构
type OpenAIRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Temperature float32       `json:"temperature,omitempty"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
}

// ChatMessage 是 OpenAI 的对话消息
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// OpenAIResponse 是 OpenAI API 的响应结构
type OpenAIResponse struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Index        int         `json:"index"`
		Message      ChatMessage `json:"message"`
		FinishReason string      `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
	Error *OpenAIError `json:"error,omitempty"`
}

// OpenAIError 是 OpenAI API 的错误结构
type OpenAIError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

func (e *OpenAIError) Error() string {
	return fmt.Sprintf("OpenAI API error: %s (type: %s, code: %s)", e.Message, e.Type, e.Code)
}

// NewOpenAIProvider 创建 OpenAI Provider 实例
func NewOpenAIProvider(config *Config) *OpenAIProvider {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 60
	}

	return &OpenAIProvider{
		config: config,
		client: &http.Client{
			Timeout: time.Duration(timeout) * time.Second,
		},
	}
}

// Name 返回 Provider 名称
func (p *OpenAIProvider) Name() string {
	return "openai"
}

// Analyze 调用 OpenAI API 分析异常事件
func (p *OpenAIProvider) Analyze(ctx context.Context, req *AnalysisRequest) (*AnalysisResult, error) {
	// 构建系统提示词
	systemPrompt := `你是一名专业的 Kubernetes SRE 专家，擅长分析集群异常事件。
请分析以下 Kubernetes 异常事件，并提供：
1. 问题根因分析
2. 风险等级评估 (Critical/Warning/Info/Success)
3. 修复建议
4. 置信度评分 (0-1)

请以 JSON 格式输出，格式如下：
{
  "level": "Warning",
  "reason": "详细的根因分析...",
  "confidence": 0.92,
  "suggestions": ["建议1", "建议2"],
  "autoFixable": false
}`

	// 构建用户提示词
	userPrompt := fmt.Sprintf(`Kubernetes 异常事件分析请求：

资源类型: %s
资源名称: %s
命名空间: %s

事件类型: %s
严重程度: %s
消息: %s

上下文信息：
日志: %v
指标: %v
相关事件: %v`,
		req.ResourceType, req.ResourceName, req.Namespace,
		req.EventType, req.Severity, req.Message,
		req.Logs, req.Metrics, req.Events)

	// 构建 OpenAI 请求
	model := p.config.Model
	if model == "" {
		model = defaultOpenAIModel
	}

	temp := p.config.Temperature
	if temp == 0 {
		temp = 0.3
	}

	maxTokens := p.config.MaxTokens
	if maxTokens == 0 {
		maxTokens = 2000
	}

	openAIReq := OpenAIRequest{
		Model: model,
		Messages: []ChatMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
		Temperature: temp,
		MaxTokens:   maxTokens,
	}

	// 发送请求
	baseURL := p.config.BaseURL
	if baseURL == "" {
		baseURL = defaultOpenAIBaseURL
	}

	reqBody, err := json.Marshal(openAIReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal OpenAI request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/chat/completions", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.config.APIKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to OpenAI: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// 解析响应
	var openAIResp OpenAIResponse
	if err := json.Unmarshal(body, &openAIResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal OpenAI response: %w", err)
	}

	// 检查 API 错误
	if openAIResp.Error != nil {
		return nil, openAIResp.Error
	}

	if len(openAIResp.Choices) == 0 {
		return nil, fmt.Errorf("no choices in OpenAI response")
	}

	// 解析 LLM 返回的 JSON
	content := openAIResp.Choices[0].Message.Content

	// 尝试从 Markdown 代码块中提取 JSON
	var result AnalysisResult
	if err := parseLLMResponse(content, &result); err != nil {
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

// parseLLMResponse 解析 LLM 返回的 JSON 响应
func parseLLMResponse(content string, result *AnalysisResult) error {
	// 尝试直接解析
	if err := json.Unmarshal([]byte(content), result); err == nil {
		return nil
	}

	// 尝试从 Markdown 代码块中提取
	var jsonContent string
	contentBytes := []byte(content)
	if idx := bytes.Index(contentBytes, []byte("```json")); idx != -1 {
		start := idx + 7
		if end := bytes.Index(contentBytes[start:], []byte("```")); end != -1 {
			jsonContent = string(contentBytes[start : start+end])
		}
	}

	if jsonContent == "" {
		if idx := bytes.Index(contentBytes, []byte("```")); idx != -1 {
			start := idx + 3
			if end := bytes.Index(contentBytes[start:], []byte("```")); end != -1 {
				jsonContent = string(contentBytes[start : start+end])
			}
		}
	}

	if jsonContent != "" {
		return json.Unmarshal([]byte(jsonContent), result)
	}

	return fmt.Errorf("unable to parse JSON from response")
}

// HealthCheck 检查 Provider 是否可用
func (p *OpenAIProvider) HealthCheck(ctx context.Context) error {
	// 简单检查：尝试列出模型或发送一个简单请求
	baseURL := p.config.BaseURL
	if baseURL == "" {
		baseURL = defaultOpenAIBaseURL
	}

	req, err := http.NewRequestWithContext(ctx, "GET", baseURL+"/models", nil)
	if err != nil {
		return fmt.Errorf("failed to create health check request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+p.config.APIKey)

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
