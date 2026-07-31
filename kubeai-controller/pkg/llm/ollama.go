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
	defaultOllamaBaseURL = "http://localhost:11434"
	defaultOllamaModel   = "llama2"
)

// OllamaProvider 实现了本地 Ollama API 的调用
type OllamaProvider struct {
	config *Config
	client *http.Client
}

// OllamaRequest 是 Ollama API 的请求结构
type OllamaRequest struct {
	Model    string `json:"model"`
	Prompt   string `json:"prompt,omitempty"`
	Messages []OllamaMessage `json:"messages,omitempty"`
	Stream   bool   `json:"stream"`
	Options  *OllamaOptions `json:"options,omitempty"`
}

// OllamaMessage 是 Ollama 的消息结构（用于 chat API）
type OllamaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// OllamaOptions 是 Ollama 的配置选项
type OllamaOptions struct {
	Temperature float32 `json:"temperature,omitempty"`
	NumPredict  int     `json:"num_predict,omitempty"`
	TopP        float32 `json:"top_p,omitempty"`
	TopK        int     `json:"top_k,omitempty"`
}

// OllamaResponse 是 Ollama API 的响应结构
type OllamaResponse struct {
	Model     string `json:"model"`
	CreatedAt string `json:"created_at"`
	Response  string `json:"response"`
	Done      bool   `json:"done"`
	Message   *OllamaMessage `json:"message,omitempty"`
	DoneReason string `json:"done_reason,omitempty"`
	Error     string `json:"error,omitempty"`
	
	// 统计信息
	TotalDuration      int64 `json:"total_duration,omitempty"`
	LoadDuration       int64 `json:"load_duration,omitempty"`
	PromptEvalCount    int   `json:"prompt_eval_count,omitempty"`
	PromptEvalDuration int64 `json:"prompt_eval_duration,omitempty"`
	EvalCount          int   `json:"eval_count,omitempty"`
	EvalDuration       int64 `json:"eval_duration,omitempty"`
}

// NewOllamaProvider 创建 Ollama Provider 实例
func NewOllamaProvider(config *Config) *OllamaProvider {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 120 // Ollama 本地模型可能需要更长时间
	}

	return &OllamaProvider{
		config: config,
		client: &http.Client{
			Timeout: time.Duration(timeout) * time.Second,
		},
	}
}

// Name 返回 Provider 名称
func (p *OllamaProvider) Name() string {
	return "ollama"
}

// Analyze 调用 Ollama API 分析异常事件
func (p *OllamaProvider) Analyze(ctx context.Context, req *AnalysisRequest) (*AnalysisResult, error) {
	// 构建系统提示词
	systemPrompt := `You are KubePilot AI, a professional Kubernetes SRE expert specializing in analyzing cluster anomalies.

Your task is to analyze Kubernetes anomaly events and provide structured diagnostic results.

Analysis dimensions:
1. Root Cause Analysis - identify the root cause of the problem
2. Risk Level - Critical/Warning/Info/Success
3. Confidence - a decimal between 0-1
4. Remediation Suggestions - specific actionable recommendations
5. Whether it can be automatically fixed

Output must be in valid JSON format:
{
  "level": "Warning",
  "reason": "Detailed root cause analysis...",
  "confidence": 0.92,
  "suggestions": ["Suggestion 1", "Suggestion 2"],
  "autoFixable": false,
  "fixCommand": ""
}

Note:
- JSON must be valid, do not add markdown code block markers
- Analysis should be concise but professional
- Reduce confidence for uncertain information
- Suggestions should be specific and actionable`;

	// 构建用户提示词
	userPrompt := fmt.Sprintf(`Please analyze the following Kubernetes anomaly event:

[Resource Information]
- Type: %s
- Name: %s
- Namespace: %s

[Event Information]
- Type: %s
- Severity: %s
- Message: %s

[Context Information]
Log Snippets:
%s

Metric Data:
%s

Related Events:
%s

Please provide structured analysis results.`,
		req.ResourceType, req.ResourceName, req.Namespace,
		req.EventType, req.Severity, req.Message,
		formatLogs(req.Logs),
		formatMetrics(req.Metrics),
		formatEvents(req.Events))

	// 构建 Ollama 请求 - 使用 chat API
	model := p.config.Model
	if model == "" {
		model = defaultOllamaModel
	}

	temp := p.config.Temperature
	if temp == 0 {
		temp = 0.3
	}

	maxTokens := p.config.MaxTokens
	if maxTokens == 0 {
		maxTokens = 2000
	}

	ollamaReq := OllamaRequest{
		Model:    model,
		Messages: []OllamaMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
		Stream: false,
		Options: &OllamaOptions{
			Temperature: temp,
			NumPredict:  maxTokens,
		},
	}

	// 发送请求
	baseURL := p.config.BaseURL
	if baseURL == "" {
		baseURL = defaultOllamaBaseURL
	}

	reqBody, err := json.Marshal(ollamaReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Ollama request: %w", err)
	}

	// 使用 chat API
	httpReq, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/api/chat", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to Ollama: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// 解析响应
	var ollamaResp OllamaResponse
	if err := json.Unmarshal(body, &ollamaResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Ollama response: %w", err)
	}

	// 检查错误
	if ollamaResp.Error != "" {
		return nil, fmt.Errorf("Ollama API error: %s", ollamaResp.Error)
	}

	// 解析 LLM 返回的 JSON
	var content string
	if ollamaResp.Message != nil {
		content = ollamaResp.Message.Content
	} else {
		content = ollamaResp.Response
	}

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
func (p *OllamaProvider) HealthCheck(ctx context.Context) error {
	baseURL := p.config.BaseURL
	if baseURL == "" {
		baseURL = defaultOllamaBaseURL
	}

	req, err := http.NewRequestWithContext(ctx, "GET", baseURL+"/api/tags", nil)
	if err != nil {
		return fmt.Errorf("failed to create health check request: %w", err)
	}

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
