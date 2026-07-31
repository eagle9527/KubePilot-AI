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
	defaultQwenBaseURL = "https://dashscope.aliyuncs.com/api/v1"
	defaultQwenModel   = "qwen-turbo"
)

// QwenProvider 实现了通义千问 API 的调用
type QwenProvider struct {
	config *Config
	client *http.Client
}

// QwenRequest 是通义千问 API 的请求结构
type QwenRequest struct {
	Model string `json:"model"`
	Input struct {
		Messages []QwenMessage `json:"messages"`
	} `json:"input"`
	Parameters *QwenParameters `json:"parameters,omitempty"`
}

// QwenMessage 是通义千问的消息结构
type QwenMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// QwenParameters 是通义千问的参数结构
type QwenParameters struct {
	ResultFormat   string  `json:"result_format,omitempty"`
	Temperature    float32 `json:"temperature,omitempty"`
	MaxTokens      int     `json:"max_tokens,omitempty"`
	TopP           float32 `json:"top_p,omitempty"`
	TopK           int     `json:"top_k,omitempty"`
	IncrementalOutput bool `json:"incremental_output,omitempty"`
}

// QwenResponse 是通义千问 API 的响应结构
type QwenResponse struct {
	Output struct {
		Choices []struct {
			FinishReason string      `json:"finish_reason"`
			Message      QwenMessage `json:"message"`
		} `json:"choices"`
	} `json:"output"`
	Usage struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
		TotalTokens  int `json:"total_tokens"`
	} `json:"usage"`
	RequestID string `json:"request_id"`
	Code      string `json:"code,omitempty"`
	Message   string `json:"message,omitempty"`
}

// NewQwenProvider 创建通义千问 Provider 实例
func NewQwenProvider(config *Config) *QwenProvider {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 60
	}

	return &QwenProvider{
		config: config,
		client: &http.Client{
			Timeout: time.Duration(timeout) * time.Second,
		},
	}
}

// Name 返回 Provider 名称
func (p *QwenProvider) Name() string {
	return "qwen"
}

// Analyze 调用通义千问 API 分析异常事件
func (p *QwenProvider) Analyze(ctx context.Context, req *AnalysisRequest) (*AnalysisResult, error) {
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

	// 构建通义千问请求
	model := p.config.Model
	if model == "" {
		model = defaultQwenModel
	}

	temp := p.config.Temperature
	if temp == 0 {
		temp = 0.3
	}

	maxTokens := p.config.MaxTokens
	if maxTokens == 0 {
		maxTokens = 2000
	}

	qwenReq := QwenRequest{
		Model: model,
		Input: struct {
			Messages []QwenMessage `json:"messages"`
		}{
			Messages: []QwenMessage{
				{Role: "system", Content: systemPrompt},
				{Role: "user", Content: userPrompt},
			},
		},
		Parameters: &QwenParameters{
			ResultFormat: "text",
			Temperature:  temp,
			MaxTokens:    maxTokens,
		},
	}

	// 发送请求
	baseURL := p.config.BaseURL
	if baseURL == "" {
		baseURL = defaultQwenBaseURL
	}

	reqBody, err := json.Marshal(qwenReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Qwen request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/services/aigc/text-generation/generation", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.config.APIKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to Qwen: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// 解析响应
	var qwenResp QwenResponse
	if err := json.Unmarshal(body, &qwenResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Qwen response: %w", err)
	}

	// 检查 API 错误
	if qwenResp.Code != "" && qwenResp.Code != "200" {
		return nil, fmt.Errorf("Qwen API error: code=%s, message=%s", qwenResp.Code, qwenResp.Message)
	}

	if len(qwenResp.Output.Choices) == 0 {
		return nil, fmt.Errorf("no choices in Qwen response")
	}

	// 解析 LLM 返回的 JSON
	content := qwenResp.Output.Choices[0].Message.Content

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
func (p *QwenProvider) HealthCheck(ctx context.Context) error {
	// 简单检查：尝试获取模型列表
	baseURL := p.config.BaseURL
	if baseURL == "" {
		baseURL = defaultQwenBaseURL
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
