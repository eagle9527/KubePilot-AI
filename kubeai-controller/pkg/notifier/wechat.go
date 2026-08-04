package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// WeChatNotifier 实现了企业微信机器人通知
type WeChatNotifier struct {
	webhookURL string
	client     *http.Client
}

// WeChatMessage 是企业微信消息结构
type WeChatMessage struct {
	MsgType  string          `json:"msgtype"`
	Markdown *WeChatMarkdown `json:"markdown,omitempty"`
}

type WeChatMarkdown struct {
	Content string `json:"content"`
}

type WeChatResponse struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

// NewWeChatNotifier 创建企业微信通知器
func NewWeChatNotifier(webhookURL string) *WeChatNotifier {
	return &WeChatNotifier{
		webhookURL: webhookURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Name 返回通知器名称
func (w *WeChatNotifier) Name() string {
	return "wechat"
}

// Send 发送企业微信通知
func (w *WeChatNotifier) Send(ctx context.Context, msg *Message) error {
	content := w.buildMarkdownContent(msg)

	wechatMsg := WeChatMessage{
		MsgType: "markdown",
		Markdown: &WeChatMarkdown{
			Content: content,
		},
	}

	return w.sendMessage(ctx, wechatMsg)
}

// buildMarkdownContent 构建 Markdown 格式的消息内容。
// 企业微信仅支持标题、加粗、链接、行内代码、引用和 <font color>（info/comment/warning），
// 不支持表格与列表语法，这里统一走 adaptForWeChat 适配。
func (w *WeChatNotifier) buildMarkdownContent(msg *Message) string {
	if strings.TrimSpace(msg.Content) != "" {
		content := strings.TrimSpace(msg.Content)
		if strings.TrimSpace(msg.ResourceInfo.Cluster) != "" {
			content = fmt.Sprintf("> 集群：%s\n\n%s", strings.TrimSpace(msg.ResourceInfo.Cluster), content)
		}
		return adaptForWeChat(content)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("## %s %s\n", LevelEmoji(msg.Level), msg.Title))
	b.WriteString(fmt.Sprintf("> 级别 <font color=\"%s\">%s</font>\n\n", levelColor(msg.Level), LevelText(msg.Level)))
	if strings.TrimSpace(msg.ResourceInfo.Cluster) != "" {
		b.WriteString(fmt.Sprintf("> 集群 `%s`\n\n", strings.TrimSpace(msg.ResourceInfo.Cluster)))
	}

	b.WriteString("**资源**\n")
	b.WriteString(fmt.Sprintf("> %s `%s`", msg.ResourceInfo.Kind, msg.ResourceInfo.Name))
	if msg.ResourceInfo.Namespace != "" {
		b.WriteString(fmt.Sprintf(" · 命名空间 `%s`", msg.ResourceInfo.Namespace))
	}
	b.WriteString("\n\n")

	if strings.TrimSpace(msg.AnalysisResult) != "" {
		b.WriteString("**AI 分析**\n")
		for _, line := range strings.Split(strings.TrimSpace(msg.AnalysisResult), "\n") {
			b.WriteString("> " + strings.TrimSpace(line) + "\n")
		}
		b.WriteString("\n")
	}

	if len(msg.Suggestions) > 0 {
		b.WriteString("**处理建议**\n")
		for i, suggestion := range msg.Suggestions {
			b.WriteString(fmt.Sprintf("%d. %s\n", i+1, suggestion))
		}
		b.WriteString("\n")
	}

	b.WriteString(fmt.Sprintf("<font color=\"comment\">%s · KubePilot AI</font>", msg.Timestamp.Format("2006-01-02 15:04:05")))

	return adaptForWeChat(b.String())
}

// sendMessage 发送消息到企业微信
func (w *WeChatNotifier) sendMessage(ctx context.Context, msg WeChatMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal wechat message: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", w.webhookURL, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	var wechatResp WeChatResponse
	if err := json.Unmarshal(respBody, &wechatResp); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if wechatResp.ErrCode != 0 {
		return fmt.Errorf("wechat api error: code=%d, msg=%s", wechatResp.ErrCode, wechatResp.ErrMsg)
	}

	return nil
}

// HealthCheck 检查企业微信配置是否正确
func (w *WeChatNotifier) HealthCheck(ctx context.Context) error {
	testMsg := WeChatMessage{
		MsgType: "markdown",
		Markdown: &WeChatMarkdown{
			Content: "KubePilot AI 健康检查测试",
		},
	}
	return w.sendMessage(ctx, testMsg)
}
