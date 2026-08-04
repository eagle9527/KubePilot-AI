package notifier

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultDingTalkTimeout = 30 * time.Second
	dingTalkAPIURL         = "https://oapi.dingtalk.com/robot/send"
)

// DingTalkNotifier 实现了钉钉机器人通知
type DingTalkNotifier struct {
	webhookURL  string
	accessToken string
	secret      string
	atMobiles   []string
	isAtAll     bool
	client      *http.Client
}

// DingTalkMessage 是钉钉消息结构
type DingTalkMessage struct {
	MsgType  string            `json:"msgtype"`
	Markdown *DingTalkMarkdown `json:"markdown,omitempty"`
	Text     *DingTalkText     `json:"text,omitempty"`
	Link     *DingTalkLink     `json:"link,omitempty"`
	At       *DingTalkAt       `json:"at,omitempty"`
}

// DingTalkMarkdown 是钉钉 Markdown 消息
type DingTalkMarkdown struct {
	Title string `json:"title"`
	Text  string `json:"text"`
}

// DingTalkText 是钉钉文本消息
type DingTalkText struct {
	Content string `json:"content"`
}

// DingTalkLink 是钉钉链接消息
type DingTalkLink struct {
	Title      string `json:"title"`
	Text       string `json:"text"`
	MessageURL string `json:"messageUrl"`
	PicURL     string `json:"picUrl,omitempty"`
}

// DingTalkAt 是钉钉 @ 信息
type DingTalkAt struct {
	AtMobiles []string `json:"atMobiles,omitempty"`
	IsAtAll   bool     `json:"isAtAll,omitempty"`
}

// DingTalkResponse 是钉钉响应
type DingTalkResponse struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

// NewDingTalkNotifier 创建钉钉通知器
func NewDingTalkNotifier(webhookURL, accessToken, secret string, atMobiles []string, isAtAll bool) *DingTalkNotifier {
	webhookURL = strings.TrimSpace(webhookURL)
	webhookURL = strings.Trim(webhookURL, "`\"")
	accessToken = strings.TrimSpace(accessToken)
	accessToken = strings.Trim(accessToken, "`\"")
	secret = strings.TrimSpace(secret)
	secret = strings.Trim(secret, "`\"")

	// 如果没有提供 webhookURL，则使用 accessToken 构造
	if webhookURL == "" && accessToken != "" {
		webhookURL = fmt.Sprintf("%s?access_token=%s", dingTalkAPIURL, accessToken)
	}

	return &DingTalkNotifier{
		webhookURL:  webhookURL,
		accessToken: accessToken,
		secret:      secret,
		atMobiles:   atMobiles,
		isAtAll:     isAtAll,
		client: &http.Client{
			Timeout: defaultDingTalkTimeout,
		},
	}
}

// Name 返回通知器名称
func (d *DingTalkNotifier) Name() string {
	return "dingtalk"
}

// Send 发送钉钉通知
func (d *DingTalkNotifier) Send(ctx context.Context, msg *Message) error {
	// 构建 Markdown 内容
	content := d.buildMarkdownContent(msg)

	dingMsg := DingTalkMessage{
		MsgType: "markdown",
		Markdown: &DingTalkMarkdown{
			Title: msg.Title,
			Text:  content,
		},
		At: &DingTalkAt{
			AtMobiles: d.atMobiles,
			IsAtAll:   d.isAtAll,
		},
	}

	return d.sendMessage(ctx, dingMsg)
}

// buildMarkdownContent 构建 Markdown 格式的消息内容。
// 钉钉支持标题、加粗、链接、图片、列表和引用，但不支持表格和 <font> 标签，
// 这里统一走 adaptForDingTalk 适配。
func (d *DingTalkNotifier) buildMarkdownContent(msg *Message) string {
	if strings.TrimSpace(msg.Content) != "" {
		content := strings.TrimSpace(msg.Content)
		if strings.TrimSpace(msg.ResourceInfo.Cluster) != "" {
			content = fmt.Sprintf("> 集群：%s\n\n%s", strings.TrimSpace(msg.ResourceInfo.Cluster), content)
		}
		return adaptForDingTalk(content)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("## %s %s\n\n", LevelEmoji(msg.Level), msg.Title))
	b.WriteString(fmt.Sprintf("**级别**：%s\n\n", LevelText(msg.Level)))
	if strings.TrimSpace(msg.ResourceInfo.Cluster) != "" {
		b.WriteString(fmt.Sprintf("> 集群：%s\n\n", strings.TrimSpace(msg.ResourceInfo.Cluster)))
	}

	b.WriteString("**资源信息**\n\n")
	b.WriteString(fmt.Sprintf("- 类型：%s\n", msg.ResourceInfo.Kind))
	b.WriteString(fmt.Sprintf("- 名称：%s\n", msg.ResourceInfo.Name))
	if msg.ResourceInfo.Namespace != "" {
		b.WriteString(fmt.Sprintf("- 命名空间：%s\n", msg.ResourceInfo.Namespace))
	}
	b.WriteString("\n")

	if strings.TrimSpace(msg.AnalysisResult) != "" {
		b.WriteString("**AI 分析**\n\n")
		b.WriteString("> " + strings.ReplaceAll(strings.TrimSpace(msg.AnalysisResult), "\n", "\n> ") + "\n\n")
	}

	if len(msg.Suggestions) > 0 {
		b.WriteString("**处理建议**\n\n")
		for i, suggestion := range msg.Suggestions {
			b.WriteString(fmt.Sprintf("%d. %s\n", i+1, suggestion))
		}
		b.WriteString("\n")
	}

	b.WriteString(fmt.Sprintf("%s · KubePilot AI", msg.Timestamp.Format("2006-01-02 15:04:05")))

	return adaptForDingTalk(b.String())
}

// sendMessage 发送消息到钉钉
func (d *DingTalkNotifier) sendMessage(ctx context.Context, msg DingTalkMessage) error {
	// 如果有 secret，则计算签名
	webhookURL := d.webhookURL
	if d.secret != "" {
		timestamp := time.Now().UnixNano() / 1e6
		sign := d.calculateSign(timestamp, d.secret)
		webhookURL = fmt.Sprintf("%s&timestamp=%d&sign=%s", webhookURL, timestamp, sign)
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal dingtalk message: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", webhookURL, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	var dingResp DingTalkResponse
	if err := json.Unmarshal(respBody, &dingResp); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if dingResp.ErrCode != 0 {
		return fmt.Errorf("dingtalk api error: code=%d, msg=%s", dingResp.ErrCode, dingResp.ErrMsg)
	}

	return nil
}

// calculateSign 计算钉钉签名
func (d *DingTalkNotifier) calculateSign(timestamp int64, secret string) string {
	stringToSign := fmt.Sprintf("%d\n%s", timestamp, secret)
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return url.QueryEscape(signature)
}

// HealthCheck 检查钉钉配置是否正确
func (d *DingTalkNotifier) HealthCheck(ctx context.Context) error {
	// 发送一条测试消息
	testMsg := DingTalkMessage{
		MsgType: "text",
		Text: &DingTalkText{
			Content: "KubePilot AI 健康检查测试",
		},
	}

	return d.sendMessage(ctx, testMsg)
}
