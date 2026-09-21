package notifier

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// 企业微信 Markdown 内容上限 4096 字节，钉钉消息体上限约 20000 字节，
// 这里留出余量，超限时截断并给出提示。
const (
	wechatContentLimit   = 3800
	dingtalkContentLimit = 18000
	truncationSuffix     = "\n\n…（内容过长，已截断）"
)

var (
	tableSepLineRe = regexp.MustCompile(`^\s*\|?\s*:?-+:?\s*(\|\s*:?-+:?\s*)+\|?\s*$`)
	fontColorRe    = regexp.MustCompile(`(?i)<font\s+color\s*=\s*"?([a-zA-Z#0-9]+)"?\s*>`)
	fontEndTagRe   = regexp.MustCompile(`(?i)</font>`)
	bulletRe       = regexp.MustCompile(`^(\s*)[-*+]\s+`)
	multiBlankRe   = regexp.MustCompile(`\n{3,}`)
)

// LevelEmoji 返回消息级别对应的状态图标
func LevelEmoji(level MessageLevel) string {
	switch level {
	case Critical:
		return "🔴"
	case High:
		return "🟠"
	case Warning:
		return "🟡"
	case Info:
		return "🔵"
	case Success:
		return "🟢"
	default:
		return "⚪"
	}
}

// LevelText 返回消息级别对应的中文描述
func LevelText(level MessageLevel) string {
	switch level {
	case Critical:
		return "紧急"
	case High:
		return "高"
	case Warning:
		return "需关注"
	case Info:
		return "提示"
	case Success:
		return "正常"
	default:
		return "未知"
	}
}

// levelColor 返回企业微信支持的字体颜色（info 绿 / comment 灰 / warning 橙红）
func levelColor(level MessageLevel) string {
	switch level {
	case Critical, High, Warning:
		return "warning"
	case Info, Success:
		return "info"
	default:
		return "comment"
	}
}

// adaptForWeChat 将通用 Markdown 适配为企业微信可渲染的格式。
// 企业微信仅支持标题、加粗、链接、行内代码、引用和 <font color>，
// 不支持表格和无序列表语法，需要逐一转换。
func adaptForWeChat(md string) string {
	md = sanitizeTables(md)
	md = normalizeWeChatFonts(md)
	md = bulletsForWeChat(md)
	md = compactBlankLines(md)
	md = strings.TrimSpace(md)
	return truncateBytes(md, wechatContentLimit)
}

// adaptForDingTalk 将通用 Markdown 适配为钉钉可渲染的格式。
// 钉钉支持标题、加粗、链接、图片、列表和引用，但不支持表格和 <font> 标签。
func adaptForDingTalk(md string) string {
	md = sanitizeTables(md)
	md = stripFontTags(md)
	md = compactBlankLines(md)
	md = strings.TrimSpace(md)
	return truncateBytes(md, dingtalkContentLimit)
}

// sanitizeTables 把 Markdown 表格转换为逐行的“标签：键值”文本，
// 避免在不支持表格的 IM 渠道中显示成原始竖线。
func sanitizeTables(md string) string {
	lines := strings.Split(md, "\n")
	out := make([]string, 0, len(lines))

	i := 0
	for i < len(lines) {
		if isTableLine(lines[i]) && i+1 < len(lines) && tableSepLineRe.MatchString(lines[i+1]) {
			headers := splitTableRow(lines[i])
			j := i + 2
			for j < len(lines) && isTableLine(lines[j]) {
				out = append(out, tableRowToLine(headers, lines[j]))
				j++
			}
			i = j
			continue
		}
		out = append(out, lines[i])
		i++
	}
	return strings.Join(out, "\n")
}

func isTableLine(s string) bool {
	t := strings.TrimSpace(s)
	return strings.HasPrefix(t, "|") && strings.Count(t, "|") >= 2
}

func splitTableRow(s string) []string {
	t := strings.TrimSpace(s)
	t = strings.TrimPrefix(t, "|")
	t = strings.TrimSuffix(t, "|")
	raw := strings.Split(t, "|")
	cells := make([]string, 0, len(raw))
	for _, c := range raw {
		cells = append(cells, strings.TrimSpace(c))
	}
	return cells
}

func tableRowToLine(headers []string, row string) string {
	cells := splitTableRow(row)
	if len(cells) == 0 {
		return ""
	}
	label := cells[0]
	parts := make([]string, 0, len(cells))
	for idx := 1; idx < len(cells); idx++ {
		v := cells[idx]
		if v == "" || v == "-" {
			continue
		}
		h := ""
		if idx < len(headers) {
			h = headers[idx]
		}
		if h != "" && h != v {
			parts = append(parts, fmt.Sprintf("%s %s", h, v))
		} else {
			parts = append(parts, v)
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("**%s**", label)
	}
	return fmt.Sprintf("**%s**：%s", label, strings.Join(parts, " · "))
}

// normalizeWeChatFonts 把 <font color> 归一化为企业微信支持的三种颜色，
// 其他颜色值（如 gray、red）会被映射到最接近的合法值。
func normalizeWeChatFonts(md string) string {
	return fontColorRe.ReplaceAllStringFunc(md, func(m string) string {
		color := strings.ToLower(fontColorRe.FindStringSubmatch(m)[1])
		switch color {
		case "info", "comment", "warning":
			return m
		case "green":
			return `<font color="info">`
		case "gray", "grey":
			return `<font color="comment">`
		default:
			return `<font color="warning">`
		}
	})
}

// stripFontTags 去掉 <font> 标签但保留文字（钉钉不渲染 HTML 标签）
func stripFontTags(md string) string {
	md = fontColorRe.ReplaceAllString(md, "")
	return fontEndTagRe.ReplaceAllString(md, "")
}

// bulletsForWeChat 把无序列表符号替换为实心圆点。
// 企业微信不渲染列表语法，"- " 会原样显示，换成 "• " 观感更好。
func bulletsForWeChat(md string) string {
	lines := strings.Split(md, "\n")
	for i, line := range lines {
		lines[i] = bulletRe.ReplaceAllString(line, "$1• ")
	}
	return strings.Join(lines, "\n")
}

func compactBlankLines(md string) string {
	return multiBlankRe.ReplaceAllString(md, "\n\n")
}

// truncateBytes 按字节数截断（不切断 UTF-8 字符），并追加截断提示
func truncateBytes(md string, limit int) string {
	if len(md) <= limit {
		return md
	}
	cut := limit - len(truncationSuffix)
	if cut < 0 {
		cut = 0
	}
	for cut > 0 && !utf8.RuneStart(md[cut]) {
		cut--
	}
	return md[:cut] + truncationSuffix
}
