package notifier

import (
	"strings"
	"testing"
)

func TestSanitizeTables(t *testing.T) {
	md := "前文\n\n| 节点 | CPU% | Mem% | 状态 |\n| --- | ---: | ---: | --- |\n| node-1 | 32.5 | 61.2 | Ready |\n| node-2 | 45.1 | 70.3 | NotReady |\n\n后文"

	out := sanitizeTables(md)

	if strings.Contains(out, "|") {
		t.Fatalf("table should be flattened, got:\n%s", out)
	}
	if !strings.Contains(out, "**node-1**：CPU% 32.5 · Mem% 61.2 · 状态 Ready") {
		t.Fatalf("unexpected row rendering:\n%s", out)
	}
	if !strings.Contains(out, "前文") || !strings.Contains(out, "后文") {
		t.Fatalf("surrounding text should be preserved:\n%s", out)
	}
}

func TestNormalizeWeChatFonts(t *testing.T) {
	in := `<font color="gray">灰</font> <font color="red">红</font> <font color="info">绿</font>`
	out := normalizeWeChatFonts(in)

	want := `<font color="comment">灰</font> <font color="warning">红</font> <font color="info">绿</font>`
	if out != want {
		t.Fatalf("got %q, want %q", out, want)
	}
}

func TestStripFontTags(t *testing.T) {
	in := `<font color="comment">2026-08-04 · KubePilot AI</font>`
	out := stripFontTags(in)
	if out != "2026-08-04 · KubePilot AI" {
		t.Fatalf("got %q", out)
	}
}

func TestBulletsForWeChat(t *testing.T) {
	in := "- a\n  - b\n1. c"
	out := bulletsForWeChat(in)
	want := "• a\n  • b\n1. c"
	if out != want {
		t.Fatalf("got %q, want %q", out, want)
	}
}

func TestTruncateBytes(t *testing.T) {
	s := strings.Repeat("巡", 2000) // 6000 bytes in UTF-8
	out := truncateBytes(s, 4000)

	if len(out) > 4000 {
		t.Fatalf("truncated length %d exceeds limit", len(out))
	}
	if !strings.HasSuffix(out, truncationSuffix) {
		t.Fatal("truncated output should carry suffix")
	}
	if strings.Contains(out, "\ufffd") {
		t.Fatal("truncation broke a UTF-8 rune")
	}

	short := "abc"
	if truncateBytes(short, 4000) != short {
		t.Fatal("short content should be unchanged")
	}
}

func TestAdaptForWeChat_FullReport(t *testing.T) {
	md := strings.Join([]string{
		"# 🟢 Kubernetes 每日巡检报告 · 2026-08-04",
		"",
		"- 节点：3 台",
		"- 组件：正常",
		"",
		"| 节点 | CPU% |",
		"| --- | ---: |",
		"| node-1 | 32.5 |",
		"",
		`<font color="gray">footer</font>`,
	}, "\n")

	out := adaptForWeChat(md)

	if strings.Contains(out, "|") {
		t.Fatalf("tables must be flattened for wechat:\n%s", out)
	}
	if !strings.Contains(out, "• 节点：3 台") {
		t.Fatalf("bullets must be converted for wechat:\n%s", out)
	}
	if !strings.Contains(out, `<font color="comment">footer</font>`) {
		t.Fatalf("font colors must be normalized for wechat:\n%s", out)
	}
}

func TestAdaptForDingTalk(t *testing.T) {
	md := "- 节点：3 台\n\n| a | b |\n| --- | --- |\n| 1 | 2 |\n\n<font color=\"info\">ok</font>"
	out := adaptForDingTalk(md)

	if strings.Contains(out, "<font") || strings.Contains(out, "</font>") {
		t.Fatalf("font tags must be stripped for dingtalk:\n%s", out)
	}
	if !strings.Contains(out, "- 节点：3 台") {
		t.Fatalf("lists must be kept for dingtalk:\n%s", out)
	}
	if strings.Contains(out, "|") {
		t.Fatalf("tables must be flattened for dingtalk:\n%s", out)
	}
}
