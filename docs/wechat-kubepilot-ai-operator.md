# 把 Kubernetes 告警“翻译成人话”：KubePilot AI Operator 的事件驱动诊断实践（附关键代码）

很多团队的 Kubernetes 告警现状是这样的：

- 告警很多，信息很碎（Event 一屏刷不完）
- 术语很硬，没人能一眼看懂（CrashLoopBackOff、ImagePullBackOff、FailedScheduling…）
- 定位很慢，靠经验和碰运气（先看 Event，再看 Pod，再看日志，再猜）
- 值班很累，信息不对称（群里只有“出事了”，没有“该怎么做”）

真正让人崩溃的不是“有告警”，而是**告警不能直接指向可行动的结论**。

这篇文章结合当前实现的 **KubePilot AI Operator（kubeai-controller）**，用通俗方式讲清楚一个思路：

> 让控制器把集群里的 Warning 事件自动转成结构化的 AI 诊断单（AIIncident），再把“根因/风险/建议”推送到群里；同时再配一条“每日巡检”链路，把集群健康报告每天自动发出来。

文中会贴部分核心代码，帮助你把思路落到工程实现。

---

## 01. 总体架构：两条链路并行，互不干扰

KubePilot AI Operator 当前有两条核心能力链路：

### A. 事件驱动告警链路（Event → AIIncident → AI 分析 → 通知）

1. 监听 Kubernetes 的 `corev1.Event`（只处理 `Warning`）
2. 命中规则后创建一个 `AIIncident` CR（可追踪、可审计）
3. 由 `AIIncidentReconciler` 用状态机推进：分析、重试、通知、结束
4. 调用 LLM 输出结构化诊断（JSON），再推送到钉钉/企业微信等渠道

### B. 定时巡检链路（Schedule → 采集 → 报告渲染 → 通知）

1. Asia/Shanghai 时区按 Cron（简化 5 段）定时执行
2. 采集并汇总：
   - 集群组件健康状况
   - 节点资源使用率与压力情况
   - 工作负载统计（Deployment/StatefulSet/DaemonSet/Job/CronJob）
   - Pod 总览与异常 Pod Top 原因
   - 异常 Pod 的 AI 分析（复用同一套 LLM 能力）
3. 生成 Markdown 报告；可选用 LLM 进行“二次渲染”提升可读性
4. 推送到群里

这两条链路设计上必须并行：**告警靠事件触发，巡检靠 schedule；互不依赖，也互不“抢资源”。**

### 架构图（文本树形）

```text
KubePilot AI Operator（kubeai-controller）
├─ 事件驱动告警：Event(Warning) → EventIncidentReconciler（过滤/去重/抽取日志）→ AIIncident
│  → AIIncidentReconciler（状态机）→ LLM Analyze（结构化 JSON）→ Notifier（钉钉/企业微信）
└─ 定时巡检：Schedule（Cron5 + Asia/Shanghai）→ Runner（采集 + 异常 Pod AI）→ 报告（可选 LLM 渲染）→ Notifier
```

---

## 02. 为什么我选择“只监听 Event(Warning)”而不是 Watch Pod？

很多人第一反应是：Pod 异常就 Watch Pod。问题在于：

- Pod 变化频率很高，直接 watch 容易引入不必要的 reconcile 压力
- 告警的触发语义其实已经被 Kubernetes “总结”成 Event 了（例如 CrashLoopBackOff、FailedScheduling）
- 你要的是“异常信号”，不是“全量对象变化流”

所以我的做法是：

> **只监听 `corev1.Event`，只处理 `Warning`，并且做“时间窗过滤 + 去重”。**

核心片段（`EventIncidentReconciler`）：

```go
if ev.Type != corev1.EventTypeWarning {
    return ctrl.Result{}, nil
}
if isOldEvent(&ev, 15*time.Minute) {
    return ctrl.Result{}, nil
}
```

接着，把 Event 映射成“可分析的触发类型”（当前版本内置常见规则：CrashLoopBackOff、ImagePullBackOff、FailedScheduling、OOMKilled、NodeNotReady…）：

```go
triggerType, severity, triggerMsg := mapEventToTrigger(&ev)
if triggerType == "" {
    return ctrl.Result{}, nil
}
```

如果 Event 关联的是 Pod，还会抓取部分日志作为上下文（tail 80 行，带时间戳），让后续 AI 分析更接近“人类排障路径”：

```go
var logs []string
if r.RestConfig != nil && kind == "Pod" && ns != "" && name != "" {
    clientset, err := kubernetes.NewForConfig(r.RestConfig)
    if err == nil {
        logs = fetchPodLogs(ctx, clientset, ns, name, 80)
    }
}
```

最后创建 `AIIncident` CR（并打上 label 方便检索）：

```go
incident := &kubeaiiov1.AIIncident{
    Spec: kubeaiiov1.AIIncidentSpec{
        Resource: kubeaiiov1.ResourceInfo{
            Kind: kind, Name: name, Namespace: ns,
        },
        Trigger: kubeaiiov1.TriggerCondition{
            Type: triggerType, Severity: severity, Message: triggerMsg,
        },
        Analyze: kubeaiiov1.AnalyzeConfig{
            Context: &kubeaiiov1.AnalysisContext{
                Logs: logs,
            },
        },
    },
}
_ = r.Create(ctx, incident)
```

这一段的工程意义是：

> 我们把“瞬时 Event”固化成了“可追踪的诊断工单 CR（AIIncident）”。从此后面的 LLM 分析、重试、通知、审计，都围绕这个 CR 做，链路更可控。

---

## 03. AIIncident 怎么跑起来？一个“可重试”的状态机

`AIIncidentReconciler` 不是拿到 CR 就直接调 AI、直接发消息，而是做了一个小状态机，确保过程可重试、可观测、且每一步幂等。

核心分发逻辑（`AIIncidentReconciler.Reconcile`）：

```go
switch incident.Status.Phase {
case "", "Pending":
    return r.handlePending(ctx, &incident)
case "Analyzing":
    return r.handleAnalyzing(ctx, &incident)
case "Analyzed":
    return r.handleAnalyzed(ctx, &incident)
case "Notifying":
    return r.handleNotifying(ctx, &incident)
case "Resolved":
    return r.handleResolved(ctx, &incident)
case "Failed":
    return r.handleFailed(ctx, &incident)
default:
    return ctrl.Result{}, fmt.Errorf("unknown phase: %s", incident.Status.Phase)
}
```

在 `Analyzing` 阶段，它会把 `AIIncident.Spec` 里的资源信息、事件信息、日志/指标/事件上下文拼成 LLM 请求：

```go
req := &llm.AnalysisRequest{
    ResourceType: incident.Spec.Resource.Kind,
    ResourceName: incident.Spec.Resource.Name,
    Namespace:    incident.Spec.Resource.Namespace,
    EventType:    incident.Spec.Trigger.Type,
    Message:      incident.Spec.Trigger.Message,
    Severity:     incident.Spec.Trigger.Severity,
}
if incident.Spec.Analyze.Context != nil {
    req.Logs = incident.Spec.Analyze.Context.Logs
    req.Metrics = incident.Spec.Analyze.Context.Metrics
    req.Events = incident.Spec.Analyze.Context.Events
}
result, err := r.LLMManager.Analyze(ctx, providerName, req)
```

如果 LLM 调用失败，会把 Phase 置为 `Failed`，并支持按配置重试回 `Pending`：

```go
if incident.Status.RetryCount < int32(r.Config.LLM.RetryCount) {
    latest.Status.RetryCount++
    latest.Status.Phase = "Pending"
}
```

为什么这种状态机值得做？

- 生产环境里 “LLM 超时/网络抖动/通知失败” 都是正常情况
- 状态机让你把“不可控外部依赖”封装成可恢复流程，而不是一次性脚本式调用

---

## 04. 让 LLM 输出“结构化 JSON”，而不是一段散文

LLM 最大的问题之一是：输出不可控。要把 AI 结果用于工程化链路，必须结构化。

DeepSeek Provider 的做法是：用 system prompt 强约束输出 JSON 格式，字段包括：

- `level`（Critical/Warning/Info/Success）
- `reason`（根因）
- `confidence`（0~1）
- `suggestions`（可执行建议）
- `autoFixable` / `fixCommand`（预留：未来可做自动修复）

关键思路（简化示意）：

```go
systemPrompt := `...输出必须是有效的 JSON 格式：
{
  "level": "Warning",
  "reason": "...",
  "confidence": 0.92,
  "suggestions": ["建议1", "建议2"],
  "autoFixable": false,
  "fixCommand": ""
}
注意：
- JSON 必须有效，不要添加 markdown 代码块标记
...`
```

此外还做了“解析兜底”：

- 模型不听话输出 ```json code block → 尝试从 code block 提取 JSON
- 仍然解析失败 → 降级为 “原文 + 低置信度 + 人工介入建议”，但链路不中断

工程目标很明确：

> **宁可降级，也不要让链路崩掉；宁可不完美，也要可用。**

---

## 05. 每日巡检：不是 CronJob，而是控制器里的“稳定调度 + 报告链路”

巡检链路由 `pkg/inspection/Runner` 启动，按 schedule 计算下一次执行时间，并使用北京时间格式输出关键日志（`YYYY-MM-DD HH:MM:SS`）：

```go
next := s.next(time.Now(), loc)
wait := time.Until(next)
l.Info("next inspection scheduled", "at", next.Format("2006-01-02 15:04:05"), "wait", wait.String())
```

为了让实现足够稳定、成本足够低，目前的 Cron 解析是“简化版 5 段”，只支持 minute/hour 可配置，其余必须 `*`（也就是“每天/每小时/每分钟”的常见巡检模型）。

巡检本体逻辑大概是：

1. 采集节点、工作负载、组件、Pod 汇总
2. 对异常 Pod 做 AI 分析（复用同一套 LLM Analyze）
3. 生成原始 Markdown
4. 可选：让 LLM 对报告做一次“二次渲染”（更适合发群）
5. 推送通知

这条链路的价值在于：它解决了另一类问题——不是“出故障了怎么办”，而是“集群整体健康如何、趋势如何、风险在哪里”。

---

## 06. 通知：钉钉 Markdown + 签名 + 失败可见

通知模块当前实现了钉钉机器人：支持 Markdown、@人、签名（timestamp + secret）。

签名逻辑简化示意：

```go
if d.secret != "" {
    timestamp := time.Now().UnixNano() / 1e6
    sign := d.calculateSign(timestamp, d.secret)
    webhookURL = fmt.Sprintf("%s&timestamp=%d&sign=%s", webhookURL, timestamp, sign)
}
```

消息结构上区分了：

- `Title` / `Level`
- `ResourceInfo`（kind/name/namespace/cluster）
- `AnalysisResult` + `Suggestions`
- `Timestamp`（统一格式）

所以你在群里看到的不再是“Pod 异常”，而是“异常 + 根因 + 建议”。

---

## 07. 你可以怎么落地？一个低风险上线顺序

如果你想在团队里落地这套方案，我建议按这个顺序推进：

1. 先打开 **Event → AIIncident → 通知**：解决“告警看不懂/定位慢”的核心痛点
2. 再打开 **每日巡检报告**：把“集群健康”变成每天固定节奏的可视化汇报
3. 最后逐步迭代：
   - 增加更多 Event→Trigger 规则（覆盖更多常见故障）
   - 补齐 metrics 上下文（当前上下文以 logs/events 为主）
   - 引入自动修复（`autoFixable`/`fixCommand` 已预留，但需要权限与安全边界设计）

---

## 08. 当前能力边界（很重要）

为了避免“画大饼”，这里把当前实现的边界说清楚：

- 告警触发来源：**只监听 Kubernetes Event 的 Warning**（不直接 watch Pod）
- Cron 表达式：**简化版 5 段，仅 minute/hour 可配，其余必须 `*`**
- AI 输出：强约束 JSON，但仍做了兜底（避免解析失败导致链路中断）
- 通知：已实现钉钉机器人（并处理签名与错误码），其他渠道可按 Notifier 接口扩展

---

## 结语：让告警从“噪声”变成“行动”

Kubernetes 的复杂度本质上不会消失，但我们可以让信息更“贴近人的工作方式”：

- 事件驱动 → 聚焦异常信号
- CR 状态机 → 过程可控可追踪
- LLM 结构化输出 → 直接给建议，减少“翻译成本”
- 巡检报告 → 把隐患变成“每天可见的事实”

如果你希望我把这篇文章进一步“公众号化”（更强的故事线/案例/配图说明/标题优化），或者希望加一张“架构流程图（可直接转成 Mermaid）”，告诉我你的目标读者（管理者/一线 SRE/开发团队）即可。
