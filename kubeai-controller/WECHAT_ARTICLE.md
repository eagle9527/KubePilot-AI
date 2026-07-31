# 把 Kubernetes 告警变成“人话”：我做了一个能自己分析、还能每天巡检的 AI 运维机器人（KubePilot AI Operator）

如果你没怎么接触过 Kubernetes，也完全没关系。你只要把它理解成一个“集群操作系统”——里面跑着很多服务（Pod），有机器（Node），也会出各种小毛病。

现实里最烦的事情是：
- 出问题了，系统只会喊一句：CrashLoopBackOff / FailedScheduling / ImagePullBackOff
- 这句话对普通人来说约等于：“它坏了”
- 但你真正需要的是：“为什么坏？影响大不大？怎么修？先做什么？”
- 更要命的是：坏一次可能刷屏几十条告警（告警风暴）

所以我做了一个东西：KubePilot AI Operator（kubeai-controller）。你可以把它当成一个部署在集群里的“AI 运维机器人”：
- 看到集群里出现异常信号（Event）
- 自动收集上下文（对象信息、原因、必要时拉一点日志）
- 交给大模型（DeepSeek）分析
- 把结果用 Markdown（好看的排版）发到钉钉/企微
- 还会“降噪”：同样的告警别刷屏，并且会定时发“汇总摘要”
- 还支持每日巡检：每天自动生成一份“集群体检报告”，让 AI 润色后再发

---

## 1. 这个机器人到底能做什么？（一句话版）

### 1) 出故障时：自动把“机器话”翻译成人话

比如 Kubernetes 只告诉你：CrashLoopBackOff。机器人会补全成：
- 可能原因：配置错误 / 依赖不可用 / 权限不足 / 镜像启动失败…
- 建议操作：先看哪个 Pod、在哪个命名空间、重启次数多少、日志关键错误是什么
- 严重程度：Warning 还是 Critical
- 最后推送一条 Markdown 到群里（而不是让你去翻一堆 kubectl 输出）

### 2) 告警不刷屏：自动降噪 + 汇总

同一个问题 1 分钟内连刷 50 次，机器人不会发 50 条，而是：
- 只发 1 条
- 后面抑制掉
- 过一段时间再发一条“收敛摘要”，告诉你这段时间被压住了多少条

### 3) 每天巡检：自动生成“集群体检报告”

报告里包含：
- 核心组件健康情况（apiserver、scheduler、coredns、metrics-server 等）
- 每个节点的 CPU/内存使用率（尽量取实时 usage，拿不到就用 requests 估算）
- 工作负载规模：deployment / statefulset / daemonset / job / cronjob 数量
- Pod 总数、异常 Pod 数量、异常原因 Top
- 异常 Pod 的 AI 分析（可带日志片段）

并且：报告发出去前，会先让 AI 做“排版和摘要增强”，让普通人也能读懂重点。

---

## 2. 它为什么靠谱？因为它是“控制器”，不是脚本

你可能听过两种做法：
- 脚本/定时任务：每隔几分钟扫一遍集群
- 控制器（Controller）：一直在那儿盯着“事件流”，有变化就处理

KubePilot AI Operator 用的是第二种。好处是：
- 更快：事件一出现就能触发
- 更省：不需要频繁全量扫描
- 更像产品：逻辑可以持续演进，而不是散落脚本越堆越乱

---

## 3. 它怎么工作？（通俗的“流水线”）

我把它的工作过程拆成两条链路：

### 链路 A：异常告警链路（Event → AIIncident → 通知）

1) Kubernetes 里发生异常，会生成一个“事件”（Event），可以理解成：系统发了一条“内部告警短信”。  
2) 机器人只关注 Warning 类型的事件（真正有问题的）。  
3) 它把事件转成一张自定义故障单：AIIncident（一个 CRD）。  
4) 然后进入状态机处理：Pending → Analyzing → Notifying → Resolved/Failed。  

### 链路 B：每日巡检链路（定时 → 采集 → 报告 → AI 渲染 → 发送）

1) 按 Cron 时间到点触发（Asia/Shanghai）。  
2) 采集集群指标 + 统计信息。  
3) 生成原始 Markdown 报告。  
4) 让 AI 把报告变得更易读（摘要、重点、排版）。  
5) 推送到群里。  

---

## 4. 贴一点关键代码逻辑（看得懂的那种）

下面这些代码不是让你背语法，而是让你快速看懂“它在干嘛”。为了可读性，我用“简化逻辑版”表达核心流程。

### 4.1 监听事件（只处理 Warning）

核心思想：不是扫 Pod，而是监听集群事件。

```go
func Reconcile(event) {
  if event.Type != "Warning" {
    return // 只关心真正的异常
  }

  if event.TooOld(15 * time.Minute) {
    return // 太久远的历史事件不重复处理
  }

  trigger := mapEventToTrigger(event)   // 把事件翻译成“触发原因”
  name := incidentNameForEvent(event)   // 做一个稳定的名字，保证幂等
  createAIIncident(name, trigger, event)
}
```

它的意义是：
- Warning 才是问题（Normal 多是正常变化）
- 过滤历史事件，防止控制器重启后“翻旧账”
- 幂等命名避免重复创建

### 4.2 告警降噪：同一类问题不要刷屏

你当前实现里有一个核心“去重键”（Dedup Key）：

```
cluster/namespace/kind/name/title
```

意思是：同一个集群、同一个命名空间、同一种资源、同一个对象、同一个标题 = 同一类告警。

然后它会做两件事：

#### 冷却期（Cooldown）

```go
if now.Sub(lastSent) < cooldown {
  suppress++
  return // 直接不发
}
```

#### 限流（MaxPerMinute）

```go
if sentInLastMinute >= maxPerMinute {
  suppress++
  return // 直接不发
}
```

最后：如果这次真的要发，而且之前压住了很多次，会把标题改成：
- 标题（已收敛 N 次）

你在群里看到就会立刻明白：这不是第一次发生，但它不会刷屏。

### 4.3 定时发“收敛摘要”（把被压住的告警汇总发一条）

逻辑版：

```go
Every DigestInterval:
  items := pickSuppressedWhereCountGte(DigestMinCount)
  if len(items) == 0 {
    return
  }
  markdown := buildDigestMarkdown(items)
  send(markdown)
```

效果是：即使告警被抑制了，你也不会丢信息，反而更清晰：
- 这段时间哪些问题最频繁
- 各自被收敛了多少次
- 发生的时间范围

### 4.4 巡检报告：先生成“事实”，再让 AI 润色

巡检的核心步骤是：

```go
raw := buildReportMarkdown(components, nodes, workloads, pods, abnormalSummary, podAnalyses)
report := renderReportByLLM(raw) // AI 润色排版
sendMarkdown(report)
```

这个顺序很重要：
- 原始采集数据要客观（事实）
- AI 负责表达（让人看懂、抓重点、给建议）

---

## 5. 你在群里最终会看到什么样的消息？

### 异常告警（示例）

- 标题：Kubernetes 异常: CrashLoopBackOff
- 内容（Markdown）通常会包含：
  - 哪个 namespace / 哪个 Pod
  - 发生原因（AI 分析）
  - 建议操作（按优先级列出）
  - 置信度

### 巡检报告（示例）

- 标题：Kubernetes 每日巡检报告 2026-07-30
- 内容（Markdown）通常会包含：
  - 集群概览
  - 核心组件健康
  - 节点 CPU/内存使用率（每个节点一行）
  - 工作负载数量
  - Pod 统计
  - 异常 Pod Top 原因
  - AI 总结：重点风险 + 建议

---

## 6. 为什么普通人也能受益？

因为它做的事情本质是：
- 把机器的“信号”翻译成“人能理解的话”
- 把一堆零散信息变成“可执行建议”
- 把刷屏变成“摘要”
- 把巡检从“堆数据”变成“读得懂的日报”

你不需要会 kubectl，不需要懂调度机制，也能知道：
- 现在集群有没有危险
- 危险在哪
- 先做什么

---

## 7. 下一步：把它从“能跑”变成“更像产品”

基于你现在的能力，下一步最值的增强方向是：
- 钉钉关键词问题做成配置：自动在标题里注入关键词，避免 310000 再出现
- 巡检报告排版更“公众号风”：更多表格/分段/重点高亮
- 事件 → AIIncident 的映射规则做成可配置策略（白名单/黑名单）
- 针对常见故障（拉取镜像失败、OOM、探针失败）做固定模板 + AI 补充原因/建议

---

## 结尾

这就是 KubePilot AI Operator：一个跑在 Kubernetes 里的 AI 运维机器人。

它不只是“会发告警”，而是能把告警变成：可读、可执行、可治理 的运维信息。
