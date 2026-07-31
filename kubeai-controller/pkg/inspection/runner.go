package inspection

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kubepilot-ai/kubeai-controller/pkg/config"
	"github.com/kubepilot-ai/kubeai-controller/pkg/llm"
	"github.com/kubepilot-ai/kubeai-controller/pkg/notifier"
)

type Runner struct {
	Client          client.Client
	RestConfig      *rest.Config
	LLMManager      *llm.Manager
	NotifierManager *notifier.Manager
	Config          *config.ControllerConfig
	Location        *time.Location
}

func (r *Runner) Start(ctx context.Context) error {
	l := log.FromContext(ctx).WithName("inspection")
	if r.Config == nil || !r.Config.Inspection.Enabled {
		l.Info("inspection disabled")
		return nil
	}

	loc := r.Location
	if loc == nil {
		loc = time.FixedZone("CST", 8*3600)
	}

	s, err := parseCron5(r.Config.Inspection.Schedule)
	if err != nil {
		l.Error(err, "invalid inspection schedule, fallback to 0 2 * * *", "schedule", r.Config.Inspection.Schedule)
		s, _ = parseCron5("0 2 * * *")
	}

	for {
		next := s.next(time.Now(), loc)
		wait := time.Until(next)
		l.Info("next inspection scheduled", "at", next.Format("2006-01-02 15:04:05"), "wait", wait.String())

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}

		runCtx := ctx
		cancel := func() {}
		if r.Config.Inspection.Timeout > 0 {
			runCtx, cancel = context.WithTimeout(ctx, r.Config.Inspection.Timeout)
		}

		if err := r.runOnce(runCtx, loc); err != nil {
			l.Error(err, "inspection run failed")
		}
		cancel()
	}
}

func (r *Runner) runOnce(ctx context.Context, loc *time.Location) error {
	l := log.FromContext(ctx).WithName("inspection")

	clientset, err := kubernetes.NewForConfig(r.RestConfig)
	if err != nil {
		return err
	}

	nodes, nodeSummary, level := r.collectNodes(ctx)
	workloads := r.collectWorkloads(ctx)
	components := r.collectComponents(ctx)
	pods, abnormalPods, abnormalSummary := r.collectPods(ctx)

	podAnalyses := r.analyzePods(ctx, clientset, abnormalPods)

	rawReport := buildReportMarkdown(time.Now().In(loc), components, nodeSummary, workloads, pods, abnormalSummary, podAnalyses)
	report := r.renderReport(ctx, rawReport)

	if level == notifier.Success && abnormalSummary.AbnormalCount > 0 {
		level = notifier.Warning
	}
	if level == notifier.Success && (components.UnhealthyCount > 0 || nodeSummary.NotReadyCount > 0) {
		level = notifier.Warning
	}
	if nodeSummary.NotReadyCount > 0 {
		level = notifier.Critical
	}

	msg := &notifier.Message{
		Title:     fmt.Sprintf("Kubernetes 每日巡检报告 %s", time.Now().In(loc).Format("2006-01-02")),
		Level:     level,
		Content:   report,
		Timestamp: time.Now().In(loc),
		ForceSend: r.Config.Inspection.NotifyOnSuccess,
		ResourceInfo: notifier.ResourceInfo{
			Kind:      "ClusterInspection",
			Name:      fmt.Sprintf("daily-%s", time.Now().In(loc).Format("20060102")),
			Namespace: r.Config.Controller.Namespace,
		},
	}

	if !r.Config.Inspection.NotifyOnSuccess && level == notifier.Success {
		l.Info("inspection success notification disabled")
		return nil
	}

	if err := r.NotifierManager.Send(ctx, msg); err != nil {
		return err
	}

	l.Info("inspection report sent", "level", msg.Level, "nodes", len(nodes), "pods", pods.Total, "abnormalPods", abnormalSummary.AbnormalCount)
	return nil
}

func (r *Runner) renderReport(ctx context.Context, raw string) string {
	l := log.FromContext(ctx).WithName("inspection")

	p, err := r.LLMManager.GetDefaultProvider()
	if err != nil {
		l.Error(err, "failed to get default llm provider")
		return raw
	}
	renderer, ok := p.(llm.ReportRenderer)
	if !ok {
		l.Info("llm provider does not support report rendering", "provider", p.Name())
		return raw
	}
	out, err := renderer.RenderReport(ctx, raw)
	if err != nil {
		l.Error(err, "failed to render report via llm", "provider", p.Name())
		return raw
	}
	if out = strings.TrimSpace(out); out == "" {
		l.Error(fmt.Errorf("empty rendered report"), "failed to render report via llm", "provider", p.Name())
		return raw
	}
	return out
}

type workloadCounts struct {
	Deployments  int
	StatefulSets int
	DaemonSets   int
	Jobs         int
	CronJobs     int
}

type podCounts struct {
	Total     int
	Running   int
	Pending   int
	Succeeded int
	Failed    int
	Unknown   int
}

type componentsSummary struct {
	Items          []componentItem
	UnhealthyCount int
}

type componentItem struct {
	Name      string
	Ready     int
	Total     int
	Unhealthy int
}

type abnormalPodsSummary struct {
	AbnormalCount int
	TopReasons    []reasonCount
}

type reasonCount struct {
	Reason string
	Count  int
}

type nodeSummary struct {
	Total         int
	ReadyCount    int
	NotReadyCount int
	PressureCount int
	PerNodeUtil   []nodeUtil
	TotalCPU      resource.Quantity
	TotalMem      resource.Quantity
	RequestedCPU  resource.Quantity
	RequestedMem  resource.Quantity
}

type nodeUtil struct {
	Name       string
	CPUPercent float64
	MemPercent float64
	Ready      bool
	Pressures  []string
	Source     string
}

type podAnalysis struct {
	Namespace  string
	Name       string
	Reason     string
	Confidence float64
	Suggestion []string
}

func (r *Runner) collectWorkloads(ctx context.Context) workloadCounts {
	var wc workloadCounts

	var dl appsv1.DeploymentList
	_ = r.Client.List(ctx, &dl)
	wc.Deployments = len(dl.Items)

	var sl appsv1.StatefulSetList
	_ = r.Client.List(ctx, &sl)
	wc.StatefulSets = len(sl.Items)

	var ds appsv1.DaemonSetList
	_ = r.Client.List(ctx, &ds)
	wc.DaemonSets = len(ds.Items)

	var jl batchv1.JobList
	_ = r.Client.List(ctx, &jl)
	wc.Jobs = len(jl.Items)

	var cjl batchv1.CronJobList
	_ = r.Client.List(ctx, &cjl)
	wc.CronJobs = len(cjl.Items)

	return wc
}

func (r *Runner) collectComponents(ctx context.Context) componentsSummary {
	var pods corev1.PodList
	_ = r.Client.List(ctx, &pods, client.InNamespace("kube-system"))

	targets := []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler", "etcd", "coredns", "metrics-server"}
	m := make(map[string]*componentItem)
	for _, t := range targets {
		m[t] = &componentItem{Name: t}
	}

	for _, p := range pods.Items {
		for _, t := range targets {
			if strings.HasPrefix(p.Name, t) || strings.Contains(p.Name, t) {
				item := m[t]
				item.Total++
				ready := isPodReady(&p)
				if ready {
					item.Ready++
				} else {
					item.Unhealthy++
				}
			}
		}
	}

	out := componentsSummary{}
	for _, t := range targets {
		out.Items = append(out.Items, *m[t])
		if m[t].Unhealthy > 0 {
			out.UnhealthyCount += m[t].Unhealthy
		}
	}
	return out
}

func (r *Runner) collectNodes(ctx context.Context) ([]corev1.Node, nodeSummary, notifier.MessageLevel) {
	var nl corev1.NodeList
	_ = r.Client.List(ctx, &nl)

	var pods corev1.PodList
	_ = r.Client.List(ctx, &pods)

	podsByNode := make(map[string][]corev1.Pod)
	for _, p := range pods.Items {
		if p.Spec.NodeName == "" {
			continue
		}
		podsByNode[p.Spec.NodeName] = append(podsByNode[p.Spec.NodeName], p)
	}

	sum := nodeSummary{
		Total: len(nl.Items),
	}

	level := notifier.Success

	clientset, err := kubernetes.NewForConfig(r.RestConfig)
	if err != nil {
		clientset = nil
	}
	usage := map[string]map[string]resource.Quantity{}
	if clientset != nil {
		usage = fetchNodeUsage(ctx, clientset)
	}

	for _, n := range nl.Items {
		ready, pressures := nodeHealth(&n)
		if ready {
			sum.ReadyCount++
		} else {
			sum.NotReadyCount++
			level = notifier.Warning
		}
		if len(pressures) > 0 {
			sum.PressureCount++
			level = notifier.Warning
		}

		cpuCap := n.Status.Allocatable[corev1.ResourceCPU]
		memCap := n.Status.Allocatable[corev1.ResourceMemory]
		sum.TotalCPU.Add(cpuCap)
		sum.TotalMem.Add(memCap)

		var cpuReq, memReq resource.Quantity
		for _, p := range podsByNode[n.Name] {
			cpu, mem := podRequests(&p)
			cpuReq.Add(cpu)
			memReq.Add(mem)
		}
		sum.RequestedCPU.Add(cpuReq)
		sum.RequestedMem.Add(memReq)

		util := nodeUtil{
			Name:      n.Name,
			Ready:     ready,
			Pressures: pressures,
		}
		if u, ok := usage[n.Name]; ok {
			util.CPUPercent = percent(u["cpu"], cpuCap)
			util.MemPercent = percent(u["memory"], memCap)
			util.Source = "usage"
		} else {
			util.CPUPercent = percent(cpuReq, cpuCap)
			util.MemPercent = percent(memReq, memCap)
			util.Source = "requests"
		}
		sum.PerNodeUtil = append(sum.PerNodeUtil, util)
	}

	sort.Slice(sum.PerNodeUtil, func(i, j int) bool { return sum.PerNodeUtil[i].CPUPercent > sum.PerNodeUtil[j].CPUPercent })
	return nl.Items, sum, level
}

func (r *Runner) collectPods(ctx context.Context) (podCounts, []corev1.Pod, abnormalPodsSummary) {
	var pl corev1.PodList
	_ = r.Client.List(ctx, &pl)

	var pc podCounts
	pc.Total = len(pl.Items)

	reasonCounter := make(map[string]int)
	abnormalCount := 0
	var abnormal []corev1.Pod

	for _, p := range pl.Items {
		switch p.Status.Phase {
		case corev1.PodRunning:
			pc.Running++
		case corev1.PodPending:
			pc.Pending++
		case corev1.PodSucceeded:
			pc.Succeeded++
		case corev1.PodFailed:
			pc.Failed++
		default:
			pc.Unknown++
		}

		reason := abnormalReason(&p)
		if reason != "" {
			abnormalCount++
			abnormal = append(abnormal, p)
			reasonCounter[reason]++
		}
	}

	sort.Slice(abnormal, func(i, j int) bool {
		return totalRestarts(&abnormal[i]) > totalRestarts(&abnormal[j])
	})
	if len(abnormal) > 8 {
		abnormal = abnormal[:8]
	}

	var reasons []reasonCount
	for k, v := range reasonCounter {
		reasons = append(reasons, reasonCount{Reason: k, Count: v})
	}
	sort.Slice(reasons, func(i, j int) bool { return reasons[i].Count > reasons[j].Count })
	if len(reasons) > 5 {
		reasons = reasons[:5]
	}

	return pc, abnormal, abnormalPodsSummary{
		AbnormalCount: abnormalCount,
		TopReasons:    reasons,
	}
}

func (r *Runner) analyzePods(ctx context.Context, clientset *kubernetes.Clientset, pods []corev1.Pod) []podAnalysis {
	if len(pods) == 0 {
		return nil
	}

	var out []podAnalysis
	for i := 0; i < len(pods) && i < 5; i++ {
		p := pods[i]
		reason := abnormalReason(&p)
		logs := fetchPodLogs(ctx, clientset, p.Namespace, p.Name, 80)

		req := &llm.AnalysisRequest{
			ResourceType: "Pod",
			ResourceName: p.Name,
			Namespace:    p.Namespace,
			EventType:    reason,
			Message:      podStatusMessage(&p),
			Severity:     "warning",
			Logs:         logs,
			Metrics: map[string]string{
				"restarts": fmt.Sprintf("%d", totalRestarts(&p)),
			},
		}

		res, err := r.LLMManager.Analyze(ctx, "", req)
		if err != nil {
			continue
		}
		out = append(out, podAnalysis{
			Namespace:  p.Namespace,
			Name:       p.Name,
			Reason:     res.Reason,
			Confidence: res.Confidence,
			Suggestion: res.Suggestions,
		})
	}
	return out
}

func fetchPodLogs(ctx context.Context, clientset *kubernetes.Clientset, ns, name string, tail int64) []string {
	opt := &corev1.PodLogOptions{
		TailLines: &tail,
	}
	b, err := clientset.CoreV1().Pods(ns).GetLogs(name, opt).Do(ctx).Raw()
	if err != nil || len(b) == 0 {
		return nil
	}
	lines := strings.Split(string(b), "\n")
	var out []string
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		out = append(out, ln)
		if len(out) >= 120 {
			break
		}
	}
	return out
}

func fetchNodeUsage(ctx context.Context, clientset *kubernetes.Clientset) map[string]map[string]resource.Quantity {
	raw, err := clientset.RESTClient().Get().AbsPath("/apis/metrics.k8s.io/v1beta1/nodes").Do(ctx).Raw()
	if err != nil || len(raw) == 0 {
		return nil
	}

	var resp struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Usage map[string]string `json:"usage"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil
	}

	out := make(map[string]map[string]resource.Quantity)
	for _, it := range resp.Items {
		if it.Metadata.Name == "" {
			continue
		}
		cpuStr := it.Usage["cpu"]
		memStr := it.Usage["memory"]
		if cpuStr == "" && memStr == "" {
			continue
		}
		m := make(map[string]resource.Quantity)
		if cpuStr != "" {
			if q, err := resource.ParseQuantity(cpuStr); err == nil {
				m["cpu"] = q
			}
		}
		if memStr != "" {
			if q, err := resource.ParseQuantity(memStr); err == nil {
				m["memory"] = q
			}
		}
		if len(m) > 0 {
			out[it.Metadata.Name] = m
		}
	}
	return out
}

func isPodReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func nodeHealth(n *corev1.Node) (bool, []string) {
	ready := false
	var pressures []string
	for _, c := range n.Status.Conditions {
		switch c.Type {
		case corev1.NodeReady:
			ready = c.Status == corev1.ConditionTrue
		case corev1.NodeMemoryPressure:
			if c.Status == corev1.ConditionTrue {
				pressures = append(pressures, "MemoryPressure")
			}
		case corev1.NodeDiskPressure:
			if c.Status == corev1.ConditionTrue {
				pressures = append(pressures, "DiskPressure")
			}
		case corev1.NodePIDPressure:
			if c.Status == corev1.ConditionTrue {
				pressures = append(pressures, "PIDPressure")
			}
		}
	}
	return ready, pressures
}

func podRequests(p *corev1.Pod) (resource.Quantity, resource.Quantity) {
	var cpu, mem resource.Quantity
	for _, c := range p.Spec.Containers {
		if q, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
			cpu.Add(q)
		}
		if q, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
			mem.Add(q)
		}
	}
	return cpu, mem
}

func percent(req resource.Quantity, cap resource.Quantity) float64 {
	if cap.IsZero() {
		return 0
	}
	return float64(req.MilliValue()) / float64(cap.MilliValue()) * 100
}

func totalRestarts(p *corev1.Pod) int32 {
	var n int32
	for _, cs := range p.Status.ContainerStatuses {
		n += cs.RestartCount
	}
	return n
}

func abnormalReason(p *corev1.Pod) string {
	if p.DeletionTimestamp != nil {
		return ""
	}
	if p.Status.Phase == corev1.PodFailed {
		return "Failed"
	}
	if p.Status.Phase == corev1.PodPending {
		for _, cs := range p.Status.ContainerStatuses {
			if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
				return cs.State.Waiting.Reason
			}
		}
		if p.Status.Reason != "" {
			return p.Status.Reason
		}
		return "Pending"
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			switch cs.State.Waiting.Reason {
			case "CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError", "RunContainerError":
				return cs.State.Waiting.Reason
			}
		}
		if cs.RestartCount >= 5 {
			return "HighRestart"
		}
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			return "TerminatedNonZero"
		}
	}
	if p.Status.Phase == corev1.PodUnknown {
		return "Unknown"
	}
	return ""
}

func podStatusMessage(p *corev1.Pod) string {
	if p.Status.Message != "" {
		return p.Status.Message
	}
	if p.Status.Reason != "" {
		return p.Status.Reason
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Message != "" {
			return cs.State.Waiting.Message
		}
	}
	return ""
}

func buildReportMarkdown(now time.Time, comps componentsSummary, nodes nodeSummary, wc workloadCounts, pc podCounts, abnormal abnormalPodsSummary, analyses []podAnalysis) string {
	var b strings.Builder

	b.WriteString("Kubernetes 每日巡检报告\n\n")
	b.WriteString(fmt.Sprintf("时间：%s\n\n", now.Format("2006-01-02 15:04:05")))

	b.WriteString("### 集群组件健康\n")
	for _, it := range comps.Items {
		if it.Total == 0 {
			continue
		}
		b.WriteString(fmt.Sprintf("- %s：%d/%d Ready\n", it.Name, it.Ready, it.Total))
	}
	if comps.UnhealthyCount == 0 {
		b.WriteString("- 组件状态：正常\n\n")
	} else {
		b.WriteString(fmt.Sprintf("- 异常组件实例：%d\n\n", comps.UnhealthyCount))
	}

	b.WriteString("### 节点健康与资源使用率\n")
	b.WriteString(fmt.Sprintf("- 节点：%d（Ready=%d，NotReady=%d，Pressure=%d）\n\n", nodes.Total, nodes.ReadyCount, nodes.NotReadyCount, nodes.PressureCount))
	b.WriteString("| 节点 | CPU% | Mem% | 状态 | Pressure | 口径 |\n")
	b.WriteString("| --- | ---: | ---: | --- | --- | --- |\n")
	for _, it := range nodes.PerNodeUtil {
		status := "Ready"
		if !it.Ready {
			status = "NotReady"
		}
		p := "-"
		if len(it.Pressures) > 0 {
			p = strings.Join(it.Pressures, ",")
		}
		src := it.Source
		if src == "" {
			src = "requests"
		}
		b.WriteString(fmt.Sprintf("| %s | %.1f | %.1f | %s | %s | %s |\n", it.Name, it.CPUPercent, it.MemPercent, status, p, src))
	}
	b.WriteString("\n")

	b.WriteString("### 工作负载规模\n")
	b.WriteString(fmt.Sprintf("- Deployments=%d，StatefulSets=%d，DaemonSets=%d，Jobs=%d，CronJobs=%d\n\n", wc.Deployments, wc.StatefulSets, wc.DaemonSets, wc.Jobs, wc.CronJobs))

	b.WriteString("### Pod 统计\n")
	b.WriteString(fmt.Sprintf("- 总数=%d，Running=%d，Pending=%d，Failed=%d，Succeeded=%d，Unknown=%d\n", pc.Total, pc.Running, pc.Pending, pc.Failed, pc.Succeeded, pc.Unknown))
	if abnormal.AbnormalCount > 0 {
		b.WriteString(fmt.Sprintf("- 异常 Pod（非 Running/Succeeded）：%d\n", abnormal.AbnormalCount))
		if len(abnormal.TopReasons) > 0 {
			var parts []string
			for _, it := range abnormal.TopReasons {
				parts = append(parts, fmt.Sprintf("%s=%d", it.Reason, it.Count))
			}
			b.WriteString(fmt.Sprintf("- Top 原因：%s\n", strings.Join(parts, "，")))
		}
	} else {
		b.WriteString("- 异常 Pod：0\n")
	}
	b.WriteString("\n")

	if len(analyses) > 0 {
		b.WriteString("### 异常 Pod AI 分析（Top 5）\n")
		for i, a := range analyses {
			b.WriteString(fmt.Sprintf("%d) %s/%s\n", i+1, a.Namespace, a.Name))
			b.WriteString(fmt.Sprintf("   - 原因：%s（%.0f%%）\n", a.Reason, a.Confidence*100))
			if len(a.Suggestion) > 0 {
				for j := 0; j < len(a.Suggestion) && j < 3; j++ {
					b.WriteString(fmt.Sprintf("   - 建议：%s\n", a.Suggestion[j]))
				}
			}
			b.WriteString("\n")
		}
	}

	b.WriteString("### 总结\n")
	if comps.UnhealthyCount == 0 && nodes.NotReadyCount == 0 && abnormal.AbnormalCount == 0 {
		b.WriteString("- 集群整体健康，未发现需要立即处理的风险项。\n")
	} else {
		if comps.UnhealthyCount > 0 {
			b.WriteString("- 存在组件不健康实例，建议优先检查 kube-system 关键组件 Pod。\n")
		}
		if nodes.NotReadyCount > 0 || nodes.PressureCount > 0 {
			b.WriteString("- 存在节点异常/压力，建议检查节点资源与系统日志。\n")
		}
		if abnormal.AbnormalCount > 0 {
			b.WriteString("- 存在异常 Pod，已给出 Top 异常 Pod 的 AI 原因与建议。\n")
		}
	}

	return b.String()
}
