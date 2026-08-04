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
	quotaRisks := r.collectQuotaRisks(ctx)
	pvcs := r.collectPVCs(ctx)

	podAnalyses := r.analyzePods(ctx, clientset, abnormalPods)

	rawReport := buildReportMarkdown(time.Now().In(loc), components, nodeSummary, workloads, pods, abnormalSummary, abnormalPods, quotaRisks, pvcs, podAnalyses)
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
			Cluster:   r.Config.Controller.ClusterName,
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

type unhealthyWorkload struct {
	Kind      string
	Namespace string
	Name      string
	Desired   int32
	Ready     int32
	Extra     string
}

type workloadsSummary struct {
	Counts            workloadCounts
	Unhealthy         []unhealthyWorkload
	FailedJobs        int
	SuspendedCronJobs int
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
	TopNamespaces []namespaceCount
}

type reasonCount struct {
	Reason string
	Count  int
}

type namespaceCount struct {
	Namespace string
	Total     int
	Abnormal  int
}

type quotaRisk struct {
	Namespace string
	Name      string
	Resource  corev1.ResourceName
	Used      resource.Quantity
	Hard      resource.Quantity
	Ratio     float64
}

type pvcSummary struct {
	Total   int
	Bound   int
	Pending int
	Lost    int
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

func (r *Runner) collectWorkloads(ctx context.Context) workloadsSummary {
	var out workloadsSummary

	var dl appsv1.DeploymentList
	_ = r.Client.List(ctx, &dl)
	out.Counts.Deployments = len(dl.Items)
	for _, d := range dl.Items {
		desired := int32(1)
		if d.Spec.Replicas != nil {
			desired = *d.Spec.Replicas
		}
		ready := d.Status.ReadyReplicas
		if desired > 0 && ready < desired {
			out.Unhealthy = append(out.Unhealthy, unhealthyWorkload{
				Kind:      "Deployment",
				Namespace: d.Namespace,
				Name:      d.Name,
				Desired:   desired,
				Ready:     ready,
				Extra:     fmt.Sprintf("available=%d updated=%d", d.Status.AvailableReplicas, d.Status.UpdatedReplicas),
			})
		}
	}

	var sl appsv1.StatefulSetList
	_ = r.Client.List(ctx, &sl)
	out.Counts.StatefulSets = len(sl.Items)
	for _, s := range sl.Items {
		desired := int32(1)
		if s.Spec.Replicas != nil {
			desired = *s.Spec.Replicas
		}
		ready := s.Status.ReadyReplicas
		if desired > 0 && ready < desired {
			out.Unhealthy = append(out.Unhealthy, unhealthyWorkload{
				Kind:      "StatefulSet",
				Namespace: s.Namespace,
				Name:      s.Name,
				Desired:   desired,
				Ready:     ready,
				Extra:     fmt.Sprintf("current=%d updated=%d", s.Status.CurrentReplicas, s.Status.UpdatedReplicas),
			})
		}
	}

	var ds appsv1.DaemonSetList
	_ = r.Client.List(ctx, &ds)
	out.Counts.DaemonSets = len(ds.Items)
	for _, d := range ds.Items {
		desired := d.Status.DesiredNumberScheduled
		ready := d.Status.NumberReady
		if desired > 0 && ready < desired {
			out.Unhealthy = append(out.Unhealthy, unhealthyWorkload{
				Kind:      "DaemonSet",
				Namespace: d.Namespace,
				Name:      d.Name,
				Desired:   desired,
				Ready:     ready,
				Extra:     fmt.Sprintf("available=%d misscheduled=%d", d.Status.NumberAvailable, d.Status.NumberMisscheduled),
			})
		}
	}

	var jl batchv1.JobList
	_ = r.Client.List(ctx, &jl)
	out.Counts.Jobs = len(jl.Items)
	for _, j := range jl.Items {
		if j.Status.Failed > 0 {
			out.FailedJobs++
		}
	}

	var cjl batchv1.CronJobList
	_ = r.Client.List(ctx, &cjl)
	out.Counts.CronJobs = len(cjl.Items)
	for _, cj := range cjl.Items {
		if cj.Spec.Suspend != nil && *cj.Spec.Suspend {
			out.SuspendedCronJobs++
		}
	}

	sort.Slice(out.Unhealthy, func(i, j int) bool {
		gi := out.Unhealthy[i].Desired - out.Unhealthy[i].Ready
		gj := out.Unhealthy[j].Desired - out.Unhealthy[j].Ready
		if gi != gj {
			return gi > gj
		}
		if out.Unhealthy[i].Kind != out.Unhealthy[j].Kind {
			return out.Unhealthy[i].Kind < out.Unhealthy[j].Kind
		}
		if out.Unhealthy[i].Namespace != out.Unhealthy[j].Namespace {
			return out.Unhealthy[i].Namespace < out.Unhealthy[j].Namespace
		}
		return out.Unhealthy[i].Name < out.Unhealthy[j].Name
	})
	if len(out.Unhealthy) > 10 {
		out.Unhealthy = out.Unhealthy[:10]
	}

	return out
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
	totalByNS := make(map[string]int)
	abnormalByNS := make(map[string]int)
	abnormalCount := 0
	var abnormal []corev1.Pod

	for _, p := range pl.Items {
		totalByNS[p.Namespace]++
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
			abnormalByNS[p.Namespace]++
			abnormal = append(abnormal, p)
			reasonCounter[reason]++
		}
	}

	sort.Slice(abnormal, func(i, j int) bool {
		return totalRestarts(&abnormal[i]) > totalRestarts(&abnormal[j])
	})
	if len(abnormal) > 15 {
		abnormal = abnormal[:15]
	}

	var reasons []reasonCount
	for k, v := range reasonCounter {
		reasons = append(reasons, reasonCount{Reason: k, Count: v})
	}
	sort.Slice(reasons, func(i, j int) bool { return reasons[i].Count > reasons[j].Count })
	if len(reasons) > 5 {
		reasons = reasons[:5]
	}

	var nsList []namespaceCount
	for ns, total := range totalByNS {
		nsList = append(nsList, namespaceCount{
			Namespace: ns,
			Total:     total,
			Abnormal:  abnormalByNS[ns],
		})
	}
	sort.Slice(nsList, func(i, j int) bool {
		if nsList[i].Abnormal != nsList[j].Abnormal {
			return nsList[i].Abnormal > nsList[j].Abnormal
		}
		return nsList[i].Total > nsList[j].Total
	})
	if len(nsList) > 8 {
		nsList = nsList[:8]
	}

	return pc, abnormal, abnormalPodsSummary{
		AbnormalCount: abnormalCount,
		TopReasons:    reasons,
		TopNamespaces: nsList,
	}
}

func (r *Runner) collectPVCs(ctx context.Context) pvcSummary {
	var pl corev1.PersistentVolumeClaimList
	_ = r.Client.List(ctx, &pl)
	out := pvcSummary{Total: len(pl.Items)}
	for _, p := range pl.Items {
		switch p.Status.Phase {
		case corev1.ClaimBound:
			out.Bound++
		case corev1.ClaimPending:
			out.Pending++
		case corev1.ClaimLost:
			out.Lost++
		}
	}
	return out
}

func (r *Runner) collectQuotaRisks(ctx context.Context) []quotaRisk {
	var ql corev1.ResourceQuotaList
	_ = r.Client.List(ctx, &ql)

	var out []quotaRisk
	for _, q := range ql.Items {
		hard := q.Status.Hard
		used := q.Status.Used
		for _, rn := range []corev1.ResourceName{corev1.ResourceRequestsMemory, corev1.ResourceRequestsCPU} {
			h, okH := hard[rn]
			u, okU := used[rn]
			if !okH || !okU || h.IsZero() {
				continue
			}

			ratio := quantityRatio(u, h, rn == corev1.ResourceRequestsCPU)
			if ratio < 0.90 {
				continue
			}
			out = append(out, quotaRisk{
				Namespace: q.Namespace,
				Name:      q.Name,
				Resource:  rn,
				Used:      u,
				Hard:      h,
				Ratio:     ratio,
			})
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Ratio > out[j].Ratio })
	if len(out) > 10 {
		out = out[:10]
	}
	return out
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

func quantityRatio(used resource.Quantity, hard resource.Quantity, cpu bool) float64 {
	if hard.IsZero() {
		return 0
	}
	if cpu {
		return float64(used.MilliValue()) / float64(hard.MilliValue())
	}
	return float64(used.Value()) / float64(hard.Value())
}

func formatCPU(q resource.Quantity) string {
	return fmt.Sprintf("%.2f cores", float64(q.MilliValue())/1000.0)
}

func formatMemoryGi(q resource.Quantity) string {
	return fmt.Sprintf("%.2f Gi", float64(q.Value())/1024.0/1024.0/1024.0)
}

func shorten(s string, max int) string {
	s = strings.TrimSpace(s)
	if s == "" || max <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func clamp100(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

func scoreNodeStatus(nodes nodeSummary) float64 {
	score := 100.0
	score -= float64(nodes.NotReadyCount) * 30
	score -= float64(nodes.PressureCount) * 10
	return clamp100(score)
}

func scoreNodeResource(nodes nodeSummary) float64 {
	if len(nodes.PerNodeUtil) == 0 {
		return 100
	}
	worstCPU := 0.0
	worstMem := 0.0
	over85 := 0
	for _, it := range nodes.PerNodeUtil {
		if it.CPUPercent > worstCPU {
			worstCPU = it.CPUPercent
		}
		if it.MemPercent > worstMem {
			worstMem = it.MemPercent
		}
		if it.MemPercent >= 85 {
			over85++
		}
	}
	penalty := 0.0
	if worstMem > 70 {
		penalty += (worstMem - 70) * 0.8
	}
	if worstCPU > 80 {
		penalty += (worstCPU - 80) * 0.3
	}
	penalty += float64(over85) * 3
	return clamp100(100 - penalty)
}

func scorePodStatus(total int, abnormal int) float64 {
	if total <= 0 {
		return 100
	}
	normal := total - abnormal
	if normal < 0 {
		normal = 0
	}
	return clamp100(float64(normal) / float64(total) * 100)
}

func scoreWorkloadKind(ws workloadsSummary, kind string) float64 {
	total := 0
	switch kind {
	case "Deployment":
		total = ws.Counts.Deployments
	case "StatefulSet":
		total = ws.Counts.StatefulSets
	case "DaemonSet":
		total = ws.Counts.DaemonSets
	}
	if total <= 0 {
		return 100
	}
	unhealthy := 0
	for _, it := range ws.Unhealthy {
		if it.Kind == kind {
			unhealthy++
		}
	}
	healthy := total - unhealthy
	if healthy < 0 {
		healthy = 0
	}
	return clamp100(float64(healthy) / float64(total) * 100)
}

func workloadStatusText(ws workloadsSummary, kind string) string {
	total := 0
	switch kind {
	case "Deployment":
		total = ws.Counts.Deployments
	case "StatefulSet":
		total = ws.Counts.StatefulSets
	case "DaemonSet":
		total = ws.Counts.DaemonSets
	}
	unhealthy := 0
	for _, it := range ws.Unhealthy {
		if it.Kind == kind {
			unhealthy++
		}
	}
	if total <= 0 {
		return "✅ 无"
	}
	if unhealthy == 0 {
		return "✅ 全部正常"
	}
	return fmt.Sprintf("⚠️ 异常 %d", unhealthy)
}

func scoreStorage(pvcs pvcSummary) float64 {
	if pvcs.Total <= 0 {
		return 100
	}
	return clamp100(float64(pvcs.Bound) / float64(pvcs.Total) * 100)
}

func scoreOverall(nodeStatus, nodeResource, podStatus, deploy, sts, storage float64) float64 {
	w := 0.0
	sum := 0.0
	for _, it := range []struct {
		V float64
		W float64
	}{
		{nodeStatus, 0.20},
		{nodeResource, 0.20},
		{podStatus, 0.20},
		{deploy, 0.20},
		{sts, 0.10},
		{storage, 0.10},
	} {
		sum += it.V * it.W
		w += it.W
	}
	if w <= 0 {
		return 100
	}
	return clamp100(sum / w)
}

func scoreStars(score float64) string {
	if score >= 95 {
		return "⭐⭐"
	}
	if score >= 90 {
		return "⭐"
	}
	return ""
}

type imagePullFailure struct {
	Namespace string
	Image     string
	Message   string
	Reason    string
}

func findImagePullFailures(pods []corev1.Pod) []imagePullFailure {
	var out []imagePullFailure
	for _, p := range pods {
		reason := abnormalReason(&p)
		if reason != "ImagePullBackOff" && reason != "ErrImagePull" {
			continue
		}
		image := ""
		if len(p.Spec.Containers) > 0 {
			image = p.Spec.Containers[0].Image
		}
		for _, cs := range p.Status.ContainerStatuses {
			if cs.State.Waiting != nil && (cs.State.Waiting.Reason == "ImagePullBackOff" || cs.State.Waiting.Reason == "ErrImagePull") {
				if cs.Image != "" {
					image = cs.Image
				}
				out = append(out, imagePullFailure{
					Namespace: p.Namespace,
					Image:     image,
					Message:   shorten(cs.State.Waiting.Message, 120),
					Reason:    cs.State.Waiting.Reason,
				})
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Image < out[j].Image
	})
	if len(out) > 3 {
		out = out[:3]
	}
	return out
}

func buildFocusItems(nodes nodeSummary, abnormalPods []corev1.Pod, quotaRisks []quotaRisk) []string {
	var out []string

	if len(quotaRisks) > 0 {
		q := quotaRisks[0]
		severity := "关注"
		if q.Ratio >= 0.95 {
			severity = "紧急"
		}
		used := q.Used.String()
		hard := q.Hard.String()
		if q.Resource == corev1.ResourceRequestsMemory {
			used = formatMemoryGi(q.Used)
			hard = formatMemoryGi(q.Hard)
		} else if q.Resource == corev1.ResourceRequestsCPU {
			used = formatCPU(q.Used)
			hard = formatCPU(q.Hard)
		}
		out = append(out, fmt.Sprintf("资源配额接近上限（%s）\n- 命名空间：%s\n- 问题：%s 已用 %s / 限制 %s（%.0f%%）\n- 影响：新 Pod 可能无法创建（ExceededQuota/FailedCreate）\n- 建议：扩容配额或清理闲置工作负载", severity, q.Namespace, string(q.Resource), used, hard, clamp100(q.Ratio*100)))
	}

	var highMem []nodeUtil
	for _, it := range nodes.PerNodeUtil {
		if it.MemPercent >= 80 {
			highMem = append(highMem, it)
		}
	}
	sort.Slice(highMem, func(i, j int) bool { return highMem[i].MemPercent > highMem[j].MemPercent })
	if len(highMem) > 0 {
		var b strings.Builder
		b.WriteString(fmt.Sprintf("节点内存偏高（%d 个节点）", len(highMem)))
		for i := 0; i < len(highMem) && i < 4; i++ {
			it := highMem[i]
			b.WriteString(fmt.Sprintf("\n- %s：CPU %.0f%% / 内存 %.0f%%", it.Name, it.CPUPercent, it.MemPercent))
		}
		out = append(out, b.String())
	}

	imgFails := findImagePullFailures(abnormalPods)
	if len(imgFails) > 0 {
		var b strings.Builder
		b.WriteString("镜像拉取失败")
		for _, it := range imgFails {
			msg := ""
			if strings.TrimSpace(it.Message) != "" {
				msg = "\n  - 问题：" + it.Message
			}
			b.WriteString(fmt.Sprintf("\n- 命名空间：%s\n  - 原因：%s\n  - 镜像：%s%s", it.Namespace, it.Reason, it.Image, msg))
		}
		out = append(out, b.String())
	}

	if len(out) > 5 {
		out = out[:5]
	}
	return out
}

func buildSuggestedActions(nodes nodeSummary, abnormalPods []corev1.Pod, quotaRisks []quotaRisk) []string {
	var out []string
	if len(quotaRisks) > 0 {
		out = append(out, fmt.Sprintf("紧急：检查 %s 资源配额（ResourceQuota），考虑扩容或清理闲置 Pod/工作负载", quotaRisks[0].Namespace))
	}
	for _, it := range nodes.PerNodeUtil {
		if it.MemPercent >= 80 {
			out = append(out, "关注：监控高内存节点，必要时迁移/分散负载，检查内存泄漏与 Requests/Limits 配置")
			break
		}
	}
	if len(findImagePullFailures(abnormalPods)) > 0 {
		out = append(out, "检查：镜像仓库权限、网络连通性、镜像 tag 是否存在；必要时在节点侧验证拉取")
	}
	if len(out) > 3 {
		out = out[:3]
	}
	return out
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

func buildReportMarkdown(now time.Time, comps componentsSummary, nodes nodeSummary, ws workloadsSummary, pc podCounts, abnormal abnormalPodsSummary, abnormalPods []corev1.Pod, quotaRisks []quotaRisk, pvcs pvcSummary, analyses []podAnalysis) string {
	var b strings.Builder

	statusEmoji := "🟢"
	if comps.UnhealthyCount > 0 || abnormal.AbnormalCount > 0 || nodes.PressureCount > 0 {
		statusEmoji = "🟡"
	}
	if nodes.NotReadyCount > 0 {
		statusEmoji = "🔴"
	}

	b.WriteString(fmt.Sprintf("# %s Kubernetes 每日巡检报告 · %s\n\n", statusEmoji, now.Format("2006-01-02")))
	b.WriteString(fmt.Sprintf("> 巡检时间：%s\n\n", now.Format("2006-01-02 15:04:05")))

	nodeStatusScore := scoreNodeStatus(nodes)
	nodeResourceScore := scoreNodeResource(nodes)
	podStatusScore := scorePodStatus(pc.Total, abnormal.AbnormalCount)
	deployScore := scoreWorkloadKind(ws, "Deployment")
	stsScore := scoreWorkloadKind(ws, "StatefulSet")
	storageScore := scoreStorage(pvcs)
	totalScore := scoreOverall(nodeStatusScore, nodeResourceScore, podStatusScore, deployScore, stsScore, storageScore)

	b.WriteString("## 📊 健康评分\n")
	b.WriteString(fmt.Sprintf("- 节点状态：%.1f%%\n", nodeStatusScore))
	b.WriteString(fmt.Sprintf("- 节点资源：%.1f%%\n", nodeResourceScore))
	b.WriteString(fmt.Sprintf("- Pod 状态：%.1f%%\n", podStatusScore))
	b.WriteString(fmt.Sprintf("- Deployments：%.1f%%\n", deployScore))
	b.WriteString(fmt.Sprintf("- StatefulSets：%.1f%%\n", stsScore))
	b.WriteString(fmt.Sprintf("- 存储：%.1f%%\n\n", storageScore))
	b.WriteString(fmt.Sprintf("- 综合评分：%.1f / 100 %s\n\n", totalScore, scoreStars(totalScore)))

	b.WriteString("## 📋 集群概况\n")
	nodeStatusText := "✅ 全部正常"
	if nodes.NotReadyCount > 0 || nodes.PressureCount > 0 {
		nodeStatusText = fmt.Sprintf("⚠️ Ready %d / NotReady %d / Pressure %d", nodes.ReadyCount, nodes.NotReadyCount, nodes.PressureCount)
	}
	b.WriteString(fmt.Sprintf("- 节点总数：%d（%s）\n", nodes.Total, nodeStatusText))
	normalPods := pc.Total - abnormal.AbnormalCount
	if normalPods < 0 {
		normalPods = 0
	}
	b.WriteString(fmt.Sprintf("- Pod 总数：%d（%d 正常 / %d 异常）\n", pc.Total, normalPods, abnormal.AbnormalCount))
	b.WriteString(fmt.Sprintf("- Deployments：%d（%s）\n", ws.Counts.Deployments, workloadStatusText(ws, "Deployment")))
	b.WriteString(fmt.Sprintf("- StatefulSets：%d（%s）\n", ws.Counts.StatefulSets, workloadStatusText(ws, "StatefulSet")))
	pvcStatusText := "✅ 全部正常"
	if pvcs.Pending > 0 || pvcs.Lost > 0 {
		pvcStatusText = fmt.Sprintf("⚠️ Bound %d / Pending %d / Lost %d", pvcs.Bound, pvcs.Pending, pvcs.Lost)
	}
	b.WriteString(fmt.Sprintf("- PVC：%d（%s）\n\n", pvcs.Total, pvcStatusText))

	focus := buildFocusItems(nodes, abnormalPods, quotaRisks)
	if len(focus) > 0 {
		b.WriteString("## ⚠️ 重点关注\n")
		for i, it := range focus {
			b.WriteString(fmt.Sprintf("%d. %s\n", i+1, it))
		}
		b.WriteString("\n")

		actions := buildSuggestedActions(nodes, abnormalPods, quotaRisks)
		if len(actions) > 0 {
			b.WriteString("## 💡 建议操作\n")
			for i, it := range actions {
				b.WriteString(fmt.Sprintf("%d. %s\n", i+1, it))
			}
			b.WriteString("\n")
		}
	} else {
		b.WriteString("## ⚠️ 重点关注\n- 无明显风险项\n\n")
	}

	b.WriteString("## 🔍 详细信息\n")
	b.WriteString(fmt.Sprintf("- 节点：%d 台（Ready %d / NotReady %d / Pressure %d）\n", nodes.Total, nodes.ReadyCount, nodes.NotReadyCount, nodes.PressureCount))
	if comps.UnhealthyCount == 0 {
		b.WriteString("- 集群组件：正常\n")
	} else {
		b.WriteString(fmt.Sprintf("- 集群组件：%d 个实例异常\n", comps.UnhealthyCount))
		for _, it := range comps.Items {
			if it.Unhealthy > 0 {
				b.WriteString(fmt.Sprintf("  - %s：%d/%d Ready\n", it.Name, it.Ready, it.Total))
			}
		}
	}
	b.WriteString(fmt.Sprintf("- Pod：%d 个（Running %d / Pending %d / Failed %d / Succeeded %d / Unknown %d）\n", pc.Total, pc.Running, pc.Pending, pc.Failed, pc.Succeeded, pc.Unknown))
	if abnormal.AbnormalCount > 0 {
		b.WriteString(fmt.Sprintf("- 异常 Pod：%d 个", abnormal.AbnormalCount))
		if len(abnormal.TopReasons) > 0 {
			var parts []string
			for _, it := range abnormal.TopReasons {
				parts = append(parts, fmt.Sprintf("%s %d", it.Reason, it.Count))
			}
			b.WriteString(fmt.Sprintf("（Top 原因：%s）", strings.Join(parts, "、")))
		}
		b.WriteString("\n")
	}
	if len(abnormal.TopNamespaces) > 0 {
		var parts []string
		for i := 0; i < len(abnormal.TopNamespaces) && i < 5; i++ {
			it := abnormal.TopNamespaces[i]
			if it.Abnormal <= 0 {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s %d/%d", it.Namespace, it.Abnormal, it.Total))
		}
		if len(parts) > 0 {
			b.WriteString(fmt.Sprintf("- 异常命名空间：%s\n", strings.Join(parts, "、")))
		}
	}
	if len(ws.Unhealthy) > 0 || ws.FailedJobs > 0 || ws.SuspendedCronJobs > 0 {
		var parts []string
		if len(ws.Unhealthy) > 0 {
			parts = append(parts, fmt.Sprintf("不健康工作负载 %d", len(ws.Unhealthy)))
		}
		if ws.FailedJobs > 0 {
			parts = append(parts, fmt.Sprintf("失败 Job %d", ws.FailedJobs))
		}
		if ws.SuspendedCronJobs > 0 {
			parts = append(parts, fmt.Sprintf("暂停 CronJob %d", ws.SuspendedCronJobs))
		}
		b.WriteString(fmt.Sprintf("- 工作负载：%s\n", strings.Join(parts, " / ")))
	}
	b.WriteString("\n")

	if !nodes.TotalCPU.IsZero() && !nodes.TotalMem.IsZero() {
		b.WriteString("## 🧮 资源水位（集群汇总）\n")
		b.WriteString(fmt.Sprintf("- CPU：总量 %s / 请求 %s（%.1f%%）\n", formatCPU(nodes.TotalCPU), formatCPU(nodes.RequestedCPU), percent(nodes.RequestedCPU, nodes.TotalCPU)))
		b.WriteString(fmt.Sprintf("- 内存：总量 %s / 请求 %s（%.1f%%）\n\n", formatMemoryGi(nodes.TotalMem), formatMemoryGi(nodes.RequestedMem), percent(nodes.RequestedMem, nodes.TotalMem)))
	}

	if len(nodes.PerNodeUtil) > 0 {
		b.WriteString("## 🖥️ 节点资源使用率\n")
		for _, it := range nodes.PerNodeUtil {
			status := "Ready"
			if !it.Ready {
				status = "NotReady"
			}
			line := fmt.Sprintf("- %s：CPU %.1f%% / Mem %.1f%% / %s", it.Name, it.CPUPercent, it.MemPercent, status)
			if len(it.Pressures) > 0 {
				line += fmt.Sprintf(" / 压力：%s", strings.Join(it.Pressures, "、"))
			}
			if it.Source != "" && it.Source != "requests" {
				line += fmt.Sprintf("（口径：%s）", it.Source)
			}
			b.WriteString(line + "\n")
		}
		b.WriteString("\n")
	}

	b.WriteString("## 📦 工作负载\n")
	b.WriteString(fmt.Sprintf("- 规模：Deployment %d / StatefulSet %d / DaemonSet %d / Job %d / CronJob %d\n", ws.Counts.Deployments, ws.Counts.StatefulSets, ws.Counts.DaemonSets, ws.Counts.Jobs, ws.Counts.CronJobs))
	if ws.FailedJobs > 0 || ws.SuspendedCronJobs > 0 {
		var parts []string
		if ws.FailedJobs > 0 {
			parts = append(parts, fmt.Sprintf("失败 Job %d", ws.FailedJobs))
		}
		if ws.SuspendedCronJobs > 0 {
			parts = append(parts, fmt.Sprintf("暂停 CronJob %d", ws.SuspendedCronJobs))
		}
		b.WriteString(fmt.Sprintf("- 风险提示：%s\n", strings.Join(parts, " / ")))
	}
	if len(ws.Unhealthy) > 0 {
		b.WriteString("- 不健康工作负载（Top 10）：\n")
		for _, it := range ws.Unhealthy {
			extra := ""
			if strings.TrimSpace(it.Extra) != "" {
				extra = " / " + it.Extra
			}
			b.WriteString(fmt.Sprintf("  - %s %s/%s：Ready %d/%d%s\n", it.Kind, it.Namespace, it.Name, it.Ready, it.Desired, extra))
		}
	}
	b.WriteString("\n")

	if len(abnormalPods) > 0 {
		b.WriteString("## 🧩 异常 Pod 明细（Top 15）\n")
		for i := 0; i < len(abnormalPods) && i < 15; i++ {
			p := abnormalPods[i]
			reason := abnormalReason(&p)
			if reason == "" {
				reason = string(p.Status.Phase)
			}
			msg := shorten(podStatusMessage(&p), 90)
			node := p.Spec.NodeName
			if node == "" {
				node = "-"
			}
			b.WriteString(fmt.Sprintf("- %s/%s（node=%s / phase=%s / restarts=%d / reason=%s）\n", p.Namespace, p.Name, node, p.Status.Phase, totalRestarts(&p), reason))
			if msg != "" {
				b.WriteString(fmt.Sprintf("  - message：%s\n", msg))
			}
		}
		b.WriteString("\n")
	}

	if len(analyses) > 0 {
		b.WriteString("## 🤖 异常 Pod AI 分析（Top 5）\n")
		for i, a := range analyses {
			b.WriteString(fmt.Sprintf("**%d. %s/%s**\n", i+1, a.Namespace, a.Name))
			b.WriteString(fmt.Sprintf("- 原因：%s（置信度 %.0f%%）\n", a.Reason, a.Confidence*100))
			for j := 0; j < len(a.Suggestion) && j < 3; j++ {
				b.WriteString(fmt.Sprintf("- 建议：%s\n", a.Suggestion[j]))
			}
			b.WriteString("\n")
		}
	}

	b.WriteString("## ✅ 总结与建议\n")
	if comps.UnhealthyCount == 0 && nodes.NotReadyCount == 0 && abnormal.AbnormalCount == 0 {
		b.WriteString("- 集群整体健康，当前无需人工干预。\n")
	} else {
		if comps.UnhealthyCount > 0 {
			b.WriteString("- 存在组件不健康实例，建议优先检查 kube-system 关键组件 Pod。\n")
		}
		if nodes.NotReadyCount > 0 || nodes.PressureCount > 0 {
			b.WriteString("- 存在节点异常/压力，建议检查节点资源与系统日志。\n")
		}
		if abnormal.AbnormalCount > 0 {
			b.WriteString("- 存在异常 Pod，建议优先处理“异常 Pod 明细”中的 Top 项，并结合 AI 分析建议执行。\n")
			b.WriteString("- 常用排查命令：kubectl -n <ns> describe pod <pod>；kubectl -n <ns> logs <pod> --previous；kubectl -n <ns> get event --sort-by=.lastTimestamp\n")
		}
		if len(ws.Unhealthy) > 0 || ws.FailedJobs > 0 {
			b.WriteString("- 存在不健康工作负载/失败 Job，建议对照 desired/ready 差距与事件进行定位（Deployment/StatefulSet/DaemonSet/Job）。\n")
		}
	}

	return b.String()
}
