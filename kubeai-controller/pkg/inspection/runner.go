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
	networkingv1 "k8s.io/api/networking/v1"
	storagev1 "k8s.io/api/storage/v1"
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

	now := time.Now().In(loc)
	nodes, nodeSummary, level := r.collectNodes(ctx)
	workloads := r.collectWorkloads(ctx, now, loc)
	components := r.collectComponents(ctx)
	pods, abnormalPods, abnormalSummary := r.collectPods(ctx)
	quotaRisks := r.collectQuotaRisks(ctx)
	pvcSum, pvcRisks := r.collectPVCs(ctx, now)
	storage := r.collectStorage(ctx, now, pvcSum, pvcRisks)
	events := r.collectWarningEvents(ctx, now.Add(-24*time.Hour), now)
	network := r.collectNetwork(ctx, now)
	nsRes := r.collectNamespaceResources(ctx)

	podAnalyses := r.analyzePods(ctx, clientset, abnormalPods)

	rawReport := buildReportMarkdown(now, components, nodeSummary, workloads, pods, abnormalSummary, abnormalPods, quotaRisks, storage, events, network, nsRes, podAnalyses)
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
		Title:     fmt.Sprintf("Kubernetes 每日巡检报告 %s", now.Format("2006-01-02")),
		Level:     level,
		Content:   report,
		Timestamp: now,
		ForceSend: r.Config.Inspection.NotifyOnSuccess,
		ResourceInfo: notifier.ResourceInfo{
			Kind:      "ClusterInspection",
			Name:      fmt.Sprintf("daily-%s", now.Format("20060102")),
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

type jobRiskItem struct {
	Namespace       string
	Name            string
	Failed          int32
	Active          int32
	Succeeded       int32
	StartTime       *time.Time
	CompletionTime  *time.Time
	Age             time.Duration
	FailureReasons  []string
	OwnerCronJobKey string
}

type cronJobRiskItem struct {
	Namespace               string
	Name                    string
	Schedule                string
	Suspended               bool
	ConcurrencyPolicy       batchv1.ConcurrencyPolicy
	StartingDeadlineSeconds *int64
	Active                  int
	LastScheduleTime        *time.Time
	NextScheduleTime        *time.Time
	Age                     time.Duration
	RiskReason              string
}

type workloadsSummary struct {
	Counts            workloadCounts
	Unhealthy         []unhealthyWorkload
	FailedJobs        int
	SuspendedCronJobs int
	ActiveJobs        int
	ActiveCronJobs    int

	CronJobNeverRun  int
	CronJobOverdue   int
	UnsupportedCron  int
	RiskyCronJobs    []cronJobRiskItem
	FailedJobDetails []jobRiskItem
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

type pvcRiskItem struct {
	Namespace string
	Name      string
	Phase     corev1.PersistentVolumeClaimPhase
	Storage   string
	ClassName string
	Volume    string
	Age       time.Duration
}

type pvSummary struct {
	Total     int
	Bound     int
	Available int
	Released  int
	Failed    int
}

type pvRiskItem struct {
	Name      string
	Phase     corev1.PersistentVolumePhase
	Capacity  string
	ClassName string
	Claim     string
	Age       time.Duration
	Reason    string
}

type storageClassSummary struct {
	Total        int
	DefaultCount int
	Defaults     []string
}

type storageSummary struct {
	PVC         pvcSummary
	RiskyPVCs   []pvcRiskItem
	PV          pvSummary
	RiskyPVs    []pvRiskItem
	StorageCLs  storageClassSummary
	HasPVAccess bool
}

type warningEventSample struct {
	Namespace string
	Kind      string
	Name      string
	Reason    string
	Message   string
	Count     int32
	LastTime  time.Time
}

type warningEventSummary struct {
	Since        time.Time
	Total        int
	TopReasons   []reasonCount
	TopNamespace []namespaceCount
	Samples      []warningEventSample
}

type serviceRiskItem struct {
	Namespace string
	Name      string
	Type      corev1.ServiceType
	Age       time.Duration
	Reason    string
}

type serviceSummary struct {
	Total       int
	LBTotal     int
	LBPending   int
	RiskyTop    []serviceRiskItem
	ClusterIPs  int
	ExternalIPs int
}

type ingressRiskItem struct {
	Namespace string
	Name      string
	ClassName string
	Hosts     string
	Age       time.Duration
	Reason    string
}

type ingressSummary struct {
	Total     int
	PendingLB int
	RiskyTop  []ingressRiskItem
}

type networkSummary struct {
	Services serviceSummary
	Ingress  ingressSummary
}

type nsResourceItem struct {
	Namespace   string
	Pods        int
	Abnormal    int
	CPURequests resource.Quantity
	MemRequests resource.Quantity
}

type nsResourceSummary struct {
	TopCPU []nsResourceItem
	TopMem []nsResourceItem
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

func (r *Runner) collectWorkloads(ctx context.Context, now time.Time, loc *time.Location) workloadsSummary {
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
		if j.Status.Active > 0 {
			out.ActiveJobs++
		}

		var reasons []string
		for _, c := range j.Status.Conditions {
			if c.Status != corev1.ConditionTrue {
				continue
			}
			if strings.TrimSpace(c.Reason) != "" {
				reasons = append(reasons, string(c.Type)+"("+c.Reason+")")
			} else {
				reasons = append(reasons, string(c.Type))
			}
		}

		ownerKey := ""
		for _, ref := range j.OwnerReferences {
			if ref.Kind == "CronJob" && ref.Name != "" {
				ownerKey = j.Namespace + "/" + ref.Name
				break
			}
		}

		var start *time.Time
		if j.Status.StartTime != nil {
			t := j.Status.StartTime.Time
			start = &t
		} else if !j.CreationTimestamp.IsZero() {
			t := j.CreationTimestamp.Time
			start = &t
		}
		var age time.Duration
		if start != nil {
			age = now.Sub(*start)
			if age < 0 {
				age = 0
			}
		}

		if j.Status.Failed > 0 || len(reasons) > 0 {
			var completion *time.Time
			if j.Status.CompletionTime != nil {
				t := j.Status.CompletionTime.Time
				completion = &t
			}
			out.FailedJobDetails = append(out.FailedJobDetails, jobRiskItem{
				Namespace:       j.Namespace,
				Name:            j.Name,
				Failed:          j.Status.Failed,
				Active:          j.Status.Active,
				Succeeded:       j.Status.Succeeded,
				StartTime:       start,
				CompletionTime:  completion,
				Age:             age,
				FailureReasons:  reasons,
				OwnerCronJobKey: ownerKey,
			})
		}
	}
	sort.Slice(out.FailedJobDetails, func(i, j int) bool {
		if out.FailedJobDetails[i].Failed != out.FailedJobDetails[j].Failed {
			return out.FailedJobDetails[i].Failed > out.FailedJobDetails[j].Failed
		}
		if out.FailedJobDetails[i].Age != out.FailedJobDetails[j].Age {
			return out.FailedJobDetails[i].Age > out.FailedJobDetails[j].Age
		}
		if out.FailedJobDetails[i].Namespace != out.FailedJobDetails[j].Namespace {
			return out.FailedJobDetails[i].Namespace < out.FailedJobDetails[j].Namespace
		}
		return out.FailedJobDetails[i].Name < out.FailedJobDetails[j].Name
	})
	if len(out.FailedJobDetails) > 10 {
		out.FailedJobDetails = out.FailedJobDetails[:10]
	}

	var cjl batchv1.CronJobList
	_ = r.Client.List(ctx, &cjl)
	out.Counts.CronJobs = len(cjl.Items)
	for _, cj := range cjl.Items {
		if cj.Spec.Suspend != nil && *cj.Spec.Suspend {
			out.SuspendedCronJobs++
		}
		if len(cj.Status.Active) > 0 {
			out.ActiveCronJobs++
		}

		suspended := cj.Spec.Suspend != nil && *cj.Spec.Suspend
		var last *time.Time
		if cj.Status.LastScheduleTime != nil {
			t := cj.Status.LastScheduleTime.Time
			last = &t
		}
		var age time.Duration
		if !cj.CreationTimestamp.IsZero() {
			age = now.Sub(cj.CreationTimestamp.Time)
			if age < 0 {
				age = 0
			}
		}

		spec, ok := parseCronSimple5(cj.Spec.Schedule)
		next := (*time.Time)(nil)
		if ok {
			if last != nil {
				if n, ok2 := spec.next(*last, loc); ok2 {
					n2 := n
					next = &n2
				}
			} else {
				base := cj.CreationTimestamp.Time
				if n, ok2 := spec.next(base, loc); ok2 {
					n2 := n
					next = &n2
				}
			}
		} else {
			out.UnsupportedCron++
		}

		riskReason := ""
		grace := 3 * time.Minute
		if suspended {
			riskReason = "已暂停"
		} else if last == nil && age > 2*time.Hour {
			out.CronJobNeverRun++
			if ok {
				if next != nil && next.Before(now.Add(-grace)) {
					out.CronJobOverdue++
					riskReason = "未执行且疑似漏跑"
				} else {
					riskReason = "未观察到执行记录"
				}
			} else {
				riskReason = "未观察到执行记录（schedule 无法解析）"
			}
		} else if last != nil && ok {
			if next != nil && next.Before(now.Add(-grace)) {
				out.CronJobOverdue++
				riskReason = "疑似漏跑（lastScheduleTime 距今较久）"
			}
		}
		if !suspended && last != nil && len(cj.Status.Active) > 0 {
			if now.Sub(*last) > 2*time.Hour {
				if strings.TrimSpace(riskReason) != "" {
					riskReason += "；"
				}
				riskReason += "存在长时间 Active Job"
			}
		}

		if strings.TrimSpace(riskReason) != "" {
			out.RiskyCronJobs = append(out.RiskyCronJobs, cronJobRiskItem{
				Namespace:               cj.Namespace,
				Name:                    cj.Name,
				Schedule:                cj.Spec.Schedule,
				Suspended:               suspended,
				ConcurrencyPolicy:       cj.Spec.ConcurrencyPolicy,
				StartingDeadlineSeconds: cj.Spec.StartingDeadlineSeconds,
				Active:                  len(cj.Status.Active),
				LastScheduleTime:        last,
				NextScheduleTime:        next,
				Age:                     age,
				RiskReason:              riskReason,
			})
		}
	}
	sort.Slice(out.RiskyCronJobs, func(i, j int) bool {
		if out.RiskyCronJobs[i].Suspended != out.RiskyCronJobs[j].Suspended {
			return out.RiskyCronJobs[i].Suspended
		}
		if out.RiskyCronJobs[i].Active != out.RiskyCronJobs[j].Active {
			return out.RiskyCronJobs[i].Active > out.RiskyCronJobs[j].Active
		}
		if out.RiskyCronJobs[i].Namespace != out.RiskyCronJobs[j].Namespace {
			return out.RiskyCronJobs[i].Namespace < out.RiskyCronJobs[j].Namespace
		}
		return out.RiskyCronJobs[i].Name < out.RiskyCronJobs[j].Name
	})
	if len(out.RiskyCronJobs) > 10 {
		out.RiskyCronJobs = out.RiskyCronJobs[:10]
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

func (r *Runner) collectPVCs(ctx context.Context, now time.Time) (pvcSummary, []pvcRiskItem) {
	var pl corev1.PersistentVolumeClaimList
	_ = r.Client.List(ctx, &pl)

	out := pvcSummary{Total: len(pl.Items)}
	var risks []pvcRiskItem
	for _, p := range pl.Items {
		switch p.Status.Phase {
		case corev1.ClaimBound:
			out.Bound++
		case corev1.ClaimPending:
			out.Pending++
		case corev1.ClaimLost:
			out.Lost++
		}
		if p.Status.Phase != corev1.ClaimBound {
			st := "-"
			if q, ok := p.Spec.Resources.Requests[corev1.ResourceStorage]; ok && !q.IsZero() {
				st = q.String()
			}
			className := "-"
			if p.Spec.StorageClassName != nil && strings.TrimSpace(*p.Spec.StorageClassName) != "" {
				className = *p.Spec.StorageClassName
			}
			age := time.Duration(0)
			if !p.CreationTimestamp.IsZero() {
				age = now.Sub(p.CreationTimestamp.Time)
				if age < 0 {
					age = 0
				}
			}
			vol := strings.TrimSpace(p.Spec.VolumeName)
			if vol == "" {
				vol = "-"
			}
			risks = append(risks, pvcRiskItem{
				Namespace: p.Namespace,
				Name:      p.Name,
				Phase:     p.Status.Phase,
				Storage:   st,
				ClassName: className,
				Volume:    vol,
				Age:       age,
			})
		}
	}
	sort.Slice(risks, func(i, j int) bool {
		if risks[i].Phase != risks[j].Phase {
			return string(risks[i].Phase) < string(risks[j].Phase)
		}
		if risks[i].Age != risks[j].Age {
			return risks[i].Age > risks[j].Age
		}
		if risks[i].Namespace != risks[j].Namespace {
			return risks[i].Namespace < risks[j].Namespace
		}
		return risks[i].Name < risks[j].Name
	})
	if len(risks) > 10 {
		risks = risks[:10]
	}
	return out, risks
}

func (r *Runner) collectStorage(ctx context.Context, now time.Time, pvc pvcSummary, riskyPVCs []pvcRiskItem) storageSummary {
	out := storageSummary{
		PVC:       pvc,
		RiskyPVCs: riskyPVCs,
	}

	var pvl corev1.PersistentVolumeList
	if err := r.Client.List(ctx, &pvl); err == nil {
		out.HasPVAccess = true
		out.PV.Total = len(pvl.Items)
		for _, pv := range pvl.Items {
			switch pv.Status.Phase {
			case corev1.VolumeBound:
				out.PV.Bound++
			case corev1.VolumeAvailable:
				out.PV.Available++
			case corev1.VolumeReleased:
				out.PV.Released++
			case corev1.VolumeFailed:
				out.PV.Failed++
			}

			if pv.Status.Phase != corev1.VolumeBound {
				age := time.Duration(0)
				if !pv.CreationTimestamp.IsZero() {
					age = now.Sub(pv.CreationTimestamp.Time)
					if age < 0 {
						age = 0
					}
				}
				capacity := "-"
				if q, ok := pv.Spec.Capacity[corev1.ResourceStorage]; ok && !q.IsZero() {
					capacity = q.String()
				}
				className := strings.TrimSpace(pv.Spec.StorageClassName)
				if className == "" {
					className = "-"
				}
				claim := "-"
				if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name != "" {
					claim = pv.Spec.ClaimRef.Namespace + "/" + pv.Spec.ClaimRef.Name
				}
				reason := ""
				if pv.Status.Phase == corev1.VolumeFailed {
					reason = "Failed"
				} else if pv.Status.Phase == corev1.VolumeReleased {
					reason = "Released"
				} else if pv.Status.Phase == corev1.VolumeAvailable {
					reason = "Available"
				}
				out.RiskyPVs = append(out.RiskyPVs, pvRiskItem{
					Name:      pv.Name,
					Phase:     pv.Status.Phase,
					Capacity:  capacity,
					ClassName: className,
					Claim:     claim,
					Age:       age,
					Reason:    reason,
				})
			}
		}
		sort.Slice(out.RiskyPVs, func(i, j int) bool {
			if out.RiskyPVs[i].Phase != out.RiskyPVs[j].Phase {
				return string(out.RiskyPVs[i].Phase) < string(out.RiskyPVs[j].Phase)
			}
			if out.RiskyPVs[i].Age != out.RiskyPVs[j].Age {
				return out.RiskyPVs[i].Age > out.RiskyPVs[j].Age
			}
			return out.RiskyPVs[i].Name < out.RiskyPVs[j].Name
		})
		if len(out.RiskyPVs) > 10 {
			out.RiskyPVs = out.RiskyPVs[:10]
		}
	}

	var scl storagev1.StorageClassList
	if err := r.Client.List(ctx, &scl); err == nil {
		out.StorageCLs.Total = len(scl.Items)
		for _, sc := range scl.Items {
			if strings.EqualFold(sc.Annotations["storageclass.kubernetes.io/is-default-class"], "true") ||
				strings.EqualFold(sc.Annotations["storageclass.beta.kubernetes.io/is-default-class"], "true") {
				out.StorageCLs.DefaultCount++
				out.StorageCLs.Defaults = append(out.StorageCLs.Defaults, sc.Name)
			}
		}
		sort.Strings(out.StorageCLs.Defaults)
		if len(out.StorageCLs.Defaults) > 5 {
			out.StorageCLs.Defaults = out.StorageCLs.Defaults[:5]
		}
	}
	return out
}

func (r *Runner) collectWarningEvents(ctx context.Context, since time.Time, now time.Time) warningEventSummary {
	var el corev1.EventList
	_ = r.Client.List(ctx, &el)

	reasonCounter := make(map[string]int)
	totalByNS := make(map[string]int)
	var samples []warningEventSample
	m := make(map[string]warningEventSample)
	total := 0

	for _, e := range el.Items {
		if strings.TrimSpace(e.Type) != "Warning" {
			continue
		}
		ts := eventTimestamp(&e)
		if ts.IsZero() || ts.Before(since) || ts.After(now.Add(1*time.Minute)) {
			continue
		}
		total++
		reason := strings.TrimSpace(e.Reason)
		if reason == "" {
			reason = "Unknown"
		}
		reasonCounter[reason]++
		totalByNS[e.Namespace]++

		kind := strings.TrimSpace(e.InvolvedObject.Kind)
		name := strings.TrimSpace(e.InvolvedObject.Name)
		if kind == "" {
			kind = "-"
		}
		if name == "" {
			name = "-"
		}
		key := e.Namespace + "|" + kind + "|" + name + "|" + reason
		msg := shorten(e.Message, 120)
		item, ok := m[key]
		if !ok {
			item = warningEventSample{
				Namespace: e.Namespace,
				Kind:      kind,
				Name:      name,
				Reason:    reason,
				Message:   msg,
				Count:     e.Count,
				LastTime:  ts,
			}
		} else {
			item.Count += e.Count
			if ts.After(item.LastTime) {
				item.LastTime = ts
			}
			if item.Message == "" && msg != "" {
				item.Message = msg
			}
		}
		m[key] = item
	}

	for _, it := range m {
		samples = append(samples, it)
	}
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].Count != samples[j].Count {
			return samples[i].Count > samples[j].Count
		}
		return samples[i].LastTime.After(samples[j].LastTime)
	})
	if len(samples) > 8 {
		samples = samples[:8]
	}

	var reasons []reasonCount
	for k, v := range reasonCounter {
		reasons = append(reasons, reasonCount{Reason: k, Count: v})
	}
	sort.Slice(reasons, func(i, j int) bool { return reasons[i].Count > reasons[j].Count })
	if len(reasons) > 8 {
		reasons = reasons[:8]
	}

	var nsList []namespaceCount
	for ns, total := range totalByNS {
		nsList = append(nsList, namespaceCount{
			Namespace: ns,
			Total:     total,
			Abnormal:  0,
		})
	}
	sort.Slice(nsList, func(i, j int) bool { return nsList[i].Total > nsList[j].Total })
	if len(nsList) > 8 {
		nsList = nsList[:8]
	}

	return warningEventSummary{
		Since:        since,
		Total:        total,
		TopReasons:   reasons,
		TopNamespace: nsList,
		Samples:      samples,
	}
}

func eventTimestamp(e *corev1.Event) time.Time {
	if e == nil {
		return time.Time{}
	}
	if !e.EventTime.IsZero() {
		return e.EventTime.Time
	}
	if !e.LastTimestamp.IsZero() {
		return e.LastTimestamp.Time
	}
	if !e.FirstTimestamp.IsZero() {
		return e.FirstTimestamp.Time
	}
	if !e.CreationTimestamp.IsZero() {
		return e.CreationTimestamp.Time
	}
	return time.Time{}
}

func (r *Runner) collectNetwork(ctx context.Context, now time.Time) networkSummary {
	return networkSummary{
		Services: r.collectServices(ctx, now),
		Ingress:  r.collectIngresses(ctx, now),
	}
}

func (r *Runner) collectServices(ctx context.Context, now time.Time) serviceSummary {
	var sl corev1.ServiceList
	_ = r.Client.List(ctx, &sl)

	out := serviceSummary{Total: len(sl.Items)}
	for _, s := range sl.Items {
		switch s.Spec.Type {
		case corev1.ServiceTypeLoadBalancer:
			out.LBTotal++
			if len(s.Status.LoadBalancer.Ingress) == 0 && !s.CreationTimestamp.IsZero() && now.Sub(s.CreationTimestamp.Time) > 15*time.Minute {
				out.LBPending++
				out.RiskyTop = append(out.RiskyTop, serviceRiskItem{
					Namespace: s.Namespace,
					Name:      s.Name,
					Type:      s.Spec.Type,
					Age:       now.Sub(s.CreationTimestamp.Time),
					Reason:    "LoadBalancer 外网地址未分配",
				})
			}
		case corev1.ServiceTypeClusterIP:
			out.ClusterIPs++
		default:
		}
		if len(s.Spec.ExternalIPs) > 0 {
			out.ExternalIPs++
		}
	}
	sort.Slice(out.RiskyTop, func(i, j int) bool {
		if out.RiskyTop[i].Age != out.RiskyTop[j].Age {
			return out.RiskyTop[i].Age > out.RiskyTop[j].Age
		}
		if out.RiskyTop[i].Namespace != out.RiskyTop[j].Namespace {
			return out.RiskyTop[i].Namespace < out.RiskyTop[j].Namespace
		}
		return out.RiskyTop[i].Name < out.RiskyTop[j].Name
	})
	if len(out.RiskyTop) > 8 {
		out.RiskyTop = out.RiskyTop[:8]
	}
	return out
}

func (r *Runner) collectIngresses(ctx context.Context, now time.Time) ingressSummary {
	var il networkingv1.IngressList
	_ = r.Client.List(ctx, &il)

	out := ingressSummary{Total: len(il.Items)}
	for _, ing := range il.Items {
		className := ""
		if ing.Spec.IngressClassName != nil {
			className = *ing.Spec.IngressClassName
		}
		if className == "" {
			className = "-"
		}

		var hosts []string
		for _, r := range ing.Spec.Rules {
			if strings.TrimSpace(r.Host) != "" {
				hosts = append(hosts, r.Host)
			}
		}
		if len(hosts) > 3 {
			hosts = hosts[:3]
		}
		hostText := "-"
		if len(hosts) > 0 {
			hostText = strings.Join(hosts, ",")
		}

		if len(ing.Status.LoadBalancer.Ingress) == 0 && !ing.CreationTimestamp.IsZero() && now.Sub(ing.CreationTimestamp.Time) > 15*time.Minute {
			out.PendingLB++
			out.RiskyTop = append(out.RiskyTop, ingressRiskItem{
				Namespace: ing.Namespace,
				Name:      ing.Name,
				ClassName: className,
				Hosts:     hostText,
				Age:       now.Sub(ing.CreationTimestamp.Time),
				Reason:    "Ingress LoadBalancer 地址未就绪",
			})
		}
	}
	sort.Slice(out.RiskyTop, func(i, j int) bool {
		if out.RiskyTop[i].Age != out.RiskyTop[j].Age {
			return out.RiskyTop[i].Age > out.RiskyTop[j].Age
		}
		if out.RiskyTop[i].Namespace != out.RiskyTop[j].Namespace {
			return out.RiskyTop[i].Namespace < out.RiskyTop[j].Namespace
		}
		return out.RiskyTop[i].Name < out.RiskyTop[j].Name
	})
	if len(out.RiskyTop) > 8 {
		out.RiskyTop = out.RiskyTop[:8]
	}
	return out
}

func (r *Runner) collectNamespaceResources(ctx context.Context) nsResourceSummary {
	var pl corev1.PodList
	_ = r.Client.List(ctx, &pl)

	type agg struct {
		pods     int
		abnormal int
		cpu      resource.Quantity
		mem      resource.Quantity
	}
	m := make(map[string]*agg)

	for _, p := range pl.Items {
		a := m[p.Namespace]
		if a == nil {
			a = &agg{}
			m[p.Namespace] = a
		}
		a.pods++
		cpu, mem := podRequests(&p)
		a.cpu.Add(cpu)
		a.mem.Add(mem)
		if abnormalReason(&p) != "" {
			a.abnormal++
		}
	}

	var items []nsResourceItem
	for ns, a := range m {
		items = append(items, nsResourceItem{
			Namespace:   ns,
			Pods:        a.pods,
			Abnormal:    a.abnormal,
			CPURequests: a.cpu,
			MemRequests: a.mem,
		})
	}

	topCPU := append([]nsResourceItem(nil), items...)
	sort.Slice(topCPU, func(i, j int) bool {
		ci := topCPU[i].CPURequests.MilliValue()
		cj := topCPU[j].CPURequests.MilliValue()
		if ci != cj {
			return ci > cj
		}
		return topCPU[i].Namespace < topCPU[j].Namespace
	})
	if len(topCPU) > 8 {
		topCPU = topCPU[:8]
	}

	topMem := append([]nsResourceItem(nil), items...)
	sort.Slice(topMem, func(i, j int) bool {
		mi := topMem[i].MemRequests.Value()
		mj := topMem[j].MemRequests.Value()
		if mi != mj {
			return mi > mj
		}
		return topMem[i].Namespace < topMem[j].Namespace
	})
	if len(topMem) > 8 {
		topMem = topMem[:8]
	}

	return nsResourceSummary{TopCPU: topCPU, TopMem: topMem}
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

func scoreStorage(s storageSummary) float64 {
	if s.PVC.Total <= 0 {
		return 100
	}
	base := clamp100(float64(s.PVC.Bound) / float64(s.PVC.Total) * 100)
	penalty := 0.0
	penalty += float64(s.PVC.Lost) * 15
	penalty += float64(s.PVC.Pending) * 5
	penalty += float64(s.PV.Failed) * 20
	return clamp100(base - penalty)
}

func scoreWarningEvents(e warningEventSummary) float64 {
	if e.Total <= 0 {
		return 100
	}
	penalty := float64(e.Total) * 0.5
	if penalty > 60 {
		penalty = 60
	}
	return clamp100(100 - penalty)
}

func scoreNetwork(n networkSummary) float64 {
	penalty := 0.0
	penalty += float64(n.Services.LBPending) * 20
	penalty += float64(n.Ingress.PendingLB) * 20
	return clamp100(100 - penalty)
}

func scoreOverall(nodeStatus, nodeResource, podStatus, deploy, sts, storage, events, network float64) float64 {
	w := 0.0
	sum := 0.0
	for _, it := range []struct {
		V float64
		W float64
	}{
		{nodeStatus, 0.18},
		{nodeResource, 0.18},
		{podStatus, 0.18},
		{deploy, 0.18},
		{sts, 0.09},
		{storage, 0.09},
		{events, 0.05},
		{network, 0.05},
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

func buildFocusItems(nodes nodeSummary, abnormalPods []corev1.Pod, quotaRisks []quotaRisk, ws workloadsSummary, storage storageSummary, events warningEventSummary, network networkSummary) []string {
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

	if ws.CronJobOverdue > 0 || ws.CronJobNeverRun > 0 || ws.SuspendedCronJobs > 0 || ws.FailedJobs > 0 {
		var b strings.Builder
		b.WriteString("计划任务/批处理存在风险")
		if ws.CronJobOverdue > 0 {
			b.WriteString(fmt.Sprintf("\n- CronJob 疑似漏跑：%d", ws.CronJobOverdue))
		}
		if ws.CronJobNeverRun > 0 {
			b.WriteString(fmt.Sprintf("\n- CronJob 未观察到执行：%d", ws.CronJobNeverRun))
		}
		if ws.SuspendedCronJobs > 0 {
			b.WriteString(fmt.Sprintf("\n- CronJob 已暂停：%d", ws.SuspendedCronJobs))
		}
		if ws.FailedJobs > 0 {
			b.WriteString(fmt.Sprintf("\n- 失败 Job：%d", ws.FailedJobs))
		}
		for i := 0; i < len(ws.RiskyCronJobs) && i < 3; i++ {
			it := ws.RiskyCronJobs[i]
			b.WriteString(fmt.Sprintf("\n- CronJob：%s/%s（%s）", it.Namespace, it.Name, it.RiskReason))
		}
		for i := 0; i < len(ws.FailedJobDetails) && i < 3; i++ {
			it := ws.FailedJobDetails[i]
			reason := ""
			if len(it.FailureReasons) > 0 {
				reason = " / " + strings.Join(it.FailureReasons, "、")
			}
			b.WriteString(fmt.Sprintf("\n- Job：%s/%s（failed=%d%s）", it.Namespace, it.Name, it.Failed, reason))
		}
		out = append(out, b.String())
	}

	if storage.PVC.Lost > 0 || storage.PVC.Pending > 0 || (storage.HasPVAccess && storage.PV.Failed > 0) {
		var b strings.Builder
		b.WriteString("存储存在风险")
		if storage.PVC.Pending > 0 {
			b.WriteString(fmt.Sprintf("\n- PVC Pending：%d", storage.PVC.Pending))
		}
		if storage.PVC.Lost > 0 {
			b.WriteString(fmt.Sprintf("\n- PVC Lost：%d", storage.PVC.Lost))
		}
		if storage.HasPVAccess && storage.PV.Failed > 0 {
			b.WriteString(fmt.Sprintf("\n- PV Failed：%d", storage.PV.Failed))
		}
		out = append(out, b.String())
	}

	if events.Total > 0 {
		var b strings.Builder
		b.WriteString(fmt.Sprintf("近期告警事件（24h）较多：%d", events.Total))
		if len(events.TopReasons) > 0 {
			var parts []string
			for i := 0; i < len(events.TopReasons) && i < 3; i++ {
				parts = append(parts, fmt.Sprintf("%s %d", events.TopReasons[i].Reason, events.TopReasons[i].Count))
			}
			b.WriteString("\n- Top 原因：" + strings.Join(parts, "、"))
		}
		if len(events.Samples) > 0 {
			s := events.Samples[0]
			msg := ""
			if strings.TrimSpace(s.Message) != "" {
				msg = " / " + s.Message
			}
			b.WriteString(fmt.Sprintf("\n- 示例：%s %s/%s（%s）%s", s.Kind, s.Namespace, s.Name, s.Reason, msg))
		}
		out = append(out, b.String())
	}

	if network.Services.LBPending > 0 || network.Ingress.PendingLB > 0 {
		var b strings.Builder
		b.WriteString("网络入口/服务存在风险")
		if network.Services.LBPending > 0 {
			b.WriteString(fmt.Sprintf("\n- Service LoadBalancer 未就绪：%d", network.Services.LBPending))
		}
		if network.Ingress.PendingLB > 0 {
			b.WriteString(fmt.Sprintf("\n- Ingress LoadBalancer 未就绪：%d", network.Ingress.PendingLB))
		}
		out = append(out, b.String())
	}

	if len(out) > 5 {
		out = out[:5]
	}
	return out
}

func buildSuggestedActions(nodes nodeSummary, abnormalPods []corev1.Pod, quotaRisks []quotaRisk, ws workloadsSummary, storage storageSummary, events warningEventSummary, network networkSummary) []string {
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
	if ws.CronJobOverdue > 0 || ws.CronJobNeverRun > 0 || ws.SuspendedCronJobs > 0 {
		out = append(out, "检查：CronJob 是否被 Suspend、lastScheduleTime 是否长期不更新、是否存在长时间 Active Job；必要时查看 kube-controller-manager 日志与事件")
	}
	if ws.FailedJobs > 0 {
		out = append(out, "检查：失败 Job 的 BackoffLimit/DeadlineExceeded/镜像拉取/权限等原因；对照 Job 条件、Pod 日志与事件定位根因")
	}
	if storage.PVC.Pending > 0 || storage.PVC.Lost > 0 || (storage.HasPVAccess && storage.PV.Failed > 0) {
		out = append(out, "检查：PVC/PV 异常（Pending/Lost/Failed），确认 StorageClass、Provisioner、节点/存储后端连通性与事件；必要时查看 provisioner/controller 日志")
	}
	if events.Total > 0 {
		out = append(out, "检查：近期 Warning Event Top 原因与 Top 对象，按 namespace 聚焦；必要时增加事件采样与告警收敛")
	}
	if network.Services.LBPending > 0 || network.Ingress.PendingLB > 0 {
		out = append(out, "检查：LB 未就绪的 Service/Ingress，关注云厂商控制器/MetalLB/Ingress Controller 日志与事件，以及地址池/安全组/路由配置")
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

func buildReportMarkdown(now time.Time, comps componentsSummary, nodes nodeSummary, ws workloadsSummary, pc podCounts, abnormal abnormalPodsSummary, abnormalPods []corev1.Pod, quotaRisks []quotaRisk, storage storageSummary, events warningEventSummary, network networkSummary, nsRes nsResourceSummary, analyses []podAnalysis) string {
	var b strings.Builder
	tz := now.Location()

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
	storageScore := scoreStorage(storage)
	eventScore := scoreWarningEvents(events)
	networkScore := scoreNetwork(network)
	totalScore := scoreOverall(nodeStatusScore, nodeResourceScore, podStatusScore, deployScore, stsScore, storageScore, eventScore, networkScore)

	b.WriteString("## 📊 健康评分\n")
	b.WriteString(fmt.Sprintf("- 节点状态：%.1f%%\n", nodeStatusScore))
	b.WriteString(fmt.Sprintf("- 节点资源：%.1f%%\n", nodeResourceScore))
	b.WriteString(fmt.Sprintf("- Pod 状态：%.1f%%\n", podStatusScore))
	b.WriteString(fmt.Sprintf("- Deployments：%.1f%%\n", deployScore))
	b.WriteString(fmt.Sprintf("- StatefulSets：%.1f%%\n", stsScore))
	b.WriteString(fmt.Sprintf("- 存储：%.1f%%\n", storageScore))
	b.WriteString(fmt.Sprintf("- 事件告警：%.1f%%\n", eventScore))
	b.WriteString(fmt.Sprintf("- 网络入口：%.1f%%\n\n", networkScore))
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
	if storage.PVC.Pending > 0 || storage.PVC.Lost > 0 {
		pvcStatusText = fmt.Sprintf("⚠️ Bound %d / Pending %d / Lost %d", storage.PVC.Bound, storage.PVC.Pending, storage.PVC.Lost)
	}
	b.WriteString(fmt.Sprintf("- PVC：%d（%s）\n", storage.PVC.Total, pvcStatusText))
	if events.Total > 0 {
		b.WriteString(fmt.Sprintf("- 过去 24h Warning Event：%d\n", events.Total))
	} else {
		b.WriteString("- 过去 24h Warning Event：✅ 0\n")
	}
	var netParts []string
	netParts = append(netParts, fmt.Sprintf("Service %d（LB pending %d）", network.Services.Total, network.Services.LBPending))
	netParts = append(netParts, fmt.Sprintf("Ingress %d（LB pending %d）", network.Ingress.Total, network.Ingress.PendingLB))
	b.WriteString(fmt.Sprintf("- 网络：%s\n\n", strings.Join(netParts, " / ")))

	focus := buildFocusItems(nodes, abnormalPods, quotaRisks, ws, storage, events, network)
	if len(focus) > 0 {
		b.WriteString("## ⚠️ 重点关注\n")
		for i, it := range focus {
			b.WriteString(fmt.Sprintf("%d. %s\n", i+1, it))
		}
		b.WriteString("\n")

		actions := buildSuggestedActions(nodes, abnormalPods, quotaRisks, ws, storage, events, network)
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
	if len(ws.Unhealthy) > 0 || ws.FailedJobs > 0 || ws.SuspendedCronJobs > 0 || events.Total > 0 || network.Services.LBPending > 0 || network.Ingress.PendingLB > 0 || storage.PVC.Pending > 0 || storage.PVC.Lost > 0 {
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
		if ws.CronJobOverdue > 0 {
			parts = append(parts, fmt.Sprintf("疑似漏跑 CronJob %d", ws.CronJobOverdue))
		}
		if storage.PVC.Pending > 0 || storage.PVC.Lost > 0 {
			parts = append(parts, fmt.Sprintf("PVC 异常 Pending/Lost %d", storage.PVC.Pending+storage.PVC.Lost))
		}
		if events.Total > 0 {
			parts = append(parts, fmt.Sprintf("Warning Event %d", events.Total))
		}
		if network.Services.LBPending > 0 || network.Ingress.PendingLB > 0 {
			parts = append(parts, fmt.Sprintf("网络入口未就绪 %d", network.Services.LBPending+network.Ingress.PendingLB))
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
		if ws.CronJobOverdue > 0 {
			parts = append(parts, fmt.Sprintf("疑似漏跑 CronJob %d", ws.CronJobOverdue))
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
	if ws.Counts.CronJobs > 0 {
		b.WriteString("\n### 计划任务（CronJob）\n")
		var parts []string
		parts = append(parts, fmt.Sprintf("总数 %d", ws.Counts.CronJobs))
		if ws.ActiveCronJobs > 0 {
			parts = append(parts, fmt.Sprintf("Active %d", ws.ActiveCronJobs))
		}
		if ws.SuspendedCronJobs > 0 {
			parts = append(parts, fmt.Sprintf("Suspend %d", ws.SuspendedCronJobs))
		}
		if ws.CronJobNeverRun > 0 {
			parts = append(parts, fmt.Sprintf("未执行 %d", ws.CronJobNeverRun))
		}
		if ws.CronJobOverdue > 0 {
			parts = append(parts, fmt.Sprintf("疑似漏跑 %d", ws.CronJobOverdue))
		}
		if ws.UnsupportedCron > 0 {
			parts = append(parts, fmt.Sprintf("schedule 无法解析 %d", ws.UnsupportedCron))
		}
		b.WriteString(fmt.Sprintf("- 概览：%s\n", strings.Join(parts, " / ")))
		if len(ws.RiskyCronJobs) > 0 {
			b.WriteString("- 风险 CronJob（Top 10）：\n")
			for _, it := range ws.RiskyCronJobs {
				last := "-"
				if it.LastScheduleTime != nil {
					last = it.LastScheduleTime.In(tz).Format("2006-01-02 15:04")
				}
				next := "-"
				if it.NextScheduleTime != nil {
					next = it.NextScheduleTime.In(tz).Format("2006-01-02 15:04")
				}
				suspendText := "false"
				if it.Suspended {
					suspendText = "true"
				}
				sds := "-"
				if it.StartingDeadlineSeconds != nil {
					sds = fmt.Sprintf("%ds", *it.StartingDeadlineSeconds)
				}
				b.WriteString(fmt.Sprintf("  - %s/%s：schedule=%q / suspend=%s / active=%d / concurrency=%s / startingDeadline=%s / last=%s / next=%s / 风险=%s\n",
					it.Namespace, it.Name, it.Schedule, suspendText, it.Active, it.ConcurrencyPolicy, sds, last, next, it.RiskReason))
			}
		}
	}
	if ws.Counts.Jobs > 0 {
		b.WriteString("\n### 批处理（Job）\n")
		var parts []string
		parts = append(parts, fmt.Sprintf("总数 %d", ws.Counts.Jobs))
		if ws.ActiveJobs > 0 {
			parts = append(parts, fmt.Sprintf("Active %d", ws.ActiveJobs))
		}
		if ws.FailedJobs > 0 {
			parts = append(parts, fmt.Sprintf("失败 %d", ws.FailedJobs))
		}
		b.WriteString(fmt.Sprintf("- 概览：%s\n", strings.Join(parts, " / ")))
		if len(ws.FailedJobDetails) > 0 {
			b.WriteString("- 失败 Job（Top 10）：\n")
			for _, it := range ws.FailedJobDetails {
				start := "-"
				if it.StartTime != nil {
					start = it.StartTime.In(tz).Format("2006-01-02 15:04")
				}
				owner := ""
				if strings.TrimSpace(it.OwnerCronJobKey) != "" {
					owner = " / owner=" + it.OwnerCronJobKey
				}
				reason := "-"
				if len(it.FailureReasons) > 0 {
					reason = strings.Join(it.FailureReasons, "、")
				}
				b.WriteString(fmt.Sprintf("  - %s/%s：failed=%d / active=%d / succeeded=%d / start=%s / age=%s / reason=%s%s\n",
					it.Namespace, it.Name, it.Failed, it.Active, it.Succeeded, start, it.Age.Truncate(time.Minute).String(), reason, owner))
			}
		}
	}
	b.WriteString("\n")

	if events.Total > 0 {
		b.WriteString("## 🧯 近期告警事件（Warning）\n")
		b.WriteString(fmt.Sprintf("- 统计窗口：%s ~ %s\n", events.Since.In(tz).Format("2006-01-02 15:04"), now.Format("2006-01-02 15:04")))
		if len(events.TopReasons) > 0 {
			var parts []string
			for i := 0; i < len(events.TopReasons) && i < 5; i++ {
				parts = append(parts, fmt.Sprintf("%s %d", events.TopReasons[i].Reason, events.TopReasons[i].Count))
			}
			b.WriteString(fmt.Sprintf("- Top 原因：%s\n", strings.Join(parts, "、")))
		}
		if len(events.TopNamespace) > 0 {
			var parts []string
			for i := 0; i < len(events.TopNamespace) && i < 5; i++ {
				parts = append(parts, fmt.Sprintf("%s %d", events.TopNamespace[i].Namespace, events.TopNamespace[i].Total))
			}
			b.WriteString(fmt.Sprintf("- Top 命名空间：%s\n", strings.Join(parts, "、")))
		}
		if len(events.Samples) > 0 {
			b.WriteString("- 事件样例（Top 8）：\n")
			for _, it := range events.Samples {
				msg := ""
				if strings.TrimSpace(it.Message) != "" {
					msg = " / " + it.Message
				}
				b.WriteString(fmt.Sprintf("  - %s %s/%s：%s（count=%d / last=%s）%s\n", it.Kind, it.Namespace, it.Name, it.Reason, it.Count, it.LastTime.In(tz).Format("2006-01-02 15:04"), msg))
			}
		}
		b.WriteString("\n")
	}

	if network.Services.Total > 0 || network.Ingress.Total > 0 {
		b.WriteString("## 🌐 网络入口与服务\n")
		b.WriteString(fmt.Sprintf("- Service：%d（LoadBalancer %d / LB pending %d）\n", network.Services.Total, network.Services.LBTotal, network.Services.LBPending))
		b.WriteString(fmt.Sprintf("- Ingress：%d（LB pending %d）\n", network.Ingress.Total, network.Ingress.PendingLB))
		if len(network.Services.RiskyTop) > 0 {
			b.WriteString("- Service 风险（Top 8）：\n")
			for _, it := range network.Services.RiskyTop {
				b.WriteString(fmt.Sprintf("  - %s/%s：type=%s / age=%s / %s\n", it.Namespace, it.Name, it.Type, it.Age.Truncate(time.Minute).String(), it.Reason))
			}
		}
		if len(network.Ingress.RiskyTop) > 0 {
			b.WriteString("- Ingress 风险（Top 8）：\n")
			for _, it := range network.Ingress.RiskyTop {
				b.WriteString(fmt.Sprintf("  - %s/%s：class=%s / hosts=%s / age=%s / %s\n", it.Namespace, it.Name, it.ClassName, it.Hosts, it.Age.Truncate(time.Minute).String(), it.Reason))
			}
		}
		b.WriteString("\n")
	}

	if len(nsRes.TopCPU) > 0 || len(nsRes.TopMem) > 0 {
		b.WriteString("## 🗂️ 命名空间资源热区\n")
		if len(nsRes.TopCPU) > 0 {
			b.WriteString("- CPU Requests Top：\n")
			for _, it := range nsRes.TopCPU {
				b.WriteString(fmt.Sprintf("  - %s：pods=%d / abnormal=%d / cpu=%s\n", it.Namespace, it.Pods, it.Abnormal, formatCPU(it.CPURequests)))
			}
		}
		if len(nsRes.TopMem) > 0 {
			b.WriteString("- Mem Requests Top：\n")
			for _, it := range nsRes.TopMem {
				b.WriteString(fmt.Sprintf("  - %s：pods=%d / abnormal=%d / mem=%s\n", it.Namespace, it.Pods, it.Abnormal, formatMemoryGi(it.MemRequests)))
			}
		}
		b.WriteString("\n")
	}

	b.WriteString("## 💾 存储（详细）\n")
	if storage.StorageCLs.Total > 0 {
		defaultText := "0"
		if storage.StorageCLs.DefaultCount > 0 {
			defaultText = fmt.Sprintf("%d（%s）", storage.StorageCLs.DefaultCount, strings.Join(storage.StorageCLs.Defaults, ","))
		}
		b.WriteString(fmt.Sprintf("- StorageClass：%d（default %s）\n", storage.StorageCLs.Total, defaultText))
	}
	b.WriteString(fmt.Sprintf("- PVC：%d（Bound %d / Pending %d / Lost %d）\n", storage.PVC.Total, storage.PVC.Bound, storage.PVC.Pending, storage.PVC.Lost))
	if len(storage.RiskyPVCs) > 0 {
		b.WriteString("- PVC 风险（Top 10）：\n")
		for _, it := range storage.RiskyPVCs {
			b.WriteString(fmt.Sprintf("  - %s/%s：phase=%s / storage=%s / class=%s / volume=%s / age=%s\n", it.Namespace, it.Name, it.Phase, it.Storage, it.ClassName, it.Volume, it.Age.Truncate(time.Minute).String()))
		}
	}
	if storage.HasPVAccess {
		b.WriteString(fmt.Sprintf("- PV：%d（Bound %d / Available %d / Released %d / Failed %d）\n", storage.PV.Total, storage.PV.Bound, storage.PV.Available, storage.PV.Released, storage.PV.Failed))
		if len(storage.RiskyPVs) > 0 {
			b.WriteString("- PV 风险（Top 10）：\n")
			for _, it := range storage.RiskyPVs {
				reason := it.Reason
				if strings.TrimSpace(reason) == "" {
					reason = "-"
				}
				b.WriteString(fmt.Sprintf("  - %s：phase=%s / cap=%s / class=%s / claim=%s / age=%s / reason=%s\n", it.Name, it.Phase, it.Capacity, it.ClassName, it.Claim, it.Age.Truncate(time.Minute).String(), reason))
			}
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
		if len(ws.Unhealthy) > 0 || ws.FailedJobs > 0 || ws.CronJobOverdue > 0 || ws.SuspendedCronJobs > 0 || events.Total > 0 || network.Services.LBPending > 0 || network.Ingress.PendingLB > 0 || storage.PVC.Pending > 0 || storage.PVC.Lost > 0 || (storage.HasPVAccess && storage.PV.Failed > 0) {
			b.WriteString("- 存在工作负载/计划任务/事件/网络/存储风险，建议优先处理“重点关注”里的 Top 项，并结合事件与控制器日志定位。\n")
		}
	}

	return b.String()
}
