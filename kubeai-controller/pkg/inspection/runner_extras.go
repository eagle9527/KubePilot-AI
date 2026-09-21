package inspection

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sort"
	"strings"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
)

// inspectionExtras 汇总扩展巡检结果：安全配置、资源规范、证书、Endpoints、弹性伸缩、残留资源。
type inspectionExtras struct {
	Security  securitySummary
	Hygiene   hygieneSummary
	Certs     certSummary
	Endpoints endpointSummary
	HPA       hpaSummary
	PDB       pdbSummary
	Stale     staleSummary
}

func (ix inspectionExtras) hasProblems() bool {
	return ix.Security.Privileged > 0 || ix.Security.RunAsRoot > 0 ||
		ix.Certs.Expired > 0 || ix.Certs.Expiring7 > 0 || ix.Certs.Expiring30 > 0 || len(ix.Certs.MissingIngressSecrets) > 0 ||
		ix.Endpoints.Empty > 0 ||
		(ix.HPA.Available && len(ix.HPA.Risks) > 0) ||
		(ix.PDB.Available && ix.PDB.Blocked > 0) ||
		len(ix.Stale.TerminatingPods) > 0 || len(ix.Stale.TerminatingNamespaces) > 0 ||
		ix.Stale.OldJobs > 0 || len(ix.Stale.OrphanPVCs) > 0
}

type securityRiskItem struct {
	Namespace string
	Name      string
	Issues    []string
}

type securitySummary struct {
	Privileged      int
	HostNetwork     int
	HostPID         int
	HostIPC         int
	RunAsRoot       int
	HostPath        int
	LatestImages    int
	TopItems        []securityRiskItem
	TopLatestImages []string
}

type hygieneSummary struct {
	NoCPURequest   int
	NoMemRequest   int
	NoMemLimit     int
	TopNoResources []string
}

type certRiskItem struct {
	Namespace string
	Name      string
	NotAfter  time.Time
	DaysLeft  int
	Expired   bool
}

type certSummary struct {
	Available             bool
	TLSSecrets            int
	Expired               int
	Expiring7             int
	Expiring30            int
	Risks                 []certRiskItem
	MissingIngressSecrets []string
}

type endpointRiskItem struct {
	Namespace string
	Service   string
	Type      string
	Age       time.Duration
}

type endpointSummary struct {
	Available bool
	Checked   int
	Empty     int
	Risks     []endpointRiskItem
}

type hpaRiskItem struct {
	Namespace string
	Name      string
	Target    string
	Current   int32
	Min       int32
	Max       int32
	Reason    string
}

type hpaSummary struct {
	Available bool
	Total     int
	Risks     []hpaRiskItem
}

type pdbRiskItem struct {
	Namespace string
	Name      string
	Expected  int32
}

type pdbSummary struct {
	Available bool
	Total     int
	Blocked   int
	Risks     []pdbRiskItem
}

type staleItem struct {
	Namespace string
	Name      string
	Age       time.Duration
}

type staleSummary struct {
	TerminatingPods       []staleItem
	TerminatingNamespaces []string
	OldJobs               int
	OldJobTop             []staleItem
	OrphanPVCs            []staleItem
	HasNSAccess           bool
}

func (r *Runner) collectExtras(ctx context.Context, now time.Time) inspectionExtras {
	var out inspectionExtras
	out.Security, out.Hygiene = r.collectSecurityAndHygiene(ctx)
	out.Certs = r.collectCertRisks(ctx, now)
	out.Endpoints = r.collectEndpointRisks(ctx, now)
	out.HPA = r.collectHPARisks(ctx)
	out.PDB = r.collectPDBRisks(ctx)
	out.Stale = r.collectStaleResources(ctx, now)
	return out
}

// collectSecurityAndHygiene 扫描 Pod 的安全配置风险与资源声明缺失。
func (r *Runner) collectSecurityAndHygiene(ctx context.Context) (securitySummary, hygieneSummary) {
	var sec securitySummary
	var hyg hygieneSummary

	var pl corev1.PodList
	if err := r.Client.List(ctx, &pl); err != nil {
		return sec, hyg
	}

	imageSet := make(map[string]int)
	for _, p := range pl.Items {
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		var issues []string

		if p.Spec.HostNetwork {
			sec.HostNetwork++
			issues = append(issues, "hostNetwork")
		}
		if p.Spec.HostPID {
			sec.HostPID++
			issues = append(issues, "hostPID")
		}
		if p.Spec.HostIPC {
			sec.HostIPC++
			issues = append(issues, "hostIPC")
		}

		privileged := false
		root := false
		if p.Spec.SecurityContext != nil && p.Spec.SecurityContext.RunAsUser != nil && *p.Spec.SecurityContext.RunAsUser == 0 {
			root = true
		}
		containers := append(append([]corev1.Container{}, p.Spec.InitContainers...), p.Spec.Containers...)
		for _, c := range containers {
			if c.SecurityContext == nil {
				continue
			}
			if c.SecurityContext.Privileged != nil && *c.SecurityContext.Privileged {
				privileged = true
			}
			if c.SecurityContext.RunAsUser != nil && *c.SecurityContext.RunAsUser == 0 {
				root = true
			}
		}
		if privileged {
			sec.Privileged++
			issues = append(issues, "特权容器")
		}
		if root {
			sec.RunAsRoot++
			issues = append(issues, "root 用户")
		}

		for _, v := range p.Spec.Volumes {
			if v.HostPath != nil {
				sec.HostPath++
				issues = append(issues, "hostPath")
				break
			}
		}

		missingCPU := false
		missingMem := false
		missingMemLimit := false
		for _, c := range p.Spec.Containers {
			if isLatestTag(c.Image) {
				sec.LatestImages++
				imageSet[c.Image]++
			}
			if _, ok := c.Resources.Requests[corev1.ResourceCPU]; !ok {
				missingCPU = true
			}
			if _, ok := c.Resources.Requests[corev1.ResourceMemory]; !ok {
				missingMem = true
			}
			if _, ok := c.Resources.Limits[corev1.ResourceMemory]; !ok {
				missingMemLimit = true
			}
		}
		if missingCPU {
			hyg.NoCPURequest++
		}
		if missingMem {
			hyg.NoMemRequest++
		}
		if missingMemLimit {
			hyg.NoMemLimit++
		}
		if (missingCPU || missingMem || missingMemLimit) && len(hyg.TopNoResources) < 10 {
			var missing []string
			if missingCPU {
				missing = append(missing, "CPU 请求")
			}
			if missingMem {
				missing = append(missing, "内存请求")
			}
			if missingMemLimit {
				missing = append(missing, "内存限制")
			}
			hyg.TopNoResources = append(hyg.TopNoResources, fmt.Sprintf("%s/%s（缺 %s）", p.Namespace, p.Name, strings.Join(missing, "、")))
		}

		if len(issues) > 0 {
			sec.TopItems = append(sec.TopItems, securityRiskItem{Namespace: p.Namespace, Name: p.Name, Issues: issues})
		}
	}

	sort.Slice(sec.TopItems, func(i, j int) bool {
		if len(sec.TopItems[i].Issues) != len(sec.TopItems[j].Issues) {
			return len(sec.TopItems[i].Issues) > len(sec.TopItems[j].Issues)
		}
		if sec.TopItems[i].Namespace != sec.TopItems[j].Namespace {
			return sec.TopItems[i].Namespace < sec.TopItems[j].Namespace
		}
		return sec.TopItems[i].Name < sec.TopItems[j].Name
	})
	if len(sec.TopItems) > 10 {
		sec.TopItems = sec.TopItems[:10]
	}

	type imgCount struct {
		Image string
		Count int
	}
	var ics []imgCount
	for img, n := range imageSet {
		ics = append(ics, imgCount{Image: img, Count: n})
	}
	sort.Slice(ics, func(i, j int) bool {
		if ics[i].Count != ics[j].Count {
			return ics[i].Count > ics[j].Count
		}
		return ics[i].Image < ics[j].Image
	})
	if len(ics) > 8 {
		ics = ics[:8]
	}
	for _, it := range ics {
		sec.TopLatestImages = append(sec.TopLatestImages, fmt.Sprintf("%s（%d）", it.Image, it.Count))
	}

	return sec, hyg
}

// isLatestTag 判断镜像是否使用 latest 标签（含未写 tag 的隐式 latest），digest 镜像视为已固定。
func isLatestTag(image string) bool {
	image = strings.TrimSpace(image)
	if image == "" || strings.Contains(image, "@") {
		return false
	}
	name := image
	if idx := strings.LastIndex(image, "/"); idx >= 0 {
		name = image[idx+1:]
	}
	colon := strings.LastIndex(name, ":")
	if colon < 0 {
		return true
	}
	return strings.EqualFold(name[colon+1:], "latest")
}

// collectCertRisks 检查 TLS Secret 证书有效期与 Ingress TLS 引用的 Secret 是否存在。
func (r *Runner) collectCertRisks(ctx context.Context, now time.Time) certSummary {
	var out certSummary

	var sl corev1.SecretList
	if err := r.Client.List(ctx, &sl); err != nil {
		return out
	}
	out.Available = true

	secretKeys := make(map[string]bool, len(sl.Items))
	for _, s := range sl.Items {
		secretKeys[s.Namespace+"/"+s.Name] = true
		if s.Type != corev1.SecretTypeTLS {
			continue
		}
		out.TLSSecrets++
		data := s.Data["tls.crt"]
		if len(data) == 0 {
			continue
		}
		notAfter, ok := earliestNotAfter(data)
		if !ok {
			continue
		}
		item := certRiskItem{
			Namespace: s.Namespace,
			Name:      s.Name,
			NotAfter:  notAfter,
			DaysLeft:  int(notAfter.Sub(now).Hours() / 24),
		}
		if notAfter.Before(now) {
			item.Expired = true
			out.Expired++
			out.Risks = append(out.Risks, item)
		} else if item.DaysLeft <= 7 {
			out.Expiring7++
			out.Risks = append(out.Risks, item)
		} else if item.DaysLeft <= 30 {
			out.Expiring30++
			out.Risks = append(out.Risks, item)
		}
	}

	sort.Slice(out.Risks, func(i, j int) bool {
		if out.Risks[i].Expired != out.Risks[j].Expired {
			return out.Risks[i].Expired
		}
		if out.Risks[i].DaysLeft != out.Risks[j].DaysLeft {
			return out.Risks[i].DaysLeft < out.Risks[j].DaysLeft
		}
		if out.Risks[i].Namespace != out.Risks[j].Namespace {
			return out.Risks[i].Namespace < out.Risks[j].Namespace
		}
		return out.Risks[i].Name < out.Risks[j].Name
	})
	if len(out.Risks) > 10 {
		out.Risks = out.Risks[:10]
	}

	var il networkingv1.IngressList
	if err := r.Client.List(ctx, &il); err == nil {
		seen := make(map[string]bool)
		for _, ing := range il.Items {
			for _, t := range ing.Spec.TLS {
				name := strings.TrimSpace(t.SecretName)
				if name == "" {
					continue
				}
				key := ing.Namespace + "/" + ing.Name + " -> " + name
				if secretKeys[ing.Namespace+"/"+name] || seen[key] {
					continue
				}
				seen[key] = true
				out.MissingIngressSecrets = append(out.MissingIngressSecrets, key)
			}
		}
		sort.Strings(out.MissingIngressSecrets)
		if len(out.MissingIngressSecrets) > 8 {
			out.MissingIngressSecrets = out.MissingIngressSecrets[:8]
		}
	}

	return out
}

// earliestNotAfter 解析 PEM 证书链，返回最早的过期时间。
func earliestNotAfter(pemData []byte) (time.Time, bool) {
	var earliest time.Time
	found := false
	rest := pemData
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		if !found || cert.NotAfter.Before(earliest) {
			earliest = cert.NotAfter
			found = true
		}
	}
	return earliest, found
}

// collectEndpointRisks 检查带 selector 的 Service 是否存在空的就绪 Endpoints。
func (r *Runner) collectEndpointRisks(ctx context.Context, now time.Time) endpointSummary {
	var out endpointSummary

	var esl discoveryv1.EndpointSliceList
	if err := r.Client.List(ctx, &esl); err != nil {
		return out
	}
	out.Available = true

	ready := make(map[string]bool)
	for _, es := range esl.Items {
		svc := es.Labels["kubernetes.io/service-name"]
		if svc == "" {
			continue
		}
		key := es.Namespace + "/" + svc
		for _, ep := range es.Endpoints {
			if ep.Conditions.Ready == nil || *ep.Conditions.Ready {
				ready[key] = true
				break
			}
		}
	}

	var sl corev1.ServiceList
	if err := r.Client.List(ctx, &sl); err != nil {
		return out
	}
	for _, s := range sl.Items {
		if s.Spec.Type == corev1.ServiceTypeExternalName || len(s.Spec.Selector) == 0 {
			continue
		}
		if s.CreationTimestamp.IsZero() || now.Sub(s.CreationTimestamp.Time) <= 15*time.Minute {
			continue
		}
		out.Checked++
		if ready[s.Namespace+"/"+s.Name] {
			continue
		}
		out.Empty++
		out.Risks = append(out.Risks, endpointRiskItem{
			Namespace: s.Namespace,
			Service:   s.Name,
			Type:      string(s.Spec.Type),
			Age:       now.Sub(s.CreationTimestamp.Time),
		})
	}
	sort.Slice(out.Risks, func(i, j int) bool {
		if out.Risks[i].Age != out.Risks[j].Age {
			return out.Risks[i].Age > out.Risks[j].Age
		}
		if out.Risks[i].Namespace != out.Risks[j].Namespace {
			return out.Risks[i].Namespace < out.Risks[j].Namespace
		}
		return out.Risks[i].Service < out.Risks[j].Service
	})
	if len(out.Risks) > 10 {
		out.Risks = out.Risks[:10]
	}
	return out
}

// collectHPARisks 检查 HPA 指标可用性与副本数边界。
func (r *Runner) collectHPARisks(ctx context.Context) hpaSummary {
	var out hpaSummary

	var hl autoscalingv2.HorizontalPodAutoscalerList
	if err := r.Client.List(ctx, &hl); err != nil {
		return out
	}
	out.Available = true
	out.Total = len(hl.Items)

	for _, h := range hl.Items {
		var reasons []string
		for _, c := range h.Status.Conditions {
			if c.Type == autoscalingv2.ScalingActive && c.Status == corev1.ConditionFalse {
				reason := strings.TrimSpace(c.Reason)
				if reason == "" {
					reason = "ScalingActive=False"
				}
				reasons = append(reasons, reason)
			}
		}
		min := int32(1)
		if h.Spec.MinReplicas != nil {
			min = *h.Spec.MinReplicas
		}
		if h.Status.CurrentReplicas >= h.Spec.MaxReplicas {
			reasons = append(reasons, "已达最大副本数")
		} else if h.Status.CurrentReplicas < min {
			reasons = append(reasons, "低于最小副本数")
		}
		if len(reasons) == 0 {
			continue
		}
		out.Risks = append(out.Risks, hpaRiskItem{
			Namespace: h.Namespace,
			Name:      h.Name,
			Target:    h.Spec.ScaleTargetRef.Kind + "/" + h.Spec.ScaleTargetRef.Name,
			Current:   h.Status.CurrentReplicas,
			Min:       min,
			Max:       h.Spec.MaxReplicas,
			Reason:    strings.Join(reasons, "、"),
		})
	}
	sort.Slice(out.Risks, func(i, j int) bool {
		if out.Risks[i].Namespace != out.Risks[j].Namespace {
			return out.Risks[i].Namespace < out.Risks[j].Namespace
		}
		return out.Risks[i].Name < out.Risks[j].Name
	})
	if len(out.Risks) > 10 {
		out.Risks = out.Risks[:10]
	}
	return out
}

// collectPDBRisks 检查 DisruptionsAllowed=0 的 PDB（变更/驱逐可能被阻塞）。
func (r *Runner) collectPDBRisks(ctx context.Context) pdbSummary {
	var out pdbSummary

	var pl policyv1.PodDisruptionBudgetList
	if err := r.Client.List(ctx, &pl); err != nil {
		return out
	}
	out.Available = true
	out.Total = len(pl.Items)

	for _, p := range pl.Items {
		if p.Status.DisruptionsAllowed > 0 || p.Status.ExpectedPods <= 0 {
			continue
		}
		out.Blocked++
		out.Risks = append(out.Risks, pdbRiskItem{
			Namespace: p.Namespace,
			Name:      p.Name,
			Expected:  p.Status.ExpectedPods,
		})
	}
	sort.Slice(out.Risks, func(i, j int) bool {
		if out.Risks[i].Namespace != out.Risks[j].Namespace {
			return out.Risks[i].Namespace < out.Risks[j].Namespace
		}
		return out.Risks[i].Name < out.Risks[j].Name
	})
	if len(out.Risks) > 10 {
		out.Risks = out.Risks[:10]
	}
	return out
}

// collectStaleResources 检查残留资源：卡住的 Terminating Pod/Namespace、超期未清理 Job、闲置 PVC。
func (r *Runner) collectStaleResources(ctx context.Context, now time.Time) staleSummary {
	var out staleSummary

	var pl corev1.PodList
	if err := r.Client.List(ctx, &pl); err == nil {
		usedPVC := make(map[string]bool)
		for _, p := range pl.Items {
			for _, v := range p.Spec.Volumes {
				if v.PersistentVolumeClaim != nil && strings.TrimSpace(v.PersistentVolumeClaim.ClaimName) != "" {
					usedPVC[p.Namespace+"/"+v.PersistentVolumeClaim.ClaimName] = true
				}
			}
			if p.DeletionTimestamp == nil {
				continue
			}
			age := now.Sub(p.DeletionTimestamp.Time)
			if age > 15*time.Minute {
				out.TerminatingPods = append(out.TerminatingPods, staleItem{Namespace: p.Namespace, Name: p.Name, Age: age})
			}
		}
		sort.Slice(out.TerminatingPods, func(i, j int) bool { return out.TerminatingPods[i].Age > out.TerminatingPods[j].Age })
		if len(out.TerminatingPods) > 10 {
			out.TerminatingPods = out.TerminatingPods[:10]
		}

		var pcl corev1.PersistentVolumeClaimList
		if err := r.Client.List(ctx, &pcl); err == nil {
			for _, pvc := range pcl.Items {
				if pvc.Status.Phase != corev1.ClaimBound {
					continue
				}
				if pvc.CreationTimestamp.IsZero() || now.Sub(pvc.CreationTimestamp.Time) < 24*time.Hour {
					continue
				}
				if usedPVC[pvc.Namespace+"/"+pvc.Name] {
					continue
				}
				out.OrphanPVCs = append(out.OrphanPVCs, staleItem{
					Namespace: pvc.Namespace,
					Name:      pvc.Name,
					Age:       now.Sub(pvc.CreationTimestamp.Time),
				})
			}
			sort.Slice(out.OrphanPVCs, func(i, j int) bool { return out.OrphanPVCs[i].Age > out.OrphanPVCs[j].Age })
			if len(out.OrphanPVCs) > 10 {
				out.OrphanPVCs = out.OrphanPVCs[:10]
			}
		}
	}

	var jl batchv1.JobList
	if err := r.Client.List(ctx, &jl); err == nil {
		for _, j := range jl.Items {
			if j.Spec.TTLSecondsAfterFinished != nil || j.Status.CompletionTime == nil {
				continue
			}
			age := now.Sub(j.Status.CompletionTime.Time)
			if age <= 7*24*time.Hour {
				continue
			}
			out.OldJobs++
			if len(out.OldJobTop) < 10 {
				out.OldJobTop = append(out.OldJobTop, staleItem{Namespace: j.Namespace, Name: j.Name, Age: age})
			}
		}
		sort.Slice(out.OldJobTop, func(i, j int) bool { return out.OldJobTop[i].Age > out.OldJobTop[j].Age })
	}

	var nl corev1.NamespaceList
	if err := r.Client.List(ctx, &nl); err == nil {
		out.HasNSAccess = true
		for _, ns := range nl.Items {
			if ns.Status.Phase == corev1.NamespaceTerminating {
				out.TerminatingNamespaces = append(out.TerminatingNamespaces, ns.Name)
			}
		}
		sort.Strings(out.TerminatingNamespaces)
		if len(out.TerminatingNamespaces) > 5 {
			out.TerminatingNamespaces = out.TerminatingNamespaces[:5]
		}
	}

	return out
}

// ---------- 扩展巡检评分 ----------

func scoreSecurity(sec securitySummary, hyg hygieneSummary) float64 {
	penalty := 0.0
	penalty += float64(sec.Privileged) * 8
	penalty += float64(sec.HostNetwork+sec.HostPID+sec.HostIPC) * 4
	penalty += float64(sec.RunAsRoot) * 2
	penalty += float64(sec.HostPath) * 2
	penalty += float64(sec.LatestImages) * 0.5
	penalty += float64(hyg.NoMemLimit) * 0.2
	return clamp100(100 - penalty)
}

func scoreCerts(c certSummary) float64 {
	if !c.Available || c.TLSSecrets == 0 {
		return 100
	}
	penalty := 0.0
	penalty += float64(c.Expired) * 40
	penalty += float64(c.Expiring7) * 25
	penalty += float64(c.Expiring30) * 8
	penalty += float64(len(c.MissingIngressSecrets)) * 5
	return clamp100(100 - penalty)
}

// ---------- 扩展巡检报告渲染 ----------

func securityStatusText(sec securitySummary) string {
	if sec.Privileged == 0 && sec.RunAsRoot == 0 && sec.HostNetwork == 0 && sec.HostPID == 0 && sec.HostIPC == 0 && sec.HostPath == 0 && sec.LatestImages == 0 {
		return "✅ 无明显风险"
	}
	var parts []string
	if sec.Privileged > 0 {
		parts = append(parts, fmt.Sprintf("特权 %d", sec.Privileged))
	}
	if sec.RunAsRoot > 0 {
		parts = append(parts, fmt.Sprintf("root %d", sec.RunAsRoot))
	}
	if host := sec.HostNetwork + sec.HostPID + sec.HostIPC; host > 0 {
		parts = append(parts, fmt.Sprintf("host 命名空间 %d", host))
	}
	if sec.HostPath > 0 {
		parts = append(parts, fmt.Sprintf("hostPath %d", sec.HostPath))
	}
	if sec.LatestImages > 0 {
		parts = append(parts, fmt.Sprintf("latest 镜像 %d", sec.LatestImages))
	}
	return "⚠️ " + strings.Join(parts, "、")
}

func certStatusText(c certSummary) string {
	if !c.Available {
		return "- 未获取 Secret 权限，跳过"
	}
	if c.Expired > 0 || c.Expiring7 > 0 || c.Expiring30 > 0 {
		return fmt.Sprintf("⚠️ 已过期 %d / 7 天内到期 %d / 30 天内到期 %d", c.Expired, c.Expiring7, c.Expiring30)
	}
	return fmt.Sprintf("✅ %d 个均在有效期", c.TLSSecrets)
}

func writeExtrasSections(b *strings.Builder, extras inspectionExtras, nodes nodeSummary) {
	writeSecuritySection(b, extras.Security, extras.Hygiene)
	writeCertSection(b, extras.Certs)
	writeEndpointSection(b, extras.Endpoints)
	writeScalingSection(b, extras.HPA, extras.PDB)
	writeStaleSection(b, extras.Stale)
	writeNodeExtraSection(b, nodes)
}

func writeSecuritySection(b *strings.Builder, sec securitySummary, hyg hygieneSummary) {
	b.WriteString("## 🛡️ 安全与配置巡检\n")
	empty := sec.Privileged == 0 && sec.RunAsRoot == 0 && sec.HostNetwork == 0 && sec.HostPID == 0 &&
		sec.HostIPC == 0 && sec.HostPath == 0 && sec.LatestImages == 0 &&
		hyg.NoCPURequest == 0 && hyg.NoMemRequest == 0 && hyg.NoMemLimit == 0
	if empty {
		b.WriteString("- ✅ 未发现特权/root/host 命名空间/hostPath/latest 镜像等高风险配置，资源声明完整\n\n")
		return
	}
	b.WriteString(fmt.Sprintf("- 特权容器 Pod：%d\n", sec.Privileged))
	b.WriteString(fmt.Sprintf("- hostNetwork / hostPID / hostIPC：%d / %d / %d\n", sec.HostNetwork, sec.HostPID, sec.HostIPC))
	b.WriteString(fmt.Sprintf("- root 用户运行 Pod：%d\n", sec.RunAsRoot))
	b.WriteString(fmt.Sprintf("- hostPath 挂载 Pod：%d\n", sec.HostPath))
	b.WriteString(fmt.Sprintf("- latest 标签镜像：%d 个容器\n", sec.LatestImages))
	if len(sec.TopLatestImages) > 0 {
		b.WriteString(fmt.Sprintf("  - Top：%s\n", strings.Join(sec.TopLatestImages, "、")))
	}
	b.WriteString(fmt.Sprintf("- 资源声明缺失 Pod：无 CPU 请求 %d / 无内存请求 %d / 无内存限制 %d\n", hyg.NoCPURequest, hyg.NoMemRequest, hyg.NoMemLimit))
	if len(sec.TopItems) > 0 {
		b.WriteString("- 安全风险 Pod（Top 10）：\n")
		for _, it := range sec.TopItems {
			b.WriteString(fmt.Sprintf("  - %s/%s：%s\n", it.Namespace, it.Name, strings.Join(it.Issues, "、")))
		}
	}
	if len(hyg.TopNoResources) > 0 {
		b.WriteString("- 缺资源声明（Top 10）：\n")
		for _, it := range hyg.TopNoResources {
			b.WriteString(fmt.Sprintf("  - %s\n", it))
		}
	}
	b.WriteString("\n")
}

func writeCertSection(b *strings.Builder, c certSummary) {
	b.WriteString("## 📜 证书巡检（TLS Secret）\n")
	if !c.Available {
		b.WriteString("- 未获取 Secret 权限，跳过证书巡检\n\n")
		return
	}
	b.WriteString(fmt.Sprintf("- TLS Secret：%d（已过期 %d / 7 天内到期 %d / 30 天内到期 %d）\n", c.TLSSecrets, c.Expired, c.Expiring7, c.Expiring30))
	if len(c.Risks) > 0 {
		b.WriteString("- 风险证书（Top 10）：\n")
		for _, it := range c.Risks {
			if it.Expired {
				b.WriteString(fmt.Sprintf("  - %s/%s：已过期（NotAfter %s）\n", it.Namespace, it.Name, it.NotAfter.Format("2006-01-02")))
			} else {
				b.WriteString(fmt.Sprintf("  - %s/%s：剩余 %d 天（NotAfter %s）\n", it.Namespace, it.Name, it.DaysLeft, it.NotAfter.Format("2006-01-02")))
			}
		}
	}
	if len(c.MissingIngressSecrets) > 0 {
		b.WriteString("- Ingress 引用的 TLS Secret 不存在：\n")
		for _, it := range c.MissingIngressSecrets {
			b.WriteString(fmt.Sprintf("  - %s\n", it))
		}
	}
	b.WriteString("\n")
}

func writeEndpointSection(b *strings.Builder, ep endpointSummary) {
	b.WriteString("## 🔌 Service Endpoints 健康\n")
	if !ep.Available {
		b.WriteString("- 未获取 EndpointSlice 权限，跳过 Endpoints 巡检\n\n")
		return
	}
	b.WriteString(fmt.Sprintf("- 检查带 selector 的 Service：%d，无就绪 Endpoints：%d\n", ep.Checked, ep.Empty))
	if len(ep.Risks) > 0 {
		b.WriteString("- 空 Endpoints（Top 10）：\n")
		for _, it := range ep.Risks {
			b.WriteString(fmt.Sprintf("  - %s/%s：type=%s / age=%s\n", it.Namespace, it.Service, it.Type, it.Age.Truncate(time.Minute).String()))
		}
	}
	b.WriteString("\n")
}

func writeScalingSection(b *strings.Builder, hpa hpaSummary, pdb pdbSummary) {
	b.WriteString("## 📈 弹性伸缩与可用性（HPA/PDB）\n")
	wrote := false
	if hpa.Available {
		b.WriteString(fmt.Sprintf("- HPA：%d（风险 %d）\n", hpa.Total, len(hpa.Risks)))
		for _, it := range hpa.Risks {
			b.WriteString(fmt.Sprintf("  - %s/%s -> %s：current=%d min=%d max=%d / %s\n", it.Namespace, it.Name, it.Target, it.Current, it.Min, it.Max, it.Reason))
		}
		wrote = true
	}
	if pdb.Available {
		b.WriteString(fmt.Sprintf("- PDB：%d（DisruptionsAllowed=0：%d）\n", pdb.Total, pdb.Blocked))
		for _, it := range pdb.Risks {
			b.WriteString(fmt.Sprintf("  - %s/%s：expected=%d\n", it.Namespace, it.Name, it.Expected))
		}
		wrote = true
	}
	if !wrote {
		b.WriteString("- 未获取 HPA/PDB 权限或集群无相关资源\n")
	}
	b.WriteString("\n")
}

func writeStaleSection(b *strings.Builder, st staleSummary) {
	b.WriteString("## 🧹 残留与闲置资源\n")
	empty := len(st.TerminatingPods) == 0 && len(st.TerminatingNamespaces) == 0 && st.OldJobs == 0 && len(st.OrphanPVCs) == 0
	if empty {
		b.WriteString("- ✅ 未发现残留/闲置资源\n\n")
		return
	}
	if len(st.TerminatingPods) > 0 {
		b.WriteString(fmt.Sprintf("- Terminating 卡住 Pod（>15min）：%d\n", len(st.TerminatingPods)))
		for _, it := range st.TerminatingPods {
			b.WriteString(fmt.Sprintf("  - %s/%s（age=%s）\n", it.Namespace, it.Name, it.Age.Truncate(time.Minute).String()))
		}
	}
	if len(st.TerminatingNamespaces) > 0 {
		b.WriteString(fmt.Sprintf("- Terminating 卡住 Namespace：%s\n", strings.Join(st.TerminatingNamespaces, "、")))
	}
	if st.OldJobs > 0 {
		b.WriteString(fmt.Sprintf("- 已完成 Job 超 7 天未清理：%d（建议设置 ttlSecondsAfterFinished）\n", st.OldJobs))
		for _, it := range st.OldJobTop {
			b.WriteString(fmt.Sprintf("  - %s/%s（age=%s）\n", it.Namespace, it.Name, truncateAge(it.Age)))
		}
	}
	if len(st.OrphanPVCs) > 0 {
		b.WriteString(fmt.Sprintf("- 疑似闲置 PVC（Bound 且 24h 无 Pod 引用）：%d\n", len(st.OrphanPVCs)))
		for _, it := range st.OrphanPVCs {
			b.WriteString(fmt.Sprintf("  - %s/%s（age=%s）\n", it.Namespace, it.Name, truncateAge(it.Age)))
		}
	}
	b.WriteString("\n")
}

func writeNodeExtraSection(b *strings.Builder, nodes nodeSummary) {
	if len(nodes.Unschedulable) == 0 && len(nodes.PodOverload) == 0 {
		return
	}
	b.WriteString("## 🖥️ 节点补充检查\n")
	if len(nodes.Unschedulable) > 0 {
		b.WriteString(fmt.Sprintf("- 已封锁（Unschedulable）节点：%d（%s）\n", len(nodes.Unschedulable), strings.Join(nodes.Unschedulable, "、")))
	}
	if len(nodes.PodOverload) > 0 {
		b.WriteString("- Pod 数接近上限（≥90%）节点：\n")
		for _, it := range nodes.PodOverload {
			b.WriteString(fmt.Sprintf("  - %s：pods=%d/%d（%.0f%%）\n", it.Name, it.Pods, it.Capacity, it.Percent))
		}
	}
	b.WriteString("\n")
}

// truncateAge 将超过 24h 的时长格式化为天。
func truncateAge(d time.Duration) string {
	if d < 24*time.Hour {
		return d.Truncate(time.Minute).String()
	}
	return fmt.Sprintf("%.1fd", d.Hours()/24)
}

// ---------- 扩展巡检的重点关注与建议操作 ----------

func buildExtrasFocusItems(extras inspectionExtras) []string {
	var out []string

	if extras.Certs.Expired > 0 || extras.Certs.Expiring7 > 0 {
		severity := "关注"
		if extras.Certs.Expired > 0 {
			severity = "紧急"
		}
		var b strings.Builder
		b.WriteString(fmt.Sprintf("TLS 证书到期风险（%s）", severity))
		for i := 0; i < len(extras.Certs.Risks) && i < 3; i++ {
			it := extras.Certs.Risks[i]
			if it.Expired {
				b.WriteString(fmt.Sprintf("\n- %s/%s：已过期（%s）", it.Namespace, it.Name, it.NotAfter.Format("2006-01-02")))
			} else {
				b.WriteString(fmt.Sprintf("\n- %s/%s：剩余 %d 天", it.Namespace, it.Name, it.DaysLeft))
			}
		}
		out = append(out, b.String())
	} else if extras.Certs.Expiring30 > 0 {
		out = append(out, fmt.Sprintf("TLS 证书 30 天内到期：%d 个，建议安排续期", extras.Certs.Expiring30))
	}

	if extras.Endpoints.Empty > 0 {
		var b strings.Builder
		b.WriteString(fmt.Sprintf("Service 无就绪 Endpoints（%d 个），流量无法到达", extras.Endpoints.Empty))
		for i := 0; i < len(extras.Endpoints.Risks) && i < 3; i++ {
			it := extras.Endpoints.Risks[i]
			b.WriteString(fmt.Sprintf("\n- %s/%s", it.Namespace, it.Service))
		}
		out = append(out, b.String())
	}

	if extras.Security.Privileged > 0 {
		out = append(out, fmt.Sprintf("存在特权容器 Pod（%d 个），容器逃逸风险高，建议评估必要性并收敛", extras.Security.Privileged))
	}

	if len(extras.Stale.TerminatingNamespaces) > 0 {
		out = append(out, fmt.Sprintf("命名空间卡在 Terminating：%s，可能存在 finalizer 未清理", strings.Join(extras.Stale.TerminatingNamespaces, "、")))
	}

	if len(extras.Stale.TerminatingPods) > 0 {
		out = append(out, fmt.Sprintf("Terminating 卡住 Pod：%d 个（>15min），可能存在 finalizer 或卷卸载阻塞", len(extras.Stale.TerminatingPods)))
	}

	if extras.HPA.Available && len(extras.HPA.Risks) > 0 {
		var b strings.Builder
		b.WriteString(fmt.Sprintf("HPA 异常（%d 个）", len(extras.HPA.Risks)))
		for i := 0; i < len(extras.HPA.Risks) && i < 3; i++ {
			it := extras.HPA.Risks[i]
			b.WriteString(fmt.Sprintf("\n- %s/%s -> %s：%s", it.Namespace, it.Name, it.Target, it.Reason))
		}
		out = append(out, b.String())
	}

	if len(extras.Stale.OrphanPVCs) > 0 {
		out = append(out, fmt.Sprintf("疑似闲置 PVC %d 个（Bound 但无 Pod 引用），建议确认后回收以节省成本", len(extras.Stale.OrphanPVCs)))
	}

	return out
}

func buildExtrasActions(extras inspectionExtras) []string {
	var out []string
	if extras.Certs.Expired > 0 {
		out = append(out, "紧急：续期/轮换已过期 TLS 证书，避免 HTTPS 入口与 Webhook 中断")
	} else if extras.Certs.Expiring7 > 0 {
		out = append(out, "紧急：7 天内到期的 TLS 证书尽快续期（cert-manager 自动轮换或人工处理）")
	}
	if extras.Endpoints.Empty > 0 {
		out = append(out, "检查：空 Endpoints 的 Service，核对 selector 与 Pod label 是否匹配、就绪探针是否通过")
	}
	if extras.Security.Privileged > 0 || extras.Security.RunAsRoot > 0 {
		out = append(out, "安全：收敛特权/root 容器，启用 PodSecurity Admission 或准入策略限制高风险配置")
	}
	if len(extras.Stale.TerminatingNamespaces) > 0 || len(extras.Stale.TerminatingPods) > 0 {
		out = append(out, "检查：Terminating 卡住的命名空间/Pod，排查 finalizer、卷卸载与依赖 API 可用性问题")
	}
	if extras.Stale.OldJobs > 0 {
		out = append(out, "清理：为 Job/CronJob 设置 ttlSecondsAfterFinished，自动回收历史完成的 Job")
	}
	return out
}
