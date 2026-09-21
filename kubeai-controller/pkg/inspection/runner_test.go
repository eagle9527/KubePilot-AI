package inspection

import (
	"strings"
	"testing"
	"time"
)

func TestBuildReportMarkdown_NoTablesAndStructured(t *testing.T) {
	now := time.Date(2026, 8, 4, 2, 0, 5, 0, time.FixedZone("CST", 8*3600))

	comps := componentsSummary{
		Items: []componentItem{
			{Name: "coredns", Ready: 2, Total: 2},
			{Name: "kube-proxy", Ready: 2, Total: 3, Unhealthy: 1},
		},
		UnhealthyCount: 1,
	}
	nodes := nodeSummary{
		Total:         2,
		ReadyCount:    1,
		NotReadyCount: 1,
		PressureCount: 1,
		PerNodeUtil: []nodeUtil{
			{Name: "node-1", CPUPercent: 32.5, MemPercent: 61.2, Ready: true, Source: "requests"},
			{Name: "node-2", CPUPercent: 88.1, MemPercent: 91.3, Ready: false, Pressures: []string{"MemoryPressure"}, Source: "metrics"},
		},
	}
	ws := workloadsSummary{
		Counts: workloadCounts{Deployments: 24, StatefulSets: 5, DaemonSets: 3, Jobs: 2, CronJobs: 4},
	}
	pc := podCounts{Total: 128, Running: 120, Pending: 3, Failed: 2, Succeeded: 2, Unknown: 1}
	abnormal := abnormalPodsSummary{
		AbnormalCount: 5,
		TopReasons:    []reasonCount{{Reason: "CrashLoopBackOff", Count: 3}, {Reason: "ImagePullBackOff", Count: 2}},
	}
	analyses := []podAnalysis{{
		Namespace:  "default",
		Name:       "nginx-7d5b8c",
		Reason:     "CrashLoopBackOff",
		Confidence: 0.9,
		Suggestion: []string{"查看崩溃前日志：kubectl logs nginx-7d5b8c --previous", "检查配置挂载是否正确"},
	}}

	storage := storageSummary{PVC: pvcSummary{Total: 28, Bound: 28}}
	out := buildReportMarkdown(now, comps, nodes, ws, pc, abnormal, nil, nil, storage, warningEventSummary{}, networkSummary{}, nsResourceSummary{}, analyses, inspectionExtras{})

	if strings.Contains(out, "|") {
		t.Fatalf("report must not contain markdown tables:\n%s", out)
	}
	for _, want := range []string{
		"# 🔴 Kubernetes 每日巡检报告 · 2026-08-04",
		"## 📊 健康评分",
		"## 📋 集群概况",
		"## ⚠️ 重点关注",
		"## 💡 建议操作",
		"## 🔍 详细信息",
		"- node-2：CPU 88.1% / Mem 91.3% / NotReady / 压力：MemoryPressure（口径：metrics）",
		"- node-1：CPU 32.5% / Mem 61.2% / Ready",
		"**1. default/nginx-7d5b8c**",
		"（Top 原因：CrashLoopBackOff 3、ImagePullBackOff 2）",
		"## ✅ 总结与建议",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("report missing %q:\n%s", want, out)
		}
	}

	t.Logf("rendered report:\n%s", out)
}

func TestBuildReportMarkdown_BatchDetails(t *testing.T) {
	now := time.Date(2026, 8, 4, 9, 0, 0, 0, time.FixedZone("CST", 8*3600))

	ws := workloadsSummary{
		Counts:            workloadCounts{Deployments: 1, StatefulSets: 1, DaemonSets: 1, Jobs: 3, CronJobs: 2},
		FailedJobs:        1,
		SuspendedCronJobs: 1,
		CronJobNeverRun:   1,
		CronJobOverdue:    1,
		ActiveJobs:        1,
		ActiveCronJobs:    1,
		RiskyCronJobs: []cronJobRiskItem{{
			Namespace:         "ops",
			Name:              "daily-backup",
			Schedule:          "0 2 * * *",
			Suspended:         true,
			Active:            0,
			RiskReason:        "已暂停",
			ConcurrencyPolicy: "Forbid",
		}},
		FailedJobDetails: []jobRiskItem{{
			Namespace:      "ops",
			Name:           "daily-backup-123",
			Failed:         3,
			Active:         1,
			Succeeded:      0,
			Age:            3 * time.Hour,
			FailureReasons: []string{"Failed(BackoffLimitExceeded)"},
		}},
	}

	storage := storageSummary{PVC: pvcSummary{}}
	out := buildReportMarkdown(now, componentsSummary{}, nodeSummary{}, ws, podCounts{}, abnormalPodsSummary{}, nil, nil, storage, warningEventSummary{}, networkSummary{}, nsResourceSummary{}, nil, inspectionExtras{})
	for _, want := range []string{
		"### 计划任务（CronJob）",
		"### 批处理（Job）",
		"ops/daily-backup",
		"ops/daily-backup-123",
		"Failed(BackoffLimitExceeded)",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("report missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "|") {
		t.Fatalf("report must not contain markdown tables:\n%s", out)
	}
}

func TestBuildReportMarkdown_Extras(t *testing.T) {
	now := time.Date(2026, 8, 10, 2, 0, 0, 0, time.FixedZone("CST", 8*3600))

	extras := inspectionExtras{
		Security: securitySummary{
			Privileged:   1,
			RunAsRoot:    2,
			LatestImages: 3,
			TopItems: []securityRiskItem{
				{Namespace: "kube-system", Name: "node-agent-x", Issues: []string{"hostNetwork", "特权容器"}},
			},
			TopLatestImages: []string{"nginx（2）"},
		},
		Hygiene: hygieneSummary{NoCPURequest: 4, NoMemLimit: 6, TopNoResources: []string{"default/app（缺 内存限制）"}},
		Certs: certSummary{
			Available:  true,
			TLSSecrets: 5,
			Expired:    1,
			Expiring7:  1,
			Risks: []certRiskItem{
				{Namespace: "prod", Name: "api-tls", NotAfter: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), DaysLeft: -9, Expired: true},
				{Namespace: "prod", Name: "web-tls", NotAfter: time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC), DaysLeft: 5},
			},
			MissingIngressSecrets: []string{"prod/web -> web-tls-missing"},
		},
		Endpoints: endpointSummary{
			Available: true,
			Checked:   20,
			Empty:     1,
			Risks:     []endpointRiskItem{{Namespace: "prod", Service: "legacy-api", Type: "ClusterIP", Age: 48 * time.Hour}},
		},
		HPA: hpaSummary{
			Available: true,
			Total:     3,
			Risks:     []hpaRiskItem{{Namespace: "prod", Name: "web-hpa", Target: "Deployment/web", Current: 5, Min: 2, Max: 5, Reason: "已达最大副本数"}},
		},
		PDB: pdbSummary{Available: true, Total: 2, Blocked: 1, Risks: []pdbRiskItem{{Namespace: "prod", Name: "web-pdb", Expected: 3}}},
		Stale: staleSummary{
			TerminatingPods:       []staleItem{{Namespace: "dev", Name: "stuck-pod", Age: 2 * time.Hour}},
			TerminatingNamespaces: []string{"old-ns"},
			OldJobs:               2,
			OldJobTop:             []staleItem{{Namespace: "ops", Name: "migrate-1", Age: 10 * 24 * time.Hour}},
			OrphanPVCs:            []staleItem{{Namespace: "dev", Name: "data-old", Age: 72 * time.Hour}},
			HasNSAccess:           true,
		},
	}
	nodes := nodeSummary{
		Total:         1,
		ReadyCount:    1,
		Unschedulable: []string{"node-1"},
		PodOverload:   []nodePodLoad{{Name: "node-1", Pods: 108, Capacity: 110, Percent: 98.2}},
	}

	out := buildReportMarkdown(now, componentsSummary{}, nodes, workloadsSummary{}, podCounts{Total: 1, Running: 1}, abnormalPodsSummary{}, nil, nil, storageSummary{}, warningEventSummary{}, networkSummary{}, nsResourceSummary{}, nil, extras)
	for _, want := range []string{
		"- 安全配置：",
		"- TLS 证书：",
		"## 🛡️ 安全与配置巡检",
		"特权容器 Pod：1",
		"kube-system/node-agent-x：hostNetwork、特权容器",
		"## 📜 证书巡检（TLS Secret）",
		"prod/api-tls：已过期",
		"prod/web-tls：剩余 5 天",
		"prod/web -> web-tls-missing",
		"## 🔌 Service Endpoints 健康",
		"prod/legacy-api：type=ClusterIP",
		"## 📈 弹性伸缩与可用性（HPA/PDB）",
		"prod/web-hpa -> Deployment/web",
		"prod/web-pdb：expected=3",
		"## 🧹 残留与闲置资源",
		"dev/stuck-pod",
		"Terminating 卡住 Namespace：old-ns",
		"ops/migrate-1",
		"dev/data-old",
		"## 🖥️ 节点补充检查",
		"已封锁（Unschedulable）节点：1（node-1）",
		"node-1：pods=108/110（98%）",
		"TLS 证书到期风险（紧急）",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("report missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "|") {
		t.Fatalf("report must not contain markdown tables:\n%s", out)
	}
}

func TestIsLatestTag(t *testing.T) {
	cases := map[string]bool{
		"nginx":                           true,
		"nginx:latest":                    true,
		"registry.example.com/a/b:LATEST": true,
		"nginx:1.25":                      false,
		"nginx@sha256:abc123":             false,
		"":                                false,
	}
	for image, want := range cases {
		if got := isLatestTag(image); got != want {
			t.Fatalf("isLatestTag(%q) = %v, want %v", image, got, want)
		}
	}
}
