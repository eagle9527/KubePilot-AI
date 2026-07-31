package controllers

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kubeaiiov1 "github.com/kubepilot-ai/kubeai-controller/api/v1"
	"github.com/kubepilot-ai/kubeai-controller/pkg/config"
)

type EventIncidentReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	RestConfig *rest.Config
	Config     *config.ControllerConfig
}

func (r *EventIncidentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithName("event-incident")

	var ev corev1.Event
	if err := r.Get(ctx, req.NamespacedName, &ev); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if ev.Type != corev1.EventTypeWarning {
		return ctrl.Result{}, nil
	}
	if isOldEvent(&ev, 15*time.Minute) {
		return ctrl.Result{}, nil
	}

	kind := ev.InvolvedObject.Kind
	name := ev.InvolvedObject.Name
	ns := ev.InvolvedObject.Namespace

	triggerType, severity, triggerMsg := mapEventToTrigger(&ev)
	if triggerType == "" {
		return ctrl.Result{}, nil
	}

	incidentName := incidentNameForEvent(&ev, triggerType)
	key := client.ObjectKey{Namespace: ev.Namespace, Name: incidentName}
	var existing kubeaiiov1.AIIncident
	if err := r.Get(ctx, key, &existing); err == nil {
		return ctrl.Result{}, nil
	} else if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	var logs []string
	if r.RestConfig != nil && kind == "Pod" && ns != "" && name != "" {
		clientset, err := kubernetes.NewForConfig(r.RestConfig)
		if err == nil {
			logs = fetchPodLogs(ctx, clientset, ns, name, 80)
		}
	}

	incident := &kubeaiiov1.AIIncident{
		ObjectMeta: metav1.ObjectMeta{
			Name:      incidentName,
			Namespace: ev.Namespace,
			Labels: map[string]string{
				"kubepilot.ai/source":          "event",
				"kubepilot.ai/trigger":         triggerType,
				"kubepilot.ai/resource-kind":   kind,
				"kubepilot.ai/resource-name":   name,
				"kubepilot.ai/resource-ns":     ns,
				"kubepilot.ai/event-reason":    ev.Reason,
				"kubepilot.ai/event-type":      ev.Type,
				"app.kubernetes.io/managed-by": "kubepilot-ai-controller",
			},
		},
		Spec: kubeaiiov1.AIIncidentSpec{
			Resource: kubeaiiov1.ResourceInfo{
				Kind:       kind,
				Name:       name,
				Namespace:  ns,
				APIVersion: ev.InvolvedObject.APIVersion,
			},
			Trigger: kubeaiiov1.TriggerCondition{
				Type:     triggerType,
				Severity: severity,
				Message:  triggerMsg,
				Count:    int32(ev.Count),
			},
			Analyze: kubeaiiov1.AnalyzeConfig{
				Provider: "",
				Context: &kubeaiiov1.AnalysisContext{
					Logs: logs,
					Events: []string{
						fmt.Sprintf("reason=%s message=%s", ev.Reason, firstNonEmpty(ev.Message, triggerMsg)),
					},
				},
			},
		},
	}

	if err := r.Create(ctx, incident); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, nil
		}
		l.Error(err, "failed to create aiincident", "aiincident", key.String())
		return ctrl.Result{}, err
	}

	l.Info("created aiincident for event",
		"event", req.NamespacedName.String(),
		"aiincident", key.String(),
		"resourceKind", kind,
		"resourceNamespace", ns,
		"resourceName", name,
		"trigger", triggerType,
	)

	return ctrl.Result{}, nil
}

func (r *EventIncidentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Event{}).
		Complete(r)
}

func isOldEvent(ev *corev1.Event, maxAge time.Duration) bool {
	var t time.Time
	if !ev.LastTimestamp.IsZero() {
		t = ev.LastTimestamp.Time
	} else if !ev.EventTime.IsZero() {
		t = ev.EventTime.Time
	} else if !ev.FirstTimestamp.IsZero() {
		t = ev.FirstTimestamp.Time
	} else {
		return false
	}
	return time.Since(t) > maxAge
}

func incidentNameForEvent(ev *corev1.Event, trigger string) string {
	base := fmt.Sprintf("%s-%s-%s", strings.ToLower(ev.InvolvedObject.Kind), sanitizeName(ev.InvolvedObject.Name), strings.ToLower(trigger))
	if len(base) > 45 {
		base = base[:45]
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(string(ev.InvolvedObject.UID)))
	_, _ = h.Write([]byte("/"))
	_, _ = h.Write([]byte(trigger))
	_, _ = h.Write([]byte("/"))
	_, _ = h.Write([]byte(ev.Reason))
	suffix := fmt.Sprintf("%08x", h.Sum32())
	return fmt.Sprintf("%s-%s", strings.Trim(base, "-"), suffix)
}

func sanitizeName(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
			continue
		}
		b.WriteRune('-')
	}
	out := strings.Trim(b.String(), "-")
	for strings.Contains(out, "--") {
		out = strings.ReplaceAll(out, "--", "-")
	}
	if out == "" {
		return "x"
	}
	return out
}

func mapEventToTrigger(ev *corev1.Event) (string, string, string) {
	msg := strings.ToLower(ev.Message)
	reason := strings.ToLower(ev.Reason)

	if ev.InvolvedObject.Kind == "Pod" {
		if strings.Contains(msg, "crashloopbackoff") || reason == "backoff" && strings.Contains(msg, "restarting") {
			return "CrashLoopBackOff", "High", ev.Message
		}
		if strings.Contains(msg, "imagepullbackoff") || strings.Contains(msg, "errimagepull") || reason == "failed" && strings.Contains(msg, "image") {
			return "ImagePullBackOff", "High", ev.Message
		}
		if reason == "failedscheduling" || strings.Contains(msg, "failedscheduling") {
			return "FailedScheduling", "Medium", ev.Message
		}
		if strings.Contains(msg, "oomkilled") {
			return "OOMKilled", "High", ev.Message
		}
		if reason == "unhealthy" && strings.Contains(msg, "readiness probe failed") {
			return "Custom", "Medium", ev.Message
		}
	}

	if ev.InvolvedObject.Kind == "Node" {
		if reason == "nodenotready" || strings.Contains(msg, "node not ready") {
			return "NodeNotReady", "High", ev.Message
		}
		if strings.Contains(msg, "memorypressure") {
			return "MemoryPressure", "High", ev.Message
		}
		if strings.Contains(msg, "diskpressure") {
			return "DiskPressure", "High", ev.Message
		}
	}

	return "", "", ""
}

func fetchPodLogs(ctx context.Context, clientset *kubernetes.Clientset, ns, name string, tail int64) []string {
	opt := &corev1.PodLogOptions{
		TailLines:  &tail,
		Timestamps: true,
	}
	b, err := clientset.CoreV1().Pods(ns).GetLogs(name, opt).Do(ctx).Raw()
	if err != nil || len(b) == 0 {
		return nil
	}
	lines := strings.Split(string(b), "\n")
	out := make([]string, 0, 120)
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

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}
