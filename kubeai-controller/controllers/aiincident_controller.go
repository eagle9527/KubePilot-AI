package controllers

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kubeaiiov1 "github.com/kubepilot-ai/kubeai-controller/api/v1"
	"github.com/kubepilot-ai/kubeai-controller/pkg/config"
	"github.com/kubepilot-ai/kubeai-controller/pkg/llm"
	"github.com/kubepilot-ai/kubeai-controller/pkg/notifier"
)

// init 函数确保 package 被正确导入
func init() {
	// 这里的代码会在包被导入时执行
}

func init() {
	// 确保 scheme 被正确注册
	_ = kubeaiiov1.AddToScheme
}

// AIIncidentReconciler 协调 AIIncident 资源
type AIIncidentReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	LLMManager      *llm.Manager
	NotifierManager *notifier.Manager
	Config          *config.ControllerConfig
}

func (r *AIIncidentReconciler) updateStatus(ctx context.Context, incident *kubeaiiov1.AIIncident, mutate func(*kubeaiiov1.AIIncident)) error {
	key := client.ObjectKeyFromObject(incident)
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var latest kubeaiiov1.AIIncident
		if err := r.Get(ctx, key, &latest); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		mutate(&latest)
		if err := r.Status().Update(ctx, &latest); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		return nil
	})
}

// +kubebuilder:rbac:groups=kubeai.io,resources=aiincidents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubeai.io,resources=aiincidents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kubeai.io,resources=aiincidents/finalizers,verbs=update

// Reconcile 是主要的协调循环
func (r *AIIncidentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 获取 AIIncident 资源
	var incident kubeaiiov1.AIIncident
	if err := r.Get(ctx, req.NamespacedName, &incident); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("Reconciling AIIncident", "name", incident.Name, "phase", incident.Status.Phase)

	// 根据当前阶段处理
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
}

// handlePending 处理 Pending 阶段
func (r *AIIncidentReconciler) handlePending(ctx context.Context, incident *kubeaiiov1.AIIncident) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Handling Pending phase", "incident", incident.Name)

	now := time.Now()
	if err := r.updateStatus(ctx, incident, func(latest *kubeaiiov1.AIIncident) {
		latest.Status.Phase = "Analyzing"
		latest.Status.StartTime = &metav1.Time{Time: now}
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status to Analyzing: %w", err)
	}

	return ctrl.Result{Requeue: true}, nil
}

// handleAnalyzing 处理 Analyzing 阶段
func (r *AIIncidentReconciler) handleAnalyzing(ctx context.Context, incident *kubeaiiov1.AIIncident) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Handling Analyzing phase", "incident", incident.Name)

	// 准备 LLM 分析请求
	req := &llm.AnalysisRequest{
		ResourceType: incident.Spec.Resource.Kind,
		ResourceName: incident.Spec.Resource.Name,
		Namespace:    incident.Spec.Resource.Namespace,
		EventType:    incident.Spec.Trigger.Type,
		Message:      incident.Spec.Trigger.Message,
		Severity:     incident.Spec.Trigger.Severity,
	}

	// 添加上下文信息
	if incident.Spec.Analyze.Context != nil {
		req.Logs = incident.Spec.Analyze.Context.Logs
		req.Metrics = incident.Spec.Analyze.Context.Metrics
		req.Events = incident.Spec.Analyze.Context.Events
	}

	// 获取 Provider 名称
	providerName := incident.Spec.Analyze.Provider
	if providerName == "" {
		providerName = r.Config.LLM.DefaultProvider
	}

	// 调用 LLM 进行分析
	log.Info("Calling LLM for analysis", "provider", providerName)
	result, err := r.LLMManager.Analyze(ctx, providerName, req)
	if err != nil {
		log.Error(err, "Failed to analyze with LLM")

		if updateErr := r.updateStatus(ctx, incident, func(latest *kubeaiiov1.AIIncident) {
			latest.Status.Phase = "Failed"
			latest.Status.LastError = err.Error()
			latest.Status.CompletionTime = &metav1.Time{Time: time.Now()}
		}); updateErr != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update status to Failed: %w", updateErr)
		}

		return ctrl.Result{}, nil
	}

	if err := r.updateStatus(ctx, incident, func(latest *kubeaiiov1.AIIncident) {
		latest.Status.Phase = "Analyzed"
		latest.Status.Level = result.Level
		latest.Status.Reason = result.Reason
		latest.Status.Confidence = result.Confidence
		latest.Status.Suggestion = result.Suggestions
		latest.Status.AnalysisResult = result.RawResponse
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status to Analyzed: %w", err)
	}

	return ctrl.Result{Requeue: true}, nil
}

// handleAnalyzed 处理 Analyzed 阶段
func (r *AIIncidentReconciler) handleAnalyzed(ctx context.Context, incident *kubeaiiov1.AIIncident) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Handling Analyzed phase", "incident", incident.Name)

	if err := r.updateStatus(ctx, incident, func(latest *kubeaiiov1.AIIncident) {
		latest.Status.Phase = "Notifying"
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status to Notifying: %w", err)
	}

	return ctrl.Result{Requeue: true}, nil
}

// handleNotifying 处理 Notifying 阶段
func (r *AIIncidentReconciler) handleNotifying(ctx context.Context, incident *kubeaiiov1.AIIncident) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Handling Notifying phase", "incident", incident.Name)

	// 构建通知消息
	msg := &notifier.Message{
		Title:          fmt.Sprintf("Kubernetes 异常: %s", incident.Spec.Trigger.Type),
		Level:          notifier.MessageLevel(incident.Status.Level),
		AnalysisResult: incident.Status.Reason,
		Suggestions:    incident.Status.Suggestion,
		Timestamp:      time.Now(),
		ResourceInfo: notifier.ResourceInfo{
			Kind:      incident.Spec.Resource.Kind,
			Name:      incident.Spec.Resource.Name,
			Namespace: incident.Spec.Resource.Namespace,
		},
	}

	var notifyErr error
	if err := r.NotifierManager.Send(ctx, msg); err != nil {
		notifyErr = err
		log.Error(err, "Failed to send notification")
	}

	if err := r.updateStatus(ctx, incident, func(latest *kubeaiiov1.AIIncident) {
		if notifyErr != nil {
			latest.Status.NotificationResult = fmt.Sprintf("Failed: %v", notifyErr)
		} else {
			latest.Status.NotificationResult = "Success"
			latest.Status.Notified = true
		}
		latest.Status.Phase = "Resolved"
		latest.Status.CompletionTime = &metav1.Time{Time: time.Now()}
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status to Resolved: %w", err)
	}

	return ctrl.Result{}, nil
}

// handleResolved 处理 Resolved 阶段
func (r *AIIncidentReconciler) handleResolved(ctx context.Context, incident *kubeaiiov1.AIIncident) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Incident already resolved", "incident", incident.Name)
	return ctrl.Result{}, nil
}

// handleFailed 处理 Failed 阶段
func (r *AIIncidentReconciler) handleFailed(ctx context.Context, incident *kubeaiiov1.AIIncident) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 检查是否需要重试
	if incident.Status.RetryCount < int32(r.Config.LLM.RetryCount) {
		log.Info("Retrying incident", "incident", incident.Name, "retry", incident.Status.RetryCount)

		if err := r.updateStatus(ctx, incident, func(latest *kubeaiiov1.AIIncident) {
			latest.Status.RetryCount++
			latest.Status.Phase = "Pending"
			latest.Status.LastError = ""
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update status for retry: %w", err)
		}

		return ctrl.Result{Requeue: true}, nil
	}

	log.Info("Incident failed after all retries", "incident", incident.Name)
	return ctrl.Result{}, nil
}

// SetupWithManager 设置 Controller
func (r *AIIncidentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kubeaiiov1.AIIncident{}).
		Complete(r)
}
