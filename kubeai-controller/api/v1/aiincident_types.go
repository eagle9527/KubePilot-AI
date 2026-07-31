// AIIncident 是 KubePilot AI Operator 的核心 CRD
// 用于记录 AI 分析的集群异常事件
package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AIIncidentSpec 定义了 AI 分析事件的期望状态
type AIIncidentSpec struct {
	// Resource 定义了发生异常的资源信息
	Resource ResourceInfo `json:"resource,omitempty"`

	// Trigger 定义了触发分析的条件
	Trigger TriggerCondition `json:"trigger,omitempty"`

	// Analyze 定义了 AI 分析的配置
	Analyze AnalyzeConfig `json:"analyze,omitempty"`
}

// ResourceInfo 定义了 Kubernetes 资源信息
type ResourceInfo struct {
	// Kind 是资源类型，如 Pod、Node、Deployment 等
	// +kubebuilder:validation:Enum=Pod;Node;Deployment;StatefulSet;DaemonSet;Ingress;Service
	Kind string `json:"kind,omitempty"`

	// Name 是资源名称
	Name string `json:"name,omitempty"`

	// Namespace 是资源所在的命名空间（对集群级资源可为空）
	Namespace string `json:"namespace,omitempty"`

	// APIVersion 是资源的 API 版本
	APIVersion string `json:"apiVersion,omitempty"`
}

// TriggerCondition 定义了触发条件
type TriggerCondition struct {
	// Type 是触发类型
	// +kubebuilder:validation:Enum=CrashLoopBackOff;ImagePullBackOff;OOMKilled;NodeNotReady;DiskPressure;MemoryPressure;HighCPU;HighMemory;PodPending;FailedScheduling;IngressError;Custom
	Type string `json:"type,omitempty"`

	// Severity 是严重程度
	// +kubebuilder:validation:Enum=Critical;High;Medium;Low;Info
	Severity string `json:"severity,omitempty"`

	// Message 是触发时的原始消息
	Message string `json:"message,omitempty"`

	// Count 是触发次数
	Count int32 `json:"count,omitempty"`
}

// AnalyzeConfig 定义了 AI 分析配置
type AnalyzeConfig struct {
	// Provider 是 LLM 提供商
	// +kubebuilder:validation:Enum=openai;deepseek;qwen;claude;ollama
	Provider string `json:"provider,omitempty"`

	// Model 是使用的模型名称
	Model string `json:"model,omitempty"`

	// Temperature 控制输出的随机性
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=2
	Temperature *float32 `json:"temperature,omitempty"`

	// MaxTokens 是最大输出 token 数
	MaxTokens *int32 `json:"maxTokens,omitempty"`

	// Context 是额外的上下文信息，如相关日志、指标等
	Context *AnalysisContext `json:"context,omitempty"`
}

// AnalysisContext 定义了分析的上下文信息
type AnalysisContext struct {
	// Logs 是相关的日志内容
	Logs []string `json:"logs,omitempty"`

	// Metrics 是相关的指标数据
	Metrics map[string]string `json:"metrics,omitempty"`

	// Events 是相关的 Kubernetes 事件
	Events []string `json:"events,omitempty"`

	// RelatedResources 是相关资源的信息
	RelatedResources []ResourceInfo `json:"relatedResources,omitempty"`
}

// AIIncidentStatus 定义了 AI 分析事件的观测状态
type AIIncidentStatus struct {
	// Phase 是当前阶段
	// +kubebuilder:validation:Enum=Pending;Analyzing;Analyzed;Notifying;Resolved;Failed
	Phase string `json:"phase,omitempty"`

	// Level 是诊断的级别
	// +kubebuilder:validation:Enum=Critical;Warning;Info;Success
	Level string `json:"level,omitempty"`

	// Reason 是 AI 分析的原因
	Reason string `json:"reason,omitempty"`

	// Confidence 是置信度 (0-1)
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	Confidence float64 `json:"confidence,omitempty"`

	// Suggestion 是 AI 给出的建议
	Suggestion []string `json:"suggestion,omitempty"`

	// AnalysisResult 是完整的分析结果
	AnalysisResult string `json:"analysisResult,omitempty"`

	// Notified 是否已通知
	Notified bool `json:"notified,omitempty"`

	// NotificationResult 通知结果
	NotificationResult string `json:"notificationResult,omitempty"`

	// Resolved 是否已解决
	Resolved bool `json:"resolved,omitempty"`

	// Resolution 解决方案
	Resolution string `json:"resolution,omitempty"`

	// StartTime 开始时间
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime 完成时间
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// RetryCount 重试次数
	RetryCount int32 `json:"retryCount,omitempty"`

	// LastError 最后一次错误
	LastError string `json:"lastError,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=aiincidents,scope=Namespaced,categories={kubeai,ai}
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`,priority=0
// +kubebuilder:printcolumn:name="Level",type=string,JSONPath=`.status.level`,priority=0
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.reason`,priority=1
// +kubebuilder:printcolumn:name="Confidence",type=number,JSONPath=`.status.confidence`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`,priority=0

// AIIncident 是 KubePilot AI 分析事件的主资源
type AIIncident struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AIIncidentSpec   `json:"spec,omitempty"`
	Status AIIncidentStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// AIIncidentList 包含 AIIncident 资源的列表
type AIIncidentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIIncident `json:"items"`
}
