// Package v1 包含 KubePilot AI API 的 Schema 定义
// +kubebuilder:object:generate=true
// +groupName=kubeai.io
package v1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion 是 KubePilot AI API 的 GroupVersion
	GroupVersion = schema.GroupVersion{Group: "kubeai.io", Version: "v1"}

	// SchemeBuilder 用于构建 Scheme
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme 将当前 Scheme 添加到父 Scheme 中
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&AIIncident{}, &AIIncidentList{})
}
