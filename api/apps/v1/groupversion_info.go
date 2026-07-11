// +kubebuilder:object:generate=true
// +groupName=apps.shophub.local
package v1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	GroupVersion = schema.GroupVersion{Group: "apps.shophub.local", Version: "v1"}

	//nolint:staticcheck // SA1019: scheme.Builder still scaffolded by kubebuilder; tracked upstream.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	AddToScheme = SchemeBuilder.AddToScheme
)
