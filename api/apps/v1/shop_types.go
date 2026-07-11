package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:Enum=standard;high
type Availability string

const (
	AvailabilityStandard Availability = "standard"
	AvailabilityHigh     Availability = "high"
)

// +kubebuilder:validation:Enum=postgres;mongodb
type DatabaseKind string

const (
	DatabasePostgres DatabaseKind = "postgres"
	DatabaseMongoDB  DatabaseKind = "mongodb"
)

type ShopSpec struct { //
	Title string `json:"title"` // obavezno pa je vrednost
	// +kubebuilder:default:=standard
	Availability Availability `json:"availability"`
	// override za skaliranje
	// +optional
	Replicas *int32 `json:"replicas,omitempty"` // opciono pa je pokazivac
	// +kubebuilder:default:=postgres
	Database      DatabaseKind `json:"database"`
	WalletAddress string       `json:"walletAddress"`
	// discord opcion pa je pokazivac
	// +optional
	DiscordWebhookSecretRef *corev1.SecretReference `json:"discordWebhookSecretRef,omitempty"`
	// +optional
	Image *string `json:"image,omitempty"`
}

// stvarnost koju pise operator u status, sve mora da bude optional da ne padne odma pri prvom apply-u
type ShopStatus struct {
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"` // semafori: available, progressing, degraded
	// +optional
	URL string `json:"url,omitempty"`
	// +optional
	DatabaseSecret string `json:"databaseSecret,omitempty"`
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// +optional
	Selector string `json:"selector,omitempty"` // selektor podova
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.readyReplicas,selectorpath=.status.selector
// +kubebuilder:resource:shortName=sh,categories={shophub}
// +kubebuilder:printcolumn:name="TITLE",type="string",JSONPath=".spec.title"
// +kubebuilder:printcolumn:name="DB",type="string",JSONPath=".spec.database"
// +kubebuilder:printcolumn:name="AVAILABILITY",type="string",JSONPath=".spec.availability"
// +kubebuilder:printcolumn:name="READY",type="integer",JSONPath=".status.readyReplicas"
// +kubebuilder:printcolumn:name="URL",type="string",JSONPath=".status.url"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
type Shop struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ShopSpec `json:"spec"`

	// +optional
	Status ShopStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true
type ShopList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Shop `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Shop{}, &ShopList{})
}
