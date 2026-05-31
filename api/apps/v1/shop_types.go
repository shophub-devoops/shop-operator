/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Availability picks how many replicas the Shop Deployment runs.
// +kubebuilder:validation:Enum=standard;high
type Availability string

const (
	AvailabilityStandard Availability = "standard"
	AvailabilityHigh     Availability = "high"
)

// DatabaseKind selects which database operator backs the Shop instance.
// +kubebuilder:validation:Enum=postgres;mongodb
type DatabaseKind string

const (
	DatabasePostgres DatabaseKind = "postgres"
	DatabaseMongoDB  DatabaseKind = "mongodb"
)

// ShopSpec defines the desired state of Shop.
type ShopSpec struct {
	// Title is the storefront display name shown to buyers.
	Title string `json:"title"`

	// Availability maps to replica count: standard=2, high=3.
	// +kubebuilder:default:=standard
	Availability Availability `json:"availability"`

	// Replicas overrides the availability-derived replica count. When set
	// (e.g. by `kubectl scale shop` or an HPA via the scale subresource) it
	// wins; when nil, Availability decides.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Database picks the DB operator that provisions persistence for this shop.
	// +kubebuilder:default:=postgres
	Database DatabaseKind `json:"database"`

	// WalletAddress is the public crypto address where buyers send payment.
	// Owner-supplied; not validated on-chain by the operator.
	WalletAddress string `json:"walletAddress"`

	// DiscordWebhookSecretRef points to a Secret with the Discord webhook URL.
	// If nil, Discord notifications are skipped for this shop.
	// +optional
	DiscordWebhookSecretRef *corev1.SecretReference `json:"discordWebhookSecretRef,omitempty"`

	// Image overrides the default Shop container image.
	// Used by CI/CD to bump shop image without touching the operator chart.
	// +optional
	Image *string `json:"image,omitempty"`
}

// ShopStatus defines the observed state of Shop.
type ShopStatus struct {
	// Conditions track high-level reconcile state (Available, Progressing, Degraded).
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// URL is the ingress hostname where the storefront is reachable.
	// +optional
	URL string `json:"url,omitempty"`

	// DatabaseSecret is the name of the Secret holding DB credentials
	// (auto-created by CNPG for postgres or MongoDB operator for mongodb).
	// +optional
	DatabaseSecret string `json:"databaseSecret,omitempty"`

	// ReadyReplicas mirrors the Deployment's readyReplicas.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Selector is the label selector for the Shop's pods, as a string. The
	// scale subresource exposes it so an HPA can find the pods it scales.
	// +optional
	Selector string `json:"selector,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.readyReplicas,selectorpath=.status.selector
// +kubebuilder:resource:shortName=sh,categories={shophub}
// +kubebuilder:printcolumn:name="TITLE",type="string",JSONPath=".spec.title"
// +kubebuilder:printcolumn:name="DB",type="string",JSONPath=".spec.database"
// +kubebuilder:printcolumn:name="READY",type="integer",JSONPath=".status.readyReplicas"
// +kubebuilder:printcolumn:name="URL",type="string",JSONPath=".status.url"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"

// Shop is the Schema for the shops API
type Shop struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Shop
	// +required
	Spec ShopSpec `json:"spec"`

	// status defines the observed state of Shop
	// +optional
	Status ShopStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ShopList contains a list of Shop
type ShopList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Shop `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Shop{}, &ShopList{})
}
