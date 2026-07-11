package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:Enum=ethereum;sepolia;polygon-amoy;solana
type Network string

const (
	NetworkEthereum    Network = "ethereum"
	NetworkSepolia     Network = "sepolia"
	NetworkPolygonAmoy Network = "polygon-amoy"
	NetworkSolana      Network = "solana"
)

type WalletSpec struct {
	Network Network `json:"network"`
	// +optional
	OwnerAddress *string `json:"ownerAddress,omitempty"`
}

type WalletStatus struct {
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	Address string `json:"address,omitempty"`

	// tip je SECRET REFERENCE
	// +optional
	PrivateKeySecretRef *corev1.SecretReference `json:"privateKeySecretRef,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=wlt,categories={shophub}
// +kubebuilder:printcolumn:name="NETWORK",type="string",JSONPath=".spec.network"
// +kubebuilder:printcolumn:name="ADDRESS",type="string",JSONPath=".status.address"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"

type Wallet struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec WalletSpec `json:"spec"`

	// +optional
	Status WalletStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

type WalletList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Wallet `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Wallet{}, &WalletList{})
}
