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

// Network identifies the blockchain (test)net the wallet targets.
// +kubebuilder:validation:Enum=ethereum;sepolia;polygon-amoy;solana
type Network string

const (
	NetworkEthereum    Network = "ethereum"
	NetworkSepolia     Network = "sepolia"
	NetworkPolygonAmoy Network = "polygon-amoy"
	NetworkSolana      Network = "solana"
)

// WalletSpec defines the desired state of Wallet.
type WalletSpec struct {
	// Network is the blockchain the wallet operates on. Testnets only for this project.
	Network Network `json:"network"`

	// OwnerAddress optionally pins the wallet to a pre-existing on-chain address.
	// If nil, the operator generates a fresh keypair and stores the private key in a Secret.
	// +optional
	OwnerAddress *string `json:"ownerAddress,omitempty"`
}

// WalletStatus defines the observed state of Wallet.
type WalletStatus struct {
	// Conditions track high-level reconcile state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Address is the wallet's public address (either OwnerAddress copy or operator-generated).
	// +optional
	Address string `json:"address,omitempty"`

	// PrivateKeySecretRef points to the Secret (key: private-key) holding the wallet's
	// private key. Set only when the operator generated the keypair itself.
	// +optional
	PrivateKeySecretRef *corev1.SecretReference `json:"privateKeySecretRef,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=wlt,categories={shophub}
// +kubebuilder:printcolumn:name="NETWORK",type="string",JSONPath=".spec.network"
// +kubebuilder:printcolumn:name="ADDRESS",type="string",JSONPath=".status.address"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"

// Wallet is the Schema for the wallets API
type Wallet struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Wallet
	// +required
	Spec WalletSpec `json:"spec"`

	// status defines the observed state of Wallet
	// +optional
	Status WalletStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WalletList contains a list of Wallet
type WalletList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Wallet `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Wallet{}, &WalletList{})
}
