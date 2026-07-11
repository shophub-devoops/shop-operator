package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DiscordChannelSpec struct {
	GuildID     string                 `json:"guildId"` // samo id discord servera
	Name        string                 `json:"name"`
	BotTokenRef corev1.SecretReference `json:"botTokenRef"` // token je OBAVEZAN nije pokazivac
}

type DiscordChannelStatus struct {
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	ChannelID string `json:"channelId,omitempty"` // diskord vrati id kanala po kreiranju
	// +optional
	WebhookSecretName string `json:"webhookSecretName,omitempty"` // lepak izmedju 2 crd-ja
	// shop referencira ovaj secret i tamo salje poruke
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dc,categories={shophub}
// +kubebuilder:printcolumn:name="GUILD",type="string",JSONPath=".spec.guildId"
// +kubebuilder:printcolumn:name="NAME",type="string",JSONPath=".spec.name"
// +kubebuilder:printcolumn:name="CHANNEL",type="string",JSONPath=".status.channelId"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"

type DiscordChannel struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec DiscordChannelSpec `json:"spec"`

	// +optional
	Status DiscordChannelStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

type DiscordChannelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []DiscordChannel `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DiscordChannel{}, &DiscordChannelList{})
}
