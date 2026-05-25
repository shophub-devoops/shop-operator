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

// DiscordChannelSpec defines the desired state of DiscordChannel.
type DiscordChannelSpec struct {
	// GuildID is the Discord server (guild) snowflake where the channel lives.
	GuildID string `json:"guildId"`

	// Name is the channel name to create or maintain on the guild.
	Name string `json:"name"`

	// BotTokenRef points to a Secret (key: token) containing the Discord bot token.
	// The bot must hold Manage Channels and Manage Webhooks permissions on the guild.
	BotTokenRef corev1.SecretReference `json:"botTokenRef"`
}

// DiscordChannelStatus defines the observed state of DiscordChannel.
type DiscordChannelStatus struct {
	// Conditions track high-level reconcile state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ChannelID is the snowflake returned by Discord after channel creation.
	// +optional
	ChannelID string `json:"channelId,omitempty"`

	// WebhookSecretName is the Secret (key: webhook-url) that the Shop controller
	// reads to post notifications. Created by this controller after channel setup.
	// +optional
	WebhookSecretName string `json:"webhookSecretName,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dc,categories={shophub}
// +kubebuilder:printcolumn:name="GUILD",type="string",JSONPath=".spec.guildId"
// +kubebuilder:printcolumn:name="NAME",type="string",JSONPath=".spec.name"
// +kubebuilder:printcolumn:name="CHANNEL",type="string",JSONPath=".status.channelId"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"

// DiscordChannel is the Schema for the discordchannels API
type DiscordChannel struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of DiscordChannel
	// +required
	Spec DiscordChannelSpec `json:"spec"`

	// status defines the observed state of DiscordChannel
	// +optional
	Status DiscordChannelStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// DiscordChannelList contains a list of DiscordChannel
type DiscordChannelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []DiscordChannel `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DiscordChannel{}, &DiscordChannelList{})
}
