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

package notify

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	notifyv1 "github.com/shophub-devoops/shop-operator/api/notify/v1"
)

const (
	// discordChannelFinalizer guards external Discord state cleanup before the
	// CR is removed from K8s. Without it, deleting the CR would orphan the
	// channel on the Discord guild.
	discordChannelFinalizer = "discordchannel.notify.shophub.local/finalizer"
	// botTokenField is the Secret key under which the bot token is stored.
	botTokenField = "token"
	// webhookURLField is the Secret key under which we store the webhook URL.
	webhookURLField = "webhook-url"
)

// DiscordChannelReconciler reconciles a DiscordChannel object
type DiscordChannelReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=notify.shophub.local,resources=discordchannels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=notify.shophub.local,resources=discordchannels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=notify.shophub.local,resources=discordchannels/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

// Reconcile orchestrates Discord-side channel + webhook lifecycle for a
// DiscordChannel CR. Uses a finalizer so external Discord state is cleaned up
// before the CR is removed from K8s.
func (r *DiscordChannelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	ch := &notifyv1.DiscordChannel{}
	if err := r.Get(ctx, req.NamespacedName, ch); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	botToken, err := r.readBotToken(ctx, ch)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("read bot token: %w", err)
	}
	dc := newDiscordClient(botToken)

	// Handle deletion via finalizer.
	if !ch.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, ch, dc)
	}

	// Ensure finalizer is present before any external work.
	if !controllerutil.ContainsFinalizer(ch, discordChannelFinalizer) {
		controllerutil.AddFinalizer(ch, discordChannelFinalizer)
		if err := r.Update(ctx, ch); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Channel: create on first run, reuse known ID afterward.
	channelID := ch.Status.ChannelID
	if channelID == "" {
		dch, err := dc.createChannel(ctx, ch.Spec.GuildID, ch.Spec.Name)
		if err != nil {
			log.Error(err, "createChannel failed")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		channelID = dch.ID
		// Persist the channel ID before any further external work: if the
		// webhook step below fails and we requeue without it, the retry would
		// create a duplicate channel on the guild (and the finalizer could
		// never clean the orphans up).
		ch.Status.ChannelID = channelID
		if err := r.Status().Update(ctx, ch); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, fmt.Errorf("persist channel id: %w", err)
		}
	}

	// Webhook Secret: create on first run, reuse afterward.
	webhookSecretName := ch.Name + "-webhook"
	if err := r.ensureWebhookSecret(ctx, ch, dc, channelID, webhookSecretName); err != nil {
		log.Error(err, "ensureWebhookSecret failed")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Status: only write if drift.
	if ch.Status.ChannelID == channelID && ch.Status.WebhookSecretName == webhookSecretName {
		return ctrl.Result{}, nil
	}
	ch.Status.ChannelID = channelID
	ch.Status.WebhookSecretName = webhookSecretName
	if err := r.Status().Update(ctx, ch); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("update status: %w", err)
	}
	log.Info("DiscordChannel provisioned", "channelId", channelID, "webhookSecret", webhookSecretName)
	return ctrl.Result{}, nil
}

func (r *DiscordChannelReconciler) reconcileDelete(ctx context.Context, ch *notifyv1.DiscordChannel, dc *discordClient) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(ch, discordChannelFinalizer) {
		return ctrl.Result{}, nil
	}

	if ch.Status.ChannelID != "" {
		if err := dc.deleteChannel(ctx, ch.Status.ChannelID); err != nil {
			log.Error(err, "deleteChannel failed; will retry")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}

	controllerutil.RemoveFinalizer(ch, discordChannelFinalizer)
	if err := r.Update(ctx, ch); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}
	log.Info("DiscordChannel cleaned up", "channelId", ch.Status.ChannelID)
	return ctrl.Result{}, nil
}

// readBotToken pulls the bot token from the referenced Secret.
func (r *DiscordChannelReconciler) readBotToken(ctx context.Context, ch *notifyv1.DiscordChannel) (string, error) {
	ns := ch.Spec.BotTokenRef.Namespace
	if ns == "" {
		ns = ch.Namespace
	}
	sec := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: ch.Spec.BotTokenRef.Name}, sec); err != nil {
		return "", err
	}
	token, ok := sec.Data[botTokenField]
	if !ok || len(token) == 0 {
		return "", fmt.Errorf("secret %s/%s has no %q field", ns, ch.Spec.BotTokenRef.Name, botTokenField)
	}
	return string(token), nil
}

// ensureWebhookSecret creates a webhook on the channel (and a Secret holding
// the URL) if absent. If the Secret already exists, assume the webhook still
// works and no-op — recreating would invalidate the URL used by Shop apps.
func (r *DiscordChannelReconciler) ensureWebhookSecret(ctx context.Context, ch *notifyv1.DiscordChannel, dc *discordClient, channelID, secretName string) error {
	existing := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Namespace: ch.Namespace, Name: secretName}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	// Fixed webhook name: Discord rejects webhook names containing the word
	// "discord" (USERNAME_INVALID_CONTAINS), and channel/shop-derived names
	// can legitimately contain it. The name is only a display label on the
	// posted messages, so one constant fits every channel.
	wh, err := dc.createWebhook(ctx, channelID, "shophub-alerts")
	if err != nil {
		return fmt.Errorf("create webhook: %w", err)
	}
	if wh.URL == "" {
		return fmt.Errorf("discord returned empty webhook URL")
	}

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: ch.Namespace,
		},
		StringData: map[string]string{webhookURLField: wh.URL},
	}
	if err := controllerutil.SetControllerReference(ch, sec, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, sec); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("create webhook secret: %w", err)
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DiscordChannelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&notifyv1.DiscordChannel{}).
		Owns(&corev1.Secret{}).
		Named("notify-discordchannel").
		Complete(r)
}
