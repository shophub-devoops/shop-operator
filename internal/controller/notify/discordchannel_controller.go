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
	discordChannelFinalizer = "discordchannel.notify.shophub.local/finalizer"
	botTokenField           = "token"       // kljuc bot tokena
	webhookURLField         = "webhook-url" // kljuc webhook url-a, isti kljuc koji shop controller cita
) // taj lepak izmedju 2 kontrolera

type DiscordChannelReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=notify.shophub.local,resources=discordchannels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=notify.shophub.local,resources=discordchannels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=notify.shophub.local,resources=discordchannels/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

func (r *DiscordChannelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	ch := &notifyv1.DiscordChannel{}
	if err := r.Get(ctx, req.NamespacedName, ch); err != nil { // ako ga nije naso izlazi
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// prvo brisanje pa onda gledamo normalan rad
	if !ch.DeletionTimestamp.IsZero() {
		botToken, err := r.readBotToken(ctx, ch)
		if apierrors.IsNotFound(err) { // ako je token secret vec obrisan onda ne mozemo da
			// ocistimo diskord, pa onda moramo da otkocimo brisanje, skidamo finalizer jer
			// je bolje jedan kanal siroce nego CR koji se nikad ne brise
			log.Info("bot token secret missing on delete; skipping Discord cleanup",
				"channelId", ch.Status.ChannelID)
			return r.removeFinalizer(ctx, ch)
		}
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("read bot token: %w", err)
		}
		return r.reconcileDelete(ctx, ch, newDiscordClient(botToken))
	}

	botToken, err := r.readBotToken(ctx, ch)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("read bot token: %w", err)
	}
	dc := newDiscordClient(botToken)

	// dodajemo finalizer pre pocetka rada da nebi bilo sirocica
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

	// pravimo kanal, prvi put je id prazan, dole ga upisujemo, da izbegnemo duplikate kanala
	channelID := ch.Status.ChannelID
	if channelID == "" {
		dch, err := dc.createChannel(ctx, ch.Spec.GuildID, ch.Spec.Name)
		if err != nil {
			log.Error(err, "createChannel failed")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		channelID = dch.ID
		ch.Status.ChannelID = channelID
		if err := r.Status().Update(ctx, ch); err != nil { // upisujemo id kanala
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, fmt.Errorf("persist channel id: %w", err)
		}
		// ovime smo sprecili anitepattern, id se upise pre webhook koraka, da je obrnutu i da
		// webhook pukne retry nebi znao da knaal vec postoji, bilo bi sirocica.
	}

	// pravimo webhook i secret
	webhookSecretName := ch.Name + "-webhook"
	if err := r.ensureWebhookSecret(ctx, ch, dc, channelID, webhookSecretName); err != nil {
		log.Error(err, "ensureWebhookSecret failed")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// status update samo ako ima promene
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

	if ch.Status.ChannelID != "" { // obrisi kanal na discordu, ako fail-uje idi opet za 30s
		if err := dc.deleteChannel(ctx, ch.Status.ChannelID); err != nil {
			log.Error(err, "deleteChannel failed; will retry")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}

	res, err := r.removeFinalizer(ctx, ch) // skidanje kocnice
	if err == nil && res.IsZero() {
		log.Info("DiscordChannel cleaned up", "channelId", ch.Status.ChannelID)
	}
	return res, err
}

func (r *DiscordChannelReconciler) removeFinalizer(ctx context.Context, ch *notifyv1.DiscordChannel) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(ch, discordChannelFinalizer) {
		return ctrl.Result{}, nil
	}
	controllerutil.RemoveFinalizer(ch, discordChannelFinalizer)
	if err := r.Update(ctx, ch); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

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

	// ime webhooka je fiksno jer discord odbija webhook imena koja sadrze rec discord u njima
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
	// owner
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

// mnnogo prostiji od shopa, nema watches/predicates, ne zavisi od drugih objekata
func (r *DiscordChannelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&notifyv1.DiscordChannel{}).
		Owns(&corev1.Secret{}).
		Named("notify-discordchannel").
		Complete(r)
}
