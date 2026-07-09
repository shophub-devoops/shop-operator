/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package apps

import (
	"context"
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	monitoringv1alpha1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1alpha1"
	k8sappsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1 "github.com/shophub-devoops/shop-operator/api/apps/v1"
)

// Test fixture names (kept as constants so goconst doesn't flag repeats).
const (
	tNS    = "ns1"
	tShop  = "shop1"
	tShop2 = "myshop"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(appsv1.AddToScheme(s))
	utilruntime.Must(cnpgv1.AddToScheme(s))
	utilruntime.Must(monitoringv1.AddToScheme(s))
	utilruntime.Must(monitoringv1alpha1.AddToScheme(s))
	return s
}

// --- pure-function unit tests -------------------------------------------------

func TestReplicasFor(t *testing.T) {
	if got := replicasFor(&appsv1.Shop{Spec: appsv1.ShopSpec{Availability: appsv1.AvailabilityStandard}}); got != 2 {
		t.Errorf("standard replicas = %d, want 2", got)
	}
	if got := replicasFor(&appsv1.Shop{Spec: appsv1.ShopSpec{Availability: appsv1.AvailabilityHigh}}); got != 3 {
		t.Errorf("high replicas = %d, want 3", got)
	}
	// An explicit replica count (scale subresource / HPA) overrides availability.
	five := int32(5)
	if got := replicasFor(&appsv1.Shop{Spec: appsv1.ShopSpec{Availability: appsv1.AvailabilityHigh, Replicas: &five}}); got != 5 {
		t.Errorf("explicit replicas = %d, want 5", got)
	}
}

func TestDbEnvFromSecret(t *testing.T) {
	pg := dbEnvFromSecret(appsv1.DatabasePostgres, "sh-app")
	if len(pg) != 1 || pg[0].Name != envDatabaseURL || pg[0].ValueFrom.SecretKeyRef.Key != cnpgURIKey {
		t.Errorf("postgres env = %+v, want %s from key %q", pg, envDatabaseURL, cnpgURIKey)
	}
	mongo := dbEnvFromSecret(appsv1.DatabaseMongoDB, "sh-app")
	if len(mongo) != 1 || mongo[0].ValueFrom.SecretKeyRef.Key != mongoURIKey {
		t.Errorf("mongo env = %+v, want key %q", mongo, mongoURIKey)
	}
}

func TestShopEnvIncludesTracing(t *testing.T) {
	shop := &appsv1.Shop{
		ObjectMeta: metav1.ObjectMeta{Name: tShop2},
		Spec:       appsv1.ShopSpec{Database: appsv1.DatabasePostgres},
	}
	got := map[string]string{}
	for _, e := range shopEnv(shop, tShop2+"-app") {
		got[e.Name] = e.Value
	}
	if _, ok := got[envOTLPEndpoint]; !ok {
		t.Errorf("missing %s", envOTLPEndpoint)
	}
	if got[envOTELService] != tShop2 {
		t.Errorf("%s = %q, want %q", envOTELService, got[envOTELService], tShop2)
	}

	// The admin password is a secret-ref to the per-shop admin Secret.
	var adminRef *corev1.SecretKeySelector
	for _, e := range shopEnv(shop, tShop2+"-app") {
		if e.Name == envAdminPassword && e.ValueFrom != nil {
			adminRef = e.ValueFrom.SecretKeyRef
		}
	}
	if adminRef == nil || adminRef.Name != tShop2+"-admin" || adminRef.Key != passwordKey {
		t.Errorf("%s secret-ref = %+v, want %s-admin/%s", envAdminPassword, adminRef, tShop2, passwordKey)
	}

	// No Discord webhook ref → no DISCORD_WEBHOOK_URL env at all.
	if _, ok := got[envDiscordWebhookURL]; ok {
		t.Errorf("unexpected %s for shop without a Discord channel", envDiscordWebhookURL)
	}
}

func TestShopEnvDiscordWebhook(t *testing.T) {
	shop := &appsv1.Shop{
		ObjectMeta: metav1.ObjectMeta{Name: tShop2},
		Spec: appsv1.ShopSpec{
			Database:                appsv1.DatabasePostgres,
			DiscordWebhookSecretRef: &corev1.SecretReference{Name: tShop2 + "-webhook"},
		},
	}
	var ref *corev1.SecretKeySelector
	for _, e := range shopEnv(shop, tShop2+"-app") {
		if e.Name == envDiscordWebhookURL && e.ValueFrom != nil {
			ref = e.ValueFrom.SecretKeyRef
		}
	}
	if ref == nil || ref.Name != tShop2+"-webhook" || ref.Key != discordWebhookKey {
		t.Fatalf("%s secret-ref = %+v, want %s-webhook/%s", envDiscordWebhookURL, ref, tShop2, discordWebhookKey)
	}
	// Optional: pods must start even before the DiscordChannel controller
	// publishes the webhook Secret.
	if ref.Optional == nil || !*ref.Optional {
		t.Errorf("webhook secret-ref should be optional")
	}
}

func TestShopForConnectionSecret(t *testing.T) {
	r := &ShopReconciler{}
	// Both database kinds publish their connection Secret as "<shop>-app", so the
	// mapper keys off the name suffix (covers CNPG and the MongoDB operator alike).
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: tShop2 + "-app", Namespace: tNS,
	}}
	reqs := r.shopForConnectionSecret(context.Background(), secret)
	if len(reqs) != 1 || reqs[0].Name != tShop2 || reqs[0].Namespace != tNS {
		t.Errorf("requests = %+v, want one for %s/%s", reqs, tNS, tShop2)
	}
	// A Secret whose name isn't "<shop>-app" maps to nothing.
	plain := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: tNS}}
	if reqs := r.shopForConnectionSecret(context.Background(), plain); len(reqs) != 0 {
		t.Errorf("non-connection secret produced requests: %+v", reqs)
	}
}

// --- reconcile integration test (fake client) ---------------------------------

func TestReconcileCreatesWorkloadAndConditions(t *testing.T) {
	ctx := context.Background()
	s := testScheme(t)

	shop := &appsv1.Shop{
		ObjectMeta: metav1.ObjectMeta{Name: tShop, Namespace: tNS},
		Spec: appsv1.ShopSpec{
			Title:         "Shop One",
			Availability:  appsv1.AvailabilityStandard,
			Database:      appsv1.DatabasePostgres,
			WalletAddress: "0xABC",
		},
	}
	// Pre-create the CNPG-published connection Secret so ensureDatabase finds it
	// (no CNPG controller runs against the fake client).
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: tShop + "-app", Namespace: tNS,
			Labels: map[string]string{cnpgClusterLabel: tShop},
		},
		Data: map[string][]byte{cnpgURIKey: []byte("postgres://u:p@host:5432/db")},
	}

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(shop, secret).
		WithStatusSubresource(&appsv1.Shop{}).
		Build()
	r := &ShopReconciler{Client: c, Scheme: s}

	if _, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: tShop, Namespace: tNS},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	key := types.NamespacedName{Name: tShop, Namespace: tNS}

	// Deployment: 2 replicas (standard), container port 8080, DB + tracing env.
	dep := &k8sappsv1.Deployment{}
	if err := c.Get(ctx, key, dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 2 {
		t.Errorf("replicas = %v, want 2", dep.Spec.Replicas)
	}
	ctr := dep.Spec.Template.Spec.Containers[0]
	if ctr.Ports[0].ContainerPort != 8080 {
		t.Errorf("container port = %d, want 8080", ctr.Ports[0].ContainerPort)
	}
	envNames := map[string]bool{}
	for _, e := range ctr.Env {
		envNames[e.Name] = true
	}
	for _, want := range []string{envDatabaseURL, envOTLPEndpoint, envOTELService, envWalletAddress} {
		if !envNames[want] {
			t.Errorf("deployment env missing %s", want)
		}
	}

	// Service: carries the app label so the ServiceMonitor can select it.
	svc := &corev1.Service{}
	if err := c.Get(ctx, key, svc); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if svc.Labels[appLabelKey] != tShop {
		t.Errorf("service app label = %q, want %q", svc.Labels[appLabelKey], tShop)
	}

	// Status conditions: replicas aren't ready (no deployment controller in the
	// fake), so Available is False with reason Deploying.
	got := &appsv1.Shop{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get shop: %v", err)
	}
	avail := meta.FindStatusCondition(got.Status.Conditions, condAvailable)
	if avail == nil {
		t.Fatal("Available condition not set")
	}
	if avail.Status != metav1.ConditionFalse || avail.Reason != reasonDeploying {
		t.Errorf("Available = %s/%s, want False/%s", avail.Status, avail.Reason, reasonDeploying)
	}
}

func TestReconcileAlertmanagerConfigScopedToShop(t *testing.T) {
	ctx := context.Background()
	s := testScheme(t)

	shop := &appsv1.Shop{
		ObjectMeta: metav1.ObjectMeta{Name: tShop, Namespace: tNS},
		Spec: appsv1.ShopSpec{
			Title:                   "Shop One",
			Availability:            appsv1.AvailabilityStandard,
			Database:                appsv1.DatabasePostgres,
			WalletAddress:           "0xABC",
			DiscordWebhookSecretRef: &corev1.SecretReference{Name: tShop + "-webhook"},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: tShop + "-app", Namespace: tNS,
			Labels: map[string]string{cnpgClusterLabel: tShop},
		},
		Data: map[string][]byte{cnpgURIKey: []byte("postgres://u:p@host:5432/db")},
	}

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(shop, secret).
		WithStatusSubresource(&appsv1.Shop{}).
		Build()
	r := &ShopReconciler{Client: c, Scheme: s}

	if _, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: tShop, Namespace: tNS},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	ac := &monitoringv1alpha1.AlertmanagerConfig{}
	if err := c.Get(ctx, types.NamespacedName{Name: tShop, Namespace: tNS}, ac); err != nil {
		t.Fatalf("get alertmanagerconfig: %v", err)
	}
	// The route must be scoped to THIS shop: OnNamespace pins the namespace, but
	// several shops can share one tenant namespace — without the service matcher
	// each shop's channel would get every sibling shop's alerts.
	ms := ac.Spec.Route.Matchers
	if len(ms) != 1 || ms[0].Name != "service" || ms[0].Value != tShop || ms[0].MatchType != monitoringv1alpha1.MatchEqual {
		t.Fatalf("route matchers = %+v, want single service=%s equality matcher", ms, tShop)
	}
}
