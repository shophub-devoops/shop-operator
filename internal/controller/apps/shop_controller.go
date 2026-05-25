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

package apps

import (
	"context"
	"fmt"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	k8sappsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	appsv1 "github.com/shophub-devoops/shop-operator/api/apps/v1"
)

// ShopReconciler reconciles a Shop object
type ShopReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	// Placeholder until Faza 3 produces a real Shop backend image.
	defaultShopImage = "nginx:alpine"
	// nginx:alpine listens on 80; Service exposes 8080 → "http" named port.
	containerHTTPPort   = 80
	serviceHTTPPort     = 8080
	databaseStorageSize = "1Gi"
)

// +kubebuilder:rbac:groups=apps.shophub.local,resources=shops,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.shophub.local,resources=shops/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.shophub.local,resources=shops/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete

func (r *ShopReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.reconcile(ctx, req)
	if apierrors.IsConflict(err) {
		// Stale resourceVersion from a parallel writer (K8s deployment controller
		// updating .status, CNPG updating its Cluster, etc.). Benign — re-reconcile
		// with a fresh read.
		return ctrl.Result{Requeue: true}, nil
	}
	return res, err
}

func (r *ShopReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	shop := &appsv1.Shop{}
	if err := r.Get(ctx, req.NamespacedName, shop); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	dbSecretName, err := r.ensureDatabase(ctx, shop)
	if err != nil {
		log.Error(err, "ensureDatabase failed")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if dbSecretName == "" {
		// CNPG hasn't published the app Secret yet — wait and retry.
		log.Info("database secret not ready, requeueing")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if err := r.ensureDeployment(ctx, shop, dbSecretName); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensureDeployment: %w", err)
	}

	if err := r.ensureService(ctx, shop); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensureService: %w", err)
	}

	if err := r.updateStatusFromDeployment(ctx, shop, dbSecretName); err != nil {
		return ctrl.Result{}, fmt.Errorf("updateStatus: %w", err)
	}

	return ctrl.Result{}, nil
}

// ensureDatabase creates a CNPG Cluster if absent and waits for the
// auto-generated app Secret. Returns the Secret name once present, "" if
// still waiting (caller should requeue), or an error for hard failures.
//
// Note: spec is set only at creation time. CNPG owns the Cluster lifecycle
// after that — we don't fight it with updates. Shop spec changes that affect
// the database (e.g., switching DB kind) are handled by deletion + recreate.
func (r *ShopReconciler) ensureDatabase(ctx context.Context, shop *appsv1.Shop) (string, error) {
	if shop.Spec.Database != appsv1.DatabasePostgres {
		return "", fmt.Errorf("database kind %q not implemented yet (postgres only for now)", shop.Spec.Database)
	}

	cluster := &cnpgv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shop.Name,
			Namespace: shop.Namespace,
		},
		Spec: cnpgv1.ClusterSpec{
			Instances: 1,
			Bootstrap: &cnpgv1.BootstrapConfiguration{
				InitDB: &cnpgv1.BootstrapInitDB{
					Database: shop.Name,
					Owner:    shop.Name,
				},
			},
			StorageConfiguration: cnpgv1.StorageConfiguration{
				Size: databaseStorageSize,
			},
		},
	}
	if err := controllerutil.SetControllerReference(shop, cluster, r.Scheme); err != nil {
		return "", err
	}
	if err := r.Create(ctx, cluster); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create CNPG Cluster: %w", err)
	}

	secretName := shop.Name + "-app"
	sec := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: shop.Namespace, Name: secretName}, sec); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return secretName, nil
}

func (r *ShopReconciler) ensureDeployment(ctx context.Context, shop *appsv1.Shop, dbSecretName string) error {
	image := defaultShopImage
	if shop.Spec.Image != nil && *shop.Spec.Image != "" {
		image = *shop.Spec.Image
	}
	replicas := replicasFor(shop)
	labels := map[string]string{"app": shop.Name}

	dep := &k8sappsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shop.Name,
			Namespace: shop.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Spec.Replicas = &replicas
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		dep.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "shop",
					Image: image,
					Ports: []corev1.ContainerPort{
						{Name: "http", ContainerPort: int32(containerHTTPPort), Protocol: corev1.ProtocolTCP},
					},
					EnvFrom: []corev1.EnvFromSource{{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: dbSecretName},
						},
					}},
				}},
			},
		}
		return controllerutil.SetControllerReference(shop, dep, r.Scheme)
	})
	return err
}

func (r *ShopReconciler) ensureService(ctx context.Context, shop *appsv1.Shop) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shop.Name,
			Namespace: shop.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Spec.Selector = map[string]string{"app": shop.Name}
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       "http",
			Port:       int32(serviceHTTPPort),
			TargetPort: intstr.FromString("http"),
			Protocol:   corev1.ProtocolTCP,
		}}
		return controllerutil.SetControllerReference(shop, svc, r.Scheme)
	})
	return err
}

func (r *ShopReconciler) updateStatusFromDeployment(ctx context.Context, shop *appsv1.Shop, dbSecretName string) error {
	dep := &k8sappsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: shop.Namespace, Name: shop.Name}, dep); err != nil {
		return err
	}

	shop.Status.ReadyReplicas = dep.Status.ReadyReplicas
	shop.Status.DatabaseSecret = dbSecretName

	if err := r.Status().Update(ctx, shop); err != nil {
		if apierrors.IsConflict(err) {
			// Stale resourceVersion — controller will reconcile again shortly.
			return nil
		}
		return err
	}
	return nil
}

func replicasFor(shop *appsv1.Shop) int32 {
	if shop.Spec.Availability == appsv1.AvailabilityHigh {
		return 3
	}
	return 2
}

// SetupWithManager sets up the controller with the Manager.
func (r *ShopReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Shop{}).
		Owns(&k8sappsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&cnpgv1.Cluster{}).
		Named("apps-shop").
		Complete(r)
}
