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
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	mongodbv1 "github.com/mongodb/mongodb-kubernetes-operator/api/v1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	monitoringv1alpha1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1alpha1"
	k8sappsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1 "github.com/shophub-devoops/shop-operator/api/apps/v1"
)

// ShopReconciler reconciles a Shop object
type ShopReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Grafana provisions a per-tenant Grafana org + dashboard so a ShopHub user
	// sees only their own Shop dashboards (spec 4.1 optional). Nil when Grafana
	// is not configured (GRAFANA_URL unset) — org sync is then a no-op.
	Grafana *grafanaClient
}

const (
	// Shop backend listens on 8080 (non-privileged, distroless-friendly).
	// Service exposes the same 8080 so the Ingress and ServiceMonitor configs
	// don't need port translation between layers.
	containerHTTPPort = 8080
	serviceHTTPPort   = 8080
	// appLabelKey is the Service / ServiceMonitor / Pod selector key.
	appLabelKey = "app"
	// httpPortName is the named port shared by container, Service and probes.
	httpPortName = "http"
	// discordWebhookKey is the Secret key holding the Discord webhook URL, as
	// written by the DiscordChannel controller (notify.webhookURLField).
	discordWebhookKey = "webhook-url"
	// tempoOTLPEndpoint is the in-cluster Tempo OTLP/HTTP receiver the Shop
	// backend exports spans to (installed from kube-state clusters/local/tempo).
	tempoOTLPEndpoint   = "http://tempo.monitoring.svc.cluster.local:4318"
	databaseStorageSize = "1Gi"
	mongoDBVersion      = "6.0.5"
	mongoPasswordBytes  = 16
	// mongoDBDatabaseSA is the ServiceAccount name the MongoDB community operator
	// expects on every Pod it spawns for a MongoDBCommunity CR. The operator's Helm
	// chart only creates this SA in its own install namespace, so we must materialize
	// it (plus its Role + RoleBinding) in every tenant namespace where we want
	// MongoDBCommunity to actually schedule Pods.
	mongoDBDatabaseSA = "mongodb-database"

	// Status condition types (Available/Progressing/Degraded taxonomy).
	condAvailable   = "Available"
	condProgressing = "Progressing"
	condDegraded    = "Degraded"
	// Condition Reasons.
	reasonReady     = "Ready"
	reasonDeploying = "Deploying"

	// Database connection env var name + the Secret keys each operator publishes.
	envDatabaseURL = "DATABASE_URL"
	cnpgURIKey     = "uri"
	mongoURIKey    = "connectionString.standard"
	// OTLP tracing env var names the Shop backend reads.
	envOTLPEndpoint = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOTELService  = "OTEL_SERVICE_NAME"
	// envWalletAddress is the shop's on-chain payment recipient (D12); the
	// backend verifies USDT transfers land here.
	envWalletAddress = "WALLET_ADDRESS"
	// envShopDBName names the database the backend's Mongo client should use.
	// Unlike a Postgres URI, the Mongo connection string the community operator
	// publishes carries no default database, so the backend reads it from here.
	// Ignored by the Postgres path. Set to the Shop name (the DB the operator
	// provisions for this tenant).
	envShopDBName = "SHOP_DB_NAME"
	// envAdminPassword guards the Shop's admin endpoints (item writes, order
	// listing). Injected from the generated <shop>-admin Secret; ShopHub reads
	// the same Secret to show the owner their admin password.
	envAdminPassword = "ADMIN_PASSWORD"
	// passwordKey is the Secret key under which generated passwords are stored
	// (both the MongoDB user password and the Shop admin password Secrets).
	passwordKey = "password"
	// cnpgClusterLabel is the label CNPG puts on the resources it generates
	// (including the <shop>-app connection Secret); its value is the cluster name.
	cnpgClusterLabel = "cnpg.io/cluster"
	// discordWebhookRefField indexes Shops by the Discord webhook Secret they
	// reference, for O(1) "which Shops use this Secret?" lookups on Secret events.
	discordWebhookRefField = ".spec.discordWebhookSecretRef.name"
)

// +kubebuilder:rbac:groups=apps.shophub.local,resources=shops,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.shophub.local,resources=shops/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.shophub.local,resources=shops/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mongodbcommunity.mongodb.com,resources=mongodbcommunity,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=alertmanagerconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete

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
		_ = r.setConditions(ctx, shop,
			cond(condDegraded, metav1.ConditionTrue, "DatabaseFailed", err.Error()),
			cond(condAvailable, metav1.ConditionFalse, "DatabaseFailed", "database provisioning failed"),
		)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if dbSecretName == "" {
		// CNPG hasn't published the app Secret yet — wait and retry.
		log.Info("database secret not ready, requeueing")
		_ = r.setConditions(ctx, shop,
			cond(condProgressing, metav1.ConditionTrue, "DatabaseProvisioning", "waiting for database connection secret"),
			cond(condAvailable, metav1.ConditionFalse, "DatabaseProvisioning", "database not ready"),
		)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Admin password Secret: guards the Shop's admin endpoints. ShopHub reads
	// this same Secret to show the owner their credentials.
	if err := r.ensurePasswordSecret(ctx, shop, adminSecretName(shop)); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensureAdminSecret: %w", err)
	}

	if err := r.ensureDeployment(ctx, shop, dbSecretName); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensureDeployment: %w", err)
	}

	if err := r.ensureService(ctx, shop); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensureService: %w", err)
	}

	if err := r.ensureIngress(ctx, shop); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensureIngress: %w", err)
	}

	if err := r.ensureServiceMonitor(ctx, shop); err != nil {
		// Cluster may not run the Prometheus operator. Log and continue —
		// the Shop is still functional without observability discovery.
		log.Info("ensureServiceMonitor skipped", "reason", err.Error())
	}

	if err := r.ensureDashboard(ctx, shop); err != nil {
		// Best-effort: a missing Grafana sidecar shouldn't block the Shop.
		log.Info("ensureDashboard skipped", "reason", err.Error())
	}

	if err := r.ensureAlertmanagerConfig(ctx, shop); err != nil {
		// Best-effort: missing Discord webhook ref or no Prometheus operator
		// shouldn't block the Shop. Alerts just won't route to Discord.
		log.Info("ensureAlertmanagerConfig skipped", "reason", err.Error())
	}

	if err := r.updateStatusFromDeployment(ctx, shop, dbSecretName); err != nil {
		return ctrl.Result{}, fmt.Errorf("updateStatus: %w", err)
	}

	return ctrl.Result{}, nil
}

// ensureDatabase dispatches to the postgres or mongodb branch based on Shop.Spec.Database.
// Returns the Secret name holding connection details once present, "" if still
// waiting (caller should requeue), or an error for hard failures.
//
// Spec is set only at creation time. The database operator owns the lifecycle
// after that — we don't fight it with updates. Shop spec changes that affect
// the database (e.g., switching DB kind) are handled by deletion + recreate.
func (r *ShopReconciler) ensureDatabase(ctx context.Context, shop *appsv1.Shop) (string, error) {
	switch shop.Spec.Database {
	case appsv1.DatabasePostgres:
		return r.ensurePostgresDatabase(ctx, shop)
	case appsv1.DatabaseMongoDB:
		return r.ensureMongoDBDatabase(ctx, shop)
	default:
		return "", fmt.Errorf("unsupported database kind: %q", shop.Spec.Database)
	}
}

// postInitSQL returns the application schema (items, orders) that CNPG runs once
// during cluster bootstrap. OWNER TO is double-quoted because DNS-1123 Shop
// names may contain hyphens, which are invalid in unquoted Postgres identifiers.
func postInitSQL(owner string) []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS items (
			id text PRIMARY KEY,
			name text NOT NULL,
			price_usdt numeric(36,18) NOT NULL,
			stock int NOT NULL DEFAULT 0
		)`,
		`ALTER TABLE items OWNER TO "` + owner + `"`,
		`CREATE TABLE IF NOT EXISTS orders (
			id text PRIMARY KEY,
			buyer_wallet text NOT NULL,
			tx_hash text,
			amount_usdt numeric(36,18) NOT NULL,
			created_at timestamptz DEFAULT now()
		)`,
		`ALTER TABLE orders OWNER TO "` + owner + `"`,
	}
}

// ensurePostgresDatabase creates a CNPG Cluster (if absent) and waits for the
// auto-generated <shop-name>-app Secret that CNPG publishes after bootstrap.
func (r *ShopReconciler) ensurePostgresDatabase(ctx context.Context, shop *appsv1.Shop) (string, error) {
	cluster := &cnpgv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shop.Name,
			Namespace: shop.Namespace,
		},
		Spec: cnpgv1.ClusterSpec{
			Instances: 1,
			Bootstrap: &cnpgv1.BootstrapConfiguration{
				InitDB: &cnpgv1.BootstrapInitDB{
					Database:               shop.Name,
					Owner:                  shop.Name,
					PostInitApplicationSQL: postInitSQL(shop.Name),
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

// ensureMongoDBDatabase creates a MongoDBCommunity CR with a generated user
// password Secret, then waits for the operator-published connection-string
// Secret. We pin the connection-string Secret name to <shop-name>-app so the
// downstream ensureDeployment uses the same envFrom regardless of DB kind.
func (r *ShopReconciler) ensureMongoDBDatabase(ctx context.Context, shop *appsv1.Shop) (string, error) {
	if err := r.ensureMongoDBRBAC(ctx, shop); err != nil {
		return "", fmt.Errorf("ensure mongodb-database RBAC: %w", err)
	}

	pwSecretName := shop.Name + "-mongo-pw"
	if err := r.ensurePasswordSecret(ctx, shop, pwSecretName); err != nil {
		return "", fmt.Errorf("ensure mongo password secret: %w", err)
	}

	connSecretName := shop.Name + "-app"
	mdb := &mongodbv1.MongoDBCommunity{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shop.Name,
			Namespace: shop.Namespace,
		},
		Spec: mongodbv1.MongoDBCommunitySpec{
			Members: 1,
			Type:    mongodbv1.ReplicaSet,
			Version: mongoDBVersion,
			Security: mongodbv1.Security{
				Authentication: mongodbv1.Authentication{
					Modes: []mongodbv1.AuthMode{"SCRAM"},
				},
			},
			Users: []mongodbv1.MongoDBUser{{
				Name: shop.Name,
				DB:   "admin",
				PasswordSecretRef: mongodbv1.SecretKeyReference{
					Name: pwSecretName,
					Key:  passwordKey,
				},
				Roles: []mongodbv1.Role{
					{Name: "dbOwner", DB: shop.Name},
				},
				ScramCredentialsSecretName: shop.Name + "-scram",
				ConnectionStringSecretName: connSecretName,
			}},
		},
	}
	// mongodbv1.MongoDBCommunity GetOwnerReferences returns a synthesized self-ref
	// that confuses controllerutil.SetControllerReference. Build the Shop owner ref
	// manually and assign via the embedded ObjectMeta field directly (promotion
	// bypasses the method override).
	shopGVK, err := apiutil.GVKForObject(shop, r.Scheme)
	if err != nil {
		return "", fmt.Errorf("gvk for shop: %w", err)
	}
	yes := true
	mdb.OwnerReferences = []metav1.OwnerReference{{
		APIVersion:         shopGVK.GroupVersion().String(),
		Kind:               shopGVK.Kind,
		Name:               shop.GetName(),
		UID:                shop.GetUID(),
		BlockOwnerDeletion: &yes,
		Controller:         &yes,
	}}
	if err := r.Create(ctx, mdb); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create MongoDBCommunity: %w", err)
	}

	sec := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: shop.Namespace, Name: connSecretName}, sec); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return connSecretName, nil
}

// ensurePasswordSecret creates a Secret with a random password under the
// "password" key if absent. Used for both the MongoDB user password and the
// Shop admin password. The Shop owns the Secret so it's garbage-collected when
// the Shop is deleted.
func (r *ShopReconciler) ensurePasswordSecret(ctx context.Context, shop *appsv1.Shop, name string) error {
	existing := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Namespace: shop.Namespace, Name: name}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	pw, err := generatePassword()
	if err != nil {
		return fmt.Errorf("generate password: %w", err)
	}

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: shop.Namespace,
		},
		StringData: map[string]string{"password": pw},
	}
	if err := controllerutil.SetControllerReference(shop, sec, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, sec)
}

func generatePassword() (string, error) {
	buf := make([]byte, mongoPasswordBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// ensureMongoDBRBAC materializes the ServiceAccount + Role + RoleBinding that
// the MongoDB community operator's spawned Pods require, in the Shop's
// namespace. All three are owned by the Shop CR so they're garbage-collected
// on Shop deletion.
//
// The operator's Helm chart only installs these in its own namespace, so
// without this step MongoDBCommunity CRs in other namespaces stall with
// "serviceaccount mongodb-database not found".
func (r *ShopReconciler) ensureMongoDBRBAC(ctx context.Context, shop *appsv1.Shop) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mongoDBDatabaseSA,
			Namespace: shop.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		return controllerutil.SetControllerReference(shop, sa, r.Scheme)
	}); err != nil {
		return fmt.Errorf("upsert ServiceAccount: %w", err)
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mongoDBDatabaseSA,
			Namespace: shop.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		role.Rules = []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get"}},
			{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"patch", "delete", "get"}},
		}
		return controllerutil.SetControllerReference(shop, role, r.Scheme)
	}); err != nil {
		return fmt.Errorf("upsert Role: %w", err)
	}

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mongoDBDatabaseSA,
			Namespace: shop.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, rb, func() error {
		rb.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     mongoDBDatabaseSA,
		}
		rb.Subjects = []rbacv1.Subject{{
			Kind: "ServiceAccount",
			Name: mongoDBDatabaseSA,
		}}
		return controllerutil.SetControllerReference(shop, rb, r.Scheme)
	}); err != nil {
		return fmt.Errorf("upsert RoleBinding: %w", err)
	}
	return nil
}

// dbEnvFromSecret returns the EnvVar slice that maps the database connection
// string from the operator-managed Secret into a single DATABASE_URL env var
// the Shop backend reads. The source key differs by DB kind: CNPG publishes
// the connection string under "uri", MongoDB Community under
// "connectionString.standard".
func dbEnvFromSecret(kind appsv1.DatabaseKind, secretName string) []corev1.EnvVar {
	var key string
	switch kind {
	case appsv1.DatabasePostgres:
		key = cnpgURIKey
	case appsv1.DatabaseMongoDB:
		key = mongoURIKey
	default:
		return nil
	}
	return []corev1.EnvVar{{
		Name: envDatabaseURL,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  key,
			},
		},
	}}
}

// adminSecretName is the per-shop Secret holding the admin password.
func adminSecretName(shop *appsv1.Shop) string {
	return shop.Name + "-admin"
}

// shopEnv is the full container env: the DATABASE_URL secret-ref plus the OTLP
// tracing config pointing the backend at the in-cluster Tempo. OTEL_SERVICE_NAME
// is the shop name so traces are grouped per tenant.
func shopEnv(shop *appsv1.Shop, dbSecretName string) []corev1.EnvVar {
	return append(dbEnvFromSecret(shop.Spec.Database, dbSecretName),
		corev1.EnvVar{Name: envOTLPEndpoint, Value: tempoOTLPEndpoint},
		corev1.EnvVar{Name: envOTELService, Value: shop.Name},
		corev1.EnvVar{Name: envWalletAddress, Value: shop.Spec.WalletAddress},
		corev1.EnvVar{Name: envShopDBName, Value: shop.Name},
		corev1.EnvVar{Name: envAdminPassword, ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: adminSecretName(shop)},
				Key:                  passwordKey,
			},
		}},
	)
}

func ingressBaseDomain() string {
	if v := os.Getenv("INGRESS_BASE_DOMAIN"); v != "" {
		return v
	}
	return "localhost"
}

// ingressHost is the external hostname for a Shop: "<name>.<base-domain>".
func ingressHost(shop *appsv1.Shop) string {
	return shop.Name + "." + ingressBaseDomain()
}

// ingressURLPort is an optional ":port" suffix for Status.URL when ingress is
// exposed on a non-standard host port (e.g. k3d maps cluster :80 to host :8080).
// The Ingress Host itself never carries a port.
func ingressURLPort() string {
	return os.Getenv("INGRESS_URL_PORT")
}

// ensureIngress exposes the Shop storefront so an admin can click through to the
// live site (spec 1.2): routes host <name>.<base-domain> to the Shop Service.
func (r *ShopReconciler) ensureIngress(ctx context.Context, shop *appsv1.Shop) error {
	pathType := networkingv1.PathTypePrefix
	ing := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: shop.Name, Namespace: shop.Namespace},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ing, func() error {
		ing.Spec = networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{{
				Host: ingressHost(shop),
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{{
							Path:     "/",
							PathType: &pathType,
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{
									Name: shop.Name,
									Port: networkingv1.ServiceBackendPort{Number: serviceHTTPPort},
								},
							},
						}},
					},
				},
			}},
		}
		return controllerutil.SetControllerReference(shop, ing, r.Scheme)
	})
	return err
}

// defaultShopImage is used for Shops that don't pin spec.image. Configurable via
// DEFAULT_SHOP_IMAGE (set by the operator chart) so it points at the published
// unified Shop image (backend + storefront); falls back to the public :main tag.
func defaultShopImage() string {
	if v := os.Getenv("DEFAULT_SHOP_IMAGE"); v != "" {
		return v
	}
	return "docker.io/urospetraskovic/shop-backend:main"
}

func (r *ShopReconciler) ensureDeployment(ctx context.Context, shop *appsv1.Shop, dbSecretName string) error {
	image := defaultShopImage()
	if shop.Spec.Image != nil && *shop.Spec.Image != "" {
		image = *shop.Spec.Image
	}
	replicas := replicasFor(shop)
	labels := map[string]string{appLabelKey: shop.Name}

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
						{Name: httpPortName, ContainerPort: int32(containerHTTPPort), Protocol: corev1.ProtocolTCP},
					},
					Env: shopEnv(shop, dbSecretName),
					LivenessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/probe/liveness",
								Port: intstr.FromString(httpPortName),
							},
						},
						InitialDelaySeconds: 5,
						PeriodSeconds:       10,
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/probe/readiness",
								Port: intstr.FromString(httpPortName),
							},
						},
						InitialDelaySeconds: 3,
						PeriodSeconds:       5,
					},
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
		// The ServiceMonitor selector matches Service labels (not Spec.Selector),
		// so the Service itself must carry "app: <shop>" for Prometheus discovery.
		if svc.Labels == nil {
			svc.Labels = map[string]string{}
		}
		svc.Labels[appLabelKey] = shop.Name
		svc.Spec.Selector = map[string]string{appLabelKey: shop.Name}
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       httpPortName,
			Port:       int32(serviceHTTPPort),
			TargetPort: intstr.FromString(httpPortName),
			Protocol:   corev1.ProtocolTCP,
		}}
		return controllerutil.SetControllerReference(shop, svc, r.Scheme)
	})
	return err
}

// ensureServiceMonitor creates a Prometheus ServiceMonitor that targets the
// Shop's Service. The `release: kube-prometheus-stack` label is what the
// stack's Prometheus instance selects on by default; without it the stack
// would ignore our SM. Kept best-effort because some clusters don't run the
// Prometheus operator at all.
func (r *ShopReconciler) ensureServiceMonitor(ctx context.Context, shop *appsv1.Shop) error {
	sm := &monitoringv1.ServiceMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shop.Name,
			Namespace: shop.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sm, func() error {
		if sm.Labels == nil {
			sm.Labels = map[string]string{}
		}
		sm.Labels["release"] = "kube-prometheus-stack"
		sm.Spec.Selector = metav1.LabelSelector{
			MatchLabels: map[string]string{appLabelKey: shop.Name},
		}
		sm.Spec.Endpoints = []monitoringv1.Endpoint{{
			Port:     httpPortName,
			Path:     "/metrics",
			Interval: "15s",
		}}
		return controllerutil.SetControllerReference(shop, sm, r.Scheme)
	})
	return err
}

// dashboardTemplate is the Grafana dashboard JSON, with a $shop placeholder that
// ensureDashboard replaces per tenant. Embedded so the operator can stamp a
// dedicated dashboard for every Shop.
//
//go:embed dashboard.json
var dashboardTemplate string

// ensureDashboard creates a per-Shop Grafana dashboard as a ConfigMap labeled
// grafana_dashboard=1 — the kube-prometheus-stack sidecar imports such ConfigMaps
// cluster-wide. The template's $shop placeholder is replaced with this Shop's
// name and the templating dropdown is dropped, so each Shop gets its own
// dashboard object (spec 4.1: "svaka Shop aplikacija treba da ima svoj dashboard")
// rather than one shared dashboard with a selector. Owned by the Shop, so it's
// garbage-collected on deletion.
func (r *ShopReconciler) ensureDashboard(ctx context.Context, shop *appsv1.Shop) error {
	var dash map[string]any
	if err := json.Unmarshal([]byte(strings.ReplaceAll(dashboardTemplate, "$shop", shop.Name)), &dash); err != nil {
		return fmt.Errorf("parse dashboard template: %w", err)
	}
	dash["title"] = "Shop — " + shop.Name
	dash["uid"] = "shop-" + shop.Name
	dash["templating"] = map[string]any{"list": []any{}}
	rendered, err := json.Marshal(dash)
	if err != nil {
		return err
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: shop.Name + "-dashboard", Namespace: shop.Namespace},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if cm.Labels == nil {
			cm.Labels = map[string]string{}
		}
		cm.Labels["grafana_dashboard"] = "1"
		// grafana_folder groups every dashboard of a tenant into one Grafana
		// folder (named after the tenant namespace). The Grafana sidecar reads
		// this annotation (folderAnnotation) — folder-level permissions can then
		// restrict each tenant's folder to that tenant (spec 4.1 per-user access).
		if cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}
		cm.Annotations["grafana_folder"] = shop.Namespace
		cm.Data = map[string]string{"shop.json": string(rendered)}
		return controllerutil.SetControllerReference(shop, cm, r.Scheme)
	})
	if err != nil {
		return err
	}

	// In addition to the ConfigMap (imported into Grafana's default org, where
	// maintainers see every Shop), push the same dashboard into a per-tenant
	// Grafana org so the ShopHub user sees only their own Shops (spec 4.1
	// optional). Best-effort: handled by the caller's skip-on-error.
	if r.Grafana != nil {
		if err := r.Grafana.syncTenantDashboard(ctx, shop.Namespace, dash); err != nil {
			return fmt.Errorf("grafana org sync: %w", err)
		}
	}
	return nil
}

// ensureAlertmanagerConfig creates an AlertmanagerConfig that routes this Shop's
// alerts to its Discord channel, when the Shop references a webhook Secret.
// The stack's Alertmanager selects it (alertmanagerConfigSelector) and, with the
// OnNamespace matcher strategy, auto-scopes its routes to the Shop's namespace —
// so only this Shop's alerts reach this Shop's Discord webhook. The apiURL
// secret-ref keeps the webhook URL out of the rendered config / git.
func (r *ShopReconciler) ensureAlertmanagerConfig(ctx context.Context, shop *appsv1.Shop) error {
	if shop.Spec.DiscordWebhookSecretRef == nil {
		return nil
	}
	sendResolved := true
	ac := &monitoringv1alpha1.AlertmanagerConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shop.Name,
			Namespace: shop.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ac, func() error {
		ac.Spec = monitoringv1alpha1.AlertmanagerConfigSpec{
			Route: &monitoringv1alpha1.Route{
				Receiver:       "discord",
				GroupBy:        []string{"alertname"},
				GroupWait:      "30s",
				GroupInterval:  "5m",
				RepeatInterval: "1h",
			},
			Receivers: []monitoringv1alpha1.Receiver{{
				Name: "discord",
				DiscordConfigs: []monitoringv1alpha1.DiscordConfig{{
					APIURL: corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: shop.Spec.DiscordWebhookSecretRef.Name,
						},
						Key: discordWebhookKey,
					},
					SendResolved: &sendResolved,
				}},
			}},
		}
		return controllerutil.SetControllerReference(shop, ac, r.Scheme)
	})
	return err
}

// cond builds a status Condition; ObservedGeneration is filled in by setConditions.
func cond(condType string, status metav1.ConditionStatus, reason, msg string) metav1.Condition {
	return metav1.Condition{Type: condType, Status: status, Reason: reason, Message: msg}
}

// setConditions applies the given conditions and persists the Shop status.
// meta.SetStatusCondition only bumps LastTransitionTime on a real state change,
// so the conditions reflect genuine transitions. Conflicts are benign — the next
// reconcile re-applies.
func (r *ShopReconciler) setConditions(ctx context.Context, shop *appsv1.Shop, conds ...metav1.Condition) error {
	for _, c := range conds {
		c.ObservedGeneration = shop.Generation
		meta.SetStatusCondition(&shop.Status.Conditions, c)
	}
	if err := r.Status().Update(ctx, shop); err != nil {
		if apierrors.IsConflict(err) {
			return nil
		}
		return err
	}
	return nil
}

func (r *ShopReconciler) updateStatusFromDeployment(ctx context.Context, shop *appsv1.Shop, dbSecretName string) error {
	dep := &k8sappsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: shop.Namespace, Name: shop.Name}, dep); err != nil {
		return err
	}

	shop.Status.ReadyReplicas = dep.Status.ReadyReplicas
	shop.Status.DatabaseSecret = dbSecretName
	// Selector lets the scale subresource (and any HPA) find the Shop's pods.
	shop.Status.Selector = appLabelKey + "=" + shop.Name
	shop.Status.URL = "http://" + ingressHost(shop) + ingressURLPort()

	desired := replicasFor(shop)
	if dep.Status.ReadyReplicas >= desired {
		return r.setConditions(ctx, shop,
			cond(condAvailable, metav1.ConditionTrue, reasonReady, "all replicas are ready"),
			cond(condProgressing, metav1.ConditionFalse, reasonReady, "deployment complete"),
			cond(condDegraded, metav1.ConditionFalse, reasonReady, "no errors"),
		)
	}
	return r.setConditions(ctx, shop,
		cond(condAvailable, metav1.ConditionFalse, reasonDeploying,
			fmt.Sprintf("%d/%d replicas ready", dep.Status.ReadyReplicas, desired)),
		cond(condProgressing, metav1.ConditionTrue, reasonDeploying, "waiting for replicas to become ready"),
	)
}

func replicasFor(shop *appsv1.Shop) int32 {
	// An explicit replica count (kubectl scale / HPA via the scale subresource)
	// wins over the availability-derived default.
	if shop.Spec.Replicas != nil {
		return *shop.Spec.Replicas
	}
	if shop.Spec.Availability == appsv1.AvailabilityHigh {
		return 3
	}
	return 2
}

// isConnectionSecret matches the operator-pinned database connection Secret
// named "<shop>-app". CNPG publishes its app Secret as "<cluster>-app" and we
// pin the MongoDB community operator's connection-string Secret to the same
// name, so this one predicate keeps the Secret watch off unrelated Secrets for
// both database kinds.
func isConnectionSecret(o client.Object) bool {
	return strings.HasSuffix(o.GetName(), "-app")
}

// shopForConnectionSecret maps a "<shop>-app" connection Secret back to its
// owning Shop in the same namespace, so the controller reacts the moment either
// database operator (CNPG or MongoDB) publishes the connection Secret instead
// of waiting for the requeue.
func (r *ShopReconciler) shopForConnectionSecret(_ context.Context, obj client.Object) []reconcile.Request {
	name, ok := strings.CutSuffix(obj.GetName(), "-app")
	if !ok || name == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name},
	}}
}

// hasWebhookSecretSuffix passes only the "<channel>-webhook" Secrets the
// DiscordChannel controller publishes, so the webhook watch's indexed lookup
// stays off every unrelated Secret event.
func hasWebhookSecretSuffix(o client.Object) bool {
	return strings.HasSuffix(o.GetName(), "-webhook")
}

// shopsForWebhookSecret answers "which Shops reference this Secret as their
// Discord webhook?" via the FieldIndexer (O(1)), enqueuing only those Shops.
func (r *ShopReconciler) shopsForWebhookSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var shops appsv1.ShopList
	if err := r.List(ctx, &shops,
		client.InNamespace(obj.GetNamespace()),
		client.MatchingFields{discordWebhookRefField: obj.GetName()},
	); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(shops.Items))
	for i := range shops.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: shops.Items[i].Namespace,
			Name:      shops.Items[i].Name,
		}})
	}
	return reqs
}

// SetupWithManager sets up the controller with the Manager.
func (r *ShopReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Configure the per-tenant Grafana org client from env (no-op if unset).
	if r.Grafana == nil {
		r.Grafana = newGrafanaClientFromEnv()
	}

	// D4: index Shops by their referenced Discord webhook Secret name so the
	// Secret watch below can find affected Shops in O(1).
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &appsv1.Shop{}, discordWebhookRefField,
		func(obj client.Object) []string {
			shop, ok := obj.(*appsv1.Shop)
			if !ok || shop.Spec.DiscordWebhookSecretRef == nil {
				return nil
			}
			return []string{shop.Spec.DiscordWebhookSecretRef.Name}
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Shop{}).
		Owns(&k8sappsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&cnpgv1.Cluster{}).
		Owns(&mongodbv1.MongoDBCommunity{}).
		// D3: react the moment either database operator publishes the <shop>-app
		// connection Secret (predicate keeps us off unrelated Secrets) instead of
		// waiting for the requeue.
		Watches(&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.shopForConnectionSecret),
			builder.WithPredicates(predicate.NewPredicateFuncs(isConnectionSecret))).
		// D4: re-reconcile Shops when their Discord webhook Secret changes. The
		// predicate keeps the indexed lookup off every unrelated Secret event.
		Watches(&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.shopsForWebhookSecret),
			builder.WithPredicates(predicate.NewPredicateFuncs(hasWebhookSecretSuffix))).
		Named("apps-shop").
		Complete(r)
}
