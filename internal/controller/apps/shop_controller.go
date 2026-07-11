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

type ShopReconciler struct {
	client.Client                 // klijent prica sa API serverom
	Scheme        *runtime.Scheme // registar tipova, ovo mu treba da serijalizuje shop, snpg cluster itd...
	Grafana       *grafanaClient
}

const (
	containerHTTPPort      = 8080
	serviceHTTPPort        = 8080
	appLabelKey            = "app"
	httpPortName           = "http"
	discordWebhookKey      = "webhook-url"
	tempoOTLPEndpoint      = "http://tempo.monitoring.svc.cluster.local:4318"
	databaseStorageSize    = "1Gi"
	mongoDBVersion         = "6.0.5"
	mongoPasswordBytes     = 16
	mongoDBDatabaseSA      = "mongodb-database"
	condAvailable          = "Available"
	condProgressing        = "Progressing"
	condDegraded           = "Degraded"
	reasonReady            = "Ready"
	reasonDeploying        = "Deploying"
	envDatabaseURL         = "DATABASE_URL"
	cnpgURIKey             = "uri"
	mongoURIKey            = "connectionString.standard"
	envOTLPEndpoint        = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOTELService         = "OTEL_SERVICE_NAME"
	envWalletAddress       = "WALLET_ADDRESS"
	envShopDBName          = "SHOP_DB_NAME"
	envAdminPassword       = "ADMIN_PASSWORD"
	envDiscordWebhookURL   = "DISCORD_WEBHOOK_URL"
	passwordKey            = "password"
	cnpgClusterLabel       = "cnpg.io/cluster"
	discordWebhookRefField = ".spec.discordWebhookSecretRef.name"
)

// PRIMENJENO PRAVILO LEAST PRIVILEGE: tek posle 403 dodajemo

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

// brisemo dok ovo ne obrisemo
const shopFinalizer = "apps.shophub.local/grafana-tenant-dashboard"

func (r *ShopReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.reconcile(ctx, req) // poziv logike
	if apierrors.IsConflict(err) {    // 409?
		return ctrl.Result{Requeue: true}, nil // nil nije greska, samo probamo opet
	}
	return res, err
}

func (r *ShopReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	shop := &appsv1.Shop{}
	if err := r.Get(ctx, req.NamespacedName, shop); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err) // ako je obrisano nema sta da se radi
	}

	if !shop.DeletionTimestamp.IsZero() { // ako zelimo da brisemo
		return r.reconcileDelete(ctx, shop)
	}
	if controllerutil.AddFinalizer(shop, shopFinalizer) { // dodajemo finalizer, desi se samo jednom
		if err := r.Update(ctx, shop); err != nil {
			return ctrl.Result{}, err
		}
	}

	dbSecretName, err := r.ensureDatabase(ctx, shop)
	if err != nil { // greska kod pristupa bazi
		log.Error(err, "ensureDatabase failed")
		_ = r.setConditions(ctx, shop,
			cond(condDegraded, metav1.ConditionTrue, "DatabaseFailed", err.Error()),
			cond(condAvailable, metav1.ConditionFalse, "DatabaseFailed", "database provisioning failed"),
		)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if dbSecretName == "" { // baza jos nije objavila secret, manje cekanje
		log.Info("database secret not ready, requeueing")
		_ = r.setConditions(ctx, shop,
			cond(condProgressing, metav1.ConditionTrue, "DatabaseProvisioning", "waiting for database connection secret"),
			cond(condAvailable, metav1.ConditionFalse, "DatabaseProvisioning", "database not ready"),
		)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// password za admin panel
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

	// samo se loguje greska i nastavlja se
	if err := r.ensureServiceMonitor(ctx, shop); err != nil {
		log.Info("ensureServiceMonitor skipped", "reason", err.Error())
	}

	if err := r.ensureDashboard(ctx, shop); err != nil {
		log.Info("ensureDashboard skipped", "reason", err.Error())
	}

	if err := r.ensureAlertmanagerConfig(ctx, shop); err != nil {
		log.Info("ensureAlertmanagerConfig skipped", "reason", err.Error())
	}

	// zapisi status na osnovu STVARNOG stanja
	if err := r.updateStatusFromDeployment(ctx, shop, dbSecretName); err != nil {
		return ctrl.Result{}, fmt.Errorf("updateStatus: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *ShopReconciler) reconcileDelete(ctx context.Context, shop *appsv1.Shop) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(shop, shopFinalizer) {
		return ctrl.Result{}, nil // ako je vec ocisceno
	}
	if r.Grafana != nil {
		if err := r.Grafana.deleteTenantDashboard(ctx, shop.Namespace, "shop-"+shop.Name); err != nil {
			log.Info("grafana tenant dashboard cleanup skipped", "reason", err.Error())
		}
	}
	controllerutil.RemoveFinalizer(shop, shopFinalizer) // skini kocnicu da moze da se obrise CR
	if err := r.Update(ctx, shop); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *ShopReconciler) ensureDatabase(ctx context.Context, shop *appsv1.Shop) (string, error) {
	switch shop.Spec.Database { // kad je baza spremna vraca ime secreta
	case appsv1.DatabasePostgres:
		return r.ensurePostgresDatabase(ctx, shop)
	case appsv1.DatabaseMongoDB:
		return r.ensureMongoDBDatabase(ctx, shop)
	default:
		return "", fmt.Errorf("unsupported database kind: %q", shop.Spec.Database)
	}
}

// sema baze, CNPG ih izvrsi jednom na bootstrapu baze
func postInitSQL(owner string) []string {
	// numeric a ne float (0.1 + 0.2 != 0.3)
	// pazimo da ne bude permission denied zato mora owner, i da bi moglo sa - mora navodnici ""
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
} // vlasnik baze je shop, kad se shop obrise brisu se svi kojima je on vlaskin, znaci baza

func (r *ShopReconciler) ensurePostgresDatabase(ctx context.Context, shop *appsv1.Shop) (string, error) {
	cluster := &cnpgv1.Cluster{ // pravimo CNPG-ov CR
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
					PostInitApplicationSQL: postInitSQL(shop.Name), // nasa sema od gore
				},
			},
			StorageConfiguration: cnpgv1.StorageConfiguration{
				Size: databaseStorageSize,
			},
		},
	}
	// shop je vlasnik
	if err := controllerutil.SetControllerReference(shop, cluster, r.Scheme); err != nil {
		return "", err
	}
	// kreiranje baze, + drugi uslov idempotentnost, ako klaster vec postoji ne pravimo gresku
	if err := r.Create(ctx, cluster); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create CNPG Cluster: %w", err)
	}

	// gledamo da li je CNPG objavio secret, ako nije onda vrti petlju
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

func (r *ShopReconciler) ensureMongoDBDatabase(ctx context.Context, shop *appsv1.Shop) (string, error) {
	if err := r.ensureMongoDBRBAC(ctx, shop); err != nil { // premesta iz svog namespace u tenant namespace
		return "", fmt.Errorf("ensure mongodb-database RBAC: %w", err)
	}

	pwSecretName := shop.Name + "-mongo-pw"
	if err := r.ensurePasswordSecret(ctx, shop, pwSecretName); err != nil {
		return "", fmt.Errorf("ensure mongo password secret: %w", err)
	}

	connSecretName := shop.Name + "-app"
	mdb := &mongodbv1.MongoDBCommunity{ // Mongo CR
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
				Roles: []mongodbv1.Role{ // korisnik sa db owner rolom
					{Name: "dbOwner", DB: shop.Name},
				},
				ScramCredentialsSecretName: shop.Name + "-scram",
				ConnectionStringSecretName: connSecretName, // isto kao kod CNPG, pa ne mora da znamo koja je baza
			}},
		},
	}
	// rucno vadimo owner referencu i dodeljujemo objektu
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
	// create + idempotentnost check kao pre
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

func (r *ShopReconciler) ensurePasswordSecret(ctx context.Context, shop *appsv1.Shop, name string) error {
	existing := &corev1.Secret{}
	// ako postoji ne diraj (idempotentnost)
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
	// postavljamo ownera da bi ga GC pocistio ovaj secret sa shopom kada brisemo shop
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

func (r *ShopReconciler) ensureMongoDBRBAC(ctx context.Context, shop *appsv1.Shop) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mongoDBDatabaseSA,
			Namespace: shop.Namespace,
		},
	}
	// ovde je set owner reference a ne set ctonroller reference zbog bug-a sa 2 monogo baze
	// u istom namespaceu da bi mogle da ih dele prodavnice,
	// controler ref je eksluzivan pa bi prodavnica pukla (already owned by another controller)
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		return controllerutil.SetOwnerReference(shop, sa, r.Scheme)
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
		return controllerutil.SetOwnerReference(shop, role, r.Scheme)
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
		return controllerutil.SetOwnerReference(shop, rb, r.Scheme)
	}); err != nil {
		return fmt.Errorf("upsert RoleBinding: %w", err)
	}
	return nil
}

func dbEnvFromSecret(kind appsv1.DatabaseKind, secretName string) []corev1.EnvVar {
	// mapira connection string iz shop secreta u jednnu env varijalbu
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
		ValueFrom: &corev1.EnvVarSource{ // zbog ovog SecretKeyRef
			SecretKeyRef: &corev1.SecretKeySelector{ // lozinka ne prolazi kroz operator i ne zavrsava u yamlu
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  key,
			},
		},
	}}
}

func adminSecretName(shop *appsv1.Shop) string {
	return shop.Name + "-admin"
}

// trpamo sve u env varijablu da bi konfigurisali prodavnicu
func shopEnv(shop *appsv1.Shop, dbSecretName string) []corev1.EnvVar {
	env := append(dbEnvFromSecret(shop.Spec.Database, dbSecretName), // serect ref iz gornje funkcije
		corev1.EnvVar{Name: envOTLPEndpoint, Value: tempoOTLPEndpoint}, // tempo
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

	// discord opcion
	if shop.Spec.DiscordWebhookSecretRef != nil {
		optional := true
		env = append(env, corev1.EnvVar{
			Name: envDiscordWebhookURL,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: shop.Spec.DiscordWebhookSecretRef.Name},
					Key:                  discordWebhookKey,
					Optional:             &optional,
				},
			},
		})
	}
	return env
}

func ingressBaseDomain() string {
	if v := os.Getenv("INGRESS_BASE_DOMAIN"); v != "" {
		return v
	}
	return "localhost"
}

func ingressHost(shop *appsv1.Shop) string {
	return shop.Name + "." + ingressBaseDomain()
}

func ingressURLPort() string {
	return os.Getenv("INGRESS_URL_PORT")
}

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

// fallback image
func defaultShopImage() string {
	if v := os.Getenv("DEFAULT_SHOP_IMAGE"); v != "" {
		return v
	}
	return "docker.io/urospetraskovic/shop-backend:main"
}

func (r *ShopReconciler) ensureDeployment(ctx context.Context, shop *appsv1.Shop, dbSecretName string) error {
	image := defaultShopImage()
	if shop.Spec.Image != nil && *shop.Spec.Image != "" { // ovverride iz spec-a
		image = *shop.Spec.Image
	}
	replicas := replicasFor(shop) // 2 ili 3
	labels := map[string]string{appLabelKey: shop.Name}

	dep := &k8sappsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shop.Name,
			Namespace: shop.Namespace,
		},
	}
	// deployment
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
					LivenessProbe: &corev1.Probe{ // ako nisi spreman onda restart
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/probe/liveness",
								Port: intstr.FromString(httpPortName),
							},
						},
						InitialDelaySeconds: 5,
						PeriodSeconds:       10,
					}, // readiness proverava bazu, liveness ne proverava
					ReadinessProbe: &corev1.Probe{ // ako nisi spreman onda van saobracaja
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
		// standardno da bi ga GC pokupio
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
		if svc.Labels == nil {
			svc.Labels = map[string]string{}
		}
		// selektor bira koje podove service obuhvata; labela je na samom Service
		// objektu i po njoj ga ServiceMonitor pronalazi,
		// bez labele nema Prometheus discovery-ja
		svc.Labels[appLabelKey] = shop.Name                           // labela na service objektu
		svc.Spec.Selector = map[string]string{appLabelKey: shop.Name} // koje podove service obuhvata
		svc.Spec.Type = corev1.ServiceTypeClusterIP                   // bug gde nije mogo otrikti prodavnicu
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
		sm.Labels["release"] = "kube-prometheus-stack" // da ga stack pokupi
		sm.Spec.Selector = metav1.LabelSelector{
			MatchLabels: map[string]string{appLabelKey: shop.Name}, // koji service da scrapuje
		}
		sm.Spec.Endpoints = []monitoringv1.Endpoint{{
			Port:     httpPortName,
			Path:     "/metrics", // obilazimo ovaj service na /metrics svakih 15 sekundi
			Interval: "15s",
		}}
		return controllerutil.SetControllerReference(shop, sm, r.Scheme)
	})
	return err
}

var dashboardTemplate string

func (r *ShopReconciler) ensureDashboard(ctx context.Context, shop *appsv1.Shop) error {
	var dash map[string]any
	// uzme dashboard template, zameni sa imenom prodavnice i napravi configMap
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
		if cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}
		cm.Annotations["grafana_folder"] = shop.Namespace
		cm.Data = map[string]string{shop.Name + ".json": string(rendered)}
		return controllerutil.SetControllerReference(shop, cm, r.Scheme)
	})
	if err != nil {
		return err
	}
	// da bude na oba mesta, i u main org i u per tenant pa se sync-uje
	if r.Grafana != nil {
		if err := r.Grafana.syncTenantDashboard(ctx, shop.Namespace, dash); err != nil {
			return fmt.Errorf("grafana org sync: %w", err)
		}
	}
	return nil
}

func (r *ShopReconciler) ensureAlertmanagerConfig(ctx context.Context, shop *appsv1.Shop) error {
	if shop.Spec.DiscordWebhookSecretRef == nil { // ako koristimo discord
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
				Receiver: "discord",
				// ovo je pre pinovalo sve prodavnice pod tenantom dok se nije promenilo na bas ime te prodavnice
				Matchers: []monitoringv1alpha1.Matcher{{
					Name:      "service",
					Value:     shop.Name,
					MatchType: monitoringv1alpha1.MatchEqual,
				}},
				GroupBy:        []string{"alertname"},
				GroupWait:      "30s",
				GroupInterval:  "5m",
				RepeatInterval: "1h",
			},
			Receivers: []monitoringv1alpha1.Receiver{{
				Name: "discord",
				DiscordConfigs: []monitoringv1alpha1.DiscordConfig{{
					APIURL: corev1.SecretKeySelector{ // secret za discord webhook
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

func cond(condType string, status metav1.ConditionStatus, reason, msg string) metav1.Condition {
	return metav1.Condition{Type: condType, Status: status, Reason: reason, Message: msg}
}

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

// ovo je ono sto vidimo kad uradimo kubectl describe shop
func (r *ShopReconciler) updateStatusFromDeployment(ctx context.Context, shop *appsv1.Shop, dbSecretName string) error {
	dep := &k8sappsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: shop.Namespace, Name: shop.Name}, dep); err != nil {
		return err
	}

	shop.Status.ReadyReplicas = dep.Status.ReadyReplicas
	shop.Status.DatabaseSecret = dbSecretName

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
	if shop.Spec.Replicas != nil {
		return *shop.Spec.Replicas
	}
	if shop.Spec.Availability == appsv1.AvailabilityHigh {
		return 3
	}
	return 2
}

func isConnectionSecret(o client.Object) bool {
	return strings.HasSuffix(o.GetName(), "-app")
}

func (r *ShopReconciler) shopForConnectionSecret(_ context.Context, obj client.Object) []reconcile.Request {
	name, ok := strings.CutSuffix(obj.GetName(), "-app")
	if !ok || name == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name},
	}}
}

func hasWebhookSecretSuffix(o client.Object) bool {
	return strings.HasSuffix(o.GetName(), "-webhook")
}

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

func (r *ShopReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Grafana == nil {
		r.Grafana = newGrafanaClientFromEnv()
	}

	// registrujemo indeks, zato je O(1) slozenost pretrage
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
		For(&appsv1.Shop{}).           // primarni resurs
		Owns(&k8sappsv1.Deployment{}). // child-ovi, event nad njima pravi reconcile nad shop-om
		Owns(&corev1.Service{}).
		Owns(&cnpgv1.Cluster{}).
		Owns(&mongodbv1.MongoDBCommunity{}).
		// watches su tudji objekti koji nas se ticu, svaki ima predika, to je filter koji sluzi
		// da ne pratimo sve, nego ono sto nam treba
		Watches(&corev1.Secret{}, // app secreti
						handler.EnqueueRequestsFromMapFunc(r.shopForConnectionSecret),
						builder.WithPredicates(predicate.NewPredicateFuncs(isConnectionSecret))).
		Watches(&corev1.Secret{}, // webhook secreti
			handler.EnqueueRequestsFromMapFunc(r.shopsForWebhookSecret),
			builder.WithPredicates(predicate.NewPredicateFuncs(hasWebhookSecretSuffix))).
		Named("apps-shop").
		Complete(r)
}
