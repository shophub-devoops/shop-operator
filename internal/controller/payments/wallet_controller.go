package payments

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"

	"github.com/ethereum/go-ethereum/crypto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	paymentsv1 "github.com/shophub-devoops/shop-operator/api/payments/v1"
)

type WalletReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	walletPrivateKeyField = "private-key"
	walletAddressField    = "address"
)

// +kubebuilder:rbac:groups=payments.shophub.local,resources=wallets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=payments.shophub.local,resources=wallets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=payments.shophub.local,resources=wallets/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

// reconcile osigurava da wallet ima ili spoljnu adresu ili par kljuceva koji ovde generisemo
func (r *WalletReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	wallet := &paymentsv1.Wallet{}
	if err := r.Get(ctx, req.NamespacedName, wallet); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err) // ako ga ne nadjemo, napolje
	}

	// slucaj 1, mi mu damo adresu, on je samo zapise i to je to
	if wallet.Spec.OwnerAddress != nil && *wallet.Spec.OwnerAddress != "" {
		return ctrl.Result{}, r.setStatusForExternalAddress(ctx, wallet, *wallet.Spec.OwnerAddress)
	}

	// slucaj 2: generisi i sacuvaj par kljuceva
	secretName := wallet.Name + "-key"
	address, err := r.ensureKeySecret(ctx, wallet, secretName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure key secret: %w", err)
	}

	// idempotentnost, ako se status vec poklapa onda nikom nista
	if wallet.Status.Address == address &&
		wallet.Status.PrivateKeySecretRef != nil &&
		wallet.Status.PrivateKeySecretRef.Name == secretName {
		return ctrl.Result{}, nil
	}

	wallet.Status.Address = address
	wallet.Status.PrivateKeySecretRef = &corev1.SecretReference{ // secret
		Name:      secretName,
		Namespace: wallet.Namespace,
	}
	if err := r.Status().Update(ctx, wallet); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	log.Info("Wallet provisioned", "address", address, "network", wallet.Spec.Network)
	return ctrl.Result{}, nil
}

func (r *WalletReconciler) ensureKeySecret(ctx context.Context, wallet *paymentsv1.Wallet, name string) (string, error) {
	existing := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Namespace: wallet.Namespace, Name: name}, existing)
	if err == nil { // prvo gledamo da li secret vec postoji, ako postoji procitamo samo (idempontentnost)
		addr, ok := existing.Data[walletAddressField]
		if !ok || len(addr) == 0 {
			return "", fmt.Errorf("existing secret %q has no %q field", name, walletAddressField)
		}
		return string(addr), nil
	}
	if !apierrors.IsNotFound(err) {
		return "", err
	}

	// ako ne postoji onda generisemo oba kljuca i stavljamo u secret
	privKey, err := crypto.GenerateKey()
	if err != nil {
		return "", fmt.Errorf("generate ECDSA key: %w", err)
	}
	// javni kljuc je izveden iz privatnog, i onda se iz njega racuna adresa
	publicKey, ok := privKey.Public().(*ecdsa.PublicKey)
	if !ok {
		return "", fmt.Errorf("unexpected public key type %T", privKey.Public())
	}
	address := crypto.PubkeyToAddress(*publicKey).Hex() // adresa tj. hash javnog kljuca
	privKeyHex := hex.EncodeToString(crypto.FromECDSA(privKey))

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: wallet.Namespace,
		},
		StringData: map[string]string{ // sacuvamo oba u secret
			walletPrivateKeyField: privKeyHex,
			walletAddressField:    address,
		},
	}
	// postavljamo ownera zbog garbage colletora
	if err := controllerutil.SetControllerReference(wallet, sec, r.Scheme); err != nil {
		return "", err
	}
	if err := r.Create(ctx, sec); err != nil { // konacno kreiramo secret
		if !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("create key secret: %w", err)
		}
		// ako izmedju get i create neko drugi napravi secret, onda bacimo nas tek generisani par
		// i procitamo adresu iz postojeceg,
		if err := r.Get(ctx, client.ObjectKey{Namespace: wallet.Namespace, Name: name}, existing); err != nil {
			return "", err
		}
		if addr, ok := existing.Data[walletAddressField]; ok && len(addr) > 0 {
			return string(addr), nil
		}
		return "", fmt.Errorf("existing secret %q after AlreadyExists has no %q field", name, walletAddressField)
	}
	return address, nil
}

func (r *WalletReconciler) setStatusForExternalAddress(ctx context.Context, wallet *paymentsv1.Wallet, address string) error {
	if wallet.Status.Address == address && wallet.Status.PrivateKeySecretRef == nil {
		return nil
	}
	wallet.Status.Address = address // ako je korisnik dao adresu upisi je u status
	// private key je null, mi nemamo sad kljuc, korisnik ga ima u svom metamasku
	wallet.Status.PrivateKeySecretRef = nil
	if err := r.Status().Update(ctx, wallet); err != nil {
		if apierrors.IsConflict(err) {
			return nil
		}
		return err
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *WalletReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&paymentsv1.Wallet{}).
		Owns(&corev1.Secret{}).
		Named("payments-wallet").
		Complete(r)
}
