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

// WalletReconciler reconciles a Wallet object
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

// Reconcile ensures a Wallet CR has either an externally-supplied OwnerAddress
// reflected in Status, or a freshly generated secp256k1 keypair stored in a
// Secret owned by the Wallet. The Secret is auto-GC'd on Wallet deletion via
// OwnerReference — no finalizer is needed (on-chain addresses cannot be
// "deleted" anyway; only the local private key is sensitive state).
func (r *WalletReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	wallet := &paymentsv1.Wallet{}
	if err := r.Get(ctx, req.NamespacedName, wallet); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Branch 1: user pre-supplied an address — record it in Status, no keypair.
	if wallet.Spec.OwnerAddress != nil && *wallet.Spec.OwnerAddress != "" {
		return ctrl.Result{}, r.setStatusForExternalAddress(ctx, wallet, *wallet.Spec.OwnerAddress)
	}

	// Branch 2: generate-and-store keypair (or read back from existing Secret).
	secretName := wallet.Name + "-key"
	address, err := r.ensureKeySecret(ctx, wallet, secretName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure key secret: %w", err)
	}

	// Status is desired-state: matching address + secret ref means no-op.
	if wallet.Status.Address == address &&
		wallet.Status.PrivateKeySecretRef != nil &&
		wallet.Status.PrivateKeySecretRef.Name == secretName {
		return ctrl.Result{}, nil
	}

	wallet.Status.Address = address
	wallet.Status.PrivateKeySecretRef = &corev1.SecretReference{
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

// ensureKeySecret creates a Secret with a fresh secp256k1 keypair if absent,
// otherwise reads the address out of the existing Secret. Returns the address.
func (r *WalletReconciler) ensureKeySecret(ctx context.Context, wallet *paymentsv1.Wallet, name string) (string, error) {
	existing := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Namespace: wallet.Namespace, Name: name}, existing)
	if err == nil {
		addr, ok := existing.Data[walletAddressField]
		if !ok || len(addr) == 0 {
			return "", fmt.Errorf("existing secret %q has no %q field", name, walletAddressField)
		}
		return string(addr), nil
	}
	if !apierrors.IsNotFound(err) {
		return "", err
	}

	privKey, err := crypto.GenerateKey()
	if err != nil {
		return "", fmt.Errorf("generate ECDSA key: %w", err)
	}
	publicKey, ok := privKey.Public().(*ecdsa.PublicKey)
	if !ok {
		return "", fmt.Errorf("unexpected public key type %T", privKey.Public())
	}
	address := crypto.PubkeyToAddress(*publicKey).Hex()
	privKeyHex := hex.EncodeToString(crypto.FromECDSA(privKey))

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: wallet.Namespace,
		},
		StringData: map[string]string{
			walletPrivateKeyField: privKeyHex,
			walletAddressField:    address,
		},
	}
	if err := controllerutil.SetControllerReference(wallet, sec, r.Scheme); err != nil {
		return "", err
	}
	if err := r.Create(ctx, sec); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("create key secret: %w", err)
		}
		// Race: someone else created it between Get and Create. Refetch and
		// read back the address. Discard our just-generated keypair.
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

// setStatusForExternalAddress writes the user-provided OwnerAddress into Status
// and clears PrivateKeySecretRef (we don't own the key in this branch).
func (r *WalletReconciler) setStatusForExternalAddress(ctx context.Context, wallet *paymentsv1.Wallet, address string) error {
	if wallet.Status.Address == address && wallet.Status.PrivateKeySecretRef == nil {
		return nil
	}
	wallet.Status.Address = address
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
