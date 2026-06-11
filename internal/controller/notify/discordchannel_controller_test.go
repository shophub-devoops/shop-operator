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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	notifyv1 "github.com/shophub-devoops/shop-operator/api/notify/v1"
)

var _ = Describe("DiscordChannel Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName  = "test-resource"
			testNamespace = "default"
			botSecretName = "fake-bot-token"
			finalizerKey  = "discordchannel.notify.shophub.local/finalizer"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: testNamespace,
		}
		discordchannel := &notifyv1.DiscordChannel{}

		BeforeEach(func() {
			By("creating a fake bot-token Secret the reconciler will read")
			tokenSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      botSecretName,
					Namespace: testNamespace,
				},
				StringData: map[string]string{"token": "fake-token-not-used-in-test"},
			}
			err := k8sClient.Create(ctx, tokenSecret)
			if err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}

			By("creating the custom resource for the Kind DiscordChannel")
			err = k8sClient.Get(ctx, typeNamespacedName, discordchannel)
			if err != nil && errors.IsNotFound(err) {
				resource := &notifyv1.DiscordChannel{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: testNamespace,
					},
					Spec: notifyv1.DiscordChannelSpec{
						GuildID: "1234567890",
						Name:    "test-channel",
						BotTokenRef: corev1.SecretReference{
							Name:      botSecretName,
							Namespace: testNamespace,
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			By("Cleanup the specific resource instance DiscordChannel")
			resource := &notifyv1.DiscordChannel{}
			if err := k8sClient.Get(ctx, typeNamespacedName, resource); err == nil {
				// Remove finalizer so envtest GC can delete the CR.
				if controllerutil.ContainsFinalizer(resource, finalizerKey) {
					controllerutil.RemoveFinalizer(resource, finalizerKey)
					Expect(k8sClient.Update(ctx, resource)).To(Succeed())
				}
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}

			By("Cleanup the fake bot-token Secret")
			sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: botSecretName, Namespace: testNamespace}}
			_ = k8sClient.Delete(ctx, sec)
		})

		It("unblocks deletion when the bot token secret is already gone", func() {
			controllerReconciler := &DiscordChannelReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			By("Reconciling once so the finalizer is present")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Deleting the bot-token Secret, then the CR")
			sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: botSecretName, Namespace: testNamespace}}
			Expect(k8sClient.Delete(ctx, sec)).To(Succeed())
			refreshed := &notifyv1.DiscordChannel{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, refreshed)).To(Succeed())
			Expect(k8sClient.Delete(ctx, refreshed)).To(Succeed())

			By("Reconciling the deletion — the finalizer must be lifted, not stuck on the missing Secret")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the CR is fully gone")
			err = k8sClient.Get(ctx, typeNamespacedName, &notifyv1.DiscordChannel{})
			Expect(errors.IsNotFound(err)).To(BeTrue())
		})

		It("adds the finalizer on first reconcile (no Discord API call)", func() {
			By("Reconciling the created resource — first pass should add finalizer and exit")
			controllerReconciler := &DiscordChannelReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the finalizer was added")
			refreshed := &notifyv1.DiscordChannel{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, refreshed)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(refreshed, finalizerKey)).To(BeTrue())
		})
	})
})
