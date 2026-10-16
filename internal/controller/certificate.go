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

package controller

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	pkcs12 "software.sslmate.com/src/go-pkcs12"

	camusdbv1alpha1 "github.com/camusdb/k8s-operator/api/v1alpha1"
)

// certManagerGVK identifies the resource the operator creates when asked to provision TLS through
// cert-manager. It is handled as an unstructured object on purpose: the shape used here is small
// and stable, and depending on the cert-manager Go module would drag its whole API surface — and
// its version skew — into an operator that only ever writes these few fields.
var certManagerGVK = schema.GroupVersionKind{
	Group:   "cert-manager.io",
	Version: "v1",
	Kind:    "Certificate",
}

// Keys of the Secret cert-manager writes. They are fixed by cert-manager.
const (
	certManagerCertKey = "tls.crt"
	certManagerKeyKey  = "tls.key"
	certManagerCAKey   = "ca.crt"
)

// Default keys of a user-supplied keystore Secret, and the keys of the one the operator derives
// from the cert-manager output.
const (
	defaultKeystoreKey = "certificate.pfx"
	defaultCAKey       = "ca.crt"
	derivedKeystoreKey = defaultKeystoreKey
	derivedCAKey       = defaultCAKey
)

// keystoreSourceAnnotation records the digest of the cert-manager output a derived keystore was
// built from. A PKCS#12 encoding is salted, so rebuilding on every reconcile would change the
// bytes — and the pod template hash — each time; the digest is what says a rebuild is due.
const keystoreSourceAnnotation = "camusdb.io/keystore-source"

// conditionKeystoreReady reports whether the keystore the pods mount exists yet.
const conditionKeystoreReady = "KeystoreReady"

// certificateDNSNames are the identities the Raft and client endpoints are reached by.
//
// The per-pod entry is a wildcard rather than one name per ordinal so that scaling the cluster
// does not invalidate the certificate: a fixed list would have to be rewritten on every scale,
// and each rewrite reissues the certificate and restarts the nodes that mount it.
func certificateDNSNames(cluster *camusdbv1alpha1.CamusDBCluster) []string {
	peer := peerServiceName(cluster)
	client := clientServiceName(cluster)
	ns := cluster.Namespace
	return []string{
		fmt.Sprintf("*.%s.%s.svc.cluster.local", peer, ns),
		fmt.Sprintf("%s.%s.svc.cluster.local", peer, ns),
		fmt.Sprintf("%s.%s.svc.cluster.local", client, ns),
		fmt.Sprintf("%s.%s.svc", client, ns),
		fmt.Sprintf("%s.%s", client, ns),
		client,
	}
}

// desiredCertificate builds the cert-manager Certificate backing the cluster's keystore.
//
// It asks for the plain PEM pair rather than for cert-manager's own PKCS#12 output: cert-manager
// requires a password on that keystore, and CamusDB can open a keystore only without one. The
// operator converts the pair instead — see reconcileDerivedKeystore.
func desiredCertificate(cluster *camusdbv1alpha1.CamusDBCluster) *unstructured.Unstructured {
	cm := cluster.Spec.TLS.CertManager

	issuer := map[string]any{"name": cm.IssuerRef.Name}
	if cm.IssuerRef.Kind != "" {
		issuer["kind"] = cm.IssuerRef.Kind
	}
	if cm.IssuerRef.Group != "" {
		issuer["group"] = cm.IssuerRef.Group
	}

	dns := certificateDNSNames(cluster)
	names := make([]any, 0, len(dns))
	for _, n := range dns {
		names = append(names, n)
	}

	spec := map[string]any{
		"secretName": certificateName(cluster),
		"issuerRef":  issuer,
		"commonName": fmt.Sprintf("%s.%s.svc", clientServiceName(cluster), cluster.Namespace),
		"dnsNames":   names,
		// Nodes present this certificate both as servers and, on the Raft transport, as clients.
		"usages": []any{"server auth", "client auth", "digital signature", "key encipherment"},
	}
	if cm.Duration != nil {
		spec["duration"] = cm.Duration.Duration.String()
	}
	if cm.RenewBefore != nil {
		spec["renewBefore"] = cm.RenewBefore.Duration.String()
	}

	obj := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}
	obj.SetGroupVersionKind(certManagerGVK)
	obj.SetName(certificateName(cluster))
	obj.SetNamespace(cluster.Namespace)
	obj.SetLabels(podLabels(cluster))
	return obj
}

// reconcileCertificate ensures the cert-manager Certificate exists when the spec asks for one.
//
// A missing cert-manager installation is reported rather than retried into a crash loop: the
// operator cannot install it, so the useful outcome is a status message naming the cause.
func (r *CamusDBClusterReconciler) reconcileCertificate(ctx context.Context, cluster *camusdbv1alpha1.CamusDBCluster) error {
	if cluster.Spec.TLS.CertManager == nil {
		return nil
	}

	desired := desiredCertificate(cluster)
	spec := desired.Object["spec"]
	lbls := desired.GetLabels()

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(certManagerGVK)
	obj.SetName(desired.GetName())
	obj.SetNamespace(desired.GetNamespace())

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		obj.SetLabels(lbls)
		obj.Object["spec"] = spec
		return controllerutil.SetControllerReference(cluster, obj, r.Scheme)
	})
	if err != nil && apimeta.IsNoMatchError(err) {
		return fmt.Errorf("spec.tls.certManager is set but cert-manager is not installed in this cluster: %w", err)
	}
	if err != nil {
		return err
	}
	return r.reconcileDerivedKeystore(ctx, cluster)
}

// sourceDigest fingerprints the cert-manager output a keystore is built from.
func sourceDigest(data map[string][]byte) string {
	h := sha256.New()
	for _, k := range []string{certManagerCertKey, certManagerKeyKey, certManagerCAKey} {
		_, _ = fmt.Fprintf(h, "%s:%d:", k, len(data[k]))
		_, _ = h.Write(data[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// buildKeystore encodes a PEM certificate chain and key as a PKCS#12 keystore with an empty
// password, which is what CamusDB opens when it is given no password.
//
// The modern encoding (AES and SHA-256) is used rather than the unencrypted one: a PKCS#12 file
// with no MAC at all is the form many readers mishandle, while an empty password is an ordinary
// password as far as the format is concerned.
func buildKeystore(certPEM, keyPEM []byte) ([]byte, error) {
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("parsing the issued certificate and key: %w", err)
	}
	if len(pair.Certificate) == 0 {
		return nil, fmt.Errorf("the issued certificate is empty")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parsing the issued certificate: %w", err)
	}
	var chain []*x509.Certificate
	for _, der := range pair.Certificate[1:] {
		c, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, fmt.Errorf("parsing the issued certificate chain: %w", err)
		}
		chain = append(chain, c)
	}
	return pkcs12.Modern.Encode(pair.PrivateKey, leaf, chain, "")
}

// reconcileDerivedKeystore turns the Secret cert-manager writes into the one the pods mount.
//
// It runs again whenever cert-manager renews the certificate. The new keystore changes the pod
// template's keystore hash, which makes the operator roll the nodes one at a time onto it —
// CamusDB reads its certificate only at startup.
func (r *CamusDBClusterReconciler) reconcileDerivedKeystore(ctx context.Context, cluster *camusdbv1alpha1.CamusDBCluster) error {
	issued := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: certificateName(cluster), Namespace: cluster.Namespace}, issued)
	if apierrors.IsNotFound(err) {
		// Not issued yet. KeystoreReady reports the wait.
		return nil
	}
	if err != nil {
		return err
	}
	for _, k := range []string{certManagerCertKey, certManagerKeyKey, certManagerCAKey} {
		if len(issued.Data[k]) == 0 {
			// cert-manager writes the keys together; a partial Secret is mid-issue. An issuer
			// that never writes ca.crt is reported by KeystoreReady.
			return nil
		}
	}

	digest := sourceDigest(issued.Data)
	name := keystoreSecretName(cluster)
	existing := &corev1.Secret{}
	err = r.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, existing)
	if err == nil && existing.Annotations[keystoreSourceAnnotation] == digest {
		return nil
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	pfx, err := buildKeystore(issued.Data[certManagerCertKey], issued.Data[certManagerKeyKey])
	if err != nil {
		return err
	}

	secret := &corev1.Secret{ObjectMeta: objectMeta(cluster, name)}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		secret.Labels = podLabels(cluster)
		if secret.Annotations == nil {
			secret.Annotations = map[string]string{}
		}
		secret.Annotations[keystoreSourceAnnotation] = digest
		secret.Type = corev1.SecretTypeOpaque
		secret.Data = map[string][]byte{
			derivedKeystoreKey: pfx,
			derivedCAKey:       issued.Data[certManagerCAKey],
		}
		return controllerutil.SetControllerReference(cluster, secret, r.Scheme)
	})
	return err
}

// tlsSecretName resolves the Secret holding the keystore and CA the pods mount, whether supplied
// directly or derived from cert-manager's output.
func tlsSecretName(cluster *camusdbv1alpha1.CamusDBCluster) string {
	if cluster.Spec.TLS.SecretRef != nil {
		return cluster.Spec.TLS.SecretRef.Name
	}
	return keystoreSecretName(cluster)
}

// keystoreState reports whether the keystore and CA the pods mount exist, and fingerprints them.
// Until they exist the pods would sit stuck on a missing volume, or exit at once for want of a CA,
// with no explanation anywhere.
func (r *CamusDBClusterReconciler) keystoreState(ctx context.Context, cluster *camusdbv1alpha1.CamusDBCluster) (ready bool, message, digest string) {
	name := tlsSecretName(cluster)
	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret)
	if apierrors.IsNotFound(err) {
		if cluster.Spec.TLS.CertManager != nil {
			return false, fmt.Sprintf("waiting for cert-manager to issue Secret %q", certificateName(cluster)), ""
		}
		return false, fmt.Sprintf("waiting for keystore Secret %q", name), ""
	}
	if err != nil {
		return false, err.Error(), ""
	}

	ks, ca := keystoreKey(cluster), caKey(cluster)
	if len(secret.Data[ks]) == 0 {
		return false, fmt.Sprintf("Secret %q has no %q key", name, ks), ""
	}
	if block, _ := pem.Decode(secret.Data[ca]); block == nil {
		if cluster.Spec.TLS.CertManager != nil {
			return false, fmt.Sprintf("the issuer did not write %q into Secret %q; use a CA or self-signed issuer",
				certManagerCAKey, certificateName(cluster)), ""
		}
		return false, fmt.Sprintf("Secret %q has no PEM CA certificate under %q; the nodes need it to verify each other", name, ca), ""
	}

	h := sha256.New()
	_, _ = h.Write(secret.Data[ks])
	_, _ = h.Write(secret.Data[ca])
	return true, "", hex.EncodeToString(h.Sum(nil))[:32]
}
