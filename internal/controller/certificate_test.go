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
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	pkcs12 "software.sslmate.com/src/go-pkcs12"

	camusdbv1alpha1 "github.com/camusdb/k8s-operator/api/v1alpha1"
)

func certManagerCluster() *camusdbv1alpha1.CamusDBCluster {
	c := testCluster(3)
	c.Spec.TLS = camusdbv1alpha1.TLSSpec{
		CertManager: &camusdbv1alpha1.CertManagerSpec{
			IssuerRef: camusdbv1alpha1.CertManagerIssuerRef{
				Name: "camus-ca", Kind: "ClusterIssuer", Group: "cert-manager.io",
			},
		},
	}
	return c
}

// testPKI is a CA and a leaf it signed, in the PEM forms cert-manager writes.
type testPKI struct {
	caPEM, certPEM, keyPEM []byte
	leaf                   *x509.Certificate
}

// newTestPKI issues a leaf from a fresh CA. rsaKey selects cert-manager's default key type, whose
// key PEM is PKCS#1; otherwise the leaf gets an EC key in PKCS#8.
func newTestPKI(t *testing.T, rsaKey bool) testPKI {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)

	var key crypto.Signer
	var keyPEM []byte
	if rsaKey {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		key = k
		keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
	} else {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		key = k
		der, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			t.Fatal(err)
		}
		keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	}

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "camus"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{"*.camus-peer.db.svc.cluster.local"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, ca, key.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(der)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: pemCertificate, Bytes: caDER})
	// cert-manager writes the leaf followed by its chain into tls.crt.
	certPEM := append(pem.EncodeToMemory(&pem.Block{Type: pemCertificate, Bytes: der}), caPEM...)
	return testPKI{caPEM: caPEM, certPEM: certPEM, keyPEM: keyPEM, leaf: leaf}
}

// The Certificate asks for the plain PEM pair. cert-manager's own keystore output always carries a
// password, which CamusDB cannot supply.
func TestCertificateRequestsPEMNotAKeystore(t *testing.T) {
	obj := desiredCertificate(certManagerCluster())

	if _, found, _ := unstructured.NestedMap(obj.Object, "spec", "keystores"); found {
		t.Errorf("certificate requests a cert-manager keystore: %v", obj.Object["spec"])
	}
	if got := obj.GetName(); got != testTLSSecret {
		t.Errorf("certificate name = %q, want %s", got, testTLSSecret)
	}
	if secretName, _, _ := unstructured.NestedString(obj.Object, "spec", "secretName"); secretName != testTLSSecret {
		t.Errorf("secretName = %q, want %s", secretName, testTLSSecret)
	}
	// A node presents its certificate as a client on the Raft transport too.
	usages, _, _ := unstructured.NestedStringSlice(obj.Object, "spec", "usages")
	for _, u := range []string{"server auth", "client auth"} {
		if !slices.Contains(usages, u) {
			t.Errorf("usages %v missing %q", usages, u)
		}
	}
}

// The per-pod SAN is a wildcard so that scaling does not reissue the certificate and restart the
// nodes that mount it.
func TestCertificateCoversPodsWithAWildcard(t *testing.T) {
	obj := desiredCertificate(certManagerCluster())
	names, _, _ := unstructured.NestedStringSlice(obj.Object, "spec", "dnsNames")

	if !slices.Contains(names, "*.camus-peer.db.svc.cluster.local") {
		t.Errorf("dnsNames %v missing the per-pod wildcard", names)
	}
	for _, n := range names {
		if strings.HasPrefix(n, "camus-0.") {
			t.Errorf("certificate pins a per-ordinal name %q; a scale would reissue it", n)
		}
	}
	// The client Service is reached under its own names too.
	for _, w := range []string{"camus.db.svc.cluster.local", "camus"} {
		if !slices.Contains(names, w) {
			t.Errorf("dnsNames %v missing client name %q", names, w)
		}
	}
}

func TestCertificateCarriesIssuerAndLifetime(t *testing.T) {
	c := certManagerCluster()
	c.Spec.TLS.CertManager.Duration = &metav1.Duration{Duration: 720 * time.Hour}
	c.Spec.TLS.CertManager.RenewBefore = &metav1.Duration{Duration: 240 * time.Hour}

	obj := desiredCertificate(c)
	if kind, _, _ := unstructured.NestedString(obj.Object, "spec", "issuerRef", "kind"); kind != "ClusterIssuer" {
		t.Errorf("issuerRef.kind = %q, want ClusterIssuer", kind)
	}
	if d, _, _ := unstructured.NestedString(obj.Object, "spec", "duration"); d != "720h0m0s" {
		t.Errorf("duration = %q", d)
	}
	if d, _, _ := unstructured.NestedString(obj.Object, "spec", "renewBefore"); d != "240h0m0s" {
		t.Errorf("renewBefore = %q", d)
	}
}

// With cert-manager the pods mount the keystore the operator derives, under fixed keys.
func TestCertManagerKeystoreIsTheDerivedOne(t *testing.T) {
	c := certManagerCluster()
	if got := tlsSecretName(c); got != keystoreSecretName(c) {
		t.Errorf("tlsSecretName = %q, want %q", got, keystoreSecretName(c))
	}
	if keystoreKey(c) != derivedKeystoreKey || caKey(c) != derivedCAKey {
		t.Errorf("keys = %q/%q, want %q/%q", keystoreKey(c), caKey(c), derivedKeystoreKey, derivedCAKey)
	}
	want := tlsMountPath + "/" + derivedKeystoreKey
	if got := argValues(staticArgs(c), "--raft-certificate"); len(got) != 1 || got[0] != want {
		t.Errorf("--raft-certificate = %v, want [%s]", got, want)
	}
}

// A hand-supplied keystore keeps the user's own Secret and key names.
func TestSuppliedKeystoreIsUsedVerbatim(t *testing.T) {
	c := testCluster(3)
	c.Spec.TLS.KeystoreKey = "custom.pfx"
	c.Spec.TLS.CAKey = "root.pem"

	if got := keystoreKey(c); got != "custom.pfx" {
		t.Errorf("keystoreKey = %q, want custom.pfx", got)
	}
	if got := caKey(c); got != "root.pem" {
		t.Errorf("caKey = %q, want root.pem", got)
	}
	if got := tlsSecretName(c); got != testTLSSecret {
		t.Errorf("tlsSecretName = %q", got)
	}
}

// The keystore opens with an empty password — what CamusDB uses, since it takes none — and holds
// the issued key and leaf, for either key type cert-manager produces.
func TestBuildKeystoreOpensWithoutAPassword(t *testing.T) {
	for _, rsaKey := range []bool{true, false} {
		pki := newTestPKI(t, rsaKey)
		pfx, err := buildKeystore(pki.certPEM, pki.keyPEM)
		if err != nil {
			t.Fatalf("rsa=%v: %v", rsaKey, err)
		}
		key, leaf, chain, err := pkcs12.DecodeChain(pfx, "")
		if err != nil {
			t.Fatalf("rsa=%v: keystore does not open with an empty password: %v", rsaKey, err)
		}
		if !leaf.Equal(pki.leaf) {
			t.Errorf("rsa=%v: keystore leaf is not the issued certificate", rsaKey)
		}
		if len(chain) != 1 {
			t.Errorf("rsa=%v: chain has %d certificates, want the issuing CA", rsaKey, len(chain))
		}
		pub, ok := key.(crypto.Signer)
		if !ok {
			t.Fatalf("rsa=%v: keystore key is not a signer", rsaKey)
		}
		if !leaf.PublicKey.(interface{ Equal(crypto.PublicKey) bool }).Equal(pub.Public()) {
			t.Errorf("rsa=%v: keystore key does not match the leaf", rsaKey)
		}
	}
}

func fakeReconciler(t *testing.T, objs ...runtime.Object) *CamusDBClusterReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := camusdbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return &CamusDBClusterReconciler{Client: c, Scheme: scheme}
}

func issuedSecret(cluster *camusdbv1alpha1.CamusDBCluster, pki testPKI) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: certificateName(cluster), Namespace: cluster.Namespace},
		Data: map[string][]byte{
			certManagerCertKey: pki.certPEM, certManagerKeyKey: pki.keyPEM, certManagerCAKey: pki.caPEM,
		},
	}
}

// The derived keystore is rebuilt when cert-manager renews, and only then. A PKCS#12 encoding is
// salted, so a rebuild on every pass would change the keystore hash — and restart every node —
// on every reconcile.
func TestDerivedKeystoreFollowsRenewalsOnly(t *testing.T) {
	cluster := certManagerCluster()
	pki := newTestPKI(t, true)
	r := fakeReconciler(t, cluster, issuedSecret(cluster, pki))
	ctx := context.Background()
	key := types.NamespacedName{Name: keystoreSecretName(cluster), Namespace: cluster.Namespace}

	if err := r.reconcileDerivedKeystore(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	first := &corev1.Secret{}
	if err := r.Get(ctx, key, first); err != nil {
		t.Fatalf("derived keystore not created: %v", err)
	}
	if string(first.Data[derivedCAKey]) != string(pki.caPEM) {
		t.Error("derived keystore does not carry the issuing CA")
	}
	ready, msg, digest := r.keystoreState(ctx, cluster)
	if !ready || digest == "" {
		t.Fatalf("keystore not ready after derivation: %s", msg)
	}

	if err := r.reconcileDerivedKeystore(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if _, _, again := r.keystoreState(ctx, cluster); again != digest {
		t.Error("an unchanged certificate produced a new keystore; every pass would restart the nodes")
	}

	renewed := newTestPKI(t, true)
	issued := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: certificateName(cluster), Namespace: cluster.Namespace}, issued); err != nil {
		t.Fatal(err)
	}
	issued.Data = issuedSecret(cluster, renewed).Data
	if err := r.Update(ctx, issued); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileDerivedKeystore(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if _, _, after := r.keystoreState(ctx, cluster); after == digest {
		t.Error("a renewed certificate did not produce a new keystore; the nodes would keep the old one")
	}
}

// An issuer that writes no CA (ACME, for example) cannot work: the nodes verify each other against
// it. The condition says so instead of leaving pods to fail every peer call.
func TestKeystoreStateNamesAMissingCA(t *testing.T) {
	cluster := testCluster(3)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testTLSSecret, Namespace: cluster.Namespace},
		Data:       map[string][]byte{defaultKeystoreKey: []byte("pfx")},
	}
	r := fakeReconciler(t, cluster, secret)
	ready, msg, _ := r.keystoreState(context.Background(), cluster)
	if ready {
		t.Fatal("keystore reported ready without a CA")
	}
	if !strings.Contains(msg, "CA") {
		t.Errorf("message %q does not name the missing CA", msg)
	}
}
