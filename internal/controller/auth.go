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
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	camusdbv1alpha1 "github.com/camusdb/k8s-operator/api/v1alpha1"
)

// Keys of the admin Secret, whether supplied or generated.
const (
	adminUsernameKey = "username"
	adminPasswordKey = "password"
)

// Keys of the generated Secret every node shares.
const (
	tokenKeyKey   = "token-key"
	nodeSecretKey = "node-secret"
)

// generatedAdminUser is the account name the operator creates when no adminSecretRef is given.
const generatedAdminUser = "admin"

// adminSecretRefName resolves the Secret holding the operator's account.
func adminSecretRefName(cluster *camusdbv1alpha1.CamusDBCluster) string {
	if a := cluster.Spec.Auth; a != nil && a.AdminSecretRef != nil {
		return a.AdminSecretRef.Name
	}
	return adminSecretName(cluster)
}

// randomHex returns n random bytes as hex. CamusDB requires its token key and node secret to be at
// least 32 bytes long, and counts the characters of the value, so 32 random bytes as hex leave a
// wide margin.
func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// ensureSecret creates a Secret once and never rewrites it. Every value managed this way is baked
// into state that outlives the pods: the token key into issued tokens, the node secret into the
// peers' handshake, the admin password into the user catalog. Regenerating one would lock the
// operator, or the nodes, out.
func (r *CamusDBClusterReconciler) ensureSecret(ctx context.Context, cluster *camusdbv1alpha1.CamusDBCluster, name string, generate func() (map[string][]byte, error)) error {
	existing := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	data, err := generate()
	if err != nil {
		return err
	}
	secret := &corev1.Secret{
		ObjectMeta: objectMeta(cluster, name),
		Type:       corev1.SecretTypeOpaque,
		Data:       data,
	}
	if err := controllerutil.SetControllerReference(cluster, secret, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// reconcileAuthSecrets creates the shared keys and, unless the user supplied one, the admin account.
func (r *CamusDBClusterReconciler) reconcileAuthSecrets(ctx context.Context, cluster *camusdbv1alpha1.CamusDBCluster) error {
	if cluster.Spec.Auth == nil {
		return nil
	}

	// One token key for the whole cluster: a node with a different key cannot verify a token
	// another node issued, so requests would fail depending on which node they reached.
	if err := r.ensureSecret(ctx, cluster, authKeysSecretName(cluster), func() (map[string][]byte, error) {
		tokenKey, err := randomHex(32)
		if err != nil {
			return nil, fmt.Errorf("generating the token key: %w", err)
		}
		nodeSecret, err := randomHex(32)
		if err != nil {
			return nil, fmt.Errorf("generating the node secret: %w", err)
		}
		return map[string][]byte{tokenKeyKey: []byte(tokenKey), nodeSecretKey: []byte(nodeSecret)}, nil
	}); err != nil {
		return err
	}

	if cluster.Spec.Auth.AdminSecretRef != nil {
		// A user-supplied Secret belongs to the user.
		return nil
	}
	return r.ensureSecret(ctx, cluster, adminSecretName(cluster), func() (map[string][]byte, error) {
		buf := make([]byte, 24)
		if _, err := rand.Read(buf); err != nil {
			return nil, fmt.Errorf("generating the admin password: %w", err)
		}
		return map[string][]byte{
			adminUsernameKey: []byte(generatedAdminUser),
			adminPasswordKey: []byte(base64.RawURLEncoding.EncodeToString(buf)),
		}, nil
	})
}

// authEnv wires authentication into the server process. Every value comes from a Secret through a
// reference, so none of them appears in the pod spec or on the command line.
func authEnv(cluster *camusdbv1alpha1.CamusDBCluster) []corev1.EnvVar {
	if cluster.Spec.Auth == nil {
		return nil
	}
	ref := func(secret, key string) *corev1.EnvVarSource {
		return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: secret},
			Key:                  key,
		}}
	}
	keys := authKeysSecretName(cluster)
	admin := adminSecretRefName(cluster)
	return []corev1.EnvVar{
		{Name: "CAMUSDB_AUTH_ENABLED", Value: "true"},
		{Name: "CAMUSDB_AUTH_TOKEN_KEY", ValueFrom: ref(keys, tokenKeyKey)},
		// Guards the node-to-node routes. Without it a node with authentication on refuses them.
		{Name: "CAMUSDB_NODE_SECRET", ValueFrom: ref(keys, nodeSecretKey)},
		// Read only while the user catalog is empty, to create the first superuser.
		{Name: "CAMUSDB_BOOTSTRAP_USER", ValueFrom: ref(admin, adminUsernameKey)},
		{Name: "CAMUSDB_BOOTSTRAP_PASSWORD", ValueFrom: ref(admin, adminPasswordKey)},
	}
}
