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
	"fmt"
	"maps"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	camusdbv1alpha1 "github.com/camusdb/k8s-operator/api/v1alpha1"
)

// Mount paths and volume names used by the generated pod spec.
const (
	// storageMountPath is where the data PVC is mounted.
	storageMountPath = "/storage"
	// dataPath is the server's data_dir. It is a subdirectory rather than the mount root so that
	// the filesystem's own entries (lost+found) never sit inside it. CamusDB keeps the key/value
	// store in kv/ and the Raft log in wal/ under it, plus query spill files in tmp/.
	dataPath = storageMountPath + "/data"
	// kvPath holds the key/value store. Whether it has any content is how a starting node tells a
	// fresh volume from one that already belongs to a member.
	kvPath = dataPath + "/kv"
	// walPath holds the Raft write-ahead log. CamusDB derives it from data_dir and has no setting
	// to move it, so a dedicated WAL volume is mounted here, nested inside the data volume's mount.
	walPath = dataPath + "/wal"

	configMountPath = "/etc/camusdb"
	tlsMountPath    = "/etc/camusdb/tls"
	// trustMountPath is a writable scratch volume for the CA bundle the startup script assembles.
	// It is not on the root filesystem so that a read-only root filesystem still works.
	trustMountPath = "/etc/camusdb/trust"

	entrypointFileName = "entrypoint.sh"
	entrypointPath     = configMountPath + "/" + entrypointFileName

	// serverConfigFileName is the config.yml the operator generates. It is passed with --config.
	serverConfigFileName = "config.yml"
	serverConfigPath     = configMountPath + "/" + serverConfigFileName

	// topologyFileName holds the cluster size and bootstrap size. It is a separate key so that
	// scaling does not alter the pod template — see topologyEnv.
	topologyFileName = "topology.env"
	topologyPath     = configMountPath + "/" + topologyFileName

	// backupMountPath is where the shared backup volume is mounted. The server's backup directory
	// is a subdirectory named after the cluster; see backupPath.
	backupMountPath = "/backup"
	// backupMACMountPath holds the key that signs backup manifests. It is outside the backup
	// directory, as the server requires.
	backupMACMountPath = "/etc/camusdb/backup-mac"
	backupMACFileName  = "key"
	backupMACKeyPath   = backupMACMountPath + "/" + backupMACFileName

	dataVolumeName      = "data"
	walVolumeName       = "wal"
	configVolumeName    = "config"
	tlsVolumeName       = "tls"
	trustVolumeName     = "trust"
	backupVolumeName    = "backup"
	backupMACVolumeName = "backup-mac"

	containerName = "camusdb"

	// appName is the app.kubernetes.io/name every owned object carries.
	appName = "camusdb"

	// Port names, shared by the container and both Services.
	portNameHTTP  = "http"
	portNameHTTPS = "https"
	portNameGRPC  = "grpc"
	portNameRaft  = "raft"

	// configHashAnnotation carries a digest of the generated startup script and server config. It
	// lives on the pod template so that a config change produces a new template revision — a
	// ConfigMap edit alone would otherwise never reach a running pod.
	configHashAnnotation = "camusdb.io/config-hash"

	// keystoreHashAnnotation carries a digest of the keystore and CA the pods mount. CamusDB reads
	// its certificate once at startup, so a renewed certificate reaches a node only through a
	// restart; changing the template is what makes the operator roll one.
	keystoreHashAnnotation = "camusdb.io/keystore-hash"
)

// stsName is the StatefulSet name, and therefore the prefix of every pod name.
func stsName(cluster *camusdbv1alpha1.CamusDBCluster) string { return cluster.Name }

// peerServiceName is the headless Service that gives each pod a stable DNS name.
func peerServiceName(cluster *camusdbv1alpha1.CamusDBCluster) string { return cluster.Name + "-peer" }

// clientServiceName is the load-balanced Service clients connect to.
func clientServiceName(cluster *camusdbv1alpha1.CamusDBCluster) string { return cluster.Name }

// configMapName holds the generated startup script and server config.
func configMapName(cluster *camusdbv1alpha1.CamusDBCluster) string { return cluster.Name + "-config" }

// pdbName is the PodDisruptionBudget guarding voluntary evictions.
func pdbName(cluster *camusdbv1alpha1.CamusDBCluster) string { return cluster.Name }

// certificateName is the cert-manager Certificate and the Secret it writes.
func certificateName(cluster *camusdbv1alpha1.CamusDBCluster) string { return cluster.Name + "-tls" }

// keystoreSecretName is the Secret the operator derives from the cert-manager output: a PKCS#12
// keystore CamusDB can open without a password, plus the CA.
func keystoreSecretName(cluster *camusdbv1alpha1.CamusDBCluster) string {
	return cluster.Name + "-keystore"
}

// adminSecretName is the generated Secret holding the operator's superuser credentials.
func adminSecretName(cluster *camusdbv1alpha1.CamusDBCluster) string { return cluster.Name + "-admin" }

// authKeysSecretName is the generated Secret holding the token key and node secret every node shares.
func authKeysSecretName(cluster *camusdbv1alpha1.CamusDBCluster) string {
	return cluster.Name + "-auth-keys"
}

// backupClaimName is the PersistentVolumeClaim holding the backups: the user's own claim, or the
// one the operator creates from spec.backup.volume.claim.
func backupClaimName(cluster *camusdbv1alpha1.CamusDBCluster) string {
	if v := cluster.Spec.Backup.Volume; v.ClaimName != nil {
		return *v.ClaimName
	}
	return cluster.Name + "-backup"
}

// backupMACSecretName is the Secret the operator generates when no MAC key is supplied.
func backupMACSecretName(cluster *camusdbv1alpha1.CamusDBCluster) string {
	return cluster.Name + "-backup-mac"
}

// backupPath is the server's backup directory. It is a subdirectory of the mount, not the mount
// itself, for two reasons. Two clusters can then share one claim. And the server refuses a backup
// directory that is group-writable, which the root of a volume often is (an fsGroup makes it so);
// a subdirectory that does not exist yet is created by the server with mode 0700.
func backupPath(cluster *camusdbv1alpha1.CamusDBCluster) string {
	return backupMountPath + "/" + cluster.Name
}

// backupClusterID is the id stamped into every backup.
func backupClusterID(cluster *camusdbv1alpha1.CamusDBCluster) string {
	if id := cluster.Spec.Backup.ClusterID; id != "" {
		return id
	}
	return cluster.Namespace + "/" + cluster.Name
}

// podName returns the pod name for an ordinal. StatefulSet pod names are stable, which is what
// makes the ordinal usable as a durable Raft node identity.
func podName(cluster *camusdbv1alpha1.CamusDBCluster, ordinal int32) string {
	return fmt.Sprintf("%s-%d", stsName(cluster), ordinal)
}

// peerDomain is the DNS suffix shared by every pod of the cluster.
func peerDomain(cluster *camusdbv1alpha1.CamusDBCluster) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", peerServiceName(cluster), cluster.Namespace)
}

// podFQDN is a pod's stable per-pod DNS name, served by the headless Service.
func podFQDN(cluster *camusdbv1alpha1.CamusDBCluster, ordinal int32) string {
	return fmt.Sprintf("%s.%s", podName(cluster, ordinal), peerDomain(cluster))
}

// raftEndpoint is the address a node registers in the Raft roster: its per-pod DNS name and the
// Raft port. Roster entries are matched back to pods by this string.
func raftEndpoint(cluster *camusdbv1alpha1.CamusDBCluster, ordinal int32) string {
	return fmt.Sprintf("%s:%d", podFQDN(cluster, ordinal), ports(cluster).Raft)
}

// nodeIDForOrdinal maps a StatefulSet ordinal to a Raft node id.
//
// The +1 keeps ids positive: Kommander records a provisional 0 for a member whose id it has not
// learned, so a real 0 would be indistinguishable from an unknown one.
func nodeIDForOrdinal(ordinal int32) int32 { return ordinal + 1 }

// labels are the selector labels — the immutable subset. A StatefulSet's selector cannot be
// changed after creation, so nothing that varies with the spec may appear here.
func selectorLabels(cluster *camusdbv1alpha1.CamusDBCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     appName,
		"app.kubernetes.io/instance": cluster.Name,
	}
}

// podLabels are the selector labels plus descriptive labels and any user-supplied extras.
func podLabels(cluster *camusdbv1alpha1.CamusDBCluster) map[string]string {
	out := map[string]string{
		"app.kubernetes.io/component":  "server",
		"app.kubernetes.io/managed-by": "camusdb-operator",
	}
	maps.Copy(out, cluster.Spec.PodLabels)
	// Selector labels are applied last so a user-supplied label cannot break the selector match.
	maps.Copy(out, selectorLabels(cluster))
	return out
}

// selector is the label selector shared by the StatefulSet, Services and PDB.
func selector(cluster *camusdbv1alpha1.CamusDBCluster) *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: selectorLabels(cluster)}
}

// ports resolves the port configuration, filling in defaults for an omitted block. The CRD
// defaults cover a partially specified block; this covers the block being absent entirely.
func ports(cluster *camusdbv1alpha1.CamusDBCluster) camusdbv1alpha1.PortsSpec {
	p := camusdbv1alpha1.PortsSpec{HTTP: 5095, HTTPS: 7141, GRPC: 5096, Raft: 7070}
	if cluster.Spec.Ports == nil {
		return p
	}
	if cluster.Spec.Ports.HTTP != 0 {
		p.HTTP = cluster.Spec.Ports.HTTP
	}
	if cluster.Spec.Ports.HTTPS != 0 {
		p.HTTPS = cluster.Spec.Ports.HTTPS
	}
	if cluster.Spec.Ports.GRPC != 0 {
		p.GRPC = cluster.Spec.Ports.GRPC
	}
	if cluster.Spec.Ports.Raft != 0 {
		p.Raft = cluster.Spec.Ports.Raft
	}
	return p
}

// desiredReplicas is the node count the cluster is being driven toward. Under autoscaling that is
// the autoscaler's choice, not spec.replicas: everything that sizes the cluster — the StatefulSet,
// the topology file, the PDB, status — must agree on one number, and only one actor may own it.
func desiredReplicas(cluster *camusdbv1alpha1.CamusDBCluster) int32 {
	if cluster.Spec.Autoscaling != nil {
		if as := cluster.Status.Autoscaling; as != nil && as.DesiredReplicas > 0 {
			return as.DesiredReplicas
		}
		return initialAutoscaledReplicas(cluster)
	}
	return specReplicas(cluster)
}

// specReplicas resolves spec.replicas, defaulting to 3.
func specReplicas(cluster *camusdbv1alpha1.CamusDBCluster) int32 {
	if cluster.Spec.Replicas == nil {
		return 3
	}
	return *cluster.Spec.Replicas
}

// singleNode reports whether the cluster has one node and always will. The CRD refuses to scale a
// single-node cluster into a multi-node one, and autoscaling starts at 3 nodes.
func singleNode(cluster *camusdbv1alpha1.CamusDBCluster) bool {
	return cluster.Spec.Autoscaling == nil && specReplicas(cluster) == 1
}

// initialAutoscaledReplicas is where the autoscaler starts before it has chosen anything:
// spec.replicas, held inside the autoscaling bounds.
func initialAutoscaledReplicas(cluster *camusdbv1alpha1.CamusDBCluster) int32 {
	as := cluster.Spec.Autoscaling
	return min(max(specReplicas(cluster), as.MinReplicas), as.MaxReplicas)
}
