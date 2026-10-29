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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Tuning fields are pointers throughout: an unset field means "do not write the setting" so the
// CamusDB server's own default applies. Mirroring defaults here would fork them silently the next
// time upstream changes one.

// StorageBackend selects a persistence backend of the embedded Kahuna engine.
// +kubebuilder:validation:Enum=rocksdb;sqlite;memory
type StorageBackend string

const (
	StorageBackendRocksDB StorageBackend = "rocksdb"
	StorageBackendSQLite  StorageBackend = "sqlite"
	StorageBackendMemory  StorageBackend = "memory"
)

// ClusterPhase is a coarse, human-oriented summary of what the operator is currently doing.
// Conditions carry the precise state; this exists for `kubectl get`.
// +kubebuilder:validation:Enum=Pending;Bootstrapping;Running;ScalingUp;ScalingDown;Upgrading;Degraded
type ClusterPhase string

const (
	PhasePending       ClusterPhase = "Pending"
	PhaseBootstrapping ClusterPhase = "Bootstrapping"
	PhaseRunning       ClusterPhase = "Running"
	PhaseScalingUp     ClusterPhase = "ScalingUp"
	PhaseScalingDown   ClusterPhase = "ScalingDown"
	PhaseUpgrading     ClusterPhase = "Upgrading"
	PhaseDegraded      ClusterPhase = "Degraded"
)

// Condition types set by the operator.
const (
	// ConditionAvailable is true when a quorum of Voters is ready and serving.
	ConditionAvailable = "Available"
	// ConditionProgressing is true while the operator is mid-bootstrap, mid-scale, or mid-upgrade.
	ConditionProgressing = "Progressing"
	// ConditionRosterConverged is true when the committed Raft roster matches the desired replica set.
	ConditionRosterConverged = "RosterConverged"
	// ConditionDegraded is true when the cluster cannot reach or hold its desired state.
	ConditionDegraded = "Degraded"
	// ConditionAutoscalingLimited is true when the autoscaler wants a size it is not allowed to
	// reach: a bound, an unreadable signal, or a replication factor the cluster cannot yet honour.
	ConditionAutoscalingLimited = "AutoscalingLimited"
	// ConditionBackupFailing is true when the latest scheduled backup failed, or the backup schedule
	// cannot be parsed. It stays false while backups succeed or wait for the cluster to settle.
	ConditionBackupFailing = "BackupFailing"
	// ConditionBackupVolumeReady is true when the backup claim is bound and every node can mount it.
	// While it is false because the claim is not bound, no pod can start, and the message names the
	// cause, such as a provisioner that cannot give ReadWriteMany.
	ConditionBackupVolumeReady = "BackupVolumeReady"
)

// PVCRetentionPolicy controls what happens to a node's PersistentVolumeClaims when its pod is
// removed by a scale-down.
// +kubebuilder:validation:Enum=Retain;Delete
type PVCRetentionPolicy string

const (
	// PVCRetentionRetain keeps the PVCs so a re-scale-up reuses the data (and stays above the
	// Raft compaction floor). This is the default: deleting a Raft node's data is not reversible.
	PVCRetentionRetain PVCRetentionPolicy = "Retain"
	// PVCRetentionDelete reclaims the volumes when the node is scaled away.
	PVCRetentionDelete PVCRetentionPolicy = "Delete"
)

// VolumeSpec describes one PersistentVolumeClaim template.
type VolumeSpec struct {
	// size is the requested capacity for the volume.
	// +required
	Size resource.Quantity `json:"size"`

	// storageClassName selects the StorageClass. Unset uses the cluster default.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// accessModes for the claim. Defaults to ReadWriteOnce.
	// +optional
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
}

// StorageSpec configures persistence and the per-node volumes. The backend fields are immutable:
// changing either of them on a live cluster makes existing data unreadable.
type StorageSpec struct {
	// backend is the key/value persistence backend (kahuna.storage). Immutable.
	// +kubebuilder:default=rocksdb
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storage.backend is immutable"
	// +optional
	Backend StorageBackend `json:"backend,omitempty"`

	// walBackend is the Raft write-ahead-log backend (kahuna.wal_storage). Immutable.
	// +kubebuilder:default=rocksdb
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storage.walBackend is immutable"
	// +optional
	WALBackend StorageBackend `json:"walBackend,omitempty"`

	// data is the PVC template for the node's data directory. The key/value store, the WAL (unless
	// wal is set) and the query spill files all live on it.
	// +required
	Data VolumeSpec `json:"data"`

	// wal is an optional separate PVC template for the Raft WAL. Give the WAL its own (fast)
	// volume when write latency matters — WAL writes are fsync-bound. When omitted the WAL
	// lives in a subdirectory of the data volume.
	// +optional
	WAL *VolumeSpec `json:"wal,omitempty"`

	// syncWrites enables durable synchronous WAL writes (kahuna.wal_sync_writes). Defaults to the
	// server default, which is true. Setting it false is faster, but a node can lose recently
	// acknowledged writes on an unclean shutdown. Do not disable in production.
	// +optional
	SyncWrites *bool `json:"syncWrites,omitempty"`

	// directReads enables RocksDB direct-I/O reads (kahuna.rocksdb_direct_reads). Set false on CSI
	// drivers that do not support O_DIRECT.
	// +optional
	DirectReads *bool `json:"directReads,omitempty"`

	// sharedMemory enables a single RocksDB block cache and write-buffer manager shared between
	// the key/value backend and the Raft WAL (kahuna.rocksdb_shared_memory). Requires both
	// backends to be rocksdb.
	// +optional
	SharedMemory *bool `json:"sharedMemory,omitempty"`

	// sharedMemoryBudgetMB is the total shared RocksDB block-cache budget in MiB
	// (kahuna.rocksdb_shared_memory_budget_mb).
	// +kubebuilder:validation:Minimum=1
	// +optional
	SharedMemoryBudgetMB *int32 `json:"sharedMemoryBudgetMB,omitempty"`

	// sharedMemtableBudgetMB is the shared memtable sub-budget in MiB
	// (kahuna.rocksdb_shared_memtable_budget_mb). Must be <= sharedMemoryBudgetMB.
	// +kubebuilder:validation:Minimum=1
	// +optional
	SharedMemtableBudgetMB *int32 `json:"sharedMemtableBudgetMB,omitempty"`

	// pvcRetentionPolicy decides whether a scaled-away node's volumes are kept or deleted.
	// +kubebuilder:default=Retain
	// +optional
	PVCRetentionPolicy PVCRetentionPolicy `json:"pvcRetentionPolicy,omitempty"`
}

// CertManagerIssuerRef points at a cert-manager Issuer or ClusterIssuer.
type CertManagerIssuerRef struct {
	// name of the issuer.
	// +required
	Name string `json:"name"`

	// kind of the issuer.
	// +kubebuilder:default=Issuer
	// +kubebuilder:validation:Enum=Issuer;ClusterIssuer
	// +optional
	Kind string `json:"kind,omitempty"`

	// group of the issuer.
	// +kubebuilder:default="cert-manager.io"
	// +optional
	Group string `json:"group,omitempty"`
}

// CertManagerSpec asks the operator to provision the certificate via cert-manager.
//
// The issuer must be a CA or self-signed issuer: the issued Secret has to carry the issuing CA in
// ca.crt, because the nodes verify each other against it.
type CertManagerSpec struct {
	// issuerRef selects the issuer that signs the certificate.
	// +required
	IssuerRef CertManagerIssuerRef `json:"issuerRef"`

	// duration of the issued certificate.
	// +optional
	Duration *metav1.Duration `json:"duration,omitempty"`

	// renewBefore is how long before expiry cert-manager renews.
	// +optional
	RenewBefore *metav1.Duration `json:"renewBefore,omitempty"`
}

// TLSSpec configures the certificate CamusDB serves on its Raft, gRPC and HTTPS listeners.
//
// TLS is not optional for a cluster. Nodes always reach each other over https:// and validate the
// peer's certificate — CamusDB has no switch to skip that — so every node needs a keystore and the
// CA that signed its peers' keystores. Supply exactly one of secretRef or certManager.
// +kubebuilder:validation:XValidation:rule="has(self.secretRef) != has(self.certManager)",message="exactly one of tls.secretRef or tls.certManager must be set"
type TLSSpec struct {
	// secretRef names an existing Secret holding a PKCS#12 keystore and the CA certificate that
	// signed it. The keystore must not be password protected: CamusDB opens it without one.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`

	// keystoreKey is the Secret key holding the PKCS#12 blob. Ignored with certManager, where the
	// operator builds the keystore itself.
	// +kubebuilder:default="certificate.pfx"
	// +optional
	KeystoreKey string `json:"keystoreKey,omitempty"`

	// caKey is the Secret key holding the PEM CA certificate(s) the nodes trust for each other's
	// keystores. With a self-signed keystore this is the certificate itself.
	// +kubebuilder:default="ca.crt"
	// +optional
	CAKey string `json:"caKey,omitempty"`

	// certManager provisions the certificate through cert-manager instead of using an existing Secret.
	// +optional
	CertManager *CertManagerSpec `json:"certManager,omitempty"`
}

// AuthSpec turns on CamusDB authentication.
//
// The operator needs it for anything beyond reading the roster: without authentication CamusDB
// accepts cluster administration and engine metrics from loopback callers only, so the operator
// cannot ask a node to leave the cluster or read its traffic.
//
// It also turns on mutual TLS between the nodes, because Raft and Kahuna traffic carries no user
// token: each node presents its keystore as a client certificate, so a keystore from tls.secretRef
// must allow client authentication. This needs CamusDB 0.15.2 or later; 0.15.1 rejects the
// setting.
type AuthSpec struct {
	// adminSecretRef names a Secret with "username" and "password" keys. The account is created as
	// the bootstrap superuser when the user catalog is empty, and the operator logs in with it.
	// When omitted the operator generates one in <name>-admin.
	//
	// The bootstrap applies to an empty catalog only. If the account is changed later with
	// ALTER USER, this Secret must be updated to match, or the operator cannot log in.
	// +optional
	AdminSecretRef *corev1.LocalObjectReference `json:"adminSecretRef,omitempty"`
}

// PortsSpec overrides the container ports.
type PortsSpec struct {
	// http is the plaintext REST port (--http-port). Probes use it.
	// +kubebuilder:default=5095
	// +optional
	HTTP int32 `json:"http,omitempty"`

	// https is the TLS REST port (--https-port). The operator uses it when authentication is on,
	// because CamusDB refuses credentials over plaintext.
	// +kubebuilder:default=7141
	// +optional
	HTTPS int32 `json:"https,omitempty"`

	// grpc is the client gRPC port (grpc_port).
	// +kubebuilder:default=5096
	// +optional
	GRPC int32 `json:"grpc,omitempty"`

	// raft is the inter-node Raft port (--raft-port).
	// +kubebuilder:default=7070
	// +optional
	Raft int32 `json:"raft,omitempty"`
}

// RaftSpec exposes the consensus timing knobs. Everything else on the Raft surface that CamusDB
// passes through is reachable through config.kahuna.
type RaftSpec struct {
	// heartbeatIntervalMs is the leader heartbeat interval (kahuna.heartbeat_interval_ms).
	// +optional
	HeartbeatIntervalMs *int32 `json:"heartbeatIntervalMs,omitempty"`

	// votingTimeoutMs is how long a vote round waits (kahuna.voting_timeout_ms).
	// +optional
	VotingTimeoutMs *int32 `json:"votingTimeoutMs,omitempty"`

	// startElectionTimeoutMs is the minimum election timeout (kahuna.start_election_timeout_ms).
	// +optional
	StartElectionTimeoutMs *int32 `json:"startElectionTimeoutMs,omitempty"`

	// endElectionTimeoutMs is the maximum election timeout (kahuna.end_election_timeout_ms).
	// +optional
	EndElectionTimeoutMs *int32 `json:"endElectionTimeoutMs,omitempty"`
}

// TransactionsSpec exposes the transaction timeout and admission-control knobs.
type TransactionsSpec struct {
	// defaultTimeoutMs for a transaction (kahuna.default_transaction_timeout_ms).
	// +optional
	DefaultTimeoutMs *int32 `json:"defaultTimeoutMs,omitempty"`

	// maxTimeoutMs is the largest timeout a transaction may ask for
	// (kahuna.max_transaction_timeout_ms).
	// +optional
	MaxTimeoutMs *int32 `json:"maxTimeoutMs,omitempty"`

	// defaultAdmissionWaitMs a caller queues for an admission slot (kahuna.default_admission_wait_ms).
	// +optional
	DefaultAdmissionWaitMs *int32 `json:"defaultAdmissionWaitMs,omitempty"`

	// maxAdmissionWaitMs is the hard upper bound on any admission wait (kahuna.max_admission_wait_ms).
	// +optional
	MaxAdmissionWaitMs *int32 `json:"maxAdmissionWaitMs,omitempty"`

	// maxConcurrentSessions is the ceiling on open interactive transaction sessions
	// (kahuna.max_concurrent_sessions). 0 means no limit.
	// +optional
	MaxConcurrentSessions *int32 `json:"maxConcurrentSessions,omitempty"`
}

// WorkersSpec sizes the per-subsystem actor worker pools of the embedded engine.
type WorkersSpec struct {
	// locks worker count (kahuna.locks_workers).
	// +optional
	Locks *int32 `json:"locks,omitempty"`

	// keyValue worker count (kahuna.key_value_workers).
	// +optional
	KeyValue *int32 `json:"keyValue,omitempty"`

	// backgroundWriter worker count (kahuna.background_writer_workers).
	// +optional
	BackgroundWriter *int32 `json:"backgroundWriter,omitempty"`
}

// ReplicationSpec caps how many nodes hold each data partition.
//
// Without it every node holds every partition, so each node added is one more copy of all the
// data and one more voter on every write — the cluster gets more expensive without getting more
// write capacity. With it each partition lives on `factor` nodes and the placement rebalancer
// spreads the partitions across the rest.
//
// With it the operator also raises the rebalancer's pacing (kahuna.max_concurrent_replica_transfers
// 4, kahuna.max_replica_moves_per_pass 8, kahuna.replica_count_deadband 0), so that a node added by
// a scale-up receives its share of the data in minutes. spec.config overrides any of them.
type ReplicationSpec struct {
	// factor is the number of voter replicas of each data partition (kahuna.replication_factor).
	// The system partition always spans every node regardless.
	//
	// A partition created while the cluster has no more nodes than the factor starts fully
	// replicated, and so does every partition of a cluster that ran without a factor. CamusDB
	// 0.16.0 and later convert such a partition to a replica set within a few rebalancer passes.
	// Older servers never convert one: a cluster that they created with 3 nodes and factor 3 stays
	// fully replicated.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3
	// +required
	Factor int32 `json:"factor"`
}

// AutoscalingSpec lets the operator choose the node count from traffic.
//
// The operator steps one node at a time, and only once the previous step is complete, so a scale
// never stacks a second membership change on top of one still in flight. Complete means that the
// roster has converged and that the replica placement has settled: no replica move is in flight, no
// replica is left on a node that departed, and every node carries a share of the data partitions.
// The share is waited for at most five minutes after a step. A signal must stay past its target for
// the whole stabilization window before a step is taken.
// +kubebuilder:validation:XValidation:rule="self.maxReplicas >= self.minReplicas",message="autoscaling.maxReplicas must be >= autoscaling.minReplicas"
// +kubebuilder:validation:XValidation:rule="has(self.targetLogEntriesPerSecond) || has(self.targetQueueDepth) || has(self.targetCPUPercent)",message="autoscaling needs at least one of targetLogEntriesPerSecond, targetQueueDepth or targetCPUPercent"
type AutoscalingSpec struct {
	// minReplicas is the floor. It must be at least 3: one node cannot grow into a cluster, and two
	// nodes tolerate no failures.
	// +kubebuilder:validation:Minimum=3
	// +required
	MinReplicas int32 `json:"minReplicas"`

	// maxReplicas is the ceiling.
	// +kubebuilder:validation:Minimum=3
	// +required
	MaxReplicas int32 `json:"maxReplicas"`

	// targetLogEntriesPerSecond is the write rate each node should carry on average, counted as the
	// Raft log entries its leader partitions dispatch (kahuna.kv.write.entries). Every kind of write
	// produces entries, and one transaction produces several — its prepare, decision and settle —
	// so calibrate it against a representative workload rather than a statement count.
	// +kubebuilder:validation:Minimum=1
	// +optional
	TargetLogEntriesPerSecond *int32 `json:"targetLogEntriesPerSecond,omitempty"`

	// targetQueueDepth is the number of client proposals each node should have waiting on average
	// across its partition executors (raft.executor.client_queue_depth).
	// +kubebuilder:validation:Minimum=1
	// +optional
	TargetQueueDepth *int32 `json:"targetQueueDepth,omitempty"`

	// targetCPUPercent is the CPU share of its node the server process should use on average, from
	// 1 to 100, measured over the last minute (process_cpu_percent of GET /v1/admin/stats). It is
	// the only signal that sees read load. It needs a server that serves /v1/admin/stats.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +optional
	TargetCPUPercent *int32 `json:"targetCPUPercent,omitempty"`

	// scaleUpStabilizationSeconds is how long load must stay above target before a node is added.
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=0
	// +optional
	ScaleUpStabilizationSeconds *int32 `json:"scaleUpStabilizationSeconds,omitempty"`

	// scaleDownStabilizationSeconds is how long load must stay below target before a node is
	// removed. Longer than the scale-up window on purpose: a removal costs a drain and a later
	// re-add costs a catch-up, so flapping is expensive.
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=0
	// +optional
	ScaleDownStabilizationSeconds *int32 `json:"scaleDownStabilizationSeconds,omitempty"`

	// tolerancePercent is the dead band around the target inside which no step is recommended.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	TolerancePercent *int32 `json:"tolerancePercent,omitempty"`
}

// BackupVolumeSpec selects the volume that holds the backup catalog. Every node mounts the same
// volume, because the catalog is whatever is in the backup directory: a node finds an incremental's
// parent, and lists backups, only from there. A volume per node would give each node a partial
// catalog, and an incremental taken on a node that did not take its parent would fail.
// +kubebuilder:validation:XValidation:rule="has(self.claimName) != has(self.claim)",message="exactly one of backup.volume.claimName or backup.volume.claim must be set"
type BackupVolumeSpec struct {
	// claimName names an existing PersistentVolumeClaim in the cluster's namespace. It must be
	// ReadWriteMany when the cluster has more than one node.
	// +kubebuilder:validation:MinLength=1
	// +optional
	ClaimName *string `json:"claimName,omitempty"`

	// claim asks the operator to create a PersistentVolumeClaim named <cluster>-backup. Access
	// modes default to ReadWriteMany, or to ReadWriteOnce for a single-node cluster, so the
	// StorageClass must support ReadWriteMany when the cluster has more than one node. Until the
	// claim binds, the operator replaces it when this spec changes. After it binds, the operator
	// does not change it. Deleting the CamusDBCluster, or removing spec.backup, does not delete the
	// claim.
	// +optional
	Claim *VolumeSpec `json:"claim,omitempty"`
}

// BackupRetentionSpec bounds how many backups the server keeps. The server deletes whole chains
// (a full backup and the incrementals on top of it), and always keeps the newest chain. A bound
// that is not set does not apply. With no bound set, the server keeps every backup.
type BackupRetentionSpec struct {
	// maxChains keeps at most this many of the newest chains (kahuna.backup_retention_max_chains).
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxChains *int32 `json:"maxChains,omitempty"`

	// maxAge deletes a chain when its newest backup is older than this
	// (kahuna.backup_retention_max_age_seconds). It is rounded down to whole seconds.
	// +optional
	MaxAge *metav1.Duration `json:"maxAge,omitempty"`

	// maxBytes keeps the newest chains whose combined size stays within this budget
	// (kahuna.backup_retention_max_bytes).
	// +optional
	MaxBytes *resource.Quantity `json:"maxBytes,omitempty"`
}

// BackupSpec turns on scheduled backups to a volume that every node shares.
//
// Full backups are coordinated: the server takes them at one cut time that is the same for every
// partition. Incremental backups add the log written since the latest backup. Under a replication
// factor no node needs to host every partition: CamusDB 0.16.0 and later compose the backup from
// each node's share of the partitions, so it covers every partition. An older server's backup
// holds only the partitions that the node taking it hosts, and restore and seeding refuse it, so
// the operator takes no backup when spec.image names a release before 0.16.0 and the cluster can
// have more nodes than the factor.
//
// Backups need spec.auth: without authentication CamusDB accepts backup calls from loopback only.
//
// Adding, removing or changing anything here except the schedules, suspend and timeout changes the
// generated config.yml, which rolls the cluster one node at a time.
type BackupSpec struct {
	// schedule is when to take a full backup, in cron format ("0 2 * * *", "@daily"). Times are UTC
	// unless the schedule starts with CRON_TZ=<zone>. The first full backup runs as soon as the
	// cluster is steady after backups are turned on.
	// +kubebuilder:validation:MinLength=1
	// +required
	Schedule string `json:"schedule"`

	// incrementalSchedule is when to take an incremental backup on top of the latest backup, in the
	// same format. Unset means full backups only. Each incremental must start within the server's
	// PITR window (kahuna.pitr_window_seconds, 1 hour by default) of its parent, or the server takes
	// a full backup in its place.
	// +optional
	IncrementalSchedule string `json:"incrementalSchedule,omitempty"`

	// suspend stops new scheduled backups. A backup already running continues.
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// timeout bounds one backup request. When it expires the operator cancels the request, and the
	// server stops the backup without publishing it.
	// +kubebuilder:default="1h"
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// volume is the shared volume that holds the backups. The server writes them under a
	// directory named after the cluster, so two clusters can share one claim.
	// +required
	Volume BackupVolumeSpec `json:"volume"`

	// clusterID is stamped into every backup (kahuna.backup_cluster_id). A node seeded from a backup
	// refuses a backup with a different id. Defaults to <namespace>/<name>. Set it explicitly to
	// keep the id when you restore the backups into a cluster with a different name.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	ClusterID string `json:"clusterID,omitempty"`

	// macKeySecretRef points at the key that signs every backup manifest
	// (kahuna.backup_mac_key_file). When unset, the operator generates a random key in a Secret
	// named <cluster>-backup-mac. That Secret is not deleted with the CamusDBCluster: without the
	// key the server refuses to restore or seed from the backups it signed.
	// +optional
	MACKeySecretRef *corev1.SecretKeySelector `json:"macKeySecretRef,omitempty"`

	// retention bounds how many backups the server keeps.
	// +optional
	Retention *BackupRetentionSpec `json:"retention,omitempty"`
}

// CamusDBClusterSpec defines the desired state of a CamusDB cluster.
type CamusDBClusterSpec struct {
	// replicas is the number of CamusDB nodes. Raft needs an odd count for a clean majority;
	// 3 tolerates one failure, 5 tolerates two.
	//
	// 2 is rejected: a two-Voter cluster has a quorum of 2, so it tolerates no failures at all
	// while costing twice as much as a single node.
	//
	// 1 runs a one-member cluster. It is fine for development, but it is never grown into a real
	// cluster in place: the one member formed the roster by itself, and the operator does not
	// support converting it. Growing away from 1 is therefore rejected below.
	//
	// When autoscaling is set, replicas is only the starting size (clamped to the autoscaling
	// bounds); after that the autoscaler owns the node count and edits here are ignored.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// replication caps how many nodes hold each data partition. Required for autoscaling.
	// +optional
	Replication *ReplicationSpec `json:"replication,omitempty"`

	// autoscaling lets the operator add and remove nodes with traffic. Requires replication and auth.
	// +optional
	Autoscaling *AutoscalingSpec `json:"autoscaling,omitempty"`

	// auth turns on CamusDB authentication and gives the operator an account. Without it the
	// operator cannot decommission a node gracefully: a scale-down removes the pod and waits for
	// the cluster's failure detector to evict it.
	// +optional
	Auth *AuthSpec `json:"auth,omitempty"`

	// backup turns on scheduled backups. Unset means no backups. Requires auth.
	// +optional
	Backup *BackupSpec `json:"backup,omitempty"`

	// image is the CamusDB server image. The operator overrides the image entrypoint with its own
	// generated startup script, so the stock single-node image works unmodified.
	//
	// The default is pinned to an exact version rather than to a floating tag. A cluster whose
	// image moves on its own re-rolls every node on a registry push, and the generated command
	// line is version-specific: a flag the new server no longer accepts stops the node at
	// startup. Raise this default deliberately, once the flags are checked against that release.
	// +kubebuilder:default="camusdb/camusdb:0.16.3"
	// +optional
	Image string `json:"image,omitempty"`

	// imagePullPolicy for the CamusDB container.
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// imagePullSecrets for a private registry.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// partitions is the initial Raft partition count (--initial-cluster-partitions). Immutable:
	// it is fixed when the cluster is first bootstrapped and changing it later would orphan the
	// existing partition map.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="partitions is immutable once the cluster is created"
	// +optional
	Partitions *int32 `json:"partitions,omitempty"`

	// storage configures persistence and the per-node volumes.
	// +required
	Storage StorageSpec `json:"storage"`

	// tls configures the certificate used for the Raft, gRPC and HTTPS listeners.
	// +required
	TLS TLSSpec `json:"tls"`

	// ports overrides the container port assignments.
	// +optional
	Ports *PortsSpec `json:"ports,omitempty"`

	// raft tunes consensus timing.
	// +optional
	Raft *RaftSpec `json:"raft,omitempty"`

	// transactions tunes transaction timeouts and admission control.
	// +optional
	Transactions *TransactionsSpec `json:"transactions,omitempty"`

	// workers sizes the per-subsystem actor worker pools.
	// +optional
	Workers *WorkersSpec `json:"workers,omitempty"`

	// config is merged over the config.yml the operator generates, key by key, and wins on a
	// conflict. It is the escape hatch for every CamusDB setting without a typed field here, in
	// the server's own snake_case YAML names — for example
	// {"slow_query_log_enabled": true, "kahuna": {"backup_dir": "/storage/data/backups"}}.
	// Nothing validates it here; the server rejects an unknown key at startup.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:validation:Type=object
	// +optional
	Config *runtime.RawExtension `json:"config,omitempty"`

	// logLevel for the CamusDB log categories without a more specific rule (CAMUS_LOG_LEVEL). The
	// embedded Kahuna, Kommander and gRPC categories keep the server's own default; set them
	// through env.
	// +kubebuilder:default="Information"
	// +kubebuilder:validation:Enum=Trace;Debug;Information;Warning;Error;Critical;None
	// +optional
	LogLevel string `json:"logLevel,omitempty"`

	// env is added to the CamusDB container after the variables the operator sets, for example
	// CAMUS_LOG_LEVEL_KAHUNA or DOTNET_GCHeapHardLimitPercent.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// extraArgs are appended verbatim to the server command line, after every flag the operator
	// generates. Nothing validates them, and the server's parser rejects an unknown flag, so a
	// bad value fails the pod at startup.
	// +optional
	ExtraArgs []string `json:"extraArgs,omitempty"`

	// resources for the CamusDB container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// nodeSelector for the CamusDB pods.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// tolerations for the CamusDB pods.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// affinity for the CamusDB pods. When unset the operator applies a soft anti-affinity that
	// prefers spreading nodes across hosts.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// topologySpreadConstraints for the CamusDB pods.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// podAnnotations added to every CamusDB pod.
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// podLabels added to every CamusDB pod.
	// +optional
	PodLabels map[string]string `json:"podLabels,omitempty"`

	// serviceAccountName for the CamusDB pods. The pods make no Kubernetes API calls; this exists
	// for image-pull and admission-policy purposes.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// podSecurityContext for the CamusDB pods.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// securityContext for the CamusDB container.
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`

	// terminationGracePeriodSeconds for the CamusDB pods.
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`

	// podDisruptionBudget creates a PDB allowing at most one unavailable node. Defaults to true
	// when replicas >= 3.
	// +optional
	PodDisruptionBudget *bool `json:"podDisruptionBudget,omitempty"`
}

// MemberStatus is one entry of the committed Raft roster, joined to the pod that serves it.
type MemberStatus struct {
	// podName serving this member, derived from the endpoint. Empty if the endpoint does not
	// map to a pod this cluster owns — which is how an orphaned roster entry shows up.
	// +optional
	PodName string `json:"podName,omitempty"`

	// endpoint is the member's Raft address as recorded in the roster.
	// +required
	Endpoint string `json:"endpoint"`

	// nodeID is the member's stable Raft node id.
	// +optional
	NodeID int32 `json:"nodeID,omitempty"`

	// role is Voter, Learner, Leaving or NotMember. Only Voters count toward quorum.
	// +optional
	Role string `json:"role,omitempty"`

	// joinedVersion is the membership version at which this member was admitted.
	// +optional
	JoinedVersion int64 `json:"joinedVersion,omitempty"`

	// ready reports whether the member's pod passes /v1/cluster/health.
	// +optional
	Ready bool `json:"ready,omitempty"`
}

// AutoscalingStatus is the autoscaler's memory between reconciles. It lives in status rather than in
// the operator's process so a restart of the operator does not reset a stabilization window or forget
// the size it chose.
type AutoscalingStatus struct {
	// desiredReplicas is the node count the autoscaler has chosen. It replaces spec.replicas as the
	// target while autoscaling is enabled.
	// +optional
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`

	// recommendedReplicas is what the latest signals ask for, before bounds, stabilization and the
	// one-node-at-a-time rule are applied.
	// +optional
	RecommendedReplicas int32 `json:"recommendedReplicas,omitempty"`

	// logEntriesPerSecondPerNode is the latest measured average Raft log entry rate per node.
	// +optional
	LogEntriesPerSecondPerNode *int64 `json:"logEntriesPerSecondPerNode,omitempty"`

	// queueDepthPerNode is the latest measured average client queue depth per node.
	// +optional
	QueueDepthPerNode *int64 `json:"queueDepthPerNode,omitempty"`

	// cpuPercentPerNode is the latest measured average process CPU share per node.
	// +optional
	CPUPercentPerNode *int64 `json:"cpuPercentPerNode,omitempty"`

	// aboveTargetSince is when the recommendation first rose above desiredReplicas without
	// interruption. Cleared whenever it drops back or a step is taken.
	// +optional
	AboveTargetSince *metav1.Time `json:"aboveTargetSince,omitempty"`

	// belowTargetSince is when the recommendation first fell below desiredReplicas without
	// interruption. Cleared whenever it rises back or a step is taken.
	// +optional
	BelowTargetSince *metav1.Time `json:"belowTargetSince,omitempty"`

	// lastScaleTime is when the autoscaler last changed desiredReplicas.
	// +optional
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`
}

// BackupStatus is the backup scheduler's memory between reconciles. A schedule time advances only
// when its backup succeeds, so a failed backup stays due and is retried with a backoff.
type BackupStatus struct {
	// lastFullScheduleTime is the scheduled time of the latest full backup that succeeded. The next
	// full backup is due at the first schedule time after it.
	// +optional
	LastFullScheduleTime *metav1.Time `json:"lastFullScheduleTime,omitempty"`

	// lastIncrementalScheduleTime is the scheduled time of the latest incremental backup that
	// succeeded, or the time of the latest full backup when that is later.
	// +optional
	LastIncrementalScheduleTime *metav1.Time `json:"lastIncrementalScheduleTime,omitempty"`

	// lastSuccessfulTime is when the latest backup of either type completed.
	// +optional
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty"`

	// lastBackupID is the id of the latest backup. The next incremental uses it as its parent.
	// +optional
	LastBackupID string `json:"lastBackupID,omitempty"`

	// lastBackupType is Full or Incremental, as the server took it. The server takes a full backup
	// in place of an incremental one when the incremental cannot continue the chain.
	// +optional
	LastBackupType string `json:"lastBackupType,omitempty"`

	// consecutiveFailures counts the backups that failed since the latest success.
	// +optional
	ConsecutiveFailures int32 `json:"consecutiveFailures,omitempty"`

	// lastFailureTime is when the latest failed backup ended.
	// +optional
	LastFailureTime *metav1.Time `json:"lastFailureTime,omitempty"`

	// lastFailureMessage is why the latest failed backup failed.
	// +optional
	LastFailureMessage string `json:"lastFailureMessage,omitempty"`

	// unclaimedSince is when the first of the current run of unclaimed attempts ended. An attempt
	// is unclaimed when no node took it and every node refused with an outcome that clears on its
	// own: the meta partition has no settled leader yet, leadership moved during the backup, or the
	// topology moved during the backup. The first is what a cluster looks like right after
	// bootstrap. An unclaimed attempt is retried soon and is not counted as a failure, until the
	// run lasts longer than a bound. Cleared by any other result.
	// +optional
	UnclaimedSince *metav1.Time `json:"unclaimedSince,omitempty"`

	// lastUnclaimedTime is when the latest unclaimed attempt ended. The next attempt waits a short
	// delay after it.
	// +optional
	LastUnclaimedTime *metav1.Time `json:"lastUnclaimedTime,omitempty"`
}

// CamusDBClusterStatus defines the observed state of a CamusDB cluster.
type CamusDBClusterStatus struct {
	// observedGeneration is the spec generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// phase is a coarse summary of what the operator is doing. Conditions carry the detail.
	// +optional
	Phase ClusterPhase `json:"phase,omitempty"`

	// replicas is the number of pods the StatefulSet currently has.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// readyReplicas is the number of pods passing the readiness probe.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// voters is the number of Voters in the committed roster. Quorum is a majority of this.
	// +optional
	Voters int32 `json:"voters,omitempty"`

	// learners is the number of members still catching up.
	// +optional
	Learners int32 `json:"learners,omitempty"`

	// membershipVersion is the committed roster version. It advances on every admitted change.
	// +optional
	MembershipVersion int64 `json:"membershipVersion,omitempty"`

	// members is the committed roster as last read from the cluster.
	// +optional
	// +listType=map
	// +listMapKey=endpoint
	Members []MemberStatus `json:"members,omitempty"`

	// bootstrapped records that the initial roster was seeded and every founding node was
	// admitted as a Voter.
	// +optional
	Bootstrapped bool `json:"bootstrapped,omitempty"`

	// bootstrapSize is the replica count the cluster was first created at. It is pinned when the
	// StatefulSet is created and never changes, because it is what tells a starting node whether
	// it is one of the founding members — which form the cluster through static discovery — or a
	// node added later, which must join the running cluster as a Learner instead.
	// +optional
	BootstrapSize int32 `json:"bootstrapSize,omitempty"`

	// currentImage is the image that every node runs. It changes to spec.image only when every
	// node runs that image and is ready. During an image rollout, and when a rollout is stuck,
	// it keeps the image that the cluster ran before.
	// +optional
	CurrentImage string `json:"currentImage,omitempty"`

	// labelSelector is the serialized pod selector, required by the scale subresource.
	// +optional
	LabelSelector string `json:"labelSelector,omitempty"`

	// updatePartition is the StatefulSet rolling-update partition the operator is currently
	// holding. Nodes at or above this ordinal have been updated.
	// +optional
	UpdatePartition *int32 `json:"updatePartition,omitempty"`

	// autoscaling is the autoscaler's state. Present only while spec.autoscaling is set.
	// +optional
	Autoscaling *AutoscalingStatus `json:"autoscaling,omitempty"`

	// backup is the backup scheduler's state. Present only while spec.backup is set.
	// +optional
	Backup *BackupStatus `json:"backup,omitempty"`

	// conditions represent the current state of the CamusDBCluster resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.labelSelector
// +kubebuilder:validation:XValidation:rule="self.spec.replicas != 2",message="replicas must not be 2: a two-Voter Raft cluster has a quorum of 2 and therefore tolerates no failures; use 1 or 3"
// +kubebuilder:validation:XValidation:rule="(oldSelf.spec.replicas == 1) == (self.spec.replicas == 1)",message="a single-node cluster cannot be scaled into a multi-node cluster, or vice versa. Create a new CamusDBCluster and migrate the data."
// +kubebuilder:validation:XValidation:rule="!has(self.spec.autoscaling) || has(self.spec.replication)",message="autoscaling requires spec.replication: without a replication factor every node holds every partition, so adding nodes adds copies rather than capacity"
// +kubebuilder:validation:XValidation:rule="!has(self.spec.autoscaling) || has(self.spec.auth)",message="autoscaling requires spec.auth: without authentication CamusDB serves its engine metrics and the leave call to loopback callers only"
// +kubebuilder:validation:XValidation:rule="!has(self.spec.backup) || has(self.spec.auth)",message="backup requires spec.auth: without authentication CamusDB accepts backup calls from loopback callers only"
// +kubebuilder:resource:shortName=camusdb;cdb
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Autoscaled",type=integer,JSONPath=`.status.autoscaling.desiredReplicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Voters",type=integer,JSONPath=`.status.voters`
// +kubebuilder:printcolumn:name="Roster",type=integer,JSONPath=`.status.membershipVersion`,priority=1
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Last Backup",type=date,JSONPath=`.status.backup.lastSuccessfulTime`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CamusDBCluster is the Schema for the camusdbclusters API
type CamusDBCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of CamusDBCluster
	// +required
	Spec CamusDBClusterSpec `json:"spec"`

	// status defines the observed state of CamusDBCluster
	// +optional
	Status CamusDBClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// CamusDBClusterList contains a list of CamusDBCluster
type CamusDBClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []CamusDBCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &CamusDBCluster{}, &CamusDBClusterList{})
		return nil
	})
}
