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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"sigs.k8s.io/yaml"

	camusdbv1alpha1 "github.com/camusdb/k8s-operator/api/v1alpha1"
)

// serverDLLPath is where every published CamusDB image places the server assembly. The operator
// overrides the image's own entrypoint and invokes this directly, which is what lets the stock
// single-node image serve as a cluster member without modification.
const serverDLLPath = "/app/CamusDB.dll"

// systemCABundle is the Debian trust store of the .NET runtime images CamusDB is built on.
const systemCABundle = "/etc/ssl/certs/ca-certificates.crt"

// shellQuote renders s as a single-quoted POSIX shell word.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// setInt writes an integer setting when v is non-nil. Every tuning field is a pointer precisely so
// that an unset field writes nothing and the server's own default applies — restating upstream
// defaults here would fork them the next time one changes.
func setInt(m map[string]any, key string, v *int32) {
	if v != nil {
		m[key] = *v
	}
}

// setBool writes a boolean setting when v is non-nil.
func setBool(m map[string]any, key string, v *bool) {
	if v != nil {
		m[key] = *v
	}
}

// generatedConfig builds the config.yml settings that follow from typed spec fields.
//
// Settings go through the file rather than the command line because most of them have no flag:
// CamusDB exposes its embedded engine's options only under the kahuna: key of config.yml.
func generatedConfig(cluster *camusdbv1alpha1.CamusDBCluster) map[string]any {
	s := cluster.Spec.Storage
	kahuna := map[string]any{}

	backend := s.Backend
	if backend == "" {
		backend = camusdbv1alpha1.StorageBackendRocksDB
	}
	walBackend := s.WALBackend
	if walBackend == "" {
		walBackend = camusdbv1alpha1.StorageBackendRocksDB
	}
	kahuna["storage"] = string(backend)
	kahuna["wal_storage"] = string(walBackend)
	setBool(kahuna, "wal_sync_writes", s.SyncWrites)
	setBool(kahuna, "rocksdb_direct_reads", s.DirectReads)
	setBool(kahuna, "rocksdb_shared_memory", s.SharedMemory)
	setInt(kahuna, "rocksdb_shared_memory_budget_mb", s.SharedMemoryBudgetMB)
	setInt(kahuna, "rocksdb_shared_memtable_budget_mb", s.SharedMemtableBudgetMB)

	if rep := cluster.Spec.Replication; rep != nil {
		// The rebalancer comes with the factor, not as a separate knob. Initial placement happens
		// without it, but a node added later receives replicas only through rebalancing moves — so a
		// factor without the rebalancer means every scale-up adds a node that holds no data.
		kahuna["replication_factor"] = rep.Factor
		kahuna["enable_placement_rebalancer"] = true
	}

	if r := cluster.Spec.Raft; r != nil {
		setInt(kahuna, "heartbeat_interval_ms", r.HeartbeatIntervalMs)
		setInt(kahuna, "voting_timeout_ms", r.VotingTimeoutMs)
		setInt(kahuna, "start_election_timeout_ms", r.StartElectionTimeoutMs)
		setInt(kahuna, "end_election_timeout_ms", r.EndElectionTimeoutMs)
	}

	if t := cluster.Spec.Transactions; t != nil {
		setInt(kahuna, "default_transaction_timeout_ms", t.DefaultTimeoutMs)
		setInt(kahuna, "max_transaction_timeout_ms", t.MaxTimeoutMs)
		setInt(kahuna, "default_admission_wait_ms", t.DefaultAdmissionWaitMs)
		setInt(kahuna, "max_admission_wait_ms", t.MaxAdmissionWaitMs)
		setInt(kahuna, "max_concurrent_sessions", t.MaxConcurrentSessions)
	}

	if w := cluster.Spec.Workers; w != nil {
		setInt(kahuna, "locks_workers", w.Locks)
		setInt(kahuna, "key_value_workers", w.KeyValue)
		setInt(kahuna, "background_writer_workers", w.BackgroundWriter)
	}

	cfg := map[string]any{
		"grpc_port": ports(cluster).GRPC,
		"kahuna":    kahuna,
	}

	// With authentication on, peers prove who they are with a client certificate. Raft and Kahuna
	// traffic carries no user token, and CamusDB refuses to start an authenticated node that has peers
	// unless mutual TLS is on. The node keystore is the client certificate, and the CA already mounted
	// for outbound trust is the CA that peer certificates must chain to.
	if cluster.Spec.Auth != nil {
		cfg["raft_mutual_tls_enabled"] = true
		cfg["raft_peer_ca"] = tlsMountPath + "/" + caKey(cluster)
	}

	return cfg
}

// mergeConfig overlays src onto dst key by key. Nested maps merge; any other value in src replaces
// the one in dst, so spec.config always wins over a generated setting.
func mergeConfig(dst, src map[string]any) {
	for k, v := range src {
		if sv, ok := v.(map[string]any); ok {
			if dv, ok := dst[k].(map[string]any); ok {
				mergeConfig(dv, sv)
				continue
			}
		}
		dst[k] = v
	}
}

// serverConfig renders the config.yml every node starts with: the generated settings with
// spec.config merged over them.
func serverConfig(cluster *camusdbv1alpha1.CamusDBCluster) (string, error) {
	cfg := generatedConfig(cluster)

	if raw := cluster.Spec.Config; raw != nil && len(raw.Raw) > 0 {
		// UseNumber keeps an integer an integer: a plain decode would turn 500 into 500.0 by way of
		// float64, and a server that parses an int setting can reject the fraction.
		dec := json.NewDecoder(bytes.NewReader(raw.Raw))
		dec.UseNumber()
		var user map[string]any
		if err := dec.Decode(&user); err != nil {
			return "", fmt.Errorf("spec.config is not a JSON object: %w", err)
		}
		mergeConfig(cfg, user)
	}

	out, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("rendering config.yml: %w", err)
	}
	return "# Generated by camusdb-operator. Do not edit — the next reconcile overwrites it.\n" + string(out), nil
}

// staticArgs builds the portion of the command line that is identical on every node. The
// per-node identity flags (--raft-nodename/-nodeid/-host, --initial-cluster, --join-existing)
// are computed by the startup script from the pod's own ordinal, because a StatefulSet has a
// single pod template and cannot vary them per pod.
func staticArgs(cluster *camusdbv1alpha1.CamusDBCluster) []string {
	p := ports(cluster)

	partitions := int32(3)
	if cluster.Spec.Partitions != nil {
		partitions = *cluster.Spec.Partitions
	}

	keystore := tlsMountPath + "/" + keystoreKey(cluster)
	args := make([]string, 0, 16+len(cluster.Spec.ExtraArgs))
	args = append(args,
		"--config", serverConfigPath,
		// Explicit even for one node. An empty peer list is a legal cluster, and cluster mode is
		// what gives every size the same Raft listener, roster and endpoints.
		"--mode", "cluster",
		"--data-dir", dataPath,
		"--http-port", fmt.Sprintf("%d", p.HTTP),
		"--https-port", fmt.Sprintf("%d", p.HTTPS),
		// One keystore for every listener. The Raft port needs it because peers always dial
		// https://; the HTTPS port needs it because CamusDB accepts credentials over TLS only.
		"--https-certificate", keystore,
		"--raft-certificate", keystore,
		"--initial-cluster-partitions", fmt.Sprintf("%d", partitions),
	)

	// extraArgs go last. CamusDB's parser rejects a flag given twice, so they add options; they
	// cannot override one generated above.
	return append(args, cluster.Spec.ExtraArgs...)
}

// keystoreKey is the key of the PKCS#12 keystore inside the Secret the pods mount.
func keystoreKey(cluster *camusdbv1alpha1.CamusDBCluster) string {
	if cluster.Spec.TLS.CertManager != nil {
		// The operator writes the derived keystore under this fixed key.
		return derivedKeystoreKey
	}
	if cluster.Spec.TLS.KeystoreKey != "" {
		return cluster.Spec.TLS.KeystoreKey
	}
	return defaultKeystoreKey
}

// caKey is the key of the PEM CA certificate inside the Secret the pods mount.
func caKey(cluster *camusdbv1alpha1.CamusDBCluster) string {
	if cluster.Spec.TLS.CertManager != nil {
		return derivedCAKey
	}
	if cluster.Spec.TLS.CAKey != "" {
		return cluster.Spec.TLS.CAKey
	}
	return defaultCAKey
}

// topologyEnv renders the cluster topology the startup script reads at boot.
//
// It is deliberately NOT part of the script, and therefore not part of the pod template hash.
// Topology changes on every scale, and baking it into the template would mean growing a cluster
// from 3 to 5 first restarts all three existing nodes — disruption with no purpose, since after
// the roster is committed Kommander derives its peer set from the roster and consults the seed
// list only before the first seed. Static configuration stays in the script, where a change
// *should* roll the cluster.
func topologyEnv(cluster *camusdbv1alpha1.CamusDBCluster, bootstrapSize int32) string {
	return fmt.Sprintf(`# Generated by camusdb-operator. Read at startup; changes need no pod restart.
REPLICAS=%d
BOOTSTRAP_SIZE=%d
`, desiredReplicas(cluster), bootstrapSize)
}

// entrypointScript renders the startup script mounted into every pod.
//
// It exists because a StatefulSet has one pod template but each CamusDB node needs a distinct
// --raft-nodeid, --raft-nodename and --raft-host, plus an --initial-cluster list that excludes
// itself. The script derives all of them from the pod's own ordinal at startup.
func entrypointScript(cluster *camusdbv1alpha1.CamusDBCluster) string {
	var b strings.Builder

	static := staticArgs(cluster)
	quoted := make([]string, 0, len(static))
	for _, a := range static {
		quoted = append(quoted, shellQuote(a))
	}

	fmt.Fprintf(&b, `#!/usr/bin/env bash
# Generated by camusdb-operator for %s/%s. Do not edit — the next reconcile overwrites it.
set -euo pipefail

if [[ -z "${POD_NAME:-}" ]]; then
  echo "camusdb-operator: POD_NAME is unset; the downward API env var is missing" >&2
  exit 1
fi

# The StatefulSet ordinal is the node's durable identity: pod names are stable across restarts,
# so the Raft node id derived from it is too.
ORDINAL="${POD_NAME##*-}"
if ! [[ "$ORDINAL" =~ ^[0-9]+$ ]]; then
  echo "camusdb-operator: cannot derive an ordinal from POD_NAME=$POD_NAME" >&2
  exit 1
fi

STS_NAME=%s
PEER_DOMAIN=%s
RAFT_PORT=%d

# Topology lives in a separate file rather than in this script so that scaling the cluster does
# not change the pod template and therefore does not restart every existing node.
TOPOLOGY=%s
if [[ ! -f "$TOPOLOGY" ]]; then
  echo "camusdb-operator: $TOPOLOGY is missing" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$TOPOLOGY"

# +1 because Kommander records a provisional node id of 0 for a member it has not identified.
NODE_ID=$((ORDINAL + 1))
RAFT_HOST="${POD_NAME}.${PEER_DOMAIN}"

# --initial-cluster lists this node's PEERS and must exclude the node itself. Before the roster
# is seeded, Kommander assigns the discovery list straight to its peer set without filtering
# self, so a self-entry would inflate quorum math during bootstrap.
PEERS=()
for ((i = 0; i < REPLICAS; i++)); do
  if [[ "$i" -eq "$ORDINAL" ]]; then continue; fi
  PEERS+=("${STS_NAME}-${i}.${PEER_DOMAIN}:${RAFT_PORT}")
done

# An empty peer list is not an error: it is how a single-node cluster forms a roster of one. It
# is, however, the wrong thing to hand a multi-node member, so a node that expects peers and has
# none fails rather than silently starting a second one-node cluster.
CLUSTER_ARGS=()
if [[ ${#PEERS[@]} -gt 0 ]]; then
  CLUSTER_ARGS+=(--initial-cluster "${PEERS[@]}")
elif [[ "$REPLICAS" -ne 1 ]]; then
  echo "camusdb-operator: empty --initial-cluster with REPLICAS=$REPLICAS would start a one-node cluster" >&2
  exit 1
fi

# Join mode. BOOTSTRAP_SIZE is the size the cluster was first created at and never changes, so
# an ordinal at or above it is by definition a node added after the cluster was already running.
#
#   * Ordinal below BOOTSTRAP_SIZE -> one of the founding nodes. Static discovery seeds the
#                      roster; --join-existing here could fork it into a second roster.
#   * At or above, with data on disk -> already a member. It replays its WAL and rejoins its
#                      partitions; asking it to re-admit itself is wrong.
#   * At or above, empty -> a new node arriving at a running cluster (scale-up, or a node whose
#                      volume was lost). It must enter as a Learner via the seeds.
#
# The key/value directory is what is checked, not the data directory: a dedicated WAL volume is
# mounted inside the data directory, so the data directory is never empty.
JOIN_ARGS=()
if [[ "$ORDINAL" -ge "$BOOTSTRAP_SIZE" && -z "$(ls -A %s 2>/dev/null)" ]]; then
  # CamusDB's --join-existing takes a value; the bare flag is a parse error.
  JOIN_ARGS+=(--join-existing true)
fi

mkdir -p %s

# Nodes reach each other over https:// and CamusDB always validates the peer's certificate, with
# no switch to skip it. The CA that signed the keystore is therefore added to the trust store the
# server reads. SSL_CERT_FILE replaces the system bundle rather than extending it, so the system
# bundle is copied in too, or outbound calls to public endpoints would stop validating.
CA_FILE=%s
if [[ ! -s "$CA_FILE" ]]; then
  echo "camusdb-operator: $CA_FILE is missing or empty; the nodes could not verify each other" >&2
  exit 1
fi
BUNDLE=%s
{
  if [[ -f %s ]]; then cat %s; fi
  cat "$CA_FILE"
} > "$BUNDLE"
export SSL_CERT_FILE="$BUNDLE"

STATIC_ARGS=(%s)

# ${ARR[@]+"${ARR[@]}"} rather than "${ARR[@]}": under "set -u" an empty array is an unbound
# variable in bash before 4.4, and a single-node cluster leaves CLUSTER_ARGS empty.
set -x
exec dotnet %s \
  --raft-nodename "$POD_NAME" \
  --raft-nodeid "$NODE_ID" \
  --raft-host "$RAFT_HOST" \
  --raft-port "$RAFT_PORT" \
  ${CLUSTER_ARGS[@]+"${CLUSTER_ARGS[@]}"} \
  ${JOIN_ARGS[@]+"${JOIN_ARGS[@]}"} \
  ${STATIC_ARGS[@]+"${STATIC_ARGS[@]}"}
`,
		cluster.Namespace, cluster.Name,
		shellQuote(stsName(cluster)),
		shellQuote(peerDomain(cluster)),
		ports(cluster).Raft,
		shellQuote(topologyPath),
		shellQuote(kvPath),
		shellQuote(dataPath),
		shellQuote(tlsMountPath+"/"+caKey(cluster)),
		shellQuote(trustMountPath+"/ca-bundle.crt"),
		shellQuote(systemCABundle), shellQuote(systemCABundle),
		strings.Join(quoted, " "),
		serverDLLPath,
	)

	return b.String()
}

// configHash digests the rendered script and server config. It rides on the pod template as an
// annotation so that editing the ConfigMap produces a new StatefulSet revision — without it a
// config change would sit in the ConfigMap and never reach a running pod.
func configHash(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		// The length prefix keeps ("ab", "c") and ("a", "bc") from hashing alike.
		_, _ = fmt.Fprintf(h, "%d:%s", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}
