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
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	camusdbv1alpha1 "github.com/camusdb/k8s-operator/api/v1alpha1"
)

// Shared test fixtures. Named constants because the linter — correctly — objects to the same
// literal appearing across many assertions.
const (
	testClusterName = "camus"
	testTLSSecret   = "camus-tls"
	testCAPEM       = "-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----\n"
	flagJoin        = "--join-existing"
	flagSchemaAck   = "--schema-ack-wait-timeout-ms"
	keyKahuna       = "kahuna"
	pemCertificate  = "CERTIFICATE"
	addedPod        = "camus-4"
)

func testCluster(replicas int32) *camusdbv1alpha1.CamusDBCluster {
	return &camusdbv1alpha1.CamusDBCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: "db"},
		Spec: camusdbv1alpha1.CamusDBClusterSpec{
			Replicas:   ptr.To(replicas),
			Image:      "camusdb/camusdb:latest",
			Partitions: ptr.To(int32(3)),
			Storage: camusdbv1alpha1.StorageSpec{
				Backend: camusdbv1alpha1.StorageBackendRocksDB,
				Data:    camusdbv1alpha1.VolumeSpec{Size: resource.MustParse("10Gi")},
			},
			TLS: camusdbv1alpha1.TLSSpec{
				SecretRef: &corev1.LocalObjectReference{Name: testTLSSecret},
			},
		},
	}
}

// scriptRun is the outcome of one run of the generated script.
type scriptRun struct {
	args       []string
	certFile   string
	bundle     string
	output     string
	exitedOkay bool
}

// entrypointOptions shape the filesystem the script starts on.
type entrypointOptions struct {
	// kvData puts a file in the key/value directory, as on a node that is already a member.
	kvData bool
	// walOnly creates the WAL directory and nothing else, as a dedicated WAL volume mounted
	// inside the data directory does.
	walOnly bool
	// noCA leaves the CA file out of the mounted keystore Secret.
	noCA bool
}

// runEntrypoint executes the generated script with the real server replaced by a stub that records
// its argv. This exercises the script's own logic — ordinal parsing, peer-list construction,
// join-mode selection, the trust bundle — rather than asserting on the rendered text.
func runEntrypoint(t *testing.T, cluster *camusdbv1alpha1.CamusDBCluster, bootstrapSize int32, pod string, opts entrypointOptions) scriptRun {
	t.Helper()

	dir := t.TempDir()
	topology := filepath.Join(dir, "topology.env")
	if err := os.WriteFile(topology, []byte(topologyEnv(cluster, bootstrapSize)), 0o644); err != nil {
		t.Fatal(err)
	}
	tlsDir := filepath.Join(dir, "tls")
	trustDir := filepath.Join(dir, "trust")
	for _, d := range []string{tlsDir, trustDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if !opts.noCA {
		if err := os.WriteFile(filepath.Join(tlsDir, caKey(cluster)), []byte(testCAPEM), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	systemBundle := filepath.Join(dir, "system-ca.crt")
	if err := os.WriteFile(systemBundle, []byte("SYSTEM ROOTS\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// The script's paths are absolute, so redirect each prefix into the temp dir.
	script := entrypointScript(cluster)
	script = strings.ReplaceAll(script, "'"+storageMountPath+"/", "'"+dir+"/storage/")
	script = strings.ReplaceAll(script, "'"+topologyPath+"'", "'"+topology+"'")
	script = strings.ReplaceAll(script, "'"+tlsMountPath+"/", "'"+tlsDir+"/")
	script = strings.ReplaceAll(script, "'"+trustMountPath+"/", "'"+trustDir+"/")
	script = strings.ReplaceAll(script, "'"+systemCABundle+"'", "'"+systemBundle+"'")
	// Replace the exec of the real server with one that reports the trust setting and its argv.
	script = strings.ReplaceAll(script, "exec dotnet "+serverDLLPath,
		`printf 'SSL_CERT_FILE=%s\n' "$SSL_CERT_FILE"; exec printf '%s\n'`)
	path := filepath.Join(dir, "entrypoint.sh")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	data := filepath.Join(dir, "storage", "data")
	if opts.kvData {
		if err := os.MkdirAll(filepath.Join(data, "kv"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(data, "kv", "CURRENT"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if opts.walOnly {
		if err := os.MkdirAll(filepath.Join(data, "wal", "lost+found"), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	cmd := exec.Command("/bin/bash", path)
	cmd.Env = append(os.Environ(), "POD_NAME="+pod)
	out, err := cmd.CombinedOutput()
	run := scriptRun{output: string(out), exitedOkay: err == nil}
	if err != nil {
		return run
	}

	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "" || strings.HasPrefix(line, "+ "):
		case strings.HasPrefix(line, "SSL_CERT_FILE="):
			run.certFile = strings.TrimPrefix(line, "SSL_CERT_FILE=")
		default:
			run.args = append(run.args, line)
		}
	}
	if run.certFile != "" {
		b, _ := os.ReadFile(run.certFile)
		run.bundle = string(b)
	}
	return run
}

// mustRun is runEntrypoint for a run that is expected to start the server.
func mustRun(t *testing.T, cluster *camusdbv1alpha1.CamusDBCluster, bootstrapSize int32, pod string, opts entrypointOptions) scriptRun {
	t.Helper()
	run := runEntrypoint(t, cluster, bootstrapSize, pod, opts)
	if !run.exitedOkay {
		t.Fatalf("entrypoint failed:\n%s", run.output)
	}
	return run
}

func TestEntrypointIsValidBash(t *testing.T) {
	script := entrypointScript(testCluster(3))
	cmd := exec.Command("/bin/bash", "-n")
	cmd.Stdin = strings.NewReader(script)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated script is not valid bash: %v\n%s\n---\n%s", err, out, script)
	}
}

// argValues returns every value following the named flag, up to the next flag.
func argValues(args []string, flag string) []string {
	for i, a := range args {
		if a != flag {
			continue
		}
		var vals []string
		for _, v := range args[i+1:] {
			if strings.HasPrefix(v, "--") {
				break
			}
			vals = append(vals, v)
		}
		return vals
	}
	return nil
}

func hasFlag(args []string, flag string) bool {
	return slices.Contains(args, flag)
}

// countFlag counts a flag's occurrences. CamusDB's parser rejects a flag given twice.
func countFlag(args []string, flag string) int {
	n := 0
	for _, a := range args {
		if a == flag {
			n++
		}
	}
	return n
}

func TestEntrypointDerivesNodeIdentityFromOrdinal(t *testing.T) {
	args := mustRun(t, testCluster(3), 3, "camus-1", entrypointOptions{}).args

	if got := argValues(args, "--raft-nodename"); len(got) != 1 || got[0] != "camus-1" {
		t.Errorf("--raft-nodename = %v, want [camus-1]", got)
	}
	// Ordinal 1 must map to node id 2: Kommander records a provisional 0 for an unknown member.
	if got := argValues(args, "--raft-nodeid"); len(got) != 1 || got[0] != "2" {
		t.Errorf("--raft-nodeid = %v, want [2]", got)
	}
	want := "camus-1.camus-peer.db.svc.cluster.local"
	if got := argValues(args, "--raft-host"); len(got) != 1 || got[0] != want {
		t.Errorf("--raft-host = %v, want [%s]", got, want)
	}
	if got := argValues(args, "--raft-port"); len(got) != 1 || got[0] != "7070" {
		t.Errorf("--raft-port = %v, want [7070]", got)
	}
}

// The peer list must exclude the node itself: before the roster is seeded Kommander assigns the
// discovery list straight to its peer set with no self-filter, so a self-entry inflates quorum.
func TestEntrypointExcludesSelfFromInitialCluster(t *testing.T) {
	args := mustRun(t, testCluster(3), 3, "camus-1", entrypointOptions{}).args

	got := argValues(args, "--initial-cluster")
	want := []string{
		"camus-0.camus-peer.db.svc.cluster.local:7070",
		"camus-2.camus-peer.db.svc.cluster.local:7070",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("--initial-cluster = %v, want %v", got, want)
	}
}

// A single-node cluster gets no --initial-cluster at all and still runs in cluster mode, which
// forms a roster of one with the same Raft listener and endpoints as a larger cluster.
func TestEntrypointSingleNodeOmitsInitialCluster(t *testing.T) {
	args := mustRun(t, testCluster(1), 1, "camus-0", entrypointOptions{}).args
	if got := argValues(args, "--initial-cluster"); got != nil {
		t.Errorf("--initial-cluster = %v, want absent for a single-node cluster", got)
	}
	if got := argValues(args, "--mode"); !slices.Equal(got, []string{"cluster"}) {
		t.Errorf("--mode = %v, want [cluster]", got)
	}
}

func TestEntrypointJoinMode(t *testing.T) {
	tests := []struct {
		name          string
		replicas      int32
		bootstrapSize int32
		pod           string
		opts          entrypointOptions
		wantJoin      bool
	}{
		// A founding node forms the cluster through static discovery. --join-existing here could
		// fork it into a second roster.
		{"founding node, greenfield", 3, 3, "camus-0", entrypointOptions{}, false},
		// Still a founding node even after the cluster grew, so it must never switch modes.
		{"founding node after scale up", 5, 3, "camus-1", entrypointOptions{}, false},
		// Added after the cluster was running: it must enter as a Learner via the seeds.
		{"added node, empty volume", 5, 3, addedPod, entrypointOptions{}, true},
		// A dedicated WAL volume makes the data directory non-empty on a brand-new node; the key/value
		// directory is what decides.
		{"added node, empty volume beside a WAL volume", 5, 3, addedPod, entrypointOptions{walOnly: true}, true},
		// Already a member: it replays its WAL and rejoins its partitions, so asking it to
		// re-admit itself is wrong.
		{"added node, restart with data", 5, 3, addedPod, entrypointOptions{kvData: true}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := mustRun(t, testCluster(tc.replicas), tc.bootstrapSize, tc.pod, tc.opts).args
			got := argValues(args, flagJoin)
			if tc.wantJoin {
				// CamusDB's --join-existing takes a value; the bare flag is a parse error.
				if !slices.Equal(got, []string{"true"}) {
					t.Errorf("%s = %v, want [true] (args: %v)", flagJoin, got, args)
				}
			} else if hasFlag(args, flagJoin) {
				t.Errorf("%s present, want absent (args: %v)", flagJoin, args)
			}
		})
	}
}

// The nodes verify each other's certificates against the cluster CA, and CamusDB has no switch to
// skip that, so the CA must reach the trust store the server reads — without dropping the system
// roots, which outbound calls to public endpoints still need.
func TestEntrypointTrustsTheClusterCA(t *testing.T) {
	run := mustRun(t, testCluster(3), 3, "camus-0", entrypointOptions{})
	if run.certFile == "" {
		t.Fatal("SSL_CERT_FILE is not exported to the server")
	}
	if !strings.Contains(run.bundle, testCAPEM) {
		t.Errorf("trust bundle does not contain the cluster CA:\n%s", run.bundle)
	}
	if !strings.Contains(run.bundle, "SYSTEM ROOTS") {
		t.Errorf("trust bundle dropped the system roots:\n%s", run.bundle)
	}
}

// A node without the CA would start and then fail every peer call with a TLS error, which reads as
// a network problem. Refusing to start names the cause.
func TestEntrypointFailsWithoutTheCA(t *testing.T) {
	run := runEntrypoint(t, testCluster(3), 3, "camus-0", entrypointOptions{noCA: true})
	if run.exitedOkay {
		t.Fatal("entrypoint started the server without a CA")
	}
	if !strings.Contains(run.output, "missing or empty") {
		t.Errorf("failure does not name the missing CA:\n%s", run.output)
	}
}

func TestEntrypointRejectsUnparseablePodName(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "entrypoint.sh")
	if err := os.WriteFile(script, []byte(entrypointScript(testCluster(3))), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/bash", script)
	cmd.Env = append(os.Environ(), "POD_NAME=not-an-ordinal")
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("expected failure for an unparseable pod name, got success:\n%s", out)
	}
}

// CamusDB's parser rejects any option it does not define, and exits. Kahuna flags the server does
// not share would stop every node at startup.
func TestStaticArgsUseOnlyCamusDBFlags(t *testing.T) {
	args := staticArgs(testCluster(3))
	for _, flag := range []string{"--allow-plaintext-listener", "--http-ports", "--https-ports",
		"--storage", "--wal-storage", "--raft-allow-insecure-certificate-validation", "--https-certificate-password"} {
		if hasFlag(args, flag) {
			t.Errorf("%s emitted; CamusDB does not define it and would refuse to start", flag)
		}
	}
	for _, flag := range []string{"--config", "--mode", "--data-dir", "--http-port", "--https-port",
		"--https-certificate", "--raft-certificate", "--initial-cluster-partitions"} {
		if countFlag(args, flag) != 1 {
			t.Errorf("%s emitted %d times, want once", flag, countFlag(args, flag))
		}
	}
	if got := argValues(args, "--https-certificate"); !slices.Equal(got, argValues(args, "--raft-certificate")) {
		t.Errorf("--https-certificate %v and --raft-certificate %v differ", got, argValues(args, "--raft-certificate"))
	}
}

func TestStaticArgsAppendExtraArgsLast(t *testing.T) {
	cluster := testCluster(3)
	cluster.Spec.ExtraArgs = []string{flagSchemaAck, "5000"}
	args := staticArgs(cluster)
	if !slices.Equal(args[len(args)-2:], cluster.Spec.ExtraArgs) {
		t.Errorf("extraArgs not appended last: %v", args)
	}
}

// parseConfig renders and decodes the server config.
func parseConfig(t *testing.T, cluster *camusdbv1alpha1.CamusDBCluster) (map[string]any, string) {
	t.Helper()
	out, err := serverConfig(cluster)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := yaml.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("config.yml is not YAML: %v\n%s", err, out)
	}
	return m, out
}

func TestServerConfigOmitsUnsetTuning(t *testing.T) {
	cfg, _ := parseConfig(t, testCluster(3))
	kahuna, _ := cfg[keyKahuna].(map[string]any)
	for _, key := range []string{"locks_workers", "max_concurrent_sessions", "heartbeat_interval_ms",
		"wal_sync_writes", "replication_factor", "enable_placement_rebalancer",
		"max_concurrent_replica_transfers", "max_replica_moves_per_pass", "replica_count_deadband"} {
		if _, ok := kahuna[key]; ok {
			t.Errorf("kahuna.%s written although unset; the server default should apply instead", key)
		}
	}
	rocksdb := string(camusdbv1alpha1.StorageBackendRocksDB)
	if kahuna["storage"] != rocksdb || kahuna["wal_storage"] != rocksdb {
		t.Errorf("storage backends = %v / %v, want rocksdb", kahuna["storage"], kahuna["wal_storage"])
	}
	if cfg["grpc_port"] != float64(5096) {
		t.Errorf("grpc_port = %v, want 5096", cfg["grpc_port"])
	}
}

// Peer mutual TLS follows auth. An authenticated multi-node cluster cannot form without it, and
// CamusDB 0.15.1 rejects the keys, so a cluster without auth must not get them.
func TestServerConfigEnablesPeerMutualTLSWithAuth(t *testing.T) {
	cfg, _ := parseConfig(t, testCluster(3))
	for _, key := range []string{"raft_mutual_tls_enabled", "raft_peer_ca"} {
		if _, ok := cfg[key]; ok {
			t.Errorf("%s written without auth", key)
		}
	}

	cluster := testCluster(3)
	cluster.Spec.Auth = &camusdbv1alpha1.AuthSpec{}
	cfg, _ = parseConfig(t, cluster)
	if cfg["raft_mutual_tls_enabled"] != true {
		t.Errorf("raft_mutual_tls_enabled = %v, want true", cfg["raft_mutual_tls_enabled"])
	}
	if want := tlsMountPath + "/" + caKey(cluster); cfg["raft_peer_ca"] != want {
		t.Errorf("raft_peer_ca = %v, want %s", cfg["raft_peer_ca"], want)
	}
}

func TestServerConfigWritesSetTuning(t *testing.T) {
	cluster := testCluster(3)
	cluster.Spec.Raft = &camusdbv1alpha1.RaftSpec{HeartbeatIntervalMs: ptr.To(int32(250))}
	cluster.Spec.Workers = &camusdbv1alpha1.WorkersSpec{Locks: ptr.To(int32(8))}
	cluster.Spec.Storage.SyncWrites = ptr.To(false)
	cluster.Spec.Replication = &camusdbv1alpha1.ReplicationSpec{Factor: 3}

	cfg, _ := parseConfig(t, cluster)
	kahuna, _ := cfg[keyKahuna].(map[string]any)
	want := map[string]any{
		"heartbeat_interval_ms": float64(250),
		"locks_workers":         float64(8),
		"wal_sync_writes":       false,
		"replication_factor":    float64(3),
		// The rebalancer comes with the factor: without it a node added later never gets replicas.
		"enable_placement_rebalancer": true,
		// And with the pacing that fills a new node in minutes rather than leaving it under-filled.
		"max_concurrent_replica_transfers": float64(4),
		"max_replica_moves_per_pass":       float64(8),
		"replica_count_deadband":           float64(0),
	}
	for k, v := range want {
		if kahuna[k] != v {
			t.Errorf("kahuna.%s = %v, want %v", k, kahuna[k], v)
		}
	}
}

// spec.config wins over a generated setting, and merges into a nested block rather than replacing
// it, so overriding one engine option does not silently drop the operator's others.
func TestServerConfigMergesSpecConfig(t *testing.T) {
	cluster := testCluster(3)
	cluster.Spec.Replication = &camusdbv1alpha1.ReplicationSpec{Factor: 3}
	cluster.Spec.Config = &runtime.RawExtension{Raw: []byte(
		`{"slow_query_log_threshold_ms": 500, "grpc_port": 6000, "kahuna": {"replication_factor": 2, "max_replica_moves_per_pass": 2, "backup_dir": "/storage/data/backups"}}`)}

	cfg, raw := parseConfig(t, cluster)
	kahuna, _ := cfg[keyKahuna].(map[string]any)
	if cfg["grpc_port"] != float64(6000) {
		t.Errorf("grpc_port = %v, want the spec.config value 6000", cfg["grpc_port"])
	}
	if kahuna["replication_factor"] != float64(2) {
		t.Errorf("kahuna.replication_factor = %v, want the spec.config value 2", kahuna["replication_factor"])
	}
	if kahuna["max_replica_moves_per_pass"] != float64(2) || kahuna["max_concurrent_replica_transfers"] != float64(4) {
		t.Errorf("rebalancer pacing = %v / %v, want the spec.config override and the generated value",
			kahuna["max_replica_moves_per_pass"], kahuna["max_concurrent_replica_transfers"])
	}
	if kahuna["enable_placement_rebalancer"] != true || kahuna["storage"] != string(camusdbv1alpha1.StorageBackendRocksDB) {
		t.Errorf("generated kahuna settings dropped by the merge: %v", kahuna)
	}
	if kahuna["backup_dir"] != "/storage/data/backups" {
		t.Errorf("kahuna.backup_dir = %v, want it added", kahuna["backup_dir"])
	}
	// An integer must stay an integer: a server parsing an int setting can reject "500.0".
	if !strings.Contains(raw, "slow_query_log_threshold_ms: 500\n") {
		t.Errorf("integer setting not rendered as an integer:\n%s", raw)
	}
}

// Scaling must NOT change the pod template. Topology is read from a mounted file at startup, so
// growing a cluster adds a node instead of first restarting every existing one.
func TestConfigHashIsStableAcrossScale(t *testing.T) {
	a, _ := render(testCluster(3))
	b, _ := render(testCluster(5))
	if configHash(a.script, a.config) != configHash(b.script, b.config) {
		t.Error("config hash changed with replica count; a scale would roll every existing node")
	}
}

// A real configuration change must still roll the cluster — that is the whole point of the hash.
func TestConfigHashChangesWithConfiguration(t *testing.T) {
	hash := func(c *camusdbv1alpha1.CamusDBCluster) string {
		r, err := render(c)
		if err != nil {
			t.Fatal(err)
		}
		return configHash(r.script, r.config)
	}
	base := hash(testCluster(3))

	tuned := testCluster(3)
	tuned.Spec.Raft = &camusdbv1alpha1.RaftSpec{HeartbeatIntervalMs: ptr.To(int32(250))}
	if hash(tuned) == base {
		t.Error("config hash did not change when a typed setting changed")
	}

	extra := testCluster(3)
	extra.Spec.ExtraArgs = []string{flagSchemaAck, "5000"}
	if hash(extra) == base {
		t.Error("config hash did not change when extraArgs changed")
	}

	raw := testCluster(3)
	raw.Spec.Config = &runtime.RawExtension{Raw: []byte(`{"slow_query_log_enabled": true}`)}
	if hash(raw) == base {
		t.Error("config hash did not change when spec.config changed")
	}
}

// The topology file is what carries the replica count to the pod.
func TestTopologyEnvCarriesReplicasAndBootstrapSize(t *testing.T) {
	got := topologyEnv(testCluster(5), 3)
	for _, want := range []string{"REPLICAS=5", "BOOTSTRAP_SIZE=3"} {
		if !strings.Contains(got, want) {
			t.Errorf("topology.env missing %q:\n%s", want, got)
		}
	}
}
