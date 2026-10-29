# CamusDB Kubernetes Operator (Alpha)

A Kubernetes operator for [CamusDB](https://github.com/camusdb/camusdb) — a distributed SQL database
built on Raft consensus, with an embedded [Kahuna](https://github.com/kahunakv/kahuna) storage node
in every server.

A CamusDB cluster's membership is a **consensus-committed roster**, changed one node at a time. That
is why a plain StatefulSet is not enough: node identity, the peer list and the join mode are all
per-node startup flags, and adding or removing a member is a distributed transaction with rules
about quorum. This operator owns those rules.

```yaml
apiVersion: camusdb.io/v1alpha1
kind: CamusDBCluster
metadata:
  name: camusdb
spec:
  replicas: 3
  partitions: 3
  storage:
    data:
      size: 100Gi
  tls:
    certManager:
      issuerRef: {name: camusdb-ca, kind: Issuer}
```

```
$ kubectl get camusdbcluster
NAME      DESIRED   AUTOSCALED   READY   VOTERS   PHASE     AGE
camusdb   3                      3       3        Running   12m
```

## What it does

| | |
|---|---|
| **Provisions** | Headless peer Service, client Service, per-node PVCs, ConfigMap (startup script and `config.yml`), PodDisruptionBudget, StatefulSet, and — when asked — the cert-manager Certificate, the keystore derived from it, the authentication Secrets, and the shared backup claim and its signing key. |
| **Bootstraps** | Brings up all founding nodes at once so they can form their first quorum, and reports `Running` only when the committed roster carries every node as a Voter. |
| **Scales** | One node at a time, in both directions, each step gated on the roster and on the replica placement having settled — never on what Kubernetes merely believes. With authentication on, departing nodes are decommissioned through the cluster's leave API before their pods are removed. |
| **Autoscales** | Optional. Adds and removes nodes with load — Raft log entry rate, client queue depth, or CPU — between a minimum and a maximum, one node at a time, and never grows a cluster in a way that would put more than 3 copies on any data partition. |
| **Upgrades** | Rolling restarts one node at a time, held back automatically whenever the roster is not converged or unreadable. A renewed certificate rolls the nodes onto it the same way. |
| **Backs up** | Optional. Coordinated full backups and incrementals on cron schedules, to one volume that every node shares, with retention. |
| **Reports** | `status.members` mirrors the committed roster joined to the pods serving it, `status.currentImage` names the image every node runs, plus `Available` / `Progressing` / `RosterConverged` / `KeystoreReady` / `BackupFailing` / `BackupVolumeReady` conditions. |

## Design decisions worth knowing

These are the non-obvious ones. Each is load-bearing; changing it breaks something specific.

**The roster is the source of truth, not Kubernetes.** A `Running` pod need not be a cluster member,
and a roster entry routinely outlives its pod — that gap *is* the scale-down process. Every
membership step is gated on `GET /v1/cluster/membership`, and members are matched to pods by
endpoint string rather than by node id.

**`podManagementPolicy: Parallel` is required.** Under `OrderedReady` the StatefulSet waits for
pod-0 to become ready before creating pod-1, but pod-0 cannot become ready until a quorum forms,
which needs its peers to exist. Bootstrap would deadlock permanently.

**`publishNotReadyAddresses: true` on the peer Service is required**, for the same reason: peers
must resolve each other *before* any of them is ready.

**`--initial-cluster` must exclude the node itself.** Before the roster is seeded, Kommander assigns
the discovery list straight to its peer set with no self-filter, so a self-entry inflates quorum
math during bootstrap.

**Settings go through a generated `config.yml`, identity through flags.** Most CamusDB settings —
the replication factor, the rebalancer, every engine option — exist only in `config.yml`, under its
`kahuna:` key. The operator renders that file from the typed spec fields plus `spec.config`, and
passes it with `--config`. Only what differs per node (name, id, host, peers, join mode) is on the
command line.

**Topology lives outside the pod template.** The replica count is written to a `topology.env` file
the pod reads at startup, not baked into the startup script. Only the script and `config.yml` are
hashed into the pod template. Without this split, growing a cluster from 3 to 5 would first restart
all three existing nodes — pure disruption, because once the roster is committed Kommander derives
peers from the roster and consults the seed list only before the first seed.

**`status.bootstrapSize` is pinned at creation and never revised.** A starting node decides whether
to *form* the cluster (static discovery) or *join* it (`--join-existing true`, entering as a
Learner) by comparing its ordinal against this number. Letting it drift with `spec.replicas` would
make founding nodes look like late arrivals after the first scale. A node joins only when its
key/value directory is empty, so a restarted member never asks to be admitted twice.

**Every node trusts the cluster CA explicitly.** Nodes reach each other over `https://` and CamusDB
always validates the peer's certificate — it has no switch to skip that. The startup script builds
a CA bundle from the system roots plus the keystore Secret's `ca.crt` and points `SSL_CERT_FILE` at
it. A node whose Secret lacks the CA refuses to start and says why, rather than failing every peer
call with a TLS error.

**No StatefulSet before the keystore.** The keystore's digest is part of the pod template, so that a
renewed certificate rolls the nodes. Pods created before the keystore existed would therefore be
replaced the moment it appeared — and a rolling update waits for each replaced pod to become ready,
which no founding node can do alone. The operator reports `Pending` and `KeystoreReady=False`
until the keystore is there.

**A step is complete when the data has followed it, not when the roster has.** Before any further
membership change the operator reads `GET /v1/cluster/placement` and holds while a replica is still
in a transitional role or still assigned to a node that left. A scale-down that went ahead during
those repairs would take a second copy from partitions that have already lost one. An unreadable
placement holds a scale step, like an unreadable roster. A rolling update is not held for it.

**`status.currentImage` moves only when every node runs the image.** It changes to `spec.image` once
every pod of the StatefulSet runs that image and is ready. During an image rollout, and when a
rollout is stuck, it keeps the previous image, and `Progressing` stays true and names the image
being rolled out.

**Liveness uses `/ping`, not `/v1/cluster/health` and not `/`.** The health route returns 503 for a
node that is up but not yet a serving member — a state every node passes through on restart, and
one an evicted node can sit in. Restarting the process fixes neither. `/` is the dashboard, which
refuses non-loopback callers when authentication is off. Readiness carries the membership-aware
check.

**Scale-down decommissions before deleting — when it can.** With authentication on, the departing
node is asked to leave through `POST /v1/cluster/leave`, and its pod is removed only once the cluster
confirms the removal is committed (or that it was already gone). A reachable node's refusal or
failed leave call makes the scale **hold**. Without authentication CamusDB accepts that call from
loopback only, so the operator removes the pod and waits for the failure detector to evict the
member — about 90 seconds per node, and with a replication factor the node's replicas are not
drained first. Either way it waits for roster convergence before taking another step.

**`replicas: 2` is rejected, and 1 ↔ many is rejected.** A two-Voter cluster has a quorum of 2, so it
tolerates no failures while costing twice as much as a single node. A single node forms a roster of
one by itself; the operator does not support growing it into a real cluster in place.

## Immutable fields

Enforced by CEL rules on the CRD, so the API server rejects the edit — no webhook to install:

- `spec.partitions` — the partition map is built once at bootstrap
- `spec.storage.backend`, `.walBackend` — changing either makes existing data unreadable

## Configuration

Operationally meaningful settings get typed fields (`spec.raft`, `spec.transactions`, `spec.workers`,
`spec.storage`). Every tuning field is optional and **omitted means "do not write the setting"**, so
the CamusDB server's own default applies rather than a copy of it that can silently fork.

Any other setting goes through `spec.config`, which is merged over the generated `config.yml` key by
key, in the server's own snake_case names, and wins on a conflict:

```yaml
spec:
  config:
    slow_query_log_enabled: true
    kahuna:
      backup_dir: /storage/data/backups
```

`spec.extraArgs` is appended to the command line. CamusDB's parser rejects an unknown flag and a
flag given twice, so extra arguments can add options but cannot override a generated one. `spec.env`
adds environment variables, for example `CAMUS_LOG_LEVEL_KAHUNA`.

Changing configuration rolls the cluster; changing `replicas` does not.

## Authentication

> **Needs CamusDB 0.15.2 or later.** The default image, 0.16.3, has it. Authentication turns on
> node-to-node mutual TLS (`raft_mutual_tls_enabled`), which 0.15.1 does not have and rejects at
> startup.

```yaml
spec:
  auth: {}                              # generate the admin account
  # auth:
  #   adminSecretRef: {name: my-admin}  # or supply one: keys "username" and "password"
```

With `spec.auth` set, the operator:

- generates a shared token key and node secret in `<name>-auth-keys` (once — they are never
  rewritten, because issued tokens and the peers' handshake depend on them);
- generates the admin account in `<name>-admin` unless `adminSecretRef` names one;
- passes all of them to the server through Secret references, so none appears in the pod spec or on
  the command line;
- logs in over the HTTPS port, verifying each node's certificate against the cluster CA, because
  CamusDB refuses a credential over plaintext.
- turns on mutual TLS between the nodes (`raft_mutual_tls_enabled`, with the cluster CA as
  `raft_peer_ca`). Raft and Kahuna traffic carries no user token, so a node with authentication on
  admits it only from a peer whose client certificate chains to the cluster CA, and refuses to start
  with peers otherwise. Each node presents its keystore as the client certificate, so a keystore you
  supply through `tls.secretRef` must allow client authentication as well as server authentication.
  `tls.certManager` certificates already do.

The account is the **bootstrap superuser**: CamusDB creates it only while its user catalog is empty.
If you change its password later with `ALTER USER`, update the Secret too, or the operator can no
longer log in — `RosterConverged` then says so. Turning authentication on for a running cluster
restarts it node by node, and until every node has restarted the cluster still serves
unauthenticated requests on the nodes not yet restarted.

Without authentication the operator still bootstraps, scales and upgrades the cluster: the roster,
health and placement routes answer anyone. Graceful decommissioning, autoscaling and backups need it.

## Autoscaling

For deployment examples, decision semantics, troubleshooting, and returning to a fixed size, see
[Operate and Autoscale a CamusDB Cluster](docs/operations.md).

```yaml
spec:
  replicas: 4            # starting size only, once autoscaling is on
  auth: {}               # required: metrics are served to authenticated callers only (see Known gaps)
  replication:
    factor: 3            # at most 3 copies of each data partition (1-3)
  autoscaling:
    minReplicas: 3
    maxReplicas: 9
    targetLogEntriesPerSecond: 4000  # Raft log entries per node
    targetQueueDepth: 32             # pending client proposals per node
    targetCPUPercent: 70             # server CPU share per node
    # scaleUpStabilizationSeconds: 60
    # scaleDownStabilizationSeconds: 300
    # tolerancePercent: 10
```

The operator polls every ready node. `GET /v1/dashboard/metrics` gives `kahuna.kv.write.entries`,
turned into a rate, and `raft.executor.client_queue_depth`; `GET /v1/admin/stats` gives the process
CPU share over the last minute. Each signal is averaged per node, and a node is only asked for what a
configured target needs. The recommendation is the HorizontalPodAutoscaler's rule — current size
times the worst signal's ratio to its target, with a dead band — but a step is **one node**, taken
only after the scaling direction has persisted across observations for the whole stabilization
window and only once the previous step is complete. Complete means more than the roster having
converged: the operator also reads `GET /v1/cluster/placement` and waits until no replica move is in
flight, no replica is left on a node that departed, and every node carries a share of the data
partitions. The share is waited for at most five minutes after the step, because the server's
planner stops at its own notion of even and a cluster with fewer placed replicas than nodes leaves
one empty for good; the two other conditions are waited for as long as they hold. A step up whose
node never joins — its pod cannot be scheduled or keeps failing — blocks further growth but not a
step back down, so a stuck pod cannot pin the cluster at a size it never reached. Removal goes
through the same leave-then-delete path as a manual scale-down.

What the autoscaler chose lives in `status.autoscaling` (`desiredReplicas`, `recommendedReplicas`,
the measured signals and the window timestamps), and `spec.replicas` is ignored after the first
pass. Removing `spec.autoscaling` hands the count back to `spec.replicas`.

**Why `replication` is required.** Without a replication factor every node holds every partition,
so a node added is one more copy of all data and one more voter on every write: more cost, no more
write capacity. With `factor: 3` the operator writes `kahuna.replication_factor: 3` and
`kahuna.enable_placement_rebalancer: true`; the rebalancer is what gives a newly added node any data
at all. It also raises the rebalancer's pacing (`kahuna.max_concurrent_replica_transfers: 4`,
`kahuna.max_replica_moves_per_pass: 8`, `kahuna.replica_count_deadband: 0`): at the server defaults a
node added by a scale-up took a quarter of an hour to receive a handful of replicas and then stayed
under-filled for good, because every full node was within the deadband of the even share. Override
any of them through `spec.config` if backfill competes too hard with client traffic.

**Any founding size works on CamusDB 0.16.0 and later, and so does turning the factor on later.** A
partition created while the cluster has no more nodes than the factor starts fully replicated (no
replica set), and so does every partition of a cluster that had no factor. CamusDB 0.16.0 and later
(Kahuna 1.12.8 and later) convert such a partition: the rebalancer first commits every current voter
as the replica set, which moves no data, and then trims that set down to the factor one replica at a
time. Older servers never convert one. The operator checks `GET /v1/cluster/placement` before every
scale-up and does not grow past 3 nodes while any data partition is still fully replicated,
reporting why in the `AutoscalingLimited` condition. On 0.16.0 and later that limit clears by itself
a few rebalancer passes after the cluster starts with the factor; on an older server it holds until
the cluster is recreated with more nodes than the factor.

**Signals and unknowns.** The autoscaler holds — and says so in `AutoscalingLimited` — when any
ready node's signals cannot be read, and it never scales down on a partial view, since the missing
node may be the leader carrying the load. Log entries are not statements: one transaction writes
several (its prepare, decision and settle), so calibrate the target against a representative
workload. CPU is the only signal that sees read load. `targetCPUPercent` needs a server that serves
`/v1/admin/stats`, which CamusDB 0.15.2 does and 0.15.1 does not.

## Backups

> **Needs `spec.auth`.** Without authentication CamusDB accepts backup calls from loopback callers
> only, so the API server rejects `backup` without `auth`.

```yaml
spec:
  auth: {}
  backup:
    schedule: "0 2 * * *"            # full backups, cron, UTC unless CRON_TZ=<zone> is given
    incrementalSchedule: "*/15 * * * *"  # optional
    volume:
      claim:                         # or claimName: <existing ReadWriteMany claim>
        size: 100Gi
        storageClassName: filestore  # any class that supports ReadWriteMany
    retention:
      maxChains: 7
      # maxAge: 168h
      # maxBytes: 500Gi
    # clusterID: prod/camusdb        # defaults to <namespace>/<name>
    # macKeySecretRef: {name: my-key, key: key}
    # timeout: 1h
    # suspend: false
```

Leave `backup` out and the cluster takes no backups. With it, every node mounts one shared volume
at `/backup` and runs with `kahuna.backup_dir: /backup/<name>`, so all nodes read one backup
catalog. A shared catalog is what lets a node find an incremental's parent and list every backup; a
volume per node would give each node only the backups it took itself.

The operator drives the schedule. A full backup is a coordinated backup
(`POST /v1/backups/coordinated`): the server takes it at one cut time that is the same for every
partition. Only the node that leads the backup meta partition accepts it, so the operator sends it
to each ready node in turn until one does. An incremental (`POST /v1/backups/incremental`) adds the
log written since the latest backup. The first full backup runs as soon as the cluster is steady
after `backup` is added; after that each kind runs on its own schedule, and missed runs collapse
into one, as with a CronJob. The operator calls the nodes over HTTPS as the admin account, which is
the bootstrap superuser that the backup routes require.

- **Backups run in the background.** A full backup can take many minutes, so the reconcile starts it
  and collects the result on a later pass; scaling and rollouts are not held for it.
- **A backup waits for a steady cluster:** bootstrap complete, roster converged, and every pod ready
  on the current configuration and image. Turning backups on rolls the cluster, and the first
  backup waits for the rollout to finish.
- **Status.** `status.backup` records the last backup id and type, the schedule anchors, and the
  failures. The `BackupFailing` condition is true while the latest attempt failed. A failed backup
  stays due and is retried after a backoff (1 minute, doubling to 30). If an incremental's parent is
  gone from the catalog (`CADB0705`), a new chain starts with a full backup at once.
- **A steady cluster can still have no backup coordinator.** Right after bootstrap no node leads the
  meta partition yet, so every node can refuse the first backup (`CADB070E`, not the coordinator).
  Leadership that moves during the backup (`CADB070A`) and a topology change during the backup
  (`CADB070D`) also clear on retry. When every refusal is of these kinds, the backup is retried
  after 15 seconds and is not counted as a failure: `BackupFailing` stays false with reason
  `BackupWaiting`, and `status.backup.unclaimedSince` shows since when. Only if no node takes it for
  5 minutes is it counted as a failure, with the usual backoff.
- **Retention** maps to `kahuna.backup_retention_*`. The server deletes whole chains and always
  keeps the newest one. The periodic GC pass is turned off (`kahuna.backup_gc_interval_seconds: 0`):
  its orphan sweep would delete a backup that another node has in progress on the shared volume. GC
  still runs after each backup, on the node that took it.
- **The volume must be ReadWriteMany when the cluster has more than one node.** The operator's
  `<name>-backup` claim asks for ReadWriteMany, or for ReadWriteOnce on a single-node cluster, where
  nothing shares it. Most default StorageClasses are block storage and cannot give ReadWriteMany:
  Kind's `local-path`, EBS, Persistent Disk and Azure Disk among them. Use a file-storage class
  (NFS, Filestore, EFS, Azure Files, CephFS) or name your own claim in `claimName`. Every pod mounts
  the claim, so while it is unbound no pod starts and the cluster stays in bootstrap. The
  `BackupVolumeReady` condition then names the cause, for example the provisioner's
  `ProvisioningFailed` event. Until the claim binds, the operator replaces it when
  `volume.claim` changes, so a corrected `storageClassName` takes effect without a manual delete.
  After it binds, the operator does not change it.
- **The claim and the signing key outlive the cluster.** Neither the `<name>-backup` claim nor the
  generated `<name>-backup-mac` Secret has an owner reference, so deleting the CamusDBCluster or
  removing `backup` keeps both. The server refuses to restore or seed from a signed backup without
  its key.

**Under a replication factor, backups need CamusDB 0.16.0 or later.** When `replication.factor` is
less than the most nodes the cluster can have (`autoscaling.maxReplicas`, or `replicas`), no node
hosts every partition. CamusDB 0.16.0 and later compose the coordinated backup from each node's
share of the partitions, captured at one cut, so the backup covers every partition. An older
server's backup holds only the partitions the node taking it hosts, and restore and seeding refuse
it. So on such a cluster the operator takes no backup when `image` has a release tag before 0.16.0:
`BackupFailing` is `True` with reason `UnsupportedImage`. The operator cannot read the version of an
image without a release tag (`latest`, a branch build, a digest alone), and takes backups with it. A
backup also waits until every pod runs the current `image`, because during an image rollout an older
node cannot capture its share.

**On a single-node Kind cluster,** whose only class is `local-path`, a hostPath PersistentVolume
gives a working ReadWriteMany claim. It is correct there only because every pod runs on the one
node:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: camusdb-backup
spec:
  capacity: {storage: 5Gi}
  accessModes: [ReadWriteMany]
  storageClassName: manual
  persistentVolumeReclaimPolicy: Retain
  hostPath: {path: /var/local/camusdb-backup, type: DirectoryOrCreate}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: camusdb-backup
  namespace: camusdb
spec:
  accessModes: [ReadWriteMany]
  storageClassName: manual
  volumeName: camusdb-backup
  resources: {requests: {storage: 5Gi}}
```

Then set `spec.backup.volume.claimName: camusdb-backup`.

**Restore is manual for now.** See the
[server backup guide](https://github.com/camusdb/camusdb/blob/main/docs/backups-and-point-in-time-recovery.md).

## TLS

TLS is not optional: nodes reach each other over `https://` and always verify the peer's
certificate. One keystore serves the Raft, gRPC and HTTPS listeners.

**Bring your own keystore:** a PKCS#12 file **without a password** — CamusDB opens it without one —
and the PEM CA that signed it, in one Secret:

```bash
kubectl create secret generic camusdb-tls \
  --from-file=certificate.pfx=/path/to/certificate.pfx \
  --from-file=ca.crt=/path/to/ca.crt
```

```yaml
spec:
  tls:
    secretRef: {name: camusdb-tls}
    keystoreKey: certificate.pfx   # default
    caKey: ca.crt                  # default
```

For a self-signed keystore, `ca.crt` is the certificate itself.

**Or let cert-manager issue it:**

```yaml
spec:
  tls:
    certManager:
      issuerRef: {name: camusdb-ca, kind: Issuer}
```

The operator creates a `Certificate` for the PEM pair and converts the issued Secret into
`<name>-keystore`: a PKCS#12 keystore with an empty password plus the CA. It does not use
cert-manager's own keystore output, which always carries a password. The per-pod SAN is a
**wildcard** (`*.<name>-peer.<ns>.svc.cluster.local`) rather than one name per ordinal, so scaling
the cluster does not reissue the certificate.

**Use a CA issuer**, not a self-signed one, for the cluster certificate. The nodes trust the
issuing CA; when a renewal rolls through, old and new nodes still trust each other only if that CA
stays fixed. A self-signed issuer makes every renewal a new trust root. The issuer must write
`ca.crt`, which rules out ACME issuers.

When cert-manager renews the certificate, the operator rebuilds the keystore and rolls the nodes
onto it one at a time — CamusDB reads its certificate only at startup. A supplied Secret whose
content changes rolls the nodes the same way, at the next resync.

## Server version

`spec.image` defaults to an exact tag, `camusdb/camusdb:0.16.3`, rather than to `latest`. A floating
tag re-rolls a running cluster on a registry push, and the generated command line is
version-specific: CamusDB's parser rejects an option it does not define and the node exits. Raise
the tag deliberately, after checking the flags against that release.

The stock single-node image works unmodified as a cluster member: the operator overrides the image's
entrypoint with its own generated startup script, which runs the same `CamusDB.dll` in cluster mode.

## Install

```bash
helm install camusdb-operator ./charts/camusdb-operator \
  --namespace camusdb-system --create-namespace
```

See the [chart README](charts/camusdb-operator/README.md) — in particular the CRD upgrade caveat,
since Helm never upgrades files under `crds/`.

## Development

```bash
make test                                    # unit + envtest
make test-e2e                                # kind e2e (creates and deletes its own cluster)
make helm-lint                               # sync generated CRD/RBAC into the chart, then lint
make docker-build IMG=camusdb-operator:dev
kind load docker-image camusdb-operator:dev --name <cluster>
make install                                 # CRDs
make deploy IMG=camusdb-operator:dev
kubectl apply -f config/samples/camusdb_v1alpha1_camusdbcluster.yaml
```

The operator reaches nodes by pod IP, so `make run` from a laptop cannot read the roster against a
kind cluster — pod IPs are not routable from the host. Deploy it in-cluster, or port-forward.

To test against a server built from source:

```bash
cd ../camusdb && docker build -f docker/Dockerfile.standalone -t camusdb/camusdb:dev .
CAMUSDB_IMAGE=camusdb/camusdb:dev CAMUSDB_IMAGE_SKIP_PULL=true make test-e2e
```

## Known gaps

- **Authentication needs CamusDB 0.15.2 or later.** It depends on node-to-node mutual TLS. A
  0.15.1 server rejects the setting at startup, and even without it, that server demands a bearer
  token on the Raft and Kahuna gRPC routes, so a multi-node cluster never forms. With 0.15.1, run
  without `spec.auth`, which means scale-down by eviction and no autoscaling.
- **Node-to-node HTTP is plaintext.** CamusDB forwards schema changes and cluster settings between
  nodes over the HTTP port, carrying the shared node secret in a header. Its `peer_tls_enabled`
  setting needs HTTPS on the HTTP port itself, which a cluster node does not serve, so the operator
  leaves it off. Raft and gRPC traffic is encrypted; restrict pod-to-pod traffic with a
  NetworkPolicy if the node secret matters to you.
- **Without authentication, scale-down is eviction.** See the scale-down design decision above.
- **New nodes catch up through Raft, not from a backup.** CamusDB can seed a joining node from a
  backup (`--pitr-bootstrap-from`), but the image must already be restored into the data directory,
  and the node refuses to start with the flag once its WAL has data. The operator does not seed new
  nodes yet.
- **Some backup refusals that clear on retry count as failures.** Kahuna refuses a coordinated
  backup when the transactions in flight overtake its cut, and, under a replication factor, when one
  node's share of the partitions does not arrive. Both clear on the next attempt, but CamusDB 0.16.3
  reports them with its generic backup failure code (`CADB0704`, HTTP 500), which an I/O error also
  uses. The operator therefore counts them as failures: `BackupFailing` turns true until the retry
  after the failure backoff (1 minute at first) succeeds.
- **A long-absent node can require snapshot recovery.** If the required WAL prefix was compacted,
  catch-up must use the leader's snapshot path. Retained PVCs alone do not guarantee log-only
  catch-up.
- **`nodeId` reads 0 for founding roster members.** Kommander seeds the initial roster from
  discovery, which yields endpoints but not node ids. This is intentional upstream and harmless: the
  roster keys on endpoint, and so does this operator.

## Not yet implemented

- Restore, as a CRD or as a field, over the existing `/v1/restore` endpoint
- Seeding scale-up nodes from a backup

## Verified

`make test-e2e` runs the suite below against a real kind cluster: 8 specs. Run it twice, because
authentication changes the scale-down path: `E2E_AUTH=true make test-e2e` (~6 minutes) and
`make test-e2e` (~9 minutes). Both pass 8 of 8 on the published `camusdb/camusdb:0.15.2` image. The
suite now defaults to 0.16.3, and it has not yet run on that image, nor with placement gating or
backups in the operator.

- Bootstrap to 3 Voters with a cert-manager CA issuer in ~21–26s, producing exactly **one**
  StatefulSet revision (no spurious roll). With authentication, the nodes form the cluster over
  mutual TLS.
- SQL round trip through the client Service: create a database and a table, insert, select. With
  authentication, this includes DDL that a follower forwards to the schema leader.
- `replicas: 2` and a `partitions` edit both rejected by the API server
- Scale 3 → 5 in ~43–47s: one node at a time, existing pods untouched (identical UIDs), both new
  nodes started with `--join-existing true`
- Scale 5 → 3, one node at a time:
  - with authentication, in ~36s: the operator asks each node to leave through the leave API, and
    no node is evicted;
  - without authentication, in ~4.6 minutes: each node is evicted, and the operator never calls
    the leave API.
- Data written before the scale still readable after 3 → 5 → 3
- Rolling restart on a `spec.config` change in ~114s: one pod at a time, 0 container restarts, data
  readable afterwards
