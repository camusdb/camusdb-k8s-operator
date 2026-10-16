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
| **Provisions** | Headless peer Service, client Service, per-node PVCs, ConfigMap (startup script and `config.yml`), PodDisruptionBudget, StatefulSet, and — when asked — the cert-manager Certificate, the keystore derived from it, and the authentication Secrets. |
| **Bootstraps** | Brings up all founding nodes at once so they can form their first quorum, and reports `Running` only when the committed roster carries every node as a Voter. |
| **Scales** | One node at a time, in both directions, each step gated on the roster — never on what Kubernetes merely believes. With authentication on, departing nodes are decommissioned through the cluster's leave API before their pods are removed. |
| **Autoscales** | Optional. Adds and removes nodes with load — Raft log entry rate, client queue depth, or CPU — between a minimum and a maximum, one node at a time, and never grows a cluster in a way that would put more than 3 copies on any data partition. |
| **Upgrades** | Rolling restarts one node at a time, held back automatically whenever the roster is not converged or unreadable. A renewed certificate rolls the nodes onto it the same way. |
| **Reports** | `status.members` mirrors the committed roster joined to the pods serving it, plus `Available` / `Progressing` / `RosterConverged` / `KeystoreReady` conditions. |

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

> **Needs CamusDB 0.15.2 or later**, the default image. Authentication turns on node-to-node mutual
> TLS (`raft_mutual_tls_enabled`), which 0.15.1 does not have and rejects at startup.

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
health and placement routes answer anyone. Graceful decommissioning and autoscaling need it.

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
window and only once the roster has converged on the previous step. A step up whose node never
joins — its pod cannot be scheduled or keeps failing — blocks further growth but not a step back
down, so a stuck pod cannot pin the cluster at a size it never reached. Removal goes through the
same leave-then-delete path as a manual scale-down.

What the autoscaler chose lives in `status.autoscaling` (`desiredReplicas`, `recommendedReplicas`,
the measured signals and the window timestamps), and `spec.replicas` is ignored after the first
pass. Removing `spec.autoscaling` hands the count back to `spec.replicas`.

**Why `replication` is required.** Without a replication factor every node holds every partition,
so a node added is one more copy of all data and one more voter on every write: more cost, no more
write capacity. With `factor: 3` the operator writes `kahuna.replication_factor: 3` and
`kahuna.enable_placement_rebalancer: true`; the rebalancer is what gives a newly added node any data
at all.

**Create the cluster with more nodes than the factor.** The server places a partition only if it is
created while the cluster has more nodes than the factor, and it never converts a fully replicated
partition later. A cluster founded at 3 nodes with factor 3 therefore stays fully replicated
forever. The operator checks `GET /v1/cluster/placement` before every scale-up and refuses to grow
such a cluster past 3 nodes, reporting why in the `AutoscalingLimited` condition.

**Signals and unknowns.** The autoscaler holds — and says so in `AutoscalingLimited` — when any
ready node's signals cannot be read, and it never scales down on a partial view, since the missing
node may be the leader carrying the load. Log entries are not statements: one transaction writes
several (its prepare, decision and settle), so calibrate the target against a representative
workload. CPU is the only signal that sees read load. `targetCPUPercent` needs a server that serves
`/v1/admin/stats`, which CamusDB 0.15.2 does and 0.15.1 does not.

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

`spec.image` defaults to an exact tag, `camusdb/camusdb:0.15.2`, rather than to `latest`. A floating
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
- **No backup or PITR seeding.** CamusDB can seed a joining node from a backup
  (`--pitr-bootstrap-from`), but the image must already be restored into the data directory, and the
  node refuses to start with the flag once its WAL has data. The operator has no backup storage
  mount or seed-on-join orchestration.
- **A long-absent node can require snapshot recovery.** If the required WAL prefix was compacted,
  catch-up must use the leader's snapshot path. Retained PVCs alone do not guarantee log-only
  catch-up.
- **`nodeId` reads 0 for founding roster members.** Kommander seeds the initial roster from
  discovery, which yields endpoints but not node ids. This is intentional upstream and harmless: the
  roster keys on endpoint, and so does this operator.

## Not yet implemented

- Backup/restore CRDs over the existing `/v1/backups/*` endpoints
- Scheduled coordinated backups and automatic scale-up seeding

## Verified

`make test-e2e` runs the suite below against a real kind cluster: 8 specs on the published
`camusdb/camusdb:0.15.2` image. Run it twice, because authentication changes the scale-down path:
`E2E_AUTH=true make test-e2e` (~6 minutes) and `make test-e2e` (~9 minutes). Both pass 8 of 8.

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
