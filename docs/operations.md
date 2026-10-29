# Operate and Autoscale a CamusDB Cluster

The operator chooses pod counts through the committed CamusDB roster. Use the `CamusDBCluster`, not the managed StatefulSet, as the control surface. The examples assume the operator is installed, the resource is named `camusdb` in namespace `camusdb`, and its keystore is configured as described in the [README](../README.md#tls).

## Found an Autoscaled Cluster with Placement

Create the resource with enough partitions to distribute the workload. For factor 3, beginning with four nodes gives placed partitions from the start; on CamusDB 0.16.0 and later a smaller founding size also works, as described below. Add this to a new cluster's spec:

```yaml
spec:
  replicas: 4
  image: camusdb/camusdb:0.16.3
  partitions: 16
  auth: {}
  replication:
    factor: 3
  autoscaling:
    minReplicas: 4
    maxReplicas: 9
    targetLogEntriesPerSecond: 4000
    targetQueueDepth: 32
    scaleUpStabilizationSeconds: 60
    scaleDownStabilizationSeconds: 300
    tolerancePercent: 10
```

This is a spec fragment; the required storage and TLS fields still apply. Targets are examples to calibrate against a representative workload. `auth` is required: CamusDB serves engine metrics and the leave call to authenticated callers only, and the API server rejects `autoscaling` without it. Authentication also turns on mutual TLS between the nodes, which needs CamusDB 0.15.2 or later.

A cluster founded at three nodes with factor 3 starts with fully replicated partitions, and so does a cluster that had no `replication` before. CamusDB 0.16.0 and later convert those partitions to replica sets within a few rebalancer passes; older servers never convert them. Before a traffic-driven growth step the controller reads placement, and any fully replicated data partition limits growth to three nodes until the conversion is done. An unreadable placement response blocks growth too.

With `replication` set, the operator also writes faster rebalancer pacing into `config.yml`: `kahuna.max_concurrent_replica_transfers: 4`, `kahuna.max_replica_moves_per_pass: 8` and `kahuna.replica_count_deadband: 0`. Set any of them in `spec.config` to override it.

With placed partitions, a data partition has the configured 1–3 replicas, not one per cluster node. More nodes can spread partitions but cannot split a single hot key's write leader. Factor 3 tolerates one unavailable replica of a given data partition; increasing cluster node count does not raise that tolerance. Factor 1 has no data replica fault tolerance.

## Signals and Decisions

The controller resyncs an autoscaled cluster every 15 seconds and reads each eligible ready pod over its HTTPS port, logged in as the cluster's admin account. Network policy must allow those calls; do not disable the dashboard.

- `targetLogEntriesPerSecond` uses the average per-node delta of `kahuna.kv.write.entries` from `GET /v1/dashboard/metrics`: Raft log entries dispatched by the partitions a node leads. Every kind of write produces entries, and one transaction produces several.
- `targetQueueDepth` uses the average per-node sum of `raft.executor.client_queue_depth` from the same route: pending client proposals across partition executors.
- `targetCPUPercent` uses the average per-node `cpu.processCpuPercent` from `GET /v1/admin/stats`: the server process's share of its node over the last minute, 0–100. It is the only signal that sees read load. It needs CamusDB 0.15.2 or later: `camusdb/camusdb:0.15.1` does not serve that route, so with that image this target only ever reports an unreadable signal.

At least one target is required. The largest known signal-to-target ratio drives `ceil(current desired size × ratio)` outside the tolerance band; the recommendation is then clamped to the bounds. Four nodes at twice target recommend eight, but a completed scale-up window requests only five. The controller waits for the expected roster, ready pods and settled replica placement before considering another traffic step. Settled means no replica move in flight, no replica left on a departed node, and every node holding at least half of its even share of the data replicas; the share is waited for at most five minutes after a step. A new step must earn a new stabilization window. One exception: when everything is in place except the newest node, which is not in the roster at all — its pod cannot be scheduled, crash-loops, or cannot pull its image — the controller measures the nodes that are serving, never adds another, and may still step back down after the scale-down window, which removes the node that never joined. `AutoscalingLimited` then names the pod and why it is stuck. A node catching up as a Learner is in the roster and is never cut off this way.

The default windows are 60 seconds up and 300 seconds down, with a 10% dead band. The direction must persist across observations; returning to the band or losing convergence clears the streak. This is not a historical maximum recommendation window and is not a promise that catch-up or draining finishes within the window.

The log-entry rate needs two accepted readings at least ten seconds apart. A counter/time reset or a new pod UID resets its baseline. Operator restarts lose rate samples but retain desired replicas and window timestamps in resource status. An unknown configured signal prevents traffic-driven shrink; another known signal can still justify growth. A failed metrics or stats request holds traffic decisions. Missing rows in a successful metrics response count as zero, and a CPU value of `-1` — the server's "could not read it" — counts as unknown.

There is no memory, latency, read-QPS, or external HPA input. Adding an HPA to the StatefulSet conflicts with the operator's reconciliation.

## Inspect Scaling

```bash
kubectl -n camusdb get camusdbcluster camusdb -w
kubectl -n camusdb get camusdbcluster camusdb -o yaml
kubectl -n camusdb get pods,pvc
kubectl -n camusdb logs camusdb-0 --tail=100
```

Inspect `status.autoscaling.desiredReplicas`, `recommendedReplicas`, `logEntriesPerSecondPerNode`, `queueDepthPerNode`, `cpuPercentPerNode`, `aboveTargetSince`, `belowTargetSince`, and `lastScaleTime`. The recommendation is already bounded by min/max; it does not show uncapped demand. `AutoscalingLimited` explains missing signals/placement, a growth ceiling, or maximum count. `RosterConverged` and `status.members` distinguish a running pod from a committed Voter; when the roster cannot be read, its message quotes the cause, such as a refused login. `Available` is a readiness/quorum summary, not a health proof for every data partition.

Every scale step, manual or autoscaled, waits until the roster has converged and the replica placement has settled: no replica move in flight and no replica on a node that left. An unreadable placement holds the step. `AutoscalingLimited` names the wait for an autoscaled cluster. A rolling update does not wait for placement.

A scale-down asks the highest ordinal to leave before shrinking the StatefulSet. A reachable node's refusal or transport failure holds the count. A highest ordinal the roster does not list — it never joined, or already left — is removed without a leave call, even while the roster has not converged on the current size. If that pod is already absent or has no IP — or authentication is off — the controller cannot decommission it and relies on membership eviction, which takes about 90 seconds; another step waits for roster convergence. Do not force-delete live members to make a stalled scale appear complete.

## Return to a Fixed Size

While autoscaling is enabled, `spec.replicas` is only the initial size. To hand control back deliberately:

```bash
kubectl -n camusdb patch camusdbcluster camusdb --type=merge \
  -p '{"spec":{"autoscaling":null,"replicas":4}}'
```

Removing autoscaling alone restores the old `spec.replicas` value and may immediately request a scale. Editing min/max clamps desired size immediately even without usable traffic metrics; actual membership still moves one node at a time. Two replicas and transitions between a one-node and a multi-node resource are rejected.

## Persistence, Updates, and Backups

Each node's data directory is `/storage/data` on its data PVC: the key/value store in `kv/`, the Raft log in `wal/`, and query spill files in `tmp/`. A `storage.wal` volume is mounted at `/storage/data/wal`, because CamusDB has no setting to move the log elsewhere.

`storage.pvcRetentionPolicy` defaults to `Retain` on scale-down. `Delete` reclaims scaled-away PVCs and their local state. Resource deletion retains PVCs regardless of that field; deleting the whole resource is not a graceful leave sequence. Changing an existing `storage.data.size` does not expand its PVCs: the reconciler does not mutate existing volume-claim templates.

Image, configuration and keystore changes roll pods; topology changes do not restart existing pods just to update seeds. Rollouts hold while the roster is unreadable or nonconverged. Default pod anti-affinity is soft; configure placement constraints for required failure-domain separation.

A CamusDB release that changes its storage revision starts with an empty store and does not convert the old data. Do not raise `spec.image` across such a release on a live cluster; move the data with a logical dump and reimport as the [CamusDB documentation](https://github.com/camusdb/camusdb/blob/main/docs/logical-dump-and-reimport.md) describes.

`spec.backup` schedules coordinated full backups and incrementals to one volume that every node shares; see [Backups](../README.md#backups). It needs `spec.auth`, because the backup routes, like the leave call, answer authenticated callers only. Inspect `status.backup` and the `BackupFailing` and `BackupVolumeReady` conditions. Restore stays manual, and the operator does not seed new nodes from a backup.

See the [chart upgrade instructions](../charts/camusdb-operator/README.md#upgrading-and-the-crd-caveat) before updating CRDs.
