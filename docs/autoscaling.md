# Autoscaling

The operator can add and remove nodes based on load.

```yaml
spec:
  replicas: 4                        # only the starting size once autoscaling is on
  auth: {}                           # required
  replication:
    factor: 3                        # required: 1 to 3 copies of each data partition
  autoscaling:
    minReplicas: 3
    maxReplicas: 9
    targetLogEntriesPerSecond: 4000  # writes per node
    targetQueueDepth: 32             # waiting requests per node
    targetCPUPercent: 70             # CPU per node
    # scaleUpStabilizationSeconds: 60
    # scaleDownStabilizationSeconds: 300
    # tolerancePercent: 10
```

Set at least one target. The values above are examples. Measure your own workload to choose them.

## Requirements

- **`auth`**: CamusDB gives its metrics only to logged-in callers. See
  [Authentication](authentication.md).
- **`replication`**: without it every node holds all data, so a new node adds cost but no capacity.
  With it, each data partition lives on `factor` nodes, and new nodes take a share.

## Signals

Every 15 seconds the operator reads each ready node:

| Target | Source |
|---|---|
| `targetLogEntriesPerSecond` | `kahuna.kv.write.entries` from `GET /v1/dashboard/metrics`. One transaction writes several entries. |
| `targetQueueDepth` | `raft.executor.client_queue_depth` from the same route. |
| `targetCPUPercent` | `cpu.processCpuPercent` from `GET /v1/admin/stats`, over the last minute. This is the only signal that sees read load. Needs CamusDB 0.15.2 or later. |

Each value is averaged per node.

## How it decides

1. It computes a size like the HorizontalPodAutoscaler: current size × (signal ÷ target), using the
   highest signal. Inside the tolerance band, nothing changes.
2. The result must stay above or below the current size for the whole stabilization window: 60
   seconds up, 300 seconds down by default.
3. Then it changes the size by **one node**.
4. It waits until the step is complete before it considers another. Complete means:
   - the roster matches the new size and all pods are ready,
   - no replica is still moving, and no replica is left on a removed node,
   - every node holds some data. This part is waited for at most 5 minutes.

Some rules keep it safe:

- If a signal cannot be read, it does not scale down. The missing node may be the busy one.
- If a new node never joins (its pod cannot be scheduled or keeps crashing), it adds no more nodes.
  It can still scale down, which removes that node.
- A scale-down asks the node to leave first, as in a manual scale-down.

## Replication factor and cluster size

A partition is copied to every node when it is created on a cluster with no more nodes than the
factor, or when the cluster had no factor. CamusDB 0.16.0 and later reduce these partitions to the
factor within a few minutes. Older versions never do.

While any data partition is still on every node, the autoscaler stops at 3 nodes. Each extra node
would be one more copy of everything. `AutoscalingLimited` says when this happens.

With a factor, the operator also speeds up the rebalancer that moves data to new nodes. It sets
`kahuna.max_concurrent_replica_transfers: 4`, `kahuna.max_replica_moves_per_pass: 8` and
`kahuna.replica_count_deadband: 0`. Override them in `spec.config` if the data moves compete with
client traffic.

## Status

The autoscaler keeps its state in `status.autoscaling`:

- `desiredReplicas`: the size it chose. This replaces `spec.replicas`.
- `recommendedReplicas`: what the latest signals ask for.
- the measured signals, and the times of the windows and the last step.

The `AutoscalingLimited` condition says why it cannot scale: a limit, a missing signal, or the
replication limit. While it waits for data to move, the message says so, but the condition stays
`False`.

## Turn it off

```bash
kubectl -n camusdb patch camusdbcluster camusdb --type=merge \
  -p '{"spec":{"autoscaling":null,"replicas":4}}'
```

If you remove `autoscaling` but do not set `replicas`, the cluster goes back to the old
`spec.replicas`, which can start a scale.

Do not put a HorizontalPodAutoscaler on the StatefulSet. It conflicts with the operator.
