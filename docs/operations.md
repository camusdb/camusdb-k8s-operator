# Operations

The examples use a cluster named `camusdb` in the namespace `camusdb`. Always change the
`CamusDBCluster`, not the StatefulSet that the operator manages.

## Scale by hand

Edit `spec.replicas`:

```bash
kubectl -n camusdb patch camusdbcluster camusdb --type=merge -p '{"spec":{"replicas":5}}'
```

The operator moves one node at a time. Before each step it waits until:

- the roster matches the current size, and
- no data is still moving, and no data is left on a removed node.

If either cannot be read, it waits. A rolling restart does not wait for the data.

To remove a node, the operator asks the highest-numbered node to leave, then deletes its pod. If
the node refuses or cannot be reached, the scale waits. Without [authentication](authentication.md),
or if the pod is already gone, the cluster removes the node after about 90 seconds.

Do not force-delete pods to make a stuck scale finish.

## Check the cluster

```bash
kubectl -n camusdb get camusdbcluster camusdb -w
kubectl -n camusdb get camusdbcluster camusdb -o yaml
kubectl -n camusdb get pods,pvc
kubectl -n camusdb logs camusdb-0 --tail=100
```

| Status | Meaning |
|---|---|
| `phase` | A summary: `Pending`, `Bootstrapping`, `Running`, `ScalingUp`, `ScalingDown`, `Upgrading` or `Degraded`. |
| `status.members` | The cluster's member list, with the pod for each member. |
| `status.currentImage` | The image every node runs. |
| `Available` | A majority of nodes is ready. |
| `Progressing` | The operator is starting, scaling or upgrading. |
| `RosterConverged` | The member list matches the size you asked for. If it cannot be read, the message says why. |
| `KeystoreReady` | The certificate Secret exists. |
| `AutoscalingLimited` | See [Autoscaling](autoscaling.md). |
| `BackupFailing`, `BackupVolumeReady` | See [Backups](backups.md). |

## Volumes

- Each node keeps its data on its own volume, at `/storage/data`.
- `storage.wal` puts the Raft log on a separate volume.
- `storage.pvcRetentionPolicy` decides what happens to a removed node's volume: `Retain` (default)
  or `Delete`.
- Deleting the cluster never deletes the volumes.
- Changing `storage.data.size` does not resize existing volumes.

## Upgrades

A change to the image, the config or the certificate restarts the nodes one at a time. A change to
the size does not restart existing nodes.

A CamusDB release that changes its storage format starts with an empty store. Do not upgrade a live
cluster across such a release. Move the data with a
[dump and reimport](https://github.com/camusdb/camusdb/blob/main/docs/logical-dump-and-reimport.md)
instead.

Read the [chart upgrade notes](../charts/camusdb-operator/README.md#upgrading-and-the-crd-caveat)
before you update the CRDs.

## Placement

The default pod anti-affinity only prefers to spread nodes across machines. Set `affinity` or
`topologySpreadConstraints` if you need a strict spread.
