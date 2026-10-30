# Backups

The operator can take backups on a schedule. All nodes write to one shared volume.

```yaml
spec:
  auth: {}                              # required
  backup:
    schedule: "0 2 * * *"               # full backups (cron, UTC)
    incrementalSchedule: "*/15 * * * *" # optional
    volume:
      claim:                            # or claimName: <your ReadWriteMany claim>
        size: 100Gi
        storageClassName: filestore     # must support ReadWriteMany
    retention:
      maxChains: 7
      # maxAge: 168h
      # maxBytes: 500Gi
    # clusterID: prod/camusdb           # default: <namespace>/<name>
    # macKeySecretRef: {name: my-key, key: key}
    # timeout: 1h
    # suspend: false
```

## Requirements

- **`auth`**: CamusDB accepts backup calls only from logged-in admins. See
  [Authentication](authentication.md).
- **A ReadWriteMany volume** when the cluster has more than one node. Every node mounts it.
  Block storage cannot do this: Kind's `local-path`, EBS, Persistent Disk and Azure Disk. Use file
  storage, such as NFS, Filestore, EFS, Azure Files or CephFS.
- **CamusDB 0.16.0 or later** when the cluster can have more nodes than the replication factor.
  Older versions back up only part of the data in that case, so the operator refuses them.

## How it works

- **Full backups** are taken at one point in time across the whole cluster. Only one node can take
  them, so the operator tries each node until one accepts.
- **Incremental backups** add the changes since the last backup.
- The first full backup runs as soon as the cluster is ready. After that, backups follow the
  schedules. Missed runs become one run, like a CronJob.
- A backup runs in the background. Scaling and upgrades do not wait for it.
- A backup waits until the cluster is steady: all nodes ready, on the current config and image.
- Turning on backups restarts the nodes one at a time.
- Old backups are deleted by the retention settings. The newest chain is always kept.

## Status

- `status.backup` shows the last backup, its type, and any failures.
- `BackupFailing` is `True` when the last attempt failed. The operator retries after 1 minute, then
  waits longer each time, up to 30 minutes.
- Right after a cluster starts, no node may be ready to take a backup. The operator retries every
  15 seconds and does not count this as a failure for the first 5 minutes.
- `BackupVolumeReady` is `False` while the volume is not bound. Every pod needs the volume, so no
  pod starts until it binds. The message gives the reason, for example a storage class without
  ReadWriteMany.

Before the volume binds, you can fix `volume.claim` and the operator replaces the claim. After it
binds, the operator does not change it.

## What is kept

The `<name>-backup` claim and the `<name>-backup-mac` signing key are **not** deleted with the
cluster or when you remove `backup`. You need the key to restore a backup.

## Restore

Restore is manual. See the
[CamusDB backup guide](https://github.com/camusdb/camusdb/blob/main/docs/backups-and-point-in-time-recovery.md).

## Testing on Kind

Kind has no ReadWriteMany storage class. On a single-node Kind cluster, a hostPath volume works,
because all pods run on the same machine:

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
