# Limitations

## Known gaps

- **Authentication needs CamusDB 0.15.2 or later.** On 0.15.1 a cluster with authentication does
  not form. Run 0.15.1 without `auth`.
- **Node-to-node HTTP is not encrypted.** CamusDB forwards some admin requests between nodes over
  the HTTP port, with a shared secret in a header. Raft and gRPC traffic is encrypted. Use a
  NetworkPolicy to restrict pod-to-pod traffic.
- **Without authentication, scale-down is slow.** The cluster removes each node after about 90
  seconds, and the node's data is not moved first.
- **New nodes copy data from the cluster, not from a backup.** CamusDB can start a new node from a
  backup, but the operator does not do this yet.
- **A node that is away for a long time may need a full copy.** If the cluster has deleted the log
  the node needs, keeping its volume is not enough.
- **Some backup errors that pass on their own count as failures.** CamusDB 0.16.3 reports them with
  the same code as a disk error (`CADB0704`). `BackupFailing` turns `True` until the next retry
  succeeds, usually after 1 minute.
- **`nodeID` shows 0 for founding members.** This is how CamusDB records them. It does not affect
  anything.

## Not built yet

- Restore through the operator.
- Starting new nodes from a backup.
