# CamusDB Kubernetes Operator (Alpha)

Runs [CamusDB](https://github.com/camusdb/camusdb) clusters on Kubernetes. CamusDB is a distributed
SQL database built on Raft.

A Raft cluster cannot be managed safely as a plain StatefulSet. Each node needs its own identity and
peer list, and nodes must join and leave the cluster one at a time. The operator handles this for
you.

## What it does

- **Creates** the StatefulSet, Services, volumes, config and PodDisruptionBudget for a cluster.
- **Starts** a new cluster and marks it `Running` when every node is a voting member.
- **Scales** one node at a time, and waits for the data to move before the next step.
- **Autoscales** on load, if you turn it on.
- **Upgrades** one node at a time.
- **Backs up** on a schedule to a shared volume, if you turn it on.
- **Manages TLS** with your own certificate or with cert-manager.

## Quick start

Install the operator:

```bash
helm install camusdb-operator ./charts/camusdb-operator \
  --namespace camusdb-system --create-namespace
```

Create a cluster. This example uses a cert-manager CA issuer named `camusdb-ca`:

```yaml
apiVersion: camusdb.io/v1alpha1
kind: CamusDBCluster
metadata:
  name: camusdb
spec:
  replicas: 3
  auth: {}
  storage:
    data:
      size: 100Gi
  tls:
    certManager:
      issuerRef: {name: camusdb-ca, kind: Issuer}
```

Check it:

```
$ kubectl get camusdbcluster
NAME      DESIRED   AUTOSCALED   READY   VOTERS   PHASE     AGE
camusdb   3                      3       3        Running   12m
```

To change the size, edit `spec.replicas`. Use 1, or 3 or more. The operator rejects 2, and it
cannot grow a 1-node cluster.

## Documentation

| Topic | |
|---|---|
| [How it works](docs/how-it-works.md) | The rules the operator follows, and why. |
| [Configuration](docs/configuration.md) | Spec fields, server settings, and the server version. |
| [TLS](docs/tls.md) | Certificates for the nodes. |
| [Authentication](docs/authentication.md) | The admin account, and what needs it. |
| [Autoscaling](docs/autoscaling.md) | Scaling on load. |
| [Backups](docs/backups.md) | Scheduled backups. |
| [Operations](docs/operations.md) | Scaling by hand, status, volumes and upgrades. |
| [Limitations](docs/limitations.md) | Known gaps and what is not built yet. |
| [Development](docs/development.md) | Build, test and run locally. |
| [Helm chart](charts/camusdb-operator/README.md) | Install options and CRD upgrades. |
