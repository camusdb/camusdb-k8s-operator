# Development

## Build and test

```bash
make test                                    # unit tests and envtest
make test-e2e                                # kind e2e (creates and deletes its own cluster)
make lint                                    # golangci-lint
make helm-lint                               # copy the CRD and RBAC into the chart, then lint it
```

After you change `api/v1alpha1/*_types.go` or a `+kubebuilder` marker, run
`make manifests generate`.

## Run in a local cluster

```bash
make docker-build IMG=camusdb-operator:dev
kind load docker-image camusdb-operator:dev --name <cluster>
make install                                 # CRDs
make deploy IMG=camusdb-operator:dev
kubectl apply -f config/samples/camusdb_v1alpha1_camusdbcluster.yaml
```

`make run` from your laptop does not work with kind. The operator calls the pods by IP, and pod IPs
are not reachable from the host. Deploy the operator in the cluster, or use a port-forward.

## Test a server built from source

```bash
cd ../camusdb && docker build -f docker/Dockerfile.standalone -t camusdb/camusdb:dev .
CAMUSDB_IMAGE=camusdb/camusdb:dev CAMUSDB_IMAGE_SKIP_PULL=true make test-e2e
```

## The e2e suite

Run it twice, because authentication changes how nodes leave:

```bash
E2E_AUTH=true make test-e2e   # about 6 minutes
make test-e2e                 # about 9 minutes
```

It has 8 specs. Both runs passed 8 of 8 on `camusdb/camusdb:0.15.2`. The suite now uses 0.16.3, and
it has not run on that version yet, nor with the placement checks or backups.

The specs check:

- A new 3-node cluster with a cert-manager CA starts in about 25 seconds, with no extra rollout.
  With authentication, the nodes connect with mutual TLS.
- SQL works through the client Service: create a database and a table, insert, select.
- The API server rejects `replicas: 2` and a change to `partitions`.
- Scaling from 3 to 5 adds one node at a time and does not restart the existing pods.
- Scaling from 5 to 3 removes one node at a time: with authentication, through the leave call (about
  36 seconds); without it, by eviction (about 4.6 minutes).
- Data written before the scale is still readable after 3 → 5 → 3.
- A `spec.config` change restarts one pod at a time, with no container restarts.
