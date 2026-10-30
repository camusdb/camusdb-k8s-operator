# How It Works

This page explains the rules the operator follows. You do not need it to run a cluster, but it
helps when something looks stuck.

## The cluster decides who is a member

CamusDB keeps a list of its members, called the roster. A change to the roster is agreed by the
cluster, one node at a time. The operator trusts this list, not Kubernetes. A running pod is not
always a member, and a member can still be listed after its pod is gone.

The operator reads the roster from `GET /v1/cluster/membership` before each step.

## Starting a new cluster

- All founding nodes start together. A node cannot become ready until a majority of nodes is up,
  so starting them one by one would never finish. This is why the StatefulSet uses
  `podManagementPolicy: Parallel`.
- The peer Service publishes pods that are not ready yet, so the nodes can find each other.
- The cluster is `Running` only when the roster lists every node as a voter.
- The founding size is saved in `status.bootstrapSize` and never changes. A node with a lower
  ordinal forms the cluster. A node with a higher ordinal joins the running cluster.

## Scaling

- The operator adds or removes **one node at a time**.
- Before each step, it waits until the roster matches the current size.
- It also waits until the data has moved: no replica is still moving, and no replica is left on a
  node that left. This comes from `GET /v1/cluster/placement`. Removing a second node before the
  first one's data is copied again could lose a copy.
- To remove a node, the operator first asks it to leave the cluster. It deletes the pod only after
  the cluster confirms. This needs [authentication](authentication.md). Without it, the operator
  deletes the pod and the cluster removes the node after about 90 seconds.
- A scale does not restart the existing nodes. The cluster size is in a separate file that the
  pods read at startup, so the pod template does not change.

## Upgrades and config changes

A change to the image, the config or the certificate restarts the nodes one at a time. The
operator holds the restart while the roster cannot be read or does not match.

`status.currentImage` shows the image that every node runs. It changes only after all nodes run
the new image and are ready.

## Health checks

- **Liveness** uses `/ping`. It only checks that the server answers. A node that is up but not yet a
  member is healthy, and a restart would not help it.
- **Readiness** uses `/v1/cluster/health`. A node gets client traffic only when it is a serving
  member.

## Certificates come first

The operator does not create the StatefulSet until the keystore Secret exists. Pods created without
it would be replaced as soon as it appears, and a new cluster cannot finish that rollout. Until then
the cluster is `Pending` and `KeystoreReady` is `False`.

## Cluster sizes

- **2 nodes are rejected.** Two voters need both nodes for a majority, so they tolerate no failure.
- **1 node and many nodes cannot convert.** A 1-node cluster forms by itself, and the operator cannot
  grow it in place. Create a new cluster instead.
