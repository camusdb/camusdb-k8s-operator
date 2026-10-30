# Authentication

```yaml
spec:
  auth: {}                              # the operator creates the admin account
  # auth:
  #   adminSecretRef: {name: my-admin}  # or use your own: keys "username" and "password"
```

Needs CamusDB 0.15.2 or later.

## What it turns on

- Users must log in to CamusDB.
- The nodes check each other with client certificates (mutual TLS). See [TLS](tls.md).
- The operator logs in as the admin account over HTTPS.

## What needs it

Without authentication, CamusDB accepts admin calls only from the same machine. So these features
need `spec.auth`:

- Asking a node to leave before its pod is removed. Without it, a scale-down waits about 90 seconds
  per node, and the node's data is not moved first.
- [Autoscaling](autoscaling.md).
- [Backups](backups.md).

The operator can still start, scale and upgrade a cluster without authentication.

## Secrets

- `<name>-auth-keys` holds keys that every node shares. It is created once and never changed.
- `<name>-admin` holds the admin account, unless you set `adminSecretRef`.

The values reach the server through Secret references. They do not appear in the pod spec.

The admin account is created only when the user list is empty, on the first start. If you change
its password later with `ALTER USER`, update the Secret too. Otherwise the operator cannot log in,
and `RosterConverged` says so.

## Turning it on later

Adding `auth` to a running cluster restarts the nodes one at a time. Until all nodes restart, the
nodes not yet restarted still accept requests without a login.
