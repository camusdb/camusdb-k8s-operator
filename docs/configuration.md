# Configuration

## Server version

`spec.image` defaults to `camusdb/camusdb:0.16.3`. The default is an exact version, not `latest`,
because a moving tag would restart the cluster on every new release.

Change the version on purpose. The operator builds the server command line for a known release, and
the server stops if it gets an option it does not know.

The stock CamusDB image works as is. The operator replaces its entrypoint with a generated startup
script.

## Typed settings

These spec fields set the most common server options:

| Field | Sets |
|---|---|
| `spec.storage` | Storage backends, volume sizes, and some RocksDB options. |
| `spec.raft` | Heartbeat and election timeouts. |
| `spec.transactions` | Transaction timeouts and admission limits. |
| `spec.workers` | Worker pool sizes. |
| `spec.replication` | How many nodes hold each data partition. |

If you do not set a field, the operator does not write it, and the server's own default applies.

## Other settings

The operator writes a `config.yml` for every node. Use `spec.config` to add any other setting, with
the server's own names. Your values win over the generated ones:

```yaml
spec:
  config:
    slow_query_log_enabled: true
    kahuna:
      max_replica_moves_per_pass: 2
```

- `spec.extraArgs` adds command-line flags. It cannot change a flag the operator already sets.
- `spec.env` adds environment variables, for example `CAMUS_LOG_LEVEL_KAHUNA`.

A config change restarts the nodes one at a time. A change to `replicas` does not.

## Fields you cannot change

The API server rejects a change to these fields after the cluster is created:

- `spec.partitions`: the partition map is built once, at the start.
- `spec.storage.backend` and `spec.storage.walBackend`: the existing data would become unreadable.
