# Nespa

Nespa is a namespace-native, space-isolated, queryable distributed cache platform.

This repository currently contains the first runnable scaffold: a Cobra-powered
Go command that starts the control plane and data node as core services in one local
server. The same binary can also run control-only or node-only by flag. Frontend
and admin are optional services.

The scaffold follows the design document's foundation-package direction:

- `github.com/arcgolabs/httpx` for HTTP/OpenAPI routes
- `github.com/arcgolabs/configx` for defaults, env, and command-line config
- `github.com/arcgolabs/logx` for structured logging
- `github.com/arcgolabs/dix` as the application/module/lifecycle backbone
- `github.com/arcgolabs/eventx` for in-process lifecycle events
- `github.com/spf13/cobra` for the command tree

The `cmd` package owns CLI flags and assembles the `dix` application. Runtime
components expose plain constructors, HTTP configs, and lifecycle functions;
`cmd` wraps them as `dix` modules for the server binary.

## Run

```bash
go run ./cmd
```

### Smoke Validation

```bash
pwsh ./scripts/smoke.ps1
pwsh ./scripts/smoke-multinode.ps1
```

The smoke script starts a local core server, creates `orders/session/SessionView`
catalog metadata, waits for route materialization, runs routed TCP set/get,
primitive, batch primitive, and quota rejection checks, and validates admin
summary when admin is enabled.
The multinode smoke starts one control process and two data-node processes,
validates cross-node routed batch writes and batch primitive grouping, stops one
node, waits for route shrink, and verifies writes continue through the surviving
route.

Optional overrides and feature toggles can be passed, e.g.:

```bash
pwsh ./scripts/smoke.ps1 -ControlAddr 127.0.0.1:17401 -FrontendEnabled true -FrontendAddr 127.0.0.1:17402 -NodeAddr 127.0.0.1:17403 -AdminAddr 127.0.0.1:17404
pwsh ./scripts/smoke.ps1 -ControlAddr 127.0.0.1:17401 -AdminEnabled false
```

Default local endpoints:

```text
control   http://127.0.0.1:7401
raft      tcp://127.0.0.1:7601
node      tcp://127.0.0.1:7403
admin     http://127.0.0.1:7404 (optional)
frontend  http://127.0.0.1:7402 (optional, disabled by default)
```

Health checks:

```bash
curl http://127.0.0.1:7401/healthz
curl http://127.0.0.1:7404/healthz
# optional frontend
curl http://127.0.0.1:7402/healthz
```

If frontend is disabled, skip `curl` calls against `http://127.0.0.1:7402/*`.
If admin is disabled, skip calls against `http://127.0.0.1:7404/*`.

Control-plane snapshot and vslot routes:

```bash
curl http://127.0.0.1:7401/v1/control/state
curl http://127.0.0.1:7401/v1/control/snapshot
curl http://127.0.0.1:7401/v1/control/namespaces
curl http://127.0.0.1:7401/v1/control/spaces
curl http://127.0.0.1:7401/v1/control/entities
curl http://127.0.0.1:7401/v1/control/nodes
curl http://127.0.0.1:7401/v1/control/rebalance/events
curl http://127.0.0.1:7401/v1/control/rebalance/plans
# optional frontend route cache view
curl http://127.0.0.1:7402/routes
```

Create local catalog metadata:

```bash
curl -X POST http://127.0.0.1:7401/v1/control/namespaces \
  -H 'content-type: application/json' \
  -d '{"namespace":"orders"}'

curl -X POST http://127.0.0.1:7401/v1/control/spaces \
  -H 'content-type: application/json' \
  -d '{"namespace":"orders","space":"session"}'

curl -X POST http://127.0.0.1:7401/v1/control/entities \
  -H 'content-type: application/json' \
  -d '{"namespace":"orders","space":"session","entity":"SessionView"}'
```

Flush by version bump:

```bash
curl -X POST http://127.0.0.1:7401/v1/control/namespaces/version-bump \
  -H 'content-type: application/json' \
  -d '{"namespace":"orders"}'

curl -X POST http://127.0.0.1:7401/v1/control/spaces/version-bump \
  -H 'content-type: application/json' \
  -d '{"namespace":"orders","space":"session"}'
```

Useful startup flags:

```bash
go run ./cmd \
  --control-enabled=true \
  --control-addr 127.0.0.1:7401 \
  --control-cluster-id local \
  --node-enabled=true \
  --node-addr 127.0.0.1:7403 \
  --node-id node-1 \
  --admin-enabled=true \
  --admin-addr 127.0.0.1:7404
```
```bash
go run ./cmd \
  --control-enabled=true \
  --control-addr 127.0.0.1:7401 \
  --control-cluster-id local \
  --node-enabled=false \
  --admin-enabled=false
```
```bash
go run ./cmd \
  --control-enabled=false \
  --control-addr 127.0.0.1:7401 \
  --node-enabled=true \
  --node-addr 127.0.0.1:7503 \
  --node-id node-2 \
  --admin-enabled=false
```

Quota flags use bytes. A value of `0` disables that limit.

## Data Protocol

The data plane uses a TCP framed protocol between cache transports. HTTP remains
for control APIs, the Fiber-template frontend WebUI, admin, debug, health, and
console APIs.

SDK hot-path cache operations use direct-route mode: the SDK reads the control
snapshot over HTTP, caches catalog versions and vslot routes locally, then sends
cache frames directly to the selected DataNode TCP address.
If a DataNode reports a stale `route_epoch`, the routed SDK refreshes the
snapshot and retries the operation once.
The frontend does not proxy cache reads or writes.

Current hot-path ops are `CacheGet`, `CacheSet`, `CacheDelete`, `CacheExists`,
`CacheTouch`, `CacheAdjust`, `CacheBatchGet`, `CacheBatchSet`,
`CacheBatchDelete`, `CacheBatchExists`, `CacheBatchTouch`, `CachePrimitive`,
and `CacheBatchPrimitive`.

The current frame header is fixed-width and big-endian:

```text
magic        uint32  "NSPA"
version      uint8
flags        uint8
op           uint16
request_id   uint64
route_epoch  uint64
metadata_len uint32
payload_len  uint32
metadata     []byte
payload      []byte
```

`metadata` carries `cachewire` binary metadata for cache ops, while `payload`
carries raw cache value bytes. Protocol error frames still carry `cachewire.Error`
JSON. Batch set/get and batch primitive metadata store payload offsets so values
do not need to be JSON encoded. Batch operations execute in request order and
return already completed results if a later item fails. DataNodes reject non-zero
`route_epoch` values older than the node's latest observed control revision. The
frame codec lives in `protocol`. Routed clients refresh the control snapshot and
retry once when a node reports a stale route or when a dead-node connection fails
and the refreshed route has changed.

The control plane exposes rebalance event and migration-plan views at
`/v1/control/rebalance/events` and `/v1/control/rebalance/plans`. Events record
route-table changes such as node join, node suspect/dead, node recovery, address
changes, and space route creation. Migration plans derive vslot ranges that must
move between source and target nodes. DataNodes expose internal TCP binary ops to
export a namespace/space/vslot range, import the snapshot on the target, and
delete the source range. The control migration executor is enabled by default:
it claims planned tasks through the control FSM, runs `export -> import ->
delete`, and marks tasks and plans as `running`, `done`, or `failed`. Production
retry/backoff policy remains a next stage. DataNodes cache the control snapshot
and use route `replicas` for best-effort async write-after replication after a
primary `set`, `delete`, `touch`, `adjust`, primitive mutation, or mutating
batch. Replication is represented as explicit wire commands and sent through a
bounded dispatcher queue to preserve async send order and retry transient send
failures in memory. When `--node-replication-outbox-path` is set, the primary
also appends each queued replication command to a local JSONL outbox; restart
scans that file to continue sequence numbering without replaying non-idempotent
mutations and persists per-replica ack offsets in a sidecar
`<outbox>.acks.json` file after successful sends. Replay and replica catch-up are
the next stage. The admin summary exposes queue, drop, retry, outbox, ack,
sequence, success, and failure counters for this replication path.

Control writes run through a Dragonboat-backed Raft state machine by default.
Use `--control-raft-dir` to persist the Dragonboat NodeHost data; when empty,
the server uses temporary runtime storage. `--control-snapshot-path` remains an
optional JSON import/export helper. DataNode memory-engine snapshots can be
restored/saved with `--node-snapshot-path`; this is a local persistence
foundation, not yet a replicated data log.

The primary data plane remains a versioned binary KV and primitive collection
plane. It supports `set/get/delete/exists/touch/adjust`, batch variants, and a
small native primitive set for `counter`, `map`, `set`, `scored set`, binary
`list`, `bitmap`, `hyperloglog`, and `geo` values. The core cache service also
has a transaction callback API that serializes a group of operations through the
same service/quota/ExpectedVersion path; it is the current foundation for RESP
`MULTI`/`EXEC`, not a cross-node two-phase transaction layer.

Redis RESP compatibility is an optional ingress layer for client migration and
deployment convenience, not the internal architecture. For the supported command
subset, external Redis clients should only need to replace the server address.
RESP connections must use `AUTH username password`; `username` maps to the
Nespa namespace. Redis logical DB selection maps to Nespa space, for example
`SELECT 0` maps to `0-space`. After RESP parsing, requests are translated into
Nespa service/primitive operations and routed through the existing TCP
binary/DataNode model.

The current RESP command subset covers connection setup and the common
non-stream data structures:

- Connection: `AUTH`, `HELLO`, `PING`, `QUIT`, `SELECT`, `CLIENT`, `COMMAND`
- String/counter/batch: `GET`, `SET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL`,
  `INCR`, `DECR`, `INCRBY`, `DECRBY`, `MGET`, `MSET`, `TYPE`
- Hash: `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN`
- Set: `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`
- List: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`
- Sorted set: `ZADD`, `ZREM`, `ZRANGE`, `ZCARD`
- Bitmap: `SETBIT`, `GETBIT`, `BITCOUNT`
- HyperLogLog: `PFADD`, `PFCOUNT`, `PFMERGE`
- Geo: `GEOADD`, `GEODIST`, `GEORADIUS`
- Transaction queue: `MULTI`, `EXEC`, `DISCARD`

Compatibility boundary: Nespa does not promise complete Redis Cluster, Lua,
Stream, PubSub, WATCH/Lua transaction, or full command compatibility.

The public TCP client SDK lives in `client` and uses `transport/tcp` underneath.
Use `client.NewTCP` for a direct single-node TCP client, or `client.NewRoutedTCP`
to resolve routes from the control plane and connect directly to DataNodes. The
transport client uses `github.com/arcgolabs/clientx/tcp` for dialing, timeouts,
and client policies.

## Verify

```bash
go test ./...
```
