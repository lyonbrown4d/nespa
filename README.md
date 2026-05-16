# Nespa

Nespa is a namespace-native, space-isolated, queryable distributed cache platform.

This repository currently contains the first runnable scaffold: a Cobra-powered
Go command that starts the control plane and data node as core services in one local
server. Frontend and admin are optional via flags and can be disabled.

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
```

The smoke script starts a local server, creates `orders/session/SessionView` catalog
metadata, waits for route materialization, runs a routed TCP set/get, and
validates admin summary when admin is enabled.

Optional overrides and feature toggles can be passed, e.g.:

```bash
pwsh ./scripts/smoke.ps1 -ControlAddr 127.0.0.1:17401 -FrontendAddr 127.0.0.1:17402 -NodeAddr 127.0.0.1:17403 -AdminAddr 127.0.0.1:17404
pwsh ./scripts/smoke.ps1 -ControlAddr 127.0.0.1:17401 -FrontendEnabled $false -AdminEnabled $false
```

Default local endpoints:

```text
control   http://127.0.0.1:7401
frontend  http://127.0.0.1:7402 (optional, can be disabled)
node      tcp://127.0.0.1:7403
admin     http://127.0.0.1:7404 (optional, can be disabled)
```

Health checks:

```bash
curl http://127.0.0.1:7401/healthz
curl http://127.0.0.1:7402/healthz
curl http://127.0.0.1:7404/healthz
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
  --control-addr 127.0.0.1:7401 \
  --control-cluster-id local \
  --frontend-enabled true \
  --frontend-addr 127.0.0.1:7402 \
  --node-addr 127.0.0.1:7403 \
  --node-id node-1 \
  --admin-enabled true \
  --admin-addr 127.0.0.1:7404
```
```bash
go run ./cmd \
  --control-addr 127.0.0.1:7401 \
  --control-cluster-id local \
  --frontend-enabled false \
  --admin-enabled false
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
`CacheTouch`, `CacheAdjust`, `CacheBatchGet`, and `CacheBatchSet`.

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
JSON. Batch set/get metadata stores payload offsets so values do not need to be
JSON encoded. DataNodes reject non-zero `route_epoch` values older than the
node's latest observed control revision. The frame codec lives in `protocol`.

The data plane is intentionally a versioned binary KV plane (`set/get/delete/exists/touch/adjust`
and batch variants). Redis command compatibility and native Redis data-structure
operations are not part of this stage.

The public TCP client SDK lives in `client` and uses `transport/tcp` underneath.
Use `client.NewTCP` for a direct single-node TCP client, or `client.NewRoutedTCP`
to resolve routes from the control plane and connect directly to DataNodes. The
transport client uses `github.com/arcgolabs/clientx/tcp` for dialing, timeouts,
and client policies.

## Verify

```bash
go test ./...
```
