# Nespa

Nespa is a namespace-native, space-isolated, queryable distributed cache platform.

This repository currently contains the first runnable scaffold: a Cobra-powered
Go command that starts the control plane, frontend, data node, and admin API as
one local server.

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

Default local endpoints:

```text
control   http://127.0.0.1:7401
frontend  http://127.0.0.1:7402
node      tcp://127.0.0.1:7403
admin     http://127.0.0.1:7404
```

Health checks:

```bash
curl http://127.0.0.1:7401/healthz
curl http://127.0.0.1:7402/healthz
curl http://127.0.0.1:7404/healthz
```

Control-plane snapshot and vslot routes:

```bash
curl http://127.0.0.1:7401/v1/control/state
curl http://127.0.0.1:7401/v1/control/snapshot
curl http://127.0.0.1:7401/v1/control/namespaces
curl http://127.0.0.1:7401/v1/control/spaces
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
  --frontend-addr 127.0.0.1:7402 \
  --node-addr 127.0.0.1:7403 \
  --node-id node-1 \
  --admin-addr 127.0.0.1:7404
```

Quota flags use bytes. A value of `0` disables that limit.

## Data Protocol

The data plane uses a TCP framed protocol between cache transports. HTTP remains
for control APIs, the Fiber-template frontend WebUI, admin, debug, health, and
console APIs.

SDK hot-path cache operations use direct-route mode: the SDK reads the control
snapshot over HTTP, caches catalog versions and vslot routes locally, then sends
cache frames directly to the selected DataNode TCP address.
The frontend does not proxy cache reads or writes.

Current hot-path ops are `CacheGet`, `CacheSet`, `CacheDelete`, `CacheExists`,
`CacheTouch`, `CacheBatchGet`, and `CacheBatchSet`.

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

The public TCP client SDK lives in `client` and uses `transport/tcp` underneath.
Use `client.NewTCP` for a direct single-node TCP client, or `client.NewRoutedTCP`
to resolve routes from the control plane and connect directly to DataNodes. The
transport client uses `github.com/arcgolabs/clientx/tcp` for dialing, timeouts,
and client policies.

## Verify

```bash
go test ./...
```
