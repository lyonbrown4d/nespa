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
node      http://127.0.0.1:7403
admin     http://127.0.0.1:7404
```

Health checks:

```bash
curl http://127.0.0.1:7401/healthz
curl http://127.0.0.1:7402/healthz
curl http://127.0.0.1:7403/healthz
curl http://127.0.0.1:7404/healthz
```

Control-plane snapshot:

```bash
curl http://127.0.0.1:7401/v1/control/state
curl http://127.0.0.1:7401/v1/control/snapshot
curl http://127.0.0.1:7401/v1/control/nodes
curl http://127.0.0.1:7402/v1/frontend/routes
```

Node storage smoke test:

```bash
curl -X PUT http://127.0.0.1:7403/v1/node/cache \
  -H 'content-type: application/json' \
  -d '{"namespace":"order-service","space":"session","key":"u1","value":"payload","ttl_ms":60000}'

curl 'http://127.0.0.1:7403/v1/node/cache?namespace=order-service&space=session&key=u1'
curl http://127.0.0.1:7403/v1/node/stats
curl -X DELETE 'http://127.0.0.1:7403/v1/node/cache?namespace=order-service&space=session&key=u1'
```

Frontend gateway smoke test:

```bash
curl -X PUT http://127.0.0.1:7402/v1/cache \
  -H 'content-type: application/json' \
  -d '{"namespace":"order-service","space":"session","key":"u1","value":"payload","ttl_ms":60000}'

curl 'http://127.0.0.1:7402/v1/cache?namespace=order-service&space=session&key=u1'
curl -X DELETE 'http://127.0.0.1:7402/v1/cache?namespace=order-service&space=session&key=u1'
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

The data plane is moving toward a TCP framed protocol. HTTP remains for admin,
debug, health, and console APIs.

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

`metadata` is opaque protocol metadata for now, while `payload` is raw binary
cache value bytes. The codec lives in `protocol`.

## Verify

```bash
go test ./...
```
