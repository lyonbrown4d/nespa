# Nespa

Nespa is a namespace-native, space-isolated, queryable distributed cache platform.

This repository currently contains the first runnable scaffold: a Cobra-powered
Go command with separate modes for the control plane, frontend, data node, admin
API, and local development.

The scaffold follows the design document's foundation-package direction:

- `github.com/arcgolabs/httpx` for HTTP/OpenAPI routes
- `github.com/arcgolabs/configx` for defaults, env, and command-line config
- `github.com/arcgolabs/logx` for structured logging
- `github.com/arcgolabs/dix` as the application/module/lifecycle backbone
- `github.com/arcgolabs/eventx` for in-process lifecycle events
- `github.com/spf13/cobra` for the command tree

The `cmd` package owns the Cobra command tree and directly assembles the `dix`
application. Control, frontend, data node, and admin are modeled as `dix.Module`
values whose HTTP servers start and stop through lifecycle hooks.

## Run

```bash
go run ./cmd dev
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

Node storage smoke test:

```bash
curl -X PUT http://127.0.0.1:7403/v1/node/cache \
  -H 'content-type: application/json' \
  -d '{"namespace":"order-service","space":"session","key":"u1","value":"payload","ttl_ms":60000}'

curl 'http://127.0.0.1:7403/v1/node/cache?namespace=order-service&space=session&key=u1'
curl http://127.0.0.1:7403/v1/node/stats
curl -X DELETE 'http://127.0.0.1:7403/v1/node/cache?namespace=order-service&space=session&key=u1'
```

Run a single component:

```bash
go run ./cmd control
go run ./cmd frontend
go run ./cmd node --quota-space-memory-bytes 104857600
go run ./cmd admin
```

Quota flags use bytes. A value of `0` disables that limit.

## Verify

```bash
go test ./...
```
