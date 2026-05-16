# Nespa Go SDK

Import path:

```go
import nespa "github.com/lyonbrown4d/nespa/sdk/go"
```

Direct TCP client:

```go
client, err := nespa.NewDirect("127.0.0.1:7403")
```

Routed TCP client:

```go
client, err := nespa.NewRouted("http://127.0.0.1:7401")
if err == nil {
    err = client.Refresh(ctx)
}
```

The SDK exposes stable cache operations:

- `Set`
- `Get`
- `Delete`
- `Exists`
- `Touch`
- `Adjust`
- `BatchSet`
- `BatchGet`
- `BatchDelete`
- `BatchExists`
- `BatchTouch`
- `Primitive`
- `BatchPrimitive`

Use `ErrorCodeOf(err)` to identify Nespa protocol errors such as
`ErrorNoRoute` or `ErrorInvalidArgument`.
