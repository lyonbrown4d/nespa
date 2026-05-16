# Nespa SDKs

This directory contains public SDKs for Nespa cache clients.

The repository keeps SDKs separate from the core server packages:

- `sdk/go` is the public Go SDK module.
- `sdk/java` is the public Java SDK module.

Core packages such as `protocol`, `cachewire`, `transport/tcp`, and `client`
remain implementation packages for the Nespa server and Go internals. SDKs wrap
those internals behind stable user-facing APIs.

## Layout

```text
sdk/
  go/      Go SDK module
  java/    Java SDK module
```

## Release Model

Each SDK owns its build metadata and can be released independently:

- Go: `github.com/lyonbrown4d/nespa/sdk/go`
- Java: `io.github.lyonbrown4d:nespa-java`

The root `go.work` file is only for local multi-module development. It is not a
publishing contract.
