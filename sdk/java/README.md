# Nespa Java SDK

This module is the Java SDK for Nespa's TCP binary cache protocol.

Build metadata uses Gradle Kotlin DSL. Dependency and plugin versions are kept
in `gradle/libs.versions.toml`. The wrapper task is pinned to Gradle `9.5.1`.
The TCP transport is implemented with Netty.

The SDK is JPMS-aware:

- Module name: `io.github.lyonbrown4d.nespa`
- Exported API package: `io.github.lyonbrown4d.nespa`
- Internal protocol/transport package: not exported

```bash
./gradlew build
```

On Windows:

```powershell
.\gradlew.bat build
```

The first Java surface is a direct TCP client for:

- `set`
- `get`
- `delete`
- `exists`
- `touch`
- `adjust`
- `primitive`
- `batchPrimitive`

`primitive` and `batchPrimitive` cover counter, map, set, and scored-set
operations over the TCP binary protocol.

General `batchSet` / `batchGet` helpers and routed control-plane clients are
planned as the next layer.
