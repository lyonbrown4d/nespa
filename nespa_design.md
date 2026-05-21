# Nespa 分布式缓存平台设计文档

> Draft v0.1
> 更新时间：2026-05-16
> 目标读者：核心研发、平台研发、SRE、架构评审人员

---

## 1. 项目定位

Nespa 是一个 **namespace-native、space-isolated、queryable、control-plane-driven** 的分布式缓存平台。

它不是 Redis-compatible replacement，也不是传统 KV 数据库。Nespa 的核心目标是为多应用、多进程、多缓存域提供统一缓存运行时，并从根源上支持：

- 应用级 namespace 隔离
- namespace 下的多个 cache space/database
- space 级 quota、TTL、eviction、query policy
- process/principal 级访问控制
- 内存优先的数据面
- Dragonboat 驱动的内嵌 Raft 控制面
- schema-first 的 Query DSL
- 统一可观测、统一治理、统一运维

一句话定位：

> Nespa 是一个面向多应用隔离场景的分布式缓存平台，原生支持 namespace、space、query、quota 和控制面治理。

---

## 2. 设计原则

### 2.1 做什么

Nespa 优先解决这些问题：

- 多应用统一接入缓存平台
- 每个 application 拥有独立 namespace
- 一个 namespace 下可以有多个 cache space/database
- 不同 space 可以配置不同 TTL、quota、eviction、query policy
- 不同 process/principal 可以拥有不同访问权限
- 业务不直接接触物理 keyspace
- Query 必须 schema-first、index-first、scope-bound
- 控制面强一致，缓存数据面可弱一致
- 数据面内存优先，延迟优先

### 2.2 不做什么

Nespa 第一阶段明确不做：

- Redis 协议兼容
- Redis 命令兼容
- Redis Cluster 兼容
- SQL 数据库能力
- 跨 namespace 查询
- 跨 space 查询
- JOIN
- OLAP
- 全局 keyspace
- 无索引全量扫描查询
- 把缓存 value 放进控制面 Raft
- 每个 data shard 使用 Raft 做强一致复制

### 2.2.1 数据结构能力决策

在 MVP 阶段，Nespa 不做 Redis 协议/命令兼容，也不完整复刻 Redis 数据结构矩阵。但为了降低业务方在常见缓存结构上的重复编解码成本，数据面保留一组小而稳定的原生 primitive collection op。

第一阶段支持：

- 基础 KV：`set/get/delete/exists/touch/adjust/batch set/batch get/batch delete/batch exists/batch touch`；
- Counter：`CounterAdjust`，用于低成本原子数值调整；
- Map：`MapSet/MapGet/MapDelete/MapGetAll`；
- Set：`SetAdd/SetRemove/SetContains/SetMembers`；
- ScoredSet：`ScoredSetPut/ScoredSetRemove/ScoredSetRange`；
- List：`ListPushFront/ListPushBack/ListPopFront/ListPopBack/ListRange`，列表元素保持二进制 value，不强制字符串化；
- BatchPrimitive：一次请求承载多种 primitive op，降低用户心智负担和网络往返。

边界：

- 不提供 Redis 协议、Redis 命令名、Redis Cluster、RESP 或 Redis client 兼容；
- 不在第一阶段实现 Stream/Bitmap/HyperLogLog/Geo 等扩展结构；
- primitive collection 仍绑定 `namespace/space/entity/key` 地址模型，并共享 TTL、ExpectedVersion、namespace/space version、route_epoch、quota admission 和 batch 语义；
- collection 值在 DataNode 内部使用 `collectionx` 二进制序列化；外部仍通过 Nespa TCP frame 暴露稳定协议，不暴露内部编码。首版 ListRange 使用 `start + limit + reverse` 语义，不引入 Redis LRANGE 的负索引兼容。

### 2.2.2 当前迭代状态

当前仓库已经落地到可运行的单进程/多进程 scaffold：

- control/node/admin/frontend 可由同一 Go binary 按开关启动；frontend 只保留 route/debug/console 视图，不承载 cache HTTP gateway；
- 数据热路径走 TCP binary protocol，已支持 KV、adjust、batch set/get/delete/exists/touch、primitive、batch primitive；
- DataNode memory engine 已有 TTL、ExpectedVersion 乐观锁、namespace/space version 可见性、sampled eviction、namespace/space memory quota；
- set、adjust、primitive 写入和 batch 写入已接入 quota admission，写前预估增长成本，不允许绕过 space/namespace quota；ExpectedVersion 未命中时不会提前触发 quota reject；
- routed TCP client 会读取 control snapshot，按 vslot 直连 DataNode；batch set/get/delete/exists/touch/primitive 会按 route 分组，失败后刷新 snapshot 并只重试未完成组；
- control 写路径默认通过 Dragonboat Raft proposal 驱动 FSM Apply，并使用 Dragonboat log/snapshot 做控制面恢复；`--control-snapshot-path` 只作为 JSON 导入/导出辅助；
- control rebalance 除事件外已生成 migration plan，记录需要从 source node 迁移到 target node 的 namespace/space/vslot 范围；control snapshot 的 route 仍以 `node_id/addr` 表示 primary，同时携带 `replicas` 作为 DataNode async replication 的目标元数据；
- DataNode 已具备节点级 range migration primitive：按 `namespace/space/vslot` 导出 snapshot、导入 snapshot、删除源 range，并通过 TCP binary protocol 暴露给内部迁移执行器；
- control migration executor 默认启用，会通过 Dragonboat/FSM claim planned task，按可配置并发度执行 `export -> import -> delete`，并把 task/plan 标记为 `running/done/failed`；默认并发为 1，`--control-migration-max-parallel-tasks` 可提高并发，同一批任务会按 source/target node 冲突拆成多个 wave，避免同一 DataNode 被多个迁移任务同时读写；
- DataNode 会缓存 control snapshot 并在 primary `Set/Delete/Touch/Adjust/Primitive` 及对应 mutating batch 成功后按 route replicas 进行 best-effort 异步写后复制；复制不阻塞 primary ack，并通过有界 dispatcher queue 串行发送、内存退避重试，避免异步复制乱序并覆盖短暂发送失败；admin summary 会暴露 queue/drop/retry/sequence/success/failure 等复制观测指标；
- DataNode memory engine 支持本地 snapshot/restore，并可通过 `--node-snapshot-path` 做进程重启恢复；同时支持 `--node-snapshot-interval` 周期持久化快照（0 禁用）；这是本地持久化基础，不等价于副本复制；
- Go SDK 已放在 `sdk/go`，通过 go work 参与多子包开发；Java SDK 已放在 `sdk/java`，使用 Gradle Kotlin DSL、wrapper、version catalog、Lombok plugin、JPMS，并带 direct TCP/wire smoke 覆盖。

尚未落地或仍是设计目标：

- 3 节点控制面部署、membership change、跨节点 Raft 配置管理；
- 副本追赶、复制 offset 和生产级 rebalance；
- schema/query/index planner；
- principal/grant 鉴权链路；
- Java routed SDK、更多语言 SDK、生产级连接池和 TLS 策略。

### 2.3 关键边界

控制面和数据面必须拆开：

```text
control plane:
  强一致，管理元数据、路由、schema、policy、quota、version、membership。

data plane:
  低延迟，管理缓存对象、TTL、eviction、本地索引、replication、query execution。
```

普通缓存请求不能依赖控制面实时查询 route。Frontend 和 DataNode 必须本地缓存 route/config/schema，并通过 watch/revision 机制更新。

---

## 3. 核心概念模型

### 3.1 Namespace

Namespace 是 application 级隔离边界。

示例：

```text
order-service
payment-service
risk-service
user-service
```

Namespace 负责：

- 应用级身份边界
- 应用级资源配额
- 应用级路由策略
- 应用级观测维度
- 应用级 flush/version bump
- 应用级权限域

Namespace 之间默认完全不可见。

### 3.2 Space / Database

Space 是 namespace 内部的逻辑缓存空间。也可以对外提供 alias：database。

示例：

```text
order-service/session
order-service/order-view
order-service/inventory-view
payment-service/risk-cache
```

Space 负责：

- 默认 TTL
- 最大 TTL
- memory quota
- QPS quota
- eviction policy
- query policy
- entity schema
- index schema
- flush version

建议文档和代码中优先使用 `Space`，因为 `Database` 容易让使用者误以为它具备持久数据库语义。

### 3.3 Principal

Principal 是访问主体，通常对应一个应用进程、服务实例、任务或平台管理员。

示例：

```text
order-api
order-worker
order-scheduler
risk-engine
platform-admin
```

Principal 不进入数据路径，只作为权限和审计身份。

访问路径应该是：

```text
namespace / space / entity / key
```

而不是：

```text
namespace / process / space / key
```

原因是 process 是动态运行身份，不能成为数据归属边界。

### 3.4 Entity

Entity 是 queryable space 内的对象类型。

示例：

```text
OrderView
InventoryView
SessionView
UserProfileView
```

只有 queryable space 才需要 entity schema。普通 KV cache space 可以不启用 entity/query 能力。

MVP 阶段 entity 先作为 control-plane catalog 元数据落地，记录 `namespace/space/entity` 和创建时间，并通过 control snapshot 下发给 SDK/DataNode。Schema、index、typed query 是 entity 之上的后续能力，不能和第一阶段的 entity 声明混在同一个状态对象里。

### 3.5 Key

Key 是最小缓存对象标识。

完整逻辑路径：

```text
namespace / space / entity / key
```

对于非 queryable space，可以省略 entity：

```text
namespace / space / key
```

内部物理 key 不暴露给业务。内部编码建议包含：

```text
namespace_id
namespace_version
space_id
space_version
entity_id，可选
key_hash
key_bytes
```

---

## 4. 总体架构

```text
                           ┌────────────────────────────┐
                           │      Admin / Console        │
                           │  schema / route / policy    │
                           └──────────────┬─────────────┘
                                          │
                                          ▼
                           ┌────────────────────────────┐
                           │      Control Plane          │
                           │   Dragonboat Raft Group     │
                           │  namespace/space/schema     │
                           │  route/quota/grant/version  │
                           └──────────────┬─────────────┘
                                          │ watch/revision
        ┌─────────────────────────────────┼─────────────────────────────────┐
        ▼                                 ▼                                 ▼
┌────────────────┐              ┌────────────────┐              ┌────────────────┐
│  SDK / Client  │              │   Data Node    │              │  Background    │
│ direct route   │─────────────▶│ memory engine  │◀────────────▶│ jobs/reindex   │
│ TCP frame      │              │ ttl/eviction   │              │ migration/gc   │
└────────────────┘              │ local indexes  │              └────────────────┘
        ▲                       └────────────────┘
        │
┌───────┴────────┐
│    Frontend    │
│ WebUI/console  │
└────────────────┘
```

### 4.1 组件划分

建议单仓库、多 binary：

```text
cmd/nespa-control      控制面节点
cmd/nespa-frontend     WebUI、console、route/debug view
cmd/nespa-node         数据节点
cmd/nespa-admin        管理 API / console backend
cmd/nespa-cli          命令行工具
```

也可以单 binary 多 mode：

```bash
nespa control
nespa frontend
nespa node
nespa admin
```

Frontend 只展示控制面、路由与调试视图，不承载 cache HTTP gateway；cache 读写热路径由 SDK 读取 control snapshot 后直接走 DataNode TCP binary protocol。

---

## 5. 技术选型

### 5.1 主语言

```text
Go
```

理由：

- 适合基础设施服务
- 网络服务开发效率高
- TCP 网络服务、HTTP/OpenAPI、Prometheus、OpenTelemetry 生态成熟
- 部署简单
- 适合同时实现控制面、frontend、data node 和 SDK

### 5.2 Raft 控制面

```text
github.com/lni/dragonboat/v3
```

选择 Dragonboat 的原因：

- 纯 Go
- 支持 multi-group Raft
- 支持 snapshot、log compaction、membership change
- 支持 ReadIndex 读路径
- 支持 non-voting member、witness member
- 默认使用 Pebble 存储 Raft logs
- 不需要额外引入 etcd server

当前实现锁定在：

```bash
github.com/lni/dragonboat/v3 v3.3.8
```

不建议在生产第一版使用 v4 master。

### 5.3 数据面协议

```text
Nespa TCP Frame
```

用于：

- SDK 到 DataNode direct-route
- DataNode 到 DataNode replication

协议形态：

```text
fixed-width binary header + binary metadata + raw payload
```

选择 TCP frame 的原因：

- 缓存 hot path 避免 HTTP/gRPC 额外开销
- value payload 不需要 JSON/base64 编码
- batch set/get 可以用 metadata offset 描述 payload 切片
- request_id、route_epoch、flags 等控制字段可以固定在 header 中
- 后续可以演进压缩、批量、流式 watch、replication offset 等能力

边界：

- 控制面写操作、Admin API、Console backend、debug、health 仍使用 HTTP/OpenAPI
- 控制面 watch 第一阶段可以用 HTTP streaming 或 long polling；需要进入 hot path 时再承载到 TCP frame
- 协议语义由 Nespa 自己定义，不追求 Redis/gRPC 兼容

### 5.4 管理 API

```text
HTTP + OpenAPI
```

使用：

```text
github.com/arcgolabs/httpx
```

用于：

- Admin API
- Console backend
- debug endpoint
- health endpoint
- OpenAPI 文档

### 5.5 配置

```text
github.com/arcgolabs/configx
```

用于：

- YAML / JSON / TOML 配置文件
- env override
- command-line args
- 配置校验
- 非关键配置热更新

控制面关键配置仍然以 Raft state 为准。

### 5.6 日志

```text
github.com/arcgolabs/logx
```

用于：

- slog API
- zerolog-backed output
- trace/span 字段注入
- console/file output

### 5.7 认证授权

```text
github.com/arcgolabs/authx
```

用于：

- AccessKey/Secret
- JWT
- mTLS identity
- Principal 解析
- AuthorizationModel 评估

Nespa 自己定义 domain resource：

```text
namespace:{id}
space:{id}
entity:{id}
query:{space_id}:{entity_id}
admin:{operation}
```

### 5.8 可观测性

```text
github.com/arcgolabs/observabilityx
```

用于：

- OpenTelemetry tracing
- Prometheus metrics
- Noop backend
- typed metric specs

### 5.9 事件系统

```text
github.com/arcgolabs/eventx
```

用于进程内事件，不用于跨节点消息。

场景：

- control FSM apply event
- route changed event
- schema changed event
- index build progress event
- local watcher dispatch
- metrics hook

### 5.10 集合结构

```text
github.com/arcgolabs/collectionx
```

用于：

- OrderedMap
- ConcurrentMap
- MultiMap
- Table
- Trie / PrefixMap
- Range / RangeSet / RangeMap
- RingBuffer / PriorityQueue

当前实现中，DataNode 基础 entry table、TTL、eviction 和 quota accounting 仍由 engine 自己管理；primitive Map/Set/ScoredSet/List 的集合表示与二进制序列化使用 `collectionx`，以避免手写容器和编码分叉。后续如果 primitive collection 成为极限热路径，再评估替换为专用结构。

### 5.11 客户端基础能力

```text
github.com/arcgolabs/clientx
```

用于：

- 管理 CLI 的 HTTP/TCP client
- 内部 HTTP/TCP 辅助 client
- retry / TLS / timeout policy

核心 SDK 的高性能 TCP frame client 可以自建，clientx 作为通用辅助库。

### 5.12 存储辅助库

```text
github.com/arcgolabs/storx
```

可用于：

- control snapshot 辅助持久化
- 本地 metadata
- debug/local mode
- badger/bbolt wrapper
- typed codec/keycodec

边界：

- DataNode 热路径不直接依赖 storx。
- Dragonboat 的 raft log storage 由 Dragonboat 自身管理。
- 如果未来需要 data cold tier，可通过 storage adapter 接入 storx/badgerx，但不要让它阻塞热路径。

### 5.13 DSL / Schema 声明

```text
github.com/arcgolabs/plano
```

用于：

- namespace declaration DSL
- space declaration DSL
- entity schema DSL
- index schema DSL
- query policy DSL
- saved query declaration DSL，可选

不建议用于：

- 每次在线 query 的直接执行
- 数据面动态脚本执行
- 任意函数执行
- 任意循环/import

Nespa 的在线 Query DSL 应该 lower 成自定义 QueryAST，再进入 planner。

### 5.14 可选基础库

```text
github.com/arcgolabs/dbx
```

可选用于：

- 管理后台审计记录
- 长期操作历史
- billing/cost 报表
- 非控制面关键数据

```text
github.com/arcgolabs/kvx
```

可选用于：

- 和 Redis/Valkey 的迁移工具
- 兼容层实验
- benchmark 对比

不进入 Nespa 核心数据面。

```text
github.com/arcgolabs/dix
```

可选用于：

- 依赖注入
- 模块生命周期组织

如果团队偏向显式 wiring，可以不用 dix。

---

## 6. 控制面设计

### 6.1 控制面职责

控制面只管理元数据，不管理缓存 value。

控制面管理对象：

```text
Cluster
Namespace
Space
EntitySchema
IndexSchema
QueryPolicy
QuotaPolicy
Principal
Grant
ShardGroup
DataNode
RouteTable
MigrationJob
ReindexJob
DeletionJob
```

控制面写操作必须经 Dragonboat Raft commit。

### 6.2 Dragonboat Raft 分组

第一阶段建议：

```text
1 个 Control Raft Group
3 个 control-plane 节点
```

生产重要集群可以采用：

```text
5 个 control-plane 节点
```

控制面节点和 DataNode 分离：

```text
control-plane nodes: 3 or 5
frontend nodes: N
data nodes: N
```

不要让所有 DataNode 都加入控制面 Raft quorum。

### 6.3 State Machine 类型

建议使用 Dragonboat 常规 state machine：

```text
in-memory FSM + deterministic apply + snapshot/restore
```

FSM 内部维护：

```go
type ControlState struct {
    ClusterID string
    Revision  uint64

    Namespaces map[uint64]*Namespace
    Spaces     map[uint64]*Space
    Entities   map[uint64]*EntitySchema
    Indexes    map[uint64]*IndexSchema
    Principals map[uint64]*Principal
    Grants     map[uint64]*Grant

    Nodes       map[uint64]*DataNode
    ShardGroups map[uint64]*ShardGroup
    Routes      map[uint64]*RouteTable

    Jobs        map[uint64]*ControlJob
    Requests    map[string]*IdempotentResult
}
```

### 6.4 FSM 命令

```go
type CommandEnvelope struct {
    Version   uint32
    Type      CommandType
    RequestID string
    ActorID   uint64
    CreatedAt int64
    Payload   []byte
}
```

命令类型：

```text
CreateNamespace
UpdateNamespaceQuota
BumpNamespaceVersion
CreateSpace
UpdateSpacePolicy
BumpSpaceVersion
CreateEntity
CreateEntitySchema
UpdateEntitySchema
CreateIndexSchema
ActivateIndexSchema
CreatePrincipal
GrantPrincipal
RevokePrincipal
RegisterDataNode
MarkDataNodeSuspect
MarkDataNodeDead
CreateRouteTable
UpdateRouteTable
StartMigrationJob
CommitMigrationJob
CreateReindexJob
CommitReindexJob
```

当前 MVP scaffold 先提供内存态 catalog API，用于沉淀控制面对象边界：

```text
GET  /v1/control/namespaces
POST /v1/control/namespaces
POST /v1/control/namespaces/version-bump
GET  /v1/control/spaces
POST /v1/control/spaces
POST /v1/control/spaces/version-bump
GET  /v1/control/entities
POST /v1/control/entities
```

`CreateNamespace`、`CreateSpace` 和 `CreateEntity` 语义按 FSM 命令设计：同名重复创建为幂等返回，新增对象才推进 revision。`CreateEntity` 必须引用已存在的 namespace 和 space；第一阶段只声明 entity catalog，不包含 Redis 数据结构兼容、schema DSL、index schema 或 query planner。当前 HTTP handler 只负责解析请求并调用 ServiceRuntime，写入由 Dragonboat proposal 提交后再 apply，不能直接修改状态。

HTTP API 在代码组织上按 `httpx.Endpoint` 拆分，endpoint 通过 `dix` contribution 注入到服务专属集合，再由 HTTP module 启动时统一注册。这样 control/admin 的路由注册保持声明式，后续新增 schema、query、migration endpoint 时只需要新增 endpoint contribution，不修改统一启动逻辑。

### 6.5 Deterministic Apply

FSM apply 必须 deterministic。

禁止在 apply 内部执行：

```text
time.Now()
rand()
读取外部文件
调用外部服务
查询 DataNode 实时状态
```

如果命令需要时间戳，由 leader 在 propose 前填入 command envelope。

### 6.6 幂等性

所有控制面命令必须支持幂等。

通过：

```text
request_id + actor_id + command_type
```

做去重。

重复提交时返回相同结果。

### 6.7 Revision 与 Watch

每条 committed command apply 后：

```text
global_revision += 1
```

并产生 domain event：

```go
type ControlEvent struct {
    Revision uint64
    Type     EventType
    Resource ResourceRef
    Payload  []byte
}
```

SDK/Frontend/DataNode 通过 streaming watch 订阅：

```text
WatchConfig(since_revision)
```

如果 since_revision 太旧，返回：

```text
NEED_FULL_SNAPSHOT
```

客户端重新拉取全量 config snapshot。

### 6.8 Node Heartbeat

DataNode heartbeat 不要每次写 Raft log。

流程：

```text
DataNode -> Control Leader heartbeat
leader 本地维护 live table
只有状态变化时写 Raft：Healthy/Suspect/Dead/Recovering
```

高频指标进入 Prometheus，不进入 Raft。

### 6.9 控制面故障策略

控制面短暂不可用时：

- 已有 Frontend / DataNode 使用本地 route/config 继续服务
- 禁止创建 namespace/space/schema
- 禁止修改权限和 route
- 允许已有缓存读写继续执行
- 超过 max_config_staleness 后根据策略降级

---

## 7. 数据面设计

### 7.1 DataNode 职责

DataNode 负责：

- key/value 存储
- TTL
- eviction
- quota accounting
- local secondary index
- query local execution
- async replication
- shard migration
- space flush version 过滤

### 7.2 内存优先

DataNode 热路径采用：

```text
memory index + memory value store
```

不建议直接把缓存对象放进 Go heap 的大量 map/string/[]byte 小对象里。

建议结构：

```go
type Entry struct {
    NamespaceID uint64
    SpaceID     uint64
    EntityID    uint64
    KeyHashHi   uint64
    KeyHashLo   uint64

    SegmentID   uint32
    Offset      uint64
    Length      uint32

    Version     uint64
    ExpireAtMs  int64
    Flags       uint32
    CostBytes   uint32
}
```

Value 存入 segment/slab：

```text
segment-1: []byte
segment-2: []byte
segment-3: []byte
```

Entry 只保存 offset/length。

### 7.3 分片模型

采用 virtual slot：

```text
vslot_count = 65536
vslot = hash(namespace_id, space_id, group_or_key) % vslot_count
```

RouteTable：

```go
type RouteTable struct {
    Epoch uint64
    Assignments []VSlotAssignment
}

type VSlotAssignment struct {
    NamespaceID uint64
    SpaceID     uint64
    Start       uint32
    End         uint32
    Primary     DataNodeID
    Replicas    []DataNodeID
}
```

MVP scaffold 在 namespace/space 数字 ID 完整落地前，先用 namespace、space、key 字符串做稳定 hash，并在控制面 snapshot 中携带：

```text
namespace
space
vslot_start
vslot_end
node_id
addr
weight
```

本地开发模式可以从 healthy DataNode 列表派生临时 RouteTable：按 node_id 排序后均匀切分 `0..65535`。生产控制面必须把 RouteTable 作为显式元数据，经 Raft commit 后发布 revision/watch；不能每次请求动态查询节点状态。

### 7.4 本地 shard worker

DataNode 内部切分多个 local shard：

```text
data-node
  local-shard-0
  local-shard-1
  ...
  local-shard-N
```

每个 local shard 管理自己的：

- hash index
- value segments
- TTL wheel
- eviction state
- local indexes
- replication queue

降低全局锁竞争。

### 7.5 TTL

采用：

```text
lazy expiration + timing wheel + sampling scan
```

禁止：

```text
每个 key 一个 time.AfterFunc/timer
```

GET 时：

```text
lookup -> check expire_at -> expired 则 lazy delete -> return miss
```

后台：

```text
periodic expire scan -> batch delete -> index cleanup
```

### 7.6 Eviction

Eviction 必须 namespace/space aware。

优先顺序：

```text
space quota exceeded
namespace quota exceeded
node memory pressure
```

淘汰不能跨隔离边界随意执行。

策略：

MVP：

```text
sampled LRU
sampled LFU
```

后续：

```text
TinyLFU admission
cost-based eviction
priority-aware eviction
```

### 7.7 Replication

MVP：

```text
primary -> replica async replication
```

特性：

- 异步复制
- primary ack 优先低延迟
- replica 落后通过 replication offset 追赶
- primary 故障后由控制面更新 route table
- control snapshot route 使用 `node_id/addr` 表示 primary，`replicas` 表示同一 vslot 范围的异步复制目标；当前实现已接入 primary `Set/Delete/Touch/Adjust/Primitive` 及 mutating batch 成功后的 best-effort 异步写后复制，并由有界 dispatcher queue 保序发送、内存重试和 admin stats 观测 replication sequence 进度，durable offset catch-up 后续接入。

不做：

```text
per-shard raft data replication
```

原因：缓存数据可以弱一致，控制面强一致即可。

---

## 8. API 设计

### 8.1 基础 KV 与 Primitive API

数据面使用 Nespa TCP frame。Frame header 固定宽度；cache op metadata 使用 `cachewire` binary codec；payload 保存原始 value bytes。

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

MVP ops：

```text
CacheGet
CacheSet
CacheDelete
CacheExists
CacheTouch
CacheAdjust
CacheBatchGet
CacheBatchSet
CacheBatchDelete
CacheBatchExists
CacheBatchTouch
CachePrimitive
CacheBatchPrimitive
```

逻辑 key：

```json
{
  "namespace": "order-service",
  "space": "order-view",
  "entity": "OrderView",
  "key": "order:10086"
}
```

Value 传输规则：

- 单 key op：metadata 使用 binary codec 描述 key、ttl、version、found/deleted/touched 等字段，payload 传输 value bytes
- batch set/get/delete/exists/touch：metadata 使用 binary codec 保存 item 列表；batch set/get 使用 payload_offset/payload_size，payload 拼接多个 value bytes
- primitive op：metadata 描述 kind、field/member/score/range/list start/version 等结构化参数，payload 传输 Map value、List value 或 primitive result value
- batch primitive：metadata 依次保存 primitive item 与 payload offset，payload 拼接所有 value bytes；routed SDK 发送前按 route 分组，响应按原请求顺序回填
- batch 执行语义：第一阶段为单节点内顺序执行，不提供跨 key 原子性；若第 N 个 item 失败，服务端返回前 N-1 个已完成结果和错误，routed client 刷新 route 后只重试未发送/未完成的 route group
- response 使用 flags 区分正常响应和协议错误；协议错误 frame 在 MVP 阶段仍使用 `cachewire.Error` JSON metadata
- route_epoch 用于让 SDK/DataNode 检测客户端路由是否过旧；MVP 中 DataNode 对非 0 且小于本节点已观测 control revision 的请求返回 `ErrorNoRoute`，routed SDK 收到该错误后刷新 snapshot 并重试一次

### 8.2 SDK 使用示例

```go
client := nespa.NewRoutedTCP(nespa.RoutedConfig{
    ControlAddr: "http://nespa-control.company.internal:7401",
})

orders := client.Namespace("order-service").Space("order-view")

err := orders.Set(ctx, "order:10086", payload, nespa.SetOptions{
    TTL: 5 * time.Minute,
    Entity: "OrderView",
})

obj, ok, err := orders.Get(ctx, "order:10086")
```

SDK direct-route 流程：

```text
1. SDK 从 ControlPlane 拉取 snapshot/watch
2. 本地缓存 namespace/space version 与 vslot routes
3. SDK 根据 namespace/space/key 计算 vslot
4. SDK 直接向目标 DataNode 发送 TCP frame
5. cache metadata 携带 namespace_version、space_version，frame header 携带 route_epoch
6. 如果 DataNode 返回 stale route `ErrorNoRoute`，SDK refresh snapshot 后最多重试一次
```

### 8.3 Principal 绑定

Credential 可以绑定 namespace：

```text
credential -> namespace_id -> principal_id
```

这样业务调用时不一定每次传 namespace，但服务端内部必须解析出：

```text
namespace_id
principal_id
space_id
operation
```

---

## 9. Query DSL 设计

### 9.1 Query 定位

Nespa Query DSL 是：

```text
namespace/space/entity 范围内的 typed indexed query
```

不是：

```text
全局 SQL
数据库查询引擎
搜索引擎
跨服务数据查询层
```

### 9.2 Query 范围限制

每次 query 必须限定：

```text
namespace
space
entity
```

不支持：

```text
跨 namespace
跨 space
跨 entity
JOIN
子查询
```

### 9.3 Query 语法示例

```sql
SELECT key(), version(), order_id, status, created_at_ms
FROM OrderView
WHERE merchant_id = :merchantId
  AND status = 'PAID'
  AND created_at_ms BETWEEN :from AND :to
ORDER BY created_at_ms DESC
LIMIT 100
```

全文示例，第二阶段支持：

```sql
SELECT key(), score(), order_id
FROM OrderView
WHERE MATCH(description, :q)
ORDER BY score() DESC
LIMIT 20
```

### 9.4 MVP 支持的表达式

```text
=
!=
>
>=
<
<=
IN
BETWEEN
EXISTS
AND
OR
NOT
PREFIX(field, value)
MATCH(field, query)   第二阶段
```

### 9.5 不支持的表达式

MVP 不支持：

```text
JOIN
GROUP BY
HAVING
aggregation
子查询
用户自定义函数
任意脚本
正则
LIKE '%xxx'
跨 entity 查询
```

### 9.6 Query 执行流程

```text
Frontend / Query Coordinator
  -> parse query
  -> build QueryAST
  -> validate principal permission
  -> validate namespace/space/entity
  -> validate index coverage
  -> build logical plan
  -> build physical plan
  -> fanout to target shards
  -> local index query
  -> post-filter
  -> local top-k
  -> global merge
  -> return result/cursor
```

### 9.7 Query 必须依赖索引

默认要求：

```text
require_index = true
allow_full_scan = false
```

如果 query 无法通过索引执行，直接返回：

```text
QUERY_REQUIRES_INDEX
```

只有管理员在 debug/test 环境可以打开：

```text
allow_full_scan = true
```

### 9.8 Query AST

内部 AST 示例：

```go
type QueryAST struct {
    Entity     string
    Projection []Projection
    Predicate  Expr
    OrderBy    []OrderBy
    Limit      int
    Cursor     string
}

type Expr struct {
    Op       ExprOp
    Field    string
    Value    Value
    Children []Expr
}
```

### 9.9 Query Planner

Planner 分三层：

```text
AST
  -> LogicalPlan
    -> PhysicalPlan
      -> ShardExecutionPlan
```

LogicalPlan 关注：

- predicate normalization
- index coverage
- projection
- sort requirement
- limit
- cost estimate

PhysicalPlan 关注：

- shard fanout
- local index selection
- top-k merge
- timeout budget
- result cursor

### 9.10 分页

不要主推 OFFSET。

默认：

```text
LIMIT default = 100
LIMIT max = 1000/5000，由 policy 控制
```

推荐 cursor pagination：

```sql
SELECT key(), order_id
FROM OrderView
WHERE merchant_id = :merchantId
ORDER BY created_at_ms DESC
LIMIT 100
CURSOR :cursor
```

Cursor 编码：

```text
query_hash
route_epoch
index_epoch
last_sort_value
last_doc_id
shard_positions
expiry
signature
```

### 9.11 Post-filter

索引结果必须 post-filter。

原因：

- TTL 过期但索引未清理
- object overwritten 但旧 doc_id 未清理
- space version bump 后旧索引未 GC
- replica 索引延迟

流程：

```text
index candidates
  -> fetch object
  -> check namespace_version
  -> check space_version
  -> check object_version
  -> check expire_at
  -> re-evaluate predicate
  -> projection
```

---

## 10. Index 设计

### 10.1 Index Scope

索引是 shard-local 的。

```text
对象在哪个 shard，索引也在哪个 shard。
```

不做全局二级索引。

### 10.2 Index 类型

MVP：

```text
keyword
numeric
date
boolean
```

第二阶段：

```text
text
```

第三阶段：

```text
geo
vector
```

### 10.3 Keyword Index

```text
field=status
  PAID    -> doc_id set
  CREATED -> doc_id set
  FAILED  -> doc_id set
```

### 10.4 Numeric / Date Index

```text
field=created_at_ms
  ordered value -> doc_id set
```

支持：

```text
range query
sort
between
```

### 10.5 Doc Values

用于 projection 和 sort：

```text
doc_id -> field_value
```

字段必须声明：

```text
projectable
sortable
```

### 10.6 Text Index

第二阶段可接入：

```text
Bleve
```

但 planner、permission、distributed fanout、post-filter 仍由 Nespa 自己管理。

---

## 11. Schema 与 Plano DSL

### 11.1 Schema-first

Queryable space 必须先注册 schema。

Entity schema：

```yaml
entity: OrderView
codec: protobuf
fields:
  order_id: string
  merchant_id: string
  user_id: string
  status: string
  created_at_ms: int64
  amount_cent: int64
  description: string
```

Index schema：

```yaml
indexes:
  - field: order_id
    kind: keyword
    projectable: true

  - field: merchant_id
    kind: keyword

  - field: status
    kind: keyword

  - field: created_at_ms
    kind: numeric
    sortable: true
    projectable: true

  - field: description
    kind: text
    analyzer: cjk
```

### 11.2 Plano 用途

Plano 作为控制面 DSL，不直接执行在线 query。

用途：

```text
namespace schema DSL
space schema DSL
entity schema DSL
index schema DSL
query policy DSL
saved query DSL，可选
```

### 11.3 Plano 示例

```plano
namespace order_service {
  display_name = "order-service"

  quota {
    memory = "100GiB"
    qps = 500000
  }

  space order_view {
    display_name = "order-view"
    queryable = true
    default_ttl = "10m"
    max_ttl = "24h"

    entity OrderView {
      codec = "protobuf"

      field order_id {
        type = "string"
        index = keyword
        projectable = true
      }

      field merchant_id {
        type = "string"
        index = keyword
      }

      field status {
        type = "string"
        index = keyword
        projectable = true
      }

      field created_at_ms {
        type = "int64"
        index = numeric
        sortable = true
        projectable = true
      }
    }

    query_policy {
      require_index = true
      allow_full_scan = false
      default_limit = 100
      max_limit = 1000
      max_query_time = "200ms"
      max_fanout_shards = 128
    }
  }
}
```

### 11.4 Plano 编译流程

```text
.plano source
  -> plano parser
  -> plano typed document
  -> Nespa schema lowering
  -> ControlPlaneCommand
  -> Dragonboat propose
  -> FSM apply
  -> watch events
```

### 11.5 在线 Query 不走 Plano Script Execution

在线 query 不允许：

```text
import
fn
for
while
任意 host function
任意 expr execution
```

在线 query 只允许解析成结构化 QueryAST。

---

## 12. 权限模型

### 12.1 权限对象

```text
Principal
Grant
Role
Policy
```

### 12.2 权限维度

```text
namespace.read
namespace.admin
space.read
space.write
space.delete
space.flush
space.query
space.query_text
space.schema_admin
space.reindex
```

### 12.3 示例

```yaml
principal: order-api
namespace: order-service
grants:
  - space: session
    permissions: [read, write, delete]
  - space: order-view
    permissions: [read, query]
```

### 12.4 Query 权限

能 get/set 不代表能 query。

Query 权限单独控制：

```text
space.query
space.query_text
space.query_projection_value
space.delete_by_query
```

---

## 13. Quota 与限流

### 13.1 Namespace Quota

```yaml
namespace: order-service
memory_quota: 100GiB
qps_quota: 500000
connection_quota: 10000
space_limit: 100
```

### 13.2 Space Quota

```yaml
space: order-view
memory_quota: 20GiB
qps_quota: 100000
default_ttl: 10m
max_ttl: 24h
eviction_policy: sampled_lru
```

### 13.3 Query Quota

```yaml
query_policy:
  default_limit: 100
  max_limit: 1000
  max_query_time_ms: 200
  max_fanout_shards: 128
  max_boolean_clauses: 64
  max_in_terms: 1000
```

### 13.4 Admission Control

写入流程：

```text
validate namespace/space status
validate principal permission
validate ttl
estimate object cost
check space quota
check namespace quota
maybe evict within same space
write object
update index
replicate async
```

---

## 14. Flush 与 Version Bump

### 14.1 Space Flush

不要扫描删除。

执行：

```text
space.version += 1
```

新请求读写新 version。

MVP HTTP command：

```text
POST /v1/control/spaces/version-bump
```

旧 version 数据：

- 自然 TTL 过期
- 后台 GC
- 低优先级清理

### 14.2 Namespace Flush

执行：

```text
namespace.version += 1
```

所有 space 逻辑失效。

MVP HTTP command：

```text
POST /v1/control/namespaces/version-bump
```

SDK/DataNode 必须从 control snapshot/watch 缓存最新 namespace/space version，并在 cache get/set metadata 中携带版本。DataNode 不扫描删除旧对象，而是用版本可见性过滤让旧数据立即逻辑失效。

### 14.3 Delete by Query

如未来支持：

```sql
DELETE FROM OrderView WHERE status = 'EXPIRED'
```

必须转成异步任务：

```text
create job
fanout shards
index scan
batch delete
rate limit
progress report
pause/resume/cancel
```

---

## 15. 可观测性

### 15.1 Metrics

核心指标：

```text
nespa_request_total
nespa_request_latency_ms
nespa_cache_hit_total
nespa_cache_miss_total
nespa_namespace_memory_bytes
nespa_space_memory_bytes
nespa_eviction_total
nespa_expire_total
nespa_query_total
nespa_query_latency_ms
nespa_query_candidate_docs
nespa_query_post_filter_dropped
nespa_raft_propose_latency_ms
nespa_raft_apply_latency_ms
nespa_route_epoch
```

### 15.2 Trace

Trace 维度：

```text
request_id
namespace
space
entity
principal
route_epoch
shard_id
query_hash
```

### 15.3 Logs

日志必须控制 cardinality。

避免默认记录：

```text
完整 key
完整 query parameter
完整 value
```

敏感字段脱敏。

---

## 16. 部署模式

### 16.1 单机开发模式

```text
nespa dev
```

包含：

- single control
- single frontend
- single node
- local memory engine

### 16.2 小型生产模式

```text
3 control-plane
2+ frontend
3+ data-node
```

### 16.3 标准生产模式

```text
5 control-plane
N frontend
N data-node
N background-worker
```

### 16.4 Kubernetes

第一阶段可通过 Helm 部署。

Operator 后置，不作为 MVP。

---

## 17. 推荐代码结构

```text
nespa/
  cmd/
    nespa/
    nespa-control/
    nespa-frontend/
    nespa-node/
    nespa-cli/

  api/
    proto/
      cache/v1/
      control/v1/
      query/v1/
      admin/v1/

  internal/
    control/
      raft/
      fsm/
      command/
      watch/
      snapshot/

    frontend/
      auth/
      routing/
      querycoord/
      views/

    node/
      engine/
      shard/
      ttl/
      eviction/
      index/
      replication/
      migration/

    query/
      parser/
      ast/
      planner/
      executor/
      cursor/

    schema/
      entity/
      index/
      plano/
      lowering/

    security/
      principal/
      grant/
      policy/

    observability/
      metrics/
      tracing/
      logging/

  pkg/
    sdk/
      go/
    errors/
    types/
```

---

## 18. MVP 范围

### 18.1 必须支持

```text
Go implementation
Dragonboat control plane
3-node control plane
namespace
space
principal/grant
quota policy
route table
frontend route cache
SDK direct-route mode
DataNode memory engine
TCP framed cache transport
TTL
ExpectedVersion optimistic writes
space flush by version bump
namespace flush by version bump
sampled eviction
Counter primitive op
Map/Set/ScoredSet primitive ops
List primitive ops
BatchDelete/BatchExists/BatchTouch
BatchPrimitive
async primary-replica replication
Go SDK
Java SDK direct TCP/wire foundation
Admin API
Plano-based schema declaration
Query AST
keyword index
numeric/date index
projection
ORDER BY indexed field
LIMIT
cursor pagination
Prometheus metrics
OpenTelemetry tracing
```

### 18.2 暂不支持

```text
Redis compatibility
JOIN
aggregation
continuous query
full-text search
geo search
vector search
delete by query
per-shard data raft
operator
cross-region raft
```

---

## 19. Phase 2

```text
Java routed SDK
full-text MATCH
Bleve text index integration
async delete by query
reindex job
query plan cache
hot key detection
big key detection
advanced eviction
```

---

## 20. Phase 3

```text
wire-level compression/multiplexing
continuous query
simple aggregation
geo query
vector query
tiered storage
adaptive resharding
operator
multi-region deployment
```

---

## 21. 风险与对策

### 21.1 Dragonboat 版本风险

风险：Dragonboat master 正在走 v4 方向，API 可能变化。

对策：

- 生产使用 v3 module，并在 `go.mod` 锁定版本
- 控制面业务状态保持在确定性的 FSM/command 边界内
- Dragonboat 启停、proposal、readiness、snapshot 编码集中在 control runtime 层
- 不把 Dragonboat API 泄漏到 DataNode、SDK 或业务协议

### 21.2 Go GC 风险

风险：大量 key/value 小对象导致 GC 抖动。

对策：

- value segment/slab
- entry pointer-light
- 减少 Go heap object 数量
- 定期 pprof
- 通过 GOMEMLIMIT/GOGC 配置调优

### 21.3 Query 复杂度膨胀

风险：Query DSL 逐渐变成 SQL engine。

对策：

- 单 namespace/space/entity
- require index
- no join
- no aggregation in MVP
- query policy 限制 fanout、limit、timeout

### 21.4 ArcgoLabs 包成熟度风险

风险：arcgolabs 组织包较新，版本成熟度需要内部验证。

对策：

- 作为基础设施辅助库优先使用
- 不进入 DataNode 极限热路径
- 对每个包做 adapter 封装
- 锁定版本
- 保留替换能力

### 21.5 Plano 用法风险

风险：Plano 是通用 DSL runtime，如果直接进数据面，会引入脚本执行风险。

对策：

- 只用于控制面 DSL 编译和 schema lowering
- 禁止在线 query script execution
- 在线 query 走 QueryAST 和 planner

---

## 22. 关键决策总结

| 领域 | 决策 |
|---|---|
| 项目名 | Nespa |
| 主语言 | Go |
| 控制面共识 | Dragonboat v3 |
| 外部 etcd | 不引入 |
| 数据面一致性 | primary-replica async |
| 热数据 | memory-first |
| 本地存储 | Dragonboat 管理 Raft log；storx 可辅助 metadata/cold layer |
| RPC | Nespa TCP Frame |
| Admin API | httpx + HTTP/OpenAPI |
| Auth | authx |
| Config | configx |
| Logging | logx |
| Metrics/Trace | observabilityx |
| Event Bus | eventx |
| DSL | plano 作为 schema/control DSL |
| Query | 自研 QueryAST + planner |
| Redis compatibility | 不做 |
| Query 范围 | namespace/space/entity |
| Full scan | 默认禁止 |

---

## 23. 参考资料

- Dragonboat: https://github.com/lni/dragonboat
- arcgolabs organization: https://github.com/arcgolabs
- arcgolabs/configx: https://github.com/arcgolabs/configx
- arcgolabs/authx: https://github.com/arcgolabs/authx
- arcgolabs/httpx: https://github.com/arcgolabs/httpx
- arcgolabs/clientx: https://github.com/arcgolabs/clientx
- arcgolabs/eventx: https://github.com/arcgolabs/eventx
- arcgolabs/collectionx: https://github.com/arcgolabs/collectionx
- arcgolabs/observabilityx: https://github.com/arcgolabs/observabilityx
- arcgolabs/logx: https://github.com/arcgolabs/logx
- arcgolabs/storx: https://github.com/arcgolabs/storx
- arcgolabs/plano: https://github.com/arcgolabs/plano
- arcgolabs/kvx: https://github.com/arcgolabs/kvx
- arcgolabs/dbx: https://github.com/arcgolabs/dbx

---

## 24. 最终结论

Nespa 应该按以下方向落地：

```text
Go + Dragonboat + memory-first DataNode + schema-first Query DSL + arcgolabs foundation packages
```

它的差异化不在于复刻 Redis，而在于：

```text
namespace-native
space-isolated
queryable
quota-aware
control-plane-driven
```

第一阶段应保持边界克制：

- 不兼容 Redis
- 不做 SQL engine
- 不做跨 namespace 查询
- 不做数据面强一致 Raft
- 不让 Plano 脚本执行进入数据面热路径

优先把：

- 控制面
- 隔离模型
- 内存引擎
- Query AST/planner
- quota/eviction
- 可观测性

做稳。这样 Nespa 才有机会成为一个真正品牌化、平台化的分布式缓存系统。
