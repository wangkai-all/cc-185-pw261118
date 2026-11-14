# 业务库 → 分析库同步演示（事务发件箱 + 幂等投影）

一个从零实现、零第三方依赖（仅用 Node.js 内置 `node:sqlite` / `http` / `fetch`）的本地演示，
展示订单数据如何从**业务库**可靠同步到**分析库**，并可在网页里实际操作全部故障场景。

## 架构与数据流

```
浏览器 (public/)
   │  只调用 web 的 /api/*
   ▼
web 进程 (src/server.js)  ── 读写 ──▶ data/business.db   （订单、outbox、尝试日志、暂停开关）
   │  spawn / kill            ▲
   │                          │ 只读（展示）
   ├─▶ forwarder 子进程 (src/forwarder.js)
   │      轮询 business.db 的 outbox_events，HTTP POST 事件
   │      成功确认才标记 sent；失败/确认丢失则退避重发（至少一次）
   ▼
ingest 子进程 (src/ingest.js) ── 唯一写者 ──▶ data/analytics.db
      单事务内：event_id 去重 → 版本守卫 → 投影更新
```

关键点：**网页与 web 进程从不直接写分析库**。分析库的唯一写者是独立的 ingest 进程，
浏览器也拿不到转发令牌（`x-relay-token`），无法绕过转发器直投。

## 三个事务/消息保证

| 要求 | 实现位置 |
| --- | --- |
| 业务保存订单与出站事件同事务，业务回滚不留可消费事件 | `src/db/business.js` `createOrder/changeAmount/cancelOrder`（`BEGIN IMMEDIATE` … `COMMIT/ROLLBACK`） |
| 转发完成但确认丢失 → 重发不重复累计 | forwarder 仅在 ACK 后标记 sent；ingest 以 `event_id` 为幂等键，重复事件返回 `duplicate` 且投影不动（`src/db/analytics.js` `ingest`） |
| 去重标识与投影更新同一事务 | `analytics.ingest`：inbox 插入、冲突记录、投影 upsert 全部在一个事务内提交或回滚 |
| 旧事件迟到不能把状态改回去 | 版本守卫：`event.version <= projection.version` 时记 `conflicts` 表并拒绝，投影保持高版本 |
| 顺序 | outbox 按 `(created_at, event_id)` 顺序拉取；同一订单版本号单调递增，乱序到达由版本守卫兜底 |

## 启动

```bash
npm start          # 等同于 node src/server.js
# 打开 http://127.0.0.1:4000
```

web 进程会自动拉起并监督两个子进程（ingest: 4100，forwarder）。
也可以分别用 `npm run ingest`、`npm run forwarder` 手动启动独立进程。
环境变量：`DEMO_WEB_PORT`、`DEMO_INGEST_PORT`、`DEMO_INGEST_URL`、`DEMO_RELAY_TOKEN`、`DEMO_DATA_DIR`。

## 网页操作与建议演练路径

1. **创建订单**：业务库 `orders` 与 `outbox_events` 同事务出现；约 1 秒内分析库投影出现。
2. **勾选“模拟业务失败”再创建**：事务回滚，订单表与 outbox 都查不到该事件——证明回滚不会留下可消费事件。
3. **暂停转发** → 连续改金额、撤销订单：事件在 outbox 积压（页面可看到 pending 增长），
   此时可**导出积压 CSV**；点**恢复转发**后按序投递。
4. **模拟确认丢失**：先点“模拟下一次确认丢失”，再做一次修改。ingest 正常落库但返回 500（ack_lost），
   forwarder 退避后重发；关联追踪里能看到两次尝试，分析侧第二次返回 `duplicate`，投影金额只变化一次。
5. **重启消费者（转发器进程）**：真实 kill + respawn，pid 与重启次数变化，内存退避清空，积压立即重投。
6. **乱序/旧事件迟到**：暂停转发，对同一订单连续改两次金额（产生 v2、v3），
   在 forwarder 退避窗口与恢复时机交错下，较早版本可能更晚到达；
   即使 v2 晚于 v3 到达，也只会进 `conflicts` 表，投影保持 v3。冲突可**导出 CSV**。
7. **导出**：积压（pending 事件）、冲突（版本回退拒绝记录）、关联追踪（业务提交 ↔ 每次发送尝试 ↔ 分析结果）。

## 数据库表

- `business.db`：`orders`、`outbox_events`、`dispatch_attempts`、`relay_control`、`relay_heartbeat`
- `analytics.db`：`projected_orders`、`inbox_events`、`conflicts`、`ingest_control`、`ingest_heartbeat`

“重置全部数据”会停掉子进程、删除两个 db 文件（含 WAL/SHM）并重新初始化。
