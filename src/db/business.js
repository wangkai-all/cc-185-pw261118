'use strict';

// 业务库：订单 + 事务发件箱（outbox）+ 发送尝试日志 + 转发控制
// 关键不变量：orders 的行变更与 outbox 事件写入必须在同一个数据库事务内完成。

const fs = require('node:fs');
const { DatabaseSync } = require('node:sqlite');
const { BUSINESS_DB, DATA_DIR } = require('../config');

function openBusiness() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const db = new DatabaseSync(BUSINESS_DB);
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('PRAGMA foreign_keys=ON');
  db.exec(`
    CREATE TABLE IF NOT EXISTS orders (
      id          TEXT PRIMARY KEY,
      customer    TEXT NOT NULL,
      amount      REAL NOT NULL,
      status      TEXT NOT NULL,              -- active | cancelled
      version     INTEGER NOT NULL,
      created_at  TEXT NOT NULL,
      updated_at  TEXT NOT NULL
    );

    -- 事务发件箱：与订单同事务写入，业务回滚则事件一并回滚
    CREATE TABLE IF NOT EXISTS outbox_events (
      event_id        TEXT PRIMARY KEY,      -- 全局唯一，去重键
      aggregate_id    TEXT NOT NULL,         -- 订单 id
      event_type      TEXT NOT NULL,         -- OrderCreated | AmountChanged | OrderCancelled
      version         INTEGER NOT NULL,       -- 订单版本，单调递增
      payload         TEXT NOT NULL,          -- JSON 快照
      created_at      TEXT NOT NULL,
      status          TEXT NOT NULL DEFAULT 'pending', -- pending | sent
      sent_at         TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_outbox_pick
      ON outbox_events(status, created_at, event_id);

    -- 每一次发送尝试都留痕（含失败/确认丢失），供页面做关联追踪
    CREATE TABLE IF NOT EXISTS dispatch_attempts (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id        TEXT NOT NULL,
      attempt_no      INTEGER NOT NULL,
      outcome         TEXT NOT NULL,          -- acked | http_error | timeout | paused_skip
      http_status     INTEGER,
      detail          TEXT,
      attempted_at    TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_attempts_event ON dispatch_attempts(event_id);

    -- 转发器控制开关（由网页写入，forwarder 轮询读取，实现真正的进程间暂停）
    CREATE TABLE IF NOT EXISTS relay_control (
      id              INTEGER PRIMARY KEY CHECK (id = 1),
      forwarding_paused INTEGER NOT NULL DEFAULT 0,
      updated_at      TEXT NOT NULL
    );
    INSERT OR IGNORE INTO relay_control (id, forwarding_paused, updated_at)
      VALUES (1, 0, datetime('now'));

    -- 转发器心跳，页面据此显示消费者/转发器存活状态
    CREATE TABLE IF NOT EXISTS relay_heartbeat (
      id              INTEGER PRIMARY KEY CHECK (id = 1),
      pid             INTEGER,
      started_at      TEXT,
      beat_at         TEXT
    );
  `);
  return db;
}

function nowIso() {
  return new Date().toISOString();
}

function uuid() {
  return globalThis.crypto.randomUUID();
}

function orderSnapshot(o) {
  return JSON.stringify({
    orderId: o.id,
    customer: o.customer,
    amount: o.amount,
    status: o.status,
    version: o.version,
  });
}

class BusinessStore {
  constructor(db) {
    this.db = db;
  }

  setPaused(paused) {
    this.db
      .prepare('UPDATE relay_control SET forwarding_paused = ?, updated_at = ? WHERE id = 1')
      .run(paused ? 1 : 0, nowIso());
  }

  isPaused() {
    return this.db.prepare('SELECT forwarding_paused FROM relay_control WHERE id = 1').get()
      .forwarding_paused === 1;
  }

  heartbeat(pid, startedAt) {
    this.db
      .prepare(
        `INSERT INTO relay_heartbeat (id, pid, started_at, beat_at) VALUES (1, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET pid = excluded.pid,
           started_at = excluded.started_at, beat_at = excluded.beat_at`
      )
      .run(pid, startedAt, nowIso());
  }

  /**
   * 创建订单。failBeforeCommit=true 时在事务内主动 ROLLBACK，
   * 用于演示：业务回滚后 outbox 中不会留下任何可被消费的事件。
   */
  createOrder({ customer, amount }, failBeforeCommit = false) {
    const db = this.db;
    const id = uuid();
    const now = nowIso();
    const eventId = uuid();
    db.exec('BEGIN IMMEDIATE');
    try {
      db.prepare(
        `INSERT INTO orders (id, customer, amount, status, version, created_at, updated_at)
         VALUES (?, ?, ?, 'active', 1, ?, ?)`
      ).run(id, customer, amount, now, now);

      const snapshot = orderSnapshot({
        id,
        customer,
        amount,
        status: 'active',
        version: 1,
      });
      db.prepare(
        `INSERT INTO outbox_events
           (event_id, aggregate_id, event_type, version, payload, created_at)
         VALUES (?, ?, 'OrderCreated', 1, ?, ?)`
      ).run(eventId, id, snapshot, now);

      if (failBeforeCommit) {
        db.exec('ROLLBACK');
        return { rolledBack: true, orderId: id, eventId };
      }
      db.exec('COMMIT');
      return { rolledBack: false, orderId: id, eventId };
    } catch (err) {
      db.exec('ROLLBACK');
      throw err;
    }
  }

  /** 修改金额：订单版本 +1，同事务写 AmountChanged 事件 */
  changeAmount(orderId, newAmount) {
    return this._mutate(orderId, 'AmountChanged', (o) => ({
      ...o,
      amount: newAmount,
      version: o.version + 1,
    }));
  }

  /** 撤销订单：版本 +1，状态置 cancelled，同事务写 OrderCancelled 事件 */
  cancelOrder(orderId) {
    return this._mutate(orderId, 'OrderCancelled', (o) => ({
      ...o,
      status: 'cancelled',
      version: o.version + 1,
    }));
  }

  _mutate(orderId, eventType, mutate) {
    const db = this.db;
    const current = db.prepare('SELECT * FROM orders WHERE id = ?').get(orderId);
    if (!current) throw Object.assign(new Error('订单不存在'), { status: 404 });
    if (current.status === 'cancelled') {
      throw Object.assign(new Error('订单已撤销，不能继续修改'), { status: 409 });
    }
    const next = mutate(current);
    const now = nowIso();
    const eventId = uuid();
    db.exec('BEGIN IMMEDIATE');
    try {
      db.prepare(
        'UPDATE orders SET amount = ?, status = ?, version = ?, updated_at = ? WHERE id = ?'
      ).run(next.amount, next.status, next.version, now, orderId);

      db.prepare(
        `INSERT INTO outbox_events
           (event_id, aggregate_id, event_type, version, payload, created_at)
         VALUES (?, ?, ?, ?, ?, ?)`
      ).run(eventId, orderId, eventType, next.version, orderSnapshot(next), now);
      db.exec('COMMIT');
      return { orderId, eventId, version: next.version };
    } catch (err) {
      db.exec('ROLLBACK');
      throw err;
    }
  }

  // ---------- 查询（供网页展示） ----------

  listOrders() {
    return this.db.prepare('SELECT * FROM orders ORDER BY created_at DESC').all();
  }

  listOutbox() {
    return this.db
      .prepare('SELECT * FROM outbox_events ORDER BY created_at ASC, event_id ASC')
      .all();
  }

  listAttempts() {
    return this.db
      .prepare('SELECT * FROM dispatch_attempts ORDER BY id ASC')
      .all();
  }

  getHeartbeat() {
    return this.db.prepare('SELECT * FROM relay_heartbeat WHERE id = 1').get() || null;
  }

  /** 积压导出：尚未确认送达（pending）的事件 */
  backlogRows() {
    return this.db
      .prepare(
        `SELECT event_id, aggregate_id, event_type, version, status, created_at, sent_at
         FROM outbox_events WHERE status = 'pending'
         ORDER BY created_at ASC, event_id ASC`
      )
      .all();
  }
}

module.exports = { openBusiness, BusinessStore };
