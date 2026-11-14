'use strict';

// 分析库：订单投影 + 入站事件去重表 + 冲突表 + 接收端控制
// 关键不变量：event_id 去重检查、版本守卫与投影更新必须在同一个数据库事务内完成。
// 这样即使转发端重发（确认丢失），也不会重复累计；旧版本事件迟到也无法把状态改回去。

const fs = require('node:fs');
const { DatabaseSync } = require('node:sqlite');
const { ANALYTICS_DB, DATA_DIR } = require('../config');

function openAnalytics() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const db = new DatabaseSync(ANALYTICS_DB);
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('PRAGMA foreign_keys=ON');
  db.exec(`
    CREATE TABLE IF NOT EXISTS projected_orders (
      order_id      TEXT PRIMARY KEY,
      customer      TEXT NOT NULL,
      amount        REAL NOT NULL,
      status        TEXT NOT NULL,
      version       INTEGER NOT NULL,
      updated_at    TEXT NOT NULL
    );

    -- 入站事件账本：event_id 唯一，承担幂等去重
    CREATE TABLE IF NOT EXISTS inbox_events (
      event_id      TEXT PRIMARY KEY,
      order_id      TEXT NOT NULL,
      event_type    TEXT NOT NULL,
      version       INTEGER NOT NULL,
      result        TEXT NOT NULL,           -- applied | duplicate | conflict
      payload       TEXT NOT NULL,
      received_at   TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_inbox_order ON inbox_events(order_id);

    -- 版本冲突（旧事件迟到）留痕，支持导出
    CREATE TABLE IF NOT EXISTS conflicts (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id          TEXT NOT NULL,
      order_id          TEXT NOT NULL,
      event_version     INTEGER NOT NULL,
      current_version   INTEGER NOT NULL,
      reason            TEXT NOT NULL,
      detected_at       TEXT NOT NULL
    );

    -- 接收端控制开关：drop_next_ack=1 时，下一个事件正常落库但丢弃 HTTP 确认
    CREATE TABLE IF NOT EXISTS ingest_control (
      id                INTEGER PRIMARY KEY CHECK (id = 1),
      drop_next_ack     INTEGER NOT NULL DEFAULT 0,
      updated_at        TEXT NOT NULL
    );
    INSERT OR IGNORE INTO ingest_control (id, drop_next_ack, updated_at)
      VALUES (1, 0, datetime('now'));

    CREATE TABLE IF NOT EXISTS ingest_heartbeat (
      id                INTEGER PRIMARY KEY CHECK (id = 1),
      pid               INTEGER,
      started_at        TEXT,
      beat_at           TEXT
    );
  `);
  return db;
}

function nowIso() {
  return new Date().toISOString();
}

class AnalyticsStore {
  constructor(db) {
    this.db = db;
  }

  setDropNextAck(on) {
    this.db
      .prepare('UPDATE ingest_control SET drop_next_ack = ?, updated_at = ? WHERE id = 1')
      .run(on ? 1 : 0, nowIso());
  }

  consumeDropNextAck() {
    const row = this.db.prepare('SELECT drop_next_ack FROM ingest_control WHERE id = 1').get();
    if (row && row.drop_next_ack === 1) {
      this.db
        .prepare('UPDATE ingest_control SET drop_next_ack = 0, updated_at = ? WHERE id = 1')
        .run(nowIso());
      return true;
    }
    return false;
  }

  isDropNextAckArmed() {
    const row = this.db.prepare('SELECT drop_next_ack FROM ingest_control WHERE id = 1').get();
    return !!row && row.drop_next_ack === 1;
  }

  heartbeat(pid, startedAt) {
    this.db
      .prepare(
        `INSERT INTO ingest_heartbeat (id, pid, started_at, beat_at) VALUES (1, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET pid = excluded.pid,
           started_at = excluded.started_at, beat_at = excluded.beat_at`
      )
      .run(pid, startedAt, nowIso());
  }

  /**
   * 接收一个事件。整个处理在单事务内：
   *  1) event_id 已存在 -> duplicate，投影完全不动（重发不重复累计）
   *  2) 事件版本 <= 投影当前版本 -> conflict，记冲突表，投影不动（旧事件不能回退状态）
   *  3) 否则应用快照 -> applied
   * 返回 { result, shouldDropAck }；shouldDropAck 表示确认要被模拟丢弃。
   */
  ingest(event) {
    const db = this.db;
    const { eventId, eventType, aggregateId, version, payload } = event;
    const now = nowIso();

    db.exec('BEGIN IMMEDIATE');
    try {
      const seen = db
        .prepare('SELECT event_id, result FROM inbox_events WHERE event_id = ?')
        .get(eventId);

      if (seen) {
        db.exec('COMMIT');
        // 重复投递：不做任何写入与投影变更
        return { result: 'duplicate', firstResult: seen.result, shouldDropAck: false };
      }

      const projection = db
        .prepare('SELECT * FROM projected_orders WHERE order_id = ?')
        .get(aggregateId);

      if (projection && version <= projection.version) {
        // 旧版本事件迟到：记冲突，投影保持高版本状态
        db.prepare(
          `INSERT INTO inbox_events
             (event_id, order_id, event_type, version, result, payload, received_at)
           VALUES (?, ?, ?, ?, 'conflict', ?, ?)`
        ).run(eventId, aggregateId, eventType, version, JSON.stringify(payload), now);

        db.prepare(
          `INSERT INTO conflicts
             (event_id, order_id, event_version, current_version, reason, detected_at)
           VALUES (?, ?, ?, ?, ?, ?)`
        ).run(
          eventId,
          aggregateId,
          version,
          projection.version,
          `事件版本 ${version} 不高于投影当前版本 ${projection.version}，拒绝回退`,
          now
        );
        db.exec('COMMIT');
        return { result: 'conflict', shouldDropAck: false };
      }

      // 新版本：投影更新与去重标记同事务提交
      const snap = typeof payload === 'string' ? JSON.parse(payload) : payload;
      db.prepare(
        `INSERT INTO projected_orders (order_id, customer, amount, status, version, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(order_id) DO UPDATE SET
           customer = excluded.customer,
           amount = excluded.amount,
           status = excluded.status,
           version = excluded.version,
           updated_at = excluded.updated_at`
      ).run(snap.orderId, snap.customer, snap.amount, snap.status, snap.version, now);

      db.prepare(
        `INSERT INTO inbox_events
           (event_id, order_id, event_type, version, result, payload, received_at)
         VALUES (?, ?, ?, ?, 'applied', ?, ?)`
      ).run(eventId, aggregateId, eventType, version, JSON.stringify(snap), now);

      // 确认丢失模拟：事件已提交，之后由调用方丢弃响应
      const shouldDropAck = this.consumeDropNextAck();
      db.exec('COMMIT');
      return { result: 'applied', shouldDropAck };
    } catch (err) {
      db.exec('ROLLBACK');
      throw err;
    }
  }

  // ---------- 查询 ----------

  listProjections() {
    return this.db.prepare('SELECT * FROM projected_orders ORDER BY updated_at DESC').all();
  }

  listInbox() {
    return this.db.prepare('SELECT * FROM inbox_events ORDER BY received_at ASC').all();
  }

  listConflicts() {
    return this.db.prepare('SELECT * FROM conflicts ORDER BY id ASC').all();
  }

  getHeartbeat() {
    return this.db.prepare('SELECT * FROM ingest_heartbeat WHERE id = 1').get() || null;
  }
}

module.exports = { openAnalytics, AnalyticsStore };
