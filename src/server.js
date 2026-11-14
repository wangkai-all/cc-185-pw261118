'use strict';

// Web 服务进程：
//  - 只对业务库有写权限（创建/修改订单、暂停开关）；
//  - 对分析库只读，用于展示投影/入站/冲突；
//  - 作为监督者拉起独立的 ingest（消费者）与 forwarder（转发器）子进程，
//    网页的“重启消费者”通过 kill + 重新 spawn 实现，是真实的进程重启。
// 页面任何操作都不会直接写分析库——分析库的唯一写者是 ingest 进程。

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { DatabaseSync } = require('node:sqlite');

const {
  ROOT,
  DATA_DIR,
  BUSINESS_DB,
  ANALYTICS_DB,
  WEB_PORT,
  INGEST_URL,
  RELAY_TOKEN,
} = require('./config');
const { openBusiness, BusinessStore } = require('./db/business');
const { AnalyticsStore } = require('./db/analytics');

// ---------- 业务库（读写）与分析库（只读） ----------
const business = new BusinessStore(openBusiness());

// 分析库只读连接缓存（仅用于展示，绝不在 web 进程里写分析库）
let analyticsRo = null;
function openAnalyticsReadOnly(retries = 20) {
  if (analyticsRo) return analyticsRo;
  for (let i = 0; i < retries; i++) {
    try {
      const db = new DatabaseSync(ANALYTICS_DB, { readOnly: true });
      analyticsRo = new AnalyticsStore(db);
      return analyticsRo;
    } catch {
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
    }
  }
  return null;
}

function closeAnalyticsReadOnly() {
  if (analyticsRo) {
    try {
      analyticsRo.db.close();
    } catch {
      /* 忽略 */
    }
    analyticsRo = null;
  }
}

// ---------- 子进程监督 ----------
const children = new Map(); // name -> { proc, logs: [], status, restartCount }

function makeChild(name, script) {
  const entry = {
    name,
    proc: null,
    status: 'starting',
    restartCount: children.get(name)?.restartCount ?? 0,
    logs: [],
    startedAt: null,
  };
  children.set(name, entry);

  const proc = spawn(process.execPath, [path.join(ROOT, 'src', script)], {
    cwd: ROOT,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: process.env,
  });
  entry.proc = proc;
  entry.startedAt = new Date().toISOString();

  const capture = (stream, tag) => {
    stream.setEncoding('utf8');
    stream.on('data', (chunk) => {
      chunk
        .split('\n')
        .filter(Boolean)
        .forEach((line) => {
          entry.logs.push(`${tag} ${line}`);
          if (entry.logs.length > 200) entry.logs.shift();
        });
    });
  };
  capture(proc.stdout, '[stdout]');
  capture(proc.stderr, '[stderr]');

  proc.on('exit', (code, signal) => {
    entry.status = `exited(${signal ?? code})`;
    entry.logs.push(`[supervisor] 进程退出 code=${code} signal=${signal}`);
  });
  proc.on('spawn', () => {
    entry.status = 'running';
  });
  return entry;
}

function stopChild(name) {
  const entry = children.get(name);
  if (!entry || !entry.proc || entry.proc.exitCode !== null) return;
  entry.status = 'stopping';
  entry.proc.kill('SIGTERM');
}

function startAll() {
  makeChild('ingest', 'ingest.js');
  makeChild('forwarder', 'forwarder.js');
}

function waitForExit(entry, timeoutMs = 3000) {
  return new Promise((resolve) => {
    if (!entry || !entry.proc || entry.proc.exitCode !== null) return resolve();
    const t = setTimeout(() => resolve(), timeoutMs);
    entry.proc.once('exit', () => {
      clearTimeout(t);
      resolve();
    });
  });
}

function restartForwarder() {
  const old = children.get('forwarder');
  if (old && old.proc) {
    old.proc.removeAllListeners('exit');
    old.proc.kill('SIGTERM');
    const entry = makeChild('forwarder', 'forwarder.js');
    entry.restartCount = (old.restartCount ?? 0) + 1;
    return entry;
  }
  return makeChild('forwarder', 'forwarder.js');
}

// ---------- 工具 ----------
function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', () => {
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch (err) {
        reject(Object.assign(new Error('请求体不是合法 JSON'), { status: 400 }));
      }
    });
    req.on('error', reject);
  });
}

function csvCell(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function toCsv(rows, headers) {
  const lines = [headers.map((h) => csvCell(h.label)).join(',')];
  for (const row of rows) {
    lines.push(headers.map((h) => csvCell(row[h.key])).join(','));
  }
  // BOM 让 Excel 正确识别 UTF-8
  return '﻿' + lines.join('\n');
}

function sendCsv(res, filename, csv) {
  res.writeHead(200, {
    'content-type': 'text/csv; charset=utf-8',
    'content-disposition': `attachment; filename="${filename}"`,
  });
  res.end(csv);
}

function staleHeartbeat(row, thresholdMs = 3000) {
  if (!row || !row.beat_at) return true;
  return Date.now() - new Date(row.beat_at).getTime() > thresholdMs;
}

async function callIngestControl(payload) {
  const res = await fetch(`${INGEST_URL}/control`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-relay-token': RELAY_TOKEN },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`ingest 控制失败 HTTP ${res.status}`);
  return res.json();
}

// ---------- 状态聚合 ----------
function buildState() {
  const analytics = openAnalyticsReadOnly();
  const hb = business.getHeartbeat();
  const forwarderEntry = children.get('forwarder');
  const ingestEntry = children.get('ingest');

  const outbox = business.listOutbox();
  const attempts = business.listAttempts();
  const inbox = analytics ? analytics.listInbox() : [];
  const inboxById = new Map(inbox.map((e) => [e.event_id, e]));

  // 关联视图：业务提交 → 每次发送尝试 → 分析侧结果
  const attemptsByEvent = new Map();
  for (const a of attempts) {
    if (!attemptsByEvent.has(a.event_id)) attemptsByEvent.set(a.event_id, []);
    attemptsByEvent.get(a.event_id).push(a);
  }

  const correlation = outbox.map((e) => {
    const tries = attemptsByEvent.get(e.event_id) || [];
    const inboxRow = inboxById.get(e.event_id) || null;
    return {
      eventId: e.event_id,
      orderId: e.aggregate_id,
      eventType: e.event_type,
      version: e.version,
      outboxStatus: e.status,
      createdAt: e.created_at,
      sentAt: e.sent_at,
      attempts: tries.map((t) => ({
        no: t.attempt_no,
        outcome: t.outcome,
        httpStatus: t.http_status,
        detail: t.detail,
        at: t.attempted_at,
      })),
      analyticsResult: inboxRow ? inboxRow.result : null,
      analyticsReceivedAt: inboxRow ? inboxRow.received_at : null,
    };
  });

  return {
    time: new Date().toISOString(),
    controls: {
      forwardingPaused: business.isPaused(),
      dropNextAckArmed: analytics ? analytics.isDropNextAckArmed() : false,
    },
    processes: {
      forwarder: {
        status: forwarderEntry?.status ?? 'unknown',
        pid: forwarderEntry?.proc?.pid ?? null,
        startedAt: forwarderEntry?.startedAt ?? null,
        restartCount: forwarderEntry?.restartCount ?? 0,
        heartbeatFresh: !staleHeartbeat(hb),
        heartbeatAt: hb?.beat_at ?? null,
      },
      ingest: {
        status: ingestEntry?.status ?? 'unknown',
        pid: ingestEntry?.proc?.pid ?? null,
        startedAt: ingestEntry?.startedAt ?? null,
        analyticsHeartbeat: analytics ? analytics.getHeartbeat() : null,
        heartbeatFresh: analytics ? !staleHeartbeat(analytics.getHeartbeat()) : false,
      },
    },
    orders: business.listOrders(),
    outbox,
    attempts,
    projections: analytics ? analytics.listProjections() : [],
    inbox,
    conflicts: analytics ? analytics.listConflicts() : [],
    correlation,
    logs: {
      forwarder: forwarderEntry?.logs.slice(-80) ?? [],
      ingest: ingestEntry?.logs.slice(-80) ?? [],
    },
  };
}

// ---------- HTTP 路由 ----------
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const route = `${req.method} ${url.pathname}`;
  try {
    if (route === 'GET /api/state') return sendJson(res, 200, buildState());

    if (route === 'POST /api/orders') {
      const body = await readBody(req);
      const customer = String(body.customer ?? '').trim();
      const amount = Number(body.amount);
      if (!customer) return sendJson(res, 400, { error: '客户名不能为空' });
      if (!Number.isFinite(amount) || amount <= 0)
        return sendJson(res, 400, { error: '金额必须是正数' });
      const result = business.createOrder({ customer, amount }, !!body.simulateRollback);
      return sendJson(res, 200, result);
    }

    if (route === 'POST /api/orders/amount') {
      const body = await readBody(req);
      const amount = Number(body.amount);
      if (!body.orderId) return sendJson(res, 400, { error: '缺少 orderId' });
      if (!Number.isFinite(amount) || amount <= 0)
        return sendJson(res, 400, { error: '金额必须是正数' });
      return sendJson(res, 200, business.changeAmount(String(body.orderId), amount));
    }

    if (route === 'POST /api/orders/cancel') {
      const body = await readBody(req);
      if (!body.orderId) return sendJson(res, 400, { error: '缺少 orderId' });
      return sendJson(res, 200, business.cancelOrder(String(body.orderId)));
    }

    if (route === 'POST /api/relay/pause') {
      business.setPaused(true);
      return sendJson(res, 200, { forwardingPaused: true });
    }

    if (route === 'POST /api/relay/resume') {
      business.setPaused(false);
      return sendJson(res, 200, { forwardingPaused: false });
    }

    if (route === 'POST /api/relay/restart') {
      const entry = restartForwarder();
      return sendJson(res, 200, {
        restarted: true,
        pid: entry.proc.pid,
        restartCount: entry.restartCount,
      });
    }

    if (route === 'POST /api/ingest/simulate-ack-loss') {
      const body = await readBody(req);
      const result = await callIngestControl({ dropNextAck: body.arm !== false });
      return sendJson(res, 200, result);
    }

    if (route === 'POST /api/reset') {
      await resetAll();
      return sendJson(res, 200, { reset: true });
    }

    if (route === 'GET /api/export/backlog') {
      const csv = toCsv(business.backlogRows(), [
        { key: 'event_id', label: 'event_id' },
        { key: 'aggregate_id', label: 'order_id' },
        { key: 'event_type', label: 'event_type' },
        { key: 'version', label: 'version' },
        { key: 'status', label: 'outbox_status' },
        { key: 'created_at', label: 'created_at' },
        { key: 'sent_at', label: 'sent_at' },
      ]);
      return sendCsv(res, 'backlog.csv', csv);
    }

    if (route === 'GET /api/export/conflicts') {
      const analytics = openAnalyticsReadOnly();
      const csv = toCsv(analytics ? analytics.listConflicts() : [], [
        { key: 'id', label: 'id' },
        { key: 'event_id', label: 'event_id' },
        { key: 'order_id', label: 'order_id' },
        { key: 'event_version', label: 'event_version' },
        { key: 'current_version', label: 'current_version' },
        { key: 'reason', label: 'reason' },
        { key: 'detected_at', label: 'detected_at' },
      ]);
      return sendCsv(res, 'conflicts.csv', csv);
    }

    if (route === 'GET /api/export/correlation') {
      const rows = buildState().correlation.flatMap((c) =>
        c.attempts.length
          ? c.attempts.map((a) => ({
              event_id: c.eventId,
              order_id: c.orderId,
              event_type: c.eventType,
              version: c.version,
              attempt_no: a.no,
              attempt_outcome: a.outcome,
              http_status: a.httpStatus ?? '',
              attempt_detail: a.detail ?? '',
              attempted_at: a.at,
              outbox_status: c.outboxStatus,
              analytics_result: c.analyticsResult ?? '',
            }))
          : [
              {
                event_id: c.eventId,
                order_id: c.orderId,
                event_type: c.eventType,
                version: c.version,
                attempt_no: '',
                attempt_outcome: '',
                http_status: '',
                attempt_detail: '',
                attempted_at: '',
                outbox_status: c.outboxStatus,
                analytics_result: c.analyticsResult ?? '',
              },
            ]
      );
      const csv = toCsv(rows, [
        'event_id',
        'order_id',
        'event_type',
        'version',
        'attempt_no',
        'attempt_outcome',
        'http_status',
        'attempt_detail',
        'attempted_at',
        'outbox_status',
        'analytics_result',
      ].map((k) => ({ key: k, label: k })));
      return sendCsv(res, 'correlation.csv', csv);
    }

    return serveStatic(url.pathname, res);
  } catch (err) {
    const status = err.status || 500;
    return sendJson(res, status, { error: err.message || '服务器内部错误' });
  }
});

// ---------- 静态文件 ----------
const PUBLIC_DIR = path.join(ROOT, 'public');
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
};

function serveStatic(pathname, res) {
  const rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const file = path.normalize(path.join(PUBLIC_DIR, rel));
  if (!file.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    return res.end('forbidden');
  }
  fs.readFile(file, (err, data) => {
    if (err) {
      res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
      return res.end('not found');
    }
    res.writeHead(200, { 'content-type': MIME[path.extname(file)] || 'application/octet-stream' });
    res.end(data);
  });
}

// ---------- 重置 ----------
async function resetAll() {
  const entries = [...children.entries()];
  for (const [, entry] of entries) stopChild(entry.name);
  await Promise.all(entries.map(([, entry]) => waitForExit(entry)));
  children.clear();

  closeAnalyticsReadOnly();
  business.db.close();

  // 等待 SQLite 文件锁释放（WAL/SHM 一并删除）
  for (let i = 0; i < 30; i++) {
    try {
      for (const f of fs.readdirSync(DATA_DIR)) {
        if (f.startsWith('business.db') || f.startsWith('analytics.db')) {
          fs.rmSync(path.join(DATA_DIR, f), { force: true });
        }
      }
      break;
    } catch {
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
    }
  }

  business.db = openBusiness();
  startAll();
}

server.listen(WEB_PORT, '127.0.0.1', () => {
  console.log(`[web ${process.pid}] 页面地址 http://127.0.0.1:${WEB_PORT}`);
  startAll();
});

function shutdown() {
  for (const name of [...children.keys()]) stopChild(name);
  setTimeout(() => process.exit(0), 500);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
