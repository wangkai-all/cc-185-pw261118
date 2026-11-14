'use strict';

// 独立转发器进程：只连业务库读 outbox，通过 HTTP 把事件送往分析侧接收端。
// 它不与网页共享进程，网页只能通过 relay_control 表暂停/恢复它，或由 web 进程重启它。
//
// 投递语义：至少一次（at-least-once）。
//  - 成功确认后才把 outbox 标记为 sent；
//  - 确认丢失/网络错误时事件保持 pending，退避后重发；
//  - 分析侧凭 event_id 幂等，重发不会重复累计。

const { openBusiness, BusinessStore } = require('./db/business');
const {
  INGEST_URL,
  RELAY_TOKEN,
  POLL_INTERVAL_MS,
  MAX_BACKOFF_MS,
} = require('./config');

const startedAt = new Date().toISOString();
const store = new BusinessStore(openBusiness());

// 内存中的退避表：eventId -> 下次可尝试时间戳。重启进程即清空，积压事件立即重试。
const backoffUntil = new Map();

function log(msg) {
  console.log(`[forwarder ${process.pid}] ${msg}`);
}

async function postEvent(event) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 5000);
  try {
    const res = await fetch(`${INGEST_URL}/ingest`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-relay-token': RELAY_TOKEN,
      },
      body: JSON.stringify({
        eventId: event.event_id,
        eventType: event.event_type,
        aggregateId: event.aggregate_id,
        version: event.version,
        payload: JSON.parse(event.payload),
      }),
      signal: controller.signal,
    });
    let body = null;
    try {
      body = await res.json();
    } catch {
      /* 非 JSON 响应 */
    }
    return { ok: res.ok, status: res.status, body };
  } finally {
    clearTimeout(timer);
  }
}

function recordAttempt(eventId, attemptNo, outcome, httpStatus, detail) {
  store.db
    .prepare(
      `INSERT INTO dispatch_attempts
         (event_id, attempt_no, outcome, http_status, detail, attempted_at)
       VALUES (?, ?, ?, ?, ?, ?)`
    )
    .run(eventId, attemptNo, outcome, httpStatus ?? null, detail ?? null, new Date().toISOString());
}

function attemptNoOf(eventId) {
  const row = store.db
    .prepare('SELECT COUNT(*) AS n FROM dispatch_attempts WHERE event_id = ?')
    .get(eventId);
  return row.n + 1;
}

async function dispatchOne(event) {
  const attemptNo = attemptNoOf(event.event_id);
  let outcome;
  let status = null;
  let detail = null;
  let acked = false;

  try {
    const res = await postEvent(event);
    status = res.status;
    if (res.ok) {
      outcome = 'acked';
      detail = `分析侧结果: ${res.body?.result ?? 'unknown'}`;
      acked = true;
    } else if (res.body?.error === 'ack_lost') {
      // 事件实际已落库，但确认被模拟丢弃：保持 pending 以便重发
      outcome = 'http_error';
      detail = '确认丢失（ack_lost）：事件已在分析侧提交，将重发并由其幂等去重';
    } else {
      outcome = 'http_error';
      detail = `HTTP ${res.status}: ${res.body?.error ?? '未知错误'}`;
    }
  } catch (err) {
    outcome = err.name === 'AbortError' ? 'timeout' : 'http_error';
    detail = err.message;
  }

  recordAttempt(event.event_id, attemptNo, outcome, status, detail);

  if (acked) {
    store.db
      .prepare("UPDATE outbox_events SET status = 'sent', sent_at = ? WHERE event_id = ?")
      .run(new Date().toISOString(), event.event_id);
    backoffUntil.delete(event.event_id);
    log(`事件 ${event.event_id} (v${event.version} ${event.event_type}) 已确认`);
  } else {
    // 指数退避，封顶 MAX_BACKOFF_MS
    const delay = Math.min(MAX_BACKOFF_MS, 250 * 2 ** (attemptNo - 1));
    backoffUntil.set(event.event_id, Date.now() + delay);
    log(`事件 ${event.event_id} 投递失败（${outcome}），${delay}ms 后重试`);
  }
}

async function pump() {
  if (store.isPaused()) return;

  const now = Date.now();
  const pending = store.db
    .prepare(
      `SELECT * FROM outbox_events
       WHERE status = 'pending'
       ORDER BY created_at ASC, event_id ASC`
    )
    .all();

  for (const event of pending) {
    const until = backoffUntil.get(event.event_id) ?? 0;
    if (until > now) continue;
    // 暂停可能在循环期间被打开
    if (store.isPaused()) break;
    await dispatchOne(event);
  }
}

function heartbeatLoop() {
  store.heartbeat(process.pid, startedAt);
}

log(`启动，目标接收端 ${INGEST_URL}`);
heartbeatLoop();
const hbTimer = setInterval(heartbeatLoop, 1000);
const pumpTimer = setInterval(() => {
  pump().catch((err) => log(`轮询异常: ${err.stack || err}`));
}, POLL_INTERVAL_MS);

let shuttingDown = false;
function shutdown(sig) {
  if (shuttingDown) return;
  shuttingDown = true;
  log(`收到 ${sig}，退出`);
  clearInterval(hbTimer);
  clearInterval(pumpTimer);
  store.db.close();
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
