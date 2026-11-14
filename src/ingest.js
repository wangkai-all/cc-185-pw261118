'use strict';

// 分析侧接收端进程：只连分析库，对外只暴露 /ingest（供转发器调用）与健康检查。
// 网页不直接调用这里写数据——浏览器没有 RELAY_TOKEN，且本服务只接受事件投递。

const http = require('node:http');
const { openAnalytics, AnalyticsStore } = require('./db/analytics');
const { INGEST_PORT, RELAY_TOKEN } = require('./config');

const startedAt = new Date().toISOString();
const store = new AnalyticsStore(openAnalytics());

function log(msg) {
  console.log(`[ingest ${process.pid}] ${msg}`);
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(body);
}

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/health') {
    return sendJson(res, 200, { ok: true, pid: process.pid, startedAt });
  }

  if (req.method === 'POST' && req.url === '/ingest') {
    if (req.headers['x-relay-token'] !== RELAY_TOKEN) {
      return sendJson(res, 403, { error: '禁止直连：转发令牌缺失或错误' });
    }
    let raw = '';
    req.on('data', (chunk) => (raw += chunk));
    req.on('end', () => {
      let event;
      try {
        event = JSON.parse(raw);
      } catch {
        return sendJson(res, 400, { error: '请求体不是合法 JSON' });
      }
      const required = ['eventId', 'eventType', 'aggregateId', 'version', 'payload'];
      if (!required.every((k) => event[k] !== undefined)) {
        return sendJson(res, 400, { error: '事件字段不完整' });
      }
      try {
        const { result, shouldDropAck, firstResult } = store.ingest(event);
        if (shouldDropAck) {
          // 模拟：事件已在同一事务内提交并去重，但响应在网络中“丢失”
          log(`事件 ${event.eventId} 已落库（${result}），按开关丢弃确认`);
          return sendJson(res, 500, { error: 'ack_lost', note: '确认丢失（模拟）' });
        }
        if (result === 'duplicate') {
          log(`事件 ${event.eventId} 重复投递，幂等忽略（首次结果: ${firstResult}）`);
        } else {
          log(`事件 ${event.eventId} 处理结果: ${result}`);
        }
        return sendJson(res, 200, { result, firstResult: firstResult ?? null });
      } catch (err) {
        log(`处理异常: ${err.stack || err}`);
        return sendJson(res, 500, { error: '分析库处理失败' });
      }
    });
    return;
  }

  // 控制端点：由 web 服务携带转发令牌调用；分析库的写操作始终发生在本进程内
  if (req.method === 'POST' && req.url === '/control') {
    if (req.headers['x-relay-token'] !== RELAY_TOKEN) {
      return sendJson(res, 403, { error: '控制令牌缺失或错误' });
    }
    let raw = '';
    req.on('data', (chunk) => (raw += chunk));
    req.on('end', () => {
      try {
        const body = JSON.parse(raw || '{}');
        if (typeof body.dropNextAck === 'boolean') {
          store.setDropNextAck(body.dropNextAck);
          log(`确认丢失开关 -> ${body.dropNextAck ? '单次待触发' : '取消'}`);
        }
        return sendJson(res, 200, { dropNextAckArmed: store.isDropNextAckArmed() });
      } catch {
        return sendJson(res, 400, { error: '控制请求不合法' });
      }
    });
    return;
  }

  sendJson(res, 404, { error: 'not found' });
});

server.listen(INGEST_PORT, '127.0.0.1', () => {
  log(`分析侧接收端启动于 http://127.0.0.1:${INGEST_PORT}`);
});

const hbTimer = setInterval(() => store.heartbeat(process.pid, startedAt), 1000);

function shutdown(sig) {
  log(`收到 ${sig}，退出`);
  clearInterval(hbTimer);
  server.close(() => {
    store.db.close();
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 3000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
