'use strict';

const $ = (sel) => document.querySelector(sel);

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { 'content-type': 'application/json' },
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

function toast(msg, ok = true) {
  const el = document.createElement('div');
  el.className = `toast-item ${ok ? 'ok' : 'err'}`;
  el.textContent = msg;
  $('#toast').appendChild(el);
  setTimeout(() => el.remove(), 4200);
}

function esc(v) {
  return String(v ?? '').replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])
  );
}

function shortId(id) {
  return id ? id.slice(0, 8) : '';
}

function fmtTime(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  return d.toLocaleTimeString('zh-CN', { hour12: false }) +
    '.' + String(d.getMilliseconds()).padStart(3, '0');
}

function pill(text, cls) {
  return `<span class="pill ${cls}">${esc(text)}</span>`;
}

function badge(text, cls) {
  return `<span class="badge ${cls}">${esc(text)}</span>`;
}

// ---------- 渲染 ----------
function renderStatus(s) {
  const f = s.processes.forwarder;
  const ing = s.processes.ingest;
  const fPill = f.heartbeatFresh ? pill('存活', 'ok') : pill('无心跳', 'bad');
  const iPill = ing.heartbeatFresh ? pill('存活', 'ok') : pill('无心跳', 'bad');
  $('#status-grid').innerHTML = `
    <div class="status-item">
      <div class="label">转发器 forwarder（独立进程）</div>
      <div class="value">${esc(f.status)} ${fPill}
        <span class="mono" style="font-weight:400">pid ${f.pid ?? '-'} · 重启 ${f.restartCount} 次</span>
      </div>
    </div>
    <div class="status-item">
      <div class="label">消费者 ingest（独立进程，分析库唯一写者）</div>
      <div class="value">${esc(ing.status)} ${iPill}
        <span class="mono" style="font-weight:400">pid ${ing.pid ?? '-'}</span>
      </div>
    </div>
    <div class="status-item">
      <div class="label">转发开关（relay_control 表）</div>
      <div class="value">${s.controls.forwardingPaused ? pill('已暂停', 'warn') : pill('运行中', 'ok')}</div>
    </div>
    <div class="status-item">
      <div class="label">确认丢失开关（分析侧 ingest_control）</div>
      <div class="value">${s.controls.dropNextAckArmed ? pill('下一次确认将丢失', 'bad') : pill('关闭', 'gray')}</div>
    </div>
    <div class="status-item">
      <div class="label">事件计数</div>
      <div class="value">出站 ${s.outbox.length} · 入站 ${s.inbox.length} ·
        待发积压 ${s.outbox.filter((e) => e.status === 'pending').length} · 冲突 ${s.conflicts.length}</div>
    </div>`;
  $('#control-hint').textContent = s.controls.dropNextAckArmed
    ? '已武装：下一个到达分析侧的事件会正常落库，但 HTTP 确认被丢弃，转发器重发后由 event_id 幂等忽略。'
    : '';
}

function renderOrders(s) {
  const tbody = $('#orders-table tbody');
  if (!s.orders.length) {
    tbody.innerHTML = '<tr><td colspan="7" style="color:#6b7280">暂无订单</td></tr>';
    return;
  }
  tbody.innerHTML = s.orders
    .map(
      (o) => `
    <tr>
      <td class="mono" title="${esc(o.id)}">${shortId(o.id)}</td>
      <td>${esc(o.customer)}</td>
      <td>${Number(o.amount).toFixed(2)}</td>
      <td>${badge(o.status, o.status === 'active' ? 'active' : 'cancelled')}</td>
      <td><strong>v${o.version}</strong></td>
      <td class="mono">${fmtTime(o.updated_at)}</td>
      <td><button class="mini" data-order="${esc(o.id)}">选择</button></td>
    </tr>`
    )
    .join('');
  tbody.querySelectorAll('button[data-order]').forEach((btn) =>
    btn.addEventListener('click', () => {
      $('#selected-order').value = btn.dataset.order;
    })
  );
}

function renderCorrelation(s) {
  const tbody = $('#corr-table tbody');
  if (!s.correlation.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="color:#6b7280">暂无事件</td></tr>';
    return;
  }
  tbody.innerHTML = s.correlation
    .map((c) => {
      const attempts = c.attempts.length
        ? c.attempts
            .map(
              (a) => `
          <div class="attempt ${esc(a.outcome)}">
            #${a.no} ${badge(a.outcome, a.outcome === 'acked' ? 'acked' : a.outcome)}
            ${a.httpStatus ? `HTTP ${a.httpStatus}` : ''}
            <div class="meta">${fmtTime(a.at)}</div>
            ${a.detail ? `<div>${esc(a.detail)}</div>` : ''}
          </div>`
            )
            .join('')
        : '<span style="color:#6b7280">尚未尝试</span>';
      const result = c.analyticsResult
        ? badge(c.analyticsResult, c.analyticsResult)
        : badge('未到达', 'gray');
      return `
      <tr>
        <td class="mono" title="${esc(c.eventId)}">${shortId(c.eventId)}</td>
        <td class="mono" title="${esc(c.orderId)}">${shortId(c.orderId)}</td>
        <td>${esc(c.eventType)}</td>
        <td><strong>v${c.version}</strong></td>
        <td>${badge(c.outboxStatus, c.outboxStatus)}</td>
        <td>${attempts}</td>
        <td>${result}<div class="meta mono" style="color:#6b7280">${fmtTime(c.analyticsReceivedAt)}</div></td>
        <td class="mono">${fmtTime(c.createdAt)}</td>
      </tr>`;
    })
    .join('');
}

function renderProjections(s) {
  const tbody = $('#proj-table tbody');
  if (!s.projections.length) {
    tbody.innerHTML = '<tr><td colspan="6" style="color:#6b7280">分析库暂无投影（页面不会直接写这里）</td></tr>';
    return;
  }
  tbody.innerHTML = s.projections
    .map(
      (p) => `
    <tr>
      <td class="mono" title="${esc(p.order_id)}">${shortId(p.order_id)}</td>
      <td>${esc(p.customer)}</td>
      <td>${Number(p.amount).toFixed(2)}</td>
      <td>${badge(p.status, p.status === 'active' ? 'active' : 'cancelled')}</td>
      <td><strong>v${p.version}</strong></td>
      <td class="mono">${fmtTime(p.updated_at)}</td>
    </tr>`
    )
    .join('');
}

function renderInboxAndConflicts(s) {
  const inbox = $('#inbox-table tbody');
  inbox.innerHTML = s.inbox.length
    ? s.inbox
        .map(
          (e) => `
      <tr>
        <td class="mono" title="${esc(e.event_id)}">${shortId(e.event_id)}</td>
        <td class="mono">${shortId(e.order_id)}</td>
        <td>${esc(e.event_type)}</td>
        <td>v${e.version}</td>
        <td>${badge(e.result, e.result)}</td>
        <td class="mono">${fmtTime(e.received_at)}</td>
      </tr>`
        )
        .join('')
    : '<tr><td colspan="6" style="color:#6b7280">暂无</td></tr>';

  const conflicts = $('#conflict-table tbody');
  conflicts.innerHTML = s.conflicts.length
    ? s.conflicts
        .map(
          (c) => `
      <tr>
        <td class="mono" title="${esc(c.event_id)}">${shortId(c.event_id)}</td>
        <td class="mono">${shortId(c.order_id)}</td>
        <td class="badge conflict">v${c.event_version}</td>
        <td>v${c.current_version}</td>
        <td>${esc(c.reason)}</td>
        <td class="mono">${fmtTime(c.detected_at)}</td>
      </tr>`
        )
        .join('')
    : '<tr><td colspan="6" style="color:#6b7280">暂无冲突</td></tr>';
}

function renderLogs(s) {
  $('#log-forwarder').textContent = s.logs.forwarder.join('\n') || '(暂无日志)';
  $('#log-ingest').textContent = s.logs.ingest.join('\n') || '(暂无日志)';
  const f = $('#log-forwarder');
  f.scrollTop = f.scrollHeight;
  const i = $('#log-ingest');
  i.scrollTop = i.scrollHeight;
}

async function refresh() {
  try {
    const s = await api('/api/state');
    renderStatus(s);
    renderOrders(s);
    renderCorrelation(s);
    renderProjections(s);
    renderInboxAndConflicts(s);
    renderLogs(s);
  } catch (err) {
    // 轮询失败不弹 toast，避免刷屏
    console.error(err);
  }
}

// ---------- 交互 ----------
$('#create-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  try {
    const r = await api('/api/orders', {
      method: 'POST',
      body: {
        customer: fd.get('customer'),
        amount: Number(fd.get('amount')),
        simulateRollback: fd.get('simulateRollback') === 'on',
      },
    });
    if (r.rolledBack) {
      toast('业务事务已回滚：订单与出站事件均未留下（可验证 outbox 无此事件）', false);
    } else {
      toast(`订单已创建 ${shortId(r.orderId)}，事件 ${shortId(r.eventId)} 同事务写入 outbox`);
      e.target.reset();
    }
  } catch (err) {
    toast(err.message, false);
  }
  refresh();
});

$('#mutate-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const orderId = fd.get('orderId');
  const action = e.submitter?.value || 'amount';
  if (!orderId) return toast('请先在订单表选择一个订单', false);
  try {
    if (action === 'cancel') {
      const r = await api('/api/orders/cancel', { method: 'POST', body: { orderId } });
      toast(`订单已撤销，版本推进到 v${r.version}`);
    } else {
      const amount = Number(fd.get('amount'));
      if (!amount) return toast('请填写新金额', false);
      const r = await api('/api/orders/amount', {
        method: 'POST',
        body: { orderId, amount },
      });
      toast(`金额已修改，版本推进到 v${r.version}`);
    }
  } catch (err) {
    toast(err.message, false);
  }
  refresh();
});

function bindButton(id, path, body, okMsg) {
  $(id).addEventListener('click', async () => {
    try {
      const r = await api(path, { method: 'POST', body });
      toast(typeof okMsg === 'function' ? okMsg(r) : okMsg);
    } catch (err) {
      toast(err.message, false);
    }
    refresh();
  });
}

bindButton('#btn-pause', '/api/relay/pause', {}, '转发已暂停：事件在 outbox 积压，可导出 backlog.csv');
bindButton('#btn-resume', '/api/relay/resume', {}, '转发已恢复');
bindButton(
  '#btn-restart',
  '/api/relay/restart',
  {},
  (r) => `转发器已真实重启，新 pid ${r.pid}；内存退避清空，积压事件立即重投`
);
bindButton('#btn-ackloss', '/api/ingest/simulate-ack-loss', { arm: true }, '已武装：下一次事件的确认将丢失（事件本身仍会落库）');
bindButton('#btn-ackloss-cancel', '/api/ingest/simulate-ack-loss', { arm: false }, '已取消确认丢失模拟');
$('#btn-reset').addEventListener('click', async () => {
  if (!confirm('确认清空两个数据库并重启所有进程？')) return;
  try {
    await api('/api/reset', { method: 'POST' });
    toast('已重置：两个数据库文件删除并重建，子进程重启');
  } catch (err) {
    toast(err.message, false);
  }
  refresh();
});

refresh();
setInterval(refresh, 1000);
