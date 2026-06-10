(function () {
  'use strict';

  // ========== UTILS ==========
  const $ = id => document.getElementById(id);
  const $$ = sel => document.querySelectorAll(sel);
  function escapeHtml(s) { return (s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
  function formatDuration(ms) {
    if (ms <= 0) return '-';
    const s = Math.floor(ms / 1000);
    if (s < 60) return s + 's';
    const m = Math.floor(s / 60);
    if (m < 60) return m + 'm' + (s % 60) + 's';
    const h = Math.floor(m / 60);
    return h + 'h' + (m % 60) + 'm';
  }
  function maskKey(k) { k = k||''; return k.length > 12 ? k.slice(0,6) + '\u2026' + k.slice(-4) : k; }
  async function apiFetch(path, opts) {
    opts = opts || {}; opts.headers = opts.headers || {};
    const t = getToken();
    if (t) opts.headers['X-Admin-Token'] = t;
    const res = await fetch(path, opts);
    const text = await res.text();
    let json = null; try { json = JSON.parse(text); } catch(_) {}
    return { res, text, json };
  }

  // ========== THEME ==========
  const themeToggle = $('themeToggle');
  function applyTheme(theme) {
    document.body.classList.remove('dark','light');
    if (theme === 'dark' || theme === 'light') document.body.classList.add(theme);
    if (themeToggle) themeToggle.textContent = theme === 'dark' ? '浅色' : '深色';
  }
  applyTheme(localStorage.getItem('gptload_theme') || '');
  if (themeToggle) themeToggle.onclick = () => {
    const next = document.body.classList.contains('dark') ? 'light' : 'dark';
    localStorage.setItem('gptload_theme', next); applyTheme(next);
    if (typeof refreshRequestsChart === 'function') refreshRequestsChart();
    if (typeof renderKeyLatencyChart === 'function') renderKeyLatencyChart(lastKeyStatusList);
  };

  // ========== NAV ==========
  $$('.nav-btn').forEach(btn => {
    btn.onclick = () => {
      $$('.nav-btn').forEach(b => b.classList.remove('active'));
      $$('.page').forEach(p => p.classList.remove('active'));
      btn.classList.add('active');
      const page = $('page-' + btn.dataset.page);
      if (page) page.classList.add('active');
    };
  });

  // ========== TOKEN ==========
  const tokenInput = $('token');
  function getToken() { return localStorage.getItem('gptload_admin_token') || ''; }
  function setToken(t) { localStorage.setItem('gptload_admin_token', t); }
  tokenInput.value = getToken();
  $('saveToken').onclick = () => { setToken(tokenInput.value.trim()); startStatsStream(); refreshUpstreams(); startRequestsAutoRefresh(); };
  $('clearToken').onclick = () => { localStorage.removeItem('gptload_admin_token'); tokenInput.value = ''; stopStatsStream(); stopRequestsAutoRefresh(); stopRequestsStream(); };

  // ========== STATS STREAM ==========
  const statsGrid = $('statsGrid');
  let statsAbort = null, statsRetryTimer = null;
  function scheduleStatsRetry() { if (!statsRetryTimer) statsRetryTimer = setTimeout(() => { statsRetryTimer = null; startStatsStream(); }, 2000); }

  function renderStats(json) {
    if (!statsGrid) return;
    const uc = Array.isArray(json.upstreams) ? json.upstreams.length : 0;
    const items = [
      ['上游数量', uc, '--text'], ['并发请求', json.requests_inflight ?? 0, '--accent'], ['队列深度', json.queue_depth ?? 0, json.queue_depth > 0 ? '--bad' : '--ok'],
      ['总请求', json.requests_total ?? 0, '--text'], ['RPS', (json.rps ?? 0).toFixed(1), '--accent'], ['成功 2xx', json.responses_2xx ?? 0, '--ok'],
      ['失败 4xx', json.responses_4xx ?? 0, '--bad'], ['错误 5xx', json.responses_5xx ?? 0, '--bad'], ['网络错误', json.errors_network ?? 0, '--bad'],
      ['超时', json.errors_timeout ?? 0, '--bad'], ['平均延迟', json.latency_avg_ms != null ? json.latency_avg_ms.toFixed(1)+'ms' : '-', '--text'],
      ['运行时间', json.uptime_s != null ? formatDuration(json.uptime_s*1000) : '-', '--text'],
    ];
    statsGrid.innerHTML = items.map(([l,v,c]) => '<div class="stat-card"><div class="stat-label">'+l+'</div><div class="stat-value" style="color:var('+c+')">'+v+'</div></div>').join('');
  }

  function processSse(buf, onJson) {
    buf = buf.replace(/\r\n/g, '\n');
    let idx = buf.indexOf('\n\n');
    while (idx !== -1) {
      const raw = buf.slice(0, idx); buf = buf.slice(idx + 2);
      const dl = raw.split(/\n/).filter(l => l.startsWith('data:')).map(l => l.slice(5).trim());
      if (dl.length > 0) { try { onJson(JSON.parse(dl.join('\n'))); } catch(_) {} }
      idx = buf.indexOf('\n\n');
    }
    return buf.length > 1024*1024 ? buf.slice(-512*1024) : buf;
  }

  async function startStatsStream() {
    stopStatsStream();
    const t = getToken();
    if (!t) { if (statsGrid) statsGrid.innerHTML = '<div class="muted">请输入 Token 并点击连接。</div>'; return; }
    const ctrl = new AbortController(); statsAbort = ctrl;
    let res;
    try { res = await fetch('/admin/api/v1/stats/stream', { headers: {'X-Admin-Token':t}, signal: ctrl.signal }); } catch(e) { if (!ctrl.signal.aborted) scheduleStatsRetry(); return; }
    if (!res.ok || !res.body) return;
    const reader = res.body.getReader(), dec = new TextDecoder(); let buf = '';
    try {
      while (true) { const {value,done} = await reader.read(); if (done) break; if (value) { buf += dec.decode(value,{stream:true}); buf = processSse(buf, renderStats); } }
      buf += dec.decode(); processSse(buf, renderStats);
      if (!ctrl.signal.aborted) scheduleStatsRetry();
    } catch(e) { if (!ctrl.signal.aborted) scheduleStatsRetry(); }
  }
  function stopStatsStream() { if (statsAbort) { statsAbort.abort(); statsAbort = null; } if (statsRetryTimer) { clearTimeout(statsRetryTimer); statsRetryTimer = null; } }

  // ========== REQUESTS (Page 1) ==========
  const requestsTableBody = document.querySelector('#requestsTable tbody');
  let requestsTimer = null, chartTimer = null, requestStreamAbort = null;

  function drawRequestsChart(buckets) {
    const canvas = $('requestsChart'); if (!canvas) return;
    const ctx = canvas.getContext('2d'), dpr = window.devicePixelRatio||1;
    const w = canvas.clientWidth||600, h = 180;
    canvas.width = w*dpr; canvas.height = h*dpr; ctx.setTransform(dpr,0,0,dpr,0,0); ctx.clearRect(0,0,w,h);
    if (!buckets||!buckets.length) { ctx.fillStyle='#666'; ctx.fillText('暂无数据',10,20); return; }
    const tot = buckets.map(b=>b.total||0), suc = buckets.map(b=>b.success||0), fail = buckets.map(b=>b.failure||0);
    const max = Math.max(1,...tot), pad = 24, iw = w-pad*2, ih = h-pad*2;
    ctx.strokeStyle='#eee'; ctx.lineWidth=1;
    for (let i=0;i<=3;i++) { const y=pad+(ih*i)/3; ctx.beginPath(); ctx.moveTo(pad,y); ctx.lineTo(pad+iw,y); ctx.stroke(); }
    function line(vals,col) { ctx.beginPath(); vals.forEach((v,i)=>{const x=pad+iw*(i/(vals.length-1||1)),y=pad+ih*(1-v/max); i===0?ctx.moveTo(x,y):ctx.lineTo(x,y);}); ctx.strokeStyle=col; ctx.lineWidth=2; ctx.stroke(); }
    line(tot,'#1e6bd6'); line(suc,'#0a7'); line(fail,'#999');
  }

  async function refreshRequestsChart() {
    const sel = $('requestsWindow'); if (!sel) return;
    const {res,json} = await apiFetch('/admin/api/v1/metrics?window='+encodeURIComponent(sel.value||'minute'));
    if (!res.ok) return;
    drawRequestsChart((json&&json.buckets)||[]);
    const info = $('requestsChartInfo'); if (info) info.textContent = new Date().toLocaleTimeString();
  }

  function appendRequestRow(r, prepend) {
    const tr = document.createElement('tr'); tr.className = 'clickable';
    const s = r.status||0, sc = s>=200&&s<300?'ok':(s>=400?'bad':'muted');
    const tk = r.total_tokens!=null ? (r.prompt_tokens||0)+'/'+(r.completion_tokens||0)+'/'+r.total_tokens : '-';
    tr.innerHTML = '<td class="small">'+new Date(r.ts_ms).toLocaleTimeString()+'</td><td class="mono small">'+escapeHtml(r.client_ip||'')+'</td><td class="mono small">'+escapeHtml(r.model||'-')+'</td><td class="'+sc+'">'+s+'</td><td class="mono small">'+(r.latency_ms||0)+'ms</td><td class="mono small">'+tk+'</td><td class="mono small">'+escapeHtml(r.upstream_id||'-')+'</td>';
    if (prepend && requestsTableBody.firstChild) requestsTableBody.insertBefore(tr, requestsTableBody.firstChild);
    else requestsTableBody.appendChild(tr);
    while (requestsTableBody.children.length > 200) requestsTableBody.removeChild(requestsTableBody.lastChild);
  }

  async function refreshRequests() {
    if (!requestsTableBody) return;
    const {res,json} = await apiFetch('/admin/api/v1/requests?limit=200');
    if (!res.ok) return;
    const list = (json&&json.requests)||[];
    requestsTableBody.innerHTML = ''; list.forEach(r => appendRequestRow(r, false));
    const info = $('requestsInfo'); if (info) info.textContent = list.length + ' 条 ｜ ' + new Date().toLocaleTimeString();
  }

  function startRequestsStream() {
    stopRequestsStream();
    const t = getToken(); if (!t) return alert('请先连接');
    const ctrl = new AbortController(); requestStreamAbort = ctrl;
    const btn = $('toggleRequestsStream'); if (btn) btn.textContent = '停止实时';
    (async () => {
      try {
        const res = await fetch('/admin/api/v1/requests/stream', {headers:{'X-Admin-Token':t},signal:ctrl.signal});
        if (!res.ok||!res.body) throw new Error(String(res.status));
        const reader = res.body.getReader(), dec = new TextDecoder(); let buf = '';
        while (true) {
          const {value,done} = await reader.read(); if (done) break;
          buf += dec.decode(value,{stream:true});
          buf = processSse(buf, r => { appendRequestRow(r, true); const i = $('requestsInfo'); if (i) i.textContent = '实时 ｜ ' + new Date().toLocaleTimeString(); });
        }
      } catch(e) { const i = $('requestsInfo'); if (i&&!ctrl.signal.aborted) i.textContent = '实时流断开: '+(e.message||e); }
      finally { if (requestStreamAbort===ctrl) stopRequestsStream(); }
    })();
  }
  function stopRequestsStream() { if (requestStreamAbort) { requestStreamAbort.abort(); requestStreamAbort=null; } const b=$('toggleRequestsStream'); if (b) b.textContent='实时流'; }
  function startRequestsAutoRefresh() { stopRequestsAutoRefresh(); refreshRequestsChart(); refreshRequests(); chartTimer=setInterval(refreshRequestsChart,10000); requestsTimer=setInterval(refreshRequests,5000); }
  function stopRequestsAutoRefresh() { if(chartTimer){clearInterval(chartTimer);chartTimer=null;} if(requestsTimer){clearInterval(requestsTimer);requestsTimer=null;} }

  const refreshRequestsChartBtn = $('refreshRequestsChart');
  if (refreshRequestsChartBtn) refreshRequestsChartBtn.onclick = refreshRequestsChart;
  const rw = $('requestsWindow'); if (rw) rw.onchange = refreshRequestsChart;
  const refreshRequestsBtn = $('refreshRequests'); if (refreshRequestsBtn) refreshRequestsBtn.onclick = refreshRequests;
  const toggleBtn = $('toggleRequestsStream'); if (toggleBtn) toggleBtn.onclick = () => { requestStreamAbort ? stopRequestsStream() : startRequestsStream(); };

  // ========== UPSTREAMS (Page 2) ==========
  let lastUpstreams = [], lastModels = [], selectedUpstreamId = null;
  const upstreamsList = $('upstreamsList');
  const upstreamSelects = ['upstreamSelect','modelUpstreamSelect','upstreamManageSelect'].map(id => $(id));

  function setUpstreams(list) {
    lastUpstreams = Array.isArray(list) ? list : [];
    renderUpstreamList();
    upstreamsList && (upstreamsList.scrollTop = 0);
    upstreamsList && (upstreamsList.innerHTML = '' );
    renderUpstreamList();
    upstreamsList && (upstreamsList.scrollTop = 0);
    upstreamSelects.forEach(sel => {
      if (!sel) return;
      const cur = sel.value;
      sel.innerHTML = lastUpstreams.map(u => '<option value="'+escapeHtml(u.id)+'">'+escapeHtml(u.id)+' ('+(u.keys_total||0)+')</option>').join('');
      if (cur) sel.value = cur;
    });
  }

  function renderUpstreamList() {
    if (!upstreamsList) return;
    if (lastUpstreams.length === 0) { upstreamsList.innerHTML = '<div class="empty-state">暂无上游</div>'; return; }
    upstreamsList.innerHTML = lastUpstreams.map(u => {
      const a = u.keys_active!=null ? u.keys_active : u.keys_total, inv = u.keys_invalid||0;
      const sel = u.id === selectedUpstreamId ? ' selected' : '';
      return '<div class="upstream-card'+sel+'" data-id="'+escapeHtml(u.id)+'">' +
        '<div class="upstream-card-header"><div><strong class="mono">'+escapeHtml(u.id)+'</strong> <span class="muted small">'+escapeHtml(u.format||'openai')+'</span></div></div>' +
        '<div class="upstream-card-stats">' +
          '<div>Keys: <span class="'+(inv>0?'bad':'ok')+'">'+a+'/'+inv+'</span></div>' +
          '<div>请求: <span>'+(u.selected_total||0)+'</span></div>' +
          '<div>2xx: <span class="ok">'+(u.responses_2xx||0)+'</span></div>' +
          '<div>4xx: <span class="bad">'+(u.responses_4xx||0)+'</span></div>' +
          '<div>5xx: <span class="bad">'+(u.responses_5xx||0)+'</span></div>' +
        '</div>' +
        '<div class="muted small mono" style="margin-top:6px;">'+escapeHtml(u.base_url)+'</div>' +
      '</div>';
    }).join('');
    upstreamsList.querySelectorAll('.upstream-card').forEach(card => {
      card.onclick = () => selectUpstream(card.dataset.id);
    });
  }

  function selectUpstream(id) {
    selectedUpstreamId = id;
    const u = lastUpstreams.find(x => x.id === id);
    if (!u) return;
    $('upstreamId').value = u.id || '';
    $('upstreamBaseUrl').value = u.base_url || '';
    $('upstreamWeight').value = u.weight != null ? String(u.weight) : '';
    const fmt = $('upstreamFormat'); if (fmt) fmt.value = u.format || '';
    const prox = $('upstreamProxy'); if (prox) prox.value = u.proxy || '';
    const sel = $('upstreamManageSelect'); if (sel) sel.value = u.id;
    renderUpstreamList();
  }

  async function refreshUpstreams() {
    const {res,json} = await apiFetch('/admin/api/v1/upstreams');
    if (!res.ok) return;
    setUpstreams(json || []);
    const info = $('upstreamsInfo'); if (info) info.textContent = (json||[]).length + ' 个上游 ｜ ' + new Date().toLocaleTimeString();
  }

  const refreshUpstreamsBtn = $('refreshUpstreams'); if (refreshUpstreamsBtn) refreshUpstreamsBtn.onclick = refreshUpstreams;
  const upstreamManageSel = $('upstreamManageSelect'); if (upstreamManageSel) upstreamManageSel.onchange = () => selectUpstream(upstreamManageSel.value);

  $('addUpstream').onclick = async () => {
    const id = ($('upstreamId').value||'').trim(), url = ($('upstreamBaseUrl').value||'').trim();
    const w = $('upstreamWeight').value ? parseInt($('upstreamWeight').value,10) : null;
    const fmt = ($('upstreamFormat')&&$('upstreamFormat').value||'').trim();
    const prox = ($('upstreamProxy')&&$('upstreamProxy').value||'').trim();
    if (!id) return alert('请输入 ID'); if (!url) return alert('请输入 Base URL');
    const p = {id, base_url: url}; if (Number.isInteger(w)&&w>0) p.weight=w; if (fmt) p.format=fmt; if (prox) p.proxy=prox;
    const {res} = await apiFetch('/admin/api/v1/upstreams', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)});
    if (res.ok) { await refreshUpstreams(); $('upstreamId').value=''; $('upstreamBaseUrl').value=''; $('upstreamWeight').value=''; }
  };

  $('updateUpstream').onclick = async () => {
    const id = ($('upstreamId').value||'').trim(), url = ($('upstreamBaseUrl').value||'').trim();
    const w = $('upstreamWeight').value ? parseInt($('upstreamWeight').value,10) : null;
    const fmt = ($('upstreamFormat')&&$('upstreamFormat').value||'').trim();
    const prox = ($('upstreamProxy')&&$('upstreamProxy').value||'').trim();
    if (!id) return alert('请输入 ID'); if (!url) return alert('请输入 Base URL');
    const p = {base_url: url}; if (Number.isInteger(w)&&w>0) p.weight=w; if (fmt) p.format=fmt; if (prox) p.proxy=prox;
    const {res} = await apiFetch('/admin/api/v1/upstreams/'+encodeURIComponent(id), {method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)});
    if (res.ok) await refreshUpstreams();
  };

  $('deleteUpstream').onclick = async () => {
    const id = ($('upstreamId').value||'').trim() || ($('upstreamManageSelect').value||'');
    if (!id) return alert('请选择上游'); if (!confirm('确认删除 "'+id+'" ?')) return;
    const q = $('deleteUpstreamKeys')&&$('deleteUpstreamKeys').checked ? '?delete_keys=1' : '';
    const {res} = await apiFetch('/admin/api/v1/upstreams/'+encodeURIComponent(id)+q, {method:'DELETE'});
    if (res.ok) { $('upstreamId').value=''; $('upstreamBaseUrl').value=''; $('upstreamWeight').value=''; selectedUpstreamId=null; await refreshUpstreams(); }
  };

  // --- Models ---
  function renderModelsList(models) {
    const el = $('modelsList'); if (!el) return;
    if (!models||!models.length) { el.innerHTML = '<div class="muted small">无模型</div>'; return; }
    el.innerHTML = models.map(m => '<label style="display:block;margin:2px 0;"><input type="checkbox" value="'+escapeHtml(m)+'" checked /> <span class="mono small">'+escapeHtml(m)+'</span></label>').join('');
  }

  $('refreshModels').onclick = async () => {
    const u = $('modelUpstreamSelect').value; if (!u) return alert('请选择上游');
    const info = $('modelsInfo'); if (info) info.textContent = '拉取中...';
    const {res,json} = await apiFetch('/admin/api/v1/upstreams/'+encodeURIComponent(u)+'/models/refresh', {method:'POST'});
    if (!res.ok) { if (info) info.textContent = '失败 '+res.status; return; }
    lastModels = (json&&json.models)||[]; renderModelsList(lastModels);
    if (info) info.textContent = '发现 '+lastModels.length+' 个模型';
  };

  $('applyModels').onclick = () => {
    const u = $('modelUpstreamSelect').value; if (!u) return alert('请选择上游');
    const sel = []; $('modelsList').querySelectorAll('input:checked').forEach(cb => sel.push(cb.value));
    alert('已选择 '+sel.length+' 个模型，请手动保存路由。');
  };

  $('selectAllModels').onclick = () => { $('modelsList').querySelectorAll('input').forEach(cb => cb.checked=true); };
  $('selectNoneModels').onclick = () => { $('modelsList').querySelectorAll('input').forEach(cb => cb.checked=false); };

  // ========== KEYS (Page 3) ==========
  const keyStatusTableBody = document.querySelector('#keyStatusTable tbody');
  let keyStatusOffset = 0, keyStatusTotal = 0, lastKeyStatusList = [], keyLatencyChart = null;

  function getMode() { const els = $$('input[name="mode"]'); for (const el of els) if (el.checked) return el.value; return 'add'; }

  $('submitKeys').onclick = async () => {
    const u = $('upstreamSelect').value, mode = getMode();
    const lines = ($('keysInput').value||'').split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
    if (!u) return alert('请选择上游'); if (!lines.length) return alert('请输入密钥');
    const method = mode==='replace'?'PUT':(mode==='delete'?'DELETE':'POST');
    const {res,json} = await apiFetch('/admin/api/v1/upstreams/'+encodeURIComponent(u)+'/keys', {method,headers:{'Content-Type':'text/plain;charset=utf-8'},body:lines.join('\n')+'\n'});
    if (!res.ok) { alert('失败 '+res.status); return; }
    const a = json&&json.added!=null?json.added:lines.length, r = json&&json.removed!=null?json.removed:0;
    alert('成功: 添加 '+a+' 个'+(r>0?'，删除 '+r+' 个':'')); await refreshUpstreams();
  };

  $('clearKeys').onclick = () => { $('keysInput').value = ''; };

  $('exportKeys').onclick = () => {
    const u = $('upstreamSelect').value; if (!u) return alert('请选择上游');
    const et = prompt('请输入导出密码:'); if (!et) return;
    fetch('/admin/api/v1/upstreams/'+encodeURIComponent(u)+'/keys/export', {headers:{'X-Admin-Token':getToken(),'X-Export-Token':et}})
      .then(r => { if (!r.ok) throw new Error(r.statusText); return r.blob(); })
      .then(b => { const a=document.createElement('a'); a.href=URL.createObjectURL(b); a.download=u+'_keys_'+Math.floor(Date.now()/1000)+'.txt'; a.click(); URL.revokeObjectURL(a.href); })
      .catch(e => alert('导出失败: '+e.message));
  };

  function renderKeyStatus(list, offset) {
    lastKeyStatusList = Array.isArray(list) ? list.slice() : [];
    const q = ($('keySearch')&&$('keySearch').value||'').trim().toLowerCase();
    const filter = ($('keyFilter')&&$('keyFilter').value)||'all';
    const sort = ($('keySort')&&$('keySort').value)||'index';
    list = lastKeyStatusList.filter(k => {
      if (q && !(k.key||'').toLowerCase().includes(q)) return false;
      if (filter==='active'&&k.status!=='active') return false;
      if (filter==='invalid'&&k.status!=='invalid') return false;
      return true;
    });
    if (sort==='failure_desc') list.sort((a,b)=>(b.failure_count||0)-(a.failure_count||0));
    else if (sort==='latency_desc') list.sort((a,b)=>(b.latency_p90_ms||0)-(a.latency_p90_ms||0));
    if (!keyStatusTableBody) return;
    keyStatusTableBody.innerHTML = '';
    list.forEach((k,i) => {
      const tr = document.createElement('tr');
      const inv = k.status==='invalid';
      const badge = inv ? '<span class="badge badge-bad">invalid</span>' : '<span class="badge badge-ok">active</span>';
      const lat = k.latency_p50_ms!=null ? k.latency_p50_ms+'/'+(k.latency_p90_ms||0)+'/'+(k.latency_p99_ms||0)+'ms' : '-';
      tr.innerHTML = '<td class="mono small">'+(offset+i+1)+'</td><td class="mono small key-mask" title="'+escapeHtml(k.key)+'">'+escapeHtml(maskKey(k.key))+'</td><td>'+badge+'</td><td class="mono small">'+(k.failure_count||0)+'</td><td class="mono small">'+(k.active_requests||0)+'</td><td class="mono small">'+lat+'</td>';
      const td = document.createElement('td');
      [['恢复',() => keyAction('release',[k.key])],['失效',() => keyAction('invalidate',[k.key])],['测试',() => testKey(k.key)]].forEach(([txt,fn]) => {
        const b = document.createElement('button'); b.className='btn'; b.textContent=txt; b.onclick=fn; td.appendChild(b);
      });
      tr.appendChild(td); keyStatusTableBody.appendChild(tr);
    });
    renderKeyLatencyChart(list);
  }

  async function loadKeyStatus() {
    const u = $('keyStatusUpstreamSelect').value; if (!u) return alert('请选择上游');
    const limit = parseInt($('keyStatusPageSize').value,10)||100;
    if (keyStatusOffset<0) keyStatusOffset=0;
    const {res,json} = await apiFetch('/admin/api/v1/upstreams/'+encodeURIComponent(u)+'/keys?offset='+keyStatusOffset+'&limit='+limit);
    if (!res.ok) return;
    keyStatusTotal = (json&&json.total)||0;
    if (json&&Number.isInteger(json.offset)) keyStatusOffset=json.offset;
    if (keyStatusOffset>=keyStatusTotal&&keyStatusTotal>0) { const n=Math.max(0,Math.floor((keyStatusTotal-1)/limit)*limit); if(n!==keyStatusOffset){keyStatusOffset=n;return loadKeyStatus();} }
    renderKeyStatus((json&&json.keys)||[], keyStatusOffset);
    const end = keyStatusOffset + ((json&&json.keys)||[]).length;
    const info = $('keyStatusInfo'); if (info) info.textContent = '共 '+keyStatusTotal+' 个密钥';
    const pi = $('keyStatusPageInfo'); if (pi) pi.textContent = keyStatusTotal>0 ? (keyStatusOffset+1)+'-'+end+' / '+keyStatusTotal : '0 / 0';
  }

  async function postKeyAction(action, body) {
    const u = $('keyStatusUpstreamSelect').value; if (!u) return alert('请选择上游');
    const {res} = await apiFetch('/admin/api/v1/upstreams/'+encodeURIComponent(u)+'/keys/'+action, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if (res.ok) { await loadKeyStatus(); await refreshUpstreams(); }
  }
  async function keyAction(action, keys) { await postKeyAction(action, {keys}); }
  async function testKey(key) {
    const u = $('keyStatusUpstreamSelect').value; if (!u) return alert('请选择上游');
    const {res,json} = await apiFetch('/admin/api/v1/upstreams/'+encodeURIComponent(u)+'/keys/test', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key})});
    if (res.ok&&json) alert(json.ok?'密钥有效':'密钥无效: '+(json.error||'')); else alert('测试失败');
  }

  function renderKeyLatencyChart(list) {
    const canvas = $('keyLatencyChart'); if (!canvas||typeof Chart==='undefined') return;
    const rows = (list||[]).filter(k=>k.latency_p90_ms!=null).slice(0,25);
    const labels = rows.map(k=>maskKey(k.key)), data = rows.map(k=>k.latency_p90_ms||0);
    if (keyLatencyChart) keyLatencyChart.destroy();
    keyLatencyChart = new Chart(canvas, {type:'bar',data:{labels,datasets:[{label:'p90 ms',data,backgroundColor:'#1e6bd6'}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true}}}});
  }

  // Key status events
  const loadKeyStatusBtn = $('loadKeyStatus'); if (loadKeyStatusBtn) loadKeyStatusBtn.onclick = () => { keyStatusOffset=0; loadKeyStatus(); };
  const keySearchInput = $('keySearch'); if (keySearchInput) keySearchInput.oninput = () => renderKeyStatus(lastKeyStatusList, keyStatusOffset);
  const keyFilterSel = $('keyFilter'); if (keyFilterSel) keyFilterSel.onchange = () => renderKeyStatus(lastKeyStatusList, keyStatusOffset);
  const keySortSel = $('keySort'); if (keySortSel) keySortSel.onchange = () => renderKeyStatus(lastKeyStatusList, keyStatusOffset);
  const keyStatusUpSel = $('keyStatusUpstreamSelect'); if (keyStatusUpSel) keyStatusUpSel.onchange = () => { keyStatusOffset=0; loadKeyStatus(); };
  const keyPageSizeSel = $('keyStatusPageSize'); if (keyPageSizeSel) keyPageSizeSel.onchange = () => { keyStatusOffset=0; loadKeyStatus(); };
  const releaseAllBtn = $('releaseAllKeys'); if (releaseAllBtn) releaseAllBtn.onclick = () => { const u=$('keyStatusUpstreamSelect').value; if(!u)return alert('请选择上游'); if(!confirm('确认恢复全部?'))return; postKeyAction('release',{all:true}); };
  const banAllBtn = $('banAllKeys'); if (banAllBtn) banAllBtn.onclick = () => { const u=$('keyStatusUpstreamSelect').value; if(!u)return alert('请选择上游'); if(!confirm('确认失效全部?'))return; postKeyAction('invalidate',{all:true}); };
  const prevBtn = $('keyStatusPrev'); if (prevBtn) prevBtn.onclick = () => { const l=parseInt($('keyStatusPageSize').value,10)||100; keyStatusOffset=Math.max(0,keyStatusOffset-l); loadKeyStatus(); };
  const nextBtn = $('keyStatusNext'); if (nextBtn) nextBtn.onclick = () => { const l=parseInt($('keyStatusPageSize').value,10)||100; if(keyStatusOffset+l<keyStatusTotal){keyStatusOffset+=l;loadKeyStatus();} };

  // ========== INIT ==========
  if (getToken()) { startStatsStream(); refreshUpstreams(); startRequestsAutoRefresh(); }
  else { if (statsGrid) statsGrid.innerHTML = '<div class="muted">请输入 Token 并点击连接。</div>'; }
})();
