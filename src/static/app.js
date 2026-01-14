(function () {
  const tokenInput = document.getElementById('token');
  const saveTokenBtn = document.getElementById('saveToken');
  const clearTokenBtn = document.getElementById('clearToken');
  const authStatus = document.getElementById('authStatus');

  const statsPre = document.getElementById('stats');

  const requestsWindowSelect = document.getElementById('requestsWindow');
  const refreshRequestsChartBtn = document.getElementById('refreshRequestsChart');
  const requestsChartInfo = document.getElementById('requestsChartInfo');
  const requestsChart = document.getElementById('requestsChart');

  const refreshRequestsBtn = document.getElementById('refreshRequests');
  const requestsInfo = document.getElementById('requestsInfo');
  const requestsTableBody = document.querySelector('#requestsTable tbody');

  const refreshUpstreamsBtn = document.getElementById('refreshUpstreams');
  const upstreamsInfo = document.getElementById('upstreamsInfo');
  const upstreamsTableBody = document.querySelector('#upstreamsTable tbody');
  const upstreamSelect = document.getElementById('upstreamSelect');
  const modelUpstreamSelect = document.getElementById('modelUpstreamSelect');
  const upstreamManageSelect = document.getElementById('upstreamManageSelect');
  const upstreamIdInput = document.getElementById('upstreamId');
  const upstreamBaseUrlInput = document.getElementById('upstreamBaseUrl');
  const upstreamWeightInput = document.getElementById('upstreamWeight');
  const addUpstreamBtn = document.getElementById('addUpstream');
  const updateUpstreamBtn = document.getElementById('updateUpstream');
  const deleteUpstreamBtn = document.getElementById('deleteUpstream');
  const deleteUpstreamKeys = document.getElementById('deleteUpstreamKeys');
  const upstreamResult = document.getElementById('upstreamResult');

  const keysInput = document.getElementById('keysInput');
  const submitKeysBtn = document.getElementById('submitKeys');
  const clearKeysBtn = document.getElementById('clearKeys');
  const keysResult = document.getElementById('keysResult');

  const billingKeyInput = document.getElementById('billingKey');
  const billingBalanceInput = document.getElementById('billingBalance');
  const billingDeltaInput = document.getElementById('billingDelta');
  const billingCreateBtn = document.getElementById('billingCreate');
  const billingQueryBtn = document.getElementById('billingQuery');
  const billingAdjustBtn = document.getElementById('billingAdjust');
  const billingGenerateBtn = document.getElementById('billingGenerate');
  const billingResult = document.getElementById('billingResult');

  const loadRoutesBtn = document.getElementById('loadRoutes');
  const saveRoutesBtn = document.getElementById('saveRoutes');
  const routesInput = document.getElementById('routesInput');
  const routesResult = document.getElementById('routesResult');
  const routesInfo = document.getElementById('routesInfo');

  const refreshModelsBtn = document.getElementById('refreshModels');
  const applyModelsBtn = document.getElementById('applyModels');
  const selectAllModelsBtn = document.getElementById('selectAllModels');
  const selectNoneModelsBtn = document.getElementById('selectNoneModels');
  const modelsInfo = document.getElementById('modelsInfo');
  const modelsList = document.getElementById('modelsList');
  let lastModels = [];
  let lastUpstreams = [];
  let requestsTimer = null;
  let chartTimer = null;

  function getToken() {
    return localStorage.getItem('gptload_admin_token') || '';
  }
  function setToken(t) {
    localStorage.setItem('gptload_admin_token', t);
  }
  function clearToken() {
    localStorage.removeItem('gptload_admin_token');
  }

  tokenInput.value = getToken();

  saveTokenBtn.onclick = () => {
    setToken(tokenInput.value.trim());
    authStatus.textContent = 'Token 已保存。';
    startStatsStream();
    refreshUpstreams();
    loadRoutes();
    startRequestsAutoRefresh();
  };
  clearTokenBtn.onclick = () => {
    clearToken();
    tokenInput.value = '';
    authStatus.textContent = 'Token 已清除。';
    stopStatsStream();
    refreshUpstreams();
    stopRequestsAutoRefresh();
  };

  async function apiFetch(path, opts) {
    opts = opts || {};
    opts.headers = opts.headers || {};
    const t = getToken();
    let url = path;
    if (t) {
      opts.headers['X-Admin-Token'] = t;
      const sep = path.includes('?') ? '&' : '?';
      url = `${path}${sep}token=${encodeURIComponent(t)}`;
    }
    const res = await fetch(url, opts);
    const text = await res.text();
    let json = null;
    try { json = JSON.parse(text); } catch (_) {}
    return { res, text, json };
  }

  function setUpstreams(list) {
    lastUpstreams = Array.isArray(list) ? list : [];
    // Table
    upstreamsTableBody.innerHTML = '';
    for (const u of lastUpstreams) {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td class="mono">${escapeHtml(u.id)}</td>
        <td class="mono small">${escapeHtml(u.base_url)}</td>
        <td>${u.weight}</td>
        <td>${u.keys_total}</td>
        <td class="mono small">${u.upstream_cooldown_until_ms || 0}</td>
        <td class="mono small">${u.selected_total || 0}</td>
        <td class="mono small">${u.responses_2xx || 0}</td>
        <td class="mono small">${u.responses_4xx || 0}</td>
        <td class="mono small">${u.responses_5xx || 0}</td>
        <td class="mono small">${u.errors_network || 0}</td>
        <td class="mono small">${u.errors_timeout || 0}</td>
      `;
      upstreamsTableBody.appendChild(tr);
    }

    // Select
    const current = upstreamSelect.value;
    upstreamSelect.innerHTML = '';
    for (const u of lastUpstreams) {
      const opt = document.createElement('option');
      opt.value = u.id;
      opt.textContent = `${u.id} (keys=${u.keys_total})`;
      upstreamSelect.appendChild(opt);
    }
    if (current) upstreamSelect.value = current;

    const currentModel = modelUpstreamSelect.value;
    modelUpstreamSelect.innerHTML = '';
    for (const u of lastUpstreams) {
      const opt = document.createElement('option');
      opt.value = u.id;
      opt.textContent = `${u.id}`;
      modelUpstreamSelect.appendChild(opt);
    }
    if (currentModel) modelUpstreamSelect.value = currentModel;

    const currentManage = upstreamManageSelect.value;
    upstreamManageSelect.innerHTML = '';
    for (const u of lastUpstreams) {
      const opt = document.createElement('option');
      opt.value = u.id;
      opt.textContent = `${u.id}`;
      upstreamManageSelect.appendChild(opt);
    }
    if (currentManage) upstreamManageSelect.value = currentManage;
  }

  async function refreshUpstreams() {
    const { res, json, text } = await apiFetch('/admin/api/v1/upstreams');
    if (!res.ok) {
      upstreamsInfo.textContent = `拉取 upstreams 失败: ${res.status}`;
      if (json && json.error) upstreamsInfo.textContent += ` ${json.error}`;
      return;
    }
    setUpstreams(json || []);
    upstreamsInfo.textContent = `upstreams=${(json||[]).length} ｜ ${new Date().toLocaleTimeString()}`;
  }

  refreshUpstreamsBtn.onclick = refreshUpstreams;

  function drawRequestsChart(buckets) {
    if (!requestsChart) return;
    const ctx = requestsChart.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const width = requestsChart.clientWidth || 600;
    const height = 180;
    requestsChart.width = width * dpr;
    requestsChart.height = height * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);

    if (!buckets || buckets.length === 0) {
      ctx.fillStyle = '#666';
      ctx.fillText('暂无数据', 10, 20);
      return;
    }

    const totals = buckets.map(b => b.total || 0);
    const successes = buckets.map(b => b.success || 0);
    const failures = buckets.map(b => b.failure || 0);
    const maxVal = Math.max(1, ...totals);
    const pad = 24;
    const innerW = width - pad * 2;
    const innerH = height - pad * 2;

    ctx.strokeStyle = '#eee';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 3; i++) {
      const y = pad + (innerH * i) / 3;
      ctx.beginPath();
      ctx.moveTo(pad, y);
      ctx.lineTo(pad + innerW, y);
      ctx.stroke();
    }

    function drawLine(values, color) {
      ctx.beginPath();
      values.forEach((v, i) => {
        const x = pad + innerW * (i / (values.length - 1 || 1));
        const y = pad + innerH * (1 - v / maxVal);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      ctx.stroke();
    }

    drawLine(totals, '#1e6bd6');
    drawLine(successes, '#0a7');
    drawLine(failures, '#999');
  }

  async function refreshRequestsChart() {
    if (!requestsWindowSelect) return;
    const windowKey = requestsWindowSelect.value || 'minute';
    const { res, json, text } = await apiFetch(`/admin/api/v1/metrics?window=${encodeURIComponent(windowKey)}`);
    if (!res.ok) {
      requestsChartInfo.textContent = `失败 ${res.status}`;
      return;
    }
    const buckets = (json && json.buckets) || [];
    drawRequestsChart(buckets);
    requestsChartInfo.textContent = `bucket=${buckets.length} ｜ ${new Date().toLocaleTimeString()}`;
  }

  async function refreshRequests() {
    if (!requestsTableBody) return;
    const { res, json, text } = await apiFetch('/admin/api/v1/requests?limit=200');
    if (!res.ok) {
      requestsInfo.textContent = `失败 ${res.status}`;
      return;
    }
    const list = (json && json.requests) || [];
    requestsTableBody.innerHTML = '';
    for (const r of list) {
      const tr = document.createElement('tr');
      const status = r.status || 0;
      const statusClass = status >= 200 && status < 300 ? 'ok' : (status === 404 ? 'muted' : 'bad');
      const tokens = r.total_tokens != null
        ? `${r.prompt_tokens || 0}/${r.completion_tokens || 0}/${r.total_tokens}`
        : '-';
      const bytes = `${r.req_bytes || 0}/${r.resp_bytes || 0}`;
      tr.innerHTML = `
        <td class="small">${new Date(r.ts_ms).toLocaleTimeString()}</td>
        <td class="mono small">${escapeHtml(r.client_ip || '')}</td>
        <td class="mono small">${escapeHtml(r.model || '-')}</td>
        <td class="${statusClass}">${status}</td>
        <td class="mono small">${r.latency_ms || 0}</td>
        <td class="mono small">${tokens}</td>
        <td class="mono small">${bytes}</td>
        <td class="mono small">${escapeHtml(r.upstream_id || '-')}</td>
      `;
      requestsTableBody.appendChild(tr);
    }
    requestsInfo.textContent = `count=${list.length} ｜ ${new Date().toLocaleTimeString()}`;
  }

  if (refreshRequestsChartBtn) refreshRequestsChartBtn.onclick = refreshRequestsChart;
  if (requestsWindowSelect) requestsWindowSelect.onchange = refreshRequestsChart;
  if (refreshRequestsBtn) refreshRequestsBtn.onclick = refreshRequests;

  function getMode() {
    const els = document.querySelectorAll('input[name="mode"]');
    for (const el of els) if (el.checked) return el.value;
    return 'add';
  }

  submitKeysBtn.onclick = async () => {
    const upstream = upstreamSelect.value;
    const mode = getMode();
    const text = keysInput.value || '';
    const lines = text.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
    if (!upstream) return alert('请选择 upstream');
    if (lines.length === 0) return alert('请输入 keys');

    const method = mode === 'replace' ? 'PUT' : (mode === 'delete' ? 'DELETE' : 'POST');
    keysResult.textContent = '提交中...';
    const { res, json, text: raw } = await apiFetch(`/admin/api/v1/upstreams/${encodeURIComponent(upstream)}/keys`, {
      method,
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
      body: lines.join('\n') + '\n'
    });

    if (!res.ok) {
      keysResult.textContent = `失败 ${res.status}\n` + (raw || '');
      return;
    }
    keysResult.textContent = JSON.stringify(json || raw, null, 2);
    await refreshUpstreams();
  };

  clearKeysBtn.onclick = () => {
    keysInput.value = '';
  };

  function generateBillingKey() {
    const bytes = new Uint8Array(12);
    crypto.getRandomValues(bytes);
    let hex = '';
    for (const b of bytes) {
      hex += b.toString(16).padStart(2, '0');
    }
    return `bk-${hex}`;
  }

  if (billingGenerateBtn) {
    billingGenerateBtn.onclick = () => {
      if (billingKeyInput) billingKeyInput.value = generateBillingKey();
    };
  }

  if (billingCreateBtn) {
    billingCreateBtn.onclick = async () => {
      const key = (billingKeyInput.value || '').trim();
      const balanceRaw = (billingBalanceInput.value || '').trim();
      const balance = balanceRaw ? parseInt(balanceRaw, 10) : 0;
      if (!key) return alert('请输入 key');
      if (!Number.isFinite(balance)) return alert('余额格式错误');
      billingResult.textContent = '提交中...';
      const payload = { key, balance };
      const { res, json, text } = await apiFetch('/admin/api/v1/billing/keys', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!res.ok) {
        billingResult.textContent = `失败 ${res.status}\n${text || ''}`;
        return;
      }
      billingResult.textContent = JSON.stringify(json || {}, null, 2);
    };
  }

  if (billingQueryBtn) {
    billingQueryBtn.onclick = async () => {
      const key = (billingKeyInput.value || '').trim();
      if (!key) return alert('请输入 key');
      billingResult.textContent = '查询中...';
      const { res, json, text } = await apiFetch(`/admin/api/v1/billing/keys/${encodeURIComponent(key)}`);
      if (!res.ok) {
        billingResult.textContent = `失败 ${res.status}\n${text || ''}`;
        return;
      }
      billingResult.textContent = JSON.stringify(json || {}, null, 2);
    };
  }

  if (billingAdjustBtn) {
    billingAdjustBtn.onclick = async () => {
      const key = (billingKeyInput.value || '').trim();
      const deltaRaw = (billingDeltaInput.value || '').trim();
      const delta = deltaRaw ? parseInt(deltaRaw, 10) : NaN;
      if (!key) return alert('请输入 key');
      if (!Number.isFinite(delta)) return alert('请输入 delta');
      billingResult.textContent = '提交中...';
      const payload = { delta };
      const { res, json, text } = await apiFetch(`/admin/api/v1/billing/keys/${encodeURIComponent(key)}/adjust`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!res.ok) {
        billingResult.textContent = `失败 ${res.status}\n${text || ''}`;
        return;
      }
      billingResult.textContent = JSON.stringify(json || {}, null, 2);
    };
  }

  function fillUpstreamForm(id) {
    const u = lastUpstreams.find(x => x.id === id);
    if (!u) return;
    upstreamIdInput.value = u.id || '';
    upstreamBaseUrlInput.value = u.base_url || '';
    upstreamWeightInput.value = u.weight != null ? String(u.weight) : '';
  }

  upstreamManageSelect.onchange = () => {
    fillUpstreamForm(upstreamManageSelect.value);
  };

  addUpstreamBtn.onclick = async () => {
    const id = (upstreamIdInput.value || '').trim();
    const baseUrl = (upstreamBaseUrlInput.value || '').trim();
    const weightRaw = (upstreamWeightInput.value || '').trim();
    const weight = weightRaw ? parseInt(weightRaw, 10) : null;
    if (!id) return alert('请输入 upstream id');
    if (!baseUrl) return alert('请输入 base_url');
    upstreamResult.textContent = '提交中...';
    const payload = { id, base_url: baseUrl };
    if (Number.isInteger(weight) && weight > 0) payload.weight = weight;
    const { res, text } = await apiFetch('/admin/api/v1/upstreams', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!res.ok) {
      upstreamResult.textContent = `失败 ${res.status}\n${text || ''}`;
      return;
    }
    upstreamResult.textContent = '已新增。';
    await refreshUpstreams();
    await loadRoutes();
  };

  updateUpstreamBtn.onclick = async () => {
    const id = (upstreamIdInput.value || '').trim();
    const baseUrl = (upstreamBaseUrlInput.value || '').trim();
    const weightRaw = (upstreamWeightInput.value || '').trim();
    const weight = weightRaw ? parseInt(weightRaw, 10) : null;
    if (!id) return alert('请输入 upstream id');
    if (!baseUrl) return alert('请输入 base_url');
    upstreamResult.textContent = '提交中...';
    const payload = { base_url: baseUrl };
    if (Number.isInteger(weight) && weight > 0) payload.weight = weight;
    const { res, text } = await apiFetch(`/admin/api/v1/upstreams/${encodeURIComponent(id)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!res.ok) {
      upstreamResult.textContent = `失败 ${res.status}\n${text || ''}`;
      return;
    }
    upstreamResult.textContent = '已更新。';
    await refreshUpstreams();
    await loadRoutes();
  };

  deleteUpstreamBtn.onclick = async () => {
    const id = (upstreamIdInput.value || '').trim() || upstreamManageSelect.value;
    if (!id) return alert('请选择 upstream');
    if (!confirm(`确认删除 upstream "${id}"?`)) return;
    upstreamResult.textContent = '提交中...';
    const query = deleteUpstreamKeys.checked ? '?delete_keys=1' : '';
    const { res, text } = await apiFetch(`/admin/api/v1/upstreams/${encodeURIComponent(id)}${query}`, {
      method: 'DELETE'
    });
    if (!res.ok) {
      upstreamResult.textContent = `失败 ${res.status}\n${text || ''}`;
      return;
    }
    upstreamResult.textContent = '已删除。';
    upstreamIdInput.value = '';
    upstreamBaseUrlInput.value = '';
    upstreamWeightInput.value = '';
    await refreshUpstreams();
    await loadRoutes();
  };

  async function loadRoutes() {
    routesResult.textContent = '加载中...';
    const { res, json, text } = await apiFetch('/admin/api/v1/models/routes');
    if (!res.ok) {
      routesResult.textContent = `失败 ${res.status}\n${text || ''}`;
      return;
    }
    routesInput.value = JSON.stringify(json || {}, null, 2);
    routesResult.textContent = '';
    if (json && json.updated_at_ms) {
      routesInfo.textContent = `更新时间 ${new Date(json.updated_at_ms).toLocaleString()}`;
    } else {
      routesInfo.textContent = `更新时间 ${new Date().toLocaleTimeString()}`;
    }
  }

  function parseRoutesInput() {
    const raw = (routesInput.value || '').trim();
    if (!raw) return { upstreams: {} };
    let obj;
    try {
      obj = JSON.parse(raw);
    } catch (e) {
      throw new Error('routes json 解析失败');
    }
    if (obj && typeof obj === 'object' && obj.upstreams) {
      return { upstreams: obj.upstreams };
    }
    if (obj && typeof obj === 'object') {
      return { upstreams: obj };
    }
    throw new Error('routes json 格式不正确');
  }

  function rebuildRoutesObject(upstreams) {
    const models = {};
    for (const [id, list] of Object.entries(upstreams || {})) {
      for (const model of list || []) {
        if (!models[model]) models[model] = [];
        models[model].push(id);
      }
    }
    for (const k of Object.keys(models)) {
      models[k] = Array.from(new Set(models[k])).sort();
    }
    for (const k of Object.keys(upstreams || {})) {
      upstreams[k] = Array.from(new Set(upstreams[k] || [])).sort();
    }
    return { updated_at_ms: Date.now(), models, upstreams };
  }

  async function saveRoutes() {
    routesResult.textContent = '保存中...';
    let payload;
    try {
      payload = parseRoutesInput();
    } catch (e) {
      routesResult.textContent = e.message;
      return;
    }
    const { res, json, text } = await apiFetch('/admin/api/v1/models/routes', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!res.ok) {
      routesResult.textContent = `失败 ${res.status}\n${text || ''}`;
      return;
    }
    routesInput.value = JSON.stringify(json || {}, null, 2);
    routesResult.textContent = '已保存。';
    if (json && json.updated_at_ms) {
      routesInfo.textContent = `更新时间 ${new Date(json.updated_at_ms).toLocaleString()}`;
    }
  }

  function renderModelsList(models) {
    modelsList.innerHTML = '';
    if (!models || models.length === 0) {
      modelsList.textContent = '无模型。';
      return;
    }
    for (const m of models) {
      const label = document.createElement('label');
      label.style.display = 'block';
      label.style.margin = '2px 0';
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.value = m;
      cb.checked = true;
      label.appendChild(cb);
      const span = document.createElement('span');
      span.textContent = ` ${m}`;
      label.appendChild(span);
      modelsList.appendChild(label);
    }
  }

  async function refreshModels() {
    const upstream = modelUpstreamSelect.value;
    if (!upstream) return alert('请选择 upstream');
    modelsInfo.textContent = '拉取中...';
    const { res, json, text } = await apiFetch(`/admin/api/v1/upstreams/${encodeURIComponent(upstream)}/models/refresh`, {
      method: 'POST'
    });
    if (!res.ok) {
      modelsInfo.textContent = `失败 ${res.status}`;
      routesResult.textContent = text || '';
      return;
    }
    lastModels = (json && json.models) || [];
    renderModelsList(lastModels);
    modelsInfo.textContent = `models=${lastModels.length}`;
  }

  function applyModelsToRoutes() {
    const upstream = modelUpstreamSelect.value;
    if (!upstream) return alert('请选择 upstream');
    const selected = [];
    const cbs = modelsList.querySelectorAll('input[type="checkbox"]');
    for (const cb of cbs) {
      if (cb.checked) selected.push(cb.value);
    }
    let payload;
    try {
      payload = parseRoutesInput();
    } catch (e) {
      routesResult.textContent = e.message;
      return;
    }
    if (!payload.upstreams) payload.upstreams = {};
    payload.upstreams[upstream] = selected;
    const routesObj = rebuildRoutesObject(payload.upstreams);
    routesInput.value = JSON.stringify(routesObj, null, 2);
    routesResult.textContent = '已应用到路由，点击保存生效。';
  }

  loadRoutesBtn.onclick = loadRoutes;
  saveRoutesBtn.onclick = saveRoutes;
  refreshModelsBtn.onclick = refreshModels;
  applyModelsBtn.onclick = applyModelsToRoutes;
  selectAllModelsBtn.onclick = () => {
    const cbs = modelsList.querySelectorAll('input[type="checkbox"]');
    for (const cb of cbs) cb.checked = true;
  };
  selectNoneModelsBtn.onclick = () => {
    const cbs = modelsList.querySelectorAll('input[type="checkbox"]');
    for (const cb of cbs) cb.checked = false;
  };

  function escapeHtml(s) {
    return (s || '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));
  }

  // Stats stream
  let es = null;

  function startStatsStream() {
    stopStatsStream();
    const t = getToken();
    if (!t) {
      statsPre.textContent = '未设置 token。';
      return;
    }

    es = new EventSource(`/admin/api/v1/stats/stream?token=${encodeURIComponent(t)}`);
    es.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data);
        statsPre.textContent = JSON.stringify(data, null, 2);
      } catch (e) {
        statsPre.textContent = ev.data;
      }
    };
    es.onerror = () => {
      // EventSource 会自动重连；这里轻量提示即可。
      authStatus.textContent = 'Stats stream 连接异常（将自动重连）。';
    };
    authStatus.textContent = 'Stats stream 已连接。';
  }

  function stopStatsStream() {
    if (es) {
      es.close();
      es = null;
    }
  }

  function startRequestsAutoRefresh() {
    stopRequestsAutoRefresh();
    refreshRequestsChart();
    refreshRequests();
    chartTimer = setInterval(refreshRequestsChart, 10000);
    requestsTimer = setInterval(refreshRequests, 5000);
  }

  function stopRequestsAutoRefresh() {
    if (chartTimer) {
      clearInterval(chartTimer);
      chartTimer = null;
    }
    if (requestsTimer) {
      clearInterval(requestsTimer);
      requestsTimer = null;
    }
  }

  // Init
  if (getToken()) {
    authStatus.textContent = '已加载本地 token。';
    startStatsStream();
    refreshUpstreams();
    loadRoutes();
    startRequestsAutoRefresh();
  } else {
    authStatus.textContent = '请输入并保存 admin token。';
    statsPre.textContent = '未连接。';
  }
})();
