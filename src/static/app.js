(function () {
  const themeToggle = document.getElementById('themeToggle');
  const tokenInput = document.getElementById('token');
  const saveTokenBtn = document.getElementById('saveToken');
  const clearTokenBtn = document.getElementById('clearToken');
  const authStatus = document.getElementById('authStatus');

  const statsPre = document.getElementById('stats');
  const statsGrid = document.getElementById('statsGrid');

  const requestsWindowSelect = document.getElementById('requestsWindow');
  const refreshRequestsChartBtn = document.getElementById('refreshRequestsChart');
  const requestsChartInfo = document.getElementById('requestsChartInfo');
  const requestsChart = document.getElementById('requestsChart');

  const refreshRequestsBtn = document.getElementById('refreshRequests');
  const toggleRequestsStreamBtn = document.getElementById('toggleRequestsStream');
  const requestsInfo = document.getElementById('requestsInfo');
  const requestsTableBody = document.querySelector('#requestsTable tbody');
  const requestDetail = document.getElementById('requestDetail');

  const refreshUpstreamsBtn = document.getElementById('refreshUpstreams');
  const upstreamsInfo = document.getElementById('upstreamsInfo');
  const upstreamsTableBody = document.querySelector('#upstreamsTable tbody');
  const upstreamSelect = document.getElementById('upstreamSelect');
  const modelUpstreamSelect = document.getElementById('modelUpstreamSelect');
  const upstreamManageSelect = document.getElementById('upstreamManageSelect');
  const upstreamIdInput = document.getElementById('upstreamId');
  const upstreamBaseUrlInput = document.getElementById('upstreamBaseUrl');
  const upstreamWeightInput = document.getElementById('upstreamWeight');
  const upstreamFormatInput = document.getElementById('upstreamFormat');
  const upstreamProxyInput = document.getElementById('upstreamProxy');
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

  const keyStatusUpstreamSelect = document.getElementById('keyStatusUpstreamSelect');
  const keySearchInput = document.getElementById('keySearch');
  const keyFilterSelect = document.getElementById('keyFilter');
  const keySortSelect = document.getElementById('keySort');
  const loadKeyStatusBtn = document.getElementById('loadKeyStatus');
  const releaseAllKeysBtn = document.getElementById('releaseAllKeys');
  const banAllKeysBtn = document.getElementById('banAllKeys');
  const exportKeysBtn = document.getElementById('exportKeys');
  const keyStatusInfo = document.getElementById('keyStatusInfo');
  const keyStatusPrevBtn = document.getElementById('keyStatusPrev');
  const keyStatusNextBtn = document.getElementById('keyStatusNext');
  const keyStatusPageSize = document.getElementById('keyStatusPageSize');
  const keyStatusPageInfo = document.getElementById('keyStatusPageInfo');
  const keyStatusTableBody = document.querySelector('#keyStatusTable tbody');
  const keyStatusResult = document.getElementById('keyStatusResult');
  const keyLatencyChartCanvas = document.getElementById('keyLatencyChart');

  const loadConfigBtn = document.getElementById('loadConfig');
  const configInfo = document.getElementById('configInfo');
  const configPreview = document.getElementById('configPreview');

  let lastModels = [];
  let lastUpstreams = [];
  let requestsTimer = null;
  let chartTimer = null;
  let keyStatusOffset = 0;
  let keyStatusTotal = 0;
  let lastKeyStatusList = [];
  let keyLatencyChart = null;
  let requestStreamAbort = null;

  function getToken() {
    return localStorage.getItem('gptload_admin_token') || '';
  }
  function setToken(t) {
    localStorage.setItem('gptload_admin_token', t);
  }
  function clearToken() {
    localStorage.removeItem('gptload_admin_token');
  }

  function applyTheme(theme) {
    document.body.classList.remove('dark', 'light');
    if (theme === 'dark' || theme === 'light') {
      document.body.classList.add(theme);
    }
    if (themeToggle) {
      themeToggle.textContent = theme === 'dark' ? '浅色' : '深色';
    }
  }

  function initTheme() {
    const saved = localStorage.getItem('gptload_theme') || '';
    applyTheme(saved);
  }

  if (themeToggle) {
    themeToggle.onclick = () => {
      const next = document.body.classList.contains('dark') ? 'light' : 'dark';
      localStorage.setItem('gptload_theme', next);
      applyTheme(next);
      refreshRequestsChart();
      renderKeyLatencyChart(lastKeyStatusList);
    };
  }

  initTheme();
  tokenInput.value = getToken();

  saveTokenBtn.onclick = () => {
    setToken(tokenInput.value.trim());
    authStatus.textContent = 'Token 已保存。';
    startStatsStream();
    refreshUpstreams();
    loadRoutes();
    loadConfig();
    startRequestsAutoRefresh();
  };
  clearTokenBtn.onclick = () => {
    clearToken();
    tokenInput.value = '';
    authStatus.textContent = 'Token 已清除。';
    stopStatsStream();
    refreshUpstreams();
    stopRequestsAutoRefresh();
    stopRequestsStream();
  };

  async function apiFetch(path, opts) {
    opts = opts || {};
    opts.headers = opts.headers || {};
    const t = getToken();
    if (t) {
      opts.headers['X-Admin-Token'] = t;
    }
    const res = await fetch(path, opts);
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
      const active = u.keys_active != null ? u.keys_active : u.keys_total;
      const invalid = u.keys_invalid || 0;
      const keysClass = invalid > 0 ? 'bad' : 'ok';
      tr.innerHTML = `
        <td class="mono">${escapeHtml(u.id)}</td>
        <td class="mono small">${escapeHtml(u.base_url)}</td>
        <td class="mono small">${escapeHtml(u.format || 'openai')}</td>
        <td class="mono small">${escapeHtml(u.proxy || '-')}</td>
        <td><input type="range" min="1" max="100" value="${u.weight}" data-upstream="${escapeHtml(u.id)}" class="weightSlider" style="width:120px;" /> <span class="mono small">${u.weight}</span></td>
        <td class="${keysClass}">${active}/${invalid}</td>
        <td class="mono small">${u.selected_total || 0}</td>
        <td class="mono small">${u.responses_2xx || 0}</td>
        <td class="mono small">${u.responses_4xx || 0}</td>
        <td class="mono small">${u.responses_5xx || 0}</td>
        <td class="mono small">${u.errors_network || 0}</td>
        <td class="mono small">${u.errors_timeout || 0}</td>
      `;
      upstreamsTableBody.appendChild(tr);
    }
    for (const slider of upstreamsTableBody.querySelectorAll('.weightSlider')) {
      slider.onchange = () => updateWeight(slider.dataset.upstream, parseInt(slider.value, 10));
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

    const currentKeyStatus = keyStatusUpstreamSelect.value;
    keyStatusUpstreamSelect.innerHTML = '';
    for (const u of lastUpstreams) {
      const opt = document.createElement('option');
      opt.value = u.id;
      opt.textContent = `${u.id} (keys=${u.keys_total})`;
      keyStatusUpstreamSelect.appendChild(opt);
    }
    if (currentKeyStatus) keyStatusUpstreamSelect.value = currentKeyStatus;
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
      appendRequestRow(r, false);
    }
    requestsInfo.textContent = `count=${list.length} ｜ ${new Date().toLocaleTimeString()}`;
  }

  function appendRequestRow(r, prepend) {
    const tr = document.createElement('tr');
    tr.className = 'clickable';
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
    tr.onclick = () => {
      if (requestDetail) requestDetail.textContent = JSON.stringify(r, null, 2);
    };
    if (prepend && requestsTableBody.firstChild) {
      requestsTableBody.insertBefore(tr, requestsTableBody.firstChild);
    } else {
      requestsTableBody.appendChild(tr);
    }
    while (requestsTableBody.children.length > 200) {
      requestsTableBody.removeChild(requestsTableBody.lastChild);
    }
  }

  if (refreshRequestsChartBtn) refreshRequestsChartBtn.onclick = refreshRequestsChart;
  if (requestsWindowSelect) requestsWindowSelect.onchange = refreshRequestsChart;
  if (refreshRequestsBtn) refreshRequestsBtn.onclick = refreshRequests;
  if (toggleRequestsStreamBtn) toggleRequestsStreamBtn.onclick = () => {
    if (requestStreamAbort) stopRequestsStream();
    else startRequestsStream();
  };

  async function startRequestsStream() {
    stopRequestsStream();
    const t = getToken();
    if (!t) return alert('请先保存 admin token');
    const controller = new AbortController();
    requestStreamAbort = controller;
    if (toggleRequestsStreamBtn) toggleRequestsStreamBtn.textContent = '停止实时';
    try {
      const res = await fetch('/admin/api/v1/requests/stream', {
        headers: { 'X-Admin-Token': t },
        signal: controller.signal
      });
      if (!res.ok || !res.body) throw new Error(String(res.status));
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        buf = processRequestSse(buf);
      }
    } catch (e) {
      if (!controller.signal.aborted) requestsInfo.textContent = `实时流断开: ${e.message || e}`;
    } finally {
      if (requestStreamAbort === controller) stopRequestsStream();
    }
  }

  function stopRequestsStream() {
    if (requestStreamAbort) {
      requestStreamAbort.abort();
      requestStreamAbort = null;
    }
    if (toggleRequestsStreamBtn) toggleRequestsStreamBtn.textContent = '实时流';
  }

  function processRequestSse(buf) {
    buf = buf.replace(/\r\n/g, '\n');
    let idx = buf.indexOf('\n\n');
    while (idx !== -1) {
      const raw = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      const data = raw.split(/\n/).filter(line => line.startsWith('data:')).map(line => line.slice(5).trim()).join('\n');
      if (data) {
        try {
          appendRequestRow(JSON.parse(data), true);
          requestsInfo.textContent = `实时 ｜ ${new Date().toLocaleTimeString()}`;
        } catch (_) {}
      }
      idx = buf.indexOf('\n\n');
    }
    return buf.length > 1024 * 1024 ? buf.slice(-512 * 1024) : buf;
  }

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
    if (upstreamFormatInput) upstreamFormatInput.value = u.format || '';
    if (upstreamProxyInput) upstreamProxyInput.value = u.proxy || '';
  }

  async function updateWeight(id, weight) {
    const u = lastUpstreams.find(x => x.id === id);
    if (!u || !Number.isInteger(weight) || weight <= 0) return;
    const payload = { base_url: u.base_url, weight };
    if (u.max_concurrent_per_key) payload.max_concurrent_per_key = u.max_concurrent_per_key;
    if (u.format) payload.format = u.format;
    if (u.proxy) payload.proxy = u.proxy;
    const { res, text } = await apiFetch(`/admin/api/v1/upstreams/${encodeURIComponent(id)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!res.ok) {
      upstreamResult.textContent = `权重更新失败 ${res.status}\n${text || ''}`;
      return;
    }
    await refreshUpstreams();
  }

  upstreamManageSelect.onchange = () => {
    fillUpstreamForm(upstreamManageSelect.value);
  };

  addUpstreamBtn.onclick = async () => {
    const id = (upstreamIdInput.value || '').trim();
    const baseUrl = (upstreamBaseUrlInput.value || '').trim();
    const weightRaw = (upstreamWeightInput.value || '').trim();
    const format = (upstreamFormatInput && upstreamFormatInput.value || '').trim();
    const proxy = (upstreamProxyInput && upstreamProxyInput.value || '').trim();
    const weight = weightRaw ? parseInt(weightRaw, 10) : null;
    if (!id) return alert('请输入 upstream id');
    if (!baseUrl) return alert('请输入 base_url');
    upstreamResult.textContent = '提交中...';
    const payload = { id, base_url: baseUrl };
    if (Number.isInteger(weight) && weight > 0) payload.weight = weight;
    if (format) payload.format = format;
    if (proxy) payload.proxy = proxy;
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
    const format = (upstreamFormatInput && upstreamFormatInput.value || '').trim();
    const proxy = (upstreamProxyInput && upstreamProxyInput.value || '').trim();
    const weight = weightRaw ? parseInt(weightRaw, 10) : null;
    if (!id) return alert('请输入 upstream id');
    if (!baseUrl) return alert('请输入 base_url');
    upstreamResult.textContent = '提交中...';
    const payload = { base_url: baseUrl };
    if (Number.isInteger(weight) && weight > 0) payload.weight = weight;
    if (format) payload.format = format;
    if (proxy) payload.proxy = proxy;
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
    if (upstreamFormatInput) upstreamFormatInput.value = '';
    if (upstreamProxyInput) upstreamProxyInput.value = '';
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

  async function loadConfig() {
    if (!configPreview) return;
    configPreview.textContent = '加载中...';
    const { res, json, text } = await apiFetch('/admin/api/v1/config');
    if (!res.ok) {
      configPreview.textContent = `失败 ${res.status}\n${text || ''}`;
      return;
    }
    configPreview.textContent = JSON.stringify(json || {}, null, 2);
    if (configInfo) configInfo.textContent = new Date().toLocaleTimeString();
  }

  if (loadConfigBtn) loadConfigBtn.onclick = loadConfig;

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

  function keyStatusPageSizeValue() {
    return parseInt(keyStatusPageSize.value, 10) || 100;
  }

  function maskKey(k) {
    k = k || '';
    if (k.length <= 12) return k;
    return k.slice(0, 6) + '…' + k.slice(-4);
  }

  function renderKeyStatus(list, offset) {
    lastKeyStatusList = Array.isArray(list) ? list.slice() : [];
    const q = (keySearchInput && keySearchInput.value || '').trim().toLowerCase();
    const filter = (keyFilterSelect && keyFilterSelect.value) || 'all';
    const sort = (keySortSelect && keySortSelect.value) || 'index';
    list = lastKeyStatusList.filter(k => {
      if (q && !(k.key || '').toLowerCase().includes(q)) return false;
      if (filter === 'active' && k.status !== 'active') return false;
      if (filter === 'invalid' && k.status !== 'invalid') return false;
      if (filter === 'cooldown' && !(k.cooldown_until_ms && k.cooldown_until_ms > Date.now())) return false;
      return true;
    });
    if (sort === 'failure_desc') {
      list.sort((a, b) => (b.failure_count || 0) - (a.failure_count || 0));
    } else if (sort === 'latency_desc') {
      list.sort((a, b) => (b.latency_p90_ms || 0) - (a.latency_p90_ms || 0));
    }
    keyStatusTableBody.innerHTML = '';
    list.forEach((k, i) => {
      const tr = document.createElement('tr');
      const isInvalid = k.status === 'invalid';
      const statusClass = isInvalid ? 'bad' : 'ok';
      const lat = k.latency_p50_ms != null ? `${k.latency_p50_ms}/${k.latency_p90_ms || 0}/${k.latency_p99_ms || 0}` : '-';
      tr.innerHTML = `
        <td class="mono small">${offset + i + 1}</td>
        <td class="mono small" title="${escapeHtml(k.key)}">${escapeHtml(maskKey(k.key))}</td>
        <td class="${statusClass}">${isInvalid ? 'invalid' : 'active'}</td>
        <td class="mono small">${k.failure_count || 0}</td>
        <td class="mono small">${k.active_requests || 0}</td>
        <td class="mono small">${lat}</td>
      `;
      const actionTd = document.createElement('td');
      const relBtn = document.createElement('button');
      relBtn.className = 'btn';
      relBtn.textContent = '恢复';
      relBtn.dataset.key = k.key;
      relBtn.onclick = () => keyAction('release', [relBtn.dataset.key]);
      const banBtn = document.createElement('button');
      banBtn.className = 'btn';
      banBtn.textContent = '失效';
      banBtn.style.marginLeft = '6px';
      banBtn.dataset.key = k.key;
      banBtn.onclick = () => keyAction('invalidate', [banBtn.dataset.key]);
      const testBtn = document.createElement('button');
      testBtn.className = 'btn';
      testBtn.textContent = '测试';
      testBtn.style.marginLeft = '6px';
      testBtn.dataset.key = k.key;
      testBtn.onclick = () => testKey(testBtn.dataset.key);
      actionTd.appendChild(relBtn);
      actionTd.appendChild(banBtn);
      actionTd.appendChild(testBtn);
      tr.appendChild(actionTd);
      keyStatusTableBody.appendChild(tr);
    });
    renderKeyLatencyChart(list);
  }

  async function loadKeyStatus() {
    const upstream = keyStatusUpstreamSelect.value;
    if (!upstream) return alert('请选择 upstream');
    const limit = keyStatusPageSizeValue();
    if (keyStatusOffset < 0) keyStatusOffset = 0;
    keyStatusInfo.textContent = '加载中...';
    const { res, json, text } = await apiFetch(
      `/admin/api/v1/upstreams/${encodeURIComponent(upstream)}/keys?offset=${keyStatusOffset}&limit=${limit}`
    );
    if (!res.ok) {
      keyStatusInfo.textContent = `失败 ${res.status}`;
      keyStatusResult.textContent = text || '';
      return;
    }
    keyStatusTotal = (json && json.total) || 0;
    if (json && Number.isInteger(json.offset)) {
      keyStatusOffset = json.offset;
    }
    // Clamp offset if it ran past the end (e.g. keys removed since last load).
    if (keyStatusOffset >= keyStatusTotal && keyStatusTotal > 0) {
      const nextOffset = Math.max(0, Math.floor((keyStatusTotal - 1) / limit) * limit);
      if (nextOffset !== keyStatusOffset) {
        keyStatusOffset = nextOffset;
        return loadKeyStatus();
      }
    }
    const list = (json && json.keys) || [];
    renderKeyStatus(list, keyStatusOffset);
    const end = keyStatusOffset + list.length;
    keyStatusInfo.textContent = `total=${keyStatusTotal} ｜ ${new Date().toLocaleTimeString()}`;
    keyStatusPageInfo.textContent = keyStatusTotal > 0 ? `${keyStatusOffset + 1}–${end} / ${keyStatusTotal}` : '0 / 0';
  }

  async function postKeyAction(action, body) {
    const upstream = keyStatusUpstreamSelect.value;
    if (!upstream) return alert('请选择 upstream');
    keyStatusResult.textContent = '提交中...';
    const { res, json, text } = await apiFetch(
      `/admin/api/v1/upstreams/${encodeURIComponent(upstream)}/keys/${action}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
    if (!res.ok) {
      keyStatusResult.textContent = `失败 ${res.status}\n${text || ''}`;
      return false;
    }
    keyStatusResult.textContent = JSON.stringify(json || {}, null, 2);
    await loadKeyStatus();
    await refreshUpstreams();
    return true;
  }

  async function keyAction(action, keys) {
    const body = { keys };
    await postKeyAction(action, body);
  }

  async function testKey(key) {
    const upstream = keyStatusUpstreamSelect.value;
    if (!upstream) return alert('请选择 upstream');
    keyStatusResult.textContent = '测试中...';
    const { res, json, text } = await apiFetch(
      `/admin/api/v1/upstreams/${encodeURIComponent(upstream)}/keys/test`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key })
      });
    if (!res.ok) {
      keyStatusResult.textContent = `失败 ${res.status}\n${text || ''}`;
      return;
    }
    keyStatusResult.textContent = JSON.stringify(json || {}, null, 2);
  }

  function renderKeyLatencyChart(list) {
    if (!keyLatencyChartCanvas || typeof Chart === 'undefined') return;
    const rows = (list || []).filter(k => k.latency_p90_ms != null).slice(0, 25);
    const labels = rows.map(k => maskKey(k.key));
    const data = rows.map(k => k.latency_p90_ms || 0);
    if (keyLatencyChart) keyLatencyChart.destroy();
    keyLatencyChart = new Chart(keyLatencyChartCanvas, {
      type: 'bar',
      data: { labels, datasets: [{ label: 'p90 ms', data, backgroundColor: '#1e6bd6' }] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: { y: { beginAtZero: true } }
      }
    });
  }

  async function bulkKeyAction(action) {
    const upstream = keyStatusUpstreamSelect.value;
    if (!upstream) return alert('请选择 upstream');
    const verb = action === 'invalidate' ? '失效' : '恢复';
    if (!confirm(`确认${verb} upstream "${upstream}" 的全部 keys?`)) return;
    const body = { all: true };
    await postKeyAction(action, body);
  }

  if (loadKeyStatusBtn) loadKeyStatusBtn.onclick = () => { keyStatusOffset = 0; loadKeyStatus(); };
  if (keySearchInput) keySearchInput.oninput = () => renderKeyStatus(lastKeyStatusList, keyStatusOffset);
  if (keyFilterSelect) keyFilterSelect.onchange = () => renderKeyStatus(lastKeyStatusList, keyStatusOffset);
  if (keySortSelect) keySortSelect.onchange = () => renderKeyStatus(lastKeyStatusList, keyStatusOffset);
  if (keyStatusUpstreamSelect) keyStatusUpstreamSelect.onchange = () => { keyStatusOffset = 0; loadKeyStatus(); };
  if (keyStatusPageSize) keyStatusPageSize.onchange = () => { keyStatusOffset = 0; loadKeyStatus(); };
  if (releaseAllKeysBtn) releaseAllKeysBtn.onclick = () => bulkKeyAction('release');
  if (banAllKeysBtn) banAllKeysBtn.onclick = () => bulkKeyAction('invalidate');
  if (exportKeysBtn) exportKeysBtn.onclick = () => {
    const upstream = keyStatusUpstreamSelect.value;
    if (!upstream) return alert('请选择 upstream');
    const exportToken = prompt('请输入导出密码（Export Token）:');
    if (!exportToken) return;
    const url = `/admin/api/v1/upstreams/${encodeURIComponent(upstream)}/keys/export`;
    fetch(url, { headers: { 'X-Admin-Token': getToken(), 'X-Export-Token': exportToken } })
      .then(resp => {
        if (!resp.ok) return resp.json().then(d => { throw new Error(d.error?.message || resp.statusText); });
        return resp.blob();
      })
      .then(blob => {
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = `${upstream}_keys_${Math.floor(Date.now()/1000)}.txt`;
        a.click();
        URL.revokeObjectURL(a.href);
      })
      .catch(e => alert('导出失败: ' + e.message));
  };
  if (keyStatusPrevBtn) keyStatusPrevBtn.onclick = () => {
    const limit = keyStatusPageSizeValue();
    keyStatusOffset = Math.max(0, keyStatusOffset - limit);
    loadKeyStatus();
  };
  if (keyStatusNextBtn) keyStatusNextBtn.onclick = () => {
    const limit = keyStatusPageSizeValue();
    if (keyStatusOffset + limit < keyStatusTotal) {
      keyStatusOffset += limit;
      loadKeyStatus();
    }
  };

  function formatDuration(ms) {
    if (ms <= 0) return '-';
    const s = Math.floor(ms / 1000);
    if (s < 60) return s + 's';
    const m = Math.floor(s / 60);
    if (m < 60) return m + 'm' + (s % 60) + 's';
    const h = Math.floor(m / 60);
    if (h < 24) return h + 'h' + (m % 60) + 'm';
    const d = Math.floor(h / 24);
    return d + 'd' + (h % 24) + 'h';
  }

  function escapeHtml(s) {
    return (s || '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));
  }

  // Stats stream (fetch + ReadableStream with X-Admin-Token)
  let statsAbort = null;
  let statsRetryTimer = null;

  function scheduleStatsRetry() {
    if (statsRetryTimer) return;
    statsRetryTimer = setTimeout(() => {
      statsRetryTimer = null;
      startStatsStream();
    }, 2000);
  }

  function processSseBuffer(buf) {
    buf = buf.replace(/\r\n/g, '\n');
    let idx = buf.indexOf('\n\n');
    while (idx !== -1) {
      const raw = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      const lines = raw.split(/\r?\n/);
      const dataLines = [];
      for (const line of lines) {
        if (line.startsWith('data:')) {
          let payload = line.slice(5);
          if (payload.startsWith(' ')) payload = payload.slice(1);
          dataLines.push(payload);
        }
      }
      if (dataLines.length > 0) {
        const data = dataLines.join('\n');
        try {
          const json = JSON.parse(data);
          renderStats(json);
        } catch (e) {
          if (statsPre) statsPre.textContent = data;
        }
      }
      idx = buf.indexOf('\n\n');
    }
    if (buf.length > 1024 * 1024) {
      buf = buf.slice(-512 * 1024);
    }
    return buf;
  }

  function renderStats(json) {
    if (!statsGrid) return;
    const stats = [
      { label: '在线 Keys', value: json.keys_active ?? json.keys_total ?? '-', color: 'var(--ok)' },
      { label: 'Invalid Keys', value: json.keys_invalid ?? '-', color: 'var(--bad)' },
      { label: '并发请求', value: json.requests_inflight ?? '-', color: 'var(--accent)' },
      { label: '队列深度', value: json.queue_depth ?? '-', color: json.queue_depth > 0 ? 'var(--bad)' : 'var(--ok)' },
      { label: 'Upstreams', value: json.upstreams ?? '-', color: 'var(--text)' },
      { label: '总请求', value: json.requests_total ?? '-', color: 'var(--text)' },
      { label: '成功 2xx', value: json.responses_2xx ?? '-', color: 'var(--ok)' },
      { label: '失败 4xx', value: json.responses_4xx ?? '-', color: 'var(--bad)' },
      { label: '错误 5xx', value: json.responses_5xx ?? '-', color: 'var(--bad)' },
      { label: '网络错误', value: json.errors_network ?? '-', color: 'var(--bad)' },
      { label: '超时', value: json.errors_timeout ?? '-', color: 'var(--bad)' },
      { label: '运行时间', value: json.uptime_secs != null ? formatDuration(json.uptime_secs * 1000) : '-', color: 'var(--text)' },
    ];
    statsGrid.innerHTML = stats.map(s => `
      <div class="stat-card">
        <div class="stat-label">${s.label}</div>
        <div class="stat-value" style="color:${s.color}">${s.value}</div>
      </div>
    `).join('');
  }

  async function startStatsStream() {
    stopStatsStream();
    const t = getToken();
    if (!t) {
      if (statsGrid) statsGrid.innerHTML = '<div class="muted">未设置 token。</div>';
      return;
    }

    authStatus.textContent = 'Stats stream 连接中...';

    const controller = new AbortController();
    statsAbort = controller;

    let res;
    try {
      res = await fetch('/admin/api/v1/stats/stream', {
        headers: { 'X-Admin-Token': t },
        signal: controller.signal
      });
    } catch (e) {
      if (!controller.signal.aborted) {
        authStatus.textContent = 'Stats stream 连接失败（将自动重连）。';
        scheduleStatsRetry();
      }
      return;
    }

    if (!res.ok || !res.body) {
      authStatus.textContent = `Stats stream 失败: ${res.status}`;
      return;
    }

    authStatus.textContent = 'Stats stream 已连接。';

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        if (value) {
          buf += decoder.decode(value, { stream: true });
          buf = processSseBuffer(buf);
        }
      }
      buf += decoder.decode();
      buf = processSseBuffer(buf);
      if (!controller.signal.aborted) {
        authStatus.textContent = 'Stats stream 已结束（将自动重连）。';
        scheduleStatsRetry();
      }
    } catch (e) {
      if (!controller.signal.aborted) {
        authStatus.textContent = 'Stats stream 连接异常（将自动重连）。';
        scheduleStatsRetry();
      }
    }
  }

  function stopStatsStream() {
    if (statsAbort) {
      statsAbort.abort();
      statsAbort = null;
    }
    if (statsRetryTimer) {
      clearTimeout(statsRetryTimer);
      statsRetryTimer = null;
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
    loadConfig();
    startRequestsAutoRefresh();
  } else {
    authStatus.textContent = '请输入并保存 admin token。';
    if (statsGrid) statsGrid.innerHTML = '<div class="muted">未连接。</div>';
  }
})();
