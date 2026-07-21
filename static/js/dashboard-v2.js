(() => {
  const $ = (s) => document.querySelector(s);
  const nf = new Intl.NumberFormat('pt-BR');
  let kpiTimer;

  function number(id, value) {
    const el = $(id);
    if (!el) return;
    el.textContent = nf.format(Number(value || 0));
  }

  function text(id, value, fallback = '—') {
    const el = $(id);
    if (el) el.textContent = value || fallback;
  }

  function currentPayload() {
    try {
      const payload = typeof window.buildLeadsParams === 'function'
        ? window.buildLeadsParams()
        : {};
      delete payload.limit;
      delete payload.offset;
      delete payload.order_by;
      delete payload.order_dir;
      return payload;
    } catch (_) {
      return {};
    }
  }

  async function postJson(path, payload) {
    const response = await fetch(path, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload || {}),
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok || body?.ok === false) {
      throw new Error(body?.error?.message || body?.error || `Falha HTTP ${response.status}`);
    }
    return body;
  }

  async function countLeads(payload) {
    const body = await postJson('/api/leads/search', {
      ...(payload || {}),
      limit: 1,
      offset: 0,
      order_by: 'prioridade_disparo',
      order_dir: 'ASC',
    });
    return Number(body?.total || 0);
  }

  function isoDate(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  async function loadFallbackKpis(base) {
    const todayDate = new Date();
    const sevenDaysAgo = new Date(todayDate);
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 6);
    const backlogLimit = new Date(todayDate);
    backlogLimit.setDate(backlogLimit.getDate() - 3);

    const today = isoDate(todayDate);
    const sevenStart = isoDate(sevenDaysAgo);
    const backlogEnd = isoDate(backlogLimit);

    const [queue, todayCount, sevenCount, enrolled, backlog, statusBody] = await Promise.all([
      countLeads({...base, data_disparo_situacao: 'vazias'}),
      countLeads({...base, data_ini: today, data_fim: today}),
      countLeads({...base, data_ini: sevenStart, data_fim: today}),
      countLeads({...base, matriculado: 'true'}),
      countLeads({...base, data_disparo_situacao: 'vazias', data_fim: backlogEnd}),
      postJson('/api/kpis/search', base).catch(() => ({})),
    ]);

    number('#kpiQueue', queue);
    number('#kpiToday', todayCount);
    number('#kpi7d', sevenCount);
    number('#kpiEnrolled', enrolled);
    number('#kpiBacklog', backlog);
    text('#kpiDispatchedToday', '—');
    text('#kpiConversion', 'Conversão: cálculo indisponível');
    text('#kpiAvgDelay', 'Tempo médio: cálculo indisponível');

    const top = statusBody?.top_status;
    text('#kpiTopStatus', top ? `${top.status} (${nf.format(top.cnt)})` : null);
  }

  async function loadEducationKpis() {
    document.querySelectorAll('.ops-kpi strong').forEach((el) => {
      el.classList.add('ops-number-loading');
    });

    const base = currentPayload();
    try {
      const body = await postJson('/api/kpis/education', base);
      const d = body.data || body;
      number('#kpiQueue', d.fila_disparo);
      number('#kpiToday', d.inscritos_hoje);
      number('#kpi7d', d.inscritos_7_dias);
      number('#kpiDispatchedToday', d.disparados_hoje);
      number('#kpiEnrolled', d.matriculas);
      number('#kpiBacklog', d.backlog_3_dias);
      text('#kpiConversion', `Conversão: ${Number(d.taxa_matricula || 0).toLocaleString('pt-BR',{minimumFractionDigits:1,maximumFractionDigits:1})}%`);
      text('#kpiAvgDelay', `Tempo médio: ${Number(d.tempo_medio_disparo_dias || 0).toLocaleString('pt-BR',{minimumFractionDigits:1,maximumFractionDigits:1})} dias`);
      text('#topCourse', d.top_curso?.nome ? `${d.top_curso.nome} (${nf.format(d.top_curso.total)})` : null);
      text('#topOrigin', d.top_origem?.nome ? `${d.top_origem.nome} (${nf.format(d.top_origem.total)})` : null);
      text('#topModality', d.top_modalidade?.nome ? `${d.top_modalidade.nome} (${nf.format(d.top_modalidade.total)})` : null);
    } catch (err) {
      console.warn('Endpoint educacional indisponível; usando fallback:', err);
      await loadFallbackKpis(base).catch((fallbackError) => {
        console.error('Falha também no fallback dos KPIs:', fallbackError);
        document.querySelectorAll('.ops-kpi strong').forEach((el) => {
          if (el.textContent.trim() === '—') el.textContent = 'Erro';
        });
      });
    } finally {
      document.querySelectorAll('.ops-kpi strong').forEach((el) => {
        el.classList.remove('ops-number-loading');
      });
    }
  }

  function scheduleKpis() {
    clearTimeout(kpiTimer);
    kpiTimer = setTimeout(loadEducationKpis, 350);
  }

  function enhanceRows() {
    const rows = document.querySelectorAll('#tbl tbody tr');
    let ready = 0;
    rows.forEach((row) => {
      if (row.querySelector('.table-feedback')) return;
      const value = (row.cells?.[11]?.textContent || '').trim().toLowerCase();
      const isReady = !value || value === '-' || value.includes('sem disparo') || value.includes('pronto para disparo');
      row.classList.toggle('ops-ready-row', isReady);
      row.classList.toggle('ops-sent-row', !isReady);
      if (isReady) ready += 1;
    });
    number('#pageReadyCount', ready);
  }

  function setQueueFilter(mode) {
    const situation = $('#fDataDisparoSituacao');
    const ini = $('#fIni');
    const fim = $('#fFim');
    document.querySelectorAll('.quick-action').forEach((button) => button.classList.remove('active'));

    if (mode === 'queue') {
      if (situation) situation.value = 'vazias';
      if (ini) ini.value = '';
      if (fim) fim.value = '';
      $('#quickQueue')?.classList.add('active');
    } else if (mode === 'today') {
      const today = isoDate(new Date());
      if (situation) situation.value = '';
      if (ini) ini.value = today;
      if (fim) fim.value = today;
      $('#quickToday')?.classList.add('active');
    } else {
      if (situation) situation.value = '';
      if (ini) ini.value = '';
      if (fim) fim.value = '';
      $('#quickClear')?.classList.add('active');
    }

    $('#btnApply')?.click();
    scheduleKpis();
  }

  function init() {
    const legacyCount = $('#kpiCount');
    if (legacyCount) {
      legacyCount.hidden = true;
      legacyCount.style.setProperty('display', 'none', 'important');
    }

    $('#quickQueue')?.addEventListener('click', () => setQueueFilter('queue'));
    $('#quickToday')?.addEventListener('click', () => setQueueFilter('today'));
    $('#quickClear')?.addEventListener('click', () => setQueueFilter('all'));
    $('#toggleFilters')?.addEventListener('click', () => {
      const panel = $('#filterPanel');
      const toggle = $('#toggleFilters');
      const collapsed = panel?.classList.toggle('collapsed');
      toggle?.setAttribute('aria-expanded', String(!collapsed));
      text('#filterToggleLabel', collapsed ? 'Expandir' : 'Recolher');
    });
    ['#btnApply','#btnReload','#btnClear','#btnPrevPage','#btnNextPage'].forEach((id) => $(id)?.addEventListener('click', scheduleKpis));
    $('#filterPanel')?.addEventListener('change', scheduleKpis);
    $('#fBusca')?.addEventListener('input', scheduleKpis);
    window.addEventListener('gestao:upload-concluido', scheduleKpis);
    const tbody = $('#tbl tbody');
    if (tbody) new MutationObserver(enhanceRows).observe(tbody, {childList:true,subtree:true,characterData:true});
    enhanceRows();
    loadEducationKpis();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
