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
      const payload = typeof window.buildLeadsParams === 'function' ? window.buildLeadsParams() : {};
      delete payload.limit;
      delete payload.offset;
      delete payload.order_by;
      delete payload.order_dir;
      return payload;
    } catch (_) {
      return {};
    }
  }

  async function loadEducationKpis() {
    document.querySelectorAll('.ops-kpi strong').forEach((el) => el.classList.add('ops-number-loading'));
    try {
      const response = await fetch('/api/kpis/education', {
        method: 'POST',
        credentials: 'same-origin',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(currentPayload()),
      });
      const body = await response.json();
      if (!response.ok || body?.ok === false) throw new Error(body?.error || 'Falha nos indicadores');
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
      console.error('KPIs educacionais:', err);
    } finally {
      document.querySelectorAll('.ops-kpi strong').forEach((el) => el.classList.remove('ops-number-loading'));
    }
  }

  function scheduleKpis() {
    clearTimeout(kpiTimer);
    kpiTimer = setTimeout(loadEducationKpis, 320);
  }

  function enhanceRows() {
    const rows = document.querySelectorAll('#tbl tbody tr');
    let ready = 0;
    rows.forEach((row) => {
      if (row.querySelector('.table-feedback')) return;
      const cell = row.cells?.[11];
      const value = (cell?.textContent || '').trim().toLowerCase();
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
    document.querySelectorAll('.quick-action').forEach((b) => b.classList.remove('active'));
    if (mode === 'queue') {
      if (situation) situation.value = 'vazias';
      if (ini) ini.value = '';
      if (fim) fim.value = '';
      $('#quickQueue')?.classList.add('active');
    } else if (mode === 'today') {
      const today = new Date().toISOString().slice(0,10);
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
    window.currentPage = 1;
    if (typeof window.updateDataDisparoMonthState === 'function') window.updateDataDisparoMonthState();
    if (typeof window.loadLeadsAndKpis === 'function') window.loadLeadsAndKpis();
    scheduleKpis();
  }

  function init() {
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

    ['#btnApply','#btnReload','#btnClear','#btnPrevPage','#btnNextPage'].forEach((id) => {
      $(id)?.addEventListener('click', scheduleKpis);
    });
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