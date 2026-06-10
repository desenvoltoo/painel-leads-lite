(() => {
  'use strict';

  const state = {
    filters: {},
    controllers: new Map(),
    charts: {},
    tables: {},
    qualityType: null,
    loadingCount: 0,
  };

  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const nf = new Intl.NumberFormat('pt-BR');
  const pf = new Intl.NumberFormat('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const df = new Intl.DateTimeFormat('pt-BR', { dateStyle: 'short', timeStyle: 'short' });

  const kpis = [
    ['total_leads', 'Total de leads', 'Carteira operacional filtrada.', 'info'],
    ['novos_leads_periodo', 'Novos no período', 'Leads com data_inscricao dentro do filtro.', 'info'],
    ['nunca_trabalhados', 'Nunca trabalhados', 'Leads com status vazio.', 'warning'],
    ['leads_em_carteira', 'Em carteira', 'Status preenchido e não matriculado.', 'neutral'],
    ['leads_matriculados', 'Matriculados', 'flag_matriculado ou status normalizado.', 'success'],
    ['taxa_geral_conversao', 'Conversão geral', 'Matriculados / total de leads.', 'success', 'pct'],
    ['leads_sem_status', 'Sem status', 'Status nulo ou vazio.', 'warning'],
    ['leads_parados_7_dias', 'Parados > 7 dias', 'Sem última ação recente.', 'danger'],
    ['media_horas_primeiro_contato', 'Tempo 1º contato', 'Horas médias entre inscrição e primeiro contato.', 'neutral', 'hours'],
    ['media_horas_ate_matricula', 'Tempo matrícula', 'Horas médias entre inscrição e matrícula.', 'neutral', 'hours'],
    ['quantidade_disparos', 'Disparos', 'Registros com data_disparo.', 'info'],
    ['leads_atualizados_carga_mais_recente', 'Carga mais recente', 'Registros cujo dt_upload é o maior da base filtrada.', 'info'],
  ];

  function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>'"]/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[ch]));
  }

  function formatValue(value, type) {
    if (value === null || value === undefined || value === '') return '--';
    if (type === 'pct') return `${pf.format(Number(value) || 0)}%`;
    if (type === 'hours') return `${pf.format(Number(value) || 0)}h`;
    if (type === 'date') return formatDate(value);
    if (typeof value === 'number') return nf.format(value);
    if (!Number.isNaN(Number(value)) && String(value).trim() !== '') return nf.format(Number(value));
    return String(value);
  }

  function formatDate(value) {
    if (!value) return '--';
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return String(value).slice(0, 10);
    return df.format(d);
  }

  function formatPhone(value) {
    const digits = String(value || '').replace(/\D/g, '');
    if (digits.length === 11) return `(${digits.slice(0, 2)}) ${digits.slice(2, 7)}-${digits.slice(7)}`;
    if (digits.length === 10) return `(${digits.slice(0, 2)}) ${digits.slice(2, 6)}-${digits.slice(6)}`;
    return value || '--';
  }

  function setLoading(on) {
    state.loadingCount += on ? 1 : -1;
    state.loadingCount = Math.max(0, state.loadingCount);
    const loading = state.loadingCount > 0;
    $('#refreshStatus').innerHTML = `<span class="status-dot ${loading ? 'loading' : ''}"></span>${loading ? 'Atualizando' : 'Pronto'}`;
    $('#btnRefresh').disabled = loading;
  }

  function toast(message, type = 'danger') {
    const region = $('#toastRegion');
    const el = document.createElement('div');
    el.className = `alert alert-${type} shadow-sm`;
    el.role = 'alert';
    el.innerHTML = `${escapeHtml(message)} <button type="button" class="btn-close float-end" aria-label="Fechar"></button>`;
    $('.btn-close', el).addEventListener('click', () => el.remove());
    region.appendChild(el);
    setTimeout(() => el.remove(), 7000);
  }

  function buildParams(extra = {}) {
    const params = new URLSearchParams();
    Object.entries({ ...state.filters, ...extra }).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') params.set(key, value);
    });
    return params;
  }



  async function downloadGestao(endpoint, extra, button, fallbackName) {
    const original = button ? button.textContent : '';
    if (button) { button.disabled = true; button.textContent = 'Gerando...'; }
    setLoading(true);
    try {
      const res = await fetch(`/api/gestao/${endpoint}?${buildParams(extra)}`, { headers: { Accept: 'text/csv,application/json' } });
      const type = res.headers.get('content-type') || '';
      if (!res.ok) {
        const payload = type.includes('application/json') ? await res.json().catch(() => null) : null;
        throw new Error(payload?.error?.message || 'Não foi possível gerar o arquivo.');
      }
      const blob = await res.blob();
      const cd = res.headers.get('content-disposition') || '';
      const match = cd.match(/filename="?([^";]+)"?/i);
      const filename = match?.[1] || fallbackName;
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      toast('Arquivo gerado com sucesso.', 'success');
    } catch (err) {
      toast(err.message || 'Erro ao exportar arquivo.');
    } finally {
      setLoading(false);
      if (button) { button.disabled = false; button.textContent = original; }
    }
  }

  async function apiGet(endpoint, extra = {}) {
    const old = state.controllers.get(endpoint);
    if (old) old.abort();
    const controller = new AbortController();
    state.controllers.set(endpoint, controller);
    setLoading(true);
    const timeout = setTimeout(() => controller.abort(), 45000);
    try {
      const res = await fetch(`/api/gestao/${endpoint}?${buildParams(extra)}`, { signal: controller.signal, headers: { Accept: 'application/json' } });
      const payload = await res.json().catch(() => null);
      if (!res.ok || !payload?.ok) throw new Error(payload?.error?.message || 'Não foi possível carregar os dados.');
      $('#lastUpdated').textContent = formatDate(payload.meta.generated_at);
      return payload;
    } catch (err) {
      if (err.name !== 'AbortError') toast(err.message || 'Erro de comunicação com o servidor.');
      throw err;
    } finally {
      clearTimeout(timeout);
      setLoading(false);
      if (state.controllers.get(endpoint) === controller) state.controllers.delete(endpoint);
    }
  }

  function readFilters() {
    const form = $('#filtersForm');
    const values = {};
    new FormData(form).forEach((value, key) => {
      const clean = String(value).trim();
      if (clean) values[key] = clean;
    });
    if (values.data_ini && values.data_fim && values.data_ini > values.data_fim) {
      toast('Período inicial não pode ser maior que o período final.', 'warning');
      return null;
    }
    return values;
  }

  function syncUrlAndStorage() {
    sessionStorage.setItem('gestao.filters', JSON.stringify(state.filters));
    const qs = buildParams().toString();
    history.replaceState(null, '', qs ? `/gestao?${qs}` : '/gestao');
    renderChips();
  }

  function restoreFilters() {
    const params = new URLSearchParams(location.search);
    let data = {};
    if (params.size) params.forEach((v, k) => { data[k] = v; });
    else {
      try { data = JSON.parse(sessionStorage.getItem('gestao.filters') || '{}'); } catch { data = {}; }
    }
    Object.entries(data).forEach(([key, value]) => {
      const el = $(`[name="${CSS.escape(key)}"]`);
      if (el) el.value = value;
    });
    state.filters = data;
  }

  function renderChips() {
    const box = $('#activeChips');
    const entries = Object.entries(state.filters);
    box.innerHTML = entries.length ? '' : '<span class="text-secondary">Nenhum filtro ativo.</span>';
    entries.forEach(([key, value]) => {
      const chip = document.createElement('button');
      chip.type = 'button';
      chip.className = 'chip';
      chip.textContent = `${key}: ${value} ×`;
      chip.addEventListener('click', () => {
        const el = $(`[name="${CSS.escape(key)}"]`);
        if (el) el.value = '';
        delete state.filters[key];
        syncUrlAndStorage();
        loadAll();
      });
      box.appendChild(chip);
    });
  }

  async function loadOptions() {
    try {
      const payload = await apiGet('opcoes');
      Object.entries(payload.data || {}).forEach(([key, values]) => {
        $$(`[data-option="${CSS.escape(key)}"]`).forEach(select => {
          const selected = select.value;
          const first = select.querySelector('option')?.outerHTML || '<option value="">Todos</option>';
          select.innerHTML = first + (values || []).map(v => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join('');
          select.value = selected;
        });
      });
    } catch (_) { /* toast already shown */ }
  }

  function renderKpiSkeleton() {
    $('#kpiGrid').innerHTML = kpis.map(([key, title, desc, tone]) => `<article class="kpi-card ${tone} skeleton" data-card="${key}"><span>${escapeHtml(title)}</span><strong>--</strong><small>${escapeHtml(desc)}</small></article>`).join('');
  }

  async function loadResumo() {
    renderKpiSkeleton();
    try {
      const { data } = await apiGet('resumo');
      $('#kpiGrid').innerHTML = kpis.map(([key, title, desc, tone, type]) => `<article class="kpi-card ${tone}" title="${escapeHtml(desc)}"><span>${escapeHtml(title)}</span><strong>${escapeHtml(formatValue(data[key], type))}</strong><small>${escapeHtml(desc)}</small></article>`).join('');
    } catch (_) {
      $('#kpiGrid').innerHTML = '<div class="empty-state error">Erro ao carregar indicadores. <button class="btn btn-sm btn-outline-primary" id="retryResumo">Tentar novamente</button></div>';
      $('#retryResumo')?.addEventListener('click', loadResumo);
    }
  }

  async function loadFunil() {
    const box = $('#funilList');
    box.innerHTML = '<div class="empty-state">Carregando funil...</div>';
    try {
      const { data } = await apiGet('funil');
      const etapas = data.etapas || [];
      if (!etapas.length) { box.innerHTML = '<div class="empty-state">Sem dados para o filtro.</div>'; return; }
      box.innerHTML = etapas.map(row => `<div class="funnel-row"><div><strong>${escapeHtml(row.etapa)}</strong><span>${formatValue(row.volume)} leads • ${formatValue(row.pct_total, 'pct')} do total</span></div><div class="funnel-bar" aria-hidden="true"><span style="width:${Math.min(100, Number(row.pct_total || 0))}%"></span></div><small>Conv. etapa: ${row.conversao_etapa_anterior === null ? '--' : formatValue(row.conversao_etapa_anterior, 'pct')} • Perda: ${row.perda_etapa_anterior === null ? '--' : formatValue(row.perda_etapa_anterior)}</small></div>`).join('');
    } catch (_) { box.innerHTML = '<div class="empty-state error">Erro ao carregar funil.</div>'; }
  }

  function chart(id, config) {
    if (state.charts[id]) state.charts[id].destroy();
    state.charts[id] = new Chart($(id), config);
  }

  async function loadEvolucao() {
    try {
      const gran = $('#granularity').value;
      const { data } = await apiGet('evolucao', { granularidade: gran });
      const rows = data.series || [];
      chart('#chartEvolucao', {
        type: 'line',
        data: { labels: rows.map(r => String(r.periodo).slice(0, 10)), datasets: [
          { label: 'Leads recebidos', data: rows.map(r => Number(r.leads_recebidos || 0)), borderColor: '#2563eb', backgroundColor: 'rgba(37,99,235,.12)', tension: .25 },
          { label: 'Matrículas', data: rows.map(r => Number(r.matriculas || 0)), borderColor: '#16a34a', backgroundColor: 'rgba(22,163,74,.12)', tension: .25 },
          { label: 'Disparos', data: rows.map(r => Number(r.disparos || 0)), borderColor: '#f59e0b', backgroundColor: 'rgba(245,158,11,.12)', tension: .25 },
          { label: 'Atualizações dt_upload', data: rows.map(r => Number(r.atualizacoes || 0)), borderColor: '#64748b', backgroundColor: 'rgba(100,116,139,.12)', tension: .25 },
        ]},
        options: { responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false }, plugins: { legend: { position: 'bottom' } } }
      });
    } catch (_) { /* toast already shown */ }
  }

  function rankingBlock(title, rows) {
    if (!rows?.length) return `<article class="ranking-card"><h3>${escapeHtml(title)}</h3><div class="empty-state">Sem dados suficientes.</div></article>`;
    const max = Math.max(...rows.map(r => Number(r.taxa_conversao_pct || 0)), 1);
    return `<article class="ranking-card"><h3>${escapeHtml(title)}</h3>${rows.map(r => `<div class="ranking-row"><div><strong>${r.posicao}. ${escapeHtml(r.nome)}</strong>${r.melhor_resultado ? '<span class="badge text-bg-success">melhor resultado</span>' : ''}<small>${formatValue(r.total_leads)} leads • ${formatValue(r.matriculas)} matrículas • ${formatValue(r.taxa_conversao_pct, 'pct')}</small></div><span class="ranking-bar"><i style="width:${Math.max(3, Number(r.taxa_conversao_pct || 0) / max * 100)}%"></i></span></div>`).join('')}</article>`;
  }

  async function loadRankings() {
    const box = $('#rankingsGrid');
    box.innerHTML = '<div class="empty-state">Carregando rankings...</div>';
    try {
      const { data } = await apiGet('rankings');
      $('#rankingHint').textContent = `Rankings de conversão exigem pelo menos ${data.minimo_leads_conversao} leads.`;
      box.innerHTML = [
        rankingBlock('Consultores por matrículas', data.consultores_matriculas),
        rankingBlock('Consultores por conversão', data.consultores_conversao),
        rankingBlock('Origens por volume', data.origens_volume),
        rankingBlock('Origens por conversão', data.origens_conversao),
        rankingBlock('Cursos por volume', data.cursos_volume),
        rankingBlock('Cursos por conversão', data.cursos_conversao),
      ].join('');
    } catch (_) { box.innerHTML = '<div class="empty-state error">Erro ao carregar rankings.</div>'; }
  }

  function initTables() {
    state.tables.prod = $('#produtividadeTable') && new DataTable('#produtividadeTable', { responsive: true, pageLength: 10, dom: 'Bfrtip', buttons: ['csvHtml5'], language: { url: '//cdn.datatables.net/plug-ins/1.13.8/i18n/pt-BR.json' } });
    state.tables.fila = $('#filaTable') && new DataTable('#filaTable', { responsive: true, pageLength: 10, dom: 'Bfrtip', buttons: ['csvHtml5'], order: [[11, 'desc']], language: { url: '//cdn.datatables.net/plug-ins/1.13.8/i18n/pt-BR.json' } });
  }

  function replaceTable(dt, rows) {
    dt.clear();
    rows.forEach(r => dt.row.add(r));
    dt.draw();
  }

  async function loadProdutividade() {
    try {
      const { data } = await apiGet('produtividade', { limit: 500 });
      replaceTable(state.tables.prod, (data.rows || []).map(r => [escapeHtml(r.consultor), formatValue(r.total_leads), formatValue(r.leads_novos), formatValue(r.leads_sem_status), formatValue(r.leads_em_carteira), formatValue(r.matriculados), formatValue(r.taxa_conversao_pct, 'pct'), formatValue(r.quantidade_acionamentos), formatValue(r.media_horas_primeiro_contato, 'hours'), formatValue(r.media_horas_ate_matricula, 'hours'), formatDate(r.ultima_atividade), formatValue(r.dias_sem_atividade), formatValue(r.score_medio_carteira), `<span class="status-badge ${escapeHtml(r.situacao)}">${escapeHtml(r.situacao)}</span>`]));
    } catch (_) { replaceTable(state.tables.prod, []); }
  }

  async function loadFila() {
    try {
      const { data } = await apiGet('fila', { limit: 500 });
      const rows = data.items || data.rows || [];
      replaceTable(state.tables.fila, rows.map(r => [escapeHtml(r.nome), escapeHtml(formatPhone(r.celular)), escapeHtml(r.curso), escapeHtml(r.polo), escapeHtml(r.origem), escapeHtml(r.campanha), escapeHtml(r.consultor_comercial), escapeHtml(r.status), formatDate(r.data_inscricao), formatDate(r.data_ultima_acao), formatValue(r.dias_sem_acao), formatValue(r.grupo_prioridade), `<span class="priority-badge ${escapeHtml(String(r.grupo_prioridade || '').toLowerCase())}">${escapeHtml(r.prioridade)}</span>`, escapeHtml(r.motivo_prioridade)]));
    } catch (_) { replaceTable(state.tables.fila, []); }
  }

  async function loadQualidade() {
    const box = $('#qualityGrid');
    const details = $('#qualityDetails');
    box.innerHTML = '<div class="empty-state">Carregando qualidade...</div>';
    try {
      const { data } = await apiGet('qualidade');
      const indicadores = data.indicadores || data || {};
      const labels = {
        sem_celular: 'Sem celular', sem_email: 'Sem e-mail', sem_cpf: 'Sem CPF', sem_origem: 'Sem origem', sem_curso: 'Sem curso', sem_consultor: 'Sem consultor', sem_status: 'Sem status', telefone_invalido: 'Telefone inválido', cpf_invalido: 'CPF inválido/incompleto', duplicado_celular: 'Duplicados celular', duplicado_cpf: 'Duplicados CPF', rejeitado: 'Rejeitados upload', sem_dt_upload: 'Sem dt_upload', data_invalida: 'Datas inválidas'
      };
      box.innerHTML = Object.entries(labels).map(([k, l]) => `<div class="quality-card"><span>${escapeHtml(l)}</span><strong>${formatValue(indicadores[k])}</strong><div class="d-flex gap-2 flex-wrap"><button class="btn btn-link btn-sm quality-detail" data-quality="${escapeHtml(k)}" type="button">Ver registros</button><button class="btn btn-link btn-sm quality-export" data-quality="${escapeHtml(k)}" type="button">Exportar</button></div></div>`).join('');
      details.classList.add('d-none');
    } catch (_) { box.innerHTML = '<div class="empty-state error">Erro ao carregar qualidade. <button class="btn btn-sm btn-outline-primary" id="retryQualidade">Tentar novamente</button></div>'; $('#retryQualidade')?.addEventListener('click', loadQualidade); }
  }

  async function loadQualidadeDetalhes(tipo) {
    const box = $('#qualityDetails');
    state.qualityType = tipo;
    box.classList.remove('d-none');
    box.innerHTML = '<div class="empty-state">Carregando registros...</div>';
    try {
      const { data } = await apiGet('qualidade/detalhes', { tipo, limit: 25 });
      const rows = data.items || [];
      if (!rows.length) { box.innerHTML = '<div class="empty-state">Nenhum registro encontrado para este indicador.</div>'; return; }
      box.innerHTML = `<table><thead><tr><th>Motivo</th><th>Identificador</th><th>Nome</th><th>Curso</th><th>Consultor</th><th>Inscrição</th><th>Upload</th><th>Origem</th><th>Status</th></tr></thead><tbody>${rows.map(r => `<tr><td>${escapeHtml(r.motivo)}</td><td>${escapeHtml(r.identificador || r.cpf || r.celular || '')}</td><td>${escapeHtml(r.nome)}</td><td>${escapeHtml(r.curso)}</td><td>${escapeHtml(r.consultor || r.consultor_comercial)}</td><td>${formatDate(r.data_inscricao)}</td><td>${formatDate(r.data_upload)}</td><td>${escapeHtml(r.origem)}</td><td>${escapeHtml(r.status)}</td></tr>`).join('')}</tbody></table>`;
    } catch (_) { box.innerHTML = '<div class="empty-state error">Erro ao carregar registros. <button class="btn btn-sm btn-outline-primary" id="retryQualityDetails">Tentar novamente</button></div>'; $('#retryQualityDetails')?.addEventListener('click', () => loadQualidadeDetalhes(tipo)); }
  }

  async function loadImportacoes() {
    const box = $('#importsPanel');
    box.innerHTML = '<div class="empty-state">Carregando histórico...</div>';
    try {
      const { data } = await apiGet('importacoes', { limit: 20 });
      const hist = data.items || [];
      if (!hist.length) { box.innerHTML = `<div class="empty-state">${escapeHtml(data.message || 'Sem histórico de importações para os filtros ativos.')}</div>`; return; }
      box.classList.remove('empty-state');
      box.innerHTML = `<div class="mini-table"><table><thead><tr><th>ID</th><th>Arquivo</th><th>Usuário</th><th>Upload</th><th>Status</th><th>Recebidos</th><th>Válidos</th><th>Rejeitados</th><th>Job</th></tr></thead><tbody>${hist.map(r => `<tr><td>${escapeHtml(r.id_importacao)}</td><td>${escapeHtml(r.nome_arquivo)}</td><td>${escapeHtml(r.usuario)}</td><td>${formatDate(r.dt_upload)}</td><td>${escapeHtml(r.status)}</td><td>${formatValue(r.total_recebido)}</td><td>${formatValue(r.total_valido)}</td><td>${formatValue(r.total_rejeitado)}</td><td>${escapeHtml(r.job_id)}</td></tr>`).join('')}</tbody></table></div>`;
    } catch (_) { box.innerHTML = '<div class="empty-state error">Erro ao carregar importações. <button class="btn btn-sm btn-outline-primary" id="retryImportacoes">Tentar novamente</button></div>'; $('#retryImportacoes')?.addEventListener('click', loadImportacoes); }
  }

  function loadAll(force = false) {
    const extra = force ? { force_refresh: '1' } : {};
    if (force) state.filters.force_refresh = '1'; else delete state.filters.force_refresh;
    syncUrlAndStorage();
    Promise.allSettled([loadResumo(), loadFunil(), loadEvolucao(), loadRankings(), loadProdutividade(), loadFila(), loadQualidade(), loadImportacoes()]).finally(() => {
      delete state.filters.force_refresh;
      syncUrlAndStorage();
    });
  }

  function debounce(fn, ms) {
    let t;
    return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
  }

  document.addEventListener('DOMContentLoaded', async () => {
    restoreFilters();
    renderChips();
    renderKpiSkeleton();
    initTables();
    await loadOptions();
    $('#filtersForm').addEventListener('submit', ev => { ev.preventDefault(); const values = readFilters(); if (!values) return; state.filters = values; syncUrlAndStorage(); loadAll(); });
    $('#btnClearFilters').addEventListener('click', () => { $('#filtersForm').reset(); state.filters = {}; syncUrlAndStorage(); loadAll(); });
    $('#btnRefresh').addEventListener('click', () => loadAll(true));
    $('#qualityGrid').addEventListener('click', ev => { const detail = ev.target.closest('.quality-detail'); const exp = ev.target.closest('.quality-export'); if (detail) loadQualidadeDetalhes(detail.dataset.quality); if (exp) downloadGestao('qualidade/exportar', { tipo: exp.dataset.quality }, exp, `qualidade_${exp.dataset.quality}.csv`); });
    $('#btnExportImportacoes')?.addEventListener('click', ev => downloadGestao('importacoes/exportar', {}, ev.currentTarget, 'historico_importacoes.csv'));
    $('#btnExportFila')?.addEventListener('click', ev => downloadGestao('fila/exportar', {}, ev.currentTarget, 'fila_operacional.csv'));
    $('#granularity').addEventListener('change', loadEvolucao);
    $('[name="busca"]').addEventListener('input', debounce(() => { const values = readFilters(); if (!values) return; state.filters = values; syncUrlAndStorage(); loadAll(); }, 700));
    loadAll();
  });
})();
