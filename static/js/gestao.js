(() => {
  'use strict';

  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
  const esc = (v) => String(v ?? '').replace(/[&<>'"]/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));
  const num = (v) => Number(v || 0);
  const fmtNum = (v) => num(v).toLocaleString('pt-BR');
  const pct = (v) => `${Number(v || 0).toFixed(1).replace('.0', '')}%`;
  const fmtDate = (v) => {
    if (!v) return '—';
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? esc(v) : d.toLocaleString('pt-BR', {dateStyle:'short', timeStyle:'short'});
  };

  let team = [];
  let breakdown = [];
  let imports = [];
  let summary = {};
  let funnel = {};
  let byBusiness = [];
  let byCampaign = [];
  let optionsLoaded = false;
  let lastLoadedAt = 0;
  const CACHE_MS = 30000;

  async function fetchJson(url, options = {}) {
    const response = await fetch(url, {credentials:'same-origin', headers:{Accept:'application/json', ...(options.headers || {})}, ...options});
    const body = await response.json().catch(() => ({}));
    if (!response.ok || body?.ok === false || body?.success === false) {
      const message = typeof body?.error === 'string' ? body.error : body?.error?.message;
      throw new Error(message || body?.message || `Falha HTTP ${response.status}`);
    }
    return body.data || body;
  }

  function showError(error) {
    const box = $('#globalError');
    if (!box) return;
    box.textContent = error?.message || 'Falha ao carregar os dados da gestão.';
    box.classList.remove('d-none');
  }
  function clearError() { $('#globalError')?.classList.add('d-none'); }
  function toast(message, type = 'success') {
    const region = $('#toastRegion');
    if (!region) return;
    const el = document.createElement('div');
    el.className = `alert alert-${type} shadow`;
    el.textContent = message;
    region.appendChild(el);
    setTimeout(() => el.remove(), 3800);
  }
  function setSync(state) {
    $('#syncDot')?.classList.toggle('loading', state === 'loading');
    const text = $('#syncText');
    if (text) text.textContent = state === 'loading' ? 'Atualizando' : state === 'error' ? 'Falha' : 'Atualizado';
  }
  function validConsultant(value) {
    const normalized = String(value || '').replaceAll('\\', '').trim().toUpperCase();
    return !['', 'N', 'NULL', 'N/A', 'NA', 'NONE', 'UNDEFINED', '-'].includes(normalized);
  }

  function switchPage(name) {
    $$('.page').forEach((page) => page.classList.toggle('active', page.id === `page-${name}`));
    $$('.side-nav button').forEach((button) => button.classList.toggle('active', button.dataset.page === name));
    history.replaceState(null, '', `#${name}`);
    if (name === 'imports') loadImports();
    if (name === 'quality') loadQuality();
    if (name === 'team') renderTeam();
    if (name === 'performance') renderPerformance();
  }

  function filterParams() {
    const values = {
      data_ini: $('#filterDateStart')?.value,
      data_fim: $('#filterDateEnd')?.value,
      tipo_negocio: $('#filterBusiness')?.value,
      tipo_disparo: $('#filterDispatchType')?.value,
      canal: $('#filterChannel')?.value,
      unidade: $('#filterUnit')?.value,
      campanha: $('#filterCampaign')?.value,
      curso: $('#filterCourse')?.value,
    };
    const params = new URLSearchParams();
    Object.entries(values).forEach(([key, value]) => {
      const clean = String(value || '').trim();
      if (clean) params.set(key, clean);
    });
    return params;
  }

  function renderFilterSummary() {
    const labels = [
      ['Período inicial', $('#filterDateStart')?.value], ['Período final', $('#filterDateEnd')?.value],
      ['Negócio', $('#filterBusiness')?.value], ['Disparo', $('#filterDispatchType')?.value],
      ['Canal', $('#filterChannel')?.value], ['Unidade', $('#filterUnit')?.value],
      ['Campanha', $('#filterCampaign')?.value], ['Curso', $('#filterCourse')?.value],
    ].filter(([, value]) => String(value || '').trim());
    $('#activeFilterSummary').textContent = labels.length ? labels.map(([label, value]) => `${label}: ${value}`).join(' · ') : 'Exibindo toda a operação.';
  }

  function populateSelect(selector, values) {
    const select = $(selector);
    if (!select || !Array.isArray(values)) return;
    const first = select.options[0]?.outerHTML || '<option value="">Todos</option>';
    const current = select.value;
    const clean = [...new Set(values.filter(Boolean).map(String))].sort((a, b) => a.localeCompare(b, 'pt-BR'));
    select.innerHTML = first + clean.map((value) => `<option value="${esc(value)}">${esc(value)}</option>`).join('');
    if (clean.includes(current)) select.value = current;
  }

  function renderExecutiveKpis() {
    const s = summary || {};
    const items = [
      ['Total disparado', s.total_disparado, 'Registros no recorte atual', 'blue'],
      ['Hoje', s.disparado_hoje, 'Disparos realizados hoje', 'cyan'],
      ['Esta semana', s.disparado_semana, 'Desde segunda-feira', 'violet'],
      ['Este mês', s.disparado_mes, 'Disparos no mês atual', 'indigo'],
      ['Retornos', s.retornos, `Taxa ${pct(s.taxa_retorno_pct)}`, 'amber'],
      ['Positivos', s.positivos, 'Leads interessados ou convertidos', 'green'],
      ['Negociação', s.em_negociacao, 'Leads em processo comercial', 'slate'],
      ['Matrículas', s.matriculas, `Conversão ${pct(s.taxa_matricula_pct)}`, 'emerald'],
    ];
    const html = items.map(([label, value, help, tone]) => `<article class="executive-card tone-${tone}"><span>${label}</span><strong>${fmtNum(value)}</strong><small>${help}</small></article>`).join('');
    $('#executiveKpis').innerHTML = html;
    $('#teamSummaryCards').innerHTML = html;

    const weekVar = s.variacao_semana_pct;
    const monthVar = s.variacao_mes_pct;
    $('#weekComparison').textContent = weekVar == null ? 'Sem base' : `${weekVar >= 0 ? '+' : ''}${pct(weekVar)}`;
    $('#weekComparison').className = weekVar == null ? '' : weekVar >= 0 ? 'positive' : 'negative';
    $('#weekComparisonHelp').textContent = `${fmtNum(s.disparado_semana)} atual · ${fmtNum(s.semana_anterior)} anterior`;
    $('#monthComparison').textContent = monthVar == null ? 'Sem base' : `${monthVar >= 0 ? '+' : ''}${pct(monthVar)}`;
    $('#monthComparison').className = monthVar == null ? '' : monthVar >= 0 ? 'positive' : 'negative';
    $('#monthComparisonHelp').textContent = `${fmtNum(s.disparado_mes)} atual · ${fmtNum(s.mes_anterior)} anterior`;
    $('#returnRate').textContent = pct(s.taxa_retorno_pct);
    $('#conversionRate').textContent = pct(s.taxa_matricula_pct);
  }

  function sortedTeam() {
    const sort = $('#teamSort')?.value || 'disparado_semana';
    const query = ($('#teamSearch')?.value || '').trim().toLowerCase();
    return team.filter((row) => validConsultant(row.consultor_disparo))
      .filter((row) => !query || String(row.consultor_disparo || '').toLowerCase().includes(query))
      .sort((a, b) => num(b[sort]) - num(a[sort]));
  }

  function renderTeam() {
    const data = sortedTeam();
    const period = $('#teamPeriod')?.value || 'disparado_semana';
    $('#teamCount').textContent = `${data.length} consultor(es)`;
    const maxValue = Math.max(1, ...data.map((row) => num(row[period])));
    const maxWeek = Math.max(1, ...data.map((row) => num(row.disparado_semana)));

    $('#teamRanking').innerHTML = data.slice(0, 8).map((row, index) => `<div class="ranking-row rank-${index + 1}"><div class="rank-number">${index + 1}</div><div class="rank-main"><strong>${esc(row.consultor_disparo)}</strong><small>${fmtNum(row.disparado_semana)} na semana · ${fmtNum(row.total_disparado)} no total</small><div class="rank-bar"><i style="width:${Math.max(4, num(row.disparado_semana) / maxWeek * 100)}%"></i></div></div><div class="rank-result"><strong>${fmtNum(row.matriculas)}</strong><small>matrículas</small></div></div>`).join('') || '<div class="empty-state">Nenhum consultor encontrado.</div>';

    $('#teamCards').innerHTML = data.map((row) => {
      const progress = Math.min(100, num(row[period]) / maxValue * 100);
      return `<article class="team-card"><div class="team-card-head"><div><span>Consultor</span><h3>${esc(row.consultor_disparo)}</h3></div><strong>${fmtNum(row[period])}<small> disparos</small></strong></div><div class="team-progress"><div><i style="width:${progress}%"></i></div><small>${fmtNum(row.disparado_semana)} nesta semana</small></div><div class="team-metrics"><div><span>Total</span><strong>${fmtNum(row.total_disparado)}</strong></div><div><span>Retorno</span><strong>${pct(row.taxa_retorno_pct)}</strong></div><div><span>Conversão</span><strong>${pct(row.taxa_matricula_pct)}</strong></div><div><span>Matrículas</span><strong>${fmtNum(row.matriculas)}</strong></div></div></article>`;
    }).join('') || '<div class="empty-state">Nenhum consultor encontrado.</div>';

    $('#teamTableBody').innerHTML = data.map((row) => `<tr><td><strong>${esc(row.consultor_disparo)}</strong></td><td>${fmtNum(row.total_disparado)}</td><td>${fmtNum(row.disparado_hoje)}</td><td>${fmtNum(row.disparado_semana)}</td><td>${fmtNum(row.disparado_mes)}</td><td>${fmtNum(row.retornos)}</td><td>${fmtNum(row.positivos)}</td><td>${fmtNum(row.em_negociacao)}</td><td>${fmtNum(row.negativos)}</td><td><strong>${fmtNum(row.matriculas)}</strong></td><td>${pct(row.taxa_retorno_pct)}</td><td>${pct(row.taxa_matricula_pct)}</td><td>${fmtDate(row.ultimo_disparo)}</td></tr>`).join('') || '<tr><td colspan="13" class="empty-cell">Nenhum consultor encontrado.</td></tr>';

    const consultants = new Set(data.map((row) => String(row.consultor_disparo)));
    $('#teamBreakdownBody').innerHTML = breakdown.filter((row) => consultants.has(String(row.consultor_disparo))).map((row) => `<tr><td><strong>${esc(row.consultor_disparo)}</strong></td><td>${esc(row.tipo_disparo)}</td><td>${esc(row.campanha)}</td><td>${esc(row.canal)}</td><td>${esc(row.peca_disparo)}</td><td>${fmtNum(row.total)}</td><td>${fmtNum(row.semana)}</td><td>${fmtNum(row.matriculas)}</td><td>${fmtDate(row.ultimo_disparo)}</td></tr>`).join('') || '<tr><td colspan="9" class="empty-cell">Nenhum detalhamento encontrado.</td></tr>';
  }

  function renderAlerts() {
    const alerts = [];
    if (!num(summary.disparado_semana)) alerts.push(['warning', 'Nenhum disparo nesta semana', 'Confira data_disparo e consultor_disparo.']);
    if (summary.variacao_semana_pct != null && summary.variacao_semana_pct < -10) alerts.push(['danger', `Queda semanal de ${pct(Math.abs(summary.variacao_semana_pct))}`, 'Analise consultores, campanhas e tipos de negócio.']);
    if (num(summary.positivos) > 0 && num(summary.matriculas) / num(summary.positivos) < 0.05) alerts.push(['warning', 'Muitos positivos com pouca conversão', 'Priorize o acompanhamento dos leads interessados.']);
    const noCampaign = byCampaign.find((row) => String(row.nome).toUpperCase() === 'SEM CAMPANHA');
    if (noCampaign && num(noCampaign.disparos) > 0) alerts.push(['info', `${fmtNum(noCampaign.disparos)} disparos sem campanha`, 'Corrija a origem para melhorar a leitura dos resultados.']);
    if (!alerts.length) alerts.push(['success', 'Operação sem alertas críticos', 'Os principais indicadores estão dentro do esperado.']);
    $('#managementAlerts').innerHTML = alerts.map(([type, title, text]) => `<div class="management-alert alert-${type}"><i></i><div><strong>${title}</strong><p>${text}</p></div></div>`).join('');
  }

  function renderFunnel() {
    const stages = [
      ['Disparados', funnel.disparados], ['Retornos', funnel.retornos], ['Positivos', funnel.positivos],
      ['Negociação', funnel.em_negociacao], ['Matrículas', funnel.matriculas], ['Negativos', funnel.negativos],
    ];
    const max = Math.max(1, ...stages.map(([, value]) => num(value)));
    $('#managementFunnel').innerHTML = stages.map(([label, value], index) => {
      const width = Math.max(18, num(value) / max * 100);
      const previous = index ? num(stages[index - 1][1]) : 0;
      const rate = previous ? num(value) / previous * 100 : 100;
      return `<article class="funnel-stage"><div class="funnel-bar" style="width:${width}%"><span>${esc(label)}</span><strong>${fmtNum(value)}</strong></div><small>${index ? `${pct(rate)} da etapa anterior` : 'Base do funil'}</small></article>`;
    }).join('');
  }

  function performanceRows(rows) {
    return rows.map((row) => {
      const conversion = num(row.disparos) ? num(row.matriculas) / num(row.disparos) * 100 : 0;
      return `<tr><td><strong>${esc(row.nome)}</strong></td><td>${fmtNum(row.disparos)}</td><td>${fmtNum(row.retornos)}</td><td>${fmtNum(row.positivos)}</td><td>${fmtNum(row.matriculas)}</td><td>${pct(conversion)}</td></tr>`;
    }).join('') || '<tr><td colspan="6" class="empty-cell">Nenhum dado encontrado.</td></tr>';
  }
  function renderPerformance() {
    $('#businessTableBody').innerHTML = performanceRows(byBusiness);
    $('#campaignTableBody').innerHTML = performanceRows(byCampaign);
  }

  async function loadCore(force = false) {
    if (!force && Date.now() - lastLoadedAt < CACHE_MS && team.length) return;
    clearError(); setSync('loading');
    try {
      const params = filterParams();
      const data = await fetchJson(`/api/gestao/operacional/consultores${params.toString() ? `?${params}` : ''}`);
      team = (data.items || []).filter((row) => validConsultant(row.consultor_disparo));
      breakdown = (data.breakdown || []).filter((row) => validConsultant(row.consultor_disparo));
      summary = data.summary || {};
      funnel = data.funnel || {};
      byBusiness = data.by_business || [];
      byCampaign = data.by_campaign || [];
      if (!optionsLoaded && data.options) {
        populateSelect('#filterBusiness', data.options.tipos_negocio || []);
        populateSelect('#filterDispatchType', data.options.tipos_disparo || []);
        populateSelect('#filterChannel', data.options.canais || []);
        populateSelect('#filterUnit', data.options.unidades || []);
        optionsLoaded = true;
      }
      lastLoadedAt = Date.now();
      renderFilterSummary(); renderExecutiveKpis(); renderTeam(); renderAlerts(); renderFunnel(); renderPerformance();
      $('#lastUpdated').textContent = `Atualizado em ${new Date().toLocaleString('pt-BR')}`;
      $('#dataHealth').textContent = 'Dados disponíveis'; setSync('ok');
    } catch (error) {
      setSync('error'); $('#dataHealth').textContent = 'Falha na atualização'; showError(error);
    }
  }

  async function exportTeam() {
    const button = $('#btnExportTeamProductivity');
    const month = $('#teamExportMonth')?.value;
    if (!month) return toast('Selecione o mês da exportação.', 'warning');
    const original = button.textContent; button.disabled = true; button.textContent = 'Gerando planilha...';
    try {
      const response = await fetch(`/api/gestao/operacional/consultores/exportar?mes=${encodeURIComponent(month)}`, {credentials:'same-origin'});
      if (!response.ok) { const body = await response.json().catch(() => ({})); throw new Error(body.error || body.message || `Falha HTTP ${response.status}`); }
      const blob = await response.blob(); const url = URL.createObjectURL(blob); const anchor = document.createElement('a');
      anchor.href = url; anchor.download = `produtividade_equipe_${month}.xlsx`; document.body.appendChild(anchor); anchor.click(); anchor.remove(); URL.revokeObjectURL(url);
      toast('Planilha gerada com sucesso.');
    } catch (error) { toast(error.message || 'Falha ao exportar produtividade.', 'danger'); }
    finally { button.disabled = false; button.textContent = original; }
  }

  async function loadQuality() {
    const body = $('#qualityTableBody'); body.innerHTML = '<tr><td colspan="5" class="empty-cell">Analisando...</td></tr>'; $('#qualityError')?.classList.add('d-none');
    try {
      const data = await fetchJson('/api/gestao/qualidade-dados/inconsistencias?amostras=5');
      $('#qualitySummary').innerHTML = [['Inconsistências', data.total_inconsistencias], ['Campos analisados', data.campos_analisados], ['Campos com problema', data.campos_com_problema], ['Fonte', data.fonte]].map(([label, value]) => `<div><span>${label}</span><strong>${typeof value === 'number' ? fmtNum(value) : esc(value)}</strong></div>`).join('');
      body.innerHTML = (data.items || []).map((item) => { const samples = (item.amostras || []).map((sample) => [sample.nome, sample.cpf, sample.celular, sample.email, sample.consultor_disparo].filter(Boolean).join(' · ')).join('<br>'); return `<tr><td><strong>${esc(item.campo)}</strong></td><td>${esc(item.problema)}</td><td>${fmtNum(item.quantidade)}</td><td>${esc(item.exemplo || '—')}</td><td>${samples || '—'}</td></tr>`; }).join('') || '<tr><td colspan="5" class="empty-cell">Nenhuma inconsistência encontrada.</td></tr>';
    } catch (error) { const box = $('#qualityError'); box.textContent = error.message; box.classList.remove('d-none'); body.innerHTML = '<tr><td colspan="5" class="empty-cell">Falha ao analisar os dados.</td></tr>'; }
  }

  async function loadImports(force = false) {
    if (!force && imports.length) return renderImports();
    try { const data = await fetchJson('/api/gestao/logs/importacoes?limit=100&offset=0'); imports = data.data || data.items || []; renderImports(); } catch (error) { showError(error); }
  }
  function renderImports() {
    const totals = imports.reduce((acc, row) => { acc.total += 1; acc.recebidas += num(row.linhas_recebidas); acc.validas += num(row.linhas_validas); acc.rejeitadas += num(row.linhas_rejeitadas); acc.atualizadas += num(row.linhas_atualizadas); if (String(row.status || '').includes('CONCLUIDO')) acc.concluidas += 1; return acc; }, {total:0, recebidas:0, validas:0, rejeitadas:0, atualizadas:0, concluidas:0});
    $('#importSummary').innerHTML = [['Importações', totals.total], ['Concluídas', totals.concluidas], ['Linhas recebidas', totals.recebidas], ['Linhas válidas', totals.validas], ['Atualizadas', totals.atualizadas], ['Rejeitadas', totals.rejeitadas]].map(([k,v]) => `<div><span>${k}</span><strong>${fmtNum(v)}</strong></div>`).join('');
    $('#importsTableBody').innerHTML = imports.map((row) => `<tr><td><strong>${esc(row.nome_arquivo || '—')}</strong></td><td>${esc(row.usuario || '—')}</td><td>${esc(row.status || '—')}</td><td>${esc(row.etapa || '—')}</td><td>${fmtNum(row.linhas_recebidas)}</td><td>${fmtNum(row.linhas_validas)}</td><td>${fmtNum(row.linhas_inseridas)}</td><td>${fmtNum(row.linhas_atualizadas)}</td><td>${fmtNum(row.linhas_rejeitadas)}</td><td>${esc(row.mensagem || '—')}</td><td>${fmtDate(row.criado_em)}</td></tr>`).join('') || '<tr><td colspan="11" class="empty-cell">Nenhuma importação.</td></tr>';
  }
  async function uploadFile(event) {
    event.preventDefault(); const form = event.currentTarget; const button = form.querySelector('button'); button.disabled = true; button.textContent = 'Importando...';
    try { const response = await fetch('/api/upload', {method:'POST', body:new FormData(form), credentials:'same-origin', headers:{Accept:'application/json'}}); const body = await response.json().catch(() => ({})); if (!response.ok || body?.ok === false) throw new Error(body?.error?.message || body?.error || 'Falha ao importar.'); form.reset(); $('#managementFileName').textContent = 'Nenhum arquivo selecionado'; imports = []; lastLoadedAt = 0; await Promise.all([loadImports(true), loadCore(true)]); toast('Planilha importada com sucesso.'); }
    catch (error) { $('#uploadResult').innerHTML = `<div class="alert alert-danger">${esc(error.message)}</div>`; }
    finally { button.disabled = false; button.textContent = 'Importar planilha'; }
  }
  async function searchLead() {
    const query = ($('#leadSearch')?.value || '').trim(); if (!query) return toast('Digite um nome, CPF ou telefone.', 'warning'); $('#leadResults').innerHTML = '<div class="empty-state">Buscando...</div>';
    try { const data = await fetchJson(`/api/gestao/leads/buscar?q=${encodeURIComponent(query)}`); const items = data.items || data.data || []; $('#leadResults').innerHTML = items.map((row) => `<article class="lead-card"><div><span>${esc(row.status_atendimento || row.status || 'Lead')}</span><h3>${esc(row.nome || 'Sem nome')}</h3><p>${esc(row.curso || '—')} · ${esc(row.polo || row.unidade || '—')}</p></div><dl><div><dt>CPF</dt><dd>${esc(row.cpf || '—')}</dd></div><div><dt>Celular</dt><dd>${esc(row.celular || '—')}</dd></div><div><dt>Negócio</dt><dd>${esc(row.tipo_negocio || '—')}</dd></div><div><dt>Consultor</dt><dd>${esc(row.consultor_disparo || '—')}</dd></div></dl></article>`).join('') || '<div class="empty-state">Nenhum lead encontrado.</div>'; }
    catch (error) { $('#leadResults').innerHTML = `<div class="empty-state error">${esc(error.message)}</div>`; }
  }

  function clearFilters() {
    ['#filterDateStart','#filterDateEnd','#filterBusiness','#filterDispatchType','#filterChannel','#filterUnit','#filterCampaign','#filterCourse'].forEach((selector) => { const el = $(selector); if (el) el.value = ''; });
    lastLoadedAt = 0; loadCore(true);
  }
  function bind() {
    $$('.side-nav button').forEach((button) => button.addEventListener('click', () => switchPage(button.dataset.page)));
    $$('[data-go]').forEach((button) => button.addEventListener('click', () => switchPage(button.dataset.go)));
    $('#btnRefreshAll')?.addEventListener('click', () => { imports = []; lastLoadedAt = 0; Promise.all([loadCore(true), loadImports(true)]); });
    $('#btnApplyFilters')?.addEventListener('click', () => { lastLoadedAt = 0; loadCore(true); });
    $('#btnClearFilters')?.addEventListener('click', clearFilters);
    $('#teamSearch')?.addEventListener('input', renderTeam); $('#teamSort')?.addEventListener('change', renderTeam); $('#teamPeriod')?.addEventListener('change', renderTeam);
    $('#btnExportTeamProductivity')?.addEventListener('click', exportTeam); $('#btnReloadQuality')?.addEventListener('click', loadQuality);
    $('#btnReloadImports')?.addEventListener('click', () => { imports = []; loadImports(true); }); $('#uploadForm')?.addEventListener('submit', uploadFile);
    $('#managementFile')?.addEventListener('change', (event) => { $('#managementFileName').textContent = event.target.files?.[0]?.name || 'Nenhum arquivo selecionado'; });
    $('#btnLeadSearch')?.addEventListener('click', searchLead); $('#leadSearch')?.addEventListener('keydown', (event) => { if (event.key === 'Enter') searchLead(); });
  }

  document.addEventListener('DOMContentLoaded', () => {
    bind(); const now = new Date(); const month = $('#teamExportMonth'); if (month) month.value = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const page = (location.hash || '#overview').slice(1); switchPage($(`#page-${page}`) ? page : 'overview'); loadCore();
  });
})();