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

  let dashboard = {};
  let team = [];
  let lots = [];
  let imports = [];
  let coreController = null;
  let importsController = null;
  let auditController = null;
  let lastCoreLoadedAt = 0;
  const CORE_CACHE_MS = 30000;

  async function fetchJson(url, options = {}) {
    const response = await fetch(url, {
      credentials:'same-origin',
      headers:{Accept:'application/json', ...(options.headers || {})},
      ...options,
    });
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

  function statusBadge(value) {
    const status = String(value || 'SEM STATUS').toUpperCase();
    return `<span class="status-badge status-${esc(status.toLowerCase().replace(/[^a-z0-9]+/g, '-'))}">${esc(status)}</span>`;
  }

  function loadingCards(container, quantity = 6) {
    const el = $(container);
    if (!el) return;
    el.innerHTML = Array.from({length:quantity}, () => '<article class="executive-card skeleton">Carregando</article>').join('');
  }

  function loadingRows(container, colspan, quantity = 5) {
    const el = $(container);
    if (!el) return;
    el.innerHTML = Array.from({length:quantity}, () => `<tr><td colspan="${colspan}"><div class="skeleton" style="height:20px;border-radius:7px">.</div></td></tr>`).join('');
  }

  function switchPage(name) {
    $$('.page').forEach((page) => page.classList.toggle('active', page.id === `page-${name}`));
    $$('.side-nav button').forEach((button) => button.classList.toggle('active', button.dataset.page === name));
    history.replaceState(null, '', `#${name}`);
    if (name === 'imports') loadImports();
    if (name === 'audit') loadAudit();
    if (name === 'team') renderTeam();
    if (name === 'lots') renderLots();
  }

  function renderExecutiveKpis() {
    const totalDisponiveis = num(dashboard.leads_novos_disponiveis) + num(dashboard.leads_redisparo_disponiveis);
    const activeLots = num(dashboard.lotes_abertos) + num(dashboard.lotes_em_andamento);
    const items = [
      ['Leads em operação', dashboard.leads_em_lotes, 'Base atualmente distribuída', 'blue'],
      ['Pendentes', dashboard.leads_pendentes, 'Ainda não trabalhados', 'amber'],
      ['Em atendimento', dashboard.leads_em_atendimento, 'Em tratamento pela equipe', 'cyan'],
      ['Retornos', dashboard.retornos, `Taxa ${pct(dashboard.taxa_retorno_pct)}`, 'violet'],
      ['Positivos', dashboard.positivos, 'Oportunidades qualificadas', 'green'],
      ['Matrículas', dashboard.matriculas, `Conversão ${pct(dashboard.taxa_matricula_pct)}`, 'emerald'],
      ['Lotes ativos', activeLots, 'Abertos ou em andamento', 'indigo'],
      ['Leads disponíveis', totalDisponiveis, 'Prontos para novos lotes', 'slate'],
    ];
    $('#executiveKpis').innerHTML = items.map(([label, value, help, tone]) => `
      <article class="executive-card tone-${tone}"><span>${label}</span><strong>${fmtNum(value)}</strong><small>${help}</small></article>
    `).join('');
  }

  function sortedTeam(source = team) {
    const sort = $('#teamSort')?.value || 'matriculas';
    const query = ($('#teamSearch')?.value || '').trim().toLowerCase();
    return [...source]
      .filter((r) => !query || String(r.consultor_disparo || '').toLowerCase().includes(query))
      .sort((a, b) => num(b[sort]) - num(a[sort]));
  }

  function renderTeam() {
    const data = sortedTeam();
    $('#teamCount').textContent = `${data.length} consultor(es)`;
    const maxMat = Math.max(1, ...data.map((r) => num(r.matriculas)));
    $('#teamRanking').innerHTML = data.slice(0, 6).map((r, index) => `
      <div class="ranking-row"><div class="rank-number">${index + 1}</div><div class="rank-main"><strong>${esc(r.consultor_disparo || 'Sem consultor')}</strong><small>${fmtNum(r.trabalhados)} trabalhados · ${fmtNum(r.pendentes)} pendentes</small><div class="rank-bar"><i style="width:${Math.max(4, num(r.matriculas) / maxMat * 100)}%"></i></div></div><div class="rank-result"><strong>${fmtNum(r.matriculas)}</strong><small>matrículas</small></div></div>
    `).join('') || '<div class="empty-state">Nenhum consultor encontrado.</div>';

    $('#teamCards').innerHTML = data.map((r) => {
      const total = Math.max(1, num(r.total_leads_em_lote));
      const progress = Math.min(100, num(r.percentual_trabalhado) || num(r.trabalhados) / total * 100);
      return `<article class="team-card"><div class="team-card-head"><div><span>Consultor</span><h3>${esc(r.consultor_disparo || 'Sem consultor')}</h3></div><strong>${fmtNum(r.matriculas)}<small> matrículas</small></strong></div><div class="team-progress"><div><i style="width:${progress}%"></i></div><small>${pct(progress)} trabalhado</small></div><div class="team-metrics"><div><span>Leads</span><strong>${fmtNum(r.total_leads_em_lote)}</strong></div><div><span>Pendentes</span><strong>${fmtNum(r.pendentes)}</strong></div><div><span>Retornos</span><strong>${fmtNum(r.retornos)}</strong></div><div><span>Conversão</span><strong>${pct(r.taxa_matricula_pct)}</strong></div></div></article>`;
    }).join('') || '<div class="empty-state">Nenhum consultor encontrado.</div>';

    $('#teamTableBody').innerHTML = data.map((r) => `<tr><td><strong>${esc(r.consultor_disparo || 'Sem consultor')}</strong></td><td>${fmtNum(r.total_leads_em_lote)}</td><td>${fmtNum(r.trabalhados)}</td><td>${fmtNum(r.pendentes)}</td><td>${fmtNum(r.em_atendimento)}</td><td>${fmtNum(r.retornos)}</td><td>${fmtNum(r.positivos)}</td><td>${fmtNum(r.negativos)}</td><td><strong>${fmtNum(r.matriculas)}</strong></td><td>${pct(r.taxa_retorno_pct)}</td><td>${pct(r.taxa_matricula_pct)}</td><td>${fmtDate(r.ultima_movimentacao)}</td></tr>`).join('') || '<tr><td colspan="12" class="empty-cell">Nenhum consultor encontrado.</td></tr>';
  }

  function renderManagementAlerts() {
    const alerts = [];
    const pending = num(dashboard.leads_pendentes);
    const activeLots = num(dashboard.lotes_abertos) + num(dashboard.lotes_em_andamento);
    if (pending > 0) alerts.push(['warning', `${fmtNum(pending)} leads pendentes`, 'Priorize consultores com maior fila sem movimentação.']);
    if (num(dashboard.lotes_abertos) > 0) alerts.push(['info', `${fmtNum(dashboard.lotes_abertos)} lote(s) abertos`, 'Confirme exportação ou início da operação.']);
    if (num(dashboard.lotes_em_andamento) > 0) alerts.push(['primary', `${fmtNum(dashboard.lotes_em_andamento)} lote(s) em andamento`, 'Acompanhe retorno e ritmo da equipe.']);
    if (num(dashboard.taxa_matricula_pct) < 2 && num(dashboard.leads_em_lotes) > 0) alerts.push(['danger', `Conversão em ${pct(dashboard.taxa_matricula_pct)}`, 'Revise campanha, origem, curso e qualidade dos leads.']);
    if (!activeLots && num(dashboard.leads_novos_disponiveis) > 0) alerts.push(['warning', 'Há leads disponíveis sem lote ativo', 'Crie o próximo lote para manter a operação em movimento.']);
    if (!alerts.length) alerts.push(['success', 'Operação sem alertas críticos', 'Os indicadores estão dentro do esperado.']);
    $('#managementAlerts').innerHTML = alerts.map(([type, title, text]) => `<div class="management-alert alert-${type}"><i></i><div><strong>${title}</strong><p>${text}</p></div></div>`).join('');
  }

  function filteredLots() {
    const query = ($('#lotSearch')?.value || '').trim().toLowerCase();
    const status = ($('#lotStatus')?.value || '').toUpperCase();
    return lots.filter((r) => {
      const text = `${r.nome_lote || ''} ${r.campanha || ''} ${r.consultor_disparo || ''}`.toLowerCase();
      return (!query || text.includes(query)) && (!status || String(r.status_lote || '').toUpperCase() === status);
    });
  }

  function lotRow(r, compact = false) {
    const conversion = r.taxa_matricula ?? r.taxa_matricula_pct;
    const returnRate = r.taxa_retorno ?? r.taxa_retorno_pct;
    const cells = `<td><strong>${esc(r.nome_lote || r.lote_id || '—')}</strong></td><td>${esc(r.consultor_disparo || '—')}</td>${compact ? '' : `<td>${esc(r.campanha || '—')}</td><td>${esc(r.tipo_disparo || '—')}</td>`}<td>${statusBadge(r.status_lote)}</td><td>${fmtNum(r.quantidade_leads)}</td><td>${fmtNum(r.total_retorno)}</td><td>${fmtNum(r.total_positivo)}</td>${compact ? '' : `<td>${fmtNum(r.total_negativo)}</td>`}<td><strong>${fmtNum(r.total_matriculas)}</strong></td>${compact ? '' : `<td>${pct(returnRate)}</td>`}<td>${pct(conversion)}</td><td>${compact ? esc(r.proxima_acao || '—') : fmtDate(r.exportado_em)}</td>${compact ? '' : `<td>${esc(r.proxima_acao || '—')}</td>`}`;
    return `<tr>${cells}</tr>`;
  }

  function renderLots() {
    const data = filteredLots();
    $('#lotCount').textContent = `${data.length} lote(s)`;
    $('#overviewLotsBody').innerHTML = lots.filter((r) => ['ABERTO','EM_ANDAMENTO','IMPORTADO'].includes(String(r.status_lote || '').toUpperCase())).slice(0, 8).map((r) => lotRow(r, true)).join('') || '<tr><td colspan="9" class="empty-cell">Nenhum lote ativo.</td></tr>';
    $('#lotsTableBody').innerHTML = data.map((r) => lotRow(r, false)).join('') || '<tr><td colspan="14" class="empty-cell">Nenhum lote encontrado.</td></tr>';
  }

  function setSync(state) {
    const dot = $('#syncDot');
    const text = $('#syncText');
    dot?.classList.toggle('loading', state === 'loading');
    if (text) text.textContent = state === 'loading' ? 'Atualizando' : state === 'error' ? 'Falha' : 'Atualizado';
  }

  async function loadCore(force = false) {
    if (!force && Date.now() - lastCoreLoadedAt < CORE_CACHE_MS && team.length) return;
    coreController?.abort();
    coreController = new AbortController();
    clearError(); setSync('loading');
    loadingCards('#executiveKpis', 8); loadingRows('#overviewLotsBody', 9, 4);
    try {
      const [d, t, l] = await Promise.all([
        fetchJson('/api/gestao/operacional/dashboard', {signal:coreController.signal}),
        fetchJson('/api/gestao/operacional/consultores', {signal:coreController.signal}),
        fetchJson('/api/gestao/operacional/lotes?limit=200', {signal:coreController.signal}),
      ]);
      dashboard = d || {};
      team = t.items || t.data || [];
      lots = l.items || l.data || [];
      lastCoreLoadedAt = Date.now();
      renderExecutiveKpis(); renderTeam(); renderManagementAlerts(); renderLots();
      $('#lastUpdated').textContent = `Atualizado em ${new Date().toLocaleString('pt-BR')}`;
      $('#dataHealth').textContent = 'Dados disponíveis';
      setSync('ok');
    } catch (error) {
      if (error.name === 'AbortError') return;
      setSync('error'); $('#dataHealth').textContent = 'Falha na atualização'; showError(error);
    }
  }

  async function loadImports(force = false) {
    if (!force && imports.length) return renderImports();
    importsController?.abort(); importsController = new AbortController();
    loadingRows('#importsTableBody', 11, 5);
    try {
      const d = await fetchJson('/api/gestao/logs/importacoes?limit=100&offset=0', {signal:importsController.signal});
      imports = d.data || d.items || [];
      renderImports();
    } catch (error) { if (error.name !== 'AbortError') showError(error); }
  }

  function renderImports() {
    const totals = imports.reduce((a, r) => {
      a.total += 1; a.recebidas += num(r.linhas_recebidas); a.validas += num(r.linhas_validas); a.rejeitadas += num(r.linhas_rejeitadas); a.atualizadas += num(r.linhas_atualizadas); if (String(r.status || '').includes('CONCLUIDO')) a.concluidas += 1; return a;
    }, {total:0, recebidas:0, validas:0, rejeitadas:0, atualizadas:0, concluidas:0});
    const quality = totals.recebidas ? ((totals.validas / totals.recebidas) * 100) : 0;
    $('#importSummary').innerHTML = [['Importações', totals.total], ['Concluídas', totals.concluidas], ['Linhas recebidas', totals.recebidas], ['Linhas válidas', totals.validas], ['Atualizadas', totals.atualizadas], ['Qualidade', `${quality.toFixed(1)}%`]].map(([k,v]) => `<div><span>${k}</span><strong>${typeof v === 'number' ? fmtNum(v) : esc(v)}</strong></div>`).join('');
    $('#importsTableBody').innerHTML = imports.map((r) => `<tr><td><strong>${esc(r.nome_arquivo || '—')}</strong></td><td>${esc(r.usuario || '—')}</td><td>${statusBadge(r.status)}</td><td>${esc(r.etapa || '—')}</td><td>${fmtNum(r.linhas_recebidas)}</td><td>${fmtNum(r.linhas_validas)}</td><td>${fmtNum(r.linhas_inseridas)}</td><td>${fmtNum(r.linhas_atualizadas)}</td><td>${fmtNum(r.linhas_rejeitadas)}</td><td title="${esc(r.mensagem || '')}">${esc(r.mensagem || '—')}</td><td>${fmtDate(r.criado_em)}</td></tr>`).join('') || '<tr><td colspan="11" class="empty-cell">Nenhuma importação.</td></tr>';
  }

  async function uploadFile(event) {
    event.preventDefault();
    const form = event.currentTarget;
    const button = form.querySelector('button');
    button.disabled = true; button.textContent = 'Importando...';
    try {
      const response = await fetch('/api/upload', {method:'POST', body:new FormData(form), credentials:'same-origin', headers:{Accept:'application/json'}});
      const body = await response.json().catch(() => ({}));
      if (!response.ok || body?.ok === false) throw new Error(body?.error?.message || body?.error || 'Falha ao importar.');
      const report = body.report || {};
      $('#uploadResult').innerHTML = `<div class="alert alert-success"><strong>Importação concluída.</strong><br>Recebidas: ${fmtNum(report.linhas_recebidas)} · Processadas: ${fmtNum(report.linhas_processadas)} · Rejeitadas: ${fmtNum(report.linhas_rejeitadas)}</div>`;
      form.reset(); $('#managementFileName').textContent = 'Nenhum arquivo selecionado'; imports = []; lastCoreLoadedAt = 0;
      await Promise.all([loadImports(true), loadCore(true)]); toast('Planilha importada com sucesso.');
    } catch (error) { $('#uploadResult').innerHTML = `<div class="alert alert-danger">${esc(error.message)}</div>`; }
    finally { button.disabled = false; button.textContent = 'Importar planilha'; }
  }

  async function searchLead() {
    const q = ($('#leadSearch')?.value || '').trim();
    if (!q) return toast('Digite um nome, CPF ou telefone.', 'warning');
    const button = $('#btnLeadSearch'); button.disabled = true; button.textContent = 'Buscando...';
    $('#leadResults').innerHTML = '<div class="empty-state">Buscando...</div>';
    try {
      const d = await fetchJson(`/api/gestao/operacional/leads/buscar?q=${encodeURIComponent(q)}`);
      const items = d.items || d.data || [];
      $('#leadResults').innerHTML = items.map((r) => `<article class="lead-card"><div><span>${esc(r.status_atendimento || r.status || 'Lead')}</span><h3>${esc(r.nome || 'Sem nome')}</h3><p>${esc(r.curso || '—')} · ${esc(r.polo || '—')}</p></div><dl><div><dt>CPF</dt><dd>${esc(r.cpf || '—')}</dd></div><div><dt>Celular</dt><dd>${esc(r.celular || '—')}</dd></div><div><dt>Consultor</dt><dd>${esc(r.consultor_disparo || '—')}</dd></div><div><dt>Lote</dt><dd>${esc(r.nome_lote || r.lote_id || '—')}</dd></div></dl></article>`).join('') || '<div class="empty-state">Nenhum lead encontrado.</div>';
    } catch (error) { $('#leadResults').innerHTML = `<div class="empty-state error">${esc(error.message)}</div>`; }
    finally { button.disabled = false; button.textContent = 'Buscar'; }
  }

  async function loadAudit(force = false) {
    if (!$('#auditTableBody')) return;
    auditController?.abort(); auditController = new AbortController(); loadingRows('#auditTableBody', 8, 5);
    try {
      const d = await fetchJson('/api/gestao/logs/auditoria?limit=100&offset=0', {signal:auditController.signal});
      const items = d.data || d.items || [];
      $('#auditTableBody').innerHTML = items.map((r) => `<tr><td>${fmtDate(r.created_at)}</td><td>${esc(r.usuario_email || r.usuario || '—')}</td><td>${esc(r.acao || '—')}</td><td>${esc(r.modulo || '—')}</td><td>${esc(r.entidade || '—')}</td><td>${esc(r.lote_id || '—')}</td><td>${esc(r.sk_pessoa || r.cpf || '—')}</td><td>${esc(r.descricao || '—')}</td></tr>`).join('') || '<tr><td colspan="8" class="empty-cell">Nenhum registro.</td></tr>';
    } catch (error) { if (error.name !== 'AbortError') showError(error); }
  }

  function bind() {
    $$('.side-nav button').forEach((button) => button.addEventListener('click', () => switchPage(button.dataset.page)));
    $$('[data-go]').forEach((button) => button.addEventListener('click', () => switchPage(button.dataset.go)));
    $('#btnRefreshAll')?.addEventListener('click', () => { imports = []; lastCoreLoadedAt = 0; Promise.all([loadCore(true), loadImports(true)]); });
    $('#btnReloadImports')?.addEventListener('click', () => { imports = []; loadImports(true); });
    $('#btnReloadAudit')?.addEventListener('click', () => loadAudit(true));
    $('#teamSearch')?.addEventListener('input', renderTeam);
    $('#teamSort')?.addEventListener('change', renderTeam);
    $('#lotSearch')?.addEventListener('input', renderLots);
    $('#lotStatus')?.addEventListener('change', renderLots);
    $('#uploadForm')?.addEventListener('submit', uploadFile);
    $('#managementFile')?.addEventListener('change', (e) => { $('#managementFileName').textContent = e.target.files?.[0]?.name || 'Nenhum arquivo selecionado'; });
    $('#btnLeadSearch')?.addEventListener('click', searchLead);
    $('#leadSearch')?.addEventListener('keydown', (e) => { if (e.key === 'Enter') searchLead(); });
  }

  document.addEventListener('DOMContentLoaded', () => {
    bind();
    const page = (location.hash || '#overview').slice(1);
    if ($(`#page-${page}`)) switchPage(page);
    loadCore();
  });
})();
