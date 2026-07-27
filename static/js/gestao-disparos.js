(() => {
  'use strict';

  const $ = (selector, root = document) => root.querySelector(selector);
  const esc = (value) => String(value ?? '').replace(/[&<>'"]/g, (char) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;',
  }[char]));
  const num = (value) => Number(value || 0);
  const fmtNum = (value) => num(value).toLocaleString('pt-BR');
  const pct = (value) => `${num(value).toFixed(1).replace('.0', '')}%`;
  const fmtDate = (value) => {
    if (!value) return '—';
    const date = new Date(value);
    return Number.isNaN(date.getTime())
      ? esc(value)
      : date.toLocaleString('pt-BR', { dateStyle: 'short', timeStyle: 'short' });
  };

  let items = [];
  let requestController = null;

  function parseDetails(value) {
    if (Array.isArray(value)) return value;
    if (!value) return [];
    try { return JSON.parse(value); } catch (_) { return []; }
  }

  function selectedMetric() {
    return $('#teamPeriod')?.value || 'total_disparado';
  }

  function filteredItems() {
    const query = ($('#teamSearch')?.value || '').trim().toLowerCase();
    const sort = $('#teamSort')?.value || selectedMetric();
    return [...items]
      .filter((row) => !query || String(row.consultor_disparo || '').toLowerCase().includes(query))
      .sort((a, b) => num(b[sort]) - num(a[sort]));
  }

  function renderSummary(data) {
    const totals = data.reduce((acc, row) => {
      acc.total += num(row.total_disparado);
      acc.hoje += num(row.disparado_hoje);
      acc.semana += num(row.disparado_semana);
      acc.mes += num(row.disparado_mes);
      acc.matriculas += num(row.matriculas);
      return acc;
    }, { total: 0, hoje: 0, semana: 0, mes: 0, matriculas: 0 });

    const container = $('#teamSummaryCards');
    if (!container) return;
    container.innerHTML = [
      ['Total disparado', totals.total, 'Todo o período'],
      ['Hoje', totals.hoje, 'Disparos de hoje'],
      ['Esta semana', totals.semana, 'Desde segunda-feira'],
      ['Este mês', totals.mes, 'Mês atual'],
      ['Matrículas', totals.matriculas, 'Conversões registradas'],
    ].map(([label, value, help]) => `
      <article class="executive-card">
        <span>${esc(label)}</span>
        <strong>${fmtNum(value)}</strong>
        <small>${esc(help)}</small>
      </article>
    `).join('');
  }

  function renderRanking(data) {
    const container = $('#teamRanking');
    if (!container) return;
    const metric = selectedMetric();
    const max = Math.max(1, ...data.map((row) => num(row[metric])));
    const metricLabel = {
      total_disparado: 'disparos totais',
      disparado_hoje: 'disparos hoje',
      disparado_semana: 'disparos na semana',
      disparado_mes: 'disparos no mês',
    }[metric] || 'disparos';

    container.innerHTML = data.slice(0, 6).map((row, index) => `
      <div class="ranking-row">
        <div class="rank-number">${index + 1}</div>
        <div class="rank-main">
          <strong>${esc(row.consultor_disparo || 'Sem consultor')}</strong>
          <small>${fmtNum(row.disparado_semana)} na semana · ${fmtNum(row.disparado_hoje)} hoje</small>
          <div class="rank-bar"><i style="width:${Math.max(4, num(row[metric]) / max * 100)}%"></i></div>
        </div>
        <div class="rank-result"><strong>${fmtNum(row[metric])}</strong><small>${metricLabel}</small></div>
      </div>
    `).join('') || '<div class="empty-state">Nenhum disparo encontrado.</div>';
  }

  function renderCards(data) {
    const container = $('#teamCards');
    if (!container) return;
    container.innerHTML = data.map((row) => `
      <article class="team-card">
        <div class="team-card-head">
          <div><span>Consultor</span><h3>${esc(row.consultor_disparo || 'Sem consultor')}</h3></div>
          <strong>${fmtNum(row.total_disparado)}<small> disparos</small></strong>
        </div>
        <div class="team-metrics">
          <div><span>Hoje</span><strong>${fmtNum(row.disparado_hoje)}</strong></div>
          <div><span>Semana</span><strong>${fmtNum(row.disparado_semana)}</strong></div>
          <div><span>Mês</span><strong>${fmtNum(row.disparado_mes)}</strong></div>
          <div><span>Matrículas</span><strong>${fmtNum(row.matriculas)}</strong></div>
        </div>
      </article>
    `).join('') || '<div class="empty-state">Nenhum consultor com disparos encontrados.</div>';
  }

  function renderTable(data) {
    const body = $('#teamTableBody');
    if (!body) return;
    body.innerHTML = data.map((row) => `
      <tr>
        <td><strong>${esc(row.consultor_disparo || 'Sem consultor')}</strong></td>
        <td>${fmtNum(row.total_disparado)}</td>
        <td>${fmtNum(row.disparado_hoje)}</td>
        <td><strong>${fmtNum(row.disparado_semana)}</strong></td>
        <td>${fmtNum(row.disparado_mes)}</td>
        <td>${fmtNum(row.retornos)}</td>
        <td>${fmtNum(row.positivos)}</td>
        <td>${fmtNum(row.negativos)}</td>
        <td><strong>${fmtNum(row.matriculas)}</strong></td>
        <td>${pct(row.taxa_retorno_pct)}</td>
        <td>${pct(row.taxa_matricula_pct)}</td>
        <td>${fmtDate(row.ultimo_disparo)}</td>
      </tr>
    `).join('') || '<tr><td colspan="12" class="empty-cell">Nenhum disparo encontrado.</td></tr>';
  }

  function renderBreakdown(data) {
    const body = $('#teamBreakdownBody');
    if (!body) return;
    const rows = data.flatMap((consultor) => parseDetails(consultor.detalhes_disparos).map((detail) => ({
      consultor_disparo: consultor.consultor_disparo,
      ...detail,
    })));

    body.innerHTML = rows.map((row) => `
      <tr>
        <td><strong>${esc(row.consultor_disparo || 'Sem consultor')}</strong></td>
        <td>${esc(row.tipo_disparo || 'Sem tipo')}</td>
        <td>${esc(row.campanha || 'Sem campanha')}</td>
        <td>${esc(row.canal || 'Sem canal')}</td>
        <td>${esc(row.peca_disparo || 'Sem peça')}</td>
        <td>${fmtNum(row.total_disparado)}</td>
        <td><strong>${fmtNum(row.disparado_semana)}</strong></td>
        <td>${fmtDate(row.ultimo_disparo)}</td>
      </tr>
    `).join('') || '<tr><td colspan="8" class="empty-cell">Nenhum detalhamento encontrado.</td></tr>';
  }

  function render() {
    const data = filteredItems();
    const count = $('#teamCount');
    if (count) count.textContent = `${data.length} consultor(es)`;
    renderSummary(data);
    renderRanking(data);
    renderCards(data);
    renderTable(data);
    renderBreakdown(data);
  }

  async function load() {
    requestController?.abort();
    requestController = new AbortController();
    try {
      const response = await fetch('/api/gestao/operacional/consultores?limit=1000', {
        credentials: 'same-origin',
        headers: { Accept: 'application/json' },
        signal: requestController.signal,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload?.ok === false) {
        throw new Error(payload?.error?.message || payload?.message || `Falha HTTP ${response.status}`);
      }
      const data = payload.data || payload;
      items = data.items || data.data || [];
      render();
    } catch (error) {
      if (error.name === 'AbortError') return;
      const box = $('#globalError');
      if (box) {
        box.textContent = error.message || 'Falha ao carregar os disparos da equipe.';
        box.classList.remove('d-none');
      }
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    $('#teamSearch')?.addEventListener('input', render);
    $('#teamSort')?.addEventListener('change', render);
    $('#teamPeriod')?.addEventListener('change', () => {
      const sort = $('#teamSort');
      if (sort && ['total_disparado', 'disparado_hoje', 'disparado_semana', 'disparado_mes'].includes(sort.value)) {
        sort.value = selectedMetric();
      }
      render();
    });
    $('#btnRefreshAll')?.addEventListener('click', () => setTimeout(load, 250));
    setTimeout(load, 250);
  });
})();
