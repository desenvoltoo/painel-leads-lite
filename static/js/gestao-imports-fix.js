(() => {
  'use strict';

  const $ = (selector) => document.querySelector(selector);
  const esc = (value) => String(value ?? '').replace(/[&<>'"]/g, (char) => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[char]));
  const number = (value) => Number(value || 0);
  const formatNumber = (value) => number(value).toLocaleString('pt-BR');
  const formatDate = (value) => {
    if (!value) return '—';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? esc(value) : date.toLocaleString('pt-BR', {dateStyle:'short', timeStyle:'short'});
  };

  let pollingTimer = null;

  async function fetchJson(url, options = {}) {
    const response = await fetch(url, {
      credentials: 'same-origin',
      cache: 'no-store',
      headers: {Accept: 'application/json', ...(options.headers || {})},
      ...options,
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok || body?.ok === false) {
      throw new Error(body?.error?.details || body?.error?.message || body?.message || `Falha HTTP ${response.status}`);
    }
    return body;
  }

  function setResult(message, type = 'info') {
    const box = $('#uploadResult');
    if (!box) return;
    box.innerHTML = `<div class="alert alert-${type}">${esc(message)}</div>`;
  }

  function renderHistory(items) {
    const imports = Array.isArray(items) ? items : [];
    const totals = imports.reduce((acc, row) => {
      acc.total += 1;
      acc.recebidas += number(row.linhas_recebidas);
      acc.validas += number(row.linhas_validas);
      acc.atualizadas += number(row.linhas_atualizadas);
      acc.rejeitadas += number(row.linhas_rejeitadas);
      if (String(row.status || '').toUpperCase().includes('CONCLUIDO')) acc.concluidas += 1;
      return acc;
    }, {total:0, concluidas:0, recebidas:0, validas:0, atualizadas:0, rejeitadas:0});

    const summary = $('#importSummary');
    if (summary) {
      summary.innerHTML = [
        ['Importações', totals.total],
        ['Concluídas', totals.concluidas],
        ['Linhas recebidas', totals.recebidas],
        ['Linhas válidas', totals.validas],
        ['Atualizadas', totals.atualizadas],
        ['Rejeitadas', totals.rejeitadas],
      ].map(([label, value]) => `<div><span>${label}</span><strong>${formatNumber(value)}</strong></div>`).join('');
    }

    const body = $('#importsTableBody');
    if (body) {
      body.innerHTML = imports.map((row) => `<tr>
        <td><strong>${esc(row.nome_arquivo || '—')}</strong><small style="display:block;color:#64748b">${esc(row.upload_id || '')}</small></td>
        <td>${esc(row.usuario || '—')}</td>
        <td>${esc(row.status || '—')}</td>
        <td>${esc(row.etapa || '—')}</td>
        <td>${formatNumber(row.linhas_recebidas)}</td>
        <td>${formatNumber(row.linhas_validas)}</td>
        <td>${formatNumber(row.linhas_inseridas)}</td>
        <td>${formatNumber(row.linhas_atualizadas)}</td>
        <td>${formatNumber(row.linhas_rejeitadas)}</td>
        <td>${esc(row.mensagem || '—')}<small style="display:block;color:#64748b">Staging: ${formatNumber(row.linhas_staging)}</small></td>
        <td>${formatDate(row.criado_em)}</td>
      </tr>`).join('') || '<tr><td colspan="11" class="empty-cell">Nenhuma importação encontrada.</td></tr>';
    }
  }

  async function loadHistory() {
    try {
      const body = await fetchJson('/api/gestao/importacoes/ativas');
      renderHistory(body.items || []);
    } catch (error) {
      setResult(error.message || 'Falha ao carregar histórico.', 'danger');
    }
  }

  async function poll(uploadId) {
    if (pollingTimer) clearTimeout(pollingTimer);
    try {
      const body = await fetchJson(`/api/upload/progresso/${encodeURIComponent(uploadId)}`);
      const internalStatus = String(body.internal_status || body.status || '').toUpperCase();
      const report = body.report || {};
      setResult(
        `${body.message || 'Processando'} Recebidas: ${formatNumber(report.linhas_recebidas)} · Inseridas: ${formatNumber(report.linhas_inseridas)} · Rejeitadas: ${formatNumber(report.linhas_rejeitadas)}.`,
        internalStatus === 'ERRO' ? 'danger' : internalStatus === 'CONCLUIDO' ? 'success' : 'info'
      );
      await loadHistory();
      if (internalStatus === 'CONCLUIDO' || internalStatus === 'ERRO') return;
      pollingTimer = setTimeout(() => poll(uploadId), 2500);
    } catch (error) {
      setResult(error.message || 'Falha ao acompanhar processamento.', 'danger');
    }
  }

  async function submitUpload(event) {
    event.preventDefault();
    event.stopImmediatePropagation();

    const form = event.currentTarget;
    const file = $('#managementFile')?.files?.[0];
    const button = form.querySelector('button[type="submit"]');
    if (!file) return setResult('Selecione uma planilha CSV, XLS ou XLSX.', 'warning');

    button.disabled = true;
    button.textContent = 'Enviando e processando...';
    setResult('Enviando arquivo para a staging...', 'info');

    try {
      const data = new FormData();
      data.append('file', file);
      const response = await fetchJson('/api/upload/atualizar-existentes', {method:'POST', body:data});
      const uploadId = response.upload_id || response.job_id;
      if (!uploadId) throw new Error('O servidor não retornou o upload_id.');
      form.reset();
      $('#managementFileName').textContent = 'Nenhum arquivo selecionado';
      setResult(`Arquivo recebido. Processamento iniciado: ${uploadId}.`, 'info');
      await loadHistory();
      poll(uploadId);
    } catch (error) {
      setResult(error.message || 'Falha ao importar planilha.', 'danger');
    } finally {
      button.disabled = false;
      button.textContent = 'Importar planilha';
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    const form = $('#uploadForm');
    if (form) form.addEventListener('submit', submitUpload, true);
    $('#btnReloadImports')?.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopImmediatePropagation();
      loadHistory();
    }, true);
    loadHistory();
    setInterval(loadHistory, 15000);
  });
})();
