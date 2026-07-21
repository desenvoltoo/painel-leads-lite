(() => {
  'use strict';

  const originalFetch = window.fetch.bind(window);
  let currentJobId = '';

  function number(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, char => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    })[char]);
  }

  function ensureResultPanel() {
    let panel = document.querySelector('#uploadResultPanel');
    if (panel) return panel;

    panel = document.createElement('section');
    panel.id = 'uploadResultPanel';
    panel.className = 'upload-result-panel';
    panel.hidden = true;

    const uploadCard = document.querySelector('.ops-upload-card');
    uploadCard?.insertAdjacentElement('afterend', panel);
    return panel;
  }

  function metric(label, value, tone = '') {
    return `<article class="${tone}"><span>${escapeHtml(label)}</span><strong>${number(value).toLocaleString('pt-BR')}</strong></article>`;
  }

  function normalizedReport(body) {
    const data = body?.data || body?.report || body || {};
    return {
      jobId: data.job_id || data.upload_id || body?.job_id || currentJobId || '',
      status: String(data.status || data.state || body?.status || '').toUpperCase(),
      etapa: data.etapa || '',
      mensagem: data.mensagem || data.message || body?.message || '',
      recebidas: data.linhas_recebidas ?? data.total_linhas ?? body?.report?.linhas_recebidas,
      validas: data.linhas_validas ?? data.linhas_processadas ?? body?.report?.linhas_processadas,
      inseridas: data.linhas_inseridas ?? body?.report?.linhas_inseridas,
      atualizadas: data.linhas_atualizadas ?? body?.report?.linhas_atualizadas,
      rejeitadas: data.linhas_rejeitadas ?? body?.report?.linhas_rejeitadas,
      ignoradas: data.linhas_ignoradas ?? 0,
      duplicadosArquivo: data.duplicados_arquivo ?? body?.report?.duplicados_arquivo,
      duplicadosBanco: data.duplicados_banco ?? body?.report?.duplicados_banco,
      finalizadoEm: data.finalizado_em || data.atualizado_em || '',
    };
  }

  function isFinished(report) {
    return ['DONE', 'CONCLUIDO', 'CONCLUIDO_COM_REJEICOES', 'ERRO'].includes(report.status);
  }

  function render(report) {
    const panel = ensureResultPanel();
    if (!report || !isFinished(report)) return;

    const hasError = report.status === 'ERRO';
    const hasRejects = number(report.rejeitadas) > 0;
    const title = hasError
      ? 'Importação finalizada com erro'
      : hasRejects
        ? 'Importação concluída com rejeições'
        : 'Importação concluída com sucesso';

    const statusClass = hasError ? 'danger' : hasRejects ? 'warning' : 'success';
    const jobId = escapeHtml(report.jobId || 'não informado');

    panel.hidden = false;
    panel.className = `upload-result-panel ${statusClass}`;
    panel.innerHTML = `
      <div class="upload-result-head">
        <div>
          <span class="upload-result-eyebrow">Resultado da importação</span>
          <h3>${title}</h3>
          <p>${escapeHtml(report.mensagem || 'Processamento finalizado no PostgreSQL.')}</p>
        </div>
        <span class="upload-result-status">${escapeHtml(report.status || 'FINALIZADO')}</span>
      </div>
      <div class="upload-result-metrics">
        ${metric('Recebidas', report.recebidas)}
        ${metric('Válidas', report.validas)}
        ${metric('Inseridas', report.inseridas, 'success')}
        ${metric('Atualizadas', report.atualizadas)}
        ${metric('Rejeitadas', report.rejeitadas, hasRejects ? 'warning' : '')}
        ${metric('Duplicadas no banco', report.duplicadosBanco)}
      </div>
      <div class="upload-result-footer">
        <div><strong>ID do upload:</strong> <code>${jobId}</code></div>
        <div class="upload-result-actions">
          <button type="button" id="copyUploadJobId" class="btn btn-ghost">Copiar ID</button>
          <a href="/gestao" class="btn btn-primary">Abrir painel de gestão</a>
        </div>
      </div>
    `;

    panel.scrollIntoView({behavior: 'smooth', block: 'nearest'});
  }

  window.fetch = async (...args) => {
    const response = await originalFetch(...args);
    const requestUrl = typeof args[0] === 'string' ? args[0] : args[0]?.url || '';
    const method = String(args[1]?.method || 'GET').toUpperCase();

    const isUpload = method === 'POST' && requestUrl.includes('/api/upload') && !requestUrl.includes('/preview');
    const isStatus = requestUrl.includes('/api/upload/status');

    if ((isUpload || isStatus) && response.ok) {
      try {
        const body = await response.clone().json();
        if (isUpload) {
          currentJobId = body?.job_id || body?.data?.job_id || currentJobId;
          const initial = normalizedReport(body);
          if (isFinished(initial)) render(initial);
        }
        if (isStatus) {
          const report = normalizedReport(body);
          currentJobId = report.jobId || currentJobId;
          render(report);
        }
      } catch (_) {}
    }

    return response;
  };

  document.addEventListener('click', event => {
    if (event.target?.id !== 'copyUploadJobId') return;
    const text = document.querySelector('#uploadResultPanel code')?.textContent || '';
    if (!text) return;
    navigator.clipboard?.writeText(text).then(() => {
      event.target.textContent = 'ID copiado';
      setTimeout(() => { event.target.textContent = 'Copiar ID'; }, 1600);
    }).catch(() => {});
  });

  const style = document.createElement('style');
  style.textContent = `
    .upload-result-panel{margin:18px 0;border:1px solid rgba(148,163,184,.22);border-radius:18px;padding:18px;background:rgba(15,23,42,.58)}
    .upload-result-panel.success{border-color:rgba(34,197,94,.35)}.upload-result-panel.warning{border-color:rgba(245,158,11,.4)}.upload-result-panel.danger{border-color:rgba(239,68,68,.4)}
    .upload-result-head{display:flex;justify-content:space-between;gap:18px;align-items:flex-start}.upload-result-head h3{margin:4px 0}.upload-result-head p{margin:6px 0 0;opacity:.78;max-width:900px;line-height:1.45}
    .upload-result-eyebrow{font-size:.74rem;text-transform:uppercase;letter-spacing:.08em;opacity:.68}.upload-result-status{padding:7px 10px;border-radius:999px;background:rgba(148,163,184,.12);font-size:.75rem;font-weight:700;white-space:nowrap}
    .upload-result-metrics{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:10px;margin-top:16px}.upload-result-metrics article{padding:12px;border-radius:14px;background:rgba(15,23,42,.72);display:flex;flex-direction:column;gap:3px}.upload-result-metrics span{font-size:.76rem;opacity:.7}.upload-result-metrics strong{font-size:1.35rem}.upload-result-metrics article.success strong{color:#86efac}.upload-result-metrics article.warning strong{color:#fcd34d}
    .upload-result-footer{display:flex;justify-content:space-between;gap:16px;align-items:center;margin-top:15px;padding-top:14px;border-top:1px solid rgba(148,163,184,.15)}.upload-result-footer code{word-break:break-all}.upload-result-actions{display:flex;gap:8px;align-items:center}.upload-result-actions a{text-decoration:none}
    @media(max-width:1050px){.upload-result-metrics{grid-template-columns:repeat(3,minmax(0,1fr))}}@media(max-width:700px){.upload-result-head,.upload-result-footer{flex-direction:column}.upload-result-metrics{grid-template-columns:repeat(2,minmax(0,1fr))}.upload-result-actions{width:100%;flex-wrap:wrap}}
  `;
  document.head.appendChild(style);
})();