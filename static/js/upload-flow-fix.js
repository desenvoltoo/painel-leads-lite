(() => {
  'use strict';

  let requestInProgress = false;
  let progressStartedAt = 0;
  let activeUploadId = '';

  function selectedMode() {
    return document.querySelector('#uploadMode')?.value === 'somente_novos' ? 'somente_novos' : 'normal';
  }

  function endpointForMode() {
    return selectedMode() === 'somente_novos' ? '/api/upload/somente-novos' : '/api/upload/atualizar-existentes';
  }

  function routineLabel() {
    return selectedMode() === 'somente_novos' ? 'sp_importar_somente_leads_novos' : 'sp_processar_stg_leads_site';
  }

  function setStatus(message, type = 'info') {
    const status = document.querySelector('#uploadStatus');
    if (!status) return;
    status.textContent = message;
    status.className = `alert alert-${type === 'error' ? 'danger' : type}`;
  }

  function ensureProgress() {
    let box = document.querySelector('#uploadLiveProgress');
    if (box) return box;
    box = document.createElement('div');
    box.id = 'uploadLiveProgress';
    box.className = 'upload-live-progress';
    box.hidden = true;
    box.innerHTML = `<div class="upload-live-progress-head"><strong id="uploadProgressStage">Preparando importação</strong><span id="uploadProgressPercent">0%</span></div><div class="upload-live-progress-track" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0"><div id="uploadProgressBar" class="upload-live-progress-bar"></div></div><div class="upload-live-progress-meta"><span id="uploadProgressMode">—</span><span id="uploadProgressElapsed">0s</span></div>`;
    document.querySelector('#uploadStatus')?.insertAdjacentElement('afterend', box);
    if (!document.querySelector('#uploadProgressStyle')) {
      const style = document.createElement('style');
      style.id = 'uploadProgressStyle';
      style.textContent = `.upload-live-progress{margin-top:10px;padding:12px;border:1px solid #dbe4f0;border-radius:12px;background:#fff;color:#172033}.upload-live-progress-head,.upload-live-progress-meta{display:flex;justify-content:space-between;gap:12px;align-items:center}.upload-live-progress-head{margin-bottom:8px}.upload-live-progress-head span{font-weight:700;color:#2457c5}.upload-live-progress-track{height:12px;border-radius:999px;background:#e8eef8;overflow:hidden}.upload-live-progress-bar{height:100%;width:0;border-radius:inherit;background:linear-gradient(90deg,#2563eb,#60a5fa);transition:width .4s ease}.upload-live-progress-meta{margin-top:7px;font-size:.78rem;color:#64748b}`;
      document.head.appendChild(style);
    }
    return box;
  }

  function formatElapsed() {
    const total = Math.max(0, Math.floor((Date.now() - progressStartedAt) / 1000));
    const hours = Math.floor(total / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    const seconds = total % 60;
    if (hours) return `${hours}h ${minutes}m ${String(seconds).padStart(2, '0')}s`;
    if (minutes) return `${minutes}m ${String(seconds).padStart(2, '0')}s`;
    return `${seconds}s`;
  }

  function renderProgress(value, stage, failed = false) {
    const progress = Math.max(0, Math.min(100, Number(value) || 0));
    const box = ensureProgress();
    box.hidden = false;
    box.querySelector('#uploadProgressBar').style.width = `${progress}%`;
    box.querySelector('#uploadProgressBar').style.background = failed ? '#dc2626' : 'linear-gradient(90deg,#2563eb,#60a5fa)';
    box.querySelector('.upload-live-progress-track').setAttribute('aria-valuenow', String(progress));
    box.querySelector('#uploadProgressPercent').textContent = `${Math.round(progress)}%`;
    box.querySelector('#uploadProgressStage').textContent = stage || 'Processando';
    box.querySelector('#uploadProgressElapsed').textContent = formatElapsed();
    box.querySelector('#uploadProgressMode').textContent = `${selectedMode() === 'somente_novos' ? 'Somente novos' : 'Atualizar existentes'} · ${routineLabel()}`;
  }

  function setLoading(loading) {
    const button = document.querySelector('#btnUpload');
    if (!button) return;
    button.disabled = loading;
    button.classList.toggle('is-loading', loading);
    button.textContent = loading ? 'Importação em andamento...' : (selectedMode() === 'somente_novos' ? 'Importar somente novos' : 'Atualizar base existente');
  }

  function reportMessage(body) {
    const report = body?.report || {};
    return `Concluído. Recebidas: ${report.linhas_recebidas || 0} | Inseridas: ${report.linhas_inseridas || 0} | Ignoradas: ${report.linhas_ignoradas || 0} | Rejeitadas: ${report.linhas_rejeitadas || 0}.`;
  }

  async function pollProgress(uploadId) {
    while (activeUploadId === uploadId) {
      const response = await fetch(`/api/upload/progresso/${encodeURIComponent(uploadId)}`, {credentials: 'same-origin', cache: 'no-store'});
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(body?.error?.message || 'Falha ao consultar o progresso.');

      const stageLabels = {
        AGUARDANDO: 'Aguardando processamento',
        GRAVANDO_STAGING: 'Gravando linhas na staging',
        STAGING_CONCLUIDA: 'Staging concluída',
        LOCALIZANDO_ROTINA: 'Preparando rotina do banco',
        EXECUTANDO_SP: selectedMode() === 'somente_novos' ? 'Comparando e inserindo somente novos' : 'Atualizando a base existente',
        CONCLUIDO: 'Importação concluída',
        ERRO: 'Importação com erro',
      };
      renderProgress(body.progress, stageLabels[body.stage] || body.stage || 'Processando', body.status === 'ERRO');

      if (body.done) {
        if (!body.ok || body.status === 'ERRO') throw new Error(body.error || body.message || 'Falha na importação.');
        renderProgress(100, 'Importação concluída');
        setStatus(reportMessage(body), 'success');
        activeUploadId = '';
        window.setTimeout(() => window.location.reload(), 2000);
        return;
      }
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }

  async function executeUpload() {
    if (requestInProgress) return setStatus('Já existe uma importação em andamento.', 'warning');
    const file = document.querySelector('#uploadFile')?.files?.[0];
    if (!file) return setStatus('Selecione um arquivo CSV, XLS ou XLSX.', 'error');

    requestInProgress = true;
    progressStartedAt = Date.now();
    setLoading(true);
    renderProgress(5, 'Enviando arquivo');
    setStatus('Enviando arquivo para a staging...', 'info');

    const formData = new FormData();
    formData.append('file', file);
    formData.append('import_mode', selectedMode());
    const source = document.querySelector('#uploadSource')?.value?.trim();
    if (source) formData.append('source', source);

    try {
      const response = await fetch(endpointForMode(), {method: 'POST', body: formData, credentials: 'same-origin', cache: 'no-store'});
      const body = await response.json().catch(() => ({}));
      if (!response.ok || !body.ok) throw new Error(body?.error?.details || body?.error?.message || body?.message || `Falha HTTP ${response.status}.`);
      activeUploadId = body.upload_id || body.job_id;
      if (!activeUploadId) throw new Error('O servidor não retornou o upload_id.');
      renderProgress(20, 'Staging concluída');
      setStatus(`Arquivo recebido. Processando em segundo plano: ${activeUploadId}.`, 'info');
      await pollProgress(activeUploadId);
    } catch (error) {
      console.error('upload_flow_error', error);
      renderProgress(100, 'Importação com erro', true);
      setStatus(error?.message || 'Não foi possível concluir a importação.', 'error');
      activeUploadId = '';
    } finally {
      requestInProgress = false;
      setLoading(false);
    }
  }

  document.addEventListener('click', event => {
    const button = event.target?.closest?.('#btnUpload');
    if (!button) return;
    event.preventDefault();
    event.stopPropagation();
    event.stopImmediatePropagation();
    executeUpload();
  }, true);

  document.addEventListener('change', event => {
    if (event.target?.id !== 'uploadMode') return;
    setLoading(false);
    window.dispatchEvent(new CustomEvent('upload:mode-changed', {detail: {mode: selectedMode()}}));
  });

  window.executeLeadsUpload = executeUpload;
})();
