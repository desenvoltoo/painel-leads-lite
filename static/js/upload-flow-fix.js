(() => {
  'use strict';

  let requestInProgress = false;
  let progressTimer = null;
  let progressValue = 0;
  let progressStartedAt = 0;

  function selectedMode() {
    return document.querySelector('#uploadMode')?.value === 'somente_novos'
      ? 'somente_novos'
      : 'normal';
  }

  function endpointForMode() {
    return selectedMode() === 'somente_novos'
      ? '/api/upload/somente-novos'
      : '/api/upload/atualizar-existentes';
  }

  function routineLabel() {
    return selectedMode() === 'somente_novos'
      ? 'sp_importar_somente_leads_novos'
      : 'sp_processar_stg_leads_site';
  }

  function setStatus(message, type = 'info') {
    const status = document.querySelector('#uploadStatus');
    if (!status) return;
    const normalized = type === 'error' ? 'danger' : type;
    status.textContent = message;
    status.className = `alert alert-${normalized}`;
  }

  function ensureProgress() {
    let box = document.querySelector('#uploadLiveProgress');
    if (box) return box;

    box = document.createElement('div');
    box.id = 'uploadLiveProgress';
    box.className = 'upload-live-progress';
    box.hidden = true;
    box.innerHTML = `
      <div class="upload-live-progress-head">
        <strong id="uploadProgressStage">Preparando importação</strong>
        <span id="uploadProgressPercent">0%</span>
      </div>
      <div class="upload-live-progress-track" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0">
        <div id="uploadProgressBar" class="upload-live-progress-bar"></div>
      </div>
      <div class="upload-live-progress-meta">
        <span id="uploadProgressMode">—</span>
        <span id="uploadProgressElapsed">0s</span>
      </div>
    `;
    document.querySelector('#uploadStatus')?.insertAdjacentElement('afterend', box);

    if (!document.querySelector('#uploadProgressStyle')) {
      const style = document.createElement('style');
      style.id = 'uploadProgressStyle';
      style.textContent = `
        .upload-live-progress{margin-top:10px;padding:12px;border:1px solid #dbe4f0;border-radius:12px;background:#fff;color:#172033}
        .upload-live-progress-head,.upload-live-progress-meta{display:flex;justify-content:space-between;gap:12px;align-items:center}
        .upload-live-progress-head{margin-bottom:8px}.upload-live-progress-head span{font-weight:700;color:#2457c5}
        .upload-live-progress-track{height:12px;border-radius:999px;background:#e8eef8;overflow:hidden}
        .upload-live-progress-bar{height:100%;width:0;border-radius:inherit;background:linear-gradient(90deg,#2563eb,#60a5fa);transition:width .45s ease}
        .upload-live-progress-meta{margin-top:7px;font-size:.78rem;color:#64748b}
      `;
      document.head.appendChild(style);
    }
    return box;
  }

  function formatElapsed(seconds) {
    const total = Math.max(0, Math.floor(seconds));
    const hours = Math.floor(total / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    const secs = total % 60;
    if (hours > 0) return `${hours}h ${String(minutes).padStart(2, '0')}m ${String(secs).padStart(2, '0')}s`;
    if (minutes > 0) return `${minutes}m ${String(secs).padStart(2, '0')}s`;
    return `${secs}s`;
  }

  function renderProgress(value, stage) {
    progressValue = Math.max(0, Math.min(100, Math.round(value)));
    const box = ensureProgress();
    const bar = box.querySelector('#uploadProgressBar');
    const track = box.querySelector('.upload-live-progress-track');
    const percent = box.querySelector('#uploadProgressPercent');
    const stageEl = box.querySelector('#uploadProgressStage');
    const elapsed = box.querySelector('#uploadProgressElapsed');
    const mode = box.querySelector('#uploadProgressMode');
    box.hidden = false;
    if (bar) bar.style.width = `${progressValue}%`;
    if (track) track.setAttribute('aria-valuenow', String(progressValue));
    if (percent) percent.textContent = `${progressValue}%`;
    if (stageEl) stageEl.textContent = stage;
    if (elapsed) elapsed.textContent = formatElapsed((Date.now() - progressStartedAt) / 1000);
    if (mode) mode.textContent = `${selectedMode() === 'somente_novos' ? 'Somente novos' : 'Atualizar existentes'} · ${routineLabel()}`;
  }

  function startProgress() {
    window.clearInterval(progressTimer);
    progressStartedAt = Date.now();
    const bar = ensureProgress().querySelector('#uploadProgressBar');
    if (bar) bar.style.background = 'linear-gradient(90deg,#2563eb,#60a5fa)';
    renderProgress(5, 'Validando arquivo');
    progressTimer = window.setInterval(() => {
      const elapsed = (Date.now() - progressStartedAt) / 1000;
      let next = progressValue;
      let stage = 'Enviando arquivo para staging';
      if (elapsed < 3) {
        next = Math.min(25, progressValue + 4);
      } else if (elapsed < 10) {
        stage = 'Gravando linhas na staging';
        next = Math.min(48, progressValue + 2);
      } else {
        stage = selectedMode() === 'somente_novos'
          ? 'Processando somente os leads novos no PostgreSQL'
          : 'Inserindo novos e atualizando existentes no PostgreSQL';
        next = Math.min(92, progressValue + (progressValue < 75 ? 1.5 : 0.15));
      }
      renderProgress(next, stage);
    }, 1000);
  }

  function finishProgress(success, message) {
    window.clearInterval(progressTimer);
    progressTimer = null;
    renderProgress(success ? 100 : progressValue, message);
    const bar = document.querySelector('#uploadProgressBar');
    if (bar && !success) bar.style.background = '#dc2626';
  }

  function setLoading(loading) {
    const button = document.querySelector('#btnUpload');
    if (!button) return;
    button.disabled = loading;
    button.classList.toggle('is-loading', loading);
    if (loading) button.textContent = 'Importando planilha...';
    else updateModeUi();
  }

  function updateModeUi() {
    const mode = selectedMode();
    const help = document.querySelector('#uploadModeHelp');
    const button = document.querySelector('#btnUpload');

    if (help) {
      help.textContent = mode === 'somente_novos'
        ? 'Executa sp_importar_somente_leads_novos: importa novos e ignora integralmente os existentes.'
        : 'Executa sp_processar_stg_leads_site: inclui novos e atualiza os registros já existentes.';
    }

    if (button && !button.classList.contains('is-loading')) {
      button.textContent = mode === 'somente_novos'
        ? 'Importar somente novos'
        : 'Atualizar base existente';
    }
  }

  function reportMessage(body) {
    const report = body?.report || {};
    const received = report.linhas_recebidas ?? 0;
    const inserted = report.linhas_inseridas ?? report.linhas_novas ?? report.linhas_processadas ?? 0;
    const ignoredExisting = (report.existentes_por_celular ?? 0) + (report.existentes_por_cpf ?? 0);
    const duplicates = report.duplicados_arquivo ?? 0;
    const rejected = report.linhas_rejeitadas ?? 0;

    if (body?.mode === 'somente_novos') {
      return `Concluído. Recebidas: ${received} | Inseridas: ${inserted} | Existentes ignoradas: ${ignoredExisting} | Duplicadas no arquivo: ${duplicates} | Rejeitadas: ${rejected}.`;
    }
    return `Concluído. Recebidas: ${received} | Processadas: ${inserted} | Rejeitadas: ${rejected}.`;
  }

  async function executeUpload() {
    if (requestInProgress) {
      setStatus('Já existe uma importação em andamento.', 'warning');
      return;
    }

    const input = document.querySelector('#uploadFile');
    const file = input?.files?.[0];
    if (!file) {
      setStatus('Selecione um arquivo CSV, XLS ou XLSX.', 'error');
      return;
    }
    if (!/\.(csv|xlsx|xls)$/i.test(file.name || '')) {
      setStatus('Formato inválido. Envie CSV, XLS ou XLSX.', 'error');
      return;
    }

    requestInProgress = true;
    setLoading(true);
    startProgress();
    setStatus(`Modo selecionado: ${selectedMode() === 'somente_novos' ? 'Somente leads novos' : 'Atualizar base existente'}. Não feche esta página até a conclusão.`, 'info');

    const formData = new FormData();
    formData.append('file', file);
    const source = document.querySelector('#uploadSource')?.value?.trim();
    if (source) formData.append('source', source);
    formData.append('import_mode', selectedMode());

    try {
      const response = await fetch(endpointForMode(), {
        method: 'POST',
        body: formData,
        credentials: 'same-origin',
        cache: 'no-store',
      });

      const raw = await response.text();
      let body = {};
      try { body = raw ? JSON.parse(raw) : {}; }
      catch (_) { body = {message: raw || ''}; }

      if (!response.ok || body?.ok === false) {
        const error = body?.error;
        const message = typeof error === 'string'
          ? error
          : error?.message || error?.details || body?.message || `Falha no upload (HTTP ${response.status}).`;
        throw new Error(message);
      }

      const expectedMode = selectedMode() === 'somente_novos' ? 'somente_novos' : 'atualizar_existentes';
      if (body?.mode !== expectedMode) {
        throw new Error(`Resposta inconsistente: esperado modo ${expectedMode}, recebido ${body?.mode || 'não informado'}.`);
      }

      const message = reportMessage(body);
      finishProgress(true, 'Importação concluída');
      setStatus(message, 'success');
      try {
        window.dispatchEvent(new CustomEvent('gestao:upload-concluido', {
          detail: {type: 'upload-concluido', at: new Date().toISOString(), mode: body.mode},
        }));
      } catch (_) {}
      window.setTimeout(() => window.location.reload(), 2500);
    } catch (error) {
      console.error('upload_flow_error', error);
      finishProgress(false, 'Falha na importação');
      setStatus(error?.message || 'Não foi possível concluir a importação.', 'error');
    } finally {
      requestInProgress = false;
      setLoading(false);
    }
  }

  document.addEventListener('click', (event) => {
    const button = event.target?.closest?.('#btnUpload');
    if (!button) return;
    event.preventDefault();
    event.stopPropagation();
    event.stopImmediatePropagation();
    executeUpload();
  }, true);

  document.addEventListener('change', (event) => {
    if (event.target?.id !== 'uploadMode') return;
    updateModeUi();
    window.dispatchEvent(new CustomEvent('upload:mode-changed', {
      detail: {mode: selectedMode()},
    }));
  });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', updateModeUi, {once: true});
  } else {
    updateModeUi();
  }

  window.executeLeadsUpload = executeUpload;
})();