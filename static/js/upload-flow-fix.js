(() => {
  'use strict';

  let requestInProgress = false;
  let responseAlreadyFinal = false;

  function selectedMode() {
    return document.querySelector('#uploadMode')?.value === 'somente_novos'
      ? 'somente_novos'
      : 'normal';
  }

  function endpointForMode() {
    return selectedMode() === 'somente_novos'
      ? '/api/upload/somente-novos'
      : '/api/upload';
  }

  function updateModeUi() {
    const mode = selectedMode();
    const help = document.querySelector('#uploadModeHelp');
    const button = document.querySelector('#btnUpload');

    if (help) {
      help.textContent = mode === 'somente_novos'
        ? 'Somente novos: telefone primeiro e CPF depois. Leads existentes serão ignorados. Limite de 10.000 linhas.'
        : 'Importação normal: insere novos e atualiza registros existentes conforme o arquivo.';
    }

    if (button && !button.classList.contains('is-loading')) {
      button.textContent = mode === 'somente_novos'
        ? 'Importar somente novos'
        : 'Importar planilha';
    }
  }

  async function postUpload(file, source) {
    if (requestInProgress) {
      throw new Error('Já existe uma importação em andamento. Aguarde a conclusão.');
    }

    requestInProgress = true;
    responseAlreadyFinal = false;

    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 10 * 60 * 1000);
    const formData = new FormData();
    formData.append('file', file);
    if (source) formData.append('source', source);
    formData.append('import_mode', selectedMode());

    try {
      const response = await fetch(endpointForMode(), {
        method: 'POST',
        body: formData,
        credentials: 'same-origin',
        cache: 'no-store',
        signal: controller.signal,
      });

      const raw = await response.text();
      let body = {};
      try {
        body = raw ? JSON.parse(raw) : {};
      } catch (_) {
        body = {message: raw || ''};
      }

      if (!response.ok || body?.ok === false) {
        const error = body?.error;
        const message = typeof error === 'string'
          ? error
          : error?.message || body?.message || `Falha no upload (HTTP ${response.status}).`;
        throw new Error(message);
      }

      responseAlreadyFinal = Boolean(
        body?.done === true ||
        body?.status === 'DONE' ||
        body?.mode === 'somente_novos'
      );
      return body;
    } catch (error) {
      if (error?.name === 'AbortError') {
        throw new Error('A importação ultrapassou 10 minutos e foi interrompida. Consulte o histórico antes de tentar novamente.');
      }
      throw error;
    } finally {
      window.clearTimeout(timeout);
      requestInProgress = false;
    }
  }

  // Substitui apenas as duas funções do fluxo antigo, sem interceptar window.fetch.
  window.uploadDirectToServer = postUpload;
  try { uploadDirectToServer = postUpload; } catch (_) {}

  const originalPoll = window.pollUploadStatus;
  async function stablePoll(jobId) {
    if (responseAlreadyFinal || !jobId) return;
    if (typeof originalPoll === 'function') return originalPoll(jobId);
  }
  window.pollUploadStatus = stablePoll;
  try { pollUploadStatus = stablePoll; } catch (_) {}

  document.addEventListener('DOMContentLoaded', () => {
    const mode = document.querySelector('#uploadMode');
    mode?.addEventListener('change', updateModeUi);
    updateModeUi();
  });
})();
