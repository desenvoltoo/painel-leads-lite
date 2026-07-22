(() => {
  'use strict';

  let requestInProgress = false;

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

  function setStatus(message, type = 'info') {
    const status = document.querySelector('#uploadStatus');
    if (!status) return;
    const normalized = type === 'error' ? 'danger' : type;
    status.textContent = message;
    status.className = `alert alert-${normalized}`;
  }

  function setLoading(loading) {
    const button = document.querySelector('#btnUpload');
    if (!button) return;
    button.disabled = loading;
    button.classList.toggle('is-loading', loading);
    if (loading) {
      button.textContent = 'Importando planilha...';
    } else {
      updateModeUi();
    }
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

  function reportMessage(body) {
    const report = body?.report || {};
    const received = report.linhas_recebidas ?? 0;
    const inserted = report.linhas_inseridas ?? report.linhas_novas ?? report.linhas_processadas ?? 0;
    const ignored = (report.existentes_por_celular ?? 0) + (report.existentes_por_cpf ?? 0);
    const rejected = report.linhas_rejeitadas ?? 0;

    if (selectedMode() === 'somente_novos') {
      return `Concluído. Recebidas: ${received} | Novas: ${inserted} | Existentes ignoradas: ${ignored} | Rejeitadas: ${rejected}.`;
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
    setStatus('Enviando arquivo e processando no PostgreSQL...', 'info');

    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), 10 * 60 * 1000);
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
          : error?.message || error?.details || body?.message || `Falha no upload (HTTP ${response.status}).`;
        throw new Error(message);
      }

      setStatus(reportMessage(body), 'success');

      try {
        window.dispatchEvent(new CustomEvent('gestao:upload-concluido', {
          detail: {type: 'upload-concluido', at: new Date().toISOString()},
        }));
      } catch (_) {}

      window.setTimeout(() => window.location.reload(), 1200);
    } catch (error) {
      console.error('upload_flow_error', error);
      if (error?.name === 'AbortError') {
        setStatus('A importação ultrapassou 10 minutos. Verifique os logs e o histórico antes de tentar novamente.', 'error');
      } else {
        setStatus(error?.message || 'Não foi possível concluir a importação.', 'error');
      }
    } finally {
      window.clearTimeout(timeoutId);
      requestInProgress = false;
      setLoading(false);
    }
  }

  // Captura o clique antes do listener antigo do app.js e executa um único fluxo.
  document.addEventListener('click', (event) => {
    const button = event.target?.closest?.('#btnUpload');
    if (!button) return;
    event.preventDefault();
    event.stopPropagation();
    event.stopImmediatePropagation();
    executeUpload();
  }, true);

  document.addEventListener('change', (event) => {
    if (event.target?.id === 'uploadMode') updateModeUi();
  });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', updateModeUi, {once: true});
  } else {
    updateModeUi();
  }

  window.executeLeadsUpload = executeUpload;
})();