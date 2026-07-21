(() => {
  'use strict';

  function selectedMode() {
    return document.querySelector('#uploadMode')?.value === 'somente_novos'
      ? 'somente_novos'
      : 'normal';
  }

  function refreshModeUi() {
    const mode = selectedMode();
    const help = document.querySelector('#uploadModeHelp');
    const button = document.querySelector('#btnUpload');
    const policy = document.querySelector('.import-policy');

    if (help) {
      help.textContent = mode === 'somente_novos'
        ? 'Somente novos: importa apenas quem não existe. Compara primeiro pelo celular e depois pelo CPF. Limite: 10.000 linhas.'
        : 'Importação normal: importa novos e atualiza os existentes conforme as regras do arquivo.';
    }

    if (button && !button.classList.contains('is-loading')) {
      button.textContent = mode === 'somente_novos'
        ? 'Importar somente novos'
        : 'Importar planilha';
    }

    if (policy) {
      policy.innerHTML = mode === 'somente_novos'
        ? '<strong>Regra desta importação</strong><span>Somente leads inexistentes serão inseridos.</span><span>Leads encontrados pelo celular ou pelo CPF serão ignorados integralmente.</span>'
        : '<strong>Regra desta importação</strong><span>Dados pessoais e acadêmicos vazios preservam o valor atual.</span><span>Campos operacionais presentes no arquivo substituem o banco; célula vazia limpa o campo.</span>';
    }
  }

  function installDirectUploader() {
    if (typeof window.apiPostForm !== 'function') {
      console.error('apiPostForm não está disponível para configurar o modo de importação.');
      return;
    }

    window.uploadDirectToServer = function uploadDirectToServerByMode(file, source) {
      const formData = new FormData();
      formData.append('file', file);
      if (source) formData.append('source', source);
      formData.append('import_mode', selectedMode());

      const endpoint = selectedMode() === 'somente_novos'
        ? '/api/upload/somente-novos'
        : '/api/upload';

      return window.apiPostForm(endpoint, formData);
    };
  }

  document.addEventListener('DOMContentLoaded', () => {
    installDirectUploader();
    refreshModeUi();

    document.querySelector('#uploadMode')?.addEventListener('change', () => {
      refreshModeUi();
      window.dispatchEvent(new CustomEvent('upload:mode-changed', {
        detail: { mode: selectedMode() }
      }));
    });
  });
})();