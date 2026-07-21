(() => {
  const nativeFetch = window.fetch.bind(window);

  function selectedMode() {
    return document.querySelector('#uploadMode')?.value || 'normal';
  }

  function refreshModeHelp() {
    const mode = selectedMode();
    const help = document.querySelector('#uploadModeHelp');
    const btn = document.querySelector('#btnUpload');
    if (help) {
      help.textContent = mode === 'somente_novos'
        ? 'Somente novos: compara primeiro pelo celular e depois pelo CPF. Existentes serão ignorados. Limite: 10.000 linhas.'
        : 'Importação normal: atualiza registros existentes conforme as regras de preservação e substituição.';
    }
    if (btn && !btn.classList.contains('is-loading')) {
      btn.textContent = mode === 'somente_novos' ? 'Importar somente novos' : 'Importar planilha';
    }
  }

  window.fetch = function(input, init) {
    try {
      const originalUrl = typeof input === 'string' ? input : input?.url;
      const method = String(init?.method || (typeof input !== 'string' ? input?.method : 'GET') || 'GET').toUpperCase();
      if (method === 'POST' && originalUrl) {
        const url = new URL(originalUrl, window.location.origin);
        if (url.pathname === '/api/upload' && selectedMode() === 'somente_novos') {
          url.pathname = '/api/upload/somente-novos';
          input = typeof input === 'string' ? url.toString() : new Request(url.toString(), input);
        }
      }
    } catch (error) {
      console.warn('Não foi possível selecionar o modo da importação.', error);
    }
    return nativeFetch(input, init);
  };

  document.addEventListener('DOMContentLoaded', () => {
    const mode = document.querySelector('#uploadMode');
    mode?.addEventListener('change', refreshModeHelp);
    refreshModeHelp();
  });
})();
