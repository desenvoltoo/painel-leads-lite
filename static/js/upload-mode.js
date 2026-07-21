(() => {
  'use strict';

  const nativeFetch = window.fetch.bind(window);

  function selectedMode() {
    return document.querySelector('#uploadMode')?.value || 'normal';
  }

  function isOnlyNewMode() {
    return selectedMode() === 'somente_novos';
  }

  function refreshModeHelp() {
    const onlyNew = isOnlyNewMode();
    const help = document.querySelector('#uploadModeHelp');
    const btn = document.querySelector('#btnUpload');

    if (help) {
      help.textContent = onlyNew
        ? 'Somente novos: compara primeiro pelo celular e depois pelo CPF. Registros existentes serão ignorados e nunca atualizados. Limite: 10.000 linhas.'
        : 'Importação normal: importa novos e atualiza registros existentes conforme as regras de preservação e substituição.';
    }

    if (btn && !btn.classList.contains('is-loading')) {
      btn.textContent = onlyNew ? 'Importar somente novos' : 'Importar planilha';
    }

    refreshPreviewLabels();
  }

  function adaptPreviewPayload(payload) {
    if (!isOnlyNewMode() || !payload?.ok || !payload?.data) return payload;

    const data = { ...payload.data };
    data.ignorados = Number(data.existentes || 0);
    data.alterados = 0;
    data.sem_mudanca = 0;
    data.limpezas = 0;
    data.modo_importacao = 'somente_novos';
    data.mensagem_modo = 'Registros existentes serão ignorados. Somente leads não encontrados por celular e CPF serão inseridos.';

    return { ...payload, data };
  }

  function refreshPreviewLabels() {
    const panel = document.querySelector('#uploadPreview');
    if (!panel) return;

    const onlyNew = isOnlyNewMode();
    const cards = [...panel.querySelectorAll('.upload-preview-db-metrics article')];

    for (const card of cards) {
      const label = card.querySelector('span');
      const value = card.querySelector('strong');
      if (!label || !value) continue;

      const text = label.textContent.trim();
      if (text === 'Serão alterados' || text === 'Serão ignorados') {
        label.textContent = onlyNew ? 'Serão ignorados' : 'Serão alterados';
        if (onlyNew) {
          const existingCard = cards.find(item => item.querySelector('span')?.textContent.trim() === 'Existentes');
          value.textContent = existingCard?.querySelector('strong')?.textContent || '0';
        }
      }
      if (onlyNew && (text === 'Sem mudança' || text === 'Limpezas previstas')) {
        value.textContent = '0';
      }
    }

    const columns = panel.querySelector('.upload-preview-columns');
    const clears = panel.querySelector('.upload-preview-clears');
    const warnings = panel.querySelector('.upload-preview-warnings');

    if (onlyNew) {
      if (columns) {
        columns.innerHTML = `
          <div><strong>Leads existentes</strong><p>Serão ignorados integralmente. Nenhum campo atual será alterado ou limpo.</p></div>
          <div><strong>Leads novos</strong><p>Somente registros não encontrados primeiro pelo celular e depois pelo CPF serão inseridos.</p></div>
        `;
      }
      if (clears) clears.hidden = true;
      if (warnings && /campos operacionais|limpos no banco/i.test(warnings.textContent || '')) {
        warnings.innerHTML = '<p>✓ Modo somente novos ativo: células vazias não alterarão registros existentes.</p>';
        warnings.className = 'upload-preview-ok';
      }

      const dbPreview = panel.querySelector('#uploadDbPreview');
      if (dbPreview && !dbPreview.querySelector('.only-new-preview-note')) {
        dbPreview.insertAdjacentHTML(
          'beforeend',
          '<div class="upload-preview-ok only-new-preview-note"><strong>Somente novos:</strong> todos os registros existentes serão ignorados; alterações e limpezas previstas são zero.</div>'
        );
      }
    } else {
      if (clears) clears.hidden = false;
      panel.querySelector('.only-new-preview-note')?.remove();
    }
  }

  window.fetch = async function(input, init) {
    let originalUrl = '';
    let method = 'GET';

    try {
      originalUrl = typeof input === 'string' ? input : input?.url;
      method = String(init?.method || (typeof input !== 'string' ? input?.method : 'GET') || 'GET').toUpperCase();

      if (method === 'POST' && originalUrl) {
        const url = new URL(originalUrl, window.location.origin);
        if (url.pathname === '/api/upload' && isOnlyNewMode()) {
          url.pathname = '/api/upload/somente-novos';
          input = typeof input === 'string' ? url.toString() : new Request(url.toString(), input);
          originalUrl = url.toString();
        }
      }
    } catch (error) {
      console.warn('Não foi possível selecionar o modo da importação.', error);
    }

    const response = await nativeFetch(input, init);

    try {
      const url = new URL(originalUrl || response.url, window.location.origin);
      if (method === 'POST' && url.pathname === '/api/upload/preview' && isOnlyNewMode()) {
        const cloned = response.clone();
        const payload = await cloned.json();
        const adapted = adaptPreviewPayload(payload);
        const headers = new Headers(response.headers);
        headers.set('Content-Type', 'application/json; charset=utf-8');
        return new Response(JSON.stringify(adapted), {
          status: response.status,
          statusText: response.statusText,
          headers,
        });
      }
    } catch (error) {
      console.warn('Não foi possível adaptar a prévia para o modo somente novos.', error);
    }

    return response;
  };

  document.addEventListener('DOMContentLoaded', () => {
    const mode = document.querySelector('#uploadMode');
    const file = document.querySelector('#uploadFile');

    mode?.addEventListener('change', () => {
      refreshModeHelp();
      if (file?.files?.length) {
        file.dispatchEvent(new Event('change', { bubbles: true }));
      }
    });

    const observer = new MutationObserver(refreshPreviewLabels);
    observer.observe(document.body, { childList: true, subtree: true });

    refreshModeHelp();
  });
})();