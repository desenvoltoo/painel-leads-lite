(() => {
  'use strict';

  const originalFetch = window.fetch.bind(window);
  let lastUploadError = null;

  function extractMessage(body, status) {
    const error = body?.error;
    const details = error?.details || body?.details;
    const detailText = Array.isArray(details)
      ? details.filter(Boolean).join(' · ')
      : typeof details === 'string'
        ? details
        : details?.message || details?.detail || details?.hint || '';
    const base = error?.message || (typeof error === 'string' ? error : '') || body?.message || `Falha HTTP ${status}`;
    const correlationId = body?.correlationId || error?.correlationId || '';
    return {
      message: detailText && !String(base).includes(detailText) ? `${base} Detalhe: ${detailText}` : base,
      correlationId,
    };
  }

  window.fetch = async (...args) => {
    const response = await originalFetch(...args);
    const requestUrl = typeof args[0] === 'string' ? args[0] : args[0]?.url || '';
    const method = String(args[1]?.method || 'GET').toUpperCase();

    if (method === 'POST' && requestUrl.includes('/api/upload') && !response.ok) {
      try {
        const body = await response.clone().json();
        lastUploadError = extractMessage(body, response.status);
        window.__LAST_UPLOAD_ERROR__ = lastUploadError;
      } catch (_) {
        lastUploadError = {message: `Falha HTTP ${response.status}`, correlationId: ''};
      }

      setTimeout(() => {
        const statusBox = document.querySelector('#uploadStatus');
        if (!statusBox || !lastUploadError) return;
        const code = lastUploadError.correlationId ? ` Código: ${lastUploadError.correlationId}` : '';
        statusBox.textContent = `${lastUploadError.message}${code}`;
        statusBox.className = 'alert alert-danger';
        statusBox.title = statusBox.textContent;
      }, 80);
    }

    return response;
  };

  document.addEventListener('click', (event) => {
    const statusBox = event.target.closest?.('#uploadStatus.alert-danger');
    if (!statusBox || !statusBox.textContent) return;
    navigator.clipboard?.writeText(statusBox.textContent).catch(() => {});
  });
})();
