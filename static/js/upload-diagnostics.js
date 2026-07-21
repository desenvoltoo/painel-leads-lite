(() => {
  'use strict';

  const originalFetch = window.fetch.bind(window);
  let lastUploadError = null;

  function asText(value, fallback = '') {
    if (typeof value === 'string') return value;
    if (value === null || value === undefined) return fallback;
    try {
      return JSON.stringify(value);
    } catch (_) {
      return String(value);
    }
  }

  function extractMessage(body, status) {
    const error = body?.error;
    const details = error?.details || body?.details;
    const detailText = Array.isArray(details)
      ? details.filter(Boolean).map((item) => asText(item)).join(' · ')
      : typeof details === 'string'
        ? details
        : asText(details?.message || details?.detail || details?.hint || '');

    const base = asText(
      error?.message ||
      (typeof error === 'string' ? error : '') ||
      body?.message ||
      `Falha HTTP ${status}`,
      `Falha HTTP ${status}`
    );

    const correlationId = asText(
      body?.correlationId || error?.correlationId || '',
      ''
    );

    const code = asText(
      body?.code || error?.code || body?.error_type || `HTTP_${status}`,
      `HTTP_${status}`
    );

    const message = detailText && !base.includes(detailText)
      ? `${base} Detalhe: ${detailText}`
      : base;

    const formattedError = [
      code ? `[${code}]` : '',
      message,
      correlationId ? `Código de correlação: ${correlationId}` : '',
    ].filter(Boolean).join(' ');

    return {
      code: code || `HTTP_${status}`,
      message: message || `Falha HTTP ${status}`,
      formattedError: formattedError || `Falha HTTP ${status}`,
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
      } catch (_) {
        lastUploadError = {
          code: `HTTP_${response.status}`,
          message: `Falha HTTP ${response.status}`,
          formattedError: `Falha HTTP ${response.status}`,
          correlationId: '',
        };
      }

      window.__LAST_UPLOAD_ERROR__ = lastUploadError;

      setTimeout(() => {
        const statusBox = document.querySelector('#uploadStatus');
        if (!statusBox || !lastUploadError) return;
        statusBox.textContent = lastUploadError.formattedError;
        statusBox.className = 'alert alert-danger';
        statusBox.title = lastUploadError.formattedError;
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
