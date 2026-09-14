(() => {
  'use strict';

  const isNoDispatchDate = (value) => {
    if (value === null || value === undefined) return true;
    const text = String(value).trim().toLowerCase();
    return !text || text === '-infinity' || text.startsWith('0001-01-01');
  };

  const originalApiPostJson = window.apiPostJson;
  if (typeof originalApiPostJson === 'function') {
    window.apiPostJson = async function reliableApiPostJson(path, payload = {}) {
      try {
        return await originalApiPostJson(path, payload);
      } catch (error) {
        if (String(path || '').includes('/api/kpis/search')) {
          console.warn('KPIs indisponíveis; mantendo a listagem de leads.', error);
          return {
            ok: false,
            total: 0,
            top_status: null,
            degraded: true,
            error: error?.message || 'Falha ao carregar KPIs',
          };
        }
        throw error;
      }
    };
  }

  const originalFmtDate = window.fmtDate;
  if (typeof originalFmtDate === 'function') {
    window.fmtDate = function reliableFmtDate(value) {
      if (isNoDispatchDate(value)) return '-';
      return originalFmtDate(value);
    };
  }

  window.fmtDataDisparo = function reliableFmtDataDisparo(value) {
    return isNoDispatchDate(value)
      ? 'Sem disparo'
      : (typeof window.fmtDate === 'function' ? window.fmtDate(value) : String(value));
  };

  window.__isNoDispatchDate = isNoDispatchDate;
})();
