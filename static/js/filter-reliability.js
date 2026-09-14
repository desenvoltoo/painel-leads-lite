(() => {
  'use strict';

  const normalizeLogicalValue = (value) => String(value ?? '')
    .trim()
    .replace(/\s+/g, ' ')
    .toUpperCase();

  const isNoDispatchDate = (value) => {
    if (value === null || value === undefined) return true;
    const text = String(value).trim().toLowerCase();
    return !text || text === '-infinity' || text.startsWith('0001-01-01');
  };

  const originalSetTomValues = window.setTomValues;
  if (typeof originalSetTomValues === 'function') {
    window.setTomValues = function reliableSetTomValues(ts, values) {
      if (!ts) return;

      const incoming = Array.isArray(values)
        ? values.map((item) => String(item ?? '').trim()).filter(Boolean)
        : [];

      const available = Object.keys(ts.options || {});
      const canonicalByLogicalValue = new Map();
      available.forEach((value) => {
        canonicalByLogicalValue.set(normalizeLogicalValue(value), value);
      });

      const reconciled = [];
      incoming.forEach((value) => {
        const canonical = canonicalByLogicalValue.get(normalizeLogicalValue(value));
        if (canonical && !reconciled.includes(canonical)) {
          reconciled.push(canonical);
        }
      });

      return originalSetTomValues(ts, reconciled);
    };
  }

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
  window.__normalizeLeadFilterValue = normalizeLogicalValue;
})();
