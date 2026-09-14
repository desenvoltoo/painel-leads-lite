(() => {
  'use strict';

  const $ = (s) => document.querySelector(s);
  const $$ = (s) => [...document.querySelectorAll(s)];
  const nf = new Intl.NumberFormat('pt-BR');
  const reduceMotion = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
  let kpiTimer;
  let lastFocusedElement = null;

  const FILTER_DEFS = [
    { id: 'fBusca', label: 'Busca', type: 'text', primary: true },
    { id: 'fCurso', label: 'Curso', type: 'multi', primary: true },
    { id: 'fPolo', label: 'Polo', type: 'multi', primary: true },
    { id: 'fModalidade', label: 'Modalidade', type: 'multi', primary: true },
    { id: 'fStatus', label: 'Status', type: 'multi', primary: true },
    { id: 'fDataDisparoSituacao', label: 'Disparo', type: 'select', primary: true },
    { id: 'fGraduacao', label: 'Graduação', type: 'multi', primary: false },
    { id: 'fConclusao', label: 'Conclusão', type: 'multi', primary: false },
    { id: 'fTurno', label: 'Turno', type: 'multi', primary: false },
    { id: 'fOrigem', label: 'Origem', type: 'multi', primary: false },
    { id: 'fConsultorDisparo', label: 'Consultor disparo', type: 'multi', primary: false },
    { id: 'fConsultorComercial', label: 'Consultor comercial', type: 'multi', primary: false },
    { id: 'fCanal', label: 'Canal', type: 'multi', primary: false },
    { id: 'fCampanha', label: 'Campanha', type: 'multi', primary: false },
    { id: 'fTipoDisparo', label: 'Tipo de disparo', type: 'multi', primary: false },
    { id: 'fTipoNegocio', label: 'Tipo de negócio', type: 'multi', primary: false },
    { id: 'fIni', label: 'Inscrição inicial', type: 'date', primary: false },
    { id: 'fFim', label: 'Inscrição final', type: 'date', primary: false },
    { id: 'fMesDisparo', label: 'Mês do disparo', type: 'month', primary: false },
    { id: 'fMatriculado', label: 'Matriculado', type: 'select', primary: false },
  ];

  function installBranding() {
    const faviconHref = '/static/icons/favicon.svg?v=ui-v3';
    const head = document.head;
    if (head) {
      let icon = head.querySelector('link[data-ops-favicon]');
      if (!icon) {
        icon = document.createElement('link');
        icon.rel = 'icon';
        icon.type = 'image/svg+xml';
        icon.dataset.opsFavicon = 'true';
        head.appendChild(icon);
      }
      icon.href = faviconHref;
    }

    document.title = 'Painel de Leads';

    const logo = $('.ops-logo');
    if (logo && !logo.querySelector('svg')) {
      logo.innerHTML = `
        <svg width="21" height="21" viewBox="0 0 24 24" fill="none" aria-hidden="true">
          <path d="M5 6h14M5 12h9M5 18h6" stroke="currentColor" stroke-width="2.15" stroke-linecap="round"/>
          <circle cx="18" cy="18" r="3.15" fill="currentColor"/>
          <path d="m16.7 18 1 1 2-2.3" fill="none" stroke="#2563eb" stroke-width="1.25" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>`;
    }
  }

  function animateNumber(el, target) {
    if (!el) return;
    const finalValue = Number(target || 0);
    if (!Number.isFinite(finalValue) || reduceMotion) {
      el.textContent = nf.format(finalValue || 0);
      el.dataset.numberValue = String(finalValue || 0);
      return;
    }

    const from = Number(el.dataset.numberValue || 0);
    if (from === finalValue) {
      el.textContent = nf.format(finalValue);
      return;
    }

    const started = performance.now();
    const duration = 360;
    const diff = finalValue - from;
    const ease = (t) => 1 - Math.pow(1 - t, 3);

    const tick = (now) => {
      const progress = Math.min(1, (now - started) / duration);
      el.textContent = nf.format(Math.round(from + diff * ease(progress)));
      if (progress < 1) requestAnimationFrame(tick);
      else el.dataset.numberValue = String(finalValue);
    };
    requestAnimationFrame(tick);
  }

  function number(id, value) { animateNumber($(id), value); }
  function text(id, value, fallback = '—') {
    const el = $(id);
    if (el) el.textContent = value || fallback;
  }

  function currentPayload() {
    try {
      const payload = typeof window.buildLeadsParams === 'function' ? window.buildLeadsParams() : {};
      delete payload.limit;
      delete payload.offset;
      delete payload.order_by;
      delete payload.order_dir;
      return payload;
    } catch (_) {
      return {};
    }
  }

  async function postJson(path, payload) {
    const response = await fetch(path, {
      method: 'POST',
      credentials: 'same-origin',
      cache: 'no-store',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload || {}),
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok || body?.ok === false) {
      throw new Error(body?.error?.message || body?.error || `Falha HTTP ${response.status}`);
    }
    return body;
  }

  async function countLeads(payload) {
    const body = await postJson('/api/leads/search', {
      ...(payload || {}),
      limit: 1,
      offset: 0,
      order_by: 'prioridade_disparo',
      order_dir: 'ASC',
    });
    return Number(body?.total || 0);
  }

  function isoDate(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  async function loadFallbackKpis(base) {
    const todayDate = new Date();
    const sevenDaysAgo = new Date(todayDate);
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 6);
    const backlogLimit = new Date(todayDate);
    backlogLimit.setDate(backlogLimit.getDate() - 3);

    const today = isoDate(todayDate);
    const sevenStart = isoDate(sevenDaysAgo);
    const backlogEnd = isoDate(backlogLimit);

    const [total, queue, todayCount, sevenCount, enrolled, backlog, statusBody] = await Promise.all([
      countLeads(base),
      countLeads({...base, data_disparo_situacao: 'vazias'}),
      countLeads({...base, data_ini: today, data_fim: today}),
      countLeads({...base, data_ini: sevenStart, data_fim: today}),
      countLeads({...base, matriculado: 'true'}),
      countLeads({...base, data_disparo_situacao: 'vazias', data_fim: backlogEnd}),
      postJson('/api/kpis/search', base).catch(() => ({})),
    ]);

    number('#kpiCount', total);
    number('#kpiQueue', queue);
    number('#kpiToday', todayCount);
    number('#kpi7d', sevenCount);
    number('#kpiEnrolled', enrolled);
    number('#kpiBacklog', backlog);
    text('#kpiDispatchedToday', '—');
    text('#kpiConversion', 'Conversão: cálculo indisponível');
    text('#kpiAvgDelay', 'Tempo médio: cálculo indisponível');

    const top = statusBody?.top_status;
    text('#kpiTopStatus', top ? `${top.status} (${nf.format(top.cnt)})` : null);
  }

  async function loadEducationKpis() {
    $$('.ops-kpi strong, .ops-mini-metric strong').forEach((el) => el.classList.add('ops-number-loading'));
    const base = currentPayload();

    try {
      const body = await postJson('/api/kpis/education', base);
      const d = body.data || body;
      number('#kpiCount', d.total);
      number('#kpiQueue', d.fila_disparo);
      number('#kpiToday', d.inscritos_hoje);
      number('#kpi7d', d.inscritos_7_dias);
      number('#kpiDispatchedToday', d.disparados_hoje);
      number('#kpiEnrolled', d.matriculas);
      number('#kpiBacklog', d.backlog_3_dias);
      text('#kpiConversion', `Conversão: ${Number(d.taxa_matricula || 0).toLocaleString('pt-BR',{minimumFractionDigits:1,maximumFractionDigits:1})}%`);
      text('#kpiAvgDelay', `Tempo médio: ${Number(d.tempo_medio_disparo_dias || 0).toLocaleString('pt-BR',{minimumFractionDigits:1,maximumFractionDigits:1})} dias`);
      text('#topCourse', d.top_curso?.nome ? `${d.top_curso.nome} (${nf.format(d.top_curso.total)})` : null);
      text('#topOrigin', d.top_origem?.nome ? `${d.top_origem.nome} (${nf.format(d.top_origem.total)})` : null);
      text('#topModality', d.top_modalidade?.nome ? `${d.top_modalidade.nome} (${nf.format(d.top_modalidade.total)})` : null);
    } catch (err) {
      console.warn('Endpoint educacional indisponível; usando fallback:', err);
      await loadFallbackKpis(base).catch((fallbackError) => console.error('Falha também no fallback dos KPIs:', fallbackError));
    } finally {
      $$('.ops-kpi strong, .ops-mini-metric strong').forEach((el) => el.classList.remove('ops-number-loading'));
    }
  }

  function scheduleKpis() {
    clearTimeout(kpiTimer);
    kpiTimer = setTimeout(loadEducationKpis, 260);
  }

  function getMultiValues(el) {
    if (!el) return [];
    const ts = el.tomselect;
    if (ts) {
      const value = ts.getValue();
      if (Array.isArray(value)) return value.filter(Boolean);
      if (typeof value === 'string' && value) return value.split(',').filter(Boolean);
    }
    return [...(el.selectedOptions || [])].map((option) => option.value).filter(Boolean);
  }

  function selectedLabel(el, value) {
    if (!el) return value;
    const ts = el.tomselect;
    if (ts?.options?.[value]) return String(ts.options[value].text ?? value);
    const option = [...(el.options || [])].find((item) => item.value === value);
    return option?.textContent?.trim() || value;
  }

  function displayDate(value) {
    if (!value) return value;
    if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return value.split('-').reverse().join('/');
    if (/^\d{4}-\d{2}$/.test(value)) {
      const [year, month] = value.split('-');
      return `${month}/${year}`;
    }
    return value;
  }

  function activeValues(def) {
    const el = document.getElementById(def.id);
    if (!el) return [];

    if (def.type === 'multi') {
      return getMultiValues(el).map((value) => ({ value, text: selectedLabel(el, value) }));
    }

    const value = String(el.value || '').trim();
    if (!value) return [];
    if (def.type === 'select') return [{ value, text: selectedLabel(el, value) }];
    if (def.type === 'date' || def.type === 'month') return [{ value, text: displayDate(value) }];
    return [{ value, text: value }];
  }

  function clearFilterValue(def, value) {
    const el = document.getElementById(def.id);
    if (!el) return;

    if (def.type === 'multi') {
      if (el.tomselect) el.tomselect.removeItem(value, true);
      else [...el.options].forEach((option) => { if (option.value === value) option.selected = false; });
    } else {
      el.value = '';
      el.dispatchEvent(new Event('change', { bubbles: true }));
    }
  }

  function updateAdvancedCount() {
    const activeAdvanced = FILTER_DEFS.filter((def) => !def.primary && activeValues(def).length > 0).length;
    const badge = $('#advancedFilterCount');
    if (!badge) return;
    badge.textContent = String(activeAdvanced);
    badge.hidden = activeAdvanced === 0;
  }

  function syncQuickView() {
    const situation = $('#fDataDisparoSituacao')?.value || '';
    const ini = $('#fIni')?.value || '';
    const fim = $('#fFim')?.value || '';
    const today = isoDate(new Date());

    $$('.quick-action').forEach((button) => button.classList.remove('active'));
    if (situation === 'vazias' && !ini && !fim) $('#quickQueue')?.classList.add('active');
    else if (!situation && ini === today && fim === today) $('#quickToday')?.classList.add('active');
    else $('#quickClear')?.classList.add('active');
  }

  function renderActiveFilters() {
    const bar = $('#activeFilterBar');
    const container = $('#activeFilterChips');
    if (!bar || !container) return;

    container.textContent = '';
    let count = 0;

    FILTER_DEFS.forEach((def) => {
      activeValues(def).forEach(({ value, text: valueText }) => {
        count += 1;
        const chip = document.createElement('span');
        chip.className = 'ops-filter-chip';
        chip.title = `${def.label}: ${valueText}`;

        const label = document.createElement('span');
        label.textContent = `${def.label}: ${valueText}`;

        const remove = document.createElement('button');
        remove.type = 'button';
        remove.setAttribute('aria-label', `Remover filtro ${def.label}: ${valueText}`);
        remove.textContent = '×';
        remove.addEventListener('click', () => {
          clearFilterValue(def, value);
          setTimeout(() => {
            $('#btnApply')?.click();
            renderActiveFilters();
          }, 0);
        });

        chip.append(label, remove);
        container.appendChild(chip);
      });
    });

    bar.hidden = count === 0;
    updateAdvancedCount();
    syncQuickView();
  }

  function animateTableRows() {
    if (reduceMotion) return;
    $$('#tbl tbody tr').forEach((row, index) => {
      if (row.querySelector('.table-feedback') || row.dataset.opsAnimated === '1') return;
      row.dataset.opsAnimated = '1';
      row.classList.add('ops-row-enter');
      row.style.animationDelay = `${Math.min(index * 8, 90)}ms`;
    });
  }

  function enhanceRows() {
    const rows = $$('#tbl tbody tr');
    let ready = 0;
    rows.forEach((row) => {
      if (row.querySelector('.table-feedback')) return;
      const value = (row.cells?.[13]?.textContent || '').trim().toLowerCase();
      const isReady = !value || value === '-' || value.includes('sem disparo') || value.includes('pronto para disparo');
      row.classList.toggle('ops-ready-row', isReady);
      row.classList.toggle('ops-sent-row', !isReady);
      if (isReady) ready += 1;
    });
    number('#pageReadyCount', ready);
    animateTableRows();
  }

  function initRevealAnimations() {
    const elements = [
      $('.ops-page-intro'),
      ...$$('.ops-kpi'),
      $('.ops-more-metrics-wrap'),
      $('.ops-filter-shell'),
      $('.ops-table-card'),
    ].filter(Boolean);

    if (reduceMotion) {
      elements.forEach((el) => el.classList.add('ops-visible'));
      return;
    }

    elements.forEach((el, index) => {
      el.classList.add('ops-reveal');
      el.style.setProperty('--ops-delay', `${Math.min(index * 32, 180)}ms`);
    });

    const observer = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        entry.target.classList.add('ops-visible');
        observer.unobserve(entry.target);
      });
    }, { threshold: .05, rootMargin: '30px 0px -10px' });

    elements.forEach((el) => observer.observe(el));
  }

  function setQueueFilter(mode) {
    const situation = $('#fDataDisparoSituacao');
    const ini = $('#fIni');
    const fim = $('#fFim');

    if (mode === 'queue') {
      if (situation) situation.value = 'vazias';
      if (ini) ini.value = '';
      if (fim) fim.value = '';
    } else if (mode === 'today') {
      const today = isoDate(new Date());
      if (situation) situation.value = '';
      if (ini) ini.value = today;
      if (fim) fim.value = today;
    } else {
      if (situation) situation.value = '';
      if (ini) ini.value = '';
      if (fim) fim.value = '';
    }

    $('#btnApply')?.click();
    setTimeout(renderActiveFilters, 0);
    scheduleKpis();
  }

  function setBodyLocked(locked) {
    document.body.classList.toggle('ops-lock-scroll', locked);
  }

  function openDrawer() {
    closeImportModal(false);
    const drawer = $('#advancedFilterDrawer');
    const backdrop = $('#advancedFiltersBackdrop');
    if (!drawer || !backdrop) return;
    lastFocusedElement = document.activeElement;
    backdrop.hidden = false;
    drawer.classList.add('is-open');
    drawer.setAttribute('aria-hidden', 'false');
    $('#btnMoreFilters')?.setAttribute('aria-expanded', 'true');
    setBodyLocked(true);
    setTimeout(() => $('#btnCloseAdvancedFilters')?.focus(), 40);
  }

  function closeDrawer(restoreFocus = true) {
    const drawer = $('#advancedFilterDrawer');
    const backdrop = $('#advancedFiltersBackdrop');
    if (!drawer || !backdrop) return;
    drawer.classList.remove('is-open');
    drawer.setAttribute('aria-hidden', 'true');
    $('#btnMoreFilters')?.setAttribute('aria-expanded', 'false');
    backdrop.hidden = true;
    if (!$('#importModal')?.classList.contains('is-open')) setBodyLocked(false);
    if (restoreFocus && lastFocusedElement instanceof HTMLElement) lastFocusedElement.focus();
  }

  function openImportModal() {
    closeDrawer(false);
    const modal = $('#importModal');
    const backdrop = $('#importBackdrop');
    if (!modal || !backdrop) return;
    lastFocusedElement = document.activeElement;
    backdrop.hidden = false;
    modal.classList.add('is-open');
    modal.setAttribute('aria-hidden', 'false');
    setBodyLocked(true);
    setTimeout(() => $('#btnImportClose')?.focus(), 40);
  }

  function closeImportModal(restoreFocus = true) {
    const modal = $('#importModal');
    const backdrop = $('#importBackdrop');
    if (!modal || !backdrop) return;
    modal.classList.remove('is-open');
    modal.setAttribute('aria-hidden', 'true');
    backdrop.hidden = true;
    if (!$('#advancedFilterDrawer')?.classList.contains('is-open')) setBodyLocked(false);
    if (restoreFocus && lastFocusedElement instanceof HTMLElement) lastFocusedElement.focus();
  }

  function toggleSecondaryMetrics() {
    const button = $('#btnToggleInsights');
    const panel = $('#secondaryMetrics');
    if (!button || !panel) return;
    const expanded = button.getAttribute('aria-expanded') === 'true';
    button.setAttribute('aria-expanded', String(!expanded));
    panel.hidden = expanded;
    button.querySelector('span:first-child').textContent = expanded ? 'Ver mais indicadores' : 'Ocultar indicadores';
  }

  function togglePriorityPopover(force) {
    const button = $('#btnPriorityInfo');
    const popover = $('#priorityPopover');
    if (!button || !popover) return;
    const next = typeof force === 'boolean' ? force : button.getAttribute('aria-expanded') !== 'true';
    button.setAttribute('aria-expanded', String(next));
    popover.hidden = !next;
  }

  function initFilterInteractions() {
    $('#fBusca')?.addEventListener('keydown', (event) => {
      if (event.key !== 'Enter') return;
      event.preventDefault();
      $('#btnApply')?.click();
    });

    $('#filterPanel')?.addEventListener('change', () => setTimeout(renderActiveFilters, 0));
    $('#advancedFilterDrawer')?.addEventListener('change', () => setTimeout(renderActiveFilters, 0));
    $('#fBusca')?.addEventListener('input', () => setTimeout(renderActiveFilters, 0));

    $('#btnApply')?.addEventListener('click', () => {
      setTimeout(renderActiveFilters, 20);
      scheduleKpis();
    });
    $('#btnClear')?.addEventListener('click', () => {
      setTimeout(renderActiveFilters, 30);
      scheduleKpis();
    });

    $('#btnMoreFilters')?.addEventListener('click', openDrawer);
    $('#btnCloseAdvancedFilters')?.addEventListener('click', () => closeDrawer());
    $('#advancedFiltersBackdrop')?.addEventListener('click', () => closeDrawer());
    $('#btnApplyAdvanced')?.addEventListener('click', () => {
      $('#btnApply')?.click();
      closeDrawer();
    });
    $('#btnClearAdvanced')?.addEventListener('click', () => {
      $('#btnClear')?.click();
      setTimeout(renderActiveFilters, 30);
    });

    $('#savedFilterSelect')?.addEventListener('change', () => setTimeout(renderActiveFilters, 120));
  }

  function initModalInteractions() {
    $('#btnImportOpen')?.addEventListener('click', openImportModal);
    $('#btnImportClose')?.addEventListener('click', () => closeImportModal());
    $('#btnImportCancel')?.addEventListener('click', () => closeImportModal());
    $('#importBackdrop')?.addEventListener('click', () => closeImportModal());
    window.addEventListener('gestao:upload-concluido', () => {
      scheduleKpis();
      setTimeout(() => closeImportModal(false), 600);
    });
  }

  function initGlobalInteractions() {
    $('#quickQueue')?.addEventListener('click', () => setQueueFilter('queue'));
    $('#quickToday')?.addEventListener('click', () => setQueueFilter('today'));
    $('#quickClear')?.addEventListener('click', () => setQueueFilter('all'));
    $('#btnToggleInsights')?.addEventListener('click', toggleSecondaryMetrics);
    $('#btnPriorityInfo')?.addEventListener('click', (event) => {
      event.stopPropagation();
      togglePriorityPopover();
    });

    document.addEventListener('click', (event) => {
      const popover = $('#priorityPopover');
      const help = $('.ops-priority-help');
      if (!popover?.hidden && help && !help.contains(event.target)) togglePriorityPopover(false);
    });

    document.addEventListener('keydown', (event) => {
      if (event.key !== 'Escape') return;
      if ($('#importModal')?.classList.contains('is-open')) closeImportModal();
      else if ($('#advancedFilterDrawer')?.classList.contains('is-open')) closeDrawer();
      else togglePriorityPopover(false);
    });

    ['#btnReload','#btnPrevPage','#btnNextPage'].forEach((id) => $(id)?.addEventListener('click', scheduleKpis));
    $('#fLimit')?.addEventListener('change', scheduleKpis);
  }

  function init() {
    installBranding();
    initRevealAnimations();
    initFilterInteractions();
    initModalInteractions();
    initGlobalInteractions();

    const tbody = $('#tbl tbody');
    if (tbody) new MutationObserver(enhanceRows).observe(tbody, { childList: true, subtree: true, characterData: true });

    enhanceRows();
    setTimeout(renderActiveFilters, 180);
    loadEducationKpis();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, { once: true });
  else init();
})();
