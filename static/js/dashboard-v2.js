(() => {
  'use strict';

  const $ = (s) => document.querySelector(s);
  const nf = new Intl.NumberFormat('pt-BR');
  const reduceMotion = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
  let kpiTimer;

  function installBranding() {
    const faviconHref = `/static/icons/favicon.svg?v=ui-clean-1`;
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

      let shortcut = head.querySelector('link[data-ops-shortcut]');
      if (!shortcut) {
        shortcut = document.createElement('link');
        shortcut.rel = 'shortcut icon';
        shortcut.dataset.opsShortcut = 'true';
        head.appendChild(shortcut);
      }
      shortcut.href = faviconHref;

      let theme = head.querySelector('meta[name="theme-color"]');
      if (!theme) {
        theme = document.createElement('meta');
        theme.name = 'theme-color';
        head.appendChild(theme);
      }
      theme.content = '#f8fbff';
    }

    if (document.title !== 'Painel de Leads') document.title = 'Painel de Leads';

    const logo = $('.ops-logo');
    if (logo && !logo.querySelector('svg')) {
      logo.innerHTML = `
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden="true">
          <path d="M5 6h14M5 12h9M5 18h6" stroke="currentColor" stroke-width="2.15" stroke-linecap="round"/>
          <circle cx="18" cy="18" r="3.15" fill="currentColor"/>
          <path d="m16.7 18 1 1 2-2.3" fill="none" stroke="#2563eb" stroke-width="1.25" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>`;
      logo.setAttribute('title', 'Painel de Leads');
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
    const duration = 460;
    const diff = finalValue - from;
    const ease = (t) => 1 - Math.pow(1 - t, 3);

    const tick = (now) => {
      const progress = Math.min(1, (now - started) / duration);
      const current = Math.round(from + diff * ease(progress));
      el.textContent = nf.format(current);
      if (progress < 1) requestAnimationFrame(tick);
      else el.dataset.numberValue = String(finalValue);
    };
    requestAnimationFrame(tick);
  }

  function number(id, value) {
    animateNumber($(id), value);
  }

  function text(id, value, fallback = '—') {
    const el = $(id);
    if (el) el.textContent = value || fallback;
  }

  function currentPayload() {
    try {
      const payload = typeof window.buildLeadsParams === 'function'
        ? window.buildLeadsParams()
        : {};
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

    const [queue, todayCount, sevenCount, enrolled, backlog, statusBody] = await Promise.all([
      countLeads({...base, data_disparo_situacao: 'vazias'}),
      countLeads({...base, data_ini: today, data_fim: today}),
      countLeads({...base, data_ini: sevenStart, data_fim: today}),
      countLeads({...base, matriculado: 'true'}),
      countLeads({...base, data_disparo_situacao: 'vazias', data_fim: backlogEnd}),
      postJson('/api/kpis/search', base).catch(() => ({})),
    ]);

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
    document.querySelectorAll('.ops-kpi strong').forEach((el) => el.classList.add('ops-number-loading'));

    const base = currentPayload();
    try {
      const body = await postJson('/api/kpis/education', base);
      const d = body.data || body;
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
      await loadFallbackKpis(base).catch((fallbackError) => {
        console.error('Falha também no fallback dos KPIs:', fallbackError);
        document.querySelectorAll('.ops-kpi strong').forEach((el) => {
          if (el.textContent.trim() === '—') el.textContent = 'Erro';
        });
      });
    } finally {
      document.querySelectorAll('.ops-kpi strong').forEach((el) => el.classList.remove('ops-number-loading'));
    }
  }

  function scheduleKpis() {
    clearTimeout(kpiTimer);
    kpiTimer = setTimeout(loadEducationKpis, 350);
  }

  function animateTableRows() {
    if (reduceMotion) return;
    document.querySelectorAll('#tbl tbody tr').forEach((row, index) => {
      if (row.querySelector('.table-feedback') || row.dataset.opsAnimated === '1') return;
      row.dataset.opsAnimated = '1';
      row.classList.add('ops-row-enter');
      row.style.animationDelay = `${Math.min(index * 14, 180)}ms`;
    });
  }

  function enhanceRows() {
    const rows = document.querySelectorAll('#tbl tbody tr');
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
      $('.ops-hero'),
      ...document.querySelectorAll('.ops-kpi'),
      ...document.querySelectorAll('.ops-insights > div'),
      ...document.querySelectorAll('.ops-grid-top > .ops-card'),
      $('.ops-filter-card'),
      $('.ops-table-card'),
    ].filter(Boolean);

    if (reduceMotion) {
      elements.forEach((el) => el.classList.add('ops-visible'));
      return;
    }

    elements.forEach((el, index) => {
      el.classList.add('ops-reveal');
      el.style.setProperty('--ops-delay', `${Math.min(index * 45, 360)}ms`);
    });

    const observer = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        entry.target.classList.add('ops-visible');
        observer.unobserve(entry.target);
      });
    }, {threshold: .08, rootMargin: '40px 0px -20px'});

    elements.forEach((el) => observer.observe(el));
  }

  function initHeroMotion() {
    if (reduceMotion) return;
    const hero = $('.ops-hero');
    const illustration = hero?.querySelector('svg[aria-label="Ilustração de painel de leads"]');
    if (!hero || !illustration) return;

    hero.addEventListener('pointermove', (event) => {
      const rect = hero.getBoundingClientRect();
      const x = ((event.clientX - rect.left) / rect.width - .5) * 5;
      const y = ((event.clientY - rect.top) / rect.height - .5) * 4;
      illustration.style.transform = `translate3d(${x}px, ${y}px, 0)`;
    });
    hero.addEventListener('pointerleave', () => {
      illustration.style.transform = '';
    });
  }

  function setQueueFilter(mode) {
    const situation = $('#fDataDisparoSituacao');
    const ini = $('#fIni');
    const fim = $('#fFim');
    document.querySelectorAll('.quick-action').forEach((button) => button.classList.remove('active'));

    if (mode === 'queue') {
      if (situation) situation.value = 'vazias';
      if (ini) ini.value = '';
      if (fim) fim.value = '';
      $('#quickQueue')?.classList.add('active');
    } else if (mode === 'today') {
      const today = isoDate(new Date());
      if (situation) situation.value = '';
      if (ini) ini.value = today;
      if (fim) fim.value = today;
      $('#quickToday')?.classList.add('active');
    } else {
      if (situation) situation.value = '';
      if (ini) ini.value = '';
      if (fim) fim.value = '';
      $('#quickClear')?.classList.add('active');
    }

    $('#btnApply')?.click();
    scheduleKpis();
  }

  function init() {
    installBranding();
    initRevealAnimations();
    initHeroMotion();

    const legacyCount = $('#kpiCount');
    if (legacyCount) {
      legacyCount.hidden = true;
      legacyCount.style.setProperty('display', 'none', 'important');
    }

    $('#quickQueue')?.addEventListener('click', () => setQueueFilter('queue'));
    $('#quickToday')?.addEventListener('click', () => setQueueFilter('today'));
    $('#quickClear')?.addEventListener('click', () => setQueueFilter('all'));
    $('#toggleFilters')?.addEventListener('click', () => {
      const panel = $('#filterPanel');
      const toggle = $('#toggleFilters');
      const collapsed = panel?.classList.toggle('collapsed');
      toggle?.setAttribute('aria-expanded', String(!collapsed));
      text('#filterToggleLabel', collapsed ? 'Expandir' : 'Recolher');
    });

    ['#btnApply','#btnReload','#btnClear','#btnPrevPage','#btnNextPage'].forEach((id) => $(id)?.addEventListener('click', scheduleKpis));
    $('#filterPanel')?.addEventListener('change', scheduleKpis);
    $('#fBusca')?.addEventListener('input', scheduleKpis);
    window.addEventListener('gestao:upload-concluido', scheduleKpis);

    const tbody = $('#tbl tbody');
    if (tbody) {
      new MutationObserver(enhanceRows).observe(tbody, {childList:true, subtree:true, characterData:true});
    }

    enhanceRows();
    loadEducationKpis();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, {once:true});
  else init();
})();
