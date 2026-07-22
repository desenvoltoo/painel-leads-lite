(() => {
  'use strict';

  const INSTITUTIONS = {
    anhanguera: {label: 'Anhanguera', short: 'A'},
    unifecaf: {label: 'UniFECAF', short: 'U'},
  };

  async function requestJson(url, options = {}) {
    const response = await fetch(url, {
      credentials: 'same-origin',
      cache: 'no-store',
      ...options,
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok || body?.ok === false) {
      throw new Error(body?.error?.message || 'Não foi possível trocar a instituição.');
    }
    return body;
  }

  function ensureStyle() {
    if (document.querySelector('#institutionSwitchStyle')) return;
    const style = document.createElement('style');
    style.id = 'institutionSwitchStyle';
    style.textContent = `
      .institution-switch{display:flex;align-items:center;gap:6px;padding:5px;border:1px solid var(--institution-border,#d9e3f0);border-radius:16px;background:#fff;box-shadow:0 6px 18px rgba(15,23,42,.07)}
      .institution-switch.is-loading{opacity:.65;pointer-events:none}
      .institution-button{display:inline-flex;align-items:center;gap:7px;min-height:38px;padding:8px 12px;border:1px solid transparent;border-radius:11px;background:transparent;color:#64748b;font:inherit;font-size:.78rem;font-weight:800;cursor:pointer;transition:transform .15s ease,background .2s ease,color .2s ease,box-shadow .2s ease}
      .institution-button:hover{transform:translateY(-1px);background:#f8fafc;color:#334155}
      .institution-button .institution-mark{display:grid;place-items:center;width:23px;height:23px;border-radius:8px;background:#e2e8f0;color:#475569;font-size:.72rem;font-weight:900}
      .institution-button.is-active{background:var(--institution-primary);color:#fff;box-shadow:0 7px 16px var(--institution-shadow)}
      .institution-button.is-active .institution-mark{background:rgba(255,255,255,.2);color:#fff}
      .institution-context-alert{margin:0 0 12px;padding:10px 12px;border:1px solid var(--institution-accent-border);border-radius:12px;background:var(--institution-accent-soft);color:var(--institution-accent-dark);font-size:.86rem}

      body.theme-anhanguera{
        --institution-primary:#f37021;
        --institution-primary-dark:#c94d0b;
        --institution-primary-soft:#fff2e8;
        --institution-accent:#ff8a3d;
        --institution-accent-dark:#9a3f08;
        --institution-accent-soft:#fff5ec;
        --institution-accent-border:#ffd1b1;
        --institution-border:#fed7bd;
        --institution-shadow:rgba(243,112,33,.28);
      }
      body.theme-unifecaf{
        --institution-primary:#0b5ed7;
        --institution-primary-dark:#084298;
        --institution-primary-soft:#eaf3ff;
        --institution-accent:#20a464;
        --institution-accent-dark:#0f6b3d;
        --institution-accent-soft:#eaf9f1;
        --institution-accent-border:#afe4c9;
        --institution-border:#bdd6f5;
        --institution-shadow:rgba(11,94,215,.26);
      }

      body.theme-anhanguera .ops-logo,
      body.theme-unifecaf .ops-logo{background:var(--institution-primary)!important;color:#fff!important}
      body.theme-anhanguera .btn-primary,
      body.theme-unifecaf .btn-primary{background:var(--institution-primary)!important;border-color:var(--institution-primary)!important;color:#fff!important}
      body.theme-anhanguera .btn-primary:hover,
      body.theme-unifecaf .btn-primary:hover{background:var(--institution-primary-dark)!important;border-color:var(--institution-primary-dark)!important}
      body.theme-anhanguera .ops-eyebrow,
      body.theme-unifecaf .ops-eyebrow{color:var(--institution-primary)!important}
      body.theme-anhanguera .ops-kpi-primary,
      body.theme-unifecaf .ops-kpi-primary{border-color:var(--institution-primary)!important;background:linear-gradient(145deg,#fff,var(--institution-primary-soft))!important}
      body.theme-anhanguera .quick-action.active,
      body.theme-unifecaf .quick-action.active{background:var(--institution-primary)!important;border-color:var(--institution-primary)!important;color:#fff!important}
      body.theme-anhanguera .upload-dropzone:hover,
      body.theme-unifecaf .upload-dropzone:hover{border-color:var(--institution-primary)!important;background:var(--institution-primary-soft)!important}
      body.theme-anhanguera .progress-bar,
      body.theme-anhanguera .upload-live-progress-bar,
      body.theme-unifecaf .progress-bar,
      body.theme-unifecaf .upload-live-progress-bar{background:linear-gradient(90deg,var(--institution-primary),var(--institution-accent))!important}

      body.theme-anhanguera .upload-dropzone{border-color:#ffd1b1!important;background:#fffaf6!important}
      body.theme-anhanguera .upload-dropzone:hover{border-color:#f37021!important;background:#fff2e8!important}
      body.theme-anhanguera .upload-icon{background:linear-gradient(135deg,#f37021,#ff8a3d)!important;color:#fff!important;box-shadow:0 10px 24px rgba(243,112,33,.28)!important}
      body.theme-anhanguera .upload-title{color:#1f2937!important}
      body.theme-anhanguera .upload-copy{color:#6b7280!important}
      body.theme-anhanguera .upload-file-name,
      body.theme-anhanguera #uploadFileName{background:#fff2e8!important;color:#9a3f08!important;border:1px solid #ffd1b1!important;box-shadow:none!important}
      body.theme-anhanguera .import-policy{background:#fff7f1!important;border-color:#ffd1b1!important;color:#9a3f08!important}
      body.theme-anhanguera .import-policy strong{color:#c94d0b!important}
      body.theme-anhanguera .ops-pill{background:#fff2e8!important;color:#9a3f08!important;border-color:#ffd1b1!important}
      body.theme-anhanguera .ops-user{background:#fff7f1!important;color:#9a3f08!important;border-color:#ffd1b1!important}
      body.theme-anhanguera .btn-ghost{background:#fff!important;color:#c94d0b!important;border-color:#f3b486!important}
      body.theme-anhanguera .btn-ghost:hover{background:#fff2e8!important;color:#9a3f08!important;border-color:#f37021!important}
      body.theme-anhanguera .quick-action:not(.active){background:#fff!important;color:#c94d0b!important;border-color:#f3b486!important}
      body.theme-anhanguera .priority-list li strong,
      body.theme-anhanguera .priority-summary strong,
      body.theme-anhanguera .ops-table-footer strong,
      body.theme-anhanguera #lblPage{color:#c94d0b!important}
      body.theme-anhanguera .priority-list li strong{background:#fff2e8!important}
      body.theme-anhanguera .priority-summary{background:#fff7f1!important}
      body.theme-anhanguera .legend-dot.ready{background:#f37021!important}
      body.theme-anhanguera .alert-info{background:#fff7f1!important;border-color:#ffd1b1!important;color:#9a3f08!important}
      body.theme-anhanguera a{color:#c94d0b!important}
      body.theme-anhanguera a:hover{color:#9a3f08!important}
      body.theme-anhanguera input:focus,
      body.theme-anhanguera select:focus,
      body.theme-anhanguera .ts-wrapper.focus .ts-control{border-color:#f37021!important;box-shadow:0 0 0 3px rgba(243,112,33,.14)!important}
      body.theme-anhanguera .ts-control .item{background:#fff2e8!important;color:#9a3f08!important;border-color:#ffd1b1!important}

      body.theme-unifecaf .upload-icon{color:var(--institution-primary)!important}
      body.theme-unifecaf .ops-kpi-success{border-color:#20a464!important;background:linear-gradient(145deg,#fff,#edf9f3)!important}
      body.theme-unifecaf .legend-dot.ready{background:#20a464!important}
      body.theme-unifecaf a{--link-theme:#0b5ed7}

      @media(max-width:1050px){
        .institution-button{padding:8px 10px}
        .institution-button .institution-label{display:none}
      }
    `;
    document.head.appendChild(style);
  }

  function applyTheme(institution) {
    const normalized = institution === 'unifecaf' ? 'unifecaf' : 'anhanguera';
    document.body.classList.remove('theme-anhanguera', 'theme-unifecaf');
    document.body.classList.add(`theme-${normalized}`);
    document.documentElement.dataset.institution = normalized;
  }

  function renderButtons(wrapper, data) {
    const available = Array.isArray(data.available) && data.available.length
      ? data.available
      : [
          {value: 'anhanguera', label: 'Anhanguera'},
          {value: 'unifecaf', label: 'UniFECAF'},
        ];

    wrapper.innerHTML = available.map(item => {
      const meta = INSTITUTIONS[item.value] || {label: item.label || item.value, short: String(item.label || item.value).slice(0, 1)};
      const active = item.value === data.institution;
      return `
        <button type="button" class="institution-button${active ? ' is-active' : ''}" data-institution="${item.value}" aria-pressed="${active}">
          <span class="institution-mark">${meta.short}</span>
          <span class="institution-label">${meta.label}</span>
        </button>
      `;
    }).join('');
  }

  function applyInstitutionUi(data) {
    const institution = data.institution === 'unifecaf' ? 'unifecaf' : 'anhanguera';
    applyTheme(institution);

    const wrapper = document.querySelector('.institution-switch');
    if (!wrapper) return;
    renderButtons(wrapper, {...data, institution});

    const subtitle = document.querySelector('.ops-brand-subtitle');
    if (subtitle) subtitle.textContent = `${data.label || INSTITUTIONS[institution].label} · operação educacional`;

    const uploadCard = document.querySelector('.ops-upload-card');
    const uploadButton = document.querySelector('#btnUpload');
    let alert = document.querySelector('#institutionImportAlert');

    if (!data.import_enabled && institution === 'unifecaf') {
      if (!alert && uploadCard) {
        alert = document.createElement('div');
        alert.id = 'institutionImportAlert';
        alert.className = 'institution-context-alert';
        alert.textContent = 'UniFECAF ativa para consulta, filtros e exportação. Importação protegida até as SPs próprias serem configuradas.';
        uploadCard.querySelector('.ops-card-head')?.insertAdjacentElement('afterend', alert);
      }
      if (uploadButton) {
        uploadButton.disabled = true;
        uploadButton.title = 'Importação da UniFECAF ainda não configurada.';
      }
    } else {
      alert?.remove();
      if (uploadButton) {
        uploadButton.disabled = false;
        uploadButton.removeAttribute('title');
      }
    }
  }

  async function changeInstitution(value) {
    const wrapper = document.querySelector('.institution-switch');
    const current = wrapper?.querySelector('.institution-button.is-active')?.dataset.institution;
    if (!value || value === current) return;

    wrapper?.classList.add('is-loading');
    try {
      await requestJson('/api/instituicao', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({institution: value}),
      });
      window.location.reload();
    } catch (error) {
      wrapper?.classList.remove('is-loading');
      window.alert(error?.message || 'Não foi possível trocar a instituição.');
    }
  }

  async function init() {
    ensureStyle();
    const actions = document.querySelector('.ops-top-actions');
    if (!actions || document.querySelector('.institution-switch')) return;

    const wrapper = document.createElement('div');
    wrapper.className = 'institution-switch is-loading';
    wrapper.setAttribute('role', 'group');
    wrapper.setAttribute('aria-label', 'Escolher instituição');
    wrapper.innerHTML = '<button type="button" class="institution-button" disabled>Carregando...</button>';
    actions.insertAdjacentElement('afterbegin', wrapper);

    wrapper.addEventListener('click', event => {
      const button = event.target.closest('.institution-button[data-institution]');
      if (!button) return;
      changeInstitution(button.dataset.institution);
    });

    try {
      const data = await requestJson('/api/instituicao');
      applyInstitutionUi(data);
      wrapper.classList.remove('is-loading');
    } catch (error) {
      wrapper.title = error?.message || 'Falha ao carregar instituições.';
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, {once: true});
  } else {
    init();
  }
})();