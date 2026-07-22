(() => {
  'use strict';

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
      .institution-switch{display:flex;align-items:center;gap:8px;padding:5px 8px;border:1px solid #d9e3f0;border-radius:14px;background:#f8fbff}
      .institution-switch label{font-size:.72rem;font-weight:700;color:#526174;white-space:nowrap}
      .institution-switch select{min-width:132px;border:0;background:transparent;font-weight:800;color:#17315f;outline:none;cursor:pointer}
      .institution-switch.is-loading{opacity:.65;pointer-events:none}
      .institution-context-alert{margin:0 0 12px;padding:10px 12px;border:1px solid #f6d89d;border-radius:12px;background:#fff8e8;color:#7b4b00;font-size:.86rem}
      @media(max-width:900px){.institution-switch label{display:none}.institution-switch select{min-width:110px}}
    `;
    document.head.appendChild(style);
  }

  function applyInstitutionUi(data) {
    const select = document.querySelector('#institutionSelect');
    if (!select) return;
    select.innerHTML = (data.available || []).map(item =>
      `<option value="${item.value}">${item.label}</option>`
    ).join('');
    select.value = data.institution || 'anhanguera';

    const subtitle = document.querySelector('.ops-brand-subtitle');
    if (subtitle) subtitle.textContent = `${data.label || 'Anhanguera'} · operação educacional`;

    const uploadCard = document.querySelector('.ops-upload-card');
    const uploadButton = document.querySelector('#btnUpload');
    let alert = document.querySelector('#institutionImportAlert');
    if (!data.import_enabled && data.institution === 'unifecaf') {
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
    if (!actions || document.querySelector('#institutionSelect')) return;

    const wrapper = document.createElement('div');
    wrapper.className = 'institution-switch';
    wrapper.innerHTML = '<label for="institutionSelect">Base ativa</label><select id="institutionSelect" aria-label="Selecionar instituição"><option>Carregando...</option></select>';
    actions.insertAdjacentElement('afterbegin', wrapper);

    const select = wrapper.querySelector('#institutionSelect');
    select.addEventListener('change', () => changeInstitution(select.value));

    try {
      const data = await requestJson('/api/instituicao');
      applyInstitutionUi(data);
    } catch (error) {
      wrapper.title = error?.message || 'Falha ao carregar instituições.';
      wrapper.classList.add('is-loading');
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, {once: true});
  } else {
    init();
  }
})();
