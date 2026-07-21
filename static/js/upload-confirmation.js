(() => {
  'use strict';

  let confirmedFileKey = '';

  function currentFileKey() {
    const file = document.querySelector('#uploadFile')?.files?.[0];
    return file ? `${file.name}:${file.size}:${file.lastModified}` : '';
  }

  function metricValue(label) {
    const cards = [...document.querySelectorAll('#uploadPreview .upload-preview-metrics article')];
    const card = cards.find(item => item.querySelector('span')?.textContent?.trim() === label);
    const value = card?.querySelector('strong')?.textContent || '0';
    return Number(String(value).replace(/[^0-9-]/g, '')) || 0;
  }

  function collectImpact() {
    const panel = document.querySelector('#uploadPreview');
    const loading = Boolean(panel?.querySelector('.upload-preview-loading'));
    const previewError = panel?.querySelector('.upload-preview-error')?.textContent?.trim() || '';
    const clearItems = [...(panel?.querySelectorAll('.upload-preview-clears span') || [])]
      .map(item => item.textContent.trim())
      .filter(Boolean);

    return {
      loading,
      previewError,
      newRows: metricValue('Novos'),
      existing: metricValue('Existentes'),
      changed: metricValue('Serão alterados'),
      unchanged: metricValue('Sem mudança'),
      ambiguous: metricValue('Ambíguos'),
      rejected: metricValue('Rejeitados'),
      clearCount: metricValue('Limpezas previstas'),
      clearItems,
    };
  }

  function removeModal() {
    document.querySelector('#uploadImpactModal')?.remove();
  }

  function openModal(impact, onConfirm) {
    removeModal();
    const modal = document.createElement('div');
    modal.id = 'uploadImpactModal';
    modal.className = 'upload-impact-backdrop';
    modal.innerHTML = `
      <section class="upload-impact-modal" role="dialog" aria-modal="true" aria-labelledby="uploadImpactTitle">
        <div class="upload-impact-head">
          <div><span>Confirmação obrigatória</span><h3 id="uploadImpactTitle">Revise o impacto da importação</h3></div>
          <button type="button" class="upload-impact-close" aria-label="Fechar">×</button>
        </div>
        <p class="upload-impact-copy">Campos pessoais e acadêmicos vazios serão preservados. Campos operacionais presentes e vazios serão limpos.</p>
        <div class="upload-impact-grid">
          <article><span>Novos</span><strong>${impact.newRows}</strong></article>
          <article><span>Existentes</span><strong>${impact.existing}</strong></article>
          <article><span>Alterados</span><strong>${impact.changed}</strong></article>
          <article><span>Sem mudança</span><strong>${impact.unchanged}</strong></article>
          <article class="${impact.clearCount ? 'danger' : ''}"><span>Limpezas</span><strong>${impact.clearCount}</strong></article>
          <article class="${impact.ambiguous ? 'warning' : ''}"><span>Ambíguos</span><strong>${impact.ambiguous}</strong></article>
          <article class="${impact.rejected ? 'warning' : ''}"><span>Rejeitados</span><strong>${impact.rejected}</strong></article>
        </div>
        ${impact.clearItems.length ? `<div class="upload-impact-list"><strong>Campos operacionais com células vazias</strong><div>${impact.clearItems.map(item => `<span>${item}</span>`).join('')}</div></div>` : ''}
        ${(impact.ambiguous || impact.rejected) ? '<div class="upload-impact-alert">Registros ambíguos ou rejeitados não serão aplicados automaticamente.</div>' : ''}
        <label class="upload-impact-check"><input id="uploadImpactAgree" type="checkbox"> <span>Entendi que valores operacionais vazios podem apagar dados atuais.</span></label>
        <div class="upload-impact-actions">
          <button type="button" class="btn btn-ghost upload-impact-cancel">Cancelar</button>
          <button type="button" class="btn btn-danger upload-impact-confirm" disabled>Confirmar importação</button>
        </div>
      </section>`;

    document.body.appendChild(modal);
    const checkbox = modal.querySelector('#uploadImpactAgree');
    const confirm = modal.querySelector('.upload-impact-confirm');
    checkbox?.addEventListener('change', () => { confirm.disabled = !checkbox.checked; });
    modal.querySelector('.upload-impact-close')?.addEventListener('click', removeModal);
    modal.querySelector('.upload-impact-cancel')?.addEventListener('click', removeModal);
    modal.addEventListener('click', event => { if (event.target === modal) removeModal(); });
    confirm?.addEventListener('click', () => {
      removeModal();
      onConfirm();
    });
  }

  document.addEventListener('change', event => {
    if (event.target?.id === 'uploadFile') confirmedFileKey = '';
  }, true);

  document.addEventListener('click', event => {
    const button = event.target?.closest?.('#btnUpload');
    if (!button) return;

    const key = currentFileKey();
    if (!key || confirmedFileKey === key) {
      confirmedFileKey = '';
      return;
    }

    const impact = collectImpact();
    if (impact.loading) {
      event.preventDefault();
      event.stopImmediatePropagation();
      const status = document.querySelector('#uploadStatus');
      if (status) {
        status.textContent = 'Aguarde a conclusão da prévia antes de importar.';
        status.className = 'alert alert-warning';
      }
      return;
    }

    const requiresConfirmation = impact.clearCount > 0 || impact.ambiguous > 0 || impact.rejected > 0;
    if (!requiresConfirmation) return;

    event.preventDefault();
    event.stopImmediatePropagation();
    openModal(impact, () => {
      confirmedFileKey = key;
      button.click();
    });
  }, true);

  const style = document.createElement('style');
  style.textContent = `
    .upload-impact-backdrop{position:fixed;inset:0;z-index:9999;display:grid;place-items:center;padding:20px;background:rgba(2,6,23,.78);backdrop-filter:blur(5px)}
    .upload-impact-modal{width:min(760px,100%);max-height:90vh;overflow:auto;border:1px solid rgba(148,163,184,.24);border-radius:22px;padding:22px;background:#0f172a;box-shadow:0 25px 80px rgba(0,0,0,.48)}
    .upload-impact-head{display:flex;justify-content:space-between;gap:20px;align-items:flex-start}.upload-impact-head span{font-size:.74rem;text-transform:uppercase;letter-spacing:.09em;opacity:.65}.upload-impact-head h3{margin:5px 0 0}.upload-impact-close{border:0;background:transparent;color:inherit;font-size:2rem;line-height:1;cursor:pointer}
    .upload-impact-copy{line-height:1.55;opacity:.8}.upload-impact-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:9px;margin:16px 0}.upload-impact-grid article{padding:12px;border-radius:14px;background:rgba(30,41,59,.8);display:flex;flex-direction:column;gap:4px}.upload-impact-grid strong{font-size:1.35rem}.upload-impact-grid article.danger{background:rgba(239,68,68,.15)}.upload-impact-grid article.warning{background:rgba(245,158,11,.15)}
    .upload-impact-list{padding:13px;border-radius:14px;background:rgba(239,68,68,.1)}.upload-impact-list div{display:flex;flex-wrap:wrap;gap:7px;margin-top:9px}.upload-impact-list span{padding:6px 9px;border-radius:999px;background:rgba(239,68,68,.13)}.upload-impact-alert{margin-top:12px;padding:11px 13px;border-radius:12px;background:rgba(245,158,11,.12)}
    .upload-impact-check{display:flex;gap:10px;align-items:flex-start;margin:18px 0;line-height:1.4}.upload-impact-check input{margin-top:3px}.upload-impact-actions{display:flex;justify-content:flex-end;gap:10px}.upload-impact-confirm:disabled{opacity:.45;cursor:not-allowed}
    @media(max-width:720px){.upload-impact-grid{grid-template-columns:1fr 1fr}.upload-impact-actions{flex-direction:column-reverse}.upload-impact-actions button{width:100%}}
  `;
  document.head.appendChild(style);
})();