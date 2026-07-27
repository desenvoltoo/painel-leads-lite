(() => {
  'use strict';

  function injectExportControls() {
    const toolbar = document.querySelector('.toolbar-team');
    if (!toolbar || document.querySelector('#btnExportTeamProductivity')) return;

    const wrapper = document.createElement('div');
    wrapper.className = 'd-flex gap-2 align-items-center flex-wrap';
    wrapper.innerHTML = `
      <input id="teamExportMonth" type="month" class="form-control" style="width:165px" aria-label="Mês da exportação">
      <button id="btnExportTeamProductivity" type="button" class="btn btn-success">Exportar produtividade</button>
    `;
    toolbar.appendChild(wrapper);

    const month = wrapper.querySelector('#teamExportMonth');
    const now = new Date();
    month.value = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;

    wrapper.querySelector('#btnExportTeamProductivity').addEventListener('click', async (event) => {
      const button = event.currentTarget;
      const original = button.textContent;
      button.disabled = true;
      button.textContent = 'Gerando planilha...';
      try {
        const response = await fetch(`/api/gestao/operacional/consultores/exportar?mes=${encodeURIComponent(month.value)}`, {
          credentials: 'same-origin',
          headers: {Accept: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'},
        });
        if (!response.ok) {
          const body = await response.json().catch(() => ({}));
          throw new Error(body.error || body.message || `Falha HTTP ${response.status}`);
        }
        const blob = await response.blob();
        const disposition = response.headers.get('content-disposition') || '';
        const match = disposition.match(/filename\*?=(?:UTF-8''|\")?([^\";]+)/i);
        const filename = match ? decodeURIComponent(match[1].replace(/\"/g, '')) : `produtividade_equipe_${month.value}.xlsx`;
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = filename;
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        URL.revokeObjectURL(url);
      } catch (error) {
        alert(error.message || 'Falha ao exportar produtividade.');
      } finally {
        button.disabled = false;
        button.textContent = original;
      }
    });
  }

  document.addEventListener('DOMContentLoaded', injectExportControls);
})();
