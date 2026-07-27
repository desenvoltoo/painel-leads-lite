(() => {
  'use strict';

  const esc = (value) => String(value ?? '').replace(/[&<>'"]/g, (char) => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[char]));
  const fmtNum = (value) => Number(value || 0).toLocaleString('pt-BR');

  async function fetchQuality() {
    const summary = document.querySelector('#qualitySummary');
    const body = document.querySelector('#qualityTableBody');
    const error = document.querySelector('#qualityError');
    if (!summary || !body) return;

    error?.classList.add('d-none');
    summary.innerHTML = '<div class="empty-state">Analisando a base...</div>';
    body.innerHTML = '<tr><td colspan="5" class="empty-cell">Carregando inconsistências...</td></tr>';

    try {
      const response = await fetch('/api/gestao/qualidade-dados/inconsistencias?amostras=5', {
        credentials: 'same-origin',
        headers: {Accept: 'application/json'},
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload.ok === false) throw new Error(payload.error || payload.message || `Falha HTTP ${response.status}`);
      const data = payload.data || payload;
      const items = data.items || [];

      summary.innerHTML = [
        ['Inconsistências', data.total_inconsistencias],
        ['Campos analisados', data.campos_analisados],
        ['Campos com problema', data.campos_com_problema],
        ['Fonte', data.fonte || '—'],
      ].map(([label, value]) => `<div><span>${esc(label)}</span><strong>${typeof value === 'number' ? fmtNum(value) : esc(value)}</strong></div>`).join('');

      body.innerHTML = items.map((item) => {
        const samples = (item.amostras || []).map((sample) => {
          const who = [sample.nome, sample.cpf, sample.celular].filter(Boolean).join(' · ');
          return `<div><code>${esc(sample.valor || 'vazio')}</code>${who ? `<small>${esc(who)}</small>` : ''}</div>`;
        }).join('');
        return `<tr>
          <td><strong>${esc(item.campo)}</strong></td>
          <td>${esc(item.problema).replaceAll('_', ' ')}</td>
          <td><strong>${fmtNum(item.quantidade)}</strong></td>
          <td><code>${esc(item.exemplo || '—')}</code></td>
          <td><div class="quality-samples">${samples || '—'}</div></td>
        </tr>`;
      }).join('') || '<tr><td colspan="5" class="empty-cell">Nenhuma inconsistência encontrada.</td></tr>';
    } catch (err) {
      summary.innerHTML = '';
      body.innerHTML = '<tr><td colspan="5" class="empty-cell">Falha ao analisar os dados.</td></tr>';
      if (error) {
        error.textContent = err.message || 'Falha ao analisar os dados.';
        error.classList.remove('d-none');
      }
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    document.querySelector('#btnReloadQuality')?.addEventListener('click', fetchQuality);
    document.querySelectorAll('[data-page="audit"]').forEach((button) => button.addEventListener('click', fetchQuality));
    if (location.hash === '#audit') fetchQuality();
  });
})();
