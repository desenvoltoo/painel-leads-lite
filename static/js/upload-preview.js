(() => {
  'use strict';

  const PERSONAL = new Set(['nome', 'cpf', 'celular', 'email']);
  const ACADEMIC = new Set(['curso', 'modalidade', 'turno', 'polo', 'unidade', 'origem', 'tipo_negocio', 'data_inscricao', 'data_matricula']);
  const OPERATIONAL = new Set([
    'consultor_comercial', 'consultor_disparo', 'status', 'status_inscricao',
    'campanha', 'canal', 'acao_comercial', 'tipo_disparo', 'peca_disparo',
    'texto_disparo', 'observacao', 'qtd_acionamentos', 'matriculado',
    'flag_matriculado', 'data_ultima_acao', 'data_disparo'
  ]);

  const aliases = {
    unidade: 'polo',
    campus: 'polo',
    telefone: 'celular',
    telefone_celular: 'celular',
    whatsapp: 'celular',
    phone: 'celular',
    fone: 'celular',
    documento: 'cpf',
    cpf_aluno: 'cpf',
    consultor: 'consultor_comercial',
    consultor_venda: 'consultor_comercial',
    consultor_do_disparo: 'consultor_disparo',
    acao: 'acao_comercial',
    obs: 'observacao'
  };

  function normalize(value) {
    return String(value ?? '')
      .trim()
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '');
  }

  function canonical(value) {
    const key = normalize(value);
    return aliases[key] || key;
  }

  function empty(value) {
    return value === null || value === undefined || String(value).trim() === '';
  }

  function ensurePanel() {
    let panel = document.querySelector('#uploadPreview');
    if (panel) return panel;
    panel = document.createElement('div');
    panel.id = 'uploadPreview';
    panel.className = 'upload-preview';
    panel.hidden = true;
    const card = document.querySelector('.ops-upload-card .ops-upload-layout');
    card?.insertAdjacentElement('afterend', panel);
    return panel;
  }

  function renderError(message) {
    const panel = ensurePanel();
    panel.hidden = false;
    panel.innerHTML = `<div class="upload-preview-error">${message}</div>`;
  }

  function metric(label, value, detail = '') {
    return `<article><span>${label}</span><strong>${value}</strong>${detail ? `<small>${detail}</small>` : ''}</article>`;
  }

  function render(rows) {
    const panel = ensurePanel();
    if (!rows.length) {
      renderError('A planilha não possui linhas de dados.');
      return;
    }

    const headers = [...new Set(Object.keys(rows[0] || {}).map(canonical).filter(Boolean))];
    const present = new Set(headers);
    const hasCpf = present.has('cpf');
    const hasPhone = present.has('celular');
    const identified = rows.filter(row => {
      const mapped = Object.fromEntries(Object.entries(row).map(([k, v]) => [canonical(k), v]));
      return !empty(mapped.cpf) || !empty(mapped.celular);
    }).length;

    const operational = headers.filter(h => OPERATIONAL.has(h));
    const clearing = operational.map(field => {
      const blanks = rows.reduce((total, row) => {
        const mapped = Object.fromEntries(Object.entries(row).map(([k, v]) => [canonical(k), v]));
        return total + (empty(mapped[field]) ? 1 : 0);
      }, 0);
      return {field, blanks};
    }).filter(item => item.blanks > 0).sort((a, b) => b.blanks - a.blanks);

    const preserved = headers.filter(h => PERSONAL.has(h) || ACADEMIC.has(h));
    const warnings = [];
    if (!hasCpf && !hasPhone) warnings.push('Nenhuma coluna de CPF ou celular foi reconhecida.');
    if (identified < rows.length) warnings.push(`${rows.length - identified} linha(s) sem CPF e celular serão rejeitadas.`);
    if (clearing.length) warnings.push('Campos operacionais presentes e vazios serão limpos no banco.');

    panel.hidden = false;
    panel.innerHTML = `
      <div class="upload-preview-head">
        <div><span>Prévia da importação</span><h3>Impacto estrutural do arquivo</h3></div>
        <span class="upload-preview-badge">Antes de confirmar</span>
      </div>
      <div class="upload-preview-metrics">
        ${metric('Linhas', rows.length)}
        ${metric('Colunas reconhecidas', headers.length)}
        ${metric('Com identificador', identified, `${rows.length - identified} sem CPF/celular`)}
        ${metric('Campos operacionais', operational.length, `${clearing.length} com células vazias`)}
      </div>
      <div class="upload-preview-columns">
        <div><strong>Preservados quando vazios</strong><p>${preserved.length ? preserved.join(', ') : 'Nenhum campo pessoal/acadêmico reconhecido'}</p></div>
        <div><strong>Substituídos pelo arquivo</strong><p>${operational.length ? operational.join(', ') : 'Nenhum campo operacional reconhecido'}</p></div>
      </div>
      ${clearing.length ? `<div class="upload-preview-clears"><strong>Campos que terão valores limpos</strong><div>${clearing.map(item => `<span>${item.field}: ${item.blanks}</span>`).join('')}</div></div>` : ''}
      ${warnings.length ? `<div class="upload-preview-warnings">${warnings.map(w => `<p>⚠ ${w}</p>`).join('')}</div>` : '<div class="upload-preview-ok">Arquivo estruturalmente pronto para importação.</div>'}
    `;
  }

  async function readFile(file) {
    if (!window.XLSX) throw new Error('Leitor de planilhas não carregado.');
    const data = await file.arrayBuffer();
    const workbook = XLSX.read(data, {type: 'array', cellDates: true});
    const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
    return XLSX.utils.sheet_to_json(firstSheet, {defval: null, raw: false});
  }

  document.addEventListener('change', async event => {
    if (event.target?.id !== 'uploadFile') return;
    const file = event.target.files?.[0];
    const panel = ensurePanel();
    if (!file) {
      panel.hidden = true;
      panel.innerHTML = '';
      return;
    }
    panel.hidden = false;
    panel.innerHTML = '<div class="upload-preview-loading">Analisando estrutura da planilha...</div>';
    try {
      render(await readFile(file));
    } catch (error) {
      renderError(error?.message || 'Não foi possível gerar a prévia.');
    }
  });

  const style = document.createElement('style');
  style.textContent = `
    .upload-preview{margin-top:18px;border:1px solid rgba(148,163,184,.22);border-radius:18px;padding:18px;background:rgba(15,23,42,.48)}
    .upload-preview-head{display:flex;justify-content:space-between;gap:16px;align-items:center;margin-bottom:14px}.upload-preview-head span{font-size:.74rem;text-transform:uppercase;letter-spacing:.08em;opacity:.7}.upload-preview-head h3{margin:3px 0 0}.upload-preview-badge{padding:7px 10px;border-radius:999px;background:rgba(59,130,246,.14)}
    .upload-preview-metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}.upload-preview-metrics article{padding:12px;border-radius:14px;background:rgba(15,23,42,.65);display:flex;flex-direction:column;gap:3px}.upload-preview-metrics strong{font-size:1.35rem}.upload-preview-metrics small{opacity:.65}
    .upload-preview-columns{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}.upload-preview-columns>div,.upload-preview-clears{padding:12px;border-radius:14px;background:rgba(15,23,42,.55)}.upload-preview-columns p{margin:6px 0 0;line-height:1.5;opacity:.75}
    .upload-preview-clears{margin-top:12px}.upload-preview-clears div{display:flex;flex-wrap:wrap;gap:7px;margin-top:9px}.upload-preview-clears span{padding:6px 9px;border-radius:999px;background:rgba(245,158,11,.14)}
    .upload-preview-warnings,.upload-preview-ok,.upload-preview-error,.upload-preview-loading{margin-top:12px;padding:11px 13px;border-radius:12px}.upload-preview-warnings{background:rgba(245,158,11,.11)}.upload-preview-warnings p{margin:4px 0}.upload-preview-ok{background:rgba(34,197,94,.11)}.upload-preview-error{background:rgba(239,68,68,.12)}.upload-preview-loading{opacity:.75}
    @media(max-width:800px){.upload-preview-metrics{grid-template-columns:1fr 1fr}.upload-preview-columns{grid-template-columns:1fr}}
  `;
  document.head.appendChild(style);
})();