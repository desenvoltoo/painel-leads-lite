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
    unidade: 'polo', campus: 'polo', telefone: 'celular', telefone_celular: 'celular',
    whatsapp: 'celular', phone: 'celular', fone: 'celular', documento: 'cpf',
    cpf_aluno: 'cpf', consultor: 'consultor_comercial', consultor_venda: 'consultor_comercial',
    consultor_do_disparo: 'consultor_disparo', acao: 'acao_comercial', obs: 'observacao',
    flag_matriculado: 'matriculado'
  };

  let cachedRows = null;
  let cachedStructural = null;
  let previewRequestId = 0;

  function selectedMode() {
    return document.querySelector('#uploadMode')?.value === 'somente_novos'
      ? 'somente_novos'
      : 'normal';
  }

  function normalize(value) {
    return String(value ?? '').trim().toLowerCase().normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]+/g, '_')
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
    document.querySelector('.ops-upload-card .ops-upload-layout')?.insertAdjacentElement('afterend', panel);
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

  function mappedRow(row) {
    return Object.fromEntries(Object.entries(row).map(([key, value]) => [canonical(key), value]));
  }

  function structuralData(rows) {
    const headers = [...new Set(Object.keys(rows[0] || {}).map(canonical).filter(Boolean))];
    const present = new Set(headers);
    const identified = rows.filter(row => {
      const mapped = mappedRow(row);
      return !empty(mapped.cpf) || !empty(mapped.celular);
    }).length;
    const operational = headers.filter(header => OPERATIONAL.has(header));
    const clearing = operational.map(field => ({
      field,
      blanks: rows.reduce((total, row) => total + (empty(mappedRow(row)[field]) ? 1 : 0), 0)
    })).filter(item => item.blanks > 0).sort((a, b) => b.blanks - a.blanks);
    return {
      headers,
      present,
      identified,
      operational,
      clearing,
      preserved: headers.filter(header => PERSONAL.has(header) || ACADEMIC.has(header))
    };
  }

  function renderStructural(rows, data) {
    const panel = ensurePanel();
    const onlyNew = selectedMode() === 'somente_novos';
    const warnings = [];
    if (!data.present.has('cpf') && !data.present.has('celular')) warnings.push('Nenhuma coluna de CPF ou celular foi reconhecida.');
    if (data.identified < rows.length) warnings.push(`${rows.length - data.identified} linha(s) sem CPF e celular serão rejeitadas.`);
    if (!onlyNew && data.clearing.length) warnings.push('Campos operacionais presentes e vazios poderão ser limpos no banco.');

    panel.hidden = false;
    panel.dataset.mode = onlyNew ? 'somente_novos' : 'normal';
    panel.innerHTML = `
      <div class="upload-preview-head">
        <div><span>Prévia da importação</span><h3>${onlyNew ? 'Somente leads novos' : 'Impacto estrutural e comparação com o banco'}</h3></div>
        <span class="upload-preview-badge">Antes de confirmar</span>
      </div>
      <div class="upload-preview-metrics upload-preview-structural">
        ${metric('Linhas', rows.length)}
        ${metric('Colunas reconhecidas', data.headers.length)}
        ${metric('Com identificador', data.identified, `${rows.length - data.identified} sem CPF/celular`)}
        ${metric('Campos operacionais', data.operational.length, onlyNew ? 'não alteram existentes' : `${data.clearing.length} com células vazias`)}
      </div>
      <div id="uploadDbPreview" class="upload-db-preview"><div class="upload-preview-loading">Comparando com o banco...</div></div>
      ${onlyNew ? `
        <div class="upload-preview-ok"><strong>Proteção ativa:</strong> registros encontrados pelo celular ou pelo CPF serão ignorados integralmente. Nenhum campo existente será atualizado ou limpo.</div>
      ` : `
        <div class="upload-preview-columns">
          <div><strong>Preservados quando vazios</strong><p>${data.preserved.length ? data.preserved.join(', ') : 'Nenhum campo pessoal/acadêmico reconhecido'}</p></div>
          <div><strong>Substituídos pelo arquivo</strong><p>${data.operational.length ? data.operational.join(', ') : 'Nenhum campo operacional reconhecido'}</p></div>
        </div>
        ${data.clearing.length ? `<div class="upload-preview-clears"><strong>Células operacionais vazias</strong><div>${data.clearing.map(item => `<span>${item.field}: ${item.blanks}</span>`).join('')}</div></div>` : ''}
      `}
      ${warnings.length ? `<div class="upload-preview-warnings">${warnings.map(warning => `<p>⚠ ${warning}</p>`).join('')}</div>` : ''}
    `;
  }

  function renderDatabasePreview(data) {
    const target = document.querySelector('#uploadDbPreview');
    if (!target) return;
    const onlyNew = selectedMode() === 'somente_novos';
    const examples = data.exemplos || {};
    const existentes = Number(data.existentes || 0);
    const novos = Number(data.novos || 0);

    target.innerHTML = `
      <div class="upload-preview-subhead"><strong>Comparação real com o banco</strong><span>Simulação sem gravar dados</span></div>
      <div class="upload-preview-metrics upload-preview-db-metrics ${onlyNew ? 'only-new' : ''}">
        ${metric('Novos', novos, onlyNew ? 'serão importados' : '')}
        ${metric('Existentes', existentes)}
        ${onlyNew
          ? metric('Serão ignorados', existentes, 'nenhum campo será alterado')
          : metric('Serão alterados', data.alterados || 0)}
        ${onlyNew ? '' : metric('Sem mudança', data.sem_mudanca || 0)}
        ${metric('Ambíguos', data.ambiguos || 0, examples.ambiguos?.length ? `Linhas: ${examples.ambiguos.join(', ')}` : '')}
        ${metric('Rejeitados', data.rejeitados || 0, examples.rejeitados?.length ? `Linhas: ${examples.rejeitados.join(', ')}` : '')}
        ${onlyNew ? '' : metric('Limpezas previstas', data.limpezas || 0, 'Campos operacionais')}
      </div>
      ${onlyNew
        ? '<div class="upload-preview-ok">Somente os registros marcados como novos serão enviados para inclusão.</div>'
        : ((data.ambiguos || data.rejeitados)
          ? '<div class="upload-preview-warnings"><p>⚠ Revise os registros ambíguos ou rejeitados antes da importação.</p></div>'
          : '<div class="upload-preview-ok">Nenhum conflito de identificação encontrado na simulação.</div>')}
    `;
  }

  async function fetchDatabasePreview(rows, columns) {
    const response = await fetch('/api/upload/preview', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      credentials: 'same-origin',
      cache: 'no-store',
      body: JSON.stringify({rows, columns, import_mode: selectedMode()})
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok || !body.ok) throw new Error(body?.error?.message || 'Não foi possível comparar com o banco.');
    return body.data || {};
  }

  async function readFile(file) {
    if (!window.XLSX) throw new Error('Leitor de planilhas não carregado.');
    const data = await file.arrayBuffer();
    const workbook = XLSX.read(data, {type: 'array', cellDates: true});
    const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
    return XLSX.utils.sheet_to_json(firstSheet, {defval: null, raw: false});
  }

  async function refreshPreview() {
    if (!cachedRows || !cachedStructural) return;
    const requestId = ++previewRequestId;
    renderStructural(cachedRows, cachedStructural);
    try {
      const result = await fetchDatabasePreview(cachedRows, cachedStructural.headers);
      if (requestId !== previewRequestId) return;
      renderDatabasePreview(result);
    } catch (error) {
      if (requestId !== previewRequestId) return;
      const target = document.querySelector('#uploadDbPreview');
      if (target) target.innerHTML = `<div class="upload-preview-error">${error?.message || 'Falha ao comparar com o banco.'}</div>`;
    }
  }

  document.addEventListener('change', async event => {
    if (event.target?.id !== 'uploadFile') return;
    const file = event.target.files?.[0];
    const panel = ensurePanel();
    if (!file) {
      cachedRows = null;
      cachedStructural = null;
      previewRequestId += 1;
      panel.hidden = true;
      panel.innerHTML = '';
      return;
    }

    panel.hidden = false;
    panel.innerHTML = '<div class="upload-preview-loading">Analisando estrutura da planilha...</div>';
    try {
      const rows = await readFile(file);
      if (!rows.length) {
        renderError('A planilha não possui linhas de dados.');
        return;
      }
      cachedRows = rows;
      cachedStructural = structuralData(rows);
      await refreshPreview();
    } catch (error) {
      renderError(error?.message || 'Não foi possível gerar a prévia.');
    }
  });

  window.addEventListener('upload:mode-changed', refreshPreview);

  const style = document.createElement('style');
  style.textContent = `
    .upload-preview{margin-top:18px;border:1px solid #dbe4f0;border-radius:18px;padding:18px;background:#fff;color:#172033;box-shadow:none!important;filter:none!important;opacity:1!important}
    .upload-preview *{text-shadow:none!important}
    .upload-preview-head,.upload-preview-subhead{display:flex;justify-content:space-between;gap:16px;align-items:center;margin-bottom:14px}.upload-preview-head span,.upload-preview-subhead span{font-size:.74rem;text-transform:uppercase;letter-spacing:.08em;color:#5f6f86}.upload-preview-head h3{margin:3px 0 0;color:#172033}.upload-preview-badge{padding:7px 10px;border-radius:999px;background:#eaf2ff;color:#2458a6}
    .upload-preview-metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}.upload-preview-metrics article{padding:12px;border:1px solid #e2e8f0;border-radius:14px;background:#f6f8fc;color:#172033;display:flex;flex-direction:column;gap:3px;box-shadow:none}.upload-preview-metrics article span{color:#526176}.upload-preview-metrics strong{font-size:1.35rem;color:#172033}.upload-preview-metrics small{color:#6b778c}
    .upload-db-preview{margin-top:14px;padding-top:14px;border-top:1px solid #e2e8f0}.upload-preview-db-metrics{grid-template-columns:repeat(7,minmax(0,1fr))}.upload-preview-db-metrics.only-new{grid-template-columns:repeat(5,minmax(0,1fr))}
    .upload-preview-columns{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}.upload-preview-columns>div,.upload-preview-clears{padding:12px;border:1px solid #e2e8f0;border-radius:14px;background:#f8fafc;color:#172033;box-shadow:none}.upload-preview-columns p{margin:6px 0 0;line-height:1.5;color:#5f6f86}
    .upload-preview-clears{margin-top:12px}.upload-preview-clears div{display:flex;flex-wrap:wrap;gap:7px;margin-top:9px}.upload-preview-clears span{padding:6px 9px;border-radius:999px;background:#fff3d8;color:#825b00}
    .upload-preview-warnings,.upload-preview-ok,.upload-preview-error,.upload-preview-loading{margin-top:12px;padding:11px 13px;border-radius:12px;color:#172033}.upload-preview-warnings{background:#fff4dc;border:1px solid #f4d38a}.upload-preview-warnings p{margin:4px 0}.upload-preview-ok{background:#eaf8ef;border:1px solid #b9e2c7}.upload-preview-error{background:#fff0f0;border:1px solid #efb6b6}.upload-preview-loading{background:#f4f7fb;border:1px solid #dbe4f0;color:#526176}
    .upload-impact-backdrop{background:rgba(15,23,42,.18)!important;backdrop-filter:none!important}
    .upload-impact-modal{background:#fff!important;color:#172033!important;border:1px solid #dbe4f0!important;box-shadow:0 12px 34px rgba(15,23,42,.16)!important}
    .upload-impact-grid article{background:#f6f8fc!important;color:#172033!important;border:1px solid #e2e8f0!important}
    .upload-impact-close{color:#172033!important}.upload-impact-copy{color:#526176!important;opacity:1!important}
    @media(max-width:1200px){.upload-preview-db-metrics,.upload-preview-db-metrics.only-new{grid-template-columns:repeat(3,minmax(0,1fr))}}@media(max-width:800px){.upload-preview-metrics,.upload-preview-db-metrics,.upload-preview-db-metrics.only-new{grid-template-columns:1fr 1fr}.upload-preview-columns{grid-template-columns:1fr}}
  `;
  document.head.appendChild(style);
})();