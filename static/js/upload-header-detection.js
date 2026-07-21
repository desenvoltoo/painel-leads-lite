(() => {
  'use strict';

  if (!window.XLSX?.utils?.sheet_to_json) return;

  const originalSheetToJson = window.XLSX.utils.sheet_to_json.bind(window.XLSX.utils);
  const known = new Set([
    'status_inscricao','data_inscricao','origem','unidade','polo','tipo_negocio',
    'curso','modalidade','turno','nome','cpf','celular','email','data_ultima_acao',
    'qtd_acionamentos','status','data_disparo','peca_disparo','texto_disparo',
    'consultor_disparo','tipo_disparo','campanha','observacao','data_matricula',
    'matriculado','canal','acao_comercial','consultor_comercial'
  ]);

  const aliases = {
    telefone: 'celular', telefone_celular: 'celular', whatsapp: 'celular',
    phone: 'celular', fone: 'celular', documento: 'cpf', cpf_aluno: 'cpf',
    campus: 'polo', consultor: 'consultor_comercial', consultor_venda: 'consultor_comercial',
    consultor_do_disparo: 'consultor_disparo', acao: 'acao_comercial', obs: 'observacao'
  };

  function normalize(value) {
    const key = String(value ?? '').trim().toLowerCase().normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '');
    return aliases[key] || key;
  }

  function findHeaderRow(sheet) {
    const matrix = originalSheetToJson(sheet, {header: 1, defval: null, raw: false, blankrows: false});
    let bestIndex = 0;
    let bestScore = -1;

    matrix.slice(0, 25).forEach((row, index) => {
      const fields = (row || []).map(normalize).filter(Boolean);
      const recognized = fields.filter(field => known.has(field));
      const hasIdentifier = recognized.includes('cpf') || recognized.includes('celular');
      const score = recognized.length * 10 + (hasIdentifier ? 25 : 0) - Math.max(0, fields.length - recognized.length);
      if (score > bestScore) {
        bestScore = score;
        bestIndex = index;
      }
    });

    return bestScore >= 20 ? bestIndex : 0;
  }

  window.XLSX.utils.sheet_to_json = function patchedSheetToJson(sheet, options = {}) {
    const opts = {...(options || {})};
    const isNormalObjectRead = opts.header === undefined && opts.range === undefined;
    if (isNormalObjectRead) {
      const detectedRow = findHeaderRow(sheet);
      if (detectedRow > 0) opts.range = detectedRow;
    }
    return originalSheetToJson(sheet, opts);
  };
})();
