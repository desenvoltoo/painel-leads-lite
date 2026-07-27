#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:-/etc/easypanel/projects/painel-leads/painel-leads/code}"
REPO_RAW="https://raw.githubusercontent.com/desenvoltoo/painel-leads-lite/main"
STAMP="$(date +%Y%m%d-%H%M%S)"

cd "$APP_DIR"
mkdir -p services static/js backups-gestao-v2

for file in app.py templates/gestao.html static/js/gestao.js; do
  cp "$file" "backups-gestao-v2/$(basename "$file").$STAMP.bak"
done

curl -fsSL "$REPO_RAW/services/produtividade_export.py" -o services/produtividade_export.py
curl -fsSL "$REPO_RAW/services/qualidade_dados.py" -o services/qualidade_dados.py
curl -fsSL "$REPO_RAW/static/js/gestao-exportacao.js" -o static/js/gestao-exportacao.js
curl -fsSL "$REPO_RAW/static/js/gestao-qualidade.js" -o static/js/gestao-qualidade.js

python3 - <<'PY'
from pathlib import Path
import re

# ------------------------------------------------------------
# app.py: registra exportação e qualidade dos dados
# ------------------------------------------------------------
app_path = Path('app.py')
text = app_path.read_text(encoding='utf-8')
imports = [
    'from services.produtividade_export import register_produtividade_export\n',
    'from services.qualidade_dados import register_qualidade_dados\n',
]
marker = 'from services.gestao_operacional import ('
pos = text.find(marker)
if pos < 0:
    raise SystemExit('Não encontrei o bloco services.gestao_operacional em app.py')
for line in imports:
    if line not in text:
        text = text[:pos] + line + text[pos:]
        pos += len(line)

match = re.search(r'^app\s*=\s*Flask\([^\n]+\)\s*$', text, flags=re.M)
if not match:
    raise SystemExit('Não encontrei app = Flask(...) em app.py')
insert_at = match.end()
registrations = []
if 'register_produtividade_export(app)' not in text:
    registrations.append('register_produtividade_export(app)')
if 'register_qualidade_dados(app)' not in text:
    registrations.append('register_qualidade_dados(app)')
if registrations:
    text = text[:insert_at] + '\n' + '\n'.join(registrations) + text[insert_at:]
app_path.write_text(text, encoding='utf-8')

# ------------------------------------------------------------
# gestao.html: remove todo o módulo de lotes e transforma
# auditoria em qualidade dos dados
# ------------------------------------------------------------
template_path = Path('templates/gestao.html')
template = template_path.read_text(encoding='utf-8')
template = template.replace(
    'Equipe, lotes, conversão e qualidade da operação educacional.',
    'Equipe, disparos, conversão e qualidade da operação educacional.'
)

# Remove botão lateral Lotes.
template = re.sub(
    r'\s*<button data-page="lots">.*?</button>',
    '', template, count=1, flags=re.S
)

# Remove painel de lotes ativos da visão geral.
template = re.sub(
    r'\s*<article class="panel mt-3">\s*<div class="panel-title">.*?<span class="panel-kicker">LOTEAMENTO</span>.*?</article>',
    '', template, count=1, flags=re.S
)

# Remove a página inteira de lotes.
template = re.sub(
    r'\s*<section class="page" id="page-lots">.*?</section>',
    '', template, count=1, flags=re.S
)

# Ajusta texto da equipe.
template = template.replace(
    'Carga, andamento, retorno e matrícula por consultor.',
    'Disparos, retornos e matrículas por consultor, sem dependência de lote.'
)

# Troca Auditoria por Qualidade dos dados no menu.
template = re.sub(
    r'<button data-page="audit"><span>\d+</span><div><strong>Auditoria</strong><small>Eventos e ações</small></div></button>',
    '<button data-page="audit"><span>05</span><div><strong>Qualidade dos dados</strong><small>Inconsistências por campo</small></div></button>',
    template,
)

quality_section = '''
    {% if current_user_context and 'logs:view' in current_user_context.permissions %}
    <section class="page" id="page-audit">
      <div class="page-heading">
        <div><span class="page-kicker">QUALIDADE DA BASE</span><h2>Qualidade dos dados</h2><p>Localize campos com marcadores inválidos, formatos incorretos e combinações inconsistentes.</p></div>
        <button id="btnReloadQuality" class="btn btn-outline-secondary" type="button">Analisar novamente</button>
      </div>
      <div id="qualityError" class="alert alert-danger d-none"></div>
      <div id="qualitySummary" class="summary-grid mb-3"></div>
      <article class="panel">
        <div class="panel-title"><div><span class="panel-kicker">INCONSISTÊNCIAS</span><h3>Problemas encontrados por coluna</h3><p>Use os exemplos para localizar e corrigir os registros na fonte.</p></div></div>
        <div class="table-responsive table-shell tall">
          <table class="management-table wide">
            <thead><tr><th>Campo</th><th>Problema</th><th>Quantidade</th><th>Exemplo</th><th>Registros para localizar</th></tr></thead>
            <tbody id="qualityTableBody"><tr><td colspan="5" class="empty-cell">Clique em analisar novamente.</td></tr></tbody>
          </table>
        </div>
      </article>
    </section>
    {% endif %}
'''

template = re.sub(
    r'\s*\{% if current_user_context and \'logs:view\' in current_user_context.permissions %\}\s*<section class="page" id="page-audit">.*?</section>\s*\{% endif %\}',
    '\n' + quality_section,
    template,
    count=1,
    flags=re.S,
)

# Garante carregamento dos dois recursos complementares.
scripts = [
    '<script src="{{ url_for(\'static\', filename=\'js/gestao-exportacao.js\') }}?v={{ asset_version }}"></script>',
    '<script src="{{ url_for(\'static\', filename=\'js/gestao-qualidade.js\') }}?v={{ asset_version }}"></script>',
]
for script in scripts:
    if script not in template:
        template = template.replace('</body>', script + '\n</body>')

template_path.write_text(template, encoding='utf-8')

# ------------------------------------------------------------
# gestao.js: remove consumo, renderização e alertas de lotes
# ------------------------------------------------------------
js_path = Path('static/js/gestao.js')
js = js_path.read_text(encoding='utf-8')
js = re.sub(r'^\s*let lots = \[\];\s*$', '', js, flags=re.M)
js = re.sub(r'^\s*if \(name === \'lots\'\) renderLots\(\);\s*$', '', js, flags=re.M)

# Remove funções exclusivas de lote.
js = re.sub(
    r'\n\s*function filteredLots\(\) \{.*?\n\s*function setSync\(state\) \{',
    '\n\n  function setSync(state) {',
    js,
    count=1,
    flags=re.S,
)

# Substitui os KPIs executivos por indicadores de disparo.
new_kpis = '''  function renderExecutiveKpis() {
    const totals = team.reduce((acc, row) => {
      for (const key of ['total_disparado','disparado_hoje','disparado_semana','disparado_mes','retornos','positivos','negativos','matriculas']) {
        acc[key] = (acc[key] || 0) + num(row[key]);
      }
      return acc;
    }, {});
    const retorno = totals.total_disparado ? totals.retornos / totals.total_disparado * 100 : 0;
    const conversao = totals.total_disparado ? totals.matriculas / totals.total_disparado * 100 : 0;
    const items = [
      ['Total disparado', totals.total_disparado, 'Todos os registros com data de disparo', 'blue'],
      ['Hoje', totals.disparado_hoje, 'Disparos realizados hoje', 'cyan'],
      ['Esta semana', totals.disparado_semana, 'Desde segunda-feira', 'violet'],
      ['Este mês', totals.disparado_mes, 'Disparos no mês atual', 'indigo'],
      ['Retornos', totals.retornos, `Taxa ${pct(retorno)}`, 'amber'],
      ['Positivos', totals.positivos, 'Retornos positivos', 'green'],
      ['Negativos', totals.negativos, 'Retornos negativos', 'slate'],
      ['Matrículas', totals.matriculas, `Conversão ${pct(conversao)}`, 'emerald'],
    ];
    $('#executiveKpis').innerHTML = items.map(([label, value, help, tone]) => `
      <article class="executive-card tone-${tone}"><span>${label}</span><strong>${fmtNum(value)}</strong><small>${help}</small></article>
    `).join('');
  }
'''
js = re.sub(
    r'\s*function renderExecutiveKpis\(\) \{.*?\n\s*function sortedTeam',
    '\n' + new_kpis + '\n  function sortedTeam',
    js,
    count=1,
    flags=re.S,
)

# Alertas agora olham disparos, não lote.
new_alerts = '''  function renderManagementAlerts() {
    const totalSemana = team.reduce((sum, row) => sum + num(row.disparado_semana), 0);
    const totalMatriculas = team.reduce((sum, row) => sum + num(row.matriculas), 0);
    const alerts = [];
    if (!totalSemana) alerts.push(['warning', 'Nenhum disparo nesta semana', 'Verifique data_disparo e consultor_disparo na base.']);
    if (totalSemana > 0) alerts.push(['info', `${fmtNum(totalSemana)} disparos nesta semana`, 'Acompanhe a distribuição entre os consultores.']);
    if (!totalMatriculas) alerts.push(['warning', 'Nenhuma matrícula registrada', 'Confira os campos matriculado e data_matricula.']);
    if (!alerts.length) alerts.push(['success', 'Operação sem alertas críticos', 'Os indicadores estão disponíveis para acompanhamento.']);
    $('#managementAlerts').innerHTML = alerts.map(([type, title, text]) => `<div class="management-alert alert-${type}"><i></i><div><strong>${title}</strong><p>${text}</p></div></div>`).join('');
  }
'''
js = re.sub(
    r'\s*function renderManagementAlerts\(\) \{.*?\n\s*function setSync',
    '\n' + new_alerts + '\n  function setSync',
    js,
    count=1,
    flags=re.S,
)

# Carrega apenas consultores; elimina chamada ao endpoint de lotes.
new_load = '''  async function loadCore(force = false) {
    if (!force && Date.now() - lastCoreLoadedAt < CORE_CACHE_MS && team.length) return;
    coreController?.abort();
    coreController = new AbortController();
    clearError(); setSync('loading');
    loadingCards('#executiveKpis', 8);
    try {
      const t = await fetchJson('/api/gestao/operacional/consultores', {signal:coreController.signal});
      team = t.items || t.data || [];
      lastCoreLoadedAt = Date.now();
      renderExecutiveKpis(); renderTeam(); renderManagementAlerts();
      $('#lastUpdated').textContent = `Atualizado em ${new Date().toLocaleString('pt-BR')}`;
      $('#dataHealth').textContent = 'Dados disponíveis';
      setSync('ok');
    } catch (error) {
      if (error.name === 'AbortError') return;
      setSync('error'); $('#dataHealth').textContent = 'Falha na atualização'; showError(error);
    }
  }
'''
js = re.sub(
    r'\s*async function loadCore\(force = false\) \{.*?\n\s*async function loadImports',
    '\n' + new_load + '\n  async function loadImports',
    js,
    count=1,
    flags=re.S,
)

js = re.sub(r'^\s*\$\(\'#lotSearch\'\).*$', '', js, flags=re.M)
js = re.sub(r'^\s*\$\(\'#lotStatus\'\).*$', '', js, flags=re.M)
js_path.write_text(js, encoding='utf-8')
PY

python3 -m py_compile app.py services/produtividade_export.py services/qualidade_dados.py
node --check static/js/gestao.js >/dev/null 2>&1 || true

echo "Gestão sem lotes, exportação e qualidade dos dados instalada."
echo "Reinicie o serviço do painel no EasyPanel e pressione Ctrl+F5."
