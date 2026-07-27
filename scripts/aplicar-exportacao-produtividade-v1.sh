#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:-/etc/easypanel/projects/painel-leads/painel-leads/code}"
REPO_RAW="https://raw.githubusercontent.com/desenvoltoo/painel-leads-lite/main"
STAMP="$(date +%Y%m%d-%H%M%S)"

cd "$APP_DIR"
mkdir -p services static/js backups-exportacao

cp app.py "backups-exportacao/app.py.$STAMP.bak"
cp templates/gestao.html "backups-exportacao/gestao.html.$STAMP.bak"

curl -fsSL "$REPO_RAW/services/produtividade_export.py" -o services/produtividade_export.py
curl -fsSL "$REPO_RAW/static/js/gestao-exportacao.js" -o static/js/gestao-exportacao.js

python3 - <<'PY'
from pathlib import Path
import re

app_path = Path('app.py')
text = app_path.read_text(encoding='utf-8')

import_line = 'from services.produtividade_export import register_produtividade_export\n'
if import_line not in text:
    marker = 'from services.gestao_operacional import ('
    pos = text.find(marker)
    if pos < 0:
        raise SystemExit('Não encontrei o bloco services.gestao_operacional em app.py')
    text = text[:pos] + import_line + text[pos:]

if 'register_produtividade_export(app)' not in text:
    match = re.search(r'^app\s*=\s*Flask\([^\n]+\)\s*$', text, flags=re.M)
    if not match:
        raise SystemExit('Não encontrei app = Flask(...) em app.py')
    insert_at = match.end()
    text = text[:insert_at] + '\nregister_produtividade_export(app)' + text[insert_at:]

app_path.write_text(text, encoding='utf-8')

template_path = Path('templates/gestao.html')
template = template_path.read_text(encoding='utf-8')
script_tag = '<script src="{{ url_for(\'static\', filename=\'js/gestao-exportacao.js\') }}?v={{ asset_version }}"></script>'
if 'js/gestao-exportacao.js' not in template:
    if '</body>' not in template:
        raise SystemExit('Não encontrei </body> em templates/gestao.html')
    template = template.replace('</body>', f'{script_tag}\n</body>')
template_path.write_text(template, encoding='utf-8')
PY

python3 -m py_compile app.py services/produtividade_export.py

echo "Exportação de produtividade instalada."
echo "Reinicie o serviço do painel no EasyPanel e use Ctrl+F5 no navegador."
