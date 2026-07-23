#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if new in text:
        print(f"OK já aplicado: {path}")
        return
    if old not in text:
        raise SystemExit(f"Trecho não encontrado em {path}: {old[:80]!r}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    print(f"Atualizado: {path}")


db = ROOT / "services" / "database.py"
app = ROOT / "app.py"
preview = ROOT / "usercustomize.py"

replace_once(
    db,
    '("celular", "celular"),\n    ("email", "email"),',
    '("celular", "celular"),\n    ("telefone2", "telefone2"),\n    ("email", "email"),',
)
replace_once(
    db,
    '"celular": ["celular", "telefone", "telefone_celular", "whatsapp", "phone", "fone"],',
    '"celular": ["celular", "telefone", "telefone_celular", "whatsapp", "phone", "fone"],\n    "telefone2": ["telefone2", "telefone_2", "telefone_secundario", "celular2", "celular_2", "whatsapp2"],',
)
replace_once(
    app,
    '    "celular",\n    "email",',
    '    "celular",\n    "telefone2",\n    "email",',
)
replace_once(
    preview,
    'PERSONAL_FIELDS = {"nome", "cpf", "celular", "email"}',
    'PERSONAL_FIELDS = {"nome", "cpf", "celular", "telefone2", "email"}',
)
replace_once(
    preview,
    '    "fone": "celular", "documento": "cpf", "cpf_aluno": "cpf",',
    '    "fone": "celular", "telefone_2": "telefone2", "telefone_secundario": "telefone2",\n    "celular2": "telefone2", "celular_2": "telefone2", "whatsapp2": "telefone2",\n    "documento": "cpf", "cpf_aluno": "cpf",',
)

print("Patch telefone2 concluído. Reinicie/reimplante o serviço no EasyPanel.")
