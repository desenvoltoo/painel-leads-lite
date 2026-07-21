-- Política de atualização por importação
-- Dados pessoais/acadêmicos: vazio preserva o banco.
-- Dados operacionais: vazio limpa o banco quando a coluna estiver presente no arquivo.

CREATE TABLE IF NOT EXISTS modelo_estrela.upload_colunas_presentes (
    upload_id text NOT NULL,
    coluna text NOT NULL,
    criado_em timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (upload_id, coluna)
);

COMMENT ON TABLE modelo_estrela.upload_colunas_presentes IS
'Registra quais colunas realmente existiam no arquivo enviado, permitindo diferenciar coluna ausente de célula vazia.';

CREATE INDEX IF NOT EXISTS ix_upload_colunas_presentes_upload
ON modelo_estrela.upload_colunas_presentes (upload_id);

-- Grupos oficiais da política.
CREATE OR REPLACE VIEW modelo_estrela.vw_politica_campos_importacao AS
SELECT *
FROM (
    VALUES
        ('cpf', 'PESSOAL', 'PRESERVAR_VAZIO'),
        ('celular', 'PESSOAL', 'PRESERVAR_VAZIO'),
        ('nome', 'PESSOAL', 'PRESERVAR_VAZIO'),
        ('email', 'PESSOAL', 'PRESERVAR_VAZIO'),
        ('data_inscricao', 'PESSOAL', 'PRESERVAR_VAZIO'),
        ('data_matricula', 'PESSOAL', 'PRESERVAR_VAZIO'),
        ('curso', 'ACADEMICO', 'PRESERVAR_VAZIO'),
        ('modalidade', 'ACADEMICO', 'PRESERVAR_VAZIO'),
        ('turno', 'ACADEMICO', 'PRESERVAR_VAZIO'),
        ('polo', 'ACADEMICO', 'PRESERVAR_VAZIO'),
        ('origem', 'ACADEMICO', 'PRESERVAR_VAZIO'),
        ('tipo_negocio', 'ACADEMICO', 'PRESERVAR_VAZIO'),
        ('consultor_comercial', 'OPERACIONAL', 'SUBSTITUIR'),
        ('consultor_disparo', 'OPERACIONAL', 'SUBSTITUIR'),
        ('status', 'OPERACIONAL', 'SUBSTITUIR'),
        ('status_inscricao', 'OPERACIONAL', 'SUBSTITUIR'),
        ('campanha', 'OPERACIONAL', 'SUBSTITUIR'),
        ('canal', 'OPERACIONAL', 'SUBSTITUIR'),
        ('acao_comercial', 'OPERACIONAL', 'SUBSTITUIR'),
        ('tipo_disparo', 'OPERACIONAL', 'SUBSTITUIR'),
        ('peca_disparo', 'OPERACIONAL', 'SUBSTITUIR'),
        ('texto_disparo', 'OPERACIONAL', 'SUBSTITUIR'),
        ('observacao', 'OPERACIONAL', 'SUBSTITUIR'),
        ('qtd_acionamentos', 'OPERACIONAL', 'SUBSTITUIR'),
        ('data_ultima_acao', 'OPERACIONAL', 'SUBSTITUIR'),
        ('data_disparo', 'OPERACIONAL', 'SUBSTITUIR'),
        ('flag_matriculado', 'OPERACIONAL', 'SUBSTITUIR')
) AS p(campo, grupo, comportamento);

COMMENT ON VIEW modelo_estrela.vw_politica_campos_importacao IS
'Fonte oficial da regra de preservação/substituição usada nas importações.';
