BEGIN;

CREATE TABLE IF NOT EXISTS modelo_estrela.app_modulos (
    modulo_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    codigo text NOT NULL UNIQUE,
    nome text NOT NULL,
    descricao text,
    icone text,
    rota text NOT NULL,
    cor text,
    ativo boolean NOT NULL DEFAULT true,
    ordem integer NOT NULL DEFAULT 0,
    criado_em timestamptz NOT NULL DEFAULT now(),
    atualizado_em timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS modelo_estrela.app_usuarios (
    usuario_id uuid PRIMARY KEY,
    nome text NOT NULL,
    email text NOT NULL UNIQUE,
    perfil_global text NOT NULL DEFAULT 'USUARIO',
    ativo boolean NOT NULL DEFAULT true,
    primeiro_acesso boolean NOT NULL DEFAULT true,
    avatar_url text,
    criado_em timestamptz NOT NULL DEFAULT now(),
    atualizado_em timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_app_usuarios_perfil CHECK (
        perfil_global IN ('SUPER_ADMIN','ADMIN','GESTOR','USUARIO')
    )
);

CREATE TABLE IF NOT EXISTS modelo_estrela.app_usuario_modulos (
    usuario_modulo_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usuario_id uuid NOT NULL REFERENCES modelo_estrela.app_usuarios(usuario_id) ON DELETE CASCADE,
    modulo_id bigint NOT NULL REFERENCES modelo_estrela.app_modulos(modulo_id) ON DELETE CASCADE,
    perfil_modulo text NOT NULL DEFAULT 'OPERADOR',
    pode_visualizar boolean NOT NULL DEFAULT true,
    pode_criar boolean NOT NULL DEFAULT false,
    pode_editar boolean NOT NULL DEFAULT false,
    pode_excluir boolean NOT NULL DEFAULT false,
    pode_importar boolean NOT NULL DEFAULT false,
    pode_exportar boolean NOT NULL DEFAULT false,
    pode_administrar boolean NOT NULL DEFAULT false,
    criado_em timestamptz NOT NULL DEFAULT now(),
    atualizado_em timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ux_usuario_modulo UNIQUE (usuario_id, modulo_id)
);

INSERT INTO modelo_estrela.app_modulos (
    codigo, nome, descricao, icone, rota, cor, ordem
)
VALUES
    ('LEADS','Gestão de Leads','Importação, acompanhamento e distribuição de leads','users','/modulos/leads','#2563EB',1),
    ('NOVO_MODULO','Novo módulo','Módulo independente conectado ao portal','layout-dashboard','/modulos/novo-modulo','#7C3AED',2)
ON CONFLICT (codigo) DO UPDATE SET
    nome = EXCLUDED.nome,
    descricao = EXCLUDED.descricao,
    icone = EXCLUDED.icone,
    rota = EXCLUDED.rota,
    cor = EXCLUDED.cor,
    ordem = EXCLUDED.ordem,
    atualizado_em = now();

CREATE OR REPLACE VIEW modelo_estrela.vw_app_usuario_modulos AS
SELECT
    u.usuario_id,
    u.nome AS usuario_nome,
    u.email,
    u.perfil_global,
    u.ativo AS usuario_ativo,
    m.modulo_id,
    m.codigo AS modulo_codigo,
    m.nome AS modulo_nome,
    m.descricao,
    m.icone,
    m.rota,
    m.cor,
    m.ordem,
    m.ativo AS modulo_ativo,
    um.perfil_modulo,
    um.pode_visualizar,
    um.pode_criar,
    um.pode_editar,
    um.pode_excluir,
    um.pode_importar,
    um.pode_exportar,
    um.pode_administrar
FROM modelo_estrela.app_usuarios u
JOIN modelo_estrela.app_usuario_modulos um ON um.usuario_id = u.usuario_id
JOIN modelo_estrela.app_modulos m ON m.modulo_id = um.modulo_id
WHERE u.ativo = true
  AND m.ativo = true
  AND um.pode_visualizar = true;

CREATE INDEX IF NOT EXISTS ix_app_usuario_modulos_usuario
    ON modelo_estrela.app_usuario_modulos(usuario_id);
CREATE INDEX IF NOT EXISTS ix_app_usuario_modulos_modulo
    ON modelo_estrela.app_usuario_modulos(modulo_id);
CREATE INDEX IF NOT EXISTS ix_app_modulos_ativo_ordem
    ON modelo_estrela.app_modulos(ativo, ordem);

COMMIT;
