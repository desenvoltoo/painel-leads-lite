# Instruções permanentes do projeto

- Preserve `POST /api/upload` como única rota oficial de upload; rotas antigas devem apenas orientar o uso da rota oficial.
- Use `dt_upload` preenchido pelo backend como controle de versão das cargas. Nunca substitua essa regra por `CURRENT_TIMESTAMP`.
- Uma carga antiga não pode sobrescrever carga nova: use `S.dt_upload >= F.data_atualizacao`.
- Use parâmetros do banco de dados para valores recebidos do frontend; não concatene filtros livres em SQL.
- Não exponha dados pessoais completos em logs, APIs ou telas; masque CPF, celular e e-mail quando necessário.
- Não adicione segredos ao código ou à documentação.
- Execute testes antes de concluir mudanças relevantes.
- Não execute migrações destrutivas automaticamente; scripts SQL devem ser idempotentes e evitar `DROP`, `DELETE` e `TRUNCATE` em produção.
