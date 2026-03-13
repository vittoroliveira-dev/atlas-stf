# Checklist de auditoria — interface

## Objetivo

Garantir que a interface proposta preserve explicabilidade, neutralidade e rastreabilidade.

## Aplicação

Usar antes de aprovar especificações ou mudanças em dashboard, filtros, páginas de detalhe, API consumida pela interface ou painel de evidência.

## Itens obrigatórios

- [ ] A interface não usa linguagem acusatória.
- [ ] Quando houver painel de evidência ativo, todo alerta aponta para ele.
- [ ] Todo score possui explicação acessível.
- [ ] Os filtros são compatíveis com os campos documentados.
- [ ] A origem dos dados pode ser rastreada.
- [ ] O grupo comparável pode ser inspecionado.
- [ ] O baseline pode ser inspecionado.
- [ ] As limitações estão acessíveis ao usuário.
- [ ] O status analítico e as notas de incerteza podem ser visualizados.
- [ ] Não há elementos que sugiram prova automática.

## Critérios de bloqueio

A etapa deve ser interrompida se:
- um alerta não puder ser explicado pela interface;
- a interface induzir leitura acusatória;
- o usuário não puder acessar a lógica do score ou do baseline.

Como a interface operacional já existe no repositório, este checklist se aplica a mudanças na superfície pública de produto.

## Resultado

- Status:
- Responsável:
- Data:
- Observações:
