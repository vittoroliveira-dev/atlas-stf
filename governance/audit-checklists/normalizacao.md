# Checklist de auditoria — normalização

## Objetivo

Validar se a normalização preservou o valor bruto e registrou corretamente os campos derivados.

## Aplicação

Usar este checklist antes de aceitar transformações da camada `staging` ou `curated`.

## Itens obrigatórios

- [ ] O valor bruto original foi preservado.
- [ ] Todo campo derivado está claramente identificado.
- [ ] A regra de transformação foi documentada.
- [ ] Datas foram convertidas sem perda de informação conhecida.
- [ ] Campos categóricos foram padronizados com rastreabilidade.
- [ ] Nomes de partes e advogados mantiveram valor bruto original.
- [ ] Consolidações incertas foram marcadas como `INCERTAS`.
- [ ] Colunas sem semântica comprovada não foram reinterpretadas arbitrariamente.
- [ ] A normalização não apagou variantes úteis para auditoria.
- [ ] Foi registrada a versão da regra de normalização.

## Critérios de bloqueio

A etapa deve ser interrompida se:
- o valor bruto tiver sido sobrescrito;
- não houver regra documentada;
- a transformação mudar significado sem justificativa.

## Resultado

- Status:
- Responsável:
- Data:
- Observações:
