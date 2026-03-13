# Governança e auditoria

## Objetivo

Definir como decisões metodológicas, fontes, verificações e mudanças de regra serão registradas.

## Princípios

1. Toda decisão relevante deve deixar rastro.
2. Toda mudança metodológica deve ser versionada.
3. Toda fonte deve ser registrada formalmente.
4. Toda incerteza deve ser documentada.
5. Toda saída importante deve ser auditável.
6. Nenhum arquivo de código novo ou alterado deve ultrapassar 500 linhas sem divisão prévia do artefato ou justificativa explícita.

## Componentes de governança

### 1. Registro de decisões
Arquivo com decisões de escopo, modelagem, definição de métrica e mudança de regra.

### 2. Registro de fontes
Catálogo de todas as fontes usadas no projeto.

### 3. Dicionário de dados
Descrição semântica e operacional das colunas e entidades.

### 4. Registro de riscos
Lista viva de riscos e mitigação.

### 5. Checklists de auditoria
Procedimentos mínimos antes de aceitar uma saída como válida.

### 6. Superfícies públicas materializadas
README, API HTTP, dashboard web e demais contratos expostos ao usuário devem permanecer coerentes entre si e com os artefatos determinísticos do pipeline.

---

## Política de versionamento

Toda mudança deve registrar:

- data;
- responsável;
- artefato afetado;
- motivo;
- impacto esperado;
- necessidade de reprocessamento.
- justificativa de exceção, quando algum arquivo de código novo ou alterado ultrapassar 500 linhas.

## Política de revisão

Revisões devem distinguir:

- correção factual;
- mudança metodológica;
- refinamento editorial;
- marcação de incerteza.
- mudança de superfície pública já materializada.

## Política de aprovação

Nenhum alerta sensível deve ser tratado como resultado final sem:

- baseline documentado;
- explicação do score;
- trilha de evidência explícita.

## Log mínimo de uma decisão metodológica

- identificador
- data
- título
- contexto
- alternativas consideradas
- decisão tomada
- justificativa
- impacto
- próximos passos

## Auditoria interna mínima

Antes de aceitar um ciclo analítico como válido, verificar:

1. a origem do dado está preservada;
2. o grupo comparável está definido;
3. o baseline está explícito;
4. a explicação do alerta existe;
5. os limites estão registrados.
6. API, README e interface não contradizem o estado real dos artefatos.

## Critério de reprodutibilidade

Um terceiro revisor deve conseguir entender por que um caso foi sinalizado sem depender de contexto oral externo.
