# Arquitetura da informação

## Objetivo

Definir a organização conceitual da interface do produto analítico.

## Estrutura principal

### 1. Visão geral
Entrada principal da plataforma, com indicadores agregados do corpus.

### 2. Ministros
Área dedicada a perfis decisórios e séries temporais por ministro.

### 3. Assuntos e classes
Área para distribuição temática e processual.

### 4. Atores
Área para partes, advogados e relações com processos.

### 5. Alertas
Área central de triagem e priorização.

### 6. Casos
Área de inspeção detalhada de processos sinalizados.

### 7. Evidências
Área para documentação do racional dos alertas.

### 8. Governança
Área de transparência metodológica, fontes e riscos.

## Fluxos principais

### Fluxo A — pesquisa exploratória
Visão geral → Ministro → Assunto → Alertas → Caso

### Fluxo B — triagem investigativa
Alertas → Caso → Evidência → Verificação externa opcional

### Fluxo C — auditoria metodológica
Governança → Fonte → Métrica → Alerta → Evidência

### 9. Sanções
Área para consulta de partes sancionadas (CEIS/CNEP/Leniência e CVM) e seus cruzamentos com processos do STF.

## Fluxos principais (cont.)

### Fluxo D — verificação de sanções
Sanções → Filtro (CEIS/CNEP/Leniência/CVM) → Parte sancionada → Casos vinculados → Alertas → Evidência

### 10. Doações eleitorais
Área para consulta de partes que doaram para campanhas eleitorais e seus cruzamentos com processos do STF.

## Fluxos principais (cont.)

### Fluxo E — verificação de doações
Doações → Parte doadora → Casos vinculados → Taxa de êxito → Alertas

## Regras de navegação

1. Todo alerta deve ser clicável até seu racional.
2. Todo caso deve mostrar vínculo com processo, decisão e grupo comparável.
3. Toda métrica sensível deve remeter a sua definição metodológica.
