# Especificação do painel de evidência

## Objetivo

Descrever o painel responsável por explicar cada alerta.

## Status

Há interface operacional de alertas e caso no repositório atual, mas o painel de evidência completo ainda não está consolidado como superfície dedicada e fechada.

## Estrutura

### 1. Cabeçalho do alerta
- identificador
- tipo
- score
- status

### 2. Caso sinalizado
- processo
- decisão
- ministro
- data
- tipo de decisão
- andamento

### 3. Grupo comparável
- definição da regra
- tamanho do grupo
- critérios
- janela temporal

### 4. Baseline
- comportamento esperado
- estatísticas relevantes

### 5. Observação sinalizada
- padrão observado
- distância em relação ao esperado

### 6. Notas metodológicas
- limites
- fatores de incerteza
- hipótese alternativa

### 7. Verificação externa opcional
- nota de incerteza
- referência a leitura documental complementar
- síntese derivada opcional por IA, quando existir

## Regra central

Enquanto o painel de evidência completo não estiver consolidado, a interface operacional de alertas deve pelo menos expor score, padrão esperado, padrão observado, nota de incerteza e vínculo auditável com o caso.
