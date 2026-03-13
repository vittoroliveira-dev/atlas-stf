# Registro de riscos

## Risco R-001
- `titulo`: comparabilidade inadequada
- `descricao`: casos podem ser agrupados sem similitude suficiente
- `impacto`: alto
- `probabilidade`: alta
- `mitigacao`: critérios formais de grupo comparável, explicação auditável e verificação externa quando necessária
- `status`: aberto

## Risco R-002
- `titulo`: corpus inicial incompleto
- `descricao`: exportação pode refletir recorte e não totalidade
- `impacto`: alto
- `probabilidade`: média
- `mitigacao`: registrar filtro, data e origem da exportação
- `status`: aberto

## Risco R-003
- `titulo`: ausência de fundamentação textual
- `descricao`: análise estrutural sem inteiro teor pode gerar falso positivo
- `impacto`: alto
- `probabilidade`: alta
- `mitigacao`: tratar o estágio atual como triagem e acoplar revisão textual documental aos casos priorizados
- `status`: aberto

## Risco R-004
- `titulo`: sobreinterpretação de alertas
- `descricao`: usuários podem tratar alertas como acusação
- `impacto`: alto
- `probabilidade`: média
- `mitigacao`: linguagem neutra e documentação explícita de limites
- `status`: aberto

## Risco R-005
- `titulo`: variação de nomes
- `descricao`: grafias distintas para partes e advogados
- `impacto`: médio
- `probabilidade`: alta
- `mitigacao`: normalização com preservação do valor bruto
- `status`: aberto

## Risco R-006
- `titulo`: correlação tratada como causalidade
- `descricao`: mudança temporal pode ser interpretada como relação causal
- `impacto`: alto
- `probabilidade`: média
- `mitigacao`: separar eventos externos e usar linguagem de correlação
- `status`: aberto
