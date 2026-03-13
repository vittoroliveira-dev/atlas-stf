# Riscos e limitações

## Objetivo

Registrar explicitamente os limites do projeto e os riscos de inferência indevida.

## Risco 1 — comparabilidade inadequada

### Descrição
Casos podem ser tratados como equivalentes sem realmente serem semelhantes do ponto de vista jurídico-processual.

### Impacto
Falsos positivos.

### Mitigação
- critérios formais de grupo comparável;
- verificação documental externa, quando necessária;
- documentação da composição dos grupos.

---

## Risco 2 — corpus incompleto

### Descrição
A base estruturada inicial pode refletir recortes, filtros ou limitações de cobertura.

### Impacto
Inferências enviesadas.

### Mitigação
- registrar origem exata da exportação;
- registrar data da coleta;
- evitar linguagem de completude quando não demonstrada.

---

## Risco 3 — ausência de fundamentação textual

### Descrição
Dados estruturados não substituem o inteiro teor da decisão.

### Impacto
Desvio estatístico pode ser legítimo e juridicamente justificável.

### Mitigação
- tratar o estágio atual como triagem;
- acoplar revisão textual documental quando houver caso priorizado;
- impedir conclusões fortes automáticas.

---

## Risco 4 — sobreleitura política ou narrativa

### Descrição
Usuários podem interpretar alertas como acusações.

### Impacto
Distorção do objetivo do projeto.

### Mitigação
- linguagem neutra;
- rotulagem prudente;
- documentação explícita do que o projeto não faz.

---

## Risco 5 — qualidade de nomes

### Descrição
Advogados e partes podem aparecer com grafias variantes.

### Impacto
Quebra de contagem, concentração artificial ou dispersão indevida.

### Mitigação
- normalização controlada;
- preservação do valor bruto;
- confiança por consolidação.

---

## Risco 6 — correlação vs causalidade

### Descrição
Mudanças temporais ou concentrações podem ser interpretadas como relação causal.

### Impacto
Conclusões indevidas.

### Mitigação
- separar eventos externos da camada principal;
- usar linguagem de correlação;
- exigir explicação auditável e nota de incerteza quando necessária.

---

## Risco 7 — mudança do próprio tribunal

### Descrição
O padrão do tribunal pode mudar por alteração jurisprudencial geral.

### Impacto
Desvios aparentes de um ministro podem refletir contexto institucional mais amplo.

### Mitigação
- comparar com janelas temporais adequadas;
- comparar contra o próprio histórico e o histórico agregado.

---

## Limitações assumidas

1. O projeto não prova intenção.
2. O projeto não substitui apuração jurídica.
3. O projeto depende da qualidade e cobertura das fontes.
4. O projeto pode demandar verificação externa em casos específicos, mas não depende de workflow interno de revisão.
5. O estado atual é orientado à triagem, não ao juízo final.

## Regra final

Sempre que o risco de interpretação indevida for alto, a saída deve ser classificada como `INCONCLUSIVA`.
