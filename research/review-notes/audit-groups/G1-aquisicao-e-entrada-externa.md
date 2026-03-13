# G1 — Aquisição e entrada externa

## Objetivo

Revisar a cadeia que recebe dados externos, resolve sessão HTTP, baixa arquivos e consome ZIP/CSV antes da curadoria.

## Entradas

- `src/atlas_stf/scraper/_session.py`
- `src/atlas_stf/core/http_stream_safety.py`
- `src/atlas_stf/core/zip_safety.py`
- `src/atlas_stf/rfb/_runner.py`
- `src/atlas_stf/tse/_runner.py`
- `src/atlas_stf/cgu/_runner.py`
- `src/atlas_stf/cgu/_client.py`
- `src/atlas_stf/datajud/_client.py`
- `tests/scraper/test_api.py`
- `tests/rfb/test_runner.py`
- `tests/tse/test_runner.py`
- `tests/cgu/test_runner.py`

## Saída esperada

Confirmar se a entrada externa preserva integridade mínima, limites de tamanho e comportamento reproduzível antes de alimentar camadas internas.

## Restrições

- Não inferir risco sem provar fluxo real até artefato consumido.
- Não tratar controles parciais como ausência total de controle.

## Critérios de validação

- Há limite explícito para downloads e descompressão.
- Há tratamento de erro para rede e ZIP inválido.
- Há validação ou endurecimento razoável na sessão HTTP.
- Há teste adjacente cobrindo parsing e runners revisados.

## Riscos ou incertezas

- A verificação foi focal nas bordas de aquisição e não exauriu todos os parsers CSV linha a linha.
- Qualquer hipótese além dos arquivos listados permanece `INCERTO`.

## Evidência revisada

- `write_limited_stream_to_file` impõe teto de bytes em download.
- `enforce_max_uncompressed_size` impõe teto agregado de descompressão.
- `rfb/_runner.py`, `tse/_runner.py` e `cgu/_runner.py` usam essas proteções antes de extrair ou persistir arquivos.
- A suíte adjacente passou: `tests/scraper/test_api.py`, `tests/rfb/test_runner.py`, `tests/tse/test_runner.py`, `tests/cgu/test_runner.py`.

## Achados confirmados

- **Médio** — `src/atlas_stf/scraper/_session.py:45-50` desabilita a validação TLS com `ignore_https_errors=True`.
  Evidência: o scraper cria a sessão Playwright com erro TLS ignorado e não há configuração de proxy confiável, pinning, hash ou modo degradado explícito em outro ponto da cadeia. Os dados raspados alimentam enriquecimento posterior em `src/atlas_stf/curated/build_process.py:113-150` e `src/atlas_stf/curated/build_decision_event.py:79-115`.
  Revisão contextual: a única ocorrência do bypass está na criação da sessão; não há teste ou documentação que o trate como exceção controlada.
  Impacto: um intermediário de rede ou proxy corporativo pode adulterar conteúdo de jurisprudência sem que a coleta detecte o problema, degradando a trilha de evidência do corpus.
  Correção sugerida: validar TLS por padrão e, se o bypass continuar necessário, condicioná-lo a flag explícita de ambiente com logging de risco e marcação do artefato como coletado em modo degradado.

## Itens `INCERTO`

- A filtragem de membros ZIP cobre `..` e caminho absoluto Unix. Não foi reproduzido exploit com nomes alternativos de caminho; portanto não há achado confirmado adicional sobre traversal.
