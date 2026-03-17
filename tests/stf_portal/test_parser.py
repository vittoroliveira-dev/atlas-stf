"""Tests for STF portal HTML parser.

Test HTML mirrors the real portal structure (div-based, not tables).
Patterns validated against live portal 2026-03.
"""

from __future__ import annotations

from atlas_stf.stf_portal._parser import (
    _clean_text,
    _parse_date,
    build_process_document,
    parse_andamentos_html,
    parse_deslocamentos_html,
    parse_informacoes_html,
    parse_peticoes_html,
)


def test_clean_text():
    assert _clean_text("<b>Hello</b> World") == "Hello World"
    assert _clean_text("  Multiple   spaces  ") == "Multiple spaces"
    assert _clean_text(None) is None
    assert _clean_text("") is None


def test_parse_date_dd_mm_yyyy():
    assert _parse_date("15/03/2026") == "2026-03-15"
    assert _parse_date("01/01/2020") == "2020-01-01"


def test_parse_date_iso():
    assert _parse_date("2026-03-15") == "2026-03-15"


def test_parse_date_none():
    assert _parse_date(None) is None
    assert _parse_date("") is None
    assert _parse_date("invalid") is None


def test_parse_andamentos_html():
    html = """
    <div class="processo-andamentos m-t-8">
      <ul>
        <li>
          <div class="andamento-item">
            <div class="andamento-inner">
              <div class="message-head clearfix">
                <div class="andamento-detalhe">
                  <div class="col-md-3 p-l-0">
                    <div class="andamento-data ">15/03/2026</div>
                  </div>
                  <div class="col-md-5">
                    <h5 class="andamento-nome ">Distribuído por sorteio</h5>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </li>
        <li>
          <div class="andamento-item">
            <div class="andamento-inner">
              <div class="andamento-detalhe">
                <div class="col-md-3 p-l-0">
                  <div class="andamento-data ">20/04/2026</div>
                </div>
                <div class="col-md-5">
                  <h5 class="andamento-nome ">Pedido de vista</h5>
                </div>
              </div>
            </div>
          </div>
        </li>
      </ul>
    </div>
    """
    events = parse_andamentos_html(html)
    assert len(events) == 2
    assert events[0]["date"] == "2026-03-15"
    assert events[0]["description"] == "Distribuído por sorteio"
    assert events[0]["tab_name"] == "Andamentos"
    assert events[1]["date"] == "2026-04-20"
    assert events[1]["description"] == "Pedido de vista"


def test_parse_andamentos_with_detail():
    html = """
    <div class="andamento-item">
      <div class="andamento-inner">
        <div class="andamento-detalhe">
          <div class="col-md-3 p-l-0">
            <div class="andamento-data ">17/12/2025</div>
          </div>
          <div class="col-md-5 p-l-0">
            <h5 class="andamento-nome ">Sustentação Oral</h5>
          </div>
          <div class="col-md-4 andamento-docs"></div>
          <div class="col-md-3 p-0"></div>
          <div class="col-md-9 p-0">Sustentação Oral - REQUERENTE(S): CNSEG - recebida em 17/12/2025 09:23:30</div>
        </div>
      </div>
    </div>
    """
    events = parse_andamentos_html(html)
    assert len(events) == 1
    assert events[0]["date"] == "2025-12-17"
    assert events[0]["description"] == "Sustentação Oral"
    assert events[0]["detail"] is not None
    assert "REQUERENTE(S): CNSEG" in events[0]["detail"]
    assert "recebida em 17/12/2025" in events[0]["detail"]


def test_parse_andamentos_detail_none_when_absent():
    """Andamentos without detail div should have detail=None."""
    html = """
    <div class="andamento-data ">15/03/2026</div>
    <h5 class="andamento-nome ">Distribuído por sorteio</h5>
    """
    events = parse_andamentos_html(html)
    assert len(events) == 1
    assert events[0]["detail"] is None


def test_parse_andamentos_empty():
    assert parse_andamentos_html("") == []
    assert parse_andamentos_html("<html><body>No data</body></html>") == []


def test_parse_peticoes_html():
    html = """
    <div class="col-md-12 lista-dados">
        <div class="col-6">
            <span class="processo-detalhes-bold">84897/2025</span>
            <span class="processo-detalhes">Peticionado em 18/06/2025</span>
        </div>
        <div class="col-6 d-flex justify-content-end">
            <span class="processo-detalhes">Recebido em 18/06/2025 19:05:27 por GERÊNCIA CÍVEL</span>
        </div>
    </div>
    """
    events = parse_peticoes_html(html)
    assert len(events) == 1
    assert events[0]["date"] == "2025-06-18"
    assert events[0]["protocol"] == "84897/2025"
    assert events[0]["receiver"] == "GERÊNCIA CÍVEL"


def test_parse_deslocamentos_html():
    html = """
    <div class="col-md-12 lista-dados p-r-0 p-l-0">
        <div class="lista-dados__col col-md-9">
            <div class="d-flex lh-1">
                <div class="icone-mapa col-md-1">
                    <i class="processo-detalhes-bold fas fa-map-marker-alt"></i>
                </div>
                <div class="col-md-11">
                    <span class="processo-detalhes-bold">GABINETE MINISTRO X</span>
                </div>
            </div>
            <div class="lista-dados__col--detalhes">
                <span class="processo-detalhes">Enviado por SEÇÃO CÍVEL em 10/05/2025</span>
            </div>
        </div>
        <div class="lista-dados__col col-md-3">
            <div class="col-md-12 text-right">
                <span class="processo-detalhes">Guia 12345/2025</span>
            </div>
            <div class="col-md-12 text-right">
                <span class="processo-detalhes bg-font-success">Recebido em 10/05/2025</span>
            </div>
        </div>
    </div>
    """
    events = parse_deslocamentos_html(html)
    assert len(events) == 1
    assert events[0]["destination"] == "GABINETE MINISTRO X"
    assert events[0]["origin"] == "SEÇÃO CÍVEL"
    assert events[0]["date"] == "2025-05-10"
    assert events[0]["guia"] == "12345/2025"
    assert events[0]["received_date"] == "2025-05-10"


def test_parse_informacoes_html():
    html = """
    <div id="informacoes-completas">
        <div class="informacoes__assunto col-12 m-b-8 d-flex">
            <div class="col-md-2 processo-detalhes-bold p-l-0">Assunto:</div>
            <div class="col-md-10 processo-detalhes">
                <ul>
                    <li>DIREITO TRIBUTÁRIO | Contribuições</li>
                    <li>DIREITO ADMINISTRATIVO | Controle</li>
                </ul>
            </div>
        </div>
        <div class="processo-informacoes">
            <div class="col-md-7 processo-detalhes-bold p-l-0">Data de Protocolo:</div>
            <div class="col-md-5 processo-detalhes-bold m-l-0">18/12/2024</div>
            <div class="col-md-7 processo-detalhes-bold p-l-0">Órgão de Origem:</div>
            <div class="col-md-5 processo-detalhes">SUPREMO TRIBUNAL FEDERAL</div>
            <div class="col-md-7 processo-detalhes-bold p-l-0">Origem:</div>
            <div class="col-md-5 processo-detalhes">MATO GROSSO</div>
        </div>
    </div>
    <span id="orgao-procedencia">SUPREMO TRIBUNAL FEDERAL</span>
    <span id="descricao-procedencia">MT - MATO GROSSO</span>
    """
    info = parse_informacoes_html(html)
    assert info.get("data_protocolo") == "18/12/2024"
    assert info.get("orgao_origem") == "SUPREMO TRIBUNAL FEDERAL"
    assert info.get("origem") == "MATO GROSSO"
    assert info.get("orgao_procedencia") == "SUPREMO TRIBUNAL FEDERAL"
    assert info.get("descricao_procedencia") == "MT - MATO GROSSO"
    assert "assuntos" in info
    assert len(info["assuntos"]) == 2
    assert "DIREITO TRIBUTÁRIO | Contribuições" in info["assuntos"][0]


def test_build_process_document():
    doc = build_process_document(
        process_number="ADI 1234",
        source_url="https://portal.stf.jus.br/processos/detalhe.asp?incidente=12345",
        raw_html="<html></html>",
        andamentos=[{"date": "2026-03-15", "description": "Distribuído"}],
        deslocamentos=[],
        peticoes=[],
        sessao_virtual=[],
        informacoes={"classe": "ADI"},
    )
    assert doc["process_number"] == "ADI 1234"
    assert doc["source_system"] == "stf_portal"
    assert doc["source_url"].startswith("https://")
    assert doc["raw_html_hash"] is not None
    assert len(doc["andamentos"]) == 1
    assert doc["fetched_at"] is not None
