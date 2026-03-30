"""
Download automático dos painéis do STF Transparência (Corte Aberta).

Usa Playwright para navegar nos painéis Qlik Sense, limpar o filtro
padrão de ano (que vem filtrado pelo ano corrente), e exportar os
dados completos via botão de download.

Uso:
    uv run python scripts/download_stf.py
    uv run python scripts/download_stf.py --paineis decisoes acervo
    uv run python scripts/download_stf.py --headless
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from playwright.sync_api import BrowserContext, Page, TimeoutError, sync_playwright

BASE = "https://transparencia.stf.jus.br/extensions"

PAINEIS = {
    "acervo": {
        "url": f"{BASE}/acervo/acervo.html",
        "descricao": "Acervo de processos em tramitação",
    },
    "decisoes": {
        "url": f"{BASE}/decisoes/decisoes.html",
        "descricao": "Decisões do STF",
    },
    "distribuidos": {
        "url": f"{BASE}/distribuidos/distribuidos.html",
        "descricao": "Registro e distribuição",
    },
    "recebidos_baixados": {
        "url": f"{BASE}/recebidos_baixados/recebidos_baixados.html",
        "descricao": "Recebimento e baixa",
    },
    "repercussao_geral": {
        "url": f"{BASE}/repercussao_geral/repercussao_geral.html",
        "descricao": "Repercussão Geral",
    },
    "controle_concentrado": {
        "url": f"{BASE}/controle_concentrado/controle_concentrado.html",
        "descricao": "Controle Concentrado",
    },
    "plenario_virtual": {
        "url": f"{BASE}/plenario_virtual/plenario_virtual.html",
        "descricao": "Plenário Virtual",
    },
    "decisoes_covid": {
        "url": f"{BASE}/decisoes_covid/decisoes_covid.html",
        "descricao": "Decisões Covid-19",
    },
    "reclamacoes": {
        "url": f"{BASE}/reclamacoes/reclamacoes.html",
        "descricao": "Reclamações",
    },
    "taxa_provimento": {
        "url": f"{BASE}/taxa_provimento/taxa_provimento.html",
        "descricao": "Taxa de Provimento",
    },
    "omissao_inconstitucional": {
        "url": f"{BASE}/omissao_inconstitucional/omissao_inconstitucional.html",
        "descricao": "Omissão Inconstitucional",
    },
}

TIMEOUT_QLIK_LOAD = 120_000
TIMEOUT_DOWNLOAD = 600_000


def esperar_qlik_carregar(page: Page) -> bool:
    """Espera o Qlik Sense terminar de carregar os objetos."""
    print("    Aguardando Qlik carregar...", end="", flush=True)
    try:
        page.wait_for_selector("#loader", state="hidden", timeout=TIMEOUT_QLIK_LOAD)
        time.sleep(3)
        print(" OK")
        return True
    except Exception:
        try:
            page.wait_for_selector(".qvobject", timeout=TIMEOUT_QLIK_LOAD)
            time.sleep(5)
            print(" OK (fallback)")
            return True
        except Exception:
            print(" FALHOU")
            return False


def limpar_filtros_qlik(page: Page) -> None:
    """Remove filtros/seleções ativas via RequireJS (mesmo escopo do mashup)."""
    print("    Limpando filtros (clearAll)...", end="", flush=True)

    # Estratégia 1: usar require para acessar qlik dentro do escopo correto
    result = page.evaluate("""
        (() => {
            try {
                // Tenta via require (escopo do mashup)
                if (typeof require !== 'undefined') {
                    return new Promise((resolve) => {
                        require(['js/qlik'], function(qlik) {
                            try {
                                const app = qlik.currApp();
                                if (app) {
                                    app.clearAll();
                                    resolve('ok_require');
                                } else {
                                    resolve('no_app');
                                }
                            } catch(e) {
                                resolve('error_require: ' + e.message);
                            }
                        });
                        // Timeout de segurança
                        setTimeout(() => resolve('timeout'), 10000);
                    });
                }
                return 'no_require';
            } catch(e) {
                return 'error: ' + e.message;
            }
        })()
    """)
    print(f" {result}")

    if result and result.startswith("ok"):
        time.sleep(3)
        return

    # Estratégia 2: clicar no botão de limpar seleções (Qlik nativo)
    try:
        clear_btn = page.locator(".clear-all, [title*='Limpar'], [title*='Clear']").first
        if clear_btn.is_visible(timeout=3000):
            clear_btn.click()
            print("    Limpou via botão UI")
            time.sleep(3)
    except Exception:
        pass


def fazer_download(page: Page, download_dir: Path, painel_nome: str) -> Path | None:
    """Clica no botão de exportação principal e aguarda o download."""

    # Tentar diferentes seletores de botão de export
    botoes = [
        "#EXPORT-BUTTON-PADRAO",
        "#EXPORT-BUTTON-TOP",
        "button[data-qcmd='exportar_padrao']",
        "a[data-qcmd='exportar_padrao']",
        "button[data-qcmd='exportar_selecionado']",
        "a[data-qcmd='exportar_selecionado']",
    ]

    button = None
    for sel in botoes:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible(timeout=2000):
                button = loc.first
                print(f"    Botão encontrado: {sel}")
                break
        except Exception:
            continue

    if button is None:
        print("    Nenhum botão de exportação encontrado")
        return None

    print("    Iniciando download...", end="", flush=True)

    try:
        with page.expect_download(timeout=TIMEOUT_DOWNLOAD) as download_info:
            button.click()

        download = download_info.value
        ext = Path(download.suggested_filename or "export.csv").suffix or ".csv"
        filename = f"{painel_nome}{ext}"
        dest = download_dir / filename
        download.save_as(str(dest))
        size_kb = dest.stat().st_size / 1024
        print(f" OK — {dest.name} ({size_kb:.0f} KB)")
        return dest

    except TimeoutError:
        print(" TIMEOUT (o download pode ser muito grande ou o servidor não respondeu)")
        return None
    except Exception as e:
        print(f" FALHOU: {e}")
        return None


def processar_painel(
    page: Page,
    context: BrowserContext,
    nome: str,
    config: dict,
    download_dir: Path,
) -> list[Path]:
    """Processa um painel: navega, limpa filtros e exporta."""
    print(f"\n{'='*60}")
    print(f"  Painel: {nome} — {config['descricao']}")
    print(f"  URL: {config['url']}")
    print(f"{'='*60}")

    page.goto(config["url"], wait_until="domcontentloaded", timeout=60_000)

    if not esperar_qlik_carregar(page):
        print("    ERRO: Qlik não carregou, pulando painel")
        return []

    # Limpar o filtro padrão de ano corrente → exporta todos os dados
    limpar_filtros_qlik(page)
    time.sleep(2)

    # Exportar
    arq = fazer_download(page, download_dir, nome)
    return [arq] if arq else []


def main() -> None:
    parser = argparse.ArgumentParser(description="Download dos painéis STF Transparência")
    parser.add_argument(
        "--paineis",
        nargs="+",
        choices=list(PAINEIS.keys()),
        default=list(PAINEIS.keys()),
        help="Painéis para baixar (padrão: todos)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw/transparencia"),
        help="Diretório de saída (padrão: data/raw/transparencia)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=False,
        help="Executar sem interface gráfica",
    )

    args = parser.parse_args()
    download_dir = args.output.resolve()
    download_dir.mkdir(parents=True, exist_ok=True)

    print("STF Transparência — Download Automático")
    print(f"Painéis: {', '.join(args.paineis)}")
    print(f"Diretório: {download_dir}")
    print(f"Headless: {args.headless}")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=args.headless,
            args=["--ignore-certificate-errors", "--disable-web-security"],
        )
        context = browser.new_context(
            accept_downloads=True,
            ignore_https_errors=True,
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()
        page.on("dialog", lambda d: d.accept())

        resultados = {}
        for nome in args.paineis:
            config = PAINEIS[nome]
            try:
                arquivos = processar_painel(page, context, nome, config, download_dir)
                resultados[nome] = arquivos
            except Exception as e:
                print(f"    ERRO no painel {nome}: {e}")
                resultados[nome] = []

        browser.close()

    print(f"\n{'='*60}")
    print("RESUMO")
    print(f"{'='*60}")
    total = 0
    for nome, arquivos in resultados.items():
        status = f"{len(arquivos)} arquivo(s)" if arquivos else "FALHOU"
        print(f"  {nome:30s} {status}")
        total += len(arquivos)
    print(f"\nTotal: {total} arquivo(s) em {download_dir}")

    sys.exit(0 if total > 0 else 1)


if __name__ == "__main__":
    main()
