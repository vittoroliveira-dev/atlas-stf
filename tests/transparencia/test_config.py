"""Tests for transparencia fetch configuration."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.transparencia._config import (
    ALL_PAINEL_SLUGS,
    BASE_URL,
    PAINEIS,
    TransparenciaFetchConfig,
    painel_url,
)


class TestTransparenciaConfig:
    def test_defaults(self) -> None:
        config = TransparenciaFetchConfig()
        assert config.output_dir == Path("data/raw/transparencia")
        assert config.paineis == ALL_PAINEL_SLUGS
        assert config.headless is True
        assert config.ignore_tls is False
        assert config.dry_run is False

    def test_custom_paineis(self) -> None:
        config = TransparenciaFetchConfig(paineis=("acervo", "decisoes"))
        assert config.paineis == ("acervo", "decisoes")

    def test_all_painel_slugs_count(self) -> None:
        assert len(ALL_PAINEL_SLUGS) == 11

    def test_key_paineis_present(self) -> None:
        for slug in ("acervo", "decisoes", "distribuidos", "plenario_virtual", "taxa_provimento"):
            assert slug in PAINEIS

    def test_base_url(self) -> None:
        assert "transparencia.stf.jus.br" in BASE_URL

    def test_painel_url_contains_slug(self) -> None:
        for slug in ALL_PAINEL_SLUGS:
            url = painel_url(slug)
            assert slug in url
            assert url.startswith(BASE_URL)
            assert url.endswith(".html")
