"""Jurisprudência scraper for jurisprudencia.stf.jus.br."""

from typing import TYPE_CHECKING

__all__ = ["scrape_acordaos", "scrape_decisoes", "scrape_target"]

if TYPE_CHECKING:
    from ._runner import scrape_acordaos, scrape_decisoes, scrape_target


def __getattr__(name: str):
    if name in __all__:
        from ._runner import scrape_acordaos, scrape_decisoes, scrape_target

        exports = {
            "scrape_acordaos": scrape_acordaos,
            "scrape_decisoes": scrape_decisoes,
            "scrape_target": scrape_target,
        }
        return exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
