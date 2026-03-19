"""Atlas STF."""

from importlib.metadata import version

from .cli import main

__version__ = version("atlas-stf")

__all__ = ["__version__", "main"]
