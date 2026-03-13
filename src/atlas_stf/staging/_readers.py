"""CSV readers with per-file parameters."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ._config import FileConfig


def read_csv(raw_dir: Path, config: FileConfig) -> pd.DataFrame:
    """Read a raw CSV with the correct parameters for the file."""
    filepath = raw_dir / config.filename
    na_values = config.null_values

    if config.has_multiline_fields:
        df = pd.read_csv(
            filepath,
            engine="python",
            on_bad_lines="warn",
            na_values=na_values,
            keep_default_na=True,
            dtype=str,
        )
    else:
        df = pd.read_csv(
            filepath,
            engine="c",
            na_values=na_values,
            keep_default_na=True,
            low_memory=False,
            dtype=str,
        )

    return df
