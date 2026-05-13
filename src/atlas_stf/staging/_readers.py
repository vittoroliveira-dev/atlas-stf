"""CSV readers with per-file parameters."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError, ParserError

from ._config import FileConfig


def read_csv(raw_dir: Path, config: FileConfig) -> pd.DataFrame:
    """Read a raw CSV with the correct parameters for the file."""
    filepath = raw_dir / config.filename
    na_values = config.null_values

    try:
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
    except EmptyDataError as exc:
        raise EmptyDataError(f"No columns to parse from file: {filepath}") from exc
    except ParserError as exc:
        raise ParserError(f"{exc} in {filepath}") from exc
    except UnicodeDecodeError as exc:
        raise UnicodeDecodeError(exc.encoding, exc.object, exc.start, exc.end, f"{exc.reason} in {filepath}") from exc
    except FileNotFoundError as exc:
        raise FileNotFoundError(exc.errno, exc.strerror, str(filepath)) from exc
    except PermissionError as exc:
        raise PermissionError(exc.errno, exc.strerror, str(filepath)) from exc
    except OSError as exc:
        raise OSError(exc.errno, f"{exc.strerror}: {filepath}", str(filepath)) from exc

    return df
