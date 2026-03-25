"""Rich progress bar helpers for CLI commands."""

from __future__ import annotations

import logging
from collections.abc import Callable, Generator
from contextlib import contextmanager
from threading import RLock

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

ProgressFn = Callable[[int, int, str], None]
_ROOT_LOGGING_LOCK = RLock()


@contextmanager
def cli_progress(label: str) -> Generator[ProgressFn]:
    """Create a rich progress bar and yield a callback ``(current, total, desc)``.

    ETA is provided by ``ProgressTracker`` via the description string, not by
    Rich's ``TimeRemainingColumn``.  This avoids two competing ETA estimates
    and ensures the ETA source is the tracker's throughput-based calculation
    (with confidence gating), not Rich's internal linear extrapolation.

    Builders that don't use ``ProgressTracker`` will show elapsed time only —
    which is honest when granular progress data isn't available.

    Logs from the ``logging`` module are rendered above the progress bar
    via ``rich.console.Console`` so they stay visible.

    When stderr is not a TTY (e.g. ``nohup``), falls back to plain
    ``logging.StreamHandler`` so that log output is not suppressed.
    """
    console = Console(stderr=True)
    is_tty = console.is_terminal

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        TextColumn("[cyan]({task.percentage:>5.1f}%)"),
        TimeElapsedColumn(),
        console=console,
        disable=not is_tty,
    )

    # Handler mutation is global state, so serialize concurrent usage.
    with _ROOT_LOGGING_LOCK:
        root = logging.getLogger()
        original_level = root.level
        original_handlers = root.handlers[:]

        if is_tty:
            handler: logging.Handler = RichHandler(console=console, show_path=False, show_time=False)
        else:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(levelname)-8s %(message)s"))
        handler.setLevel(logging.DEBUG)
        root.handlers = [handler]
        root.setLevel(logging.INFO)

        try:
            with progress:
                task_id = progress.add_task(label, total=None)

                def _update(current: int, total: int, description: str) -> None:
                    progress.update(task_id, completed=current, total=total, description=description)

                yield _update
        finally:
            root.handlers = original_handlers
            root.level = original_level
