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
    TimeRemainingColumn,
)

ProgressFn = Callable[[int, int, str], None]
_ROOT_LOGGING_LOCK = RLock()


@contextmanager
def cli_progress(label: str) -> Generator[ProgressFn]:
    """Create a rich progress bar and yield a callback ``(current, total, desc)``.

    Logs from the ``logging`` module are rendered above the progress bar
    via ``rich.console.Console`` so they stay visible.
    """
    console = Console(stderr=True)

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        TextColumn("[cyan]({task.percentage:>5.1f}%)"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    )

    # Handler mutation is global state, so serialize concurrent usage.
    with _ROOT_LOGGING_LOCK:
        root = logging.getLogger()
        rich_handler = RichHandler(console=console, show_path=False, show_time=False)
        rich_handler.setLevel(logging.DEBUG)
        original_handlers = root.handlers[:]
        root.handlers = [rich_handler]

        try:
            with progress:
                task_id = progress.add_task(label, total=None)

                def _update(current: int, total: int, description: str) -> None:
                    progress.update(task_id, completed=current, total=total, description=description)

                yield _update
        finally:
            root.handlers = original_handlers
