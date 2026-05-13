from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

def _main() -> None:
    from atlas_stf import main

    main()

if __name__ == "__main__":
    _main()
