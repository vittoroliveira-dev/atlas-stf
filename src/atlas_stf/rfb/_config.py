"""Configuration for RFB CNPJ partner data fetch."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# New URL since RFB migrated to NextCloud (SERPRO+) — replaces dadosabertos.rfb.gov.br/CNPJ
RFB_NEXTCLOUD_BASE = "https://arquivos.receitafederal.gov.br"
# Public share token for RFB open data on NextCloud (SERPRO+).
# This is NOT a private credential — it's the public download link equivalent,
# published by the Receita Federal for open data access.
# Override via env var if the token changes.
RFB_NEXTCLOUD_SHARE_TOKEN = os.getenv("ATLAS_STF_RFB_NEXTCLOUD_SHARE_TOKEN", "YggdBLfdninEJX9")
RFB_WEBDAV_BASE = f"{RFB_NEXTCLOUD_BASE}/public.php/webdav"
# Legacy URL kept as fallback
RFB_LEGACY_BASE_URL = "https://dadosabertos.rfb.gov.br/CNPJ"
RFB_SOCIOS_FILE_COUNT = 10
RFB_EMPRESAS_FILE_COUNT = 10


@dataclass(frozen=True)
class RfbFetchConfig:
    """Configuration for an RFB CNPJ data fetch run."""

    output_dir: Path = field(default_factory=lambda: Path("data/raw/rfb"))
    minister_bio_path: Path = field(default_factory=lambda: Path("data/curated/minister_bio.json"))
    party_path: Path = field(default_factory=lambda: Path("data/curated/party.jsonl"))
    counsel_path: Path = field(default_factory=lambda: Path("data/curated/counsel.jsonl"))
    timeout_seconds: int = 300
    dry_run: bool = False
