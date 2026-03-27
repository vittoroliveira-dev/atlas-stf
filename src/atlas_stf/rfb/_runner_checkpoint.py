"""RFB checkpoint helpers: manifest conversion, target extraction, hash computation."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import is_valid_cnpj, is_valid_cpf, normalize_entity_name, normalize_tax_id
from ..fetch._manifest_model import FetchUnit, RemoteState, SourceManifest, build_unit_id
from ..fetch._manifest_store import load_manifest  # noqa: F401 — re-exported for convenience

logger = logging.getLogger(__name__)


def _build_target_names(config: Any) -> set[str]:
    """Build set of normalized target names from minister_bio + party + counsel."""
    names: set[str] = set()

    if config.minister_bio_path.exists():
        data = json.loads(config.minister_bio_path.read_text(encoding="utf-8"))
        for _key, entry in data.items():
            name = entry.get("minister_name", "")
            norm = normalize_entity_name(name)
            if norm:
                names.add(norm)
            civil = entry.get("civil_name", "")
            civil_norm = normalize_entity_name(civil)
            if civil_norm:
                names.add(civil_norm)

    for path in (config.party_path, config.counsel_path):
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed JSONL line in %s", path)
                    continue
                raw = (
                    record.get("party_name_normalized")
                    or record.get("counsel_name_normalized")
                    or record.get("party_name_raw", "")
                    or record.get("counsel_name_raw", "")
                )
                norm = normalize_entity_name(raw)
                if norm:
                    names.add(norm)

    return names


def _manifest_to_checkpoint(manifest: SourceManifest) -> dict[str, Any]:
    """Convert manifest units to the dict format expected by pass functions."""
    completed_p1: list[int] = []
    completed_p2: list[int] = []
    completed_emp: list[int] = []
    completed_est: list[int] = []
    cnpjs: list[str] = []
    completed_ref = False
    tse_hash = ""
    artifact_commits: dict[str, Any] = {}

    for uid, unit in manifest.units.items():
        if unit.status != "committed":
            continue
        meta = unit.metadata
        pass_name = meta.get("pass_name", "")
        idx = meta.get("file_index")
        if pass_name == "socios_pass1" and idx is not None:
            completed_p1.append(idx)
        elif pass_name == "socios_pass2" and idx is not None:
            completed_p2.append(idx)
        elif pass_name == "empresas" and idx is not None:
            completed_emp.append(idx)
        elif pass_name == "estabelecimentos" and idx is not None:
            completed_est.append(idx)
        elif uid == "rfb:reference":
            completed_ref = True
        elif uid.startswith("rfb:artifact:"):
            # Recover per-artifact commit; uid format: rfb:artifact:{sanitised_name}
            artifact_name = meta.get("artifact_name", "")
            if artifact_name:
                artifact_commits[artifact_name] = {
                    "run_id": meta.get("run_id", ""),
                    "record_count": meta.get("record_count", 0),
                    "committed_at": meta.get("committed_at", ""),
                }
        if "cnpjs" in meta:
            cnpjs = meta["cnpjs"]
        if "tse_targets_hash" in meta:
            tse_hash = meta["tse_targets_hash"]

    result: dict[str, Any] = {
        "completed_socios_pass1": sorted(completed_p1),
        "completed_socios_pass2": sorted(completed_p2),
        "completed_empresas": sorted(completed_emp),
        "completed_estabelecimentos": sorted(completed_est),
        "completed_reference": completed_ref,
        "cnpjs": cnpjs,
        "tse_targets_hash": tse_hash,
        "artifact_commits": artifact_commits,
    }

    return result


def _checkpoint_to_manifest(state: dict[str, Any], source: str = "rfb") -> SourceManifest:
    """Convert dict checkpoint back to manifest after pass functions mutate it."""
    manifest = SourceManifest(source=source)
    now = datetime.now(timezone.utc).isoformat()

    for pass_name, key in [
        ("socios_pass1", "completed_socios_pass1"),
        ("socios_pass2", "completed_socios_pass2"),
        ("empresas", "completed_empresas"),
        ("estabelecimentos", "completed_estabelecimentos"),
    ]:
        for idx in state.get(key, []):
            uid = build_unit_id("rfb", f"{pass_name}_{idx}")
            manifest.units[uid] = FetchUnit(
                unit_id=uid,
                source="rfb",
                label=f"RFB {pass_name} #{idx}",
                remote_url="",
                remote_state=RemoteState(url=""),
                status="committed",
                fetch_date=now,
                metadata={
                    "pass_name": pass_name,
                    "file_index": idx,
                    "cnpjs": state.get("cnpjs", []),
                    "tse_targets_hash": state.get("tse_targets_hash", ""),
                },
            )

    if state.get("completed_reference", False):
        uid = build_unit_id("rfb", "reference")
        manifest.units[uid] = FetchUnit(
            unit_id=uid,
            source="rfb",
            label="RFB reference tables",
            remote_url="",
            remote_state=RemoteState(url=""),
            status="committed",
            fetch_date=now,
        )

    # Persist per-artifact commits
    for artifact_name, commit in state.get("artifact_commits", {}).items():
        # Sanitise artifact_name for use in unit_id: strip extension, replace dots
        safe_name = artifact_name.replace(".", "_").replace("-", "_")
        uid = f"rfb:artifact:{safe_name}"
        manifest.units[uid] = FetchUnit(
            unit_id=uid,
            source="rfb",
            label=f"RFB artifact {artifact_name}",
            remote_url="",
            remote_state=RemoteState(url=""),
            status="committed",
            fetch_date=now,
            metadata={
                "artifact_name": artifact_name,
                "run_id": commit.get("run_id", ""),
                "record_count": commit.get("record_count", 0),
                "committed_at": commit.get("committed_at", ""),
            },
        )

    manifest.last_updated = now
    return manifest


def _save_checkpoint_via_manifest(output_dir: Path, state: dict[str, Any]) -> None:
    """Adapter: convert checkpoint dict to manifest and write atomically.

    Writes directly without re-acquiring FetchLock (caller already holds it).
    Uses a temp-file + os.replace for atomicity, mirroring save_manifest_locked.
    """
    from ..fetch._manifest_model import serialize_manifest

    manifest = _checkpoint_to_manifest(state)
    dest = output_dir / "_manifest_rfb.json"
    content = serialize_manifest(manifest)
    fd, tmp_path_str = tempfile.mkstemp(
        dir=str(output_dir),
        prefix=".manifest_rfb_",
        suffix=".tmp",
    )
    tmp_path = Path(tmp_path_str)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        tmp_path.replace(dest)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def _extract_tse_donor_targets(
    donations_path: Path,
) -> tuple[set[str], set[str], set[str]]:
    """Extract TSE donor CPF/CNPJ targets for RFB scan.

    Returns ``(pj_cnpjs_basico, pf_cpfs, pj_cnpjs_full)``:
    - pj_cnpjs_basico: first 8 digits of valid 14-digit CNPJs (for direct matched_cnpjs injection)
    - pf_cpfs: normalized valid 11-digit CPFs (for partner scan)
    - pj_cnpjs_full: normalized valid 14-digit CNPJs (for partner-as-PJ scan)
    """
    pj_basico: set[str] = set()
    pf_cpfs: set[str] = set()
    pj_full: set[str] = set()

    if not donations_path.exists():
        return pj_basico, pf_cpfs, pj_full

    with donations_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed JSONL line in %s", donations_path)
                continue
            raw_doc = record.get("donor_cpf_cnpj", "")
            normalized = normalize_tax_id(raw_doc)
            if not normalized:
                continue
            if len(normalized) == 14 and is_valid_cnpj(normalized):
                pj_basico.add(normalized[:8])
                pj_full.add(normalized)
            elif len(normalized) == 11 and is_valid_cpf(normalized):
                pf_cpfs.add(normalized)

    logger.info(
        "TSE donor targets: %d PJ cnpj_basico, %d PF CPFs, %d PJ full CNPJs",
        len(pj_basico),
        len(pf_cpfs),
        len(pj_full),
    )
    return pj_basico, pf_cpfs, pj_full


def _compute_tse_targets_hash(
    pj_cnpjs: set[str],
    pf_cpfs: set[str],
    pj_cnpjs_full: set[str],
) -> str:
    """Compute a stable hash of TSE targets for checkpoint invalidation."""
    payload = "\n".join(sorted(pj_cnpjs) + ["---"] + sorted(pf_cpfs) + ["---"] + sorted(pj_cnpjs_full))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


__all__ = [
    "_build_target_names",
    "_checkpoint_to_manifest",
    "_compute_tse_targets_hash",
    "_extract_tse_donor_targets",
    "_manifest_to_checkpoint",
    "_save_checkpoint_via_manifest",
]
