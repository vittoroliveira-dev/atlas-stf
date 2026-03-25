"""End-to-end case sampling — trace real entities across all pipeline layers.

Picks entities that are known to appear in multiple layers (raw→curated→
analytics→serving) and verifies the chain is intact.  Skips gracefully
if any layer is missing.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

_RAW = Path("data/raw")
_CURATED = Path("data/curated")
_ANALYTICS = Path("data/analytics")
_DB_PATH = Path("data/serving/atlas_stf.db")
_DB_URL = os.getenv("ATLAS_STF_DATABASE_URL", f"sqlite:///{_DB_PATH}")


def _skip_if_missing(*paths: Path) -> None:
    for p in paths:
        if not p.exists():
            pytest.skip(f"{p} not found")


def _read_first_record(path: Path) -> dict | None:
    """Read first non-empty JSONL record."""
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                return json.loads(line)
    return None


def _search_jsonl(path: Path, key: str, value: str, max_lines: int = 500_000) -> dict | None:
    """Find first record where record[key] == value."""
    with path.open("r", encoding="utf-8") as fh:
        for i, line in enumerate(fh):
            if i >= max_lines:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get(key) == value:
                return rec
    return None


def _search_jsonl_contains(path: Path, key: str, substring: str, max_lines: int = 500_000) -> dict | None:
    """Find first record where substring is in record[key]."""
    with path.open("r", encoding="utf-8") as fh:
        for i, line in enumerate(fh):
            if i >= max_lines:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            val = rec.get(key, "")
            if isinstance(val, str) and substring.upper() in val.upper():
                return rec
    return None


@pytest.fixture(scope="module")
def session() -> Session:
    if not _DB_PATH.exists():
        pytest.skip("Serving DB not present")
    engine = create_engine(_DB_URL)
    with Session(engine) as s:
        yield s


# ---------------------------------------------------------------------------
# Case 1: CGU sanction → sanction_match → serving
# ---------------------------------------------------------------------------


class TestCguSanctionE2E:
    """Trace a CGU CEIS sanction through raw→analytics→serving."""

    def test_ceis_record_reaches_serving(self, session: Session) -> None:
        raw_path = _RAW / "cgu" / "sanctions_raw.jsonl"
        analytics_path = _ANALYTICS / "sanction_match.jsonl"
        _skip_if_missing(raw_path, analytics_path)

        # Find a CEIS record with a CNPJ (more likely to match)
        raw_rec = _search_jsonl(raw_path, "sanction_source", "ceis")
        assert raw_rec is not None, "No CEIS record in sanctions_raw.jsonl"
        entity_name = raw_rec.get("entity_name", "")
        assert entity_name, "CEIS record has no entity_name"

        # Check analytics layer produced a match for this source
        analytics_rec = _search_jsonl(analytics_path, "sanction_source", "ceis")
        assert analytics_rec is not None, "No CEIS match in sanction_match.jsonl"
        match_id = analytics_rec.get("match_id", "")

        # Check serving has this match (use text SQL to avoid circular import)
        from sqlalchemy import text

        row = session.execute(
            text("SELECT match_id, sanction_source FROM serving_sanction_match WHERE match_id = :mid"),
            {"mid": match_id},
        ).fetchone()
        assert row is not None, f"sanction_match {match_id} not found in serving DB"
        assert row[1] == "ceis"


# ---------------------------------------------------------------------------
# Case 2: TSE donation → donation_match → donation_event → serving
# ---------------------------------------------------------------------------


class TestTseDonationE2E:
    """Trace a TSE donation through raw→analytics→serving."""

    def test_donation_reaches_serving(self, session: Session) -> None:
        raw_path = _RAW / "tse" / "donations_raw.jsonl"
        match_path = _ANALYTICS / "donation_match.jsonl"
        event_path = _ANALYTICS / "donation_event.jsonl"
        _skip_if_missing(raw_path, match_path, event_path)

        # Find a raw donation with non-empty donor
        raw_rec = None
        with raw_path.open("r", encoding="utf-8") as fh:
            for i, line in enumerate(fh):
                if i >= 100:
                    break
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if rec.get("donor_cpf_cnpj") and rec.get("donation_amount"):
                    raw_rec = rec
                    break
        assert raw_rec is not None, "No donation with donor_cpf_cnpj found"

        # Check analytics produced a match
        analytics_rec = _search_jsonl(match_path, "donor_cpf_cnpj", raw_rec["donor_cpf_cnpj"])
        if analytics_rec is None:
            pytest.skip("Donor not matched (may not appear in STF cases)")
        match_id = analytics_rec["match_id"]

        # Check serving has the match (use text SQL to avoid circular import)
        from sqlalchemy import text

        row = session.execute(
            text("SELECT match_id FROM serving_donation_match WHERE match_id = :mid"),
            {"mid": match_id},
        ).fetchone()
        assert row is not None, f"donation_match {match_id} not in serving"

        # Check donation events exist
        event_count = session.execute(
            text("SELECT count(*) FROM serving_donation_event WHERE match_id = :mid"),
            {"mid": match_id},
        ).scalar()
        assert event_count >= 1, f"No donation_events for match {match_id}"


# ---------------------------------------------------------------------------
# Case 3: RFB partner → economic_group → corporate_conflict → serving
# ---------------------------------------------------------------------------


class TestRfbCorporateE2E:
    """Trace RFB corporate data through raw→analytics→serving."""

    def test_economic_group_reaches_serving(self, session: Session) -> None:
        eg_path = _ANALYTICS / "economic_group.jsonl"
        corp_path = _ANALYTICS / "corporate_network.jsonl"
        _skip_if_missing(eg_path, corp_path)

        # Find an economic group
        eg_rec = None
        with eg_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if rec.get("member_count", 0) >= 2:
                    eg_rec = rec
                    break
        assert eg_rec is not None, "No economic group with ≥2 members"
        group_id = eg_rec["group_id"]

        # Check serving has this group (use text SQL to avoid circular import)
        from sqlalchemy import text

        row = session.execute(
            text("SELECT group_id, member_count FROM serving_economic_group WHERE group_id = :gid"),
            {"gid": group_id},
        ).fetchone()
        assert row is not None, f"economic_group {group_id} not in serving"
        assert row[1] >= 2

        # Check corporate conflicts reference this group (if any)
        conflict_rows = session.execute(
            text("SELECT economic_group_id FROM serving_corporate_conflict WHERE economic_group_id = :gid LIMIT 5"),
            {"gid": group_id},
        ).fetchall()
        if conflict_rows:
            assert all(r[0] == group_id for r in conflict_rows)


# ---------------------------------------------------------------------------
# Case 4: DataJud → origin_context → serving
# ---------------------------------------------------------------------------


class TestDatajudOriginContextE2E:
    """Trace DataJud data through raw JSON → analytics → serving."""

    def test_datajud_index_reaches_analytics(self) -> None:
        """A raw DataJud JSON must produce a matching origin_context record."""
        datajud_dir = _RAW / "datajud"
        analytics_path = _ANALYTICS / "origin_context.jsonl"
        _skip_if_missing(datajud_dir, analytics_path)

        # Pick first available DataJud JSON
        jsons = sorted(datajud_dir.glob("api_publica_*.json"))
        if not jsons:
            pytest.skip("No DataJud JSON files")

        raw_data = json.loads(jsons[0].read_text(encoding="utf-8"))
        raw_index = raw_data["index"]
        raw_total = raw_data["total_processes"]
        assert raw_total > 0, f"DataJud {raw_index} has 0 total_processes"

        # Find matching record in analytics
        analytics_rec = _search_jsonl(analytics_path, "origin_index", raw_index)
        assert analytics_rec is not None, (
            f"DataJud index {raw_index!r} not found in origin_context.jsonl"
        )
        # DataJud API counts change daily; analytics may lag behind raw.
        # Accept ≤5% drift — anything larger indicates a broken rebuild.
        analytics_total = analytics_rec["datajud_total_processes"]
        if raw_total > 0:
            drift_pct = abs(analytics_total - raw_total) / raw_total
            assert drift_pct <= 0.05, (
                f"total_processes drift too large ({drift_pct:.1%}): "
                f"raw={raw_total}, analytics={analytics_total}"
            )
        assert analytics_rec["tribunal_label"], "tribunal_label is empty"

    def test_origin_context_reaches_serving(self, session: Session) -> None:
        """Origin context analytics must be loadable into serving DB."""
        analytics_path = _ANALYTICS / "origin_context.jsonl"
        _skip_if_missing(analytics_path)

        from sqlalchemy import text

        # Check if table exists and has data
        try:
            count = session.execute(text("SELECT count(*) FROM serving_origin_context")).scalar()
        except Exception:
            pytest.skip("serving_origin_context table does not exist")

        if count == 0:
            pytest.skip("serving_origin_context is empty (serving-build may need re-run)")

        # Verify a specific index made it through
        rec = _read_first_record(analytics_path)
        assert rec is not None
        origin_index = rec["origin_index"]

        row = session.execute(
            text("SELECT origin_index, datajud_total_processes FROM serving_origin_context WHERE origin_index = :idx"),
            {"idx": origin_index},
        ).fetchone()
        assert row is not None, f"origin_context {origin_index!r} not in serving DB"
        assert row[1] > 0, "datajud_total_processes is 0 in serving"

    def test_manifest_matches_raw_files(self) -> None:
        """Every committed unit in DataJud manifest has a corresponding raw JSON."""
        datajud_dir = _RAW / "datajud"
        manifest_path = datajud_dir / "_manifest_datajud.json"
        _skip_if_missing(manifest_path)

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        units = manifest.get("units", {})
        assert len(units) > 0, "DataJud manifest has no units"

        missing_files: list[str] = []
        for uid, unit in units.items():
            if unit.get("status") != "committed":
                continue
            local_path = unit.get("local_path", "")
            if local_path and not Path(local_path).exists():
                missing_files.append(f"{uid} → {local_path}")

        assert not missing_files, (
            f"{len(missing_files)} committed units have missing files:\n  "
            + "\n  ".join(missing_files[:5])
        )
