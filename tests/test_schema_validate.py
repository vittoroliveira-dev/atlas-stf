from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.schema_validate import SchemaValidationError, validate_record, validate_records


def test_validate_records_accepts_process_record():
    records = [
        {
            "process_id": "proc_1",
            "process_number": "AC 1",
            "process_class": "AC",
            "filing_date": "2026-03-06",
            "closing_date": None,
            "origin_description": None,
            "origin_court_or_body": None,
            "branch_of_law": None,
            "subjects_raw": ["A"],
            "subjects_normalized": ["A"],
            "case_environment": None,
            "procedural_status": None,
            "raw_fields": {},
            "normalization_version": "process-v1",
            "source_id": "STF-TRANSP-REGDIST",
            "source_record_hash": "abc",
            "created_at": "2026-03-06T12:00:00+00:00",
            "updated_at": "2026-03-06T12:00:00+00:00",
        }
    ]
    validate_records(records, Path("schemas/process.schema.json"))


def test_validate_records_rejects_missing_required_field():
    records = [{"process_number": "AC 1"}]
    with pytest.raises(SchemaValidationError):
        validate_records(records, Path("schemas/process.schema.json"))


def test_validate_records_rejects_numeric_bounds_from_schema():
    records = [
        {
            "party_id": "party_1",
            "party_name_raw": "PARTE A",
            "party_name_normalized": None,
            "party_type": None,
            "normalization_confidence": 1.5,
            "normalization_version": None,
            "notes": None,
            "created_at": None,
            "updated_at": None,
        }
    ]
    with pytest.raises(SchemaValidationError, match="above maximum"):
        validate_records(records, Path("schemas/party.schema.json"))


def test_validate_record_rejects_nested_unknown_fields():
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["outer"],
        "properties": {
            "outer": {
                "type": "object",
                "additionalProperties": False,
                "required": ["inner"],
                "properties": {
                    "inner": {"type": "string"},
                },
            }
        },
    }

    with pytest.raises(SchemaValidationError, match="unknown fields: extra"):
        validate_record({"outer": {"inner": "ok", "extra": "x"}}, schema)


def test_validate_record_rejects_nested_missing_required_field():
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["outer"],
        "properties": {
            "outer": {
                "type": "object",
                "additionalProperties": False,
                "required": ["inner"],
                "properties": {
                    "inner": {"type": "string"},
                },
            }
        },
    }

    with pytest.raises(SchemaValidationError, match="missing required field inner"):
        validate_record({"outer": {}}, schema)


def test_validate_record_rejects_array_item_object_shape():
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["items"],
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["id"],
                    "properties": {
                        "id": {"type": "string"},
                    },
                },
            }
        },
    }

    with pytest.raises(SchemaValidationError, match="missing required field id"):
        validate_record({"items": [{}]}, schema)


def test_validate_record_accepts_nested_object_from_schema_file(tmp_path: Path):
    schema_path = tmp_path / "nested.schema.json"
    schema_path.write_text(
        json.dumps(
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["bundle"],
                "properties": {
                    "bundle": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["version", "items"],
                        "properties": {
                            "version": {"type": "string"},
                            "items": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": ["name"],
                                    "properties": {
                                        "name": {"type": "string"},
                                    },
                                },
                            },
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    validate_records(
        [{"bundle": {"version": "1", "items": [{"name": "ok"}]}}],
        schema_path,
    )
