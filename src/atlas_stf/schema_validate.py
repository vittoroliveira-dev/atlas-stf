"""Minimal JSON Schema validator for project-owned schemas."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class SchemaValidationError(ValueError):
    """Raised when a record does not satisfy a project schema."""


def load_schema(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_type(value: Any, allowed: list[str], path: str) -> None:
    if value is None:
        if "null" in allowed:
            return
        raise SchemaValidationError(f"{path}: null is not allowed")

    for allowed_type in allowed:
        if allowed_type == "null":
            continue
        if allowed_type == "string" and isinstance(value, str):
            return
        if allowed_type == "integer" and isinstance(value, int) and not isinstance(value, bool):
            return
        if allowed_type == "number" and isinstance(value, (int, float)) and not isinstance(value, bool):
            return
        if allowed_type == "boolean" and isinstance(value, bool):
            return
        if allowed_type == "object" and isinstance(value, dict):
            return
        if allowed_type == "array" and isinstance(value, list):
            return
    raise SchemaValidationError(f"{path}: expected {allowed}, got {type(value).__name__}")


def _validate_format(value: Any, fmt: str, path: str) -> None:
    if value is None:
        return
    if not isinstance(value, str):
        raise SchemaValidationError(f"{path}: format {fmt} requires string")
    if fmt == "date":
        if not _DATE_RE.match(value):
            raise SchemaValidationError(f"{path}: invalid date format")
        datetime.strptime(value, "%Y-%m-%d")
        return
    if fmt == "date-time":
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise SchemaValidationError(f"{path}: invalid date-time format") from exc


def _validate_items(values: list[Any], items_schema: dict[str, Any], path: str) -> None:
    for idx, value in enumerate(values):
        validate_value(value, items_schema, f"{path}[{idx}]")


def _validate_numeric_bounds(value: Any, schema: dict[str, Any], path: str) -> None:
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        return
    minimum = schema.get("minimum")
    if minimum is not None and value < minimum:
        raise SchemaValidationError(f"{path}: value {value!r} is below minimum {minimum}")
    maximum = schema.get("maximum")
    if maximum is not None and value > maximum:
        raise SchemaValidationError(f"{path}: value {value!r} is above maximum {maximum}")


def _validate_object(value: dict[str, Any], schema: dict[str, Any], path: str) -> None:
    required = schema.get("required", [])
    for field in required:
        if field not in value:
            raise SchemaValidationError(f"{path}: missing required field {field}")

    properties = schema.get("properties", {})
    additional_properties = schema.get("additionalProperties", True)
    if additional_properties is False:
        unknown = set(value) - set(properties)
        if unknown:
            unknown_list = ", ".join(sorted(unknown))
            raise SchemaValidationError(f"{path}: unknown fields: {unknown_list}")

    for field, field_value in value.items():
        if field in properties:
            validate_value(field_value, properties[field], f"{path}.{field}")


def validate_value(value: Any, schema: dict[str, Any], path: str) -> None:
    field_type = schema.get("type")
    if field_type is not None:
        allowed = field_type if isinstance(field_type, list) else [field_type]
        _validate_type(value, allowed, path)

    if value is None:
        return

    if "enum" in schema and value not in schema["enum"]:
        raise SchemaValidationError(f"{path}: value {value!r} not in enum")

    if "format" in schema:
        _validate_format(value, schema["format"], path)

    _validate_numeric_bounds(value, schema, path)

    if isinstance(value, list) and "items" in schema:
        _validate_items(value, schema["items"], path)

    if isinstance(value, dict):
        _validate_object(value, schema, path)


def validate_record(record: dict[str, Any], schema: dict[str, Any], path: str = "$") -> None:
    validate_value(record, schema, path)


def validate_records(records: list[dict[str, Any]], schema_path: Path) -> None:
    schema = load_schema(schema_path)
    for idx, record in enumerate(records):
        validate_record(record, schema, path=f"$[{idx}]")
