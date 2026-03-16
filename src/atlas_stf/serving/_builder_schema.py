from __future__ import annotations

import logging
import warnings
from hashlib import sha256

from sqlalchemy import inspect
from sqlalchemy.orm import Session

from .models import Base, ServingSchemaMeta

SERVING_SCHEMA_VERSION = 16
SERVING_SCHEMA_SINGLETON_KEY = "serving"
logger = logging.getLogger(__name__)


def _serving_schema_fingerprint() -> str:
    parts: list[str] = []
    for table in Base.metadata.sorted_tables:
        parts.append(f"table:{table.name}")
        for column in table.columns:
            parts.append(
                ":".join(
                    [
                        "column",
                        table.name,
                        column.name,
                        str(column.type),
                        "nullable" if column.nullable else "not-null",
                        "pk" if column.primary_key else "non-pk",
                    ]
                )
            )
        for constraint in sorted(table.indexes, key=lambda item: item.name or ""):
            parts.append(
                ":".join(
                    [
                        "index",
                        table.name,
                        constraint.name or "",
                        ",".join(column.name for column in constraint.columns),
                    ]
                )
            )
    return sha256("|".join(parts).encode("utf-8")).hexdigest()


def _schema_is_compatible(engine) -> bool:  # noqa: ANN001
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    managed_tables = set(Base.metadata.tables)
    existing_managed_tables = existing_tables & managed_tables
    if not existing_managed_tables:
        return True
    if ServingSchemaMeta.__tablename__ not in existing_managed_tables:
        return False

    with Session(engine) as session:
        meta = session.get(ServingSchemaMeta, SERVING_SCHEMA_SINGLETON_KEY)

    if meta is None:
        return False

    return meta.schema_version == SERVING_SCHEMA_VERSION and meta.schema_fingerprint == _serving_schema_fingerprint()


def _ensure_compatible_schema(engine) -> None:  # noqa: ANN001
    if _schema_is_compatible(engine):
        Base.metadata.create_all(engine)
        return

    message = "Detected incompatible serving schema; dropping managed tables before rebuild."
    warnings.warn(message, RuntimeWarning, stacklevel=2)
    logger.warning(message)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
