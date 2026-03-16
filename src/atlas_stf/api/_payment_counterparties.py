"""Query functions for payment counterparty endpoints."""

from __future__ import annotations

import json
from typing import Any, cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..serving.models import ServingPaymentCounterparty
from .schemas import (
    PaginatedPaymentCounterpartiesResponse,
    PaymentCounterpartyItem,
)


def _parse_json_list(raw: str | None) -> list:
    if not raw:
        return []
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []


def _parse_json_dict(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def _row_to_item(row: ServingPaymentCounterparty) -> PaymentCounterpartyItem:
    return PaymentCounterpartyItem(
        counterparty_id=row.counterparty_id,
        counterparty_identity_key=row.counterparty_identity_key,
        identity_basis=row.identity_basis,
        counterparty_name=row.counterparty_name,
        counterparty_tax_id=row.counterparty_tax_id,
        counterparty_tax_id_normalized=row.counterparty_tax_id_normalized,
        counterparty_document_type=row.counterparty_document_type,
        total_received_brl=row.total_received_brl,
        payment_count=row.payment_count,
        election_years=_parse_json_list(row.election_years_json),
        payer_parties=_parse_json_list(row.payer_parties_json),
        payer_actor_type=row.payer_actor_type,
        first_payment_date=row.first_payment_date,
        last_payment_date=row.last_payment_date,
        states=_parse_json_list(row.states_json),
        cnae_codes=_parse_json_list(row.cnae_codes_json),
        provenance=_parse_json_dict(row.provenance_json),
    )


def get_payment_counterparties(
    session: Session,
    page: int,
    page_size: int,
) -> PaginatedPaymentCounterpartiesResponse:
    count_stmt = select(func.count()).select_from(ServingPaymentCounterparty)
    total = session.execute(count_stmt).scalar_one()

    stmt = (
        select(ServingPaymentCounterparty)
        .order_by(
            ServingPaymentCounterparty.total_received_brl.desc(),
            ServingPaymentCounterparty.counterparty_id.asc(),
        )
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    rows = cast(
        list[ServingPaymentCounterparty],
        session.scalars(stmt).all(),
    )

    return PaginatedPaymentCounterpartiesResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[_row_to_item(r) for r in rows],
    )
