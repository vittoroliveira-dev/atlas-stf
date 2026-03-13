from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..serving.models import ServingMlOutlierScore
from .schemas import MlOutlierScoreResponse


def _to_ml_outlier_response(ml_outlier: ServingMlOutlierScore) -> MlOutlierScoreResponse:
    return MlOutlierScoreResponse(
        decision_event_id=ml_outlier.decision_event_id,
        comparison_group_id=ml_outlier.comparison_group_id,
        ml_anomaly_score=ml_outlier.ml_anomaly_score,
        ml_rarity_score=ml_outlier.ml_rarity_score,
        ensemble_score=ml_outlier.ensemble_score,
        n_features=ml_outlier.n_features,
        n_samples=ml_outlier.n_samples,
        generated_at=ml_outlier.generated_at,
    )


def _load_ml_outlier_map(
    session: Session,
    decision_event_ids: list[str],
) -> dict[str, ServingMlOutlierScore]:
    if not decision_event_ids:
        return {}
    rows = session.scalars(
        select(ServingMlOutlierScore).where(ServingMlOutlierScore.decision_event_id.in_(decision_event_ids))
    ).all()
    return {row.decision_event_id: row for row in rows}


def _ensemble_score_for(
    ml_outlier_map: dict[str, ServingMlOutlierScore],
    decision_event_id: str,
) -> float | None:
    ml_outlier = ml_outlier_map.get(decision_event_id)
    return ml_outlier.ensemble_score if ml_outlier is not None else None
