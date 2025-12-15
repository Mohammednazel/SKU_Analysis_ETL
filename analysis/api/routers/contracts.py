from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Optional

from analysis.api.dependencies import get_db
from analysis.api.schemas import (
    ContractCandidateOut,
    ContractCandidateDetailOut
)

router = APIRouter(
    prefix="/api/analytics/contracts",
    tags=["Contract Opportunities"]
)

# ---------------------------------------------------------
# 1️⃣ GET TOP CONTRACT CANDIDATES (RANKED)
# ---------------------------------------------------------
@router.get(
    "/candidates",
    response_model=List[ContractCandidateOut],
    summary="Top SKUs recommended for contracts"
)
def get_contract_candidates(
    limit: int = Query(50, ge=1, le=200),
    min_score: Optional[int] = Query(None, ge=0, le=100),
    db: Session = Depends(get_db)
):
    sql = """
        SELECT
            unified_sku_id,
            sku_name,
            total_spend,
            active_months,
            supplier_count,
            contract_priority_score,
            contract_recommendation
        FROM app_analytics.mv_contract_candidates
        WHERE (:min_score IS NULL OR contract_priority_score >= :min_score)
        ORDER BY contract_priority_score DESC
        LIMIT :limit
    """

    return db.execute(
        text(sql),
        {"limit": limit, "min_score": min_score}
    ).mappings().all()


# ---------------------------------------------------------
# 2️⃣ GET CONTRACT SCORING BREAKDOWN (EXPLAINABILITY)
# ---------------------------------------------------------
@router.get(
    "/{unified_sku_id}",
    response_model=ContractCandidateDetailOut,
    summary="Explain why a SKU is recommended for contract"
)
def get_contract_candidate_detail(
    unified_sku_id: str,
    db: Session = Depends(get_db)
):
    sql = """
        SELECT
            unified_sku_id,
            sku_name,
            total_spend,
            active_months,
            supplier_count,
            avg_unit_price,
            price_stddev,
            frequency_score,
            materiality_score,
            volatility_score,
            fragmentation_score,
            contract_priority_score,
            contract_recommendation
        FROM app_analytics.mv_contract_candidates
        WHERE unified_sku_id = :sku_id
    """

    row = db.execute(
        text(sql),
        {"sku_id": unified_sku_id}
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="SKU not found")

    return row
