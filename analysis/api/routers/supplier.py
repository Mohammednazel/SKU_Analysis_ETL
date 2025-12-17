from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Optional

from analysis.api.dependencies import get_db
from analysis.api.schemas import (
    SupplierSummary,
    SupplierTier,
    SupplierMonthlyMetric
)

router = APIRouter(
    prefix="/api/analytics/suppliers",
    tags=["Supplier Analytics"]
)

# ---------------------------------------------------------
# 1️⃣ Supplier Overview
# ---------------------------------------------------------
@router.get(
    "",
    response_model=List[SupplierSummary],
    summary="Supplier master overview"
)
def get_suppliers(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db)
):
    sql = """
        SELECT
            supplier_name,
            total_spend,
            order_count,
            sku_count
        FROM app_analytics.mv_supplier_base
        ORDER BY total_spend DESC
        LIMIT :limit
    """

    return db.execute(
        text(sql),
        {"limit": limit}
    ).mappings().all()


# ---------------------------------------------------------
# 2️⃣ Supplier Tiering (A/B/C)
# ---------------------------------------------------------
@router.get(
    "/tiers",
    response_model=List[SupplierTier],
    summary="Supplier tiering with dependency risk"
)
def get_supplier_tiers(db: Session = Depends(get_db)):
    sql = """
        SELECT
            supplier_name,
            supplier_tier AS tier,
            dependency_risk_level AS dependency_ratio,
            total_spend,
            sku_count,
            order_count
        FROM app_analytics.mv_supplier_tiering
        ORDER BY total_spend DESC
    """

    return db.execute(text(sql)).mappings().all()


# ---------------------------------------------------------
# 3️⃣ Supplier Monthly Trend
# ---------------------------------------------------------
@router.get(
    "/{supplier_name}/monthly",
    response_model=List[SupplierMonthlyMetric],
    summary="Monthly performance of a supplier"
)
def get_supplier_monthly_metrics(
    supplier_name: str,
    year: Optional[int] = Query(None),
    db: Session = Depends(get_db)
):
    sql = """
        SELECT
            supplier_name,
            order_month,
            order_year,
            total_spend,
            order_count,
            sku_count
        FROM app_analytics.mv_supplier_monthly_metrics
        WHERE supplier_name = UPPER(TRIM(:supplier))
          AND (:year IS NULL OR order_year = :year)
        ORDER BY order_month
    """

    rows = db.execute(
        text(sql),
        {"supplier": supplier_name, "year": year}
    ).mappings().all()

    if not rows:
        raise HTTPException(status_code=404, detail="Supplier not found")

    return rows