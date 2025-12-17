from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Optional, Literal
from pydantic import BaseModel
from datetime import date

from analysis.api.dependencies import get_db

router = APIRouter(
    prefix="/api/analytics/skus",
    tags=["SKU Intelligence"]
)

# --- SCHEMAS (Defined inline for clarity, or import from schemas.py) ---
class SKUTrendOut(BaseModel):
    period: date 
    total_spend: float
    total_quantity: float
    order_count: int
    avg_unit_price: float

class SKUSpend(BaseModel):
    unified_sku_id: str
    sku_name: str
    total_spend: float
    total_quantity: float
    order_count: int

class SKUPriceVarianceOut(BaseModel):
    supplier_name: str
    avg_unit_price: float
    min_unit_price: float
    max_unit_price: float
    price_stddev: Optional[float]

class SKUProfileOut(BaseModel):
    unified_sku_id: str
    sku_name: str
    total_spend: float
    order_count: int
    active_months: int
    supplier_count: int
    avg_unit_price: float
    price_stddev: Optional[float] = 0.0

# ---------------------------------------------------------
# 1️⃣ SKU PROFILE (The "Header" Card)
# ---------------------------------------------------------
@router.get(
    "/{unified_sku_id}/profile",
    response_model=SKUProfileOut,
    summary="Get high-level stats for a single SKU"
)
def get_sku_profile(
    unified_sku_id: str,
    db: Session = Depends(get_db)
):
    """
    Returns summary stats (Total Spend, Volatility, etc.) for a SKU.
    """
    sql = """
        SELECT 
            unified_sku_id,
            sku_name,
            total_spend,
            order_count,
            active_months,
            supplier_count,
            avg_unit_price,
            COALESCE(price_stddev, 0) as price_stddev
        FROM app_analytics.mv_sku_contract_base
        WHERE unified_sku_id = :sku_id
    """
    row = db.execute(text(sql), {"sku_id": unified_sku_id}).mappings().first()
    
    if not row:
        raise HTTPException(status_code=404, detail="SKU not found")
        
    return row

# ---------------------------------------------------------
# 2️⃣ SKU SPEND RANKING
# ---------------------------------------------------------
@router.get(
    "/ranking",
    response_model=List[SKUSpend],
    summary="Top SKUs by spend / quantity / orders"
)
def get_sku_ranking(
    year: Optional[int] = Query(None, description="Filter by year"),
    month: Optional[int] = Query(None, ge=1, le=12),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db)
):
    """
    Returns high-impact SKUs.
    Aggregates data from the Monthly MV.
    """
    sql = """
        SELECT 
            unified_sku_id,
            sku_name,
            SUM(total_spend)     AS total_spend,
            SUM(total_quantity)  AS total_quantity,
            SUM(order_count)     AS order_count
        FROM app_analytics.mv_sku_monthly_metrics
        WHERE (:year IS NULL OR order_year = :year)
          -- Safe date filter logic
          AND (:month IS NULL OR order_month >= MAKE_DATE(:year, :month, 1) AND order_month < MAKE_DATE(:year, :month, 1) + INTERVAL '1 month')
        GROUP BY unified_sku_id, sku_name
        ORDER BY total_spend DESC
        LIMIT :limit
    """
    
    if month and not year:
        raise HTTPException(status_code=400, detail="Year is required when filtering by Month.")

    return db.execute(
        text(sql), 
        {"year": year, "month": month, "limit": limit}
    ).mappings().all()

# ---------------------------------------------------------
# 3️⃣ SKU TRENDS (The Chart Data)
# ---------------------------------------------------------
@router.get(
    "/{unified_sku_id}/trend",
    response_model=List[SKUTrendOut],
    summary="Get spending trend over time (Weekly or Monthly)"
)
def get_sku_trend(
    unified_sku_id: str,
    grain: Literal["week", "month"] = Query("month"),
    limit: int = Query(52, le=100),
    db: Session = Depends(get_db)
):
    """
    Powers the Line Chart. Switches between Weekly/Monthly MVs.
    """
    if grain == "week":
        sql = """
            SELECT 
                order_week as period,
                weekly_spend as total_spend,
                weekly_quantity as total_quantity,
                weekly_order_count as order_count,
                avg_unit_price
            FROM app_analytics.mv_sku_weekly_metrics
            WHERE unified_sku_id = :sku_id
            ORDER BY order_week DESC
            LIMIT :limit
        """
    else:
        sql = """
            SELECT 
                order_month as period,
                total_spend,
                total_quantity,
                order_count,
                CASE WHEN total_quantity > 0 THEN total_spend / total_quantity ELSE 0 END as avg_unit_price
            FROM app_analytics.mv_sku_monthly_metrics
            WHERE unified_sku_id = :sku_id
            ORDER BY order_month DESC
            LIMIT :limit
        """

    rows = db.execute(text(sql), {"sku_id": unified_sku_id, "limit": limit}).mappings().all()
    return list(reversed(rows))

# ---------------------------------------------------------
# 4️⃣ SKU PRICE VARIANCE
# ---------------------------------------------------------
@router.get(
    "/{unified_sku_id}/price-variance",
    response_model=List[SKUPriceVarianceOut],
    summary="Price variance across suppliers for a SKU"
)
def get_sku_price_variance(
    unified_sku_id: str,
    db: Session = Depends(get_db)
):
    """
    Identifies pricing instability.
    """
    # 🚨 FIX APPLIED: Renamed columns to match Pydantic schema
    sql = """
        SELECT 
            supplier_name,
            avg_unit_price,
            min_price AS min_unit_price, 
            max_price AS max_unit_price,
            price_stddev
        FROM app_analytics.mv_sku_price_variance
        WHERE unified_sku_id = :sku_id
        ORDER BY price_stddev DESC
    """

    rows = db.execute(text(sql), {"sku_id": unified_sku_id}).mappings().all()
    
    if not rows:
        raise HTTPException(status_code=404, detail="SKU not found or has no variance data")

    return rows

# ---------------------------------------------------------
# 5️⃣ SKU WEEKLY FREQUENCY (Simple)
# ---------------------------------------------------------
@router.get(
    "/{unified_sku_id}/weekly",
    summary="Raw weekly metrics for table view"
)
def get_sku_weekly_frequency(
    unified_sku_id: str,
    year: Optional[int] = Query(None),
    db: Session = Depends(get_db)
):
    sql = """
        SELECT 
            order_week,
            weekly_spend,
            weekly_quantity,
            weekly_order_count,
            supplier_count
        FROM app_analytics.mv_sku_weekly_metrics
        WHERE unified_sku_id = :sku_id
          AND (:year IS NULL OR order_year = :year)
        ORDER BY order_week DESC
    """
    return db.execute(text(sql), {"sku_id": unified_sku_id, "year": year}).mappings().all()


