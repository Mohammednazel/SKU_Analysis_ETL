from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from analysis.api.dependencies import get_db
from analysis.api.schemas import GlobalKPIs

router = APIRouter(
    prefix="/api/analytics/kpis",
    tags=["Executive KPIs"]
)

@router.get(
    "/global",
    response_model=GlobalKPIs,
    summary="Global procurement KPIs"
)
def get_global_kpis(db: Session = Depends(get_db)):
    """
    Business Purpose:
    -----------------
    Executive snapshot of procurement health.
    Used on dashboard landing page.
    """

    sql = """
        SELECT
            total_orders,
            total_skus,
            total_suppliers,
            total_spend,
            first_order_date,
            last_order_date
        FROM app_analytics.mv_global_kpis
    """

    row = db.execute(text(sql)).mappings().first()
    return row