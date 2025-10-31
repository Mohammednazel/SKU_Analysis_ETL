from fastapi import APIRouter, Depends, Response, Request, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
import time
from datetime import datetime

# Absolute imports (important for uvicorn reload)
from api.deps import get_session, positive_limit, non_negative_offset, validate_order_by
from api.schemas import PageSKUSpend, PageSupplierMonthly, PagePGroupSpend
from api.utils_cache import make_etag

router = APIRouter(prefix="/api/v1", tags=["Instant Analytics"])

# -------------------------------------------------
# In-memory cache store
# -------------------------------------------------
CACHE_TTL = 60  # seconds
_cache_store = {}

def cache_result(key: str, value: dict, ttl: int = CACHE_TTL):
    _cache_store[key] = (value, time.time() + ttl)

def get_cached(key: str):
    entry = _cache_store.get(key)
    if not entry:
        return None
    value, expiry = entry
    if time.time() > expiry:
        del _cache_store[key]
        return None
    return value

# -------------------------------------------------
# Utility
# -------------------------------------------------
def set_cache_headers(resp: Response, etag: str, max_age: int = 60):
    resp.headers["ETag"] = etag
    resp.headers["Cache-Control"] = f"public, max-age={max_age}"

# -------------------------------------------------
# Health endpoint
# -------------------------------------------------
@router.get("/health")
def health(db: Session = Depends(get_session)):
    db.execute(text("SELECT 1"))
    return {"status": "ok"}

# -------------------------------------------------
# Top SKUs (with in-memory caching)
# -------------------------------------------------
@router.get("/sku/top", response_model=PageSKUSpend)
def top_skus(
    request: Request,
    response: Response,
    db: Session = Depends(get_session),
    order_by: str = Depends(validate_order_by),
    limit: int = Depends(positive_limit),
    offset: int = Depends(non_negative_offset),
):
    cache_key = f"sku_top:{order_by}:{limit}:{offset}"
    cached = get_cached(cache_key)
    if cached:
        response.headers["X-Cache-Hit"] = "true"
        return cached

    # Determine sort column
    if order_by == "qty":
        order_clause = "ORDER BY total_qty DESC"
    elif order_by == "orders":
        order_clause = "ORDER BY order_count DESC"
    else:
        order_clause = "ORDER BY total_spend DESC"

    total = db.execute(text("SELECT COUNT(*) FROM mv_sku_spend")).scalar()

    rows = db.execute(
        text(f"""
            SELECT product_id, total_spend, total_qty, order_count,
                   COALESCE(avg_unit_price_weighted,0) AS avg_unit_price_weighted,
                   last_order_date
            FROM mv_sku_spend
            {order_clause}
            LIMIT :limit OFFSET :offset
        """),
        {"limit": limit, "offset": offset}
    ).mappings().all()

    def serialize_datetime(val):
        if isinstance(val, datetime):
            return val.strftime("%Y-%m-%d")
        return val

    data = [
        {
            **dict(r),
            "last_order_date": serialize_datetime(r.get("last_order_date"))
        }
        for r in rows
    ]

    payload = {"data": data, "meta": {"limit": limit, "offset": offset, "count": total}}

    # Save in-memory cache
    cache_result(cache_key, payload)
    response.headers["X-Cache-Hit"] = "false"

    # ETag + cache control
    etag = make_etag({"path": str(request.url.path), "query": dict(request.query_params), "total": total})
    set_cache_headers(response, etag, max_age=60)
    return payload

# -------------------------------------------------
# Supplier monthly (no caching)
# -------------------------------------------------
@router.get("/supplier/monthly", response_model=PageSupplierMonthly)
def supplier_monthly(
    request: Request,
    response: Response,
    db: Session = Depends(get_session),
    supplier_id: Optional[str] = Query(None),
    start_month: Optional[str] = Query(None, description="YYYY-MM-01"),
    end_month: Optional[str] = Query(None, description="YYYY-MM-01"),
    limit: int = Depends(positive_limit),
    offset: int = Depends(non_negative_offset),
):
    conds = []
    params = {"limit": limit, "offset": offset}
    if supplier_id:
        conds.append("supplier_id = :supplier_id")
        params["supplier_id"] = supplier_id
    if start_month and end_month:
        conds.append("month BETWEEN :start_month AND :end_month")
        params["start_month"] = start_month
        params["end_month"] = end_month

    where_clause = f"WHERE {' AND '.join(conds)}" if conds else ""

    total = db.execute(
        text(f"SELECT COUNT(*) FROM mv_supplier_monthly {where_clause}"),
        params
    ).scalar()

    rows = db.execute(
        text(f"""
            SELECT supplier_id, month, total_spend, po_count, unique_skus
            FROM mv_supplier_monthly
            {where_clause}
            ORDER BY month DESC, total_spend DESC
            LIMIT :limit OFFSET :offset
        """),
        params
    ).mappings().all()

    data = [dict(r) for r in rows]
    payload = {"data": data, "meta": {"limit": limit, "offset": offset, "count": total}}
    etag = make_etag({"path": str(request.url.path), "query": dict(request.query_params), "total": total})
    set_cache_headers(response, etag, max_age=60)
    return payload

# -------------------------------------------------
# Purchasing group spend (no caching)
# -------------------------------------------------
@router.get("/pgroup/top", response_model=PagePGroupSpend)
def pgroup_top(
    request: Request,
    response: Response,
    db: Session = Depends(get_session),
    limit: int = Depends(positive_limit),
    offset: int = Depends(non_negative_offset),
):
    total = db.execute(text("SELECT COUNT(*) FROM mv_pgroup_spend")).scalar()

    rows = db.execute(
        text("""
            SELECT purchasing_group, total_spend, po_count, avg_order_value
            FROM mv_pgroup_spend
            ORDER BY total_spend DESC
            LIMIT :limit OFFSET :offset
        """),
        {"limit": limit, "offset": offset}
    ).mappings().all()

    data = [dict(r) for r in rows]
    payload = {"data": data, "meta": {"limit": limit, "offset": offset, "count": total}}
    etag = make_etag({"path": str(request.url.path), "query": dict(request.query_params), "total": total})
    set_cache_headers(response, etag, max_age=120)
    return payload
