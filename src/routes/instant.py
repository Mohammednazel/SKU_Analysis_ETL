# API endpoints powered by materialized views

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
from api.deps_auth import verify_api_key


# -------------------------------------------------
# Router with global authentication dependency
# -------------------------------------------------
router = APIRouter(
    prefix="/api/v1",
    tags=["Instant Analytics"],
    dependencies=[Depends(verify_api_key)]  # <-- ✅ Auth applied to all endpoints
)

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


# -------------------------------------------------
# KPI Summary (cached, ultra-fast)
# -------------------------------------------------
@router.get("/kpi")
def get_kpi_summary(
    request: Request,
    response: Response,
    db: Session = Depends(get_session)
):
    """
    Returns global KPI metrics from mv_kpi_summary.
    Uses in-memory caching + ETag for instant load.
    """
    cache_key = "global_kpi_summary"
    cached = get_cached(cache_key)
    if cached:
        response.headers["X-Cache-Hit"] = "true"
        return cached

    # Query precomputed KPI summary
    row = db.execute(text("SELECT * FROM mv_kpi_summary;")).mappings().first()
    if not row:
        return {"message": "KPI summary not found"}

    result = dict(row)
    result["last_refresh_time"] = (
        result["last_refresh_time"].strftime("%Y-%m-%d %H:%M:%S")
        if result.get("last_refresh_time")
        else None
    )

    # Prepare payload
    payload = {
        "meta": {
            "source": "mv_kpi_summary",
            "refreshed_at": result["last_refresh_time"],
        },
        "data": {
            "total_pos": result.get("total_pos", 0),
            "total_skus": result.get("total_skus", 0),
            "total_suppliers": result.get("total_suppliers", 0),
            "total_spend": float(result.get("total_spend", 0)),
            "total_quantity": float(result.get("total_quantity", 0)),
            "avg_unit_price_weighted": float(result.get("avg_unit_price_weighted", 0)),
            "avg_order_value": float(result.get("avg_order_value", 0)),
            "spend_per_supplier": float(result.get("spend_per_supplier", 0)),
            "spend_per_sku": float(result.get("spend_per_sku", 0)),
            "spend_variability_ratio": float(result.get("spend_variability_ratio", 0)),
            "last_po_date": result.get("last_po_date"),
        },
    }

    # Cache results in-memory
    cache_result(cache_key, payload, ttl=300)  # Cache for 5 mins
    response.headers["X-Cache-Hit"] = "false"

    # ETag for client-side caching
    etag = make_etag({"path": str(request.url.path), "timestamp": result["last_refresh_time"]})
    set_cache_headers(response, etag, max_age=300)

    return payload


# -------------------------------------------------
# Supplier Price Analysis (avg unit price per SKU per supplier)
# -------------------------------------------------
@router.get("/supplier/price_analysis")
def supplier_price_analysis(
    request: Request,
    response: Response,
    db: Session = Depends(get_session),
    supplier_id: Optional[str] = Query(None),
    product_id: Optional[str] = Query(None),
    limit: int = Depends(positive_limit),
    offset: int = Depends(non_negative_offset),
):
    """
    Analyze supplier pricing by SKU and supplier.
    Useful for identifying supplier-level price variance.
    """
    conds = []
    params = {"limit": limit, "offset": offset}
    if supplier_id:
        conds.append("supplier_id = :supplier_id")
        params["supplier_id"] = supplier_id
    if product_id:
        conds.append("product_id = :product_id")
        params["product_id"] = product_id

    where_clause = f"WHERE {' AND '.join(conds)}" if conds else ""

    total = db.execute(
        text(f"SELECT COUNT(*) FROM mv_supplier_price_analysis {where_clause}")
    ).scalar()

    rows = db.execute(
        text(f"""
            SELECT supplier_id, product_id, po_count, total_qty, total_spend,
                   avg_unit_price, last_purchase_date
            FROM mv_supplier_price_analysis
            {where_clause}
            ORDER BY avg_unit_price DESC
            LIMIT :limit OFFSET :offset
        """),
        params,
    ).mappings().all()

    def serialize_datetime(val):
        if isinstance(val, datetime):
            return val.strftime("%Y-%m-%d")
        return val

    data = [
        {
            **dict(r),
            "last_purchase_date": serialize_datetime(r.get("last_purchase_date")),
        }
        for r in rows
    ]
    payload = {
        "data": data,
        "meta": {"limit": limit, "offset": offset, "count": total},
    }

    etag = make_etag(
        {"path": str(request.url.path), "query": dict(request.query_params), "total": total}
    )
    set_cache_headers(response, etag, max_age=120)
    return payload


# -------------------------------------------------
# Spend Trend / Monthly (all suppliers)
# -------------------------------------------------
@router.get("/spend/trend")
def spend_trend_monthly(
    request: Request,
    response: Response,
    db: Session = Depends(get_session),
):
    """
    Returns total spend trend by month for all suppliers.
    Ideal for line chart visualizations.
    """
    rows = db.execute(
        text("""
            SELECT month, total_spend, total_qty, total_pos
            FROM mv_spend_trend_monthly
            ORDER BY month
        """)
    ).mappings().all()

    data = [
        {
            "month": r["month"].strftime("%Y-%m") if r.get("month") else None,
            "total_spend": float(r["total_spend"] or 0),
            "total_qty": float(r["total_qty"] or 0),
            "total_pos": int(r["total_pos"] or 0),
        }
        for r in rows
    ]
    payload = {"data": data, "count": len(data)}

    etag = make_etag(
        {"path": str(request.url.path), "count": len(data), "last": data[-1] if data else None}
    )
    set_cache_headers(response, etag, max_age=300)
    return payload


# -------------------------------------------------
# Helpers: pagination validation (page + page_size)
# -------------------------------------------------
from fastapi import HTTPException

def validate_page(page: int) -> int:
    if page < 1:
        raise HTTPException(status_code=400, detail="page must be >= 1")
    return page

ALLOWED_PAGE_SIZES = (5, 10, 25, 50)
def validate_page_size(page_size: int) -> int:
    if page_size not in ALLOWED_PAGE_SIZES:
        raise HTTPException(status_code=400, detail=f"page_size must be one of {ALLOWED_PAGE_SIZES}")
    return page_size

def build_search_clause(term: str) -> str:
    # Search against product_id + description + supplier_id
    # Using ILIKE with %term% (GIN trigram accelerates this)
    return "(product_id ILIKE :q OR description ILIKE :q OR supplier_id ILIKE :q)"

# -------------------------------------------------
# Filters: purchasing groups list (for dropdown)
# -------------------------------------------------
@router.get("/filters/purchasing_groups")
def filter_purchasing_groups(
    db: Session = Depends(get_session),
    q: Optional[str] = Query(None, description="Search filter (optional)"),
    limit: int = Query(50, ge=1, le=200)
):
    where = ""
    params = {"limit": limit}
    if q:
        where = "WHERE purchasing_group ILIKE :q"
        params["q"] = f"%{q}%"

    rows = db.execute(
        text(f"""
            SELECT DISTINCT purchasing_group
            FROM mv_sku_analysis
            {where}
            ORDER BY purchasing_group NULLS LAST
            LIMIT :limit
        """),
        params
    ).scalars().all()
    return {"data": rows, "count": len(rows)}

# -------------------------------------------------
# Filters: suppliers list (for dropdown)
# -------------------------------------------------
@router.get("/filters/suppliers")
def filter_suppliers(
    db: Session = Depends(get_session),
    q: Optional[str] = Query(None, description="Search filter (optional)"),
    limit: int = Query(50, ge=1, le=200)
):
    where = ""
    params = {"limit": limit}
    if q:
        where = "WHERE supplier_id ILIKE :q"
        params["q"] = f"%{q}%"

    rows = db.execute(
        text(f"""
            SELECT DISTINCT supplier_id
            FROM mv_sku_analysis
            {where}
            ORDER BY supplier_id
            LIMIT :limit
        """),
        params
    ).scalars().all()
    return {"data": rows, "count": len(rows)}

# -------------------------------------------------
# Detailed SKU Analysis (search + filters + pagination)
# -------------------------------------------------
@router.get("/sku/analysis")  # , response_model=PageSKUDetailed)  # optional
def sku_analysis_table(
    request: Request,
    response: Response,
    db: Session = Depends(get_session),

    # Filters
    search: Optional[str] = Query(None, description="Search by SKU/product_id, description or supplier"),
    purchasing_group: Optional[str] = Query(None),
    supplier_id: Optional[str] = Query(None),

    # Sorting
    order_by: Optional[str] = Query("spend", description="spend|qty|price|orders|product"),
    sort: Optional[str] = Query("desc", description="asc|desc"),

    # Pagination
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1),
):
    page = validate_page(page)
    page_size = validate_page_size(page_size)
    offset = (page - 1) * page_size

    # WHERE clause
    conds = []
    params = {"limit": page_size, "offset": offset}

    if purchasing_group:
        conds.append("purchasing_group = :pgroup")
        params["pgroup"] = purchasing_group

    if supplier_id:
        conds.append("supplier_id = :supplier_id")
        params["supplier_id"] = supplier_id

    if search:
        conds.append(build_search_clause(search))
        params["q"] = f"%{search}%"

    where_clause = f"WHERE {' AND '.join(conds)}" if conds else ""

    # ORDER BY mapping
    order_map = {
        "spend": "total_spend",
        "qty": "total_qty",
        "price": "avg_unit_price",
        "orders": "order_count",
        "product": "product_id"
    }
    order_col = order_map.get(order_by, "total_spend")
    sort_dir = "ASC" if (sort or "").lower() == "asc" else "DESC"
    order_clause = f"ORDER BY {order_col} {sort_dir}, product_id ASC"

    # Count
    total = db.execute(
        text(f"SELECT COUNT(*) FROM mv_sku_analysis {where_clause}"),
        params
    ).scalar() or 0

    # Data
    rows = db.execute(
        text(f"""
            SELECT product_id, description, purchasing_group, supplier_id,
                   order_count, total_qty, total_spend, avg_unit_price, last_order_date
            FROM mv_sku_analysis
            {where_clause}
            {order_clause}
            LIMIT :limit OFFSET :offset
        """),
        params
    ).mappings().all()

    def dts(v):
        return v.strftime("%Y-%m-%d") if isinstance(v, datetime) else v

    data = [
        {
            **dict(r),
            "last_order_date": dts(r.get("last_order_date")),
            "total_spend": float(r.get("total_spend") or 0),
            "total_qty": float(r.get("total_qty") or 0),
            "avg_unit_price": float(r.get("avg_unit_price") or 0),
        } for r in rows
    ]

    total_pages = (total + page_size - 1) // page_size if page_size else 1

    payload = {
        "data": data,
        "meta": {
            "count": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "order_by": order_col,
            "sort": sort_dir.lower(),
            "filters": {
                "search": search,
                "purchasing_group": purchasing_group,
                "supplier_id": supplier_id
            }
        }
    }

    # Caching headers
    etag = make_etag({
        "path": str(request.url.path),
        "query": dict(request.query_params),
        "count": total
    })
    set_cache_headers(response, etag, max_age=60)
    return payload
