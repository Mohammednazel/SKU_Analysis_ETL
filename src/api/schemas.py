from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date

# Responses
class SKUSpend(BaseModel):
    product_id: str
    total_spend: float = Field(..., description="Aggregated spend")
    total_qty: float
    order_count: int
    avg_unit_price_weighted: float
    last_order_date: Optional[str] = None

class SupplierMonthly(BaseModel):
    supplier_id: str
    month: date
    total_spend: float
    po_count: int
    unique_skus: int

class PGroupSpend(BaseModel):
    purchasing_group: str
    total_spend: float
    po_count: int
    avg_order_value: float

class PageMeta(BaseModel):
    limit: int
    offset: int
    count: int

class PageSKUSpend(BaseModel):
    data: List[SKUSpend]
    meta: PageMeta

class PageSupplierMonthly(BaseModel):
    data: List[SupplierMonthly]
    meta: PageMeta

class PagePGroupSpend(BaseModel):
    data: List[PGroupSpend]
    meta: PageMeta

# Filters
class DateRange(BaseModel):
    start_month: Optional[date] = None  # first day of month
    end_month: Optional[date] = None
class SKUDetailedRow(BaseModel):
    product_id: str
    description: Optional[str]
    purchasing_group: Optional[str]
    supplier_id: Optional[str]
    order_count: int
    total_qty: float
    total_spend: float
    avg_unit_price: float
    last_order_date: Optional[str]

class PageSKUDetailed(BaseModel):
    data: List[SKUDetailedRow]
    meta: dict