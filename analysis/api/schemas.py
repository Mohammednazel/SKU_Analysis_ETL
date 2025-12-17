from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class ContractCandidateOut(BaseModel):
    unified_sku_id: str
    sku_name: str
    total_spend: float
    active_months: int
    supplier_count: int
    contract_priority_score: int
    contract_recommendation: str


class ContractCandidateDetailOut(ContractCandidateOut):
    avg_unit_price: float
    price_stddev: float
    frequency_score: int
    materiality_score: int
    volatility_score: int
    fragmentation_score: int


class SKUProfileOut(BaseModel):
    unified_sku_id: str
    sku_name: str
    total_spend: float
    order_count: int
    active_months: int
    supplier_count: int
    avg_unit_price: float
    price_stddev: Optional[float] = 0.0

class GlobalKPIs(BaseModel):
    total_orders: int
    total_skus: int
    total_suppliers: int
    total_spend: float
    first_order_date: Optional[datetime] = None
    last_order_date: Optional[datetime] = None

class SupplierSummary(BaseModel):
    supplier_name: str
    total_spend: float
    order_count: int
    sku_count: int

class SupplierTier(BaseModel):
    supplier_name: str
    tier: str
    dependency_ratio: str
    total_spend: float
    sku_count: int
    order_count: int

class SupplierMonthlyMetric(BaseModel):
    supplier_name: str
    order_month: str
    order_year: int
    total_spend: float
    order_count: int
    sku_count: int