from pydantic import BaseModel, field_validator
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
    
    @field_validator('contract_priority_score', mode='before')
    @classmethod
    def convert_score_to_int(cls, v):
        """Convert float to int for score"""
        if v is None:
            return 0
        return int(float(v))


class ContractCandidateDetailOut(ContractCandidateOut):
    avg_unit_price: float
    price_stddev: float
    frequency_score: int
    materiality_score: int
    volatility_score: int
    fragmentation_score: int
    
    @field_validator('frequency_score', 'materiality_score', 'volatility_score', 'fragmentation_score', mode='before')
    @classmethod
    def convert_scores_to_int(cls, v):
        """Convert score fields to int"""
        if v is None:
            return 0
        return int(float(v))


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