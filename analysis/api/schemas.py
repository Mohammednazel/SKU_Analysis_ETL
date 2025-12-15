from pydantic import BaseModel
from typing import Optional


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