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


class ContractOpportunity(BaseModel):
     product_id: str 
     supplier_id: Optional[str] 
     avg_orders_per_month: float 
     purchase_consistency_pct: float 
     purchase_frequency: str 
     contract_recommendation: str 
     annual_spend_projected: float

class PageContractOpportunities(BaseModel):
     data: List[ContractOpportunity] 
     meta: dict 

class SupplierConsolidation(BaseModel):
     supplier_id: str 
     unique_skus: int 
     total_spend: float 
     spend_rank: int 
     spend_pct: float 
     supplier_tier: str 
     consolidation_action: str 
     
class PageSupplierConsolidation(BaseModel): 
    data: List[SupplierConsolidation] 
    meta: dict 
    
class VolumeDiscountOpportunity(BaseModel): 
    product_id: str 
    current_supplier_id: str 
    best_supplier_id: str 
    current_avg_price: float 
    best_unit_price: float 
    potential_savings: float 
    savings_pct: float 
    opportunity_level: str 
    
class PageVolumeDiscountOpportunities(BaseModel): 
    data: List[VolumeDiscountOpportunity] 
    meta: dict 

class SKUFragmentation(BaseModel): 
    product_id: str 
    supplier_count: int 
    total_spend: float 
    fragmentation_score: float 
    fragmentation_level: str 
    
class PageSKUFragmentation(BaseModel): 
    data: List[SKUFragmentation] 
    meta: dict