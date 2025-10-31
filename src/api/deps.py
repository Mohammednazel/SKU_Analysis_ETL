from fastapi import Depends, HTTPException, Query
from sqlalchemy.orm import Session
from .database import get_db

def get_session(db: Session = Depends(get_db)) -> Session:
    return db

def positive_limit(limit: int = Query(50, ge=1, le=500)) -> int:
    return limit

def non_negative_offset(offset: int = Query(0, ge=0)) -> int:
    return offset

def validate_order_by(order_by: str = Query("spend")) -> str:
    allowed = {"spend", "qty", "orders"}
    if order_by not in allowed:
        raise HTTPException(status_code=400, detail=f"order_by must be one of {sorted(allowed)}")
    return order_by
