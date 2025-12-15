# analysis/api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import the router we created
from analysis.api.routers import contracts
from analysis.api.routers import sku
# from analysis.api.routers import supplier  <-- KEEP COMMENTED UNTIL NEXT STEP

app = FastAPI(
    title="SKU Analysis Platform API",
    description="Enterprise Procurement Intelligence Engine",
    version="1.0.0"
)

# Enable CORS (Allows React/Frontend to talk to this API)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connect the routers to the app
app.include_router(contracts.router)
app.include_router(sku.router)
# app.include_router(supplier.router)        <-- KEEP COMMENTED UNTIL NEXT STEP

@app.get("/")
def root():
    return {"message": "SKU Analytics Engine is Online 🚀"}