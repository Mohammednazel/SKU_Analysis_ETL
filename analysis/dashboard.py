# dashboard_sku_profile.py
import streamlit as st
import requests
import pandas as pd

API_BASE = "http://localhost:8000/api/analytics/skus"

st.set_page_config(page_title="SKU Profile", layout="wide")

st.title("📦 SKU Contract Profile")

# --- SKU Selector ---
sku_id = st.text_input("Enter Unified SKU ID")

if not sku_id:
    st.info("Please enter a SKU ID to view the profile.")
    st.stop()

# --- API Call ---
url = f"{API_BASE}/{sku_id}/profile"
resp = requests.get(url)

if resp.status_code != 200:
    st.error("SKU not found or API error")
    st.stop()

data = resp.json()

# --------------------
# 1️⃣ Header Section
# --------------------
st.subheader(data["sku_name"])
st.caption(f"Unified SKU ID: {data['unified_sku_id']}")

uom_df = pd.DataFrame(data["uoms"])

# --------------------
# 2️⃣ KPI Tiles
# --------------------
total_spend = uom_df["total_spend"].sum()
total_orders = uom_df["order_count"].sum()
supplier_count = uom_df["supplier_count"].max()
active_months = uom_df["active_months"].max()
avg_price = uom_df["avg_unit_price"].mean()
price_volatility = uom_df["price_stddev"].mean()

col1, col2, col3, col4, col5, col6 = st.columns(6)

col1.metric("💰 Total Spend", f"{total_spend:,.0f}")
col2.metric("📦 Orders", total_orders)
col3.metric("📆 Active Months", active_months)
col4.metric("🏭 Suppliers", supplier_count)
col5.metric("💲 Avg Price", f"{avg_price:,.2f}")
col6.metric("📉 Price Volatility", f"{price_volatility:,.2f}")

# --------------------
# 3️⃣ UOM Profile Table
# --------------------
st.subheader("📏 Unit of Measure Breakdown")

st.dataframe(
    uom_df[[
        "unit_of_measure",
        "order_count",
        "active_months",
        "supplier_count",
        "total_quantity",
        "total_spend",
        "avg_unit_price",
        "price_stddev"
    ]].sort_values("total_spend", ascending=False),
    use_container_width=True
)

# --------------------
# 4️⃣ Contract Readiness Signals
# --------------------
st.subheader("📊 Contract Readiness Signals")

signals = []

if total_orders > 50:
    signals.append("🟢 High purchase frequency")

if active_months >= 6:
    signals.append("🟢 Stable demand over time")

if supplier_count > 3:
    signals.append("🟡 High supplier fragmentation")

if price_volatility > 5:
    signals.append("🔴 High price volatility")

for s in signals:
    st.write(s)
