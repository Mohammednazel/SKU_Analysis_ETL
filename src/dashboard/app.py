import streamlit as st
import pandas as pd
import requests
import plotly.express as px
from datetime import datetime

# =========================================================
# CONFIG
# =========================================================
API_BASE = "http://127.0.0.1:8000/api/v1"
ETL_LOG_QUERY = "SELECT mode, start_time, end_time, rows_processed, status FROM etl_run_log ORDER BY run_id DESC LIMIT 1"

st.set_page_config(
    page_title="Procurement Analytics Dashboard",
    page_icon="📦",
    layout="wide"
)

st.title("📊 Procurement Analytics Dashboard")
st.caption("Powered by FastAPI Materialized Views + PostgreSQL")

# =========================================================
# API HELPER
# =========================================================
@st.cache_data(ttl=120)
def fetch_data(endpoint: str, params=None):
    """Fetch JSON data from the FastAPI endpoints."""
    try:
        res = requests.get(f"{API_BASE}/{endpoint}", params=params or {}, timeout=30)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        st.error(f"❌ API call failed: {endpoint} → {e}")
        return None

# =========================================================
# KPI SECTION
# =========================================================
st.markdown("### 🔹 Key Performance Indicators (Last Refresh)")

col1, col2, col3, col4 = st.columns(4)

# Fetch summaries
sku_data = fetch_data("sku/top", {"limit": 100})
supplier_data = fetch_data("supplier/monthly", {"limit": 100})
pgroup_data = fetch_data("pgroup/top", {"limit": 100})

if sku_data and supplier_data and pgroup_data:
    df_sku = pd.DataFrame(sku_data["data"])
    df_supplier = pd.DataFrame(supplier_data["data"])
    df_group = pd.DataFrame(pgroup_data["data"])

    total_spend = df_sku["total_spend"].sum()
    total_orders = df_supplier["po_count"].sum()
    total_suppliers = df_supplier["supplier_id"].nunique()
    unique_skus = df_sku["product_id"].nunique()

    col1.metric("💰 Total Spend (SAR)", f"{total_spend:,.0f}")
    col2.metric("📦 Unique SKUs", unique_skus)
    col3.metric("🏭 Active Suppliers", total_suppliers)
    col4.metric("🧾 Total POs", total_orders)
else:
    st.warning("No KPI data available — check API health.")

# =========================================================
# FILTERS
# =========================================================
st.markdown("### 🔹 Filters")
colf1, colf2, colf3 = st.columns(3)
order_by = colf1.selectbox("Order By", ["spend", "qty", "orders"])
limit = colf2.slider("Limit Results", 10, 100, 50)
refresh_btn = colf3.button("🔄 Refresh Data")

if refresh_btn:
    st.cache_data.clear()
    st.rerun()


# =========================================================
# TABS
# =========================================================
tab1, tab2, tab3, tab4 = st.tabs(["🏷️ Top SKUs", "🏭 Supplier Trends", "🧾 Purchasing Groups", "⚙️ ETL Health"])

# =========================================================
# TAB 1: TOP SKUs
# =========================================================
with tab1:
    st.subheader("Top SKUs by Spend / Quantity / Orders")

    data = fetch_data("sku/top", {"order_by": order_by, "limit": limit, "offset": 0})
    if data:
        df = pd.DataFrame(data["data"])
        st.dataframe(df, width='stretch')

        fig = px.bar(
            df.head(10),
            x="product_id",
            y="total_spend",
            color="total_spend",
            title=f"Top 10 SKUs by {order_by.capitalize()}",
        )
        st.plotly_chart(fig, width='stretch')

# =========================================================
# TAB 2: SUPPLIER MONTHLY
# =========================================================
with tab2:
    st.subheader("Supplier Monthly Spend Trends")

    supplier_id = st.text_input("🔍 Supplier ID (optional):", "")
    params = {"limit": 200, "offset": 0}
    if supplier_id:
        params["supplier_id"] = supplier_id

    data = fetch_data("supplier/monthly", params)
    if data:
        df = pd.DataFrame(data["data"])
        st.dataframe(df, width='stretch')
        if not df.empty:
            fig = px.line(
                df,
                x="month",
                y="total_spend",
                color="supplier_id",
                title="📈 Monthly Spend by Supplier",
                markers=True
            )
            st.plotly_chart(fig, width='stretch')

# =========================================================
# TAB 3: PURCHASING GROUP SPEND
# =========================================================
with tab3:
    st.subheader("Purchasing Group Spend Breakdown")

    data = fetch_data("pgroup/top", {"limit": limit, "offset": 0})
    if data:
        df = pd.DataFrame(data["data"])
        st.dataframe(df, width='stretch')

        colp1, colp2 = st.columns(2)
        with colp1:
            fig1 = px.pie(
                df,
                values="total_spend",
                names="purchasing_group",
                title="Spend by Purchasing Group",
            )
            st.plotly_chart(fig1, use_container_width=True)
        with colp2:
            fig2 = px.bar(
                df,
                x="purchasing_group",
                y="avg_order_value",
                title="Average Order Value per Group",
            )
            st.plotly_chart(fig2, use_container_width=True)

# =========================================================
# TAB 4: ETL HEALTH
# =========================================================
with tab4:
    st.subheader("⚙️ ETL Health Monitor")

    try:
        import sqlalchemy
        from sqlalchemy import create_engine, text
        import os
        from dotenv import load_dotenv

        load_dotenv()
        DATABASE_URL = os.getenv("DATABASE_URL")
        engine = create_engine(DATABASE_URL)

        with engine.connect() as conn:
            df_log = pd.read_sql("SELECT * FROM etl_run_log ORDER BY run_id DESC LIMIT 5", conn)
        st.dataframe(df_log, use_container_width=True)
        st.caption(f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        st.error(f"Cannot connect to database: {e}")
