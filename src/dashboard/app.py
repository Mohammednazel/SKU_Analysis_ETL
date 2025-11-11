# src/dashboard/app.py
import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import plotly.express as px
import os
from dotenv import load_dotenv

# ======================
# CONFIGURATION
# ======================
load_dotenv()  # Load API key and config from .env

API_BASE = os.getenv("API_BASE_URL", "http://127.0.0.1:8000/api/v1")
API_KEY = os.getenv("API_SECRET_KEY", "")  # Secure API key for backend access

st.set_page_config(
    page_title="Procurement Intelligence Dashboard",
    page_icon="📊",
    layout="wide",
)

# ======================
# HELPERS
# ======================
@st.cache_data(ttl=300)
def fetch_json(endpoint: str, params: dict = None):
    """Fetch JSON from backend API with API key authentication"""
    headers = {"x-api-key": API_KEY}
    url = f"{API_BASE}/{endpoint}"
    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        if r.status_code == 401:
            st.error("🔒 Unauthorized: Invalid or missing API key. Please check your configuration.")
            return {}
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"❌ Error fetching {endpoint}: {e}")
        return {}

def format_currency(val):
    return f"SAR {val:,.0f}"

# ======================
# NAVIGATION BAR
# ======================
st.markdown(
    """
    <style>
    .nav-bar {
        background-color: #0e1117;
        padding: 0.8rem 1rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
        color: white;
        border-bottom: 1px solid #262730;
    }
    .nav-title {
        font-size: 1.5rem;
        font-weight: 600;
        color: #00b4d8;
    }
    .nav-links a {
        margin-right: 15px;
        color: #f1f1f1;
        text-decoration: none;
        font-weight: 500;
    }
    .nav-links a:hover {
        color: #00b4d8;
    }
    </style>
    <div class="nav-bar">
        <div class="nav-title">📦 Procurement Analytics</div>
        <div class="nav-links">
            <a href="#">Dashboard Home</a>
            <a href="#" style="color:#00b4d8;">Fragmented SKU Analysis</a>
            <a href="javascript:history.back()">← Back</a>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("## 🧾 Fragmented SKU Analysis")

# ======================
# SECTION 1: PERFORMANCE OVERVIEW
# ======================
st.markdown("### 📈 Section 1: Performance Overview")

kpi_data = fetch_json("kpi")
if kpi_data and "data" in kpi_data:
    data = kpi_data["data"]
    last_refresh = kpi_data["meta"].get("refreshed_at")

    st.caption(f"Last updated: {last_refresh or datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Spend", format_currency(data["total_spend"]))
    col2.metric("Total POs", f"{data['total_pos']:,}")
    col3.metric("Total Suppliers", f"{data['total_suppliers']:,}")
    col4.metric("Total SKUs", f"{data['total_skus']:,}")

# ======================
# SECTION 2: ANALYTICS & TRENDS
# ======================
st.markdown("---")
st.markdown("### 📊 Section 2: Analytics & Trends")

st.subheader("Spending Patterns & Supplier Analysis")

# --- Spend Trend Chart (Line Chart)
spend_trend = fetch_json("supplier/monthly", {"limit": 100})
if spend_trend.get("data"):
    df_trend = pd.DataFrame(spend_trend["data"])
    df_trend["month"] = pd.to_datetime(df_trend["month"])
    df_month = df_trend.groupby("month", as_index=False)["total_spend"].sum()

    fig_trend = px.line(
        df_month,
        x="month",
        y="total_spend",
        title="Monthly Spend Trend (Last 12 Months)",
        markers=True,
        line_shape="spline",
    )
    fig_trend.update_layout(xaxis_title="Month", yaxis_title="Total Spend (SAR)")
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("No spend trend data available.")

# --- Spend by Purchasing Group (Pie Chart)
st.subheader("Spend by Purchasing Group")
pgroup_data = fetch_json("pgroup/top", {"limit": 10})
if pgroup_data.get("data"):
    df_pg = pd.DataFrame(pgroup_data["data"])
    fig_pg = px.pie(
        df_pg,
        names="purchasing_group",
        values="total_spend",
        title="Spend Distribution by Purchasing Group",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    st.plotly_chart(fig_pg, use_container_width=True)
else:
    st.info("No purchasing group spend data found.")

# --- Top SKUs by Spend (Bar Chart)
st.subheader("Top 8 SKUs by Spend")
top_skus = fetch_json("sku/top", {"order_by": "spend", "limit": 8})
if top_skus.get("data"):
    df_skus = pd.DataFrame(top_skus["data"])
    df_skus = df_skus.sort_values("total_spend", ascending=True)
    fig_sku = px.bar(
        df_skus,
        x="total_spend",
        y="product_id",
        orientation="h",
        title="Top 8 SKUs by Total Spend",
        text_auto=".2s",
        color="total_spend",
        color_continuous_scale="Blues",
    )
    fig_sku.update_layout(xaxis_title="Total Spend (SAR)", yaxis_title="Product ID")
    st.plotly_chart(fig_sku, use_container_width=True)
else:
    st.info("No SKU data available.")

# ======================
# SECTION 3: TRANSACTION ANALYSIS
# ======================
st.markdown("---")
st.markdown("### 💳 Section 3: Transaction Analysis")
st.caption("Complete view of SKU transactions with advanced filters")

col1, col2, col3, col4 = st.columns([2, 2, 2, 1])

filter_pg = fetch_json("filters/purchasing_groups")
filter_sup = fetch_json("filters/suppliers")

pgroups_data = filter_pg.get("data", [])
suppliers_data = filter_sup.get("data", [])

if pgroups_data and isinstance(pgroups_data[0], dict):
    pgroups = ["All"] + sorted([r.get("purchasing_group", "") for r in pgroups_data])
else:
    pgroups = ["All"] + sorted([str(r) for r in pgroups_data])

if suppliers_data and isinstance(suppliers_data[0], dict):
    suppliers = ["All"] + sorted([r.get("supplier_id", "") for r in suppliers_data])
else:
    suppliers = ["All"] + sorted([str(r) for r in suppliers_data])

search = col1.text_input("🔍 Search SKUs / Description / Supplier", "")
pgroup_filter = col2.selectbox("Purchasing Group", pgroups, index=0)
supplier_filter = col3.selectbox("Supplier", suppliers, index=0)
page_size = col4.selectbox("Rows per page", [5, 10, 25, 50], index=1)

params = {"page": 1, "page_size": page_size}
if search:
    params["search"] = search
if pgroup_filter != "All":
    params["purchasing_group"] = pgroup_filter
if supplier_filter != "All":
    params["supplier_id"] = supplier_filter

sku_analysis = fetch_json("sku/analysis", params)

if sku_analysis.get("data"):
    df_analysis = pd.DataFrame(sku_analysis["data"])
    df_analysis.rename(
        columns={
            "product_id": "SKU ID",
            "description": "Description",
            "purchasing_group": "Purchasing Group",
            "supplier_id": "Supplier",
            "total_qty": "Quantity",
            "avg_unit_price": "Unit Price",
            "total_spend": "Total Cost",
        },
        inplace=True,
    )
    df_analysis["Total Cost"] = df_analysis["Total Cost"].apply(format_currency)

    st.dataframe(
        df_analysis[
            [
                "SKU ID",
                "Description",
                "Purchasing Group",
                "Supplier",
                "Quantity",
                "Unit Price",
                "Total Cost",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )
    st.caption(
        f"Showing {len(df_analysis)} of {sku_analysis['meta']['count']} total results"
    )
else:
    st.info("No SKU transaction data available.")
