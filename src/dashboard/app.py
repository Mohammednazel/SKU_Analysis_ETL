# src/dashboard/app.py
import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import plotly.express as px
import os
from dotenv import load_dotenv

# ======================================================
# CONFIG
# ======================================================
load_dotenv()

API_BASE = os.getenv("API_BASE_URL", "http://127.0.0.1:8000/api/v1")
API_KEY = os.getenv("API_SECRET_KEY", "")

st.set_page_config(
    page_title="Procurement Intelligence",
    page_icon="📦",
    layout="wide"
)

# ======================================================
# GLOBAL CSS FOR MODERN UI
# ======================================================
st.markdown("""
<style>

html, body, [class*="css"]  {
    font-family: 'Inter', sans-serif;
}

.kpi-card {
    background: #111827;
    padding: 20px;
    border-radius: 12px;
    border: 1px solid #1f2937;
    color: white;
    text-align: center;
}

.kpi-value {
    font-size: 1.6rem;
    font-weight: 600;
}

.kpi-label {
    font-size: 0.9rem;
    opacity: 0.8;
}

.nav-title {
    font-size: 1.4rem;
    font-weight: 600;
    color: #00b4d8;
}

</style>
""", unsafe_allow_html=True)


# ======================================================
# HELPERS
# ======================================================
@st.cache_data(ttl=180)
def api_get(endpoint: str, params: dict = None):
    headers = {"x-api-key": API_KEY}
    url = f"{API_BASE}/{endpoint}"

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=20)
        if resp.status_code == 401:
            st.error("❌ Unauthorized. Check API key.")
            return {}
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error(f"API Error ({endpoint}): {e}")
        return {}


def currency(v):
    try:
        return f"SAR {float(v):,.0f}"
    except:
        return v


# ======================================================
# NAVIGATION BAR
# ======================================================
st.markdown("""
<div style="padding:15px; background:#0f1117; border-radius:8px; margin-bottom:10px;">
    <span class="nav-title">📦 Procurement Intelligence Analytics</span>
</div>
""", unsafe_allow_html=True)


# ======================================================
# TABS
# ======================================================
tab_kpi, tab_trend, tab_sku, tab_frag, tab_contract, tab_supplier = st.tabs([
    "📈 KPI Dashboard",
    "📊 Spend Trends",
    "💳 SKU Explorer",
    "🧩 Fragmentation",
    "📜 Contract Opportunities",
    "🏭 Supplier Insights"
])

# ======================================================
# TAB 1: KPI DASHBOARD
# ======================================================
with tab_kpi:
    st.subheader("📈 KPI Summary")

    kpi = api_get("kpi")
    if kpi.get("data"):
        data = kpi["data"]
        last = kpi["meta"]["refreshed_at"]

        st.caption(f"Last refreshed: {last}")

        metric_cols = st.columns(4)
        metrics = [
            ("Total Spend", currency(data["total_spend"])),
            ("Total POs", f"{data['total_pos']:,}"),
            ("Total Suppliers", f"{data['total_suppliers']:,}"),
            ("Total SKUs", f"{data['total_skus']:,}")
        ]

        for col, (label, value) in zip(metric_cols, metrics):
            col.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-value">{value}</div>
                    <div class="kpi-label">{label}</div>
                </div>
            """, unsafe_allow_html=True)


# ======================================================
# TAB 2: SPEND TRENDS
# ======================================================
with tab_trend:
    st.subheader("📊 Monthly Spend Trend")

    trend = api_get("spend/trend")
    if trend.get("data"):
        df = pd.DataFrame(trend["data"])
        df["month"] = pd.to_datetime(df["month"])

        fig = px.area(
            df,
            x="month",
            y="total_spend",
            title="Company-wide Spend Trend",
            markers=True
        )
        fig.update_traces(opacity=0.85)
        st.plotly_chart(fig, use_container_width=True)

    # ------ Purchasing Group ------
    st.subheader("🏷 Spend by Purchasing Group")
    pg = api_get("pgroup/top", {"limit": 12})
    if pg.get("data"):
        df_pg = pd.DataFrame(pg["data"])
        fig = px.bar(
            df_pg,
            x="purchasing_group",
            y="total_spend",
            color="total_spend",
            title="Top Purchasing Groups by Spend",
            color_continuous_scale="Purples"
        )
        st.plotly_chart(fig, use_container_width=True)


# ======================================================
# TAB 3: SKU EXPLORER
# ======================================================
with tab_sku:
    st.subheader("💳 SKU Transaction Explorer")

    # Filters
    col1, col2, col3 = st.columns([3, 2, 2])

    suppliers = api_get("filters/suppliers").get("data", [])
    pgroups = api_get("filters/purchasing_groups").get("data", [])

    search = col1.text_input("Search (SKU / Description / Supplier)")
    filter_pg = col2.selectbox("Purchasing Group", ["All"] + pgroups)
    filter_sup = col3.selectbox("Supplier", ["All"] + suppliers)

    params = {"page": 1, "page_size": 10}
    if search:
        params["search"] = search
    if filter_pg != "All":
        params["purchasing_group"] = filter_pg
    if filter_sup != "All":
        params["supplier_id"] = filter_sup

    sku_data = api_get("sku/analysis", params)

    if sku_data.get("data"):
        df = pd.DataFrame(sku_data["data"])
        st.dataframe(df, use_container_width=True)
        st.caption(f"{sku_data['meta']['count']} total results")


# ======================================================
# TAB 4: SKU FRAGMENTATION (Action List Only)
# ======================================================
with tab_frag:
    st.subheader("🧩 SKU Fragmentation: Top Consolidation Opportunities")

    frag_data = api_get("sku/fragmentation", {"limit": 200})
    if frag_data.get("data"):
        df_frag = pd.DataFrame(frag_data["data"])

        # Actionable fragmentation rows
        st.markdown("#### 🎯 Action List: High-Priority SKU Consolidation")
        df_action = df_frag[df_frag["fragmentation_level"].isin(
            ["HIGHLY_FRAGMENTED", "MODERATELY_FRAGMENTED"]
        )].sort_values("total_spend", ascending=False)

        df_action["total_spend"] = df_action["total_spend"].map(currency)

        st.dataframe(
            df_action[[
                "product_id",
                "fragmentation_level",
                "supplier_count",
                "total_spend"
            ]].rename(columns={
                "product_id": "Product ID",
                "fragmentation_level": "Fragmentation",
                "supplier_count": "Supplier Count",
                "total_spend": "Total Spend"
            }),
            use_container_width=True
        )


# ======================================================
# TAB 5: CONTRACT OPPORTUNITIES
# ======================================================
with tab_contract:
    st.subheader("📜 Contracting Opportunities")

    contracts = api_get("contracts/opportunities", {"limit": 50})
    if contracts.get("data"):
        st.dataframe(pd.DataFrame(contracts["data"]), use_container_width=True)


# ======================================================
# TAB 6: SUPPLIER INSIGHTS
# ======================================================
with tab_supplier:
    st.subheader("🏭 Supplier Consolidation")

    conso = api_get("suppliers/consolidation", {"limit": 50})
    if conso.get("data"):
        st.dataframe(pd.DataFrame(conso["data"]), use_container_width=True)

    st.subheader("💰 Best Price Opportunities")
    best = api_get("suppliers/best_price_opportunities", {"limit": 50})
    if best.get("data"):
        st.dataframe(pd.DataFrame(best["data"]), use_container_width=True)
