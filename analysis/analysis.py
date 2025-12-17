import streamlit as st
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
from typing import List, Dict, Optional

# ============================================================================
# PAGE CONFIG
# ============================================================================

st.set_page_config(
    page_title="SKU Procurement Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .header-title {
        font-size: 32px;
        font-weight: 700;
        color: #1a1a1a;
        margin-bottom: 10px;
    }
    .section-header {
        font-size: 20px;
        font-weight: 600;
        color: #1f77b4;
        margin-top: 30px;
        margin-bottom: 15px;
        border-bottom: 2px solid #1f77b4;
        padding-bottom: 10px;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================================================
# API BASE CONFIG
# ============================================================================

BASE_URL = "http://127.0.0.1:8000/api/analytics"

# ============================================================================
# API FUNCTIONS
# ============================================================================

@st.cache_data(ttl=3600)
def fetch_global_kpis():
    """Fetch global KPIs"""
    try:
        response = requests.get(f"{BASE_URL}/kpis/global")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Failed to fetch KPIs: {e}")
        return None

def fetch_sku_ranking(year: Optional[int] = None, month: Optional[int] = None, limit: int = 100):
    """Fetch SKU ranking - no cache due to parameters"""
    try:
        params = {"limit": limit}
        if year:
            params["year"] = year
        if month:
            params["month"] = month
        response = requests.get(f"{BASE_URL}/skus/ranking", params=params)
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Failed to fetch SKU ranking: {e}")
        return pd.DataFrame()

def fetch_contract_candidates(min_score: Optional[int] = None, limit: int = 200):
    """Fetch contract candidates - no cache due to parameters"""
    try:
        params = {"limit": limit}
        if min_score:
            params["min_score"] = min_score
        response = requests.get(f"{BASE_URL}/contracts/candidates", params=params)
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Failed to fetch contract candidates: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_contract_detail(sku_id: str):
    """Fetch contract detail for a SKU"""
    try:
        response = requests.get(f"{BASE_URL}/contracts/{sku_id}")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Failed to fetch contract detail: {e}")
        return None

@st.cache_data(ttl=3600)
def fetch_sku_trend(sku_id: str, grain: str = "month"):
    """Fetch SKU trend data"""
    try:
        response = requests.get(f"{BASE_URL}/skus/{sku_id}/trend", params={"grain": grain})
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Failed to fetch SKU trend: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_sku_price_variance(sku_id: str):
    """Fetch SKU price variance across suppliers"""
    try:
        response = requests.get(f"{BASE_URL}/skus/{sku_id}/price-variance")
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Failed to fetch price variance: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_suppliers(limit: int = 50):
    """Fetch supplier list"""
    try:
        response = requests.get(f"{BASE_URL}/suppliers", params={"limit": limit})
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Failed to fetch suppliers: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_supplier_tiers():
    """Fetch supplier tiering"""
    try:
        response = requests.get(f"{BASE_URL}/suppliers/tiers")
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Failed to fetch supplier tiers: {e}")
        return pd.DataFrame()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_currency(value):
    """Format value as currency"""
    try:
        if pd.isna(value) or value == 0:
            return "$0"
        if value >= 1e9:
            return f"${value/1e9:.2f}B"
        elif value >= 1e6:
            return f"${value/1e6:.2f}M"
        elif value >= 1e3:
            return f"${value/1e3:.2f}K"
        return f"${value:.0f}"
    except:
        return "$0"

def format_number(value):
    """Format large numbers"""
    try:
        if pd.isna(value) or value == 0:
            return "0"
        if value >= 1e9:
            return f"{value/1e9:.2f}B"
        elif value >= 1e6:
            return f"{value/1e6:.2f}M"
        elif value >= 1e3:
            return f"{value/1e3:.0f}K"
        return f"{value:.0f}"
    except:
        return "0"

def get_recommendation_emoji(recommendation: str) -> str:
    """Get emoji for recommendation"""
    if not recommendation:
        return "⚪"
    if "STRATEGIC" in str(recommendation).upper():
        return "🔴"
    elif "NEGOTIATE" in str(recommendation).upper():
        return "🟠"
    elif "MONITOR" in str(recommendation).upper():
        return "🟡"
    else:
        return "⚪"

def get_recommendation_color(recommendation: str) -> str:
    """Get color for recommendation"""
    if not recommendation:
        return "#7f7f7f"
    if "STRATEGIC" in str(recommendation).upper():
        return "#d62728"
    elif "NEGOTIATE" in str(recommendation).upper():
        return "#ff7f0e"
    elif "MONITOR" in str(recommendation).upper():
        return "#ffcc00"
    else:
        return "#7f7f7f"

def render_metric_card(col, title: str, value: str, delta: str = None, color: str = "#1f77b4"):
    """Render a metric card"""
    with col:
        st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, {color}20 0%, {color}05 100%);
                padding: 20px;
                border-radius: 10px;
                border-left: 4px solid {color};
            ">
                <p style="color: #666; font-size: 13px; font-weight: 600; margin: 0;">
                    {title}
                </p>
                <p style="color: #1a1a1a; font-size: 28px; font-weight: 700; margin: 10px 0 5px 0;">
                    {value}
                </p>
                {f'<p style="color: #666; font-size: 12px; margin: 0;">{delta}</p>' if delta else ''}
            </div>
        """, unsafe_allow_html=True)

# ============================================================================
# PAGE: DASHBOARD
# ============================================================================

def page_dashboard():
    """Main dashboard page"""
    st.markdown('<p class="header-title">📊 SKU Procurement Intelligence</p>', unsafe_allow_html=True)
    st.markdown('<p style="color: #666; font-size: 14px;">Enterprise Procurement Analytics & Contract Opportunity Platform</p>', unsafe_allow_html=True)
    
    # FILTERS
    st.markdown('<p class="section-header">🔍 Global Filters</p>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        year_filter = st.selectbox("Year", options=[2024, 2025], index=0, key="year_filter")
    with col2:
        month_filter = st.selectbox("Month", options=[None] + list(range(1, 13)), 
                                    format_func=lambda x: "All Months" if x is None else f"Month {x}",
                                    index=0, key="month_filter")
    with col3:
        suppliers_df = fetch_suppliers(limit=200)
        supplier_list = ["All Suppliers"] + (suppliers_df["supplier_name"].tolist() if not suppliers_df.empty else [])
        supplier_filter = st.selectbox("Supplier", options=supplier_list, key="supplier_filter")
    with col4:
        sku_search = st.text_input("Search SKU", placeholder="Enter SKU name or ID...", key="sku_search")
    
    st.markdown("---")
    
    # KPI CARDS
    st.markdown('<p class="section-header">💼 Executive KPI Summary</p>', unsafe_allow_html=True)
    
    kpis = fetch_global_kpis()
    
    if kpis:
        col1, col2, col3, col4 = st.columns(4)
        render_metric_card(col1, "Total Spend", format_currency(kpis.get("total_spend", 0)), "↓ 3.2% MoM", "#1f77b4")
        render_metric_card(col2, "Total SKUs", format_number(kpis.get("total_skus", 0)), "↑ 2.1% MoM", "#2ca02c")
        render_metric_card(col3, "Total Orders", format_number(kpis.get("total_orders", 0)), "↓ 1.5% MoM", "#ff7f0e")
        render_metric_card(col4, "Total Suppliers", format_number(kpis.get("total_suppliers", 0)), "↓ 0.8% MoM", "#d62728")
    else:
        st.warning("Unable to fetch KPI data")
    
    st.markdown("---")
    
    # SKU TABLE
    st.markdown('<p class="section-header">📈 SKU Intelligence Explorer</p>', unsafe_allow_html=True)
    
    sku_data = fetch_sku_ranking(year=year_filter, month=month_filter, limit=200)
    
    if not sku_data.empty:
        if sku_search:
            mask = (
                (sku_data["sku_name"].str.contains(sku_search, case=False, na=False)) |
                (sku_data["unified_sku_id"].str.contains(sku_search, case=False, na=False))
            )
            sku_data = sku_data[mask]
        
        if not sku_data.empty:
            sku_display = sku_data.head(50).copy()
            sku_display["Rank"] = range(1, len(sku_display) + 1)
            sku_display["Spend"] = sku_display["total_spend"].apply(format_currency)
            sku_display["Quantity"] = sku_display["total_quantity"].apply(lambda x: f"{format_number(x)} units")
            sku_display["Orders"] = sku_display["order_count"].astype(int)
            
            display_df = sku_display[["Rank", "sku_name", "unified_sku_id", "Spend", "Quantity", "Orders"]].copy()
            display_df.columns = ["Rank", "SKU Name", "SKU ID", "Spend", "Quantity", "Orders"]
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            st.caption(f"Showing {len(sku_display)} of {len(sku_data)} SKUs")
        else:
            st.info("No SKUs match your search criteria")
    else:
        st.info("No SKU data available for selected filters")
    
    st.markdown("---")
    
    # SUPPLIER ANALYSIS
    st.markdown('<p class="section-header">🏢 Supplier Intelligence & Risk Assessment</p>', unsafe_allow_html=True)
    
    supplier_tiers = fetch_supplier_tiers()
    
    if not supplier_tiers.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Supplier Tier Distribution")
            tier_data = supplier_tiers.groupby("tier").agg({
                "total_spend": "sum",
                "supplier_name": "count"
            }).reset_index()
            tier_data.columns = ["Tier", "Total Spend", "Count"]
            
            if not tier_data.empty:
                fig = px.pie(
                    tier_data,
                    values="Total Spend",
                    names="Tier",
                    title="Spend Distribution by Tier"
                )
                st.plotly_chart(fig, use_container_width=True, key="tier_pie")
        
        with col2:
            st.subheader("High-Risk Suppliers")
            if "dependency_ratio" in supplier_tiers.columns:
                high_risk = supplier_tiers[
                    supplier_tiers["dependency_ratio"].str.contains("HIGH", case=False, na=False)
                ].head(10)
                
                if not high_risk.empty:
                    risk_df = high_risk[["supplier_name", "total_spend", "tier", "dependency_ratio"]].copy()
                    risk_df.columns = ["Supplier", "Spend", "Tier", "Risk"]
                    st.dataframe(risk_df, use_container_width=True, hide_index=True)
                else:
                    st.success("✅ No high-risk suppliers detected")
            else:
                st.info("Dependency ratio data not available")
    else:
        st.info("No supplier tier data available")
    
    st.markdown("---")
    
    # CONTRACT OPPORTUNITIES
    st.markdown('<p class="section-header">🎯 Contract Opportunity Scorecard</p>', unsafe_allow_html=True)
    
    min_score = st.slider("Minimum Contract Score", 0, 100, 60, step=5, key="min_score")
    contract_data = fetch_contract_candidates(min_score=min_score, limit=100)
    
    if not contract_data.empty:
        contract_display = contract_data.head(30).copy()
        contract_display["Rank"] = range(1, len(contract_display) + 1)
        contract_display["Recommendation"] = contract_display["contract_recommendation"].apply(
            lambda x: f"{get_recommendation_emoji(x)} {x}"
        )
        contract_display["Spend"] = contract_display["total_spend"].apply(format_currency)
        
        display_cols = ["Rank", "sku_name", "contract_priority_score", "Recommendation", "Spend", "active_months", "supplier_count"]
        display_data = contract_display[display_cols].copy()
        display_data.columns = ["Rank", "SKU", "Score", "Recommendation", "Spend", "Active M.", "Suppliers"]
        
        st.dataframe(display_data, use_container_width=True, hide_index=True)
        st.caption(f"Showing {len(contract_display)} of {len(contract_data)} opportunities")
    else:
        st.info(f"No opportunities with score ≥ {min_score}")

# ============================================================================
# PAGE: SKU DETAIL
# ============================================================================

def page_sku_detail():
    """SKU detail page"""
    st.markdown('<p class="header-title">🔍 SKU Analysis & Breakdown</p>', unsafe_allow_html=True)
    
    contract_data = fetch_contract_candidates(limit=500)
    
    if contract_data.empty:
        st.error("No contract data available")
        return
    
    sku_labels = [f"{row['sku_name']} ({row['unified_sku_id']})" for _, row in contract_data.iterrows()]
    
    if not sku_labels:
        st.error("No SKUs available")
        return
    
    selected_idx = st.selectbox("Select SKU", range(len(sku_labels)), 
                                format_func=lambda i: sku_labels[i], key="sku_select")
    selected_sku_id = contract_data.iloc[selected_idx]["unified_sku_id"]
    
    detail = fetch_contract_detail(selected_sku_id)
    
    if not detail:
        st.error("Failed to load SKU details")
        return
    
    st.markdown("---")
    
    # HEADER
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.markdown(f"### {detail.get('sku_name', 'N/A')}")
        st.caption(f"SKU ID: {detail.get('unified_sku_id', 'N/A')}")
    with col2:
        st.markdown(f"**Total Spend:** {format_currency(detail.get('total_spend', 0))}")
    with col3:
        recommendation = detail.get("contract_recommendation", "")
        emoji = get_recommendation_emoji(recommendation)
        st.markdown(f"### {emoji} {recommendation}")
    
    st.markdown("---")
    
    # SCORES
    st.markdown('<p class="section-header">📊 Score Breakdown</p>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    render_metric_card(col1, "Materiality", f"{detail.get('materiality_score', 0)}/100", "Top spend", "#d62728")
    render_metric_card(col2, "Volatility", f"{detail.get('volatility_score', 0)}/100", "Price var", "#ff7f0e")
    render_metric_card(col3, "Frequency", f"{detail.get('frequency_score', 0)}/100", "Cadence", "#1f77b4")
    render_metric_card(col4, "Fragmentation", f"{detail.get('fragmentation_score', 0)}/100", "Suppliers", "#2ca02c")
    
    # Priority Score Gauge
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=detail.get("contract_priority_score", 0),
        title={"text": "Contract Priority Score"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": get_recommendation_color(detail.get("contract_recommendation", ""))},
            "steps": [
                {"range": [0, 60], "color": "#e8f4f8"},
                {"range": [60, 80], "color": "#b3e5fc"},
                {"range": [80, 100], "color": "#01579b"}
            ]
        }
    ))
    st.plotly_chart(fig, use_container_width=True, key="priority_gauge")
    
    st.markdown("---")
    
    # FINANCIAL FACTS
    st.markdown('<p class="section-header">💰 Financial Facts</p>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    render_metric_card(col1, "Total Spend", format_currency(detail.get("total_spend", 0)))
    render_metric_card(col2, "Avg Unit Price", format_currency(detail.get("avg_unit_price", 0)))
    render_metric_card(col3, "Price Std Dev", f"${detail.get('price_stddev', 0):.2f}")
    render_metric_card(col4, "Active Months", f"{detail.get('active_months', 0)} mo")
    
    st.markdown("---")
    
    # TREND
    st.markdown('<p class="section-header">📈 Purchase Trend</p>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    with col2:
        grain = st.selectbox("Granularity", ["month", "week"], 
                            format_func=lambda x: "Monthly" if x == "month" else "Weekly",
                            key="trend_grain")
    
    trend_data = fetch_sku_trend(selected_sku_id, grain=grain)
    
    if not trend_data.empty:
        try:
            trend_data["period"] = pd.to_datetime(trend_data["period"])
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=trend_data["period"],
                y=trend_data["total_spend"],
                mode="lines+markers",
                name="Spend",
                line=dict(color="#1f77b4", width=3),
                fill="tozeroy"
            ))
            fig.update_layout(
                title="Spending Trend",
                xaxis_title="Period",
                yaxis_title="Spend ($)",
                height=400,
                hovermode="x unified"
            )
            st.plotly_chart(fig, use_container_width=True, key="trend_chart")
        except Exception as e:
            st.error(f"Error rendering trend chart: {e}")
    else:
        st.info("No trend data available")
    
    st.markdown("---")
    
    # PRICE VARIANCE
    st.markdown('<p class="section-header">💱 Supplier Price Variance</p>', unsafe_allow_html=True)
    
    price_var = fetch_sku_price_variance(selected_sku_id)
    
    if not price_var.empty:
        if all(col in price_var.columns for col in ["supplier_name", "avg_unit_price", "min_unit_price", "max_unit_price", "price_stddev"]):
            pv_display = price_var[["supplier_name", "avg_unit_price", "min_unit_price", "max_unit_price", "price_stddev"]].copy()
            pv_display.columns = ["Supplier", "Avg Price", "Min Price", "Max Price", "Std Dev"]
            st.dataframe(pv_display, use_container_width=True, hide_index=True)
        else:
            st.warning("Price variance data is incomplete")
    else:
        st.info("No price variance data available")

# ============================================================================
# PAGE: SUPPLIER ANALYSIS
# ============================================================================

def page_supplier_analysis():
    """Supplier analysis page"""
    st.markdown('<p class="header-title">🏢 Supplier Analytics</p>', unsafe_allow_html=True)
    
    suppliers_df = fetch_suppliers(limit=200)
    
    if suppliers_df.empty:
        st.error("No supplier data available")
        return
    
    selected_supplier = st.selectbox("Select Supplier", suppliers_df["supplier_name"].tolist(), key="supplier_select")
    supplier_tiers = fetch_supplier_tiers()
    
    selected_tier = None
    if not supplier_tiers.empty:
        tier_match = supplier_tiers[supplier_tiers["supplier_name"] == selected_supplier]
        if not tier_match.empty:
            selected_tier = tier_match.iloc[0]
    
    st.markdown("---")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.markdown(f"### {selected_supplier}")
    with col2:
        if selected_tier is not None and "tier" in selected_tier:
            tier_name = str(selected_tier["tier"]).replace("TIER_", "").replace("_", " ")
            st.markdown(f"**Tier:** {tier_name}")
    with col3:
        if selected_tier is not None and "dependency_ratio" in selected_tier:
            dep_ratio = str(selected_tier["dependency_ratio"])
            risk_emoji = "🔴" if "HIGH" in dep_ratio else "🟡" if "MEDIUM" in dep_ratio else "🟢"
            st.markdown(f"**Risk:** {risk_emoji}")
    
    st.markdown("---")
    
    if selected_tier is not None:
        col1, col2, col3, col4 = st.columns(4)
        render_metric_card(col1, "Total Spend", format_currency(selected_tier.get("total_spend", 0)))
        render_metric_card(col2, "Order Count", format_number(selected_tier.get("order_count", 0)))
        render_metric_card(col3, "SKU Count", format_number(selected_tier.get("sku_count", 0)))
        render_metric_card(col4, "Risk Level", str(selected_tier.get("dependency_ratio", "N/A")))
    else:
        st.info("Supplier details not available")

# ============================================================================
# MAIN APP
# ============================================================================

def main():
    """Main application entry point"""
    with st.sidebar:
        st.markdown("## 🧭 Navigation")
        page = st.radio("Select Page", ["📊 Dashboard", "🔍 SKU Detail", "🏢 Supplier Analysis"])
        
        st.markdown("---")
        st.markdown("### ⚙️ Settings")
        st.toggle("Show advanced filters", value=False, key="adv_filters")
        st.toggle("Enable auto-refresh", value=False, key="auto_refresh")
        
        st.markdown("---")
        st.markdown("""
        ### 📚 About
        **SKU Procurement Intelligence Platform v1.0**
        
        - FastAPI backend
        - PostgreSQL data warehouse
        - Streamlit frontend
        
        **Data Updates:** Daily
        """)
    
    if page == "📊 Dashboard":
        page_dashboard()
    elif page == "🔍 SKU Detail":
        page_sku_detail()
    elif page == "🏢 Supplier Analysis":
        page_supplier_analysis()

if __name__ == "__main__":
    main()