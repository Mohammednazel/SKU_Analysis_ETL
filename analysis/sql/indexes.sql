-- 1. Time Filtering (Critical for charts)
CREATE INDEX IF NOT EXISTS idx_mv_sku_month ON app_analytics.mv_sku_monthly_metrics (order_month);
CREATE INDEX IF NOT EXISTS idx_mv_supplier_month ON app_analytics.mv_supplier_monthly_metrics (order_month);

-- 2. Dashboard Quick Filters
CREATE INDEX IF NOT EXISTS idx_mv_contract_flag ON app_analytics.mv_contract_candidates (contract_recommendation);

CREATE INDEX IF NOT EXISTS idx_supplier_tier ON app_analytics.mv_supplier_tiering (supplier_tier);
CREATE INDEX IF NOT EXISTS idx_supplier_risk ON app_analytics.mv_supplier_tiering (dependency_risk_level);

-- 3. Weekly Metrics Indexes (NEW)
CREATE INDEX IF NOT EXISTS idx_mv_sku_weekly_metrics_sku 
ON app_analytics.mv_sku_weekly_metrics (unified_sku_id);

CREATE INDEX IF NOT EXISTS idx_mv_sku_weekly_metrics_week 
ON app_analytics.mv_sku_weekly_metrics (order_week);

CREATE INDEX IF NOT EXISTS idx_mv_sku_weekly_metrics_year 
ON app_analytics.mv_sku_weekly_metrics (order_year);