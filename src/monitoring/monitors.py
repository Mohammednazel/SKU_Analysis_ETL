# Runtime heuristics, success tracking

# src/monitoring/monitors.py
import os
from statistics import mean
from typing import Tuple, List
from sqlalchemy import text
from sqlalchemy.engine import Engine

def _getint(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return default

def fetch_recent_success_stats(engine: Engine, mode: str, window: int) -> list[dict]:
    """
    Returns last N successful runs in this mode.
    """
    q = text("""
        SELECT rows_processed,
               EXTRACT(EPOCH FROM (end_time - start_time)) AS duration_sec
        FROM etl_run_log
        WHERE status='success' AND mode = :mode
        ORDER BY run_id DESC
        LIMIT :lim
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"mode": mode, "lim": window}).mappings().all()
        return [dict(r) for r in rows]

def evaluate_run(
    engine: Engine,
    mode: str,
    rows_processed: int,
    runtime_seconds: float,
) -> Tuple[bool, List[str]]:
    """
    Heuristic evaluation:
    - Fail if runtime > MAX_RUNTIME_SECONDS
    - Alert if daily rows drop to 0 while baseline average > MIN_DAILY_EXPECTED_ROWS
    - Alert if historical rows are suspiciously small (< 25% of baseline average if available)
    """
    issues: List[str] = []
    ok = True

    max_runtime = _getint("MAX_RUNTIME_SECONDS", 900)
    baseline_window = _getint("BASELINE_WINDOW", 10)
    min_daily_expected = _getint("MIN_DAILY_EXPECTED_ROWS", 1)

    # 1) Runtime SLA
    if runtime_seconds > max_runtime:
        ok = False
        issues.append(f"Runtime {runtime_seconds:.1f}s exceeded threshold {max_runtime}s.")

    # 2) Pull baseline
    baseline = fetch_recent_success_stats(engine, mode, baseline_window)
    if baseline:
        avg_rows = mean([b["rows_processed"] for b in baseline if b["rows_processed"] is not None] or [0])
    else:
        avg_rows = 0

    # 3) Daily-specific: zero rows but baseline > min expected
    if mode == "daily" and rows_processed == 0 and avg_rows >= min_daily_expected:
        ok = False
        issues.append(f"Daily rows dropped to 0; recent average ≈ {avg_rows:.0f}.")

    # 4) Historical-specific: suspiciously small
    if mode == "historical" and avg_rows > 0 and rows_processed < 0.25 * avg_rows:
        ok = False
        issues.append(f"Historical rows {rows_processed} << recent avg {avg_rows:.0f} (possible partial load).")

    return ok, issues