"""
test_alerting.py
----------------------------------
Utility to simulate ETL alert scenarios
without touching live ETL pipeline or DB data.

Usage:
  python src/monitoring/test_alerting.py
"""

import os
import sys
from datetime import datetime, timedelta, timezone

# Ensure correct path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from monitoring.alerting import send_email
from monitoring.monitors import evaluate_run, fetch_recent_success_stats

# Load environment
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def banner(title: str):
    print("\n" + "=" * 80)
    print(f"🧪 {title}")
    print("=" * 80)


def simulate_normal_run():
    """Simulate a perfectly fine ETL run (no alert)."""
    banner("TEST 1: Normal Run (no alert expected)")
    ok, issues = evaluate_run(engine, "daily", rows_processed=500, runtime_seconds=20)
    if ok and not issues:
        print("✅ PASS: Normal run evaluated healthy.")
    else:
        print("⚠️ Unexpected alert conditions:", issues)
    send_email(
        subject="[ETL Test] Normal Run",
        text_body="This is a test email for a normal ETL run.\nNo alert should be triggered.",
        html_body=None,
    )


def simulate_failure_run():
    """Simulate a failure alert."""
    banner("TEST 2: Simulated Failure Run (alert expected)")
    status = "failed"
    error_msg = "Simulated connection timeout"
    rows_processed = 0
    duration = 15
    subject = f"[ETL] FAILED - mode=daily, rows={rows_processed}, dur={duration:.1f}s"
    body = f"""
Job: purchase_order_ingest
Mode: daily
Status: {status}
Rows processed: {rows_processed}
Duration: {duration:.1f} sec
Error: {error_msg}
"""
    print("🚨 Sending failure alert email...")
    send_email(subject, body, body.replace("\n", "<br>"))
    print("✅ Simulated failure alert sent.")


def simulate_slow_run():
    """Simulate SLA breach (runtime too long)."""
    banner("TEST 3: Slow Run (alert expected)")
    ok, issues = evaluate_run(engine, "daily", rows_processed=500, runtime_seconds=2000)
    if not ok:
        print("🚨 SLA alert expected:", issues)
    send_email(
        subject="[ETL] SLA Breach - Slow Run Detected",
        text_body="\n".join(issues),
        html_body="<br>".join(issues),
    )
    print("✅ Slow run alert simulated.")


def simulate_zero_rows_daily():
    """Simulate abnormal 0 rows despite previous success baseline."""
    banner("TEST 4: Zero Rows Daily (alert expected)")
    ok, issues = evaluate_run(engine, "daily", rows_processed=0, runtime_seconds=10)
    if not ok:
        print("🚨 Zero rows alert expected:", issues)
    else:
        print("⚠️ No alert detected — may need more history in etl_run_log.")
    send_email(
        subject="[ETL] Zero Rows - Daily ETL anomaly",
        text_body="\n".join(issues) if issues else "No issues found (baseline empty).",
        html_body="<br>".join(issues) if issues else None,
    )


def simulate_watchdog_alert():
    """Simulate a missing run (watchdog trigger)."""
    banner("TEST 5: Watchdog Trigger (alert expected)")
    subj = "[ETL Watchdog] No successful runs in last 6h (simulated)"
    body = f"""
Simulated watchdog alert.

Last successful run: {(datetime.now(timezone.utc) - timedelta(hours=7)).isoformat()}
Now: {datetime.now(timezone.utc).isoformat()}
"""
    print("🚨 Sending watchdog alert email...")
    send_email(subj, body, body.replace("\n", "<br>"))
    print("✅ Watchdog alert simulated.")


def main():
    print("\n=== ETL Alerting Test Suite ===")
    print("SMTP_USER:", os.getenv("SMTP_USER"))
    print("ALERT_EMAILS:", os.getenv("ALERT_EMAILS"))
    print("ENABLE_EMAIL_ALERTS:", os.getenv("ENABLE_EMAIL_ALERTS"))
    print("Database URL loaded:", bool(DATABASE_URL))
    print("--------------------------------------------------")

    simulate_normal_run()
    simulate_failure_run()
    simulate_slow_run()
    simulate_zero_rows_daily()
    simulate_watchdog_alert()

    print("\n🎉 All alert scenarios executed. Check your inbox for test emails.\n")


if __name__ == "__main__":
    main()
