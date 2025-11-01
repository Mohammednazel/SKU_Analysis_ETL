# src/monitoring/alerting.py
import os
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def _get_bool(env, default=False):
    return str(os.getenv(env, str(default))).lower() in ("1", "true", "yes")

def send_email(subject: str, text_body: str, html_body: str | None = None) -> bool:
    if not _get_bool("ENABLE_EMAIL_ALERTS", False):
        return False

    recipients = [e.strip() for e in os.getenv("ALERT_EMAILS", "").split(",") if e.strip()]
    if not recipients:
        return False

    smtp_host   = os.getenv("SMTP_HOST", "")
    smtp_port   = int(os.getenv("SMTP_PORT", "587"))
    smtp_user   = os.getenv("SMTP_USER", "")
    smtp_pass   = os.getenv("SMTP_PASSWORD", "")
    starttls    = _get_bool("SMTP_STARTTLS", True)
    from_email  = os.getenv("ALERT_FROM", smtp_user or "no-reply@example.com")

    if not smtp_host or not smtp_user or not smtp_pass:
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_email
    msg["To"]      = ", ".join(recipients)

    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    if html_body:
        msg.attach(MIMEText(html_body, "html", "utf-8"))

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            if starttls:
                server.starttls(context=context)
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, recipients, msg.as_string())
        return True
    except Exception:
        # Don't raise; ETL should not crash because alerting failed.
        return False
