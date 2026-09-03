from __future__ import annotations

import hashlib
import os
import re
import smtplib
import ssl
from email.headerregistry import Address
from email.message import EmailMessage


TERMS_VERSION = "2026-09-03"
PRIVACY_VERSION = "2026-09-03"

_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def normalize_email(value: str | None) -> str:
    return str(value or "").strip().casefold()


def valid_email(value: str | None) -> bool:
    email = normalize_email(value)
    return 3 <= len(email) <= 254 and bool(_EMAIL_RE.fullmatch(email))


def hash_account_token(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


def smtp_enabled() -> bool:
    return bool(
        str(os.getenv("NAI_IM_SMTP_HOST") or "").strip()
        and str(os.getenv("NAI_IM_SMTP_USERNAME") or "").strip()
        and str(os.getenv("NAI_IM_SMTP_PASSWORD") or "").strip()
    )


def google_client_id() -> str:
    return str(os.getenv("NAI_IM_GOOGLE_CLIENT_ID") or "").strip()


def send_account_email(*, to_email: str, subject: str, body: str) -> None:
    host = str(os.getenv("NAI_IM_SMTP_HOST") or "").strip()
    username = str(os.getenv("NAI_IM_SMTP_USERNAME") or "").strip()
    password = str(os.getenv("NAI_IM_SMTP_PASSWORD") or "").strip()
    from_email = str(os.getenv("NAI_IM_SMTP_FROM") or username).strip()
    from_name = str(os.getenv("NAI_IM_SMTP_FROM_NAME") or "NImageManager運営").strip()
    if not host or not username or not password or not from_email:
        raise RuntimeError("SMTP is not configured")

    try:
        port = int(str(os.getenv("NAI_IM_SMTP_PORT") or "587").strip())
    except Exception:
        port = 587
    use_starttls = str(os.getenv("NAI_IM_SMTP_STARTTLS") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    message = EmailMessage()
    message["Subject"] = str(subject)
    message["From"] = Address(display_name=from_name, addr_spec=from_email)
    message["To"] = str(to_email)
    message.set_content(str(body))

    with smtplib.SMTP(host=host, port=port, timeout=20) as smtp:
        smtp.ehlo()
        if use_starttls:
            smtp.starttls(context=ssl.create_default_context())
            smtp.ehlo()
        smtp.login(username, password)
        smtp.send_message(message)


def verify_google_credential(credential: str) -> dict:
    client_id = google_client_id()
    if not client_id:
        raise ValueError("Google authentication is disabled")
    raw = str(credential or "").strip()
    if not raw:
        raise ValueError("Google credential is required")

    try:
        from google.auth.transport import requests as google_requests
        from google.oauth2 import id_token

        payload = id_token.verify_oauth2_token(raw, google_requests.Request(), client_id)
    except Exception as exc:
        raise ValueError("Google credential verification failed") from exc

    if str(payload.get("email_verified") or "").lower() not in {"true", "1"}:
        raise ValueError("Google email is not verified")

    subject = str(payload.get("sub") or "").strip()
    email = normalize_email(payload.get("email"))
    if not subject or not valid_email(email):
        raise ValueError("Google account data is incomplete")
    return {
        "sub": subject,
        "email": email,
        "name": str(payload.get("name") or "").strip(),
        "nonce": str(payload.get("nonce") or "").strip(),
    }
