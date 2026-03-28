import os

CABINET_PREFIX = os.getenv("CABINET_PREFIX", "/cabinet")
SESSION_TTL_HOURS = int(os.getenv("CABINET_SESSION_TTL_HOURS", "720"))
RESET_TTL_MINUTES = int(os.getenv("CABINET_RESET_TTL_MINUTES", "30"))
REFERRAL_RATE = float(os.getenv("CABINET_REFERRAL_RATE", "0.10"))
EXPOSE_DEBUG_RESET_TOKEN = os.getenv("CABINET_DEBUG_RESET_TOKEN", "false").lower() == "true"
AUTH_REGISTER_LIMIT_PER_MINUTE = int(os.getenv("CABINET_AUTH_REGISTER_LIMIT_PER_MINUTE", "5"))
AUTH_LOGIN_LIMIT_PER_MINUTE = int(os.getenv("CABINET_AUTH_LOGIN_LIMIT_PER_MINUTE", "20"))

SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
SMTP_FROM = os.getenv("SMTP_FROM", "").strip()
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "false").lower() == "true"
RESET_LINK_BASE_URL = os.getenv("RESET_LINK_BASE_URL", "https://codenext.ru/reset-password").strip()

ENVIRONMENT = os.getenv("ENVIRONMENT", "development").lower().strip()
if EXPOSE_DEBUG_RESET_TOKEN and ENVIRONMENT == "production":
    raise RuntimeError("CABINET_DEBUG_RESET_TOKEN must be false in production")
