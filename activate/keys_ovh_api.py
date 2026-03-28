import os
import httpx
import re
import logging
import json

logger = logging.getLogger(__name__)

API_BASE = "https://keys.ovh/api/v1"
API_TOKEN = os.getenv("KEYS_OVH_API_TOKEN", "").strip()

JWT_PATTERN = re.compile(r'^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$')


def mask_sensitive(value, show_start=8, show_end=4):
    if not isinstance(value, str) or len(value) < (show_start + show_end):
        return "***"
    return f"{value[:show_start]}...{value[-show_end:]}"


def _headers():
    return {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; CodeNext/1.0)",
    }


def validate_authsession_structure(authsession):
    if not authsession or not isinstance(authsession, dict):
        return False
    access_token = authsession.get("accessToken")
    if not access_token or not isinstance(access_token, str):
        return False
    if len(access_token) > 10000:
        return False
    if not JWT_PATTERN.match(access_token):
        return False
    return True


def check_cdk(cdk):
    """
    GET /key/{code}/status
    Совместимо с nitro_api.check_cdk()
    """
    try:
        r = httpx.get(
            f"{API_BASE}/key/{cdk}/status",
            headers=_headers(),
            timeout=15,
        )

        if r.status_code == 404:
            return {"valid": False, "error": "CDK not found"}

        if r.status_code in (401, 403):
            return {"valid": False, "error": f"API auth error: {r.status_code}"}

        if r.status_code != 200:
            return {"valid": False, "error": f"HTTP {r.status_code}"}

        data = r.json()

        if not data.get("success"):
            return {"valid": False, "error": data.get("message", "Unknown error")}

        status = data.get("data", {}).get("status", "")

        if status == "available":
            return {"valid": True, "data": data.get("data")}
        elif status == "used":
            return {"valid": False, "error": "CDK already used"}
        elif status == "expired":
            return {"valid": False, "error": "CDK expired"}
        else:
            return {"valid": False, "error": f"CDK status: {status}"}

    except Exception as e:
        logger.error(f"check_cdk error: {e}")
        return {"valid": False, "error": str(e)}


def run_flow(cdk, authsession):
    """
    Активация через keys.ovh API.
    Полная совместимость с nitro_api.run_flow(cdk, authsession)
    """
    cdk_masked = mask_sensitive(cdk, show_start=8, show_end=4)
    logger.info(f"[OVH] CDK: {cdk_masked}")

    # 1. Проверка структуры (офлайн)
    if not validate_authsession_structure(authsession):
        logger.error("[OVH] FAIL — invalid structure")
        return {
            "success": False,
            "error": "Invalid authorization data",
        }

    # 2. Активация (API сам проверяет ключ + юзера)
    user_token = json.dumps(authsession, ensure_ascii=False, separators=(',', ':'))

    try:
        r = httpx.post(
            f"{API_BASE}/activate",
            headers=_headers(),
            json={"key": cdk, "user_token": user_token},
            timeout=60,
        )
    except httpx.TimeoutException:
        logger.error(f"[OVH] TIMEOUT for {cdk_masked}")
        return {"success": False, "error": "Network timeout", "pending": True}
    except Exception as e:
        logger.error(f"[OVH] Network error: {e}")
        return {"success": False, "error": f"Network error: {e}", "pending": True}

    # Наша ошибка конфигурации — не наказываем пользователя
    if r.status_code in (401, 403):
        logger.error(f"[OVH] API auth error: {r.status_code}")
        return {"success": False, "error": "Network timeout", "pending": True}

    # Rate limit — тоже не вина пользователя
    if r.status_code == 429:
        logger.warning("[OVH] Rate limited")
        return {"success": False, "error": "Network timeout", "pending": True}

    # Server error
    if r.status_code >= 500:
        logger.error(f"[OVH] Server error: {r.status_code}")
        return {"success": False, "error": "Network timeout", "pending": True}

    # Парсим ответ
    try:
        body = r.json()
    except Exception:
        logger.error(f"[OVH] Invalid JSON: {r.text[:200]}")
        return {"success": False, "error": "Network timeout", "pending": True}

       # УСПЕХ
    if body.get("success"):
        logger.info(f"[OVH] SUCCESS for {cdk_masked}")
        return {"success": True, "error": None}

    # ОШИБКА — маппинг на формат совместимый с app.py
    error_code = body.get("error", "")
    message = body.get("message", "Activation failed")
    msg_lower = message.lower()
    error_lower = error_code.lower()

    # === НАШИ ОШИБКИ (не вина клиента) — pending=True ===
    our_errors = [
        "missing_token", "invalid_token", "token_inactive", "token_expired",
        "ip_not_allowed", "rate_limit_exceeded", "insufficient_balance",
        "purchase_failed"
    ]
    if error_code in our_errors:
        logger.error(f"[OVH] Config error — {error_code}: {message}")
        return {"success": False, "error": "Network timeout", "pending": True}

    # === CDK ПРОБЛЕМЫ ===
    if error_code in ["key_not_found", "out_of_stock", "product_not_found", 
                       "subscription_not_found", "order_not_found"]:
        return {"success": False, "error": "CDK not found"}

    # === CDK уже использован ===
    if "used" in msg_lower or "used" in error_lower:
        return {"success": False, "error": "CDK already used"}

    # === ОШИБКА ДАННЫХ КЛИЕНТА ===
    if error_code == "activation_failed":
        if any(w in msg_lower for w in ["token", "user", "auth", "invalid", "expired", "validation"]):
            return {"success": False, "error": f"Authorization failed: {message}"}
        return {"success": False, "error": message}

    # === token_invalid и подобные ===
    if "token" in error_lower or "validation" in msg_lower or "invalid" in msg_lower:
        return {"success": False, "error": f"Authorization failed: {message}"}

    # === Неизвестная ошибка ===
    logger.error(f"[OVH] FAIL — {error_code}: {message}")
    return {"success": False, "error": message}