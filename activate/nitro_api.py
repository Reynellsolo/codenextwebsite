import json
import httpx
import re
import time

API = "https://receipt-api.nitro.xin"

H_BASE = {
    "Origin": "https://receipt.nitro.xin",
    "Referer": "https://receipt.nitro.xin/",
    "User-Agent": "Mozilla/5.0",
}
H_TEXT = {**H_BASE, "Content-Type": "text/plain;charset=UTF-8", "X-Product-Id": "chatgpt"}
H_START = {**H_TEXT, "X-Device-Id": "web"}

# Regex для JWT токена
JWT_PATTERN = re.compile(r'^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$')


def mask_sensitive(value, show_start=8, show_end=4):
    """
    Маскирует чувствительные данные для логов
    Пример: "EBB0034E-4E89-4E14-86A4-C8F6CBE046C0" -> "EBB0034E...6C0"
    """
    if not isinstance(value, str) or len(value) < (show_start + show_end):
        return "***"
    return f"{value[:show_start]}...{value[-show_end:]}"


def validate_authsession_structure(authsession):
    """
    Быстрая проверка структуры (офлайн, без запросов к API)
    """
    if not authsession or not isinstance(authsession, dict):
        return False
    
    access_token = authsession.get("accessToken")
    session_token = authsession.get("sessionToken")
    
    if not access_token or not isinstance(access_token, str):
        return False
    
    if not session_token or not isinstance(session_token, str):
        return False
    
    # Проверка длины ДО regex (защита от ReDoS)
    if len(access_token) > 10000:
        return False
    
    if not JWT_PATTERN.match(access_token):
        return False
    
    if len(session_token) < 50:
        return False
    
    return True


def check_user_with_nitro(authsession, cdk):
    """
    Проверка auth данных через API Nitro
    """
    try:
        user_json_str = json.dumps(authsession, ensure_ascii=False, separators=(",", ":"))
        
        # Защита от слишком большого payload
        if len(user_json_str) > 50000:  # 50 KB макс
            return {"valid": False, "error": "Authorization data too large"}
        
        r = httpx.post(
            f"{API}/external/public/check-user",
            headers=H_TEXT,
            json={
                "user": user_json_str,
                "cdk": cdk
            },
            timeout=15
        )
        
        if r.status_code == 200:
            return {"valid": True, "data": r.text}
        else:
            return {"valid": False, "error": r.text}
            
    except Exception as e:
        return {"valid": False, "error": str(e)}


def check_cdk(cdk):
    """Проверяем что CDK существует и валидный"""
    try:
        r = httpx.post(f"{API}/cdks/public/check", headers=H_TEXT, json={"code": cdk}, timeout=15)
        try:
            res = r.json()
        except Exception:
            res = r.text
        if isinstance(res, str):
            if "not found" in res.lower() or "invalid" in res.lower():
                return {"valid": False, "error": res}
            return {"valid": True, "raw": res}
        if isinstance(res, dict) and res.get("valid") is False:
            return {"valid": False, "error": res.get("error", "invalid cdk")}
        return {"valid": True, "data": res}
    except Exception as e:
        return {"valid": False, "error": str(e)}


def start_outstock(authsession, cdk):
    """Отправляем запрос на активацию"""
    r = httpx.post(
        f"{API}/stocks/public/outstock",
        headers=H_START,
        json={
            "cdk": cdk,
            "user": json.dumps(authsession, ensure_ascii=False, separators=(",", ":")),
        },
        timeout=30,
    )
    try:
        data = r.json()
    except Exception:
        data = r.text

    task_id = None
    if isinstance(data, dict):
        task_id = data.get("task_id") or data.get("id") or data.get("uuid")
    elif isinstance(data, str):
        cleaned = data.strip().replace('"', '')
        if len(cleaned) > 10 and "error" not in cleaned.lower():
            task_id = cleaned

    return {"task_id": task_id, "raw": data}


def check_task_status(task_id, max_attempts=10, delay=6):
    """
    Проверяет статус задачи активации
    
    Формат ответа Nitro:
    {
      "task_id": "...",
      "pending": false,
      "success": true/false,
      "message": "..."
    }
    """
    # Защита от злоупотребления
    max_attempts = min(max_attempts, 20)  # Макс 20 попыток
    delay = min(max(delay, 1), 30)  # От 1 до 30 секунд
    
    for attempt in range(max_attempts):
        try:
            # Не ждём на первой попытке
            if attempt > 0:
                print(f"[TASK CHECK] Attempt {attempt + 1}/{max_attempts}, waiting {delay}s...")
                time.sleep(delay)
            else:
                print(f"[TASK CHECK] Attempt {attempt + 1}/{max_attempts} (immediate check)...")
            
            r = httpx.get(
                f"{API}/stocks/public/outstock/{task_id}",
                headers=H_BASE,
                timeout=10
            )
            
            print(f"[TASK CHECK] Status: {r.status_code}, Response: {r.text[:200]}")
            
            try:
                data = r.json()
            except:
                continue
            
            pending = data.get("pending", True)
            
            if not pending:
                success = data.get("success", False)
                message = data.get("message", "Unknown status")
                
                return {
                    "success": success,
                    "message": message,
                    "error": None if success else message,
                    "data": data
                }
            
        except Exception as e:
            print(f"[TASK CHECK] Exception: {e}")
            continue
    
    # Если исчерпали все попытки, возвращаем с флагом pending
    return {
        "success": False, 
        "message": "Activation timeout",
        "error": "Task still pending after multiple attempts",
        "pending": True  # Флаг что задача всё ещё в процессе
    }


def run_flow(cdk, authsession):
    """
    Полная логика активации с проверками:
      1) Проверка структуры (офлайн)
      2) Проверка CDK (API)
      3) Проверка auth (API)
      4) Запуск активации
      5) Проверка результата (polling)
    """
    # Маскируем CDK в логах
    cdk_masked = mask_sensitive(cdk, show_start=8, show_end=4)
    print(f"\n[ACTIVATION] CDK: {cdk_masked}")

    # 1. Проверка структуры
    if not validate_authsession_structure(authsession):
        print(f"[ACTIVATION] FAIL — Invalid structure")
        return {
            "success": False, 
            "error": "Invalid authorization data structure. Required: accessToken (JWT), sessionToken"
        }

    # 2. Проверка CDK
    cdk_check = check_cdk(cdk)
    print(f"[ACTIVATION] CDK check: valid={cdk_check.get('valid')}")

    if not cdk_check.get("valid"):
        err = cdk_check.get("error", "CDK invalid")
        print(f"[ACTIVATION] FAIL — CDK: {err}")
        return {"success": False, "error": f"CDK error: {err}"}

    # 3. Проверка auth
    print(f"[ACTIVATION] Checking auth...")
    user_check = check_user_with_nitro(authsession, cdk)
    print(f"[ACTIVATION] Auth check: valid={user_check.get('valid')}")

    if not user_check.get("valid"):
        err = user_check.get("error", "Invalid auth")
        print(f"[ACTIVATION] FAIL — Auth: {err}")
        return {"success": False, "error": f"Authorization failed: {err}"}

    # 4. Запуск активации
    try:
        start = start_outstock(authsession, cdk)
        task_id = start.get("task_id")
        if task_id:
            print(f"[ACTIVATION] Outstock: task_id={mask_sensitive(task_id, 8, 4)}")
        else:
            print(f"[ACTIVATION] Outstock: no task_id")
    except Exception as e:
        print(f"[ACTIVATION] Exception: {e}")
        return {"success": False, "error": f"Network error: {e}"}

    if not task_id:
        raw = start.get("raw", "")
        print(f"[ACTIVATION] FAIL — No task_id: {raw[:100]}")
        return {"success": False, "error": f"Nitro rejected: {raw}"}

    print(f"[ACTIVATION] Task started")

    # 5. Проверка результата (60 секунд максимум)
    print(f"[ACTIVATION] Checking result...")
    status_check = check_task_status(task_id, max_attempts=15, delay=4)
    print(f"[ACTIVATION] Result: success={status_check.get('success')}, message={status_check.get('message')}")

    if status_check.get("success"):
        print(f"[ACTIVATION] SUCCESS!")
        return {"success": True, "error": None, "task_id": task_id, "details": status_check}
    else:
        err = status_check.get("message", "Failed")
        is_pending = status_check.get("pending", False)
        print(f"[ACTIVATION] FAIL — {err}")
        return {
            "success": False, 
            "error": err, 
            "task_id": task_id,
            "pending": is_pending  # Передаём флаг pending дальше в app.py
        }