from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from pydantic import BaseModel, Field, validator
import os, json, secrets, asyncio, uuid
from databases import Database
import httpx
import re
import time
import logging
import base64
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv
from pathlib import Path
from cabinet_system.router import router as cabinet_router, set_database as set_cabinet_database
from cabinet_system.schema import cleanup_expired_cabinet_data, init_cabinet_schema
from cabinet_system.services import get_user_by_session, process_referral_bonus

load_dotenv(Path(__file__).parent / "bot.env")

# ===== ПЕРЕКЛЮЧАТЕЛЬ АКТИВАТОРА =====
USE_OVH_ACTIVATOR = os.getenv("USE_OVH_ACTIVATOR", "false").strip().lower() == "true"

if USE_OVH_ACTIVATOR:
    from keys_ovh_api import run_flow as _activator_run_flow, check_cdk as _activator_check_cdk
    logging.info("Activator: keys_ovh_api (OVH)")
else:
    from nitro_api import run_flow as _activator_run_flow, check_cdk as _activator_check_cdk
    logging.info("Activator: nitro_api")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ================== НАСТРОЙКИ ==================
PLATEGA_MERCHANT_ID = os.getenv("PLATEGA_MERCHANT_ID", "").strip()
PLATEGA_SECRET = os.getenv("PLATEGA_SECRET", "").strip()

ANTILOPAY_SECRET_ID = os.getenv("ANTILOPAY_SECRET_ID", "").strip()
ANTILOPAY_SECRET_KEY = os.getenv("ANTILOPAY_SECRET_KEY", "").strip()
ANTILOPAY_PROJECT_ID = os.getenv("ANTILOPAY_PROJECT_ID", "").strip()
ANTILOPAY_PUBLIC_KEY = os.getenv("ANTILOPAY_PUBLIC_KEY", "").strip()

DOMAIN = os.getenv("DOMAIN", os.getenv("PUBLIC_BASE_URL", "https://codenext.ru")).strip()
CORS_ORIGINS_ENV = os.getenv("CORS_ORIGINS", "")
if CORS_ORIGINS_ENV.strip():
    CORS_ORIGINS = [x.strip() for x in CORS_ORIGINS_ENV.split(",") if x.strip()]
else:
    CORS_ORIGINS = [DOMAIN, "http://localhost:3000", "http://127.0.0.1:3000"]
DATABASE_URL = os.getenv("DATABASE_URL")
SUPPORT_LINK = "https://t.me/CodeNext_support"
MAX_ATTEMPTS = 5

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не указан в .env файле!")

# ===== БЕЗОПАСНОСТЬ =====
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
REF_CODE_RE = re.compile(r"^[A-Z0-9_-]{4,40}$")
_FALLBACK_LAST_CHECK = {}
_FALLBACK_MIN_INTERVAL = 10

BASE_DIR = "/opt/activate"
LOCAL_BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = os.path.join(BASE_DIR, "static")
if not os.path.isdir(STATIC_DIR):
    STATIC_DIR = str(LOCAL_BASE_DIR / "static")

app = FastAPI()
_cleanup_task = None

app.include_router(cabinet_router)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Requested-With", "X-CSRF-Token"],
)

database = Database(DATABASE_URL)
set_cabinet_database(database)

# ================== ANTILOPAY HELPERS ==================
def sign_antilopay_request(body: dict) -> str:
    try:
        json_str = json.dumps(body, separators=(',', ':'), ensure_ascii=False)
        private_key = serialization.load_pem_private_key(
            ANTILOPAY_SECRET_KEY.encode(), password=None, backend=default_backend()
        )
        signature = private_key.sign(json_str.encode('utf-8'), padding.PKCS1v15(), hashes.SHA256())
        return base64.b64encode(signature).decode('utf-8')
    except Exception as e:
        logging.error(f"Antilopay sign error: {e}")
        raise

def verify_antilopay_callback(body: bytes, signature: str) -> bool:
    try:
        if not ANTILOPAY_PUBLIC_KEY or not signature:
            return False
        public_key = serialization.load_pem_public_key(ANTILOPAY_PUBLIC_KEY.encode(), backend=default_backend())
        public_key.verify(base64.b64decode(signature), body, padding.PKCS1v15(), hashes.SHA256())
        return True
    except Exception as e:
        logging.error(f"Antilopay callback verification failed: {e}")
        return False

# ================== ANTILOPAY STATUS CHECK ==================
async def check_antilopay_payment_status(payment_id: str):
    if not payment_id or not ANTILOPAY_SECRET_ID:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://lk.antilopay.com/api/v1/payment/status",
                params={"payment_id": payment_id},
                headers={
                    "X-Apay-Secret-Id": ANTILOPAY_SECRET_ID,
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "application/json",
                },
                follow_redirects=True,
            )
        if resp.status_code != 200:
            logging.error(f"Antilopay status API HTTP {resp.status_code} for {payment_id}")
            return None
        body_text = resp.text.strip()
        if not body_text or body_text.startswith("<"):
            logging.error(f"Antilopay status API returned HTML/Empty for {payment_id}")
            return None
        return resp.json()
    except Exception as e:
        logging.error(f"Antilopay status API error for {payment_id}: {e}")
        return None

# ================== Pydantic ==================
class CreatePaymentRequest(BaseModel):
    product: str = Field(..., max_length=50)
    method: str = Field(default="card", max_length=20)

    @validator('product')
    def validate_product(cls, v):
        allowed = ["plus_1m", "go_12m", "plus_12m", "plus_account_new", "business_1m"]
        if v not in allowed:
            raise ValueError('Invalid product')
        return v

    @validator('method')
    def validate_method(cls, v):
        allowed = ["sbp", "card", "crypto"]
        v = v.lower().strip()
        if v not in allowed:
            return "card"
        return v

# ================== STARTUP / SHUTDOWN ==================
async def verify_critical_indexes(db: Database):
    critical = [("uq_wallet_referral_bonus_order", "UNIQUE INDEX для защиты от двойных реферальных бонусов")]
    for idx_name, description in critical:
        try:
            row = await db.fetch_one("SELECT 1 FROM pg_indexes WHERE indexname=:name", values={"name": idx_name})
        except Exception as e:
            logging.critical(
                f"CRITICAL INDEX CHECK FAILED for {idx_name}: {e}. "
                "Unable to verify referral dedup protection."
            )
            continue
        if not row:
            logging.critical(
                f"CRITICAL INDEX MISSING: {idx_name} ({description}). "
                "Referral dedup protection is DISABLED!"
            )

async def periodic_cleanup():
    while True:
        await asyncio.sleep(3600)
        try:
            await cleanup_expired_cabinet_data(database)
            now = int(time.time())
            expired = [k for k, v in _FALLBACK_LAST_CHECK.items() if now - v > 3600]
            for k in expired:
                del _FALLBACK_LAST_CHECK[k]
        except Exception as e:
            logging.error(f"Periodic cleanup failed: {e}")

@app.on_event("startup")
async def startup():
    global _cleanup_task
    await database.connect()
    await init_db()
    try:
        await init_cabinet_schema(database)
        await cleanup_expired_cabinet_data(database)
    except Exception as e:
        logging.error(f"Cabinet schema init failed: {e}")
    await verify_critical_indexes(database)
    _cleanup_task = asyncio.create_task(periodic_cleanup())

@app.on_event("shutdown")
async def shutdown():
    global _cleanup_task
    if _cleanup_task is not None:
        _cleanup_task.cancel()
        _cleanup_task = None
    await database.disconnect()

async def init_db():
    async with database.transaction():
        await database.execute("""
            CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY, product TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'free', reserved_at TIMESTAMP,
                used_at TIMESTAMP, last_error TEXT)""")
        await database.execute("""
            CREATE TABLE IF NOT EXISTS activation_links (
                token TEXT PRIMARY KEY, product TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active', attempts INTEGER NOT NULL DEFAULT 0,
                cdk_code TEXT, last_error TEXT, created_at TIMESTAMP NOT NULL DEFAULT NOW())""")
        await database.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY, product TEXT NOT NULL,
                amount INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'created',
                created_at TIMESTAMP NOT NULL DEFAULT NOW(), paid_at TIMESTAMP,
                activation_token TEXT, transaction_id TEXT,
                payment_provider TEXT DEFAULT 'platega')""")
        await database.execute("""
            CREATE TABLE IF NOT EXISTS platega_webhooks (
                transaction_id TEXT PRIMARY KEY, received_at TIMESTAMP NOT NULL DEFAULT NOW())""")
        await database.execute("""
            CREATE TABLE IF NOT EXISTS antilopay_webhooks (
                payment_id TEXT PRIMARY KEY, received_at TIMESTAMP NOT NULL DEFAULT NOW())""")
        await database.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id SERIAL PRIMARY KEY, data TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'free', used_at TEXT, link_token TEXT)""")
        await database.execute("""
            CREATE TABLE IF NOT EXISTS business_requests (
                token TEXT PRIMARY KEY,
                email TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMP)""")
        await database.execute("CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status)")
    try:
        await database.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS transaction_id TEXT")
        await database.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS activation_token TEXT")
        await database.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS paid_at TIMESTAMP")
        await database.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_provider TEXT DEFAULT 'platega'")
        await database.execute("ALTER TABLE business_requests ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending'")
        await database.execute("ALTER TABLE business_requests ADD COLUMN IF NOT EXISTS processed_at TIMESTAMP")
        await database.execute("UPDATE business_requests SET status='pending' WHERE status IS NULL")
    except Exception:
        pass

# ================== ХЕЛПЕРЫ БД ==================
async def get_link(token: str):
    result = await database.fetch_one("SELECT * FROM activation_links WHERE token=:token", values={"token": token})
    return dict(result) if result else None

async def inc_attempt(token: str, err: str):
    await database.execute("UPDATE activation_links SET attempts=attempts+1, last_error=:err WHERE token=:token", values={"err": str(err)[:500], "token": token})

async def set_link_status(token: str, status: str):
    await database.execute("UPDATE activation_links SET status=:status WHERE token=:token", values={"status": status, "token": token})

async def attach_cdk(token: str, code: str):
    await database.execute("UPDATE activation_links SET cdk_code=:code WHERE token=:token", values={"code": code, "token": token})

async def create_link(product: str) -> str:
    token = secrets.token_urlsafe(16)
    await database.execute("INSERT INTO activation_links(token, product, status, attempts) VALUES (:token, :product, 'active', 0)", values={"token": token, "product": product})
    return token

async def reserve_one_cdk(product: str):
    async with database.transaction():
        row = await database.fetch_one("SELECT code FROM promo_codes WHERE product=:product AND status='free' LIMIT 1 FOR UPDATE SKIP LOCKED", values={"product": product})
        if not row:
            return None
        code = row["code"]
        await database.execute("UPDATE promo_codes SET status='reserved', reserved_at=NOW() WHERE code=:code", values={"code": code})
        return code

async def mark_cdk_used(code: str):
    await database.execute("UPDATE promo_codes SET status='used', used_at=NOW(), last_error=NULL WHERE code=:code", values={"code": code})

async def mark_cdk_bad(code: str, err: str):
    await database.execute("UPDATE promo_codes SET status='bad', last_error=:err WHERE code=:code", values={"err": str(err)[:500], "code": code})

# ================== ХЕЛПЕРЫ ОПЛАТЫ ==================
def no_store_json(data: dict, status_code: int = 200):
    return JSONResponse(data, status_code=status_code, headers={
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache", "Expires": "0"})

def _csrf_valid(request: Request) -> bool:
    csrf_header = (request.headers.get("X-CSRF-Token") or "").strip()
    csrf_cookie = (request.cookies.get("csrf_token") or "").strip()
    if not csrf_header or not csrf_cookie:
        return False
    return secrets.compare_digest(csrf_header, csrf_cookie)

# ================== MARK ORDER PAID (FIXED — NO promo_code_used) ==================
async def mark_order_paid_once(order_id: str, product: str):
    """
    FIXED: Полностью убрали promo_code_used из SELECT.
    Это устраняет DatatypeMismatchError: COALESCE types text and timestamp.
    """
    referral_user_id = None
    referral_amount = None
    referral_code = None

    async with database.transaction():
        try:
            row = await database.fetch_one(
                """
                SELECT status, activation_token, user_id, amount, referral_code
                FROM orders
                WHERE order_id=:oid
                FOR UPDATE
                """,
                values={"oid": order_id}
            )
        except Exception as e:
            logging.warning(f"Full SELECT failed for order {order_id}: {e}")
            try:
                row = await database.fetch_one(
                    "SELECT status, activation_token FROM orders WHERE order_id=:oid FOR UPDATE",
                    values={"oid": order_id}
                )
            except Exception:
                return None

        if not row:
            return None

        row = dict(row)

        if row["status"] == "paid" and row["activation_token"]:
            return row["activation_token"]

        referral_user_id = row.get("user_id")
        referral_amount = row.get("amount")
        referral_code = row.get("referral_code")

        token = row.get("activation_token")
        if not token:
            token = await create_link(product)

        updated = await database.fetch_one(
            """
            UPDATE orders
            SET status='paid',
                paid_at=COALESCE(paid_at, NOW()::text),
                activation_token = CASE WHEN activation_token IS NULL THEN :token ELSE activation_token END
            WHERE order_id=:oid
            RETURNING activation_token
            """,
            values={"token": token, "oid": order_id}
        )

        if updated:
            updated = dict(updated)
            token = updated.get("activation_token") or token

    if referral_user_id and referral_amount:
        try:
            bonus = await process_referral_bonus(database, int(referral_user_id), int(referral_amount), order_id)
            logging.info(f"Referral bonus processed for order {order_id}: bonus={bonus}")
        except Exception as e:
            logging.error(f"Referral bonus error for {order_id}: {e}")

    elif referral_code and referral_amount:
        try:
            referral_code_clean = str(referral_code).strip().upper()
            if REF_CODE_RE.match(referral_code_clean):
                referrer = await database.fetch_one(
                    "SELECT id FROM users WHERE ref_code=:code AND is_active=TRUE",
                    values={"code": referral_code_clean}
                )
                if referrer:
                    bonus = await process_referral_bonus(
                        database, 0, int(referral_amount), order_id,
                        referrer_override=int(referrer["id"]),
                    )
                    logging.info(f"Anonymous referral bonus for {order_id}: bonus={bonus}")
        except Exception as e:
            logging.error(f"Anonymous referral bonus error for {order_id}: {e}")

    return token


def _link_path_for_product(product: str, token: str) -> str:
    if product == "plus_account":
        return f"/a/{token}"
    if product == "plus_account_new":
        return f"/account/{token}"
    if product == "business_1m":
        return f"/business/{token}"
    return f"/l/{token}"

# ================== РОУТЫ СТАТИКИ ==================
@app.api_route("/", methods=["GET", "HEAD"])
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"), media_type="text/html")

@app.api_route("/success", methods=["GET", "HEAD"])
def success_page():
    return FileResponse(os.path.join(STATIC_DIR, "success.html"), media_type="text/html")

@app.api_route("/login", methods=["GET", "HEAD"])
def login_page():
    return FileResponse(os.path.join(STATIC_DIR, "login.html"), media_type="text/html")

@app.api_route("/reset-password", methods=["GET", "HEAD"])
def reset_password_page():
    return FileResponse(os.path.join(STATIC_DIR, "reset-password.html"))

@app.api_route("/cabinet", methods=["GET", "HEAD"])
def cabinet_page():
    return FileResponse(os.path.join(STATIC_DIR, "cabinet.html"), media_type="text/html")

@app.api_route("/referral", methods=["GET", "HEAD"])
def referral_page():
    return FileResponse(os.path.join(STATIC_DIR, "referral.html"), media_type="text/html")

@app.api_route("/blocked", methods=["GET", "HEAD"])
def blocked_page():
    return FileResponse(os.path.join(STATIC_DIR, "blocked.html"), media_type="text/html")

@app.api_route("/error", methods=["GET", "HEAD"])
def error_page():
    return FileResponse(os.path.join(STATIC_DIR, "error.html"), media_type="text/html")

@app.api_route("/refund", methods=["GET", "HEAD"])
def refund_page():
    return FileResponse(os.path.join(STATIC_DIR, "refund.html"), media_type="text/html")

@app.api_route("/privacy", methods=["GET", "HEAD"])
def privacy_page():
    return FileResponse(os.path.join(STATIC_DIR, "privacy.html"), media_type="text/html")

@app.api_route("/offer", methods=["GET", "HEAD"])
def offer_page():
    return FileResponse(os.path.join(STATIC_DIR, "offer.html"), media_type="text/html")

@app.api_route("/payment-success", methods=["GET", "HEAD"])
def payment_success_page():
    return FileResponse(os.path.join(STATIC_DIR, "payment_success.html"), media_type="text/html")

@app.api_route("/contacts", methods=["GET", "HEAD"])
def contacts_page():
    return FileResponse(os.path.join(STATIC_DIR, "contacts.html"), media_type="text/html")

@app.api_route("/yandex_058f016ea1ba9d0a.html", methods=["GET", "HEAD"])
def yandex_verification():
    return FileResponse(os.path.join(STATIC_DIR, "yandex_058f016ea1ba9d0a.html"))

@app.api_route("/a/{token}", methods=["GET", "HEAD"])
async def account_page(token: str):
    link = await get_link(token)
    if not link or link["product"] != "plus_account":
        return RedirectResponse("/error")
    return FileResponse(os.path.join(STATIC_DIR, "account.html"), media_type="text/html")


@app.api_route("/account/{token}", methods=["GET", "HEAD"])
async def account_site_page(token: str):
    link = await get_link(token)
    if not link or link["product"] != "plus_account_new":
        return RedirectResponse("/error")
    return FileResponse(os.path.join(STATIC_DIR, "account_site.html"), media_type="text/html")


@app.api_route("/business/{token}", methods=["GET", "HEAD"])
async def business_page(token: str):
    link = await get_link(token)
    if not link or link["product"] != "business_1m":
        return RedirectResponse("/error")
    return FileResponse(os.path.join(STATIC_DIR, "business.html"), media_type="text/html")

@app.api_route("/l/{token}", methods=["GET", "HEAD"])
async def link_page(token: str):
    link = await get_link(token)
    if not link:
        return RedirectResponse("/error")
    if link["product"] == "plus_account":
        return RedirectResponse(f"/a/{token}")
    if link["product"] == "plus_account_new":
        return RedirectResponse(f"/account/{token}")
    if link["product"] == "business_1m":
        return RedirectResponse(f"/business/{token}")
    if link["status"] == "used":
        return RedirectResponse("/success")
    if link["status"] == "blocked" or link["attempts"] >= MAX_ATTEMPTS:
        await set_link_status(token, "blocked")
        return RedirectResponse("/blocked")
    return FileResponse(os.path.join(STATIC_DIR, "activate.html"), media_type="text/html")

@app.get("/api/link/{token}")
async def api_link_info(token: str):
    link = await get_link(token)
    if not link:
        return no_store_json({"error": "not found"}, status_code=404)
    return no_store_json({
        "product": link["product"], "status": link["status"],
        "attempts": link["attempts"], "max_attempts": MAX_ATTEMPTS,
    })

# ================== ОПЛАТА ==================
@app.post("/create-payment")
@limiter.limit("60/minute")
async def create_payment(request: Request, payload: CreatePaymentRequest):
    if not _csrf_valid(request):
        return no_store_json({"error": "csrf failed"}, status_code=403)

    prices = {"plus_1m": 600, "go_12m": 1200, "plus_12m": 5000, "plus_account_new": 600, "business_1m": 600}
    amount = prices[payload.product]
    order_id = str(uuid.uuid4())
    provider = "antilopay" if payload.method == "sbp" else "platega"
    user_id = None
    user_email = None
    referral_code = None

    auth_header = request.headers.get("Authorization", "")
    if auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
        if token:
            try:
                user = await get_user_by_session(database, token)
                user_id = user.get("id")
                user_email = user.get("email")
            except Exception as e:
                logging.warning(f"Optional auth lookup failed in create_payment: {e}")

    if not user_id:
        raw_ref = str(request.cookies.get("referral_code") or "").strip().upper()
        if raw_ref and REF_CODE_RE.match(raw_ref):
            try:
                referrer = await database.fetch_one("SELECT id FROM users WHERE ref_code=:code AND is_active=TRUE", values={"code": raw_ref})
                if referrer:
                    referral_code = raw_ref
            except Exception:
                pass
        elif raw_ref:
            logging.warning("Invalid referral_code format in cookie; ignoring")

    try:
        await database.execute(
            """INSERT INTO orders(order_id, product, amount, status, payment_provider, user_id, user_email, referral_code)
               VALUES(:oid, :prod, :amt, 'created', :prov, :uid, :uemail, :ref_code)""",
            values={"oid": order_id, "prod": payload.product, "amt": amount, "prov": provider, "uid": user_id, "uemail": user_email, "ref_code": referral_code}
        )
    except Exception:
        await database.execute(
            "INSERT INTO orders(order_id, product, amount, status, payment_provider) VALUES(:oid, :prod, :amt, 'created', :prov)",
            values={"oid": order_id, "prod": payload.product, "amt": amount, "prov": provider}
        )
        if user_id is not None:
            try:
                await database.execute("UPDATE orders SET user_id=:uid WHERE order_id=:oid", values={"uid": user_id, "oid": order_id})
            except Exception:
                pass
        if user_email:
            try:
                await database.execute("UPDATE orders SET user_email=:uemail WHERE order_id=:oid", values={"uemail": user_email, "oid": order_id})
            except Exception:
                pass
        if referral_code:
            try:
                await database.execute("UPDATE orders SET referral_code=:ref_code WHERE order_id=:oid", values={"ref_code": referral_code, "oid": order_id})
            except Exception:
                pass

    # ========== ANTILOPAY ==========
    if payload.method == "sbp":
        if not ANTILOPAY_SECRET_ID or not ANTILOPAY_SECRET_KEY or not ANTILOPAY_PROJECT_ID:
            return no_store_json({"error": "СБП временно недоступен"}, status_code=500)
        body = {
            "project_identificator": ANTILOPAY_PROJECT_ID, "amount": amount,
            "order_id": order_id, "currency": "RUB",
            "product_name": f"Подписка {payload.product}", "product_type": "services",
            "description": f"Оплата подписки {payload.product}",
            "success_url": f"{DOMAIN}/payment-success?order_id={order_id}",
            "fail_url": f"{DOMAIN}/?payment=failed",
            "customer": {"email": "support@codenext.ru"}, "prefer_methods": ["SBP"]
        }
        try:
            signature = sign_antilopay_request(body)
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post("https://lk.antilopay.com/api/v1/payment/create", json=body,
                    headers={"X-Apay-Secret-Id": ANTILOPAY_SECRET_ID, "X-Apay-Sign": signature, "X-Apay-Sign-Version": "1", "Content-Type": "application/json"})
                res_json = resp.json()
        except Exception as e:
            logging.error(f"Antilopay error: {e}")
            return no_store_json({"error": "Ошибка соединения с платежной системой"}, status_code=500)
        if res_json.get("code") != 0:
            logging.error(f"Antilopay create payment error: {res_json}")
            return no_store_json({"error": "Ошибка создания платежа"}, status_code=500)
        payment_id = res_json.get("payment_id")
        if payment_id:
            await database.execute("UPDATE orders SET transaction_id=:tid WHERE order_id=:oid", values={"tid": payment_id, "oid": order_id})
        return no_store_json({"payment_url": res_json.get("payment_url"), "order_id": order_id})

    # ========== PLATEGA ==========
    else:
        if not PLATEGA_MERCHANT_ID:
            return no_store_json({"error": "Платёжная система не настроена"}, status_code=500)
        method_map = {"card": 11, "crypto": 13}
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post("https://app.platega.io/transaction/process",
                    json={"paymentMethod": method_map.get(payload.method, 11),
                          "paymentDetails": {"amount": amount, "currency": "RUB"},
                          "description": f"Подписка {payload.product}",
                          "return": f"{DOMAIN}/payment-success?order_id={order_id}",
                          "failedUrl": f"{DOMAIN}/?payment=failed", "payload": order_id},
                    headers={"X-MerchantId": PLATEGA_MERCHANT_ID, "X-Secret": PLATEGA_SECRET,
                             "Content-Type": "application/json",
                             "User-Agent": "Mozilla/5.0 (compatible; CodeNext/1.0)"})
                res_json = resp.json()
        except Exception as e:
            logging.error(f"Platega error: {e}")
            return no_store_json({"error": "Ошибка соединения с платежной системой"}, status_code=500)
        if resp.status_code >= 400 or not res_json.get("redirect"):
            logging.error(f"Platega create payment error: status={resp.status_code}, body={res_json}")
            return no_store_json({"error": "Ошибка создания платежа"}, status_code=500)
        tx_id = res_json.get("transactionId")
        if tx_id:
            await database.execute("UPDATE orders SET transaction_id=:tid WHERE order_id=:oid", values={"tid": tx_id, "oid": order_id})
        return no_store_json({"payment_url": res_json.get("redirect"), "order_id": order_id})

# ================== CHECK ORDER (FIXED & SMART) ==================
@app.get("/api/check-order/{order_id}")
@limiter.limit("60/minute")
async def check_order(request: Request, order_id: str):
    if not UUID_RE.match(order_id):
        return no_store_json({"error": "bad order_id"}, status_code=400)

    res = await database.fetch_one(
        "SELECT status, activation_token, created_at, transaction_id, payment_provider, product FROM orders WHERE order_id=:oid",
        values={"oid": order_id}
    )
    if not res:
        return no_store_json({"error": "not found"}, status_code=404)
    res = dict(res)

    if res['status'] == 'paid':
        token = res['activation_token']
        if not token:
            token = await mark_order_paid_once(order_id, res['product'])
        if token:
            return no_store_json({"status": "paid", "link": _link_path_for_product(res["product"], token)})
        return no_store_json({"status": "pending"})

    # AUTO-FALLBACK
    if res.get('created_at') and res['status'] in ['created', 'pending']:
        try:
            age_seconds = time.time() - res['created_at'].timestamp()
        except Exception:
            age_seconds = 0

        if age_seconds > 10:
            tx_id = res.get('transaction_id')
            provider = res.get('payment_provider', 'platega')

            if tx_id and provider == "antilopay":
                # 1. API Check
                data = await check_antilopay_payment_status(tx_id)
                success = data and data.get("code") == 0 and data.get("status") == "SUCCESS"

                # 2. LOCAL DB Check (если API сломан — ищем webhook в базе)
                if not success:
                    row_wh = await database.fetch_one("SELECT 1 FROM antilopay_webhooks WHERE payment_id=:pid", values={"pid": tx_id})
                    if row_wh:
                        success = True
                        logging.info(f"Order {order_id} confirmed via LOCAL webhook record (API failed)")

                if success:
                    token = await mark_order_paid_once(order_id, res['product'])
                    if token:
                        return no_store_json({"status": "paid", "link": _link_path_for_product(res["product"], token)})

            elif tx_id and provider == "platega":
                success = False
                try:
                    async with httpx.AsyncClient(timeout=10) as client:
                        resp = await client.get(f"https://app.platega.io/transaction/{tx_id}",
                            headers={"X-MerchantId": PLATEGA_MERCHANT_ID, "X-Secret": PLATEGA_SECRET,
                                     "User-Agent": "Mozilla/5.0 (compatible; CodeNext/1.0)"},
                            follow_redirects=True)
                        data = resp.json()
                    if str(data.get("status", "")).upper() == "CONFIRMED":
                        success = True
                except Exception as e:
                    logging.error(f"Auto-fallback platega error for {order_id}: {e}")

                # LOCAL DB Check — ищем webhook в базе
                if not success:
                    row_wh = await database.fetch_one("SELECT 1 FROM platega_webhooks WHERE transaction_id=:tid", values={"tid": tx_id})
                    if row_wh:
                        success = True
                        logging.info(f"Order {order_id} confirmed via LOCAL Platega webhook record")

                if success:
                    token = await mark_order_paid_once(order_id, res['product'])
                    if token:
                        return no_store_json({"status": "paid", "link": _link_path_for_product(res["product"], token)})

    return no_store_json({"status": res['status']})

# ================== FALLBACK CHECK (FIXED & SMART) ==================
@app.get("/api/fallback-check/{order_id}")
@limiter.limit("6/minute")
async def fallback_check(request: Request, order_id: str):
    if not UUID_RE.match(order_id):
        return no_store_json({"error": "bad order_id"}, status_code=400)

    now = int(time.time())
    if now - _FALLBACK_LAST_CHECK.get(order_id, 0) < _FALLBACK_MIN_INTERVAL:
        return no_store_json({"status": "cooldown"})
    _FALLBACK_LAST_CHECK[order_id] = now

    row = await database.fetch_one(
        "SELECT status, activation_token, transaction_id, product, payment_provider, amount FROM orders WHERE order_id=:oid",
        values={"oid": order_id}
    )
    if not row:
        return no_store_json({"error": "not found"}, status_code=404)
    row = dict(row)

    if row['status'] == "paid":
        token = row['activation_token']
        if not token:
            token = await mark_order_paid_once(order_id, row['product'])
        if token:
            return no_store_json({"status": "paid", "link": _link_path_for_product(row["product"], token)})
        return no_store_json({"status": "pending"})

    if not row['transaction_id']:
        return no_store_json({"status": row['status']})

    provider = row.get('payment_provider', 'platega')

    # ========== ANTILOPAY ==========
    if provider == "antilopay":
        data = await check_antilopay_payment_status(row['transaction_id'])
        success = data and data.get("code") == 0 and data.get("status") == "SUCCESS"

        if not success:
            row_wh = await database.fetch_one("SELECT 1 FROM antilopay_webhooks WHERE payment_id=:pid", values={"pid": row['transaction_id']})
            if row_wh:
                success = True
                logging.info(f"Fallback check: Order {order_id} confirmed via LOCAL webhook record")

        if success:
            token = await mark_order_paid_once(order_id, row['product'])
            if token:
                return no_store_json({"status": "paid", "link": _link_path_for_product(row["product"], token)})

    # ========== PLATEGA ==========
    else:
        success = False
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"https://app.platega.io/transaction/{row['transaction_id']}",
                    headers={"X-MerchantId": PLATEGA_MERCHANT_ID, "X-Secret": PLATEGA_SECRET,
                             "User-Agent": "Mozilla/5.0 (compatible; CodeNext/1.0)"},
                    follow_redirects=True)
                data = resp.json()
            if str(data.get("status", "")).upper() == "CONFIRMED":
                success = True
        except Exception as e:
            logging.error(f"Platega fallback status error for {order_id}: {e}")

        # LOCAL DB Check
        if not success:
            row_wh = await database.fetch_one("SELECT 1 FROM platega_webhooks WHERE transaction_id=:tid", values={"tid": row['transaction_id']})
            if row_wh:
                success = True
                logging.info(f"Fallback check: Order {order_id} confirmed via LOCAL Platega webhook record")

        if success:
            token = await mark_order_paid_once(order_id, row['product'])
            if token:
                return no_store_json({"status": "paid", "link": _link_path_for_product(row["product"], token)})

    return no_store_json({"status": row['status']})

# ================== WEBHOOKS ==================
@app.post("/webhook/platega_bbb5ee3f3d5bbc4ed3b6aa02")
async def platega_webhook(request: Request):
    ua = request.headers.get("User-Agent", "")
    if ua.startswith("Platega-CallbackUrlCheck"):
        return {"ok": True}
    if request.headers.get("X-MerchantId") != PLATEGA_MERCHANT_ID:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        payload = await request.json()
        tx_id = payload.get("id") or payload.get("transactionId")
        status = str(payload.get("status", "")).upper()
    except Exception:
        logging.error("Platega webhook JSON parse failed")
        return "OK"
    if status != "CONFIRMED" or not tx_id:
        return "OK"
    try:
        await database.execute("INSERT INTO platega_webhooks(transaction_id) VALUES (:tid)", values={"tid": str(tx_id)})
    except Exception:
        logging.warning(f"Duplicate or failed Platega webhook insert for tx_id={tx_id}")
        return "OK"
    order = await database.fetch_one(
        "SELECT order_id, status, product FROM orders WHERE transaction_id=:tid AND payment_provider='platega'",
        values={"tid": str(tx_id)}
    )
    if order:
        order = dict(order)
        if order['status'] != "paid":
            await mark_order_paid_once(order['order_id'], order['product'])
    return "OK"

@app.post("/webhook/antilopay_f9c3a8e2b1d4567a")
async def antilopay_webhook(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Apay-Callback", "")
    if not verify_antilopay_callback(body, signature):
        logging.warning("Invalid Antilopay callback signature")
        return JSONResponse({"error": "invalid signature"}, status_code=401)
    try:
        payload = json.loads(body)
        payment_id = payload.get("payment_id")
        status = payload.get("status")
        order_id = payload.get("order_id")
    except Exception:
        logging.error("Antilopay webhook JSON parse failed")
        return "OK"
    if not payment_id or status != "SUCCESS":
        return "OK"

    order = await database.fetch_one(
        "SELECT order_id, status, product, amount FROM orders WHERE order_id=:oid AND payment_provider='antilopay'",
        values={"oid": order_id}
    )
    if not order:
        return "OK"
    order = dict(order)

    # Проверка суммы (защита от мошенничества)
    original_amount = payload.get("original_amount", 0)
    try:
        if abs(float(original_amount) - float(order['amount'])) > 0.01:
            logging.error(f"Amount mismatch for order {order_id}: expected {order['amount']}, got {original_amount}")
            return "OK"
    except Exception:
        logging.error(f"Bad original_amount in Antilopay webhook for order {order_id}: {original_amount}")
        return "OK"

    # Сохраняем webhook ТОЛЬКО после проверки суммы
    try:
        await database.execute("INSERT INTO antilopay_webhooks(payment_id) VALUES (:pid)", values={"pid": payment_id})
    except Exception:
        pass  # Дубликат — OK

    if order['status'] != "paid":
        await mark_order_paid_once(order['order_id'], order['product'])

    return "OK"

# ================== АКТИВАЦИЯ (ПОЛНАЯ ЛОГИКА ВОССТАНОВЛЕНА) ==================
@app.post("/activate")
@limiter.limit("10/minute")
async def activate_submit(request: Request):
    if not _csrf_valid(request):
        return JSONResponse({"error": "CSRF check failed"}, status_code=403)

    try:
        payload = await request.json()
        token = (payload.get("token") or "").strip()
        auth_json = (payload.get("auth_json") or "").strip()
    except Exception:
        return JSONResponse({"error": "Некорректные данные"}, status_code=400)

    if not token or not auth_json:
        return JSONResponse({"error": "Пожалуйста, вставьте данные."}, status_code=400)

    link = await get_link(token)
    if not link or link["status"] != "active":
        return JSONResponse({"error": "Ссылка недействительна."}, status_code=403)

    if link["attempts"] >= MAX_ATTEMPTS:
        await set_link_status(token, "blocked")
        return JSONResponse({"error": f"Превышен лимит попыток. Поддержка: {SUPPORT_LINK}"}, status_code=403)

    try:
        parsed = json.loads(auth_json)
        if not isinstance(parsed, dict):
            raise ValueError
    except Exception:
        await inc_attempt(token, "invalid json")
        link2 = await get_link(token)
        rem = MAX_ATTEMPTS - (link2['attempts'] if link2 else MAX_ATTEMPTS)
        return JSONResponse({"error": f"Данные указаны некорректно. Осталось попыток: {rem}"}, status_code=400)

    code = link["cdk_code"]
    if not code:
        code = await reserve_one_cdk(link["product"])
        if not code:
            return JSONResponse({"error": f"Техническая ошибка. Поддержка: {SUPPORT_LINK}"}, status_code=503)
        await attach_cdk(token, code)

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: _activator_run_flow(code, parsed))
    except Exception as e:
        logging.error(f"Activation error: {e}")
        return JSONResponse({"error": "Техническая ошибка. Попробуйте позже."}, status_code=500)

    # ✅ УСПЕХ
    if result.get("success"):
        await mark_cdk_used(code)
        await set_link_status(token, "used")
        return {"status": "ok", "redirect": "/success"}

    raw_err = (result.get("error") or "").lower()
    is_pending = result.get("pending", False)

    # ✅ СЛУЧАЙ 1: Таймаут/pending — проверяем через check_cdk
    if is_pending or "timeout" in raw_err or "pending" in raw_err or "network" in raw_err:
        logging.warning(f"Timeout/pending for token {token}, checking CDK status...")
        await asyncio.sleep(10)

        try:
            cdk_status = await loop.run_in_executor(None, lambda: _activator_check_cdk(code))

            if not cdk_status.get("valid"):
                err_text = str(cdk_status.get("error", "")).lower()
                if any(x in err_text for x in ["used", "invalid", "not found", "已使用"]):
                    logging.info(f"Ghost success recovery: CDK was actually used for token {token}")
                    await mark_cdk_used(code)
                    await set_link_status(token, "used")
                    return {"status": "ok", "redirect": "/success"}
        except Exception as e:
            logging.error(f"check_cdk failed for token {token}: {e}")

        return JSONResponse({"error": "Временная ошибка соединения. Повторите через минуту."}, status_code=408)

    # ✅ СЛУЧАЙ 2: CDK уже использован
    if "cdk already used" in raw_err or "已使用" in raw_err:
        logging.warning(f"CDK already used, treating as success for token {token}")
        await mark_cdk_used(code)
        await set_link_status(token, "used")
        return {"status": "ok", "redirect": "/success"}

    # ✅ СЛУЧАЙ 3: Ошибки данных пользователя
    incorrect_data_errors = [
        "user is invalid", "token is invalid", "access token expired",
        "authorization failed", "invalid authorization data"
    ]
    if any(e in raw_err for e in incorrect_data_errors):
        await inc_attempt(token, raw_err)
        link2 = await get_link(token)
        rem = MAX_ATTEMPTS - (link2['attempts'] if link2 else MAX_ATTEMPTS)
        if rem <= 0:
            await set_link_status(token, "blocked")
            return JSONResponse({"error": f"Превышен лимит попыток. Поддержка: {SUPPORT_LINK}"}, status_code=403)
        return JSONResponse({"error": f"Данные указаны некорректно. Осталось попыток: {rem}"}, status_code=400)

    # ✅ СЛУЧАЙ 4: CDK битый
    if "not found" in raw_err and "cdk" in raw_err:
        await mark_cdk_bad(code, raw_err)
        await set_link_status(token, "blocked")
        return JSONResponse({"error": f"Техническая ошибка. Поддержка: {SUPPORT_LINK}"}, status_code=500)

    # ✅ СЛУЧАЙ 5: Неизвестная ошибка
    await inc_attempt(token, raw_err)
    link2 = await get_link(token)
    rem = MAX_ATTEMPTS - (link2['attempts'] if link2 else MAX_ATTEMPTS)
    if rem <= 0:
        await set_link_status(token, "blocked")
        return JSONResponse({"error": f"Превышен лимит попыток. Поддержка: {SUPPORT_LINK}"}, status_code=403)

    return JSONResponse({"error": f"Не удалось завершить активацию. Осталось попыток: {rem}"}, status_code=400)

# ================== АККАУНТЫ ==================
@app.post("/get-account")
@limiter.limit("10/minute")
async def get_account(request: Request):
    if not _csrf_valid(request):
        return JSONResponse({"error": "CSRF check failed"}, status_code=403)

    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"error": "Некорректные данные"}, status_code=400)

    token = (payload.get("token") or "").strip()
    if not token:
        return JSONResponse({"error": "Ссылка недействительна."}, status_code=403)

    link = await get_link(token)
    if not link:
        return JSONResponse({"error": "Ссылка недействительна."}, status_code=403)

    if link.get("product") not in {"plus_account", "plus_account_new"}:
        return JSONResponse({"error": "Ссылка недействительна."}, status_code=403)

    if link.get("status") == "blocked":
        return JSONResponse({"error": f"Ссылка заблокирована. Поддержка: {SUPPORT_LINK}"}, status_code=403)

    if link.get("attempts", 0) >= MAX_ATTEMPTS:
        await set_link_status(token, "blocked")
        return JSONResponse({"error": f"Превышен лимит попыток. Поддержка: {SUPPORT_LINK}"}, status_code=403)

    if link.get("cdk_code"):
        return {"status": "ok", "account": link["cdk_code"]}

    if link.get("status") != "active":
        return JSONResponse({"error": "Ссылка недействительна."}, status_code=403)

    async with database.transaction():
        row = await database.fetch_one("SELECT id, data FROM accounts WHERE status='free' LIMIT 1 FOR UPDATE SKIP LOCKED")
        if not row:
            return JSONResponse({"error": f"Нет доступных аккаунтов. Поддержка: {SUPPORT_LINK}"}, status_code=503)

        account_id = row["id"]
        account_data = row["data"]
        await database.execute("UPDATE accounts SET status='used', used_at=NOW(), link_token=:token WHERE id=:id", values={"token": token, "id": account_id})

    await attach_cdk(token, account_data)
    await set_link_status(token, "used")
    return {"status": "ok", "account": account_data}


@app.post("/submit-business-request")
@limiter.limit("10/minute")
async def submit_business_request(request: Request):
    if not _csrf_valid(request):
        return JSONResponse({"error": "CSRF check failed"}, status_code=403)

    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"error": "Некорректные данные"}, status_code=400)

    token = str(payload.get("token") or "").strip()
    email = str(payload.get("email") or "").strip().lower()

    if not token or not email:
        return JSONResponse({"error": "Заполните все поля."}, status_code=400)
    if "@" not in email or len(email) > 254:
        return JSONResponse({"error": "Введите корректный email."}, status_code=400)

    link = await get_link(token)
    if not link or link.get("product") != "business_1m":
        return JSONResponse({"error": "Ссылка недействительна."}, status_code=403)
    if link.get("status") == "blocked":
        return JSONResponse({"error": f"Ссылка заблокирована. Поддержка: {SUPPORT_LINK}"}, status_code=403)

    await database.execute(
        """
        INSERT INTO business_requests(token, email, status, processed_at)
        VALUES (:token, :email, 'pending', NULL)
        ON CONFLICT (token) DO UPDATE
        SET email = EXCLUDED.email,
            status = 'pending',
            processed_at = NULL
        """,
        values={"token": token, "email": email},
    )

    await attach_cdk(token, email)
    await set_link_status(token, "used")
    return {
        "status": "ok",
        "message": "Ваша заявка попала на ручную обработку. Как только приглашение будет отправлено, вы получите уведомление на почту, привязанную к ChatGPT.",
    }

# ================== АДМИН АКТИВАЦИЯ ==================
@app.api_route("/admin/nitro", methods=["GET", "HEAD"])
def admin_nitro_page():
    return FileResponse(os.path.join(STATIC_DIR, "admin_nitro_activate.html"))


# ================== АДМИН NITRO PROXY ==================
@app.post("/admin/nitro-proxy")
async def admin_nitro_proxy(request: Request):
    """Прокси для Nitro API (обход CORS)"""
    try:
        payload = await request.json()
        cdk = payload.get("cdk", "").strip()
        auth_json = payload.get("auth_json", "").strip()
    except:
        return JSONResponse({"error": "Invalid data"}, status_code=400)
    
    if not cdk or not auth_json:
        return JSONResponse({"error": "Empty fields"}, status_code=400)
    
    try:
        authsession = json.loads(auth_json)
    except:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    
    # Используем текущий активатор (nitro или ovh, зависит от USE_OVH_ACTIVATOR)
    loop = asyncio.get_event_loop()
    
    try:
        result = await loop.run_in_executor(None, lambda: _activator_run_flow(cdk, authsession))
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}
