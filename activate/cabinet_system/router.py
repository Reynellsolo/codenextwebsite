from collections import defaultdict, deque
import json
import os
import uuid
import time
import secrets
import asyncio
import logging
from typing import Optional, Dict

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from databases import Database

try:
    import redis.asyncio as redis_async
except Exception:
    redis_async = None

from .config import (
    CABINET_PREFIX,
    EXPOSE_DEBUG_RESET_TOKEN,
    AUTH_REGISTER_LIMIT_PER_MINUTE,
    AUTH_LOGIN_LIMIT_PER_MINUTE,
    ENVIRONMENT,
)
from .models import (
    RegisterRequest,
    LoginRequest,
    ForgotPasswordRequest,
    ResetPasswordRequest,
    CreatePaymentDraftRequest,
    WithdrawRequest,
)
from .services import (
    register_user,
    login_user,
    get_user_by_session,
    calculate_balance,
    create_reset_token,
    reset_password,
    smtp_configured,
    send_password_reset_email,
)

router = APIRouter(prefix=CABINET_PREFIX, tags=["cabinet"])
_db: Optional[Database] = None
_RATE_LIMIT_BUCKETS: Dict[str, deque] = defaultdict(deque)
REDIS_URL = os.getenv("REDIS_URL", "").strip()
_REDIS = None
if redis_async is not None and REDIS_URL:
    try:
        _REDIS = redis_async.from_url(REDIS_URL, decode_responses=True)
    except Exception as e:
        import logging
        logging.error(f"Failed to initialize Redis client: {e}")


def set_database(database: Database):
    global _db
    _db = database


def _db_required() -> Database:
    if _db is None:
        raise RuntimeError("cabinet_system database not configured")
    return _db


def _bearer(authorization: Optional[str]) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization")
    parts = authorization.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1]:
        raise HTTPException(status_code=401, detail="Invalid Authorization")
    return parts[1].strip()


def _client_key(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "").split(",", 1)[0].strip()
    return forwarded or (request.client.host if request.client else "unknown")


def _csrf_ok(request: Request) -> bool:
    csrf_header = (request.headers.get("X-CSRF-Token") or "").strip()
    csrf_cookie = (request.cookies.get("csrf_token") or "").strip()
    if not csrf_header or not csrf_cookie:
        return False
    return secrets.compare_digest(csrf_header, csrf_cookie)


def _require_csrf(request: Request):
    if not _csrf_ok(request):
        raise HTTPException(status_code=403, detail="CSRF check failed")


async def _enforce_rate_limit(scope: str, key: str, max_hits: int, window_seconds: int = 60):
    if _REDIS is not None:
        now_ms = int(time.time() * 1000)
        window_ms = window_seconds * 1000
        rkey = f"rl:{scope}:{key}"
        member = f"{now_ms}:{uuid.uuid4().hex}"
        pipe = _REDIS.pipeline()
        pipe.zadd(rkey, {member: now_ms})
        pipe.zremrangebyscore(rkey, 0, now_ms - window_ms)
        pipe.zcard(rkey)
        pipe.expire(rkey, window_seconds + 5)
        _, _, count, _ = await pipe.execute()
        if int(count) > max_hits:
            raise HTTPException(status_code=429, detail="Too many requests")
        return

    now = time.time()
    bucket = _RATE_LIMIT_BUCKETS[f"{scope}:{key}"]
    while bucket and now - bucket[0] > window_seconds:
        bucket.popleft()
    if len(bucket) >= max_hits:
        raise HTTPException(status_code=429, detail="Too many requests")
    bucket.append(now)

    if len(_RATE_LIMIT_BUCKETS) > 1000:
        stale_keys = [k for k, b in _RATE_LIMIT_BUCKETS.items() if not b]
        for stale_key in stale_keys:
            _RATE_LIMIT_BUCKETS.pop(stale_key, None)


@router.get("/csrf-token")
async def get_csrf_token():
    token = secrets.token_urlsafe(32)
    response = JSONResponse({"status": "ok", "csrf_token": token})
    is_production = ENVIRONMENT == "production"
    response.set_cookie(
        key="csrf_token",
        value=token,
        max_age=3600,
        httponly=True,
        secure=is_production,
        samesite="strict" if is_production else "lax",
        path="/",
    )
    return response


@router.post("/auth/register")
async def auth_register(request: Request, payload: RegisterRequest):
    _require_csrf(request)
    await _enforce_rate_limit("register", _client_key(request), AUTH_REGISTER_LIMIT_PER_MINUTE)
    database = _db_required()
    user_id, ref_code, token = await register_user(
        database,
        payload.email,
        payload.password,
        payload.referral_code,
    )
    return {
        "status": "ok",
        "token": token,
        "user": {"id": user_id, "email": payload.email, "ref_code": ref_code},
    }


@router.post("/auth/login")
async def auth_login(request: Request, payload: LoginRequest):
    _require_csrf(request)
    await _enforce_rate_limit("login", _client_key(request), AUTH_LOGIN_LIMIT_PER_MINUTE)
    database = _db_required()
    user, token = await login_user(database, payload.email, payload.password)
    return {
        "status": "ok",
        "token": token,
        "user": {"id": user["id"], "email": user["email"], "ref_code": user["ref_code"]},
    }


@router.post("/auth/forgot-password")
async def auth_forgot_password(request: Request, payload: ForgotPasswordRequest):
    _require_csrf(request)
    await _enforce_rate_limit("forgot_password", _client_key(request), 3, window_seconds=900)
    database = _db_required()
    raw = await create_reset_token(database, payload.email)

    if raw:
        if smtp_configured():
            try:
                await asyncio.to_thread(send_password_reset_email, payload.email, raw)
            except Exception as e:
                logging.error(f"Failed to send reset email to {payload.email}: {e}")
        else:
            logging.warning("SMTP is not configured; reset email was not sent")

    resp = {
        "status": "ok",
        "message": "If this email exists, reset instructions are sent",
    }
    if EXPOSE_DEBUG_RESET_TOKEN:
        resp["debug_reset_token"] = raw
    return resp


@router.post("/auth/reset-password")
async def auth_reset_password(request: Request, payload: ResetPasswordRequest):
    _require_csrf(request)
    database = _db_required()
    await reset_password(database, payload.token, payload.new_password)
    return {"status": "ok"}


@router.get("/me")
async def me(authorization: Optional[str] = Header(default=None)):
    database = _db_required()
    token = _bearer(authorization)
    user = await get_user_by_session(database, token)
    return {"status": "ok", "user": user}


@router.get("/orders")
async def my_orders(authorization: Optional[str] = Header(default=None)):
    database = _db_required()
    token = _bearer(authorization)
    user = await get_user_by_session(database, token)
    rows = await database.fetch_all(
        """
        SELECT order_id, product, amount, status, created_at, paid_at, activation_token, promo_code_used, referral_reward
        FROM orders
        WHERE user_id=:uid
        ORDER BY created_at DESC
        LIMIT 100
        """,
        values={"uid": user["id"]},
    )
    return {"status": "ok", "orders": [dict(r) for r in rows]}




@router.get("/referrals")
async def my_referrals(authorization: Optional[str] = Header(default=None)):
    database = _db_required()
    token = _bearer(authorization)
    user = await get_user_by_session(database, token)

    rows = await database.fetch_all(
        """
        SELECT
            r.referral_id,
            u.email,
            u.created_at,
            r.status,
            r.total_earned,
            r.created_at AS referred_at
        FROM referrals r
        JOIN users u ON u.id = r.referral_id
        WHERE r.referrer_id = :uid
        ORDER BY r.created_at DESC
        LIMIT 100
        """,
        values={"uid": user["id"]},
    )

    data = []
    registered_ids = set()
    for row in rows:
        item = dict(row)
        rid = item.get("referral_id")
        if rid is not None:
            registered_ids.add(int(rid))
        item.pop("referral_id", None)
        data.append(item)

    bonus_rows = await database.fetch_all(
        """
        SELECT amount, meta_json, created_at
        FROM wallet_transactions
        WHERE user_id = :uid AND tx_type = 'referral_bonus'
        ORDER BY created_at DESC
        LIMIT 100
        """,
        values={"uid": user["id"]},
    )

    anon_bonuses = []
    for tx in bonus_rows:
        meta_raw = tx["meta_json"]
        try:
            meta = json.loads(meta_raw) if meta_raw else {}
        except Exception:
            meta = {}

        rid = meta.get("referral_id")
        try:
            rid_int = int(rid) if rid is not None else None
        except Exception:
            rid_int = None

        if rid_int and rid_int in registered_ids:
            continue

        anon_bonuses.append(
            {
                "email": "Анонимный покупатель",
                "created_at": str(tx["created_at"]),
                "status": "anonymous",
                "total_earned": abs(int(tx["amount"])),
                "referred_at": str(tx["created_at"]),
            }
        )

    all_data = data + anon_bonuses
    return {"status": "ok", "referrals": all_data, "total_count": len(all_data)}


@router.get("/wallet")
async def my_wallet(authorization: Optional[str] = Header(default=None)):
    database = _db_required()
    token = _bearer(authorization)
    user = await get_user_by_session(database, token)
    balance = await calculate_balance(database, user["id"])
    return {"status": "ok", "balance": balance}


@router.post("/withdraw-request")
async def withdraw_request(request: Request, payload: WithdrawRequest, authorization: Optional[str] = Header(default=None)):
    _require_csrf(request)
    database = _db_required()
    token = _bearer(authorization)
    user = await get_user_by_session(database, token)

    if payload.amount < 500:
        raise HTTPException(status_code=400, detail="Минимальная сумма вывода — 500₽")

    async with database.transaction():
        locked = await database.fetch_one(
            "SELECT id, balance FROM users WHERE id=:uid FOR UPDATE",
            values={"uid": user["id"]},
        )
        if not locked:
            raise HTTPException(status_code=404, detail="User not found")

        pending = await database.fetch_one(
            "SELECT id FROM withdraw_requests WHERE user_id=:uid AND status='new' LIMIT 1",
            values={"uid": user["id"]},
        )
        if pending:
            raise HTTPException(status_code=409, detail="You already have a pending withdrawal")

        if int(locked["balance"] or 0) < payload.amount:
            raise HTTPException(status_code=400, detail="Insufficient balance")

        debited = await database.fetch_one(
            """
            UPDATE users
            SET balance = balance - :amount
            WHERE id=:uid AND balance >= :amount
            RETURNING id
            """,
            values={"amount": payload.amount, "uid": user["id"]},
        )
        if not debited:
            raise HTTPException(status_code=400, detail="Insufficient balance")

        wid = await database.execute(
            """
            INSERT INTO withdraw_requests(user_id, amount, note)
            VALUES(:uid, :amount, :note)
            RETURNING id
            """,
            values={"uid": user["id"], "amount": payload.amount, "note": payload.note},
        )

        await database.execute(
            """
            INSERT INTO wallet_transactions(user_id, tx_type, amount, meta_json)
            VALUES(:uid, 'withdraw_pending', :amount, :meta)
            """,
            values={
                "uid": user["id"],
                "amount": -payload.amount,
                "meta": f'{{"withdraw_request_id": {wid}}}',
            },
        )

    return {"status": "ok", "withdraw_request_id": wid}




@router.get("/withdrawals")
async def my_withdrawals(authorization: Optional[str] = Header(default=None)):
    database = _db_required()
    token = _bearer(authorization)
    user = await get_user_by_session(database, token)
    rows = await database.fetch_all(
        """
        SELECT id, amount, status, note, created_at, processed_at
        FROM withdraw_requests
        WHERE user_id=:uid
        ORDER BY created_at DESC
        LIMIT 50
        """,
        values={"uid": user["id"]},
    )
    return {"status": "ok", "withdrawals": [dict(r) for r in rows]}


@router.get("/wallet/history")
async def wallet_history(authorization: Optional[str] = Header(default=None)):
    database = _db_required()
    token = _bearer(authorization)
    user = await get_user_by_session(database, token)
    rows = await database.fetch_all(
        """
        SELECT id, tx_type, amount, meta_json, created_at
        FROM wallet_transactions
        WHERE user_id=:uid
        ORDER BY created_at DESC
        LIMIT 100
        """,
        values={"uid": user["id"]},
    )
    return {"status": "ok", "transactions": [dict(r) for r in rows]}

@router.post("/payment-draft")
async def create_payment_draft(request: Request, payload: CreatePaymentDraftRequest, authorization: Optional[str] = Header(default=None)):
    _require_csrf(request)
    database = _db_required()
    token = _bearer(authorization)
    user = await get_user_by_session(database, token)

    method = payload.method.lower().strip()
    if method not in {"card", "sbp", "crypto"}:
        method = "card"

    promo = (payload.promo_code or "").strip().upper() or None
    if promo and promo == user["ref_code"]:
        raise HTTPException(status_code=400, detail="Cannot use your own promo code")

    return {
        "status": "ok",
        "draft": {
            "user_id": user["id"],
            "user_email": user["email"],
            "product": payload.product,
            "method": method,
            "promo_code_used": promo,
        },
    }
