from datetime import datetime, timedelta
import hashlib
import json
import secrets
import string
import smtplib
import ssl
from email.message import EmailMessage
from fastapi import HTTPException
from databases import Database
from typing import Optional, Dict
import logging

from .config import (
    REFERRAL_RATE,
    RESET_TTL_MINUTES,
    SMTP_HOST,
    SMTP_PORT,
    SMTP_USER,
    SMTP_PASSWORD,
    SMTP_FROM,
    SMTP_USE_TLS,
    SMTP_USE_SSL,
    RESET_LINK_BASE_URL,
)
from .security import hash_password, verify_password, generate_token


def _is_email_unique_violation(exc: Exception) -> bool:
    msg = str(exc).lower()
    code = str(getattr(exc, "sqlstate", "") or "")
    if code == "23505":
        if "email" in msg or "users_email_key" in msg:
            return True
    return "duplicate key" in msg and ("users_email_key" in msg or "(email)" in msg)


def _make_ref_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "CNX" + "".join(secrets.choice(alphabet) for _ in range(8))


async def create_unique_ref_code(database: Database) -> str:
    for _ in range(20):
        code = _make_ref_code()
        row = await database.fetch_one("SELECT 1 FROM users WHERE ref_code=:c", values={"c": code})
        if not row:
            return code
    raise RuntimeError("Failed to generate unique referral code")



async def register_user(database: Database, email: str, password: str, referral_code: Optional[str] = None):
    normalized_email = (email or "").strip().lower()
    exists = await database.fetch_one("SELECT id FROM users WHERE LOWER(email)=:email", values={"email": normalized_email})
    if exists:
        raise HTTPException(status_code=409, detail="Email already registered")

    referrer_id = None
    if referral_code:
        code = referral_code.strip().upper()
        referrer = await database.fetch_one(
            "SELECT id, email FROM users WHERE ref_code=:code AND is_active=TRUE",
            values={"code": code},
        )
        if referrer:
            if referrer["email"].lower() == normalized_email:
                raise HTTPException(status_code=400, detail="Cannot use your own referral code")
            referrer_id = referrer["id"]

    ref_code = await create_unique_ref_code(database)
    password_hash = hash_password(password)

    try:
        async with database.transaction():
            user_id = await database.execute(
                """
                INSERT INTO users(email, password_hash, ref_code, referrer_id)
                VALUES(:email, :password_hash, :ref_code, :referrer_id)
                RETURNING id
                """,
                values={
                    "email": normalized_email,
                    "password_hash": password_hash,
                    "ref_code": ref_code,
                    "referrer_id": referrer_id,
                },
            )

            if referrer_id:
                await database.execute(
                    """
                    INSERT INTO referrals(referrer_id, referral_id, status)
                    VALUES(:r1, :r2, 'active')
                    ON CONFLICT (referrer_id, referral_id) DO NOTHING
                    """,
                    values={"r1": referrer_id, "r2": user_id},
                )

            session_token = await create_session(database, user_id)
    except Exception as exc:
        if _is_email_unique_violation(exc):
            raise HTTPException(status_code=409, detail="Email already registered") from exc
        raise

    return user_id, ref_code, session_token




async def create_session(database: Database, user_id: int) -> str:
    session_token = generate_token(32)
    await database.execute(
        "INSERT INTO user_sessions(token, user_id, expires_at) VALUES(:token, :user_id, NOW() + INTERVAL '30 days')",
        values={"token": session_token, "user_id": user_id},
    )
    return session_token


async def login_user(database: Database, email: str, password: str):
    normalized_email = (email or "").strip().lower()
    row = await database.fetch_one(
        "SELECT id, email, password_hash, ref_code, is_active FROM users WHERE LOWER(email)=:email",
        values={"email": normalized_email},
    )
    if not row:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    user = dict(row)
    if not user["is_active"]:
        raise HTTPException(status_code=403, detail="User disabled")

    if not verify_password(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    await database.execute(
        "DELETE FROM user_sessions WHERE user_id=:uid AND expires_at < NOW()",
        values={"uid": user["id"]},
    )

    session_token = await create_session(database, user["id"])

    return user, session_token


async def get_user_by_session(database: Database, token: str):
    row = await database.fetch_one(
        """
        SELECT u.id, u.email, u.ref_code, u.created_at, u.is_active, u.referrer_id
        FROM user_sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.token=:token AND s.expires_at > NOW()
        """,
        values={"token": token},
    )
    if not row:
        raise HTTPException(status_code=401, detail="Session expired or not found")
    return dict(row)


async def calculate_balance(database: Database, user_id: int) -> int:
    row = await database.fetch_one(
        "SELECT balance FROM users WHERE id=:uid",
        values={"uid": user_id},
    )
    return int(row["balance"] or 0) if row else 0


async def create_reset_token(database: Database, email: str):
    normalized_email = (email or "").strip().lower()
    row = await database.fetch_one("SELECT id FROM users WHERE LOWER(email)=:email", values={"email": normalized_email})
    if not row:
        return None

    user_id = row["id"]
    raw = generate_token(32)
    token_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    expires_at = datetime.utcnow() + timedelta(minutes=RESET_TTL_MINUTES)

    await database.execute(
        """
        INSERT INTO password_resets(token_hash, user_id, expires_at)
        VALUES(:th, :uid, :exp)
        """,
        values={"th": token_hash, "uid": user_id, "exp": expires_at},
    )
    return raw


def smtp_configured() -> bool:
    return bool(SMTP_HOST and SMTP_PORT and SMTP_FROM)


def send_password_reset_email(to_email: str, raw_token: str):
    to_email = (to_email or "").strip().lower()
    if not smtp_configured():
        raise RuntimeError("SMTP is not configured")

    reset_url = f"{RESET_LINK_BASE_URL}?reset_token={raw_token}"

    msg = EmailMessage()
    msg["Subject"] = "Восстановление пароля CodeNext"
    msg["From"] = SMTP_FROM
    msg["To"] = to_email

    text = (
        "Вы запросили восстановление пароля CodeNext.\n\n"
        f"Перейдите по ссылке для сброса пароля:\n{reset_url}\n\n"
        f"Если ссылка не открывается, используйте токен вручную:\n{raw_token}\n\n"
        "Срок действия токена: 30 минут.\n"
        "Если вы не запрашивали сброс, просто проигнорируйте это письмо."
    )
    msg.set_content(text)

    if SMTP_USE_SSL:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context, timeout=20) as server:
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        return

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
        if SMTP_USE_TLS:
            context = ssl.create_default_context()
            server.starttls(context=context)
        if SMTP_USER:
            server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)


async def reset_password(database: Database, raw_token: str, new_password: str):
    token_hash = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
    row = await database.fetch_one(
        """
        SELECT token_hash, user_id, expires_at, used_at
        FROM password_resets
        WHERE token_hash=:th
        """,
        values={"th": token_hash},
    )
    if not row:
        raise HTTPException(status_code=400, detail="Invalid reset token")

    row = dict(row)
    if row["used_at"] is not None or row["expires_at"] < datetime.utcnow():
        raise HTTPException(status_code=400, detail="Reset token expired")

    await database.execute(
        "UPDATE users SET password_hash=:ph WHERE id=:uid",
        values={"ph": hash_password(new_password), "uid": row["user_id"]},
    )
    await database.execute(
        "UPDATE password_resets SET used_at=NOW() WHERE token_hash=:th",
        values={"th": token_hash},
    )
    await database.execute("DELETE FROM user_sessions WHERE user_id=:uid", values={"uid": row["user_id"]})


async def process_referral_bonus(database: Database, user_id: int, order_amount: int, order_id: str, referrer_override: Optional[int] = None):
    if referrer_override:
        ref_check = await database.fetch_one(
            "SELECT id FROM users WHERE id=:rid AND is_active=TRUE",
            values={"rid": int(referrer_override)}
        )
        if not ref_check:
            return 0
        referrer_id = int(referrer_override)
    else:
        row = await database.fetch_one("SELECT referrer_id FROM users WHERE id=:uid", values={"uid": user_id})
        if not row or not row["referrer_id"]:
            return 0
        referrer_id = row["referrer_id"]
    bonus = int(order_amount * REFERRAL_RATE)
    if bonus <= 0:
        return 0

    referral_id_for_meta = int(user_id) if int(user_id) > 0 else None
    meta = json.dumps({"order_id": order_id, "referral_id": referral_id_for_meta}, ensure_ascii=False)

    async with database.transaction():
        row = await database.fetch_one(
            """
            INSERT INTO wallet_transactions(user_id, tx_type, amount, referral_order_id, meta_json)
            VALUES(:uid, 'referral_bonus', :amount, :oid, :meta)
            ON CONFLICT (referral_order_id) WHERE tx_type='referral_bonus' DO NOTHING
            RETURNING id
            """,
            values={"uid": referrer_id, "amount": bonus, "oid": order_id, "meta": meta},
        )
        if not row:
            logging.debug(f"Referral bonus for order {order_id} already exists, skipping")
            return 0

        logging.info(f"Referral bonus {bonus} credited to user {referrer_id} for order {order_id}")

        await database.execute(
            "UPDATE users SET balance = balance + :amount WHERE id=:uid",
            values={"amount": bonus, "uid": referrer_id},
        )

        if referral_id_for_meta is not None:
            await database.execute(
                """
                UPDATE referrals
                SET total_earned = total_earned + :bonus
                WHERE referrer_id=:r1 AND referral_id=:r2
                """,
                values={"bonus": bonus, "r1": referrer_id, "r2": referral_id_for_meta},
            )

        try:
            await database.execute(
                "UPDATE orders SET referral_reward=:bonus WHERE order_id=:oid",
                values={"bonus": bonus, "oid": order_id},
            )
        except Exception:
            pass

    return bonus

async def reject_withdraw_request(database: Database, withdraw_request_id: int) -> bool:
    """Отклоняет заявку на вывод с атомарным возвратом баланса и записью транзакции."""
    async with database.transaction():
        row = await database.fetch_one(
            """
            SELECT id, user_id, amount
            FROM withdraw_requests
            WHERE id=:wid AND status='new'
            FOR UPDATE
            """,
            values={"wid": int(withdraw_request_id)},
        )
        if not row:
            return False

        row = dict(row)

        updated = await database.execute(
            """
            UPDATE withdraw_requests
            SET status='rejected', processed_at=NOW()
            WHERE id=:wid AND status='new'
            """,
            values={"wid": int(withdraw_request_id)},
        )
        if not updated:
            return False

        await database.execute(
            "UPDATE users SET balance = balance + :amount WHERE id=:uid",
            values={"amount": int(row["amount"]), "uid": int(row["user_id"])},
        )

        await database.execute(
            """
            INSERT INTO wallet_transactions(user_id, tx_type, amount, meta_json)
            VALUES(:uid, 'withdraw_rejected', :amount, :meta)
            """,
            values={
                "uid": int(row["user_id"]),
                "amount": int(row["amount"]),
                "meta": json.dumps({"withdraw_request_id": int(withdraw_request_id)}, ensure_ascii=False),
            },
        )

    return True
