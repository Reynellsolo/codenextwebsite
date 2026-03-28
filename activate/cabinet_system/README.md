# Cabinet System (isolated module)

This folder contains a standalone personal-account system you can upload to server now
and integrate into `app.py` later.

## Included
- `models.py` — Pydantic request models
- `security.py` — password/session helpers (Argon2/bcrypt via `passlib` when available)
- `services.py` — business logic
- `schema.py` — DB schema init for new tables
- `router.py` — FastAPI router (`/cabinet/*`)
- `config.py` — env-configurable settings

## New API endpoints
- `POST /cabinet/auth/register`
- `POST /cabinet/auth/login`
- `POST /cabinet/auth/forgot-password`
- `POST /cabinet/auth/reset-password`
- `GET /cabinet/me`
- `GET /cabinet/orders`
- `GET /cabinet/wallet`
- `GET /cabinet/referrals`
- `POST /cabinet/withdraw-request`
- `POST /cabinet/payment-draft`

## Included safeguards
- Basic auth rate limiting for register/login (in-memory per process)
- Referral model with `referrer_id` + `referrals` table
- Self-referral prevention
- Circular referral protection
- Withdrawal pending-lock + immediate hold transaction (`withdraw_pending`)
- Password reset tokens stored as hash (`password_resets.token_hash`)

## Integration later (when you decide)
```python
from cabinet_system.router import router as cabinet_router, set_database as set_cabinet_database
from cabinet_system.schema import init_cabinet_schema

app.include_router(cabinet_router)
set_cabinet_database(database)

@app.on_event("startup")
async def startup():
    await database.connect()
    await init_db()               # your existing init
    await init_cabinet_schema(database)
```

## Environment variables
- `CABINET_PREFIX=/cabinet`
- `CABINET_REFERRAL_RATE=0.10`
- `CABINET_RESET_TTL_MINUTES=30`
- `CABINET_DEBUG_RESET_TOKEN=false`
- `CABINET_AUTH_REGISTER_LIMIT_PER_MINUTE=5`
- `CABINET_AUTH_LOGIN_LIMIT_PER_MINUTE=20`
- `CABINET_SMTP_HOST=smtp.your-provider.com`
- `CABINET_SMTP_PORT=587`
- `CABINET_SMTP_USER=you@example.com`
- `CABINET_SMTP_PASSWORD=...`
- `CABINET_SMTP_FROM=no-reply@codenext.ru`
- `CABINET_SMTP_USE_TLS=true`
- `CABINET_SMTP_USE_SSL=false`
- `CABINET_RESET_LINK_BASE_URL=https://codenext.ru/login`

## Production notes
- Install `passlib[argon2]` to use Argon2/bcrypt password hashing.
- Keep `CABINET_DEBUG_RESET_TOKEN=false` in production and send reset emails via SMTP/provider.
- For distributed rate-limits, prefer Nginx/Cloudflare or Redis-based limiter.
