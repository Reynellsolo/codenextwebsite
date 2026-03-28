import logging
from databases import Database


async def init_cabinet_schema(database: Database):
    await database.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            ref_code TEXT NOT NULL UNIQUE,
            referrer_id INTEGER REFERENCES users(id),
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            balance INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    await database.execute(
        """
        CREATE TABLE IF NOT EXISTS referrals (
            id SERIAL PRIMARY KEY,
            referrer_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            referral_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            status TEXT NOT NULL DEFAULT 'active',
            total_earned INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE(referrer_id, referral_id)
        )
        """
    )

    await database.execute(
        """
        CREATE TABLE IF NOT EXISTS user_sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMP NOT NULL DEFAULT (NOW() + INTERVAL '30 days')
        )
        """
    )

    await database.execute(
        """
        CREATE TABLE IF NOT EXISTS password_resets (
            token_hash TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            expires_at TIMESTAMP NOT NULL,
            used_at TIMESTAMP
        )
        """
    )

    await database.execute(
        """
        CREATE TABLE IF NOT EXISTS wallet_transactions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            tx_type TEXT NOT NULL,
            amount INTEGER NOT NULL,
            referral_order_id TEXT,
            meta_json TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
        """
    )

    await database.execute(
        """
        CREATE TABLE IF NOT EXISTS withdraw_requests (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            amount INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            note TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMP
        )
        """
    )

    # Indexes for hot paths.
    await database.execute("CREATE INDEX IF NOT EXISTS idx_users_referrer ON users(referrer_id)")
    await database.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")
    await database.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referral ON referrals(referral_id)")
    await database.execute("CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id)")
    await database.execute("CREATE INDEX IF NOT EXISTS idx_user_sessions_expires ON user_sessions(expires_at)")
    await database.execute("CREATE INDEX IF NOT EXISTS idx_password_resets_expires ON password_resets(expires_at)")
    await database.execute("CREATE INDEX IF NOT EXISTS idx_wallet_tx_user ON wallet_transactions(user_id)")
    await database.execute("CREATE INDEX IF NOT EXISTS idx_withdraw_user ON withdraw_requests(user_id)")

    try:
        await database.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)")
        await database.execute("CREATE INDEX IF NOT EXISTS idx_orders_status_created ON orders(status, created_at)")
    except Exception as e:
        logging.warning(f"Failed to create orders indexes: {e}")

    try:
        await database.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS balance INTEGER")
        await database.execute("UPDATE users SET balance = 0 WHERE balance IS NULL")
        await database.execute(
            """
            UPDATE users u
            SET balance = COALESCE((
                SELECT SUM(wt.amount)
                FROM wallet_transactions wt
                WHERE wt.user_id = u.id
            ), 0)
            """
        )
    except Exception as e:
        logging.warning(f"Failed to migrate users.balance: {e}")

    # Optional: link legacy orders to users for cabinet history.
    try:
        await database.execute("ALTER TABLE user_sessions ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP")
        await database.execute("UPDATE user_sessions SET expires_at = NOW() + INTERVAL '30 days' WHERE expires_at IS NULL")
        await database.execute("ALTER TABLE user_sessions ALTER COLUMN expires_at SET DEFAULT (NOW() + INTERVAL '30 days')")
        await database.execute("ALTER TABLE user_sessions ALTER COLUMN expires_at SET NOT NULL")
    except Exception as e:
        logging.warning(f"Failed to migrate user_sessions.expires_at: {e}")

    try:
        await database.execute("ALTER TABLE wallet_transactions ADD COLUMN IF NOT EXISTS referral_order_id TEXT")
        await database.execute(
            """
            UPDATE wallet_transactions
            SET referral_order_id = NULLIF((meta_json::jsonb ->> 'order_id'), '')
            WHERE tx_type='referral_bonus' AND referral_order_id IS NULL
            """
        )
        await database.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_wallet_referral_bonus_order ON wallet_transactions(referral_order_id) WHERE tx_type='referral_bonus' AND referral_order_id IS NOT NULL"
        )
    except Exception as e:
        logging.warning(f"Failed to migrate wallet_transactions referral id/index: {e}")

    try:
        await database.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS user_id INTEGER")
        await database.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS user_email TEXT")
        await database.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS promo_code_used TEXT")
        await database.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS referral_code TEXT")
        await database.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS referral_reward INTEGER DEFAULT 0")
        await database.execute("CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id)")
    except Exception as e:
        # Keep schema bootstrap non-fatal for environments without legacy orders table.
        logging.warning(f"Failed to migrate legacy orders columns: {e}")


async def cleanup_expired_cabinet_data(database: Database):
    try:
        await database.execute("DELETE FROM user_sessions WHERE expires_at < NOW()")
        await database.execute("DELETE FROM password_resets WHERE expires_at < NOW() OR used_at IS NOT NULL")
    except Exception as e:
        logging.warning(f"Failed to cleanup expired cabinet data: {e}")
