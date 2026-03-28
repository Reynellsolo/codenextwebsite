from flask import (
    Flask,
    Blueprint,
    render_template_string,
    request,
    jsonify,
    redirect,
    url_for,
    session,
    flash,
)
import os
import sqlite3
import psycopg2
from psycopg2.extras import RealDictCursor
from functools import wraps
from datetime import datetime
import secrets

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "change-this-secret-key")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "zZ282001282001")

admin = Blueprint("admin", __name__, url_prefix="/control-7f2p-admin-91")


# =========================
# CONFIG
# =========================
BOTS = {
    "activate": {
        "name": "🚀 Activate Bot",
        "type": "postgresql",
        "db_name": "activate_db",
        "db_user": "activateuser",
        "db_password": os.getenv("ACTIVATE_DB_PASSWORD", ""),
        "db_host": os.getenv("ACTIVATE_DB_HOST", "localhost"),
        "db_port": int(os.getenv("ACTIVATE_DB_PORT", "5432")),
        "public_base_url": os.getenv("PUBLIC_BASE_URL", "https://codenext.ru"),
        "tables": {
            "promo_codes": {"name": "Промокоды", "addable": True, "deletable": True},
            "activation_links": {"name": "Ссылки активации", "addable": True, "deletable": True},
            "orders": {"name": "Заказы", "addable": False, "deletable": False},
            "accounts": {"name": "Аккаунты", "addable": True, "deletable": True},
        },
        "products": {
            "plus_1m": "ChatGPT Plus · 1 месяц",
            "go_12m": "ChatGPT GO · 12 месяцев",
            "plus_12m": "ChatGPT Plus · 12 месяцев",
            "plus_account": "ChatGPT Plus · Аккаунт",
        },
    },
    "market": {
        "name": "🛒 Market Bot 1",
        "type": "sqlite",
        "db_path": "/opt/market-bot/keys.db",
        "tables": {
            "keys": {"name": "Ключи", "addable": True, "deletable": True},
            "skus": {"name": "Товары (SKU)", "addable": True, "deletable": True},
            "processed_orders": {"name": "Обработанные заказы", "addable": False, "deletable": False},
            "sku_groups": {"name": "Группы SKU", "addable": False, "deletable": True},
            "hidden_skus": {"name": "Скрытые SKU", "addable": True, "deletable": True},
        },
    },
    "market2": {
        "name": "🛒 Market Bot 2",
        "type": "sqlite",
        "db_path": "/opt/market-bot-2/keys.db",
        "tables": {
            "keys": {"name": "Ключи", "addable": True, "deletable": True},
            "skus": {"name": "Товары (SKU)", "addable": True, "deletable": True},
            "processed_orders": {"name": "Обработанные заказы", "addable": False, "deletable": False},
            "sku_groups": {"name": "Группы SKU", "addable": False, "deletable": True},
            "hidden_skus": {"name": "Скрытые SKU", "addable": True, "deletable": True},
        },
    },
    "market3": {
        "name": "🛒 Market Bot 3",
        "type": "sqlite",
        "db_path": "/opt/market-bot-3/keys.db",
        "tables": {
            "keys": {"name": "Ключи", "addable": True, "deletable": True},
            "skus": {"name": "Товары (SKU)", "addable": True, "deletable": True},
            "processed_orders": {"name": "Обработанные заказы", "addable": False, "deletable": False},
            "sku_groups": {"name": "Группы SKU", "addable": False, "deletable": True},
            "hidden_skus": {"name": "Скрытые SKU", "addable": True, "deletable": True},
        },
    },
}

PAGE_SIZE = 50


# =========================
# AUTH
# =========================
def login_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not session.get("admin_logged_in"):
            return redirect(url_for("admin.login"))
        return func(*args, **kwargs)
    return wrapper


# =========================
# DB HELPERS
# =========================
def get_db_connection(bot_id):
    bot = BOTS[bot_id]
    if bot["type"] == "postgresql":
        conn = psycopg2.connect(
            dbname=bot["db_name"],
            user=bot["db_user"],
            password=bot.get("db_password", ""),
            host=bot.get("db_host", "localhost"),
            port=bot.get("db_port", 5432),
            cursor_factory=RealDictCursor,
        )
        return conn
    conn = sqlite3.connect(bot["db_path"])
    conn.row_factory = sqlite3.Row
    return conn


def select_all(bot_id, query, params=None):
    conn = get_db_connection(bot_id)
    try:
        cur = conn.cursor()
        cur.execute(query, params or ())
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def execute_write(bot_id, query, params=None):
    conn = get_db_connection(bot_id)
    try:
        cur = conn.cursor()
        cur.execute(query, params or ())
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def execute_many(bot_id, query, params_list):
    conn = get_db_connection(bot_id)
    try:
        cur = conn.cursor()
        for params in params_list:
            cur.execute(query, params)
        conn.commit()
        return len(params_list)
    finally:
        conn.close()


def select_one(bot_id, query, params=None):
    conn = get_db_connection(bot_id)
    try:
        cur = conn.cursor()
        cur.execute(query, params or ())
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def select_one_value(bot_id, query, params=None):
    conn = get_db_connection(bot_id)
    try:
        cur = conn.cursor()
        cur.execute(query, params or ())
        row = cur.fetchone()
        if row is None:
            return 0
        if isinstance(row, dict):
            return list(row.values())[0]
        return row[0]
    finally:
        conn.close()


def get_table_columns(bot_id, table_name):
    bot = BOTS[bot_id]
    conn = get_db_connection(bot_id)
    try:
        cur = conn.cursor()
        if bot["type"] == "postgresql":
            cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
            return [desc.name if hasattr(desc, "name") else desc[0] for desc in cur.description]
        else:
            cur.execute(f"PRAGMA table_info({table_name})")
            rows = cur.fetchall()
            return [row["name"] for row in rows]
    finally:
        conn.close()


def get_placeholder(bot_id):
    return "%s" if BOTS[bot_id]["type"] == "postgresql" else "?"


def safe_get_bot_and_table(bot_id, table_name):
    if bot_id not in BOTS:
        return None, None
    bot = BOTS[bot_id]
    if table_name not in bot["tables"]:
        return bot, None
    return bot, bot["tables"][table_name]


# =========================
# STATISTICS HELPERS
# =========================
def get_activate_stats(bot_id):
    """Get detailed statistics for Activate Bot"""
    stats = []
    ph = get_placeholder(bot_id)
    
    # Promo codes stats by product
    products = BOTS[bot_id].get("products", {})
    for prod_id, prod_name in products.items():
        if prod_id == "plus_account":
            continue
        free = select_one_value(bot_id, f"SELECT COUNT(*) FROM promo_codes WHERE product = {ph} AND status = 'free'", (prod_id,))
        used = select_one_value(bot_id, f"SELECT COUNT(*) FROM promo_codes WHERE product = {ph} AND status = 'used'", (prod_id,))
        stats.append({"title": f"🎫 {prod_name}", "free": free, "used": used, "total": free + used})
    
    # Accounts stats
    free_acc = select_one_value(bot_id, "SELECT COUNT(*) FROM accounts WHERE status = 'free'", ())
    used_acc = select_one_value(bot_id, "SELECT COUNT(*) FROM accounts WHERE status = 'used'", ())
    stats.append({"title": "👤 Аккаунты", "free": free_acc, "used": used_acc, "total": free_acc + used_acc})
    
    # Links stats
    active_links = select_one_value(bot_id, "SELECT COUNT(*) FROM activation_links WHERE status = 'active'", ())
    used_links = select_one_value(bot_id, "SELECT COUNT(*) FROM activation_links WHERE status = 'used'", ())
    blocked_links = select_one_value(bot_id, "SELECT COUNT(*) FROM activation_links WHERE status = 'blocked'", ())
    stats.append({"title": "🔗 Ссылки активации", "active": active_links, "used": used_links, "blocked": blocked_links})
    
    return stats


def get_market_stats(bot_id):
    """Get detailed statistics for Market Bot"""
    stats = []
    
    # Get all SKUs with counts
    rows = select_all(bot_id, """
        SELECT 
            k.sku,
            s.title,
            SUM(CASE WHEN k.status='free' THEN 1 ELSE 0 END) AS free_cnt,
            SUM(CASE WHEN k.status='used' THEN 1 ELSE 0 END) AS used_cnt
        FROM keys k
        LEFT JOIN skus s ON s.sku = k.sku
        LEFT JOIN hidden_skus h ON h.sku = k.sku
        WHERE h.sku IS NULL
        GROUP BY k.sku
        ORDER BY k.sku
    """)
    
    for r in rows:
        sku = r.get("sku", "")
        title = r.get("title") or sku
        label = f"{title} ({sku})" if title != sku else sku
        stats.append({
            "title": f"🎮 {label}",
            "free": r.get("free_cnt", 0),
            "used": r.get("used_cnt", 0),
            "total": r.get("free_cnt", 0) + r.get("used_cnt", 0)
        })
    
    # Total processed orders
    total_orders = select_one_value(bot_id, "SELECT COUNT(*) FROM processed_orders", ())
    stats.append({"title": "📦 Обработано заказов", "value": total_orders})
    
    return stats


# =========================
# TEMPLATE
# =========================
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Вход в админку</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
        }
        .card {
            width: 100%;
            max-width: 420px;
            background: white;
            border-radius: 22px;
            box-shadow: 0 20px 60px rgba(0,0,0,.25);
            padding: 34px;
        }
        h1 { font-size: 28px; margin-bottom: 10px; color: #222; }
        p { color: #666; margin-bottom: 24px; }
        input, button {
            width: 100%;
            padding: 14px 16px;
            border-radius: 12px;
            font-size: 15px;
            font-family: inherit;
        }
        input { border: 2px solid #e7e7e7; margin-bottom: 14px; outline: none; }
        input:focus { border-color: #667eea; }
        button {
            border: none;
            background: #667eea;
            color: white;
            font-weight: 700;
            cursor: pointer;
        }
        button:hover { background: #5568d3; }
        .error {
            background: #fde8e8;
            color: #9b1c1c;
            padding: 12px 14px;
            border-radius: 12px;
            margin-bottom: 16px;
            font-size: 14px;
        }
    </style>
</head>
<body>
    <div class="card">
        <h1>🔐 Вход в админку</h1>
        <p>Введите пароль администратора</p>
        {% with messages = get_flashed_messages() %}
            {% if messages %}
                {% for msg in messages %}
                    <div class="error">{{ msg }}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}
        <form method="POST">
            <input type="password" name="password" placeholder="Пароль" required>
            <button type="submit">Войти</button>
        </form>
    </div>
</body>
</html>
"""


ADMIN_TEMPLATE = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Админ-панель</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1600px;
            margin: 0 auto;
            background: white;
            border-radius: 24px;
            box-shadow: 0 20px 60px rgba(0,0,0,.25);
            overflow: hidden;
        }
        
        /* Header */
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 24px 30px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 20px;
            flex-wrap: wrap;
        }
        .header h1 { font-size: 26px; font-weight: 800; }
        .header p { opacity: 0.9; font-size: 14px; margin-top: 4px; }
        .logout {
            background: rgba(255,255,255,.15);
            color: white;
            text-decoration: none;
            padding: 10px 16px;
            border-radius: 12px;
            font-weight: 700;
            transition: background 0.2s;
        }
        .logout:hover { background: rgba(255,255,255,.25); }
        
        /* Navigation */
        .nav-tabs {
            display: flex;
            gap: 0;
            background: #f8f9fb;
            border-bottom: 1px solid #eceef3;
            overflow-x: auto;
        }
        .nav-tab {
            padding: 16px 24px;
            font-size: 14px;
            font-weight: 600;
            color: #666;
            text-decoration: none;
            border-bottom: 3px solid transparent;
            white-space: nowrap;
            transition: all 0.2s;
        }
        .nav-tab:hover { color: #667eea; background: rgba(102,126,234,.05); }
        .nav-tab.active {
            color: #667eea;
            border-bottom-color: #667eea;
            background: white;
        }
        
        /* Controls */
        .controls {
            padding: 20px 30px;
            background: #fafbfc;
            border-bottom: 1px solid #eceef3;
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
            align-items: center;
        }
        select, input[type="text"], input[type="search"], button, a.btn {
            padding: 11px 16px;
            border-radius: 10px;
            border: 2px solid #e5e7eb;
            font-size: 14px;
            font-family: inherit;
        }
        select, input[type="text"], input[type="search"] {
            background: white;
            min-width: 200px;
        }
        input[type="search"] { min-width: 280px; }
        button, a.btn {
            border: none;
            background: #667eea;
            color: white;
            font-weight: 600;
            cursor: pointer;
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            transition: all 0.2s;
        }
        button:hover, a.btn:hover { background: #5568d3; transform: translateY(-1px); }
        .btn-success { background: #10b981 !important; }
        .btn-success:hover { background: #059669 !important; }
        .btn-warning { background: #f59e0b !important; }
        .btn-warning:hover { background: #d97706 !important; }
        .btn-danger { background: #ef4444 !important; }
        .btn-danger:hover { background: #dc2626 !important; }
        .btn-secondary { background: #6b7280 !important; }
        .btn-secondary:hover { background: #4b5563 !important; }
        .btn-outline {
            background: white !important;
            color: #667eea !important;
            border: 2px solid #667eea !important;
        }
        .btn-outline:hover { background: #667eea !important; color: white !important; }
        
        /* Content */
        .content { padding: 24px 30px; }
        
        /* Stats Grid */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }
        .stat-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 16px;
            box-shadow: 0 8px 24px rgba(102,126,234,.2);
        }
        .stat-card h3 {
            font-size: 13px;
            opacity: 0.9;
            margin-bottom: 12px;
            font-weight: 600;
        }
        .stat-card .values {
            display: flex;
            gap: 16px;
            flex-wrap: wrap;
        }
        .stat-card .value-item {
            text-align: center;
        }
        .stat-card .value {
            font-size: 28px;
            font-weight: 800;
            display: block;
        }
        .stat-card .label {
            font-size: 11px;
            opacity: 0.8;
            text-transform: uppercase;
        }
        .stat-card.green { background: linear-gradient(135deg, #10b981 0%, #059669 100%); }
        .stat-card.orange { background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%); }
        .stat-card.red { background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%); }
        
        /* Table */
        .table-wrap {
            overflow-x: auto;
            border-radius: 16px;
            border: 1px solid #eef0f4;
            margin-bottom: 20px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
        }
        thead { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; }
        th, td {
            padding: 12px 16px;
            text-align: left;
            vertical-align: middle;
        }
        th {
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 700;
        }
        tbody tr { border-bottom: 1px solid #f1f3f7; transition: background 0.15s; }
        tbody tr:hover { background: #fafbff; }
        .badge {
            display: inline-block;
            padding: 5px 10px;
            border-radius: 999px;
            font-size: 11px;
            font-weight: 700;
        }
        .badge.free, .badge.active { background: #d1fae5; color: #065f46; }
        .badge.used { background: #fee2e2; color: #991b1b; }
        .badge.blocked { background: #fef3c7; color: #92400e; }
        .badge.other { background: #e5e7eb; color: #374151; }
        .actions { display: flex; gap: 6px; }
        .btn-small {
            padding: 6px 10px;
            border-radius: 8px;
            font-size: 11px;
        }
        .text-muted { color: #9ca3af; }
        .text-truncate {
            max-width: 200px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        
        /* Pagination */
        .pagination {
            display: flex;
            gap: 8px;
            align-items: center;
            justify-content: center;
            margin-top: 20px;
        }
        .pagination button {
            padding: 8px 14px;
        }
        .pagination span {
            color: #666;
            font-size: 14px;
        }
        
        /* Empty state */
        .empty {
            text-align: center;
            padding: 60px 20px;
            color: #8a8f98;
        }
        .empty .icon { font-size: 48px; margin-bottom: 16px; }
        
        /* Flash messages */
        .flash {
            margin-bottom: 16px;
            padding: 14px 16px;
            border-radius: 12px;
            font-size: 14px;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .flash.error { background: #fde8e8; color: #9b1c1c; }
        .flash.success { background: #d1fae5; color: #065f46; }
        .flash.info { background: #dbeafe; color: #1e40af; }
        
        /* Modal */
        .modal {
            display: none;
            position: fixed;
            inset: 0;
            background: rgba(0,0,0,.5);
            z-index: 1000;
            align-items: center;
            justify-content: center;
            padding: 20px;
            backdrop-filter: blur(4px);
        }
        .modal.active { display: flex; }
        .modal-content {
            width: 100%;
            max-width: 600px;
            max-height: 90vh;
            overflow-y: auto;
            background: white;
            border-radius: 20px;
            padding: 28px;
            box-shadow: 0 25px 80px rgba(0,0,0,.3);
        }
        .modal-content h2 {
            margin-bottom: 20px;
            color: #222;
            font-size: 22px;
        }
        .form-group { margin-bottom: 16px; }
        .form-group label {
            display: block;
            margin-bottom: 6px;
            font-size: 13px;
            font-weight: 600;
            color: #444;
        }
        .form-group input,
        .form-group select,
        .form-group textarea {
            width: 100%;
            padding: 12px 14px;
            border: 2px solid #e5e7eb;
            border-radius: 10px;
            font-size: 14px;
            font-family: inherit;
            transition: border-color 0.2s;
        }
        .form-group input:focus,
        .form-group select:focus,
        .form-group textarea:focus {
            outline: none;
            border-color: #667eea;
        }
        .form-group textarea {
            min-height: 120px;
            resize: vertical;
        }
        .form-group small {
            display: block;
            margin-top: 4px;
            font-size: 12px;
            color: #888;
        }
        .form-row {
            display: flex;
            gap: 12px;
            margin-top: 20px;
        }
        .form-row > * { flex: 1; }
        
        /* Tabs inside content */
        .content-tabs {
            display: flex;
            gap: 8px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }
        .content-tab {
            padding: 10px 18px;
            border-radius: 10px;
            font-size: 13px;
            font-weight: 600;
            color: #666;
            background: #f3f4f6;
            cursor: pointer;
            border: none;
            transition: all 0.2s;
        }
        .content-tab:hover { background: #e5e7eb; }
        .content-tab.active { background: #667eea; color: white; }
        
        /* Quick actions panel */
        .quick-actions {
            background: #f8f9fb;
            border-radius: 16px;
            padding: 20px;
            margin-bottom: 24px;
        }
        .quick-actions h3 {
            font-size: 14px;
            color: #444;
            margin-bottom: 14px;
        }
        .quick-actions-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 10px;
        }
        .quick-action-btn {
            padding: 14px 16px;
            border-radius: 12px;
            font-size: 13px;
            font-weight: 600;
            border: none;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 8px;
            background: white;
            color: #333;
            box-shadow: 0 2px 8px rgba(0,0,0,.06);
            transition: all 0.2s;
        }
        .quick-action-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 16px rgba(0,0,0,.1);
        }
        .quick-action-btn .icon { font-size: 18px; }
        
        /* Search results highlight */
        .highlight {
            background: #fef3c7;
            padding: 2px 4px;
            border-radius: 4px;
        }
        
        /* Responsive */
        @media (max-width: 768px) {
            .controls { flex-direction: column; align-items: stretch; }
            select, input[type="text"], input[type="search"] { min-width: 100%; }
            .stats-grid { grid-template-columns: 1fr; }
            .quick-actions-grid { grid-template-columns: 1fr 1fr; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div>
                <h1>🎛️ Админ-панель</h1>
                <p>Управление ботами и базами данных</p>
            </div>
            <a class="logout" href="{{ url_for('admin.logout') }}">🚪 Выйти</a>
        </div>

        <!-- Bot Navigation -->
        <div class="nav-tabs">
            {% for bot_id, bot in bots.items() %}
                <a href="{{ url_for('admin.index', bot=bot_id) }}" 
                   class="nav-tab {% if bot_id == current_bot %}active{% endif %}">
                    {{ bot.name }}
                </a>
            {% endfor %}
        </div>

        {% if current_bot %}
        <div class="controls">
            <select id="tableSelect" onchange="loadTable()">
                <option value="">📋 Выберите таблицу...</option>
                {% for table_id, table in bots[current_bot].tables.items() %}
                    <option value="{{ table_id }}" {% if table_id == current_table %}selected{% endif %}>
                        {{ table.name }}
                    </option>
                {% endfor %}
            </select>
            
            {% if current_table %}
                <input type="search" id="searchInput" placeholder="🔍 Поиск..." 
                       value="{{ search_query }}" onkeyup="handleSearch(event)">
                
                <a class="btn btn-outline" href="{{ url_for('admin.index', bot=current_bot, table=current_table) }}">
                    🔄 Обновить
                </a>
            {% endif %}
        </div>
        {% endif %}

        <div class="content">
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    {% for category, msg in messages %}
                        <div class="flash {{ category }}">{{ msg }}</div>
                    {% endfor %}
                {% endif %}
            {% endwith %}

            {% if current_bot and not current_table %}
                <!-- Dashboard View -->
                <h2 style="margin-bottom: 20px;">📊 Статистика: {{ bots[current_bot].name }}</h2>
                
                {% if detailed_stats %}
                <div class="stats-grid">
                    {% for stat in detailed_stats %}
                        <div class="stat-card {% if stat.free is defined and stat.free == 0 %}red{% elif stat.free is defined and stat.free < 5 %}orange{% endif %}">
                            <h3>{{ stat.title }}</h3>
                            <div class="values">
                                {% if stat.value is defined %}
                                    <div class="value-item">
                                        <span class="value">{{ stat.value }}</span>
                                        <span class="label">Всего</span>
                                    </div>
                                {% else %}
                                    {% if stat.free is defined %}
                                    <div class="value-item">
                                        <span class="value">{{ stat.free }}</span>
                                        <span class="label">Свободно</span>
                                    </div>
                                    {% endif %}
                                    {% if stat.used is defined %}
                                    <div class="value-item">
                                        <span class="value">{{ stat.used }}</span>
                                        <span class="label">Использ.</span>
                                    </div>
                                    {% endif %}
                                    {% if stat.active is defined %}
                                    <div class="value-item">
                                        <span class="value">{{ stat.active }}</span>
                                        <span class="label">Активно</span>
                                    </div>
                                    {% endif %}
                                    {% if stat.blocked is defined %}
                                    <div class="value-item">
                                        <span class="value">{{ stat.blocked }}</span>
                                        <span class="label">Заблок.</span>
                                    </div>
                                    {% endif %}
                                    {% if stat.total is defined %}
                                    <div class="value-item">
                                        <span class="value">{{ stat.total }}</span>
                                        <span class="label">Всего</span>
                                    </div>
                                    {% endif %}
                                {% endif %}
                            </div>
                        </div>
                    {% endfor %}
                </div>
                {% endif %}

                <!-- Quick Actions -->
                <div class="quick-actions">
                    <h3>⚡ Быстрые действия</h3>
                    <div class="quick-actions-grid">
                        {% if current_bot == 'activate' %}
                            <button class="quick-action-btn" onclick="openModal('createLinksModal')">
                                <span class="icon">🔗</span> Создать ссылки
                            </button>
                            <button class="quick-action-btn" onclick="openModal('uploadCodesModal')">
                                <span class="icon">📥</span> Загрузить коды
                            </button>
                            <button class="quick-action-btn" onclick="openModal('checkLinkModal')">
                                <span class="icon">🔍</span> Проверить ссылку
                            </button>
                            <button class="quick-action-btn" onclick="openModal('findOrderModal')">
                                <span class="icon">📦</span> Найти заказ
                            </button>
                        {% else %}
                            <button class="quick-action-btn" onclick="openModal('addSkuModal')">
                                <span class="icon">➕</span> Добавить SKU
                            </button>
                            <button class="quick-action-btn" onclick="openModal('uploadKeysModal')">
                                <span class="icon">🔑</span> Загрузить ключи
                            </button>
                            <button class="quick-action-btn" onclick="openModal('findOrderKeysModal')">
                                <span class="icon">🔍</span> Ключи по заказу
                            </button>
                            <button class="quick-action-btn" onclick="openModal('hideSkuModal')">
                                <span class="icon">👁️</span> Скрыть/Показать SKU
                            </button>
                        {% endif %}
                    </div>
                </div>
            {% elif current_bot and current_table %}
                <!-- Table View -->
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; flex-wrap: wrap; gap: 12px;">
                    <h2>{{ bots[current_bot].tables[current_table].name }}</h2>
                    <div style="display: flex; gap: 10px; flex-wrap: wrap;">
                        {% if table_addable %}
                            <button onclick="openModal('addRecordModal')">➕ Добавить</button>
                        {% endif %}
                        {% if current_table in ['keys', 'promo_codes'] %}
                            <button class="btn-success" onclick="openModal('bulkAddModal')">📥 Массовая загрузка</button>
                        {% endif %}
                    </div>
                </div>

                <!-- Stats for current table -->
                {% if stats %}
                <div class="stats-grid" style="margin-bottom: 24px;">
                    {% for stat in stats %}
                        <div class="stat-card {% if loop.index == 1 %}green{% elif loop.index == 2 %}orange{% endif %}">
                            <h3>{{ stat.title }}</h3>
                            <div class="values">
                                <div class="value-item">
                                    <span class="value">{{ stat.value }}</span>
                                </div>
                            </div>
                        </div>
                    {% endfor %}
                </div>
                {% endif %}

                {% if data %}
                    <div class="table-wrap">
                        <table>
                            <thead>
                                <tr>
                                    {% for col in columns %}
                                        <th>{{ col }}</th>
                                    {% endfor %}
                                    {% if deletable %}
                                        <th style="width: 100px;">Действия</th>
                                    {% endif %}
                                </tr>
                            </thead>
                            <tbody>
                                {% for row in data %}
                                    <tr>
                                        {% for col in columns %}
                                            <td>
                                                {% set val = row.get(col) %}
                                                {% if col == 'status' %}
                                                    <span class="badge {% if val == 'free' or val == 'active' %}free{% elif val == 'used' %}used{% elif val == 'blocked' %}blocked{% else %}other{% endif %}">
                                                        {{ val or '—' }}
                                                    </span>
                                                {% elif col in ['created_at', 'used_at', 'paid_at'] and val %}
                                                    <span class="text-muted">{{ val }}</span>
                                                {% elif col in ['token', 'license_key', 'code', 'cdk_code', 'data'] %}
                                                    <code class="text-truncate" title="{{ val }}">{{ val[:50] }}{% if val and val|length > 50 %}...{% endif %}</code>
                                                {% else %}
                                                    {{ val if val not in [None, ''] else '—' }}
                                                {% endif %}
                                            </td>
                                        {% endfor %}
                                        {% if deletable %}
                                        <td>
                                            <div class="actions">
                                                <button class="btn-small btn-danger" onclick="deleteRow('{{ row[id_column] }}')">
                                                    🗑️
                                                </button>
                                            </div>
                                        </td>
                                        {% endif %}
                                    </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>

                    <!-- Pagination -->
                    {% if total_pages > 1 %}
                    <div class="pagination">
                        {% if current_page > 1 %}
                            <button onclick="goToPage({{ current_page - 1 }})">← Назад</button>
                        {% endif %}
                        <span>Страница {{ current_page }} из {{ total_pages }} (всего: {{ total_count }})</span>
                        {% if current_page < total_pages %}
                            <button onclick="goToPage({{ current_page + 1 }})">Вперёд →</button>
                        {% endif %}
                    </div>
                    {% endif %}
                {% else %}
                    <div class="empty">
                        <div class="icon">📭</div>
                        <p>В этой таблице пока нет данных</p>
                    </div>
                {% endif %}
            {% else %}
                <div class="empty">
                    <div class="icon">👆</div>
                    <p>Выберите бота для начала работы</p>
                </div>
            {% endif %}
        </div>
    </div>

    <!-- MODALS -->
    
    <!-- Add Record Modal (универсальный) -->
    <div id="addRecordModal" class="modal">
        <div class="modal-content">
            <h2>➕ Добавить запись</h2>
            <form method="POST" action="{{ url_for('admin.add_record') }}">
                <input type="hidden" name="bot" value="{{ current_bot }}">
                <input type="hidden" name="table" value="{{ current_table }}">

                {% if current_table == 'promo_codes' %}
                    <div class="form-group">
                        <label>Код</label>
                        <input type="text" name="code" required placeholder="Введите промокод">
                    </div>
                    <div class="form-group">
                        <label>Тариф</label>
                        <select name="product" required>
                            {% for pid, pname in bots[current_bot].get('products', {}).items() %}
                                {% if pid != 'plus_account' %}
                                <option value="{{ pid }}">{{ pname }}</option>
                                {% endif %}
                            {% endfor %}
                        </select>
                    </div>
                {% elif current_table == 'accounts' %}
                    <div class="form-group">
                        <label>Данные аккаунта</label>
                        <input type="text" name="data" required placeholder="email:password или другой формат">
                    </div>
                {% elif current_table == 'activation_links' %}
                    <div class="form-group">
                        <label>Тариф</label>
                        <select name="product" required>
                            {% for pid, pname in bots[current_bot].get('products', {}).items() %}
                            <option value="{{ pid }}">{{ pname }}</option>
                            {% endfor %}
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Количество ссылок</label>
                        <input type="number" name="count" value="1" min="1" max="500" required>
                    </div>
                {% elif current_table == 'keys' %}
                    <div class="form-group">
                        <label>SKU</label>
                        <input type="text" name="sku" required placeholder="MRKT-XXXXX">
                    </div>
                    <div class="form-group">
                        <label>Ключ</label>
                        <input type="text" name="license_key" required>
                    </div>
                {% elif current_table == 'skus' %}
                    <div class="form-group">
                        <label>SKU</label>
                        <input type="text" name="sku" required placeholder="MRKT-XXXXX">
                    </div>
                    <div class="form-group">
                        <label>Название (опционально)</label>
                        <input type="text" name="title" placeholder="Название товара">
                    </div>
                {% elif current_table == 'hidden_skus' %}
                    <div class="form-group">
                        <label>SKU для скрытия</label>
                        <input type="text" name="sku" required placeholder="MRKT-XXXXX">
                    </div>
                {% endif %}

                <div class="form-row">
                    <button type="submit">Добавить</button>
                    <button type="button" class="btn-secondary" onclick="closeModal('addRecordModal')">Отмена</button>
                </div>
            </form>
        </div>
    </div>

    <!-- Bulk Add Modal -->
    <div id="bulkAddModal" class="modal">
        <div class="modal-content">
            <h2>📥 Массовая загрузка</h2>
            <form method="POST" action="{{ url_for('admin.bulk_add') }}">
                <input type="hidden" name="bot" value="{{ current_bot }}">
                <input type="hidden" name="table" value="{{ current_table }}">

                {% if current_table == 'keys' %}
                    <div class="form-group">
                        <label>SKU</label>
                        <input type="text" name="sku" required placeholder="MRKT-XXXXX">
                    </div>
                    <div class="form-group">
                        <label>Ключи (каждый с новой строки)</label>
                        <textarea name="items" rows="10" required placeholder="KEY-001&#10;KEY-002&#10;KEY-003"></textarea>
                        <small>Каждый ключ на отдельной строке</small>
                    </div>
                {% elif current_table == 'promo_codes' %}
                    <div class="form-group">
                        <label>Тариф</label>
                        <select name="product" required>
                            {% for pid, pname in bots[current_bot].get('products', {}).items() %}
                                {% if pid != 'plus_account' %}
                                <option value="{{ pid }}">{{ pname }}</option>
                                {% endif %}
                            {% endfor %}
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Коды (каждый с новой строки)</label>
                        <textarea name="items" rows="10" required placeholder="CODE-001&#10;CODE-002&#10;CODE-003"></textarea>
                        <small>Каждый код на отдельной строке</small>
                    </div>
                {% endif %}

                <div class="form-row">
                    <button type="submit" class="btn-success">Загрузить</button>
                    <button type="button" class="btn-secondary" onclick="closeModal('bulkAddModal')">Отмена</button>
                </div>
            </form>
        </div>
    </div>

    <!-- Activate Bot Modals -->
    {% if current_bot == 'activate' %}
    
    <!-- Create Links Modal -->
    <div id="createLinksModal" class="modal">
        <div class="modal-content">
            <h2>🔗 Создать ссылки активации</h2>
            <form method="POST" action="{{ url_for('admin.create_links') }}">
                <input type="hidden" name="bot" value="{{ current_bot }}">
                <div class="form-group">
                    <label>Тариф</label>
                    <select name="product" required>
                        {% for pid, pname in bots[current_bot].get('products', {}).items() %}
                        <option value="{{ pid }}">{{ pname }}</option>
                        {% endfor %}
                    </select>
                </div>
                <div class="form-group">
                    <label>Количество</label>
                    <input type="number" name="count" value="10" min="1" max="500" required>
                </div>
                <div class="form-row">
                    <button type="submit" class="btn-success">Создать</button>
                    <button type="button" class="btn-secondary" onclick="closeModal('createLinksModal')">Отмена</button>
                </div>
            </form>
        </div>
    </div>

    <!-- Upload Codes Modal -->
    <div id="uploadCodesModal" class="modal">
        <div class="modal-content">
            <h2>📥 Загрузить коды/аккаунты</h2>
            <form method="POST" action="{{ url_for('admin.upload_codes') }}">
                <input type="hidden" name="bot" value="{{ current_bot }}">
                <div class="form-group">
                    <label>Тариф</label>
                    <select name="product" required id="uploadProduct" onchange="updateUploadHint()">
                        {% for pid, pname in bots[current_bot].get('products', {}).items() %}
                        <option value="{{ pid }}">{{ pname }}</option>
                        {% endfor %}
                    </select>
                </div>
                <div class="form-group">
                    <label>Коды (каждый с новой строки)</label>
                    <textarea name="codes" rows="10" required placeholder="CODE-001&#10;CODE-002"></textarea>
                    <small id="uploadHint">Каждый код на отдельной строке</small>
                </div>
                <div class="form-row">
                    <button type="submit" class="btn-success">Загрузить</button>
                    <button type="button" class="btn-secondary" onclick="closeModal('uploadCodesModal')">Отмена</button>
                </div>
            </form>
        </div>
    </div>

    <!-- Check Link Modal -->
    <div id="checkLinkModal" class="modal">
        <div class="modal-content">
            <h2>🔍 Проверить ссылку</h2>
            <form id="checkLinkForm" onsubmit="checkLink(event)">
                <div class="form-group">
                    <label>Токен или ссылка</label>
                    <input type="text" id="checkLinkInput" required placeholder="https://... или токен">
                </div>
                <div class="form-row">
                    <button type="submit">Проверить</button>
                    <button type="button" class="btn-secondary" onclick="closeModal('checkLinkModal')">Закрыть</button>
                </div>
            </form>
            <div id="checkLinkResult" style="margin-top: 20px;"></div>
        </div>
    </div>

    <!-- Find Order Modal -->
    <div id="findOrderModal" class="modal">
        <div class="modal-content">
            <h2>📦 Найти заказ</h2>
            <form id="findOrderForm" onsubmit="findOrder(event)">
                <div class="form-group">
                    <label>Order ID</label>
                    <input type="text" id="findOrderInput" required placeholder="ID заказа">
                </div>
                <div class="form-row">
                    <button type="submit">Найти</button>
                    <button type="button" class="btn-secondary" onclick="closeModal('findOrderModal')">Закрыть</button>
                </div>
            </form>
            <div id="findOrderResult" style="margin-top: 20px;"></div>
        </div>
    </div>
    {% endif %}

    <!-- Market Bot Modals -->
    {% if current_bot and current_bot.startswith('market') %}
    
    <!-- Add SKU Modal -->
    <div id="addSkuModal" class="modal">
        <div class="modal-content">
            <h2>➕ Добавить SKU</h2>
            <form method="POST" action="{{ url_for('admin.add_sku') }}">
                <input type="hidden" name="bot" value="{{ current_bot }}">
                <div class="form-group">
                    <label>SKU</label>
                    <input type="text" name="sku" required placeholder="MRKT-XXXXX">
                </div>
                <div class="form-group">
                    <label>Название (опционально)</label>
                    <input type="text" name="title" placeholder="Название товара">
                </div>
                <div class="form-row">
                    <button type="submit" class="btn-success">Добавить</button>
                    <button type="button" class="btn-secondary" onclick="closeModal('addSkuModal')">Отмена</button>
                </div>
            </form>
        </div>
    </div>

    <!-- Upload Keys Modal -->
    <div id="uploadKeysModal" class="modal">
        <div class="modal-content">
            <h2>🔑 Загрузить ключи</h2>
            <form method="POST" action="{{ url_for('admin.upload_keys') }}">
                <input type="hidden" name="bot" value="{{ current_bot }}">
                <div class="form-group">
                    <label>SKU</label>
                    <select name="sku" required id="uploadKeysSku">
                        <option value="">Выберите SKU...</option>
                        {% if sku_list %}
                            {% for sku in sku_list %}
                            <option value="{{ sku }}">{{ sku }}</option>
                            {% endfor %}
                        {% endif %}
                    </select>
                    <small>Или введите новый:</small>
                    <input type="text" name="sku_new" placeholder="MRKT-XXXXX" style="margin-top: 8px;">
                </div>
                <div class="form-group">
                    <label>Ключи (каждый с новой строки)</label>
                    <textarea name="keys" rows="10" required placeholder="KEY-001&#10;KEY-002&#10;KEY-003"></textarea>
                </div>
                <div class="form-row">
                    <button type="submit" class="btn-success">Загрузить</button>
                    <button type="button" class="btn-secondary" onclick="closeModal('uploadKeysModal')">Отмена</button>
                </div>
            </form>
        </div>
    </div>

    <!-- Find Order Keys Modal -->
    <div id="findOrderKeysModal" class="modal">
        <div class="modal-content">
            <h2>🔍 Ключи по заказу</h2>
            <form id="findOrderKeysForm" onsubmit="findOrderKeys(event)">
                <div class="form-group">
                    <label>ID заказа</label>
                    <input type="text" id="findOrderKeysInput" required placeholder="Order ID">
                </div>
                <div class="form-row">
                    <button type="submit">Найти</button>
                    <button type="button" class="btn-secondary" onclick="closeModal('findOrderKeysModal')">Закрыть</button>
                </div>
            </form>
            <div id="findOrderKeysResult" style="margin-top: 20px;"></div>
        </div>
    </div>

    <!-- Hide/Show SKU Modal -->
    <div id="hideSkuModal" class="modal">
        <div class="modal-content">
            <h2>👁️ Управление видимостью SKU</h2>
            <form method="POST" action="{{ url_for('admin.toggle_sku_visibility') }}">
                <input type="hidden" name="bot" value="{{ current_bot }}">
                <div class="form-group">
                    <label>SKU</label>
                    <input type="text" name="sku" required placeholder="MRKT-XXXXX">
                </div>
                <div class="form-group">
                    <label>Действие</label>
                    <select name="action" required>
                        <option value="hide">Скрыть</option>
                        <option value="show">Показать</option>
                    </select>
                </div>
                <div class="form-row">
                    <button type="submit">Применить</button>
                    <button type="button" class="btn-secondary" onclick="closeModal('hideSkuModal')">Отмена</button>
                </div>
            </form>
        </div>
    </div>
    {% endif %}

    <!-- Created Links Result Modal -->
    <div id="createdLinksModal" class="modal">
        <div class="modal-content">
            <h2>✅ Созданные ссылки</h2>
            <div id="createdLinksContent"></div>
            <div class="form-row" style="margin-top: 20px;">
                <button onclick="copyCreatedLinks()" class="btn-success">📋 Копировать все</button>
                <button type="button" class="btn-secondary" onclick="closeModal('createdLinksModal')">Закрыть</button>
            </div>
        </div>
    </div>

    <script>
        // Modal functions
        function openModal(id) {
            document.getElementById(id).classList.add('active');
        }
        function closeModal(id) {
            document.getElementById(id).classList.remove('active');
        }
        window.addEventListener('click', function(e) {
            if (e.target.classList.contains('modal')) {
                e.target.classList.remove('active');
            }
        });

        // Navigation
        function loadTable() {
            const bot = "{{ current_bot }}";
            const table = document.getElementById('tableSelect').value;
            if (!table) {
                window.location.href = "{{ url_for('admin.index') }}" + "?bot=" + encodeURIComponent(bot);
            } else {
                window.location.href = "{{ url_for('admin.index') }}" + "?bot=" + encodeURIComponent(bot) + "&table=" + encodeURIComponent(table);
            }
        }

        // Search
        let searchTimeout;
        function handleSearch(event) {
            clearTimeout(searchTimeout);
            if (event.key === 'Enter') {
                doSearch();
            } else {
                searchTimeout = setTimeout(doSearch, 500);
            }
        }
        function doSearch() {
            const q = document.getElementById('searchInput').value;
            const url = new URL(window.location.href);
            if (q) {
                url.searchParams.set('q', q);
            } else {
                url.searchParams.delete('q');
            }
            url.searchParams.set('page', '1');
            window.location.href = url.toString();
        }

        // Pagination
        function goToPage(page) {
            const url = new URL(window.location.href);
            url.searchParams.set('page', page);
            window.location.href = url.toString();
        }

        // Delete record
        async function deleteRow(id) {
            if (!confirm('Удалить запись?')) return;
            const bot = "{{ current_bot }}";
            const table = "{{ current_table }}";
            try {
                const response = await fetch("{{ url_for('admin.delete_record') }}", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ bot, table, id })
                });
                const data = await response.json();
                if (data.success) {
                    location.reload();
                } else {
                    alert("Ошибка: " + (data.error || "неизвестная ошибка"));
                }
            } catch (err) {
                alert("Ошибка сети: " + err.message);
            }
        }

        // Check link (Activate Bot)
        async function checkLink(event) {
            event.preventDefault();
            const input = document.getElementById('checkLinkInput').value.trim();
            const resultDiv = document.getElementById('checkLinkResult');
            resultDiv.innerHTML = '<p>Загрузка...</p>';
            
            try {
                const response = await fetch("{{ url_for('admin.api_check_link') }}", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ bot: "{{ current_bot }}", token: input })
                });
                const data = await response.json();
                
                if (data.error) {
                    resultDiv.innerHTML = `<div class="flash error">${data.error}</div>`;
                } else {
                    const statusClass = data.status === 'active' ? 'free' : (data.status === 'used' ? 'used' : 'blocked');
                    resultDiv.innerHTML = `
                        <div style="background: #f8f9fb; padding: 16px; border-radius: 12px;">
                            <p><strong>Token:</strong> <code>${data.token}</code></p>
                            <p><strong>Product:</strong> ${data.product_name}</p>
                            <p><strong>Status:</strong> <span class="badge ${statusClass}">${data.status}</span></p>
                            <p><strong>Attempts:</strong> ${data.attempts}/3</p>
                            <p><strong>CDK:</strong> <code>${data.cdk_code || '—'}</code></p>
                            <p><strong>Last Error:</strong> ${data.last_error || '—'}</p>
                            <p><strong>Created:</strong> ${data.created_at}</p>
                        </div>
                    `;
                }
            } catch (err) {
                resultDiv.innerHTML = `<div class="flash error">Ошибка: ${err.message}</div>`;
            }
        }

        // Find order (Activate Bot)
        async function findOrder(event) {
            event.preventDefault();
            const orderId = document.getElementById('findOrderInput').value.trim();
            const resultDiv = document.getElementById('findOrderResult');
            resultDiv.innerHTML = '<p>Загрузка...</p>';
            
            try {
                const response = await fetch("{{ url_for('admin.api_find_order') }}", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ bot: "{{ current_bot }}", order_id: orderId })
                });
                const data = await response.json();
                
                if (data.error) {
                    resultDiv.innerHTML = `<div class="flash error">${data.error}</div>`;
                } else {
                    resultDiv.innerHTML = `
                        <div style="background: #f8f9fb; padding: 16px; border-radius: 12px;">
                            <p><strong>Order ID:</strong> <code>${data.order_id}</code></p>
                            <p><strong>Product:</strong> ${data.product_name}</p>
                            <p><strong>Status:</strong> ${data.status}</p>
                            <p><strong>Paid at:</strong> ${data.paid_at || '—'}</p>
                            <p><strong>Link:</strong> ${data.link || '—'}</p>
                        </div>
                    `;
                }
            } catch (err) {
                resultDiv.innerHTML = `<div class="flash error">Ошибка: ${err.message}</div>`;
            }
        }

        // Find order keys (Market Bot)
        async function findOrderKeys(event) {
            event.preventDefault();
            const orderId = document.getElementById('findOrderKeysInput').value.trim();
            const resultDiv = document.getElementById('findOrderKeysResult');
            resultDiv.innerHTML = '<p>Загрузка...</p>';
            
            try {
                const response = await fetch("{{ url_for('admin.api_find_order_keys') }}", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ bot: "{{ current_bot }}", order_id: orderId })
                });
                const data = await response.json();
                
                if (data.error) {
                    resultDiv.innerHTML = `<div class="flash error">${data.error}</div>`;
                } else if (data.keys && data.keys.length > 0) {
                    let html = '<div style="background: #f8f9fb; padding: 16px; border-radius: 12px;">';
                    html += `<p><strong>Найдено ключей:</strong> ${data.keys.length}</p><hr style="margin: 12px 0;">`;
                    data.keys.forEach(k => {
                        html += `<p><strong>SKU:</strong> ${k.sku}<br><strong>Ключ:</strong> <code>${k.license_key}</code><br><strong>Выдан:</strong> ${k.used_at || '—'}</p><hr style="margin: 12px 0;">`;
                    });
                    html += '</div>';
                    resultDiv.innerHTML = html;
                } else {
                    resultDiv.innerHTML = `<div class="flash info">Ключи для этого заказа не найдены</div>`;
                }
            } catch (err) {
                resultDiv.innerHTML = `<div class="flash error">Ошибка: ${err.message}</div>`;
            }
        }

        // Update upload hint based on product selection
        function updateUploadHint() {
            const product = document.getElementById('uploadProduct').value;
            const hint = document.getElementById('uploadHint');
            if (product === 'plus_account') {
                hint.textContent = 'Данные аккаунтов (email:password)';
            } else {
                hint.textContent = 'Каждый код на отдельной строке';
            }
        }

        // Copy created links
        let createdLinksText = '';
        function showCreatedLinks(links) {
            createdLinksText = links.join('\n');
            const content = document.getElementById('createdLinksContent');
            content.innerHTML = `<textarea readonly style="width:100%; height:200px; font-family:monospace; padding:12px; border:1px solid #ddd; border-radius:8px;">${createdLinksText}</textarea>`;
            openModal('createdLinksModal');
        }
        function copyCreatedLinks() {
            navigator.clipboard.writeText(createdLinksText).then(() => {
                alert('Скопировано!');
            });
        }

        // Show created links if present
        {% if created_links %}
        showCreatedLinks({{ created_links | tojson }});
        {% endif %}
    </script>
</body>
</html>
"""


# =========================
# ROUTES
# =========================
@admin.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        password = request.form.get("password", "")
        if password == ADMIN_PASSWORD:
            session["admin_logged_in"] = True
            return redirect(url_for("admin.index"))
        flash("Неверный пароль")
    return render_template_string(LOGIN_TEMPLATE)


@admin.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("admin.login"))


@admin.route("/")
@login_required
def index():
    current_bot = request.args.get("bot", "").strip()
    current_table = request.args.get("table", "").strip()
    search_query = request.args.get("q", "").strip()
    page = int(request.args.get("page", 1))

    data = []
    columns = []
    stats = []
    detailed_stats = []
    id_column = None
    table_addable = False
    deletable = False
    total_count = 0
    total_pages = 1
    sku_list = []
    created_links = session.pop("created_links", None)

    if current_bot and current_bot in BOTS:
        # Get SKU list for market bots
        if current_bot.startswith("market"):
            try:
                rows = select_all(current_bot, "SELECT DISTINCT sku FROM skus UNION SELECT DISTINCT sku FROM keys")
                sku_list = sorted(set(r["sku"] for r in rows if r.get("sku")))
            except:
                pass

        if not current_table:
            # Dashboard view - show detailed stats
            try:
                if current_bot == "activate":
                    detailed_stats = get_activate_stats(current_bot)
                else:
                    detailed_stats = get_market_stats(current_bot)
            except Exception as e:
                flash(f"Ошибка загрузки статистики: {e}", "error")
        else:
            # Table view
            bot, table_info = safe_get_bot_and_table(current_bot, current_table)
            
            if not bot:
                flash("Неверный bot", "error")
                return redirect(url_for("admin.index"))
            
            if not table_info:
                flash("Неверная таблица", "error")
                return redirect(url_for("admin.index", bot=current_bot))

            try:
                columns = get_table_columns(current_bot, current_table)
                ph = get_placeholder(current_bot)
                
                # Build query with search
                where_clause = ""
                params = []
                if search_query and columns:
                    search_conditions = []
                    for col in columns:
                        if BOTS[current_bot]["type"] == "postgresql":
                            search_conditions.append(f"CAST({col} AS TEXT) ILIKE {ph}")
                        else:
                            search_conditions.append(f"CAST({col} AS TEXT) LIKE {ph}")
                        params.append(f"%{search_query}%")
                    where_clause = "WHERE " + " OR ".join(search_conditions)

                # Count total
                count_query = f"SELECT COUNT(*) FROM {current_table} {where_clause}"
                total_count = select_one_value(current_bot, count_query, tuple(params))
                total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
                page = max(1, min(page, total_pages))
                offset = (page - 1) * PAGE_SIZE

                # Get data with pagination
                data_query = f"SELECT * FROM {current_table} {where_clause} LIMIT {PAGE_SIZE} OFFSET {offset}"
                data = select_all(current_bot, data_query, tuple(params))

                table_addable = bool(table_info.get("addable", False))
                deletable = bool(table_info.get("deletable", False))

                # Determine ID column
                if current_table == "promo_codes":
                    id_column = "code"
                elif current_table == "activation_links":
                    id_column = "token"
                elif current_table == "accounts":
                    id_column = "id"
                elif current_table == "keys":
                    id_column = "id"
                elif current_table == "skus":
                    id_column = "sku"
                elif current_table == "hidden_skus":
                    id_column = "sku"
                elif current_table == "sku_groups":
                    id_column = "sku"
                elif columns:
                    id_column = columns[0]

                # Table-specific stats
                if "status" in columns:
                    if current_table == "activation_links":
                        active = select_one_value(current_bot, f"SELECT COUNT(*) FROM {current_table} WHERE status = 'active'", ())
                        used = select_one_value(current_bot, f"SELECT COUNT(*) FROM {current_table} WHERE status = 'used'", ())
                        blocked = select_one_value(current_bot, f"SELECT COUNT(*) FROM {current_table} WHERE status = 'blocked'", ())
                        stats = [
                            {"title": "Активных", "value": active},
                            {"title": "Использованных", "value": used},
                            {"title": "Заблокированных", "value": blocked},
                        ]
                    else:
                        free = select_one_value(current_bot, f"SELECT COUNT(*) FROM {current_table} WHERE status = 'free'", ())
                        used = select_one_value(current_bot, f"SELECT COUNT(*) FROM {current_table} WHERE status = 'used'", ())
                        stats = [
                            {"title": "Свободных", "value": free},
                            {"title": "Использованных", "value": used},
                            {"title": "Всего", "value": free + used},
                        ]
                else:
                    stats = [{"title": "Записей", "value": total_count}]

            except Exception as e:
                flash(f"Ошибка загрузки таблицы: {e}", "error")

    return render_template_string(
        ADMIN_TEMPLATE,
        bots=BOTS,
        current_bot=current_bot,
        current_table=current_table,
        data=data,
        columns=columns,
        stats=stats,
        detailed_stats=detailed_stats,
        id_column=id_column,
        table_addable=table_addable,
        deletable=deletable,
        current_page=page,
        total_pages=total_pages,
        total_count=total_count,
        search_query=search_query,
        sku_list=sku_list,
        created_links=created_links,
    )


@admin.route("/add", methods=["POST"])
@login_required
def add_record():
    bot_id = request.form.get("bot", "").strip()
    table = request.form.get("table", "").strip()

    bot, table_info = safe_get_bot_and_table(bot_id, table)
    if not bot or not table_info:
        flash("Неверный bot или table", "error")
        return redirect(url_for("admin.index"))

    ph = get_placeholder(bot_id)

    try:
        if table == "promo_codes":
            code = request.form.get("code", "").strip()
            product = request.form.get("product", "").strip()
            if not code or not product:
                flash("Заполните все поля", "error")
                return redirect(url_for("admin.index", bot=bot_id, table=table))
            sql = f"INSERT INTO promo_codes (code, product, status) VALUES ({ph}, {ph}, 'free')"
            execute_write(bot_id, sql, (code, product))
            flash("Промокод добавлен", "success")

        elif table == "accounts":
            data = request.form.get("data", "").strip()
            if not data:
                flash("Заполните данные аккаунта", "error")
                return redirect(url_for("admin.index", bot=bot_id, table=table))
            sql = f"INSERT INTO accounts (data, status) VALUES ({ph}, 'free')"
            execute_write(bot_id, sql, (data,))
            flash("Аккаунт добавлен", "success")

        elif table == "activation_links":
            product = request.form.get("product", "").strip()
            count = int(request.form.get("count", 1))
            if not product or count < 1:
                flash("Неверные параметры", "error")
                return redirect(url_for("admin.index", bot=bot_id, table=table))
            
            base_url = BOTS[bot_id].get("public_base_url", "https://codenext.ru")
            links = []
            for _ in range(count):
                token = secrets.token_urlsafe(16)
                sql = f"INSERT INTO activation_links (token, product, status, attempts) VALUES ({ph}, {ph}, 'active', 0)"
                execute_write(bot_id, sql, (token, product))
                prefix = "/a/" if product == "plus_account" else "/l/"
                links.append(f"{base_url}{prefix}{token}")
            
            session["created_links"] = links
            flash(f"Создано {count} ссылок", "success")

        elif table == "keys":
            sku = request.form.get("sku", "").strip()
            license_key = request.form.get("license_key", "").strip()
            if not sku or not license_key:
                flash("Заполните все поля", "error")
                return redirect(url_for("admin.index", bot=bot_id, table=table))
            sql = f"INSERT INTO keys (sku, license_key, status) VALUES ({ph}, {ph}, 'free')"
            execute_write(bot_id, sql, (sku, license_key))
            flash("Ключ добавлен", "success")

        elif table == "skus":
            sku = request.form.get("sku", "").strip()
            title = request.form.get("title", "").strip() or None
            if not sku:
                flash("Введите SKU", "error")
                return redirect(url_for("admin.index", bot=bot_id, table=table))
            sql = f"INSERT INTO skus (sku, title) VALUES ({ph}, {ph})"
            execute_write(bot_id, sql, (sku, title))
            flash("SKU добавлен", "success")

        elif table == "hidden_skus":
            sku = request.form.get("sku", "").strip()
            if not sku:
                flash("Введите SKU", "error")
                return redirect(url_for("admin.index", bot=bot_id, table=table))
            sql = f"INSERT OR IGNORE INTO hidden_skus (sku) VALUES ({ph})" if BOTS[bot_id]["type"] == "sqlite" else f"INSERT INTO hidden_skus (sku) VALUES ({ph}) ON CONFLICT DO NOTHING"
            execute_write(bot_id, sql, (sku,))
            flash("SKU скрыт", "success")

        else:
            flash("Добавление для этой таблицы не поддерживается", "error")

    except Exception as e:
        flash(f"Ошибка добавления: {e}", "error")

    return redirect(url_for("admin.index", bot=bot_id, table=table))


@admin.route("/bulk-add", methods=["POST"])
@login_required
def bulk_add():
    bot_id = request.form.get("bot", "").strip()
    table = request.form.get("table", "").strip()
    items_raw = request.form.get("items", "")
    
    items = [line.strip() for line in items_raw.splitlines() if line.strip()]
    if not items:
        flash("Нет данных для загрузки", "error")
        return redirect(url_for("admin.index", bot=bot_id, table=table))

    ph = get_placeholder(bot_id)
    added = 0

    try:
        if table == "keys":
            sku = request.form.get("sku", "").strip()
            if not sku:
                flash("Укажите SKU", "error")
                return redirect(url_for("admin.index", bot=bot_id, table=table))
            
            sql = f"INSERT INTO keys (sku, license_key, status) VALUES ({ph}, {ph}, 'free')"
            for key in items:
                try:
                    execute_write(bot_id, sql, (sku, key))
                    added += 1
                except:
                    pass

        elif table == "promo_codes":
            product = request.form.get("product", "").strip()
            if not product:
                flash("Укажите тариф", "error")
                return redirect(url_for("admin.index", bot=bot_id, table=table))
            
            if BOTS[bot_id]["type"] == "postgresql":
                sql = f"INSERT INTO promo_codes (code, product, status) VALUES ({ph}, {ph}, 'free') ON CONFLICT DO NOTHING"
            else:
                sql = f"INSERT OR IGNORE INTO promo_codes (code, product, status) VALUES ({ph}, {ph}, 'free')"
            
            for code in items:
                try:
                    execute_write(bot_id, sql, (code, product))
                    added += 1
                except:
                    pass

        flash(f"Добавлено {added} из {len(items)}", "success")

    except Exception as e:
        flash(f"Ошибка: {e}", "error")

    return redirect(url_for("admin.index", bot=bot_id, table=table))


@admin.route("/delete", methods=["POST"])
@login_required
def delete_record():
    try:
        payload = request.get_json(force=True)
        bot_id = (payload.get("bot") or "").strip()
        table = (payload.get("table") or "").strip()
        record_id = payload.get("id")

        bot, table_info = safe_get_bot_and_table(bot_id, table)
        if not bot or not table_info:
            return jsonify({"success": False, "error": "Неверный bot или table"}), 400

        ph = get_placeholder(bot_id)

        id_columns = {
            "promo_codes": "code",
            "activation_links": "token",
            "accounts": "id",
            "keys": "id",
            "skus": "sku",
            "hidden_skus": "sku",
            "sku_groups": "sku",
        }

        id_col = id_columns.get(table)
        if not id_col:
            return jsonify({"success": False, "error": "Удаление не поддерживается"}), 400

        sql = f"DELETE FROM {table} WHERE {id_col} = {ph}"
        execute_write(bot_id, sql, (record_id,))
        return jsonify({"success": True})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# === ACTIVATE BOT SPECIFIC ROUTES ===

@admin.route("/create-links", methods=["POST"])
@login_required
def create_links():
    bot_id = request.form.get("bot", "").strip()
    product = request.form.get("product", "").strip()
    count = int(request.form.get("count", 1))

    if bot_id != "activate" or not product or count < 1 or count > 500:
        flash("Неверные параметры", "error")
        return redirect(url_for("admin.index", bot=bot_id))

    base_url = BOTS[bot_id].get("public_base_url", "https://codenext.ru")
    ph = get_placeholder(bot_id)
    links = []

    try:
        for _ in range(count):
            token = secrets.token_urlsafe(16)
            sql = f"INSERT INTO activation_links (token, product, status, attempts) VALUES ({ph}, {ph}, 'active', 0)"
            execute_write(bot_id, sql, (token, product))
            prefix = "/a/" if product == "plus_account" else "/l/"
            links.append(f"{base_url}{prefix}{token}")

        session["created_links"] = links
        flash(f"Создано {count} ссылок", "success")

    except Exception as e:
        flash(f"Ошибка: {e}", "error")

    return redirect(url_for("admin.index", bot=bot_id))


@admin.route("/upload-codes", methods=["POST"])
@login_required
def upload_codes():
    bot_id = request.form.get("bot", "").strip()
    product = request.form.get("product", "").strip()
    codes_raw = request.form.get("codes", "")

    codes = [line.strip() for line in codes_raw.splitlines() if line.strip()]
    if not codes:
        flash("Нет кодов для загрузки", "error")
        return redirect(url_for("admin.index", bot=bot_id))

    ph = get_placeholder(bot_id)
    added = 0

    try:
        if product == "plus_account":
            # Upload to accounts table
            sql = f"INSERT INTO accounts (data, status) VALUES ({ph}, 'free')"
            for code in codes:
                try:
                    execute_write(bot_id, sql, (code,))
                    added += 1
                except:
                    pass
        else:
            # Upload to promo_codes table
            if BOTS[bot_id]["type"] == "postgresql":
                sql = f"INSERT INTO promo_codes (code, product, status) VALUES ({ph}, {ph}, 'free') ON CONFLICT DO NOTHING"
            else:
                sql = f"INSERT OR IGNORE INTO promo_codes (code, product, status) VALUES ({ph}, {ph}, 'free')"
            
            for code in codes:
                try:
                    execute_write(bot_id, sql, (code, product))
                    added += 1
                except:
                    pass

        flash(f"Добавлено {added} из {len(codes)}", "success")

    except Exception as e:
        flash(f"Ошибка: {e}", "error")

    return redirect(url_for("admin.index", bot=bot_id))


@admin.route("/api/check-link", methods=["POST"])
@login_required
def api_check_link():
    try:
        payload = request.get_json(force=True)
        bot_id = payload.get("bot", "").strip()
        token_input = payload.get("token", "").strip()
        
        # Extract token from URL if needed
        import re
        m = re.search(r"/[la]/([A-Za-z0-9_\-]+)", token_input)
        token = m.group(1) if m else token_input.split('/')[-1]

        ph = get_placeholder(bot_id)
        row = select_one(bot_id, f"SELECT * FROM activation_links WHERE token = {ph}", (token,))
        
        if not row:
            return jsonify({"error": "Ссылка не найдена"})

        products = BOTS[bot_id].get("products", {})
        return jsonify({
            "token": row.get("token"),
            "product": row.get("product"),
            "product_name": products.get(row.get("product"), row.get("product")),
            "status": row.get("status"),
            "attempts": row.get("attempts", 0),
            "cdk_code": row.get("cdk_code"),
            "last_error": row.get("last_error"),
            "created_at": str(row.get("created_at") or ""),
        })

    except Exception as e:
        return jsonify({"error": str(e)})


@admin.route("/api/find-order", methods=["POST"])
@login_required
def api_find_order():
    try:
        payload = request.get_json(force=True)
        bot_id = payload.get("bot", "").strip()
        order_id = payload.get("order_id", "").strip()

        ph = get_placeholder(bot_id)
        row = select_one(bot_id, f"SELECT * FROM orders WHERE order_id = {ph}", (order_id,))
        
        if not row:
            return jsonify({"error": "Заказ не найден"})

        products = BOTS[bot_id].get("products", {})
        base_url = BOTS[bot_id].get("public_base_url", "https://codenext.ru")
        
        link = None
        if row.get("activation_token"):
            link = f"{base_url}/l/{row['activation_token']}"

        return jsonify({
            "order_id": row.get("order_id"),
            "product": row.get("product"),
            "product_name": products.get(row.get("product"), row.get("product")),
            "status": row.get("status"),
            "paid_at": str(row.get("paid_at") or ""),
            "link": link,
        })

    except Exception as e:
        return jsonify({"error": str(e)})


# === MARKET BOT SPECIFIC ROUTES ===

@admin.route("/add-sku", methods=["POST"])
@login_required
def add_sku():
    bot_id = request.form.get("bot", "").strip()
    sku = request.form.get("sku", "").strip()
    title = request.form.get("title", "").strip() or None

    if not sku:
        flash("Введите SKU", "error")
        return redirect(url_for("admin.index", bot=bot_id))

    ph = get_placeholder(bot_id)
    
    try:
        # Try to insert, or update title if exists
        try:
            sql = f"INSERT INTO skus (sku, title) VALUES ({ph}, {ph})"
            execute_write(bot_id, sql, (sku, title))
            flash(f"SKU {sku} добавлен", "success")
        except:
            if title:
                sql = f"UPDATE skus SET title = {ph} WHERE sku = {ph}"
                execute_write(bot_id, sql, (title, sku))
                flash(f"SKU {sku} обновлён", "success")
            else:
                flash(f"SKU {sku} уже существует", "info")

    except Exception as e:
        flash(f"Ошибка: {e}", "error")

    return redirect(url_for("admin.index", bot=bot_id))


@admin.route("/upload-keys", methods=["POST"])
@login_required
def upload_keys():
    bot_id = request.form.get("bot", "").strip()
    sku = request.form.get("sku", "").strip() or request.form.get("sku_new", "").strip()
    keys_raw = request.form.get("keys", "")

    if not sku:
        flash("Укажите SKU", "error")
        return redirect(url_for("admin.index", bot=bot_id))

    keys = [line.strip() for line in keys_raw.splitlines() if line.strip()]
    if not keys:
        flash("Нет ключей для загрузки", "error")
        return redirect(url_for("admin.index", bot=bot_id))

    ph = get_placeholder(bot_id)
    added = 0

    try:
        sql = f"INSERT INTO keys (sku, license_key, status) VALUES ({ph}, {ph}, 'free')"
        for key in keys:
            try:
                execute_write(bot_id, sql, (sku, key))
                added += 1
            except:
                pass

        flash(f"Добавлено {added} ключей для {sku}", "success")

    except Exception as e:
        flash(f"Ошибка: {e}", "error")

    return redirect(url_for("admin.index", bot=bot_id))


@admin.route("/toggle-sku-visibility", methods=["POST"])
@login_required
def toggle_sku_visibility():
    bot_id = request.form.get("bot", "").strip()
    sku = request.form.get("sku", "").strip()
    action = request.form.get("action", "").strip()

    if not sku:
        flash("Укажите SKU", "error")
        return redirect(url_for("admin.index", bot=bot_id))

    ph = get_placeholder(bot_id)

    try:
        if action == "hide":
            sql = f"INSERT OR IGNORE INTO hidden_skus (sku) VALUES ({ph})" if BOTS[bot_id]["type"] == "sqlite" else f"INSERT INTO hidden_skus (sku) VALUES ({ph}) ON CONFLICT DO NOTHING"
            execute_write(bot_id, sql, (sku,))
            flash(f"SKU {sku} скрыт", "success")
        else:
            sql = f"DELETE FROM hidden_skus WHERE sku = {ph}"
            execute_write(bot_id, sql, (sku,))
            flash(f"SKU {sku} показан", "success")

    except Exception as e:
        flash(f"Ошибка: {e}", "error")

    return redirect(url_for("admin.index", bot=bot_id))


@admin.route("/api/find-order-keys", methods=["POST"])
@login_required
def api_find_order_keys():
    try:
        payload = request.get_json(force=True)
        bot_id = payload.get("bot", "").strip()
        order_id = payload.get("order_id", "").strip()

        ph = get_placeholder(bot_id)
        rows = select_all(bot_id, f"SELECT sku, license_key, status, used_at FROM keys WHERE order_id = {ph} ORDER BY used_at", (order_id,))
        
        keys = []
        for r in rows:
            keys.append({
                "sku": r.get("sku"),
                "license_key": r.get("license_key"),
                "status": r.get("status"),
                "used_at": str(r.get("used_at") or ""),
            })

        return jsonify({"keys": keys})

    except Exception as e:
        return jsonify({"error": str(e)})


# =========================
# APP
# =========================
app.register_blueprint(admin)


@app.route("/")
def root():
    return redirect(url_for("admin.index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000, debug=False)