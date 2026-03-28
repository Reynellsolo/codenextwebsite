import os
from pathlib import Path
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from databases import Database
import secrets
import re

# ================== CONFIG ==================
def load_env_file(path: str):
    p = Path(path)
    if not p.exists(): return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

ENV_PATH = os.environ.get("BOT_ENV_PATH", "/opt/activate/bot.env")
load_env_file(ENV_PATH)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
DATABASE_URL = os.environ.get("DATABASE_URL")
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://codenext.ru")

if not BOT_TOKEN or not DATABASE_URL:
    raise RuntimeError("Config missing (BOT_TOKEN or DATABASE_URL)")

# ================== DB & BOT ==================
database = Database(DATABASE_URL)
bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())

PRODUCTS = {
    "plus_1m": "ChatGPT Plus · 1 месяц",
    "go_12m": "ChatGPT GO · 12 месяцев",
    "plus_12m": "ChatGPT Plus · 12 месяцев",
    "plus_account": "ChatGPT Plus · Аккаунт",
}


PAGE_SIZE = 50
MAX_ATTEMPTS = 3

# ================== FSM ==================
class CreateLinks(StatesGroup):
    choosing_tariff = State()
    waiting_count = State()

class UploadCDK(StatesGroup):
    choosing_tariff = State()
    waiting_codes = State()

class CheckLink(StatesGroup):
    waiting_token = State()

class FindOrder(StatesGroup):
    waiting_id = State()

class KeysView(StatesGroup):
    choosing_tariff = State()

# ================== HELPERS ==================
def only_admin(message_or_call) -> bool:
    uid = message_or_call.from_user.id
    return uid == ADMIN_ID

async def on_startup(_):
    await database.connect()
    print("Bot DB connected")

async def on_shutdown(_):
    await database.disconnect()
    print("Bot DB disconnected")

def parse_token(text: str) -> str:
    t = (text or "").strip()
    m = re.search(r"/l/([A-Za-z0-9_\-]+)", t)
    if m: return m.group(1)
    return t.split('/')[-1] # Fallback

# ================== KEYBOARDS ==================
def main_kb():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("➕ Создать ссылки", "📥 Загрузить лицензии")
    kb.row("📦 Остатки по тарифам", "📋 Лицензии по тарифу")
    kb.row("🔍 Проверить ссылку", "🔎 Найти заказ")
    return kb

def cancel_kb():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("↩️ Отмена")
    return kb

def tariffs_inline(prefix):
    kb = types.InlineKeyboardMarkup()
    for k, v in PRODUCTS.items():
        kb.add(types.InlineKeyboardButton(v, callback_data=f"{prefix}:{k}"))
    kb.add(types.InlineKeyboardButton("↩️ В меню", callback_data="menu"))
    return kb

def keys_pager_kb(product: str, offset: int, total: int) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    prev_off = max(0, offset - PAGE_SIZE)
    next_off = offset + PAGE_SIZE
    buttons = []
    if offset > 0:
        buttons.append(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"keys:{product}:{prev_off}"))
    if next_off < total:
        buttons.append(types.InlineKeyboardButton("➡️ Вперёд", callback_data=f"keys:{product}:{next_off}"))
    if buttons: kb.row(*buttons)
    kb.add(types.InlineKeyboardButton("🔁 Другой тариф", callback_data="keys_choose"))
    kb.add(types.InlineKeyboardButton("↩️ В меню", callback_data="menu"))
    return kb

# ================== HANDLERS ==================
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    if not only_admin(message): return
    await message.answer("Админ-панель готова ✅", reply_markup=main_kb())

@dp.message_handler(lambda m: m.text == "↩️ Отмена", state="*")
async def cancel_action(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer("Действие отменено.", reply_markup=main_kb())

# --- Create Links ---
@dp.message_handler(lambda m: only_admin(m) and m.text == "➕ Создать ссылки")
async def menu_create_links(message: types.Message):
    await CreateLinks.choosing_tariff.set()
    await message.answer("Выбери тариф:", reply_markup=tariffs_inline("mklinks"))

@dp.callback_query_handler(lambda c: c.data.startswith("mklinks:"), state=CreateLinks.choosing_tariff)
async def mklinks_tariff(call: types.CallbackQuery, state: FSMContext):
    prod = call.data.split(":")[1]
    await state.update_data(product=prod)
    await CreateLinks.waiting_count.set()
    await call.message.edit_text(f"Тариф: {PRODUCTS[prod]}\nНапиши количество (1-500):")

@dp.message_handler(state=CreateLinks.waiting_count)
async def mklinks_count(message: types.Message, state: FSMContext):
    try:
        count = int(message.text)
        if not 1 <= count <= 500: raise ValueError
    except:
        return await message.answer("Введи число от 1 до 500")
    
    data = await state.get_data()
    prod = data['product']
    links = []
    
    # Транзакция для целостности
    async with database.transaction():
        for _ in range(count):
            token = secrets.token_urlsafe(16)
            await database.execute(
                "INSERT INTO activation_links(token, product, status, attempts) VALUES (:t, :p, 'active', 0)",
                values={"t": token, "p": prod}
            )
            # Для аккаунтов — /a/, для остальных — /l/
            if prod == "plus_account":
                links.append(f"{PUBLIC_BASE_URL}/a/{token}")
            else:
                links.append(f"{PUBLIC_BASE_URL}/l/{token}")
    
    await message.answer(f"✅ Создано {count} ссылок:")
    # Send in chunks
    chunk_size = 20
    for i in range(0, len(links), chunk_size):
        await message.answer("\n".join(links[i:i+chunk_size]))
    
    await state.finish()
    await message.answer("Готово", reply_markup=main_kb())

# --- Upload CDK ---
@dp.message_handler(lambda m: only_admin(m) and m.text == "📥 Загрузить лицензии")
async def menu_upload(message: types.Message):
    await UploadCDK.choosing_tariff.set()
    await message.answer("Выбери тариф:", reply_markup=tariffs_inline("upcdk"))

@dp.callback_query_handler(lambda c: c.data.startswith("upcdk:"), state=UploadCDK.choosing_tariff)
async def upcdk_tariff(call: types.CallbackQuery, state: FSMContext):
    prod = call.data.split(":")[1]
    await state.update_data(product=prod)
    await UploadCDK.waiting_codes.set()
    await call.message.edit_text(f"Тариф: {PRODUCTS[prod]}\nПришли коды (каждый с новой строки):")

@dp.message_handler(state=UploadCDK.waiting_codes)
async def upcdk_codes(message: types.Message, state: FSMContext):
    data = await state.get_data()
    prod = data['product']
    codes = [c.strip() for c in message.text.splitlines() if c.strip()]
    
    added = 0
    
    # Если это аккаунты — пишем в таблицу accounts
    if prod == "plus_account":
        for code in codes:
            try:
                await database.execute(
                    "INSERT INTO accounts(data, status) VALUES (:data, 'free')",
                    values={"data": code}
                )
                added += 1
            except: pass
    else:
        # Остальные тарифы — в promo_codes как раньше
        for code in codes:
            try:
                await database.execute(
                    "INSERT INTO promo_codes(code, product, status) VALUES (:c, :p, 'free') ON CONFLICT DO NOTHING",
                    values={"c": code, "p": prod}
                )
                added += 1
            except: pass
        
    await state.finish()
    await message.answer(f"✅ Добавлено: {added} из {len(codes)}", reply_markup=main_kb())

# --- Stock ---
@dp.message_handler(lambda m: only_admin(m) and m.text == "📦 Остатки по тарифам")
async def menu_stock(message: types.Message):
    res = ["📦 <b>Остатки:</b>"]
    for p, name in PRODUCTS.items():
        if p == "plus_account":
            # Аккаунты — из таблицы accounts
            row = await database.fetch_one(
                "SELECT COUNT(*) as cnt FROM accounts WHERE status='free'"
            )
        else:
            # Остальные — из promo_codes
            row = await database.fetch_one(
                "SELECT COUNT(*) as cnt FROM promo_codes WHERE product=:p AND status='free'",
                values={"p": p}
            )
        cnt = row['cnt'] if row else 0
        res.append(f"{name}: <b>{cnt}</b>")
    await message.answer("\n".join(res), reply_markup=main_kb())

# --- Check Link ---
@dp.message_handler(lambda m: only_admin(m) and m.text == "🔍 Проверить ссылку")
async def menu_check(message: types.Message):
    await CheckLink.waiting_token.set()
    await message.answer("Пришли токен или ссылку:", reply_markup=cancel_kb())

@dp.message_handler(state=CheckLink.waiting_token)
async def check_token(message: types.Message, state: FSMContext):
    t = parse_token(message.text)
    row = await database.fetch_one(
        "SELECT * FROM activation_links WHERE token=:t",
        values={"t": t}
    )
    if not row:
        await message.answer("❌ Не найдено", reply_markup=main_kb())
    else:
        status_map = {"active": "⏳ Ожидает", "used": "✅ Использован", "blocked": "🚫 Блок"}
        status = status_map.get(row['status'], row['status'])
        
        await message.answer(
            f"🔍 <b>Ссылка:</b>\n"
            f"Token: <code>{t}</code>\n"
            f"Product: {PRODUCTS.get(row['product'], row['product'])}\n"
            f"Status: {status}\n"
            f"Attempts: {row['attempts']}/{MAX_ATTEMPTS}\n"
            f"CDK: <code>{row['cdk_code'] or '-'}</code>\n"
            f"Error: <code>{row['last_error'] or '-'}</code>\n"
            f"Created: {row['created_at']}",
            reply_markup=main_kb()
        )
    await state.finish()

# --- Find Order ---
@dp.message_handler(lambda m: only_admin(m) and m.text == "🔎 Найти заказ")
async def menu_find(message: types.Message):
    await FindOrder.waiting_id.set()
    await message.answer("Пришли Order ID:", reply_markup=cancel_kb())

@dp.message_handler(state=FindOrder.waiting_id)
async def find_order(message: types.Message, state: FSMContext):
    oid = message.text.strip()
    row = await database.fetch_one(
        "SELECT * FROM orders WHERE order_id=:oid",
        values={"oid": oid}
    )
    if not row:
        await message.answer("❌ Не найдено", reply_markup=main_kb())
    else:
        link = f"{PUBLIC_BASE_URL}/l/{row['activation_token']}" if row['activation_token'] else "нет"
        await message.answer(
            f"📦 <b>Заказ:</b>\n"
            f"ID: <code>{oid}</code>\n"
            f"Product: {PRODUCTS.get(row['product'], row['product'])}\n"
            f"Status: {row['status']}\n"
            f"Paid: {row['paid_at']}\n"
            f"Link: {link}",
            reply_markup=main_kb()
        )
    await state.finish()

# --- Keys List (Pagination) ---
@dp.message_handler(lambda m: only_admin(m) and m.text == "📋 Лицензии по тарифу")
async def menu_keys(message: types.Message):
    await KeysView.choosing_tariff.set()
    await message.answer("Выбери тариф:", reply_markup=tariffs_inline("keyschoose"))

@dp.callback_query_handler(lambda c: c.data.startswith("keyschoose:"), state=KeysView.choosing_tariff)
async def keys_choose(call: types.CallbackQuery, state: FSMContext):
    prod = call.data.split(":")[1]
    await show_keys_page(call.message, prod, 0)
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("keys:"), state="*")
async def keys_pager(call: types.CallbackQuery):
    _, prod, offset = call.data.split(":")
    await show_keys_page(call.message, prod, int(offset), edit=True)
    await call.answer()

@dp.callback_query_handler(text="keys_choose", state="*")
async def keys_back_choose(call: types.CallbackQuery):
    await KeysView.choosing_tariff.set()
    await call.message.edit_text("Выбери тариф:", reply_markup=tariffs_inline("keyschoose"))
    await call.answer()

async def show_keys_page(message: types.Message, product: str, offset: int, edit: bool = False):
    if product == "plus_account":
        total_row = await database.fetch_one(
            "SELECT COUNT(*) as cnt FROM accounts WHERE status='free'"
        )
        total = total_row['cnt'] if total_row else 0

        rows = await database.fetch_all(
            "SELECT data as code FROM accounts WHERE status='free' ORDER BY id LIMIT :limit OFFSET :off",
            values={"limit": PAGE_SIZE, "off": offset}
        )
        codes = [r['code'] for r in rows]
    else:
        total_row = await database.fetch_one(
            "SELECT COUNT(*) as cnt FROM promo_codes WHERE product=:p AND status='free'",
            values={"p": product}
        )
        total = total_row['cnt'] if total_row else 0

        rows = await database.fetch_all(
            "SELECT code FROM promo_codes WHERE product=:p AND status='free' ORDER BY code LIMIT :limit OFFSET :off",
            values={"p": product, "limit": PAGE_SIZE, "off": offset}
        )
        codes = [r['code'] for r in rows]
    
    text = (
        f"📋 <b>{PRODUCTS[product]}</b>\n"
        f"Показано {offset+1}-{min(offset+PAGE_SIZE, total)} из {total}\n\n"
    )
    if not codes:
        text += "Пусто"
    else:
        text += "<code>" + "\n".join(codes) + "</code>"
        
    kb = keys_pager_kb(product, offset, total)
    
    if edit:
        await message.edit_text(text, reply_markup=kb)
    else:
        await message.answer(text, reply_markup=kb)

# --- Menu Callback ---
@dp.callback_query_handler(text="menu", state="*")
async def cb_menu(call: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await call.message.answer("Главное меню", reply_markup=main_kb())
    await call.message.delete()

if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown, skip_updates=True)