import asyncio
import logging
import os
import secrets
import time
import html
import json
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Tuple
from uuid import uuid4
from enum import StrEnum

import aiosqlite
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import TelegramError

# ---------- Перечисления ----------
class UserState(StrEnum):
    PLAYING = "playing"
    AWAITING_BET = "awaiting_bet"
    AWAITING_FIELD = "awaiting_field"
    CHOOSING_MINES = "choosing_mines"
    READY = "ready"

class InvoiceStatus(StrEnum):
    PENDING = "pending"
    PAID = "paid"
    QUEUED = "queued"
    ACTIVE = "active"
    CANCELLED = "cancelled"

# ---------- Конфигурация ----------
@dataclass
class Config:
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    DB_FILE: str = "mines_game.db"
    INVOICE_TTL: int = 600
    NAME_CACHE_TTL: int = 600
    MAX_BET: int = 2500
    MIN_BET: int = 1
    FIELD_SIZES: Dict[int, int] = field(default_factory=lambda: {3: 9, 4: 16, 5: 25})
    MINE_OPTIONS: Dict[int, List[int]] = field(default_factory=lambda: {
        3: [3, 5, 7],
        4: [5, 8, 10],
        5: [7, 15, 20]
    })
    MULTIPLIERS: Dict[int, Dict[int, List[float]]] = field(default_factory=lambda: {
        3: {
            3: [1.3, 1.8, 2.5, 3.9, 5.0, 8.0],
            5: [1.8, 2.5, 5.0, 10.0],
            7: [5.0, 12.0],
        },
        4: {
            5: [1.10, 1.3, 1.5, 2.00, 2.50, 3.10, 3.50, 3.90, 4.30, 8.70, 25.00],
            8: [1.30, 1.80, 2.2, 2.7, 3.60, 4.20, 6.80, 15.00],
            10: [1.70, 2.3, 4.00, 8.00, 16.00, 25.00],
        },
        5: {
            7: [1.10, 1.4, 1.7, 1.9, 2.1, 2.5, 3.00, 3.30, 3.60, 3.90, 4.20, 4.50, 4.80, 5.10, 5.40, 5.70, 6.00, 25.00],
            15: [1.30, 1.9, 2.5, 3.5, 4.50, 6.30, 8.50, 11.00, 15.00, 25.00],
            20: [1.90, 3.00, 6.00, 14.00, 25.00],
        }
    })
    TOP_PAGE_SIZE: int = 10
    RATE_LIMIT_SEC: float = 0.5
    STALE_GAME_TIMEOUT: int = 3600 * 24
    ORPHAN_INVOICE_TIMEOUT: int = 3600 * 48
    OWNER_USERNAME: str = os.environ.get("OWNER_USERNAME", "tolpaz")
    OWNER_USER_ID: int = int(os.environ.get("OWNER_USER_ID", 0))  # если указан, используется вместо username
    HUB_PHOTO_ID: str = "AgACAgIAAxkBAAFO2SBqUpOUMryc-Kp-u5FtVRxYDgs-bwACjhtrG-1SkUqsoRXZaVLU0QEAAwIAA3kAAzwE"
    CHANNEL_LINK: str = "https://t.me/dollarpunch"
    CHAT_LINK: str = "https://t.me/honeyholder"
    REQUEST_TIMEOUT: float = 15.0   # таймаут для ботовских вызовов

config = Config()
if not config.BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set in environment")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------- База данных ----------
class Database:
    def __init__(self, path: str):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None
        self._write_lock = asyncio.Lock()

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._create_tables()
        await self._run_migrations()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()

    async def _create_tables(self) -> None:
        await self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS top (
                user_id INTEGER PRIMARY KEY,
                total INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS invoices (
                payload TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                bet INTEGER NOT NULL,
                mines INTEGER NOT NULL,
                field_size INTEGER NOT NULL DEFAULT 3,
                created REAL NOT NULL,
                paid INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                chat_id INTEGER,
                message_id INTEGER,
                origin_chat_id INTEGER,
                prepaid INTEGER NOT NULL DEFAULT 0,
                charge_id TEXT
            );
            CREATE TABLE IF NOT EXISTS active_games (
                user_id INTEGER PRIMARY KEY,
                bet INTEGER NOT NULL,
                prepaid INTEGER NOT NULL DEFAULT 0,
                mines INTEGER NOT NULL,
                field_size INTEGER NOT NULL DEFAULT 3,
                board TEXT NOT NULL,
                opened TEXT NOT NULL,
                step INTEGER NOT NULL DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 1,
                message_id INTEGER,
                chat_id INTEGER,
                origin_chat_id INTEGER,
                invoice_payload TEXT,
                created REAL NOT NULL DEFAULT (strftime('%s', 'now'))
            );
            CREATE TABLE IF NOT EXISTS promos (
                code TEXT PRIMARY KEY,
                stars INTEGER NOT NULL,
                max_activations INTEGER NOT NULL,
                used_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS promo_activations (
                user_id INTEGER NOT NULL,
                promo_code TEXT NOT NULL,
                PRIMARY KEY (user_id, promo_code)
            );
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                created REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
            );
            CREATE TABLE IF NOT EXISTS finance_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                type TEXT NOT NULL,
                description TEXT,
                created REAL NOT NULL DEFAULT (strftime('%s', 'now'))
            );
            CREATE TABLE IF NOT EXISTS processed_charges (
                charge_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                created REAL NOT NULL DEFAULT (strftime('%s', 'now'))
            );
            CREATE INDEX IF NOT EXISTS idx_invoices_user_status ON invoices(user_id, status);
            CREATE INDEX IF NOT EXISTS idx_invoices_status ON invoices(status);
            CREATE INDEX IF NOT EXISTS idx_active_games_active ON active_games(active);
        """)

    async def _run_migrations(self) -> None:
        migrations = {
            "active_games": {"prepaid": "INTEGER NOT NULL DEFAULT 0", "field_size": "INTEGER NOT NULL DEFAULT 3"},
            "invoices": {"field_size": "INTEGER NOT NULL DEFAULT 3", "charge_id": "TEXT"},
        }
        for table, columns in migrations.items():
            cursor = await self._conn.execute(f"PRAGMA table_info({table})")
            existing_cols = [row[1] for row in await cursor.fetchall()]
            for col, col_def in columns.items():
                if col not in existing_cols:
                    await self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")
        # processed_charges table migration (if not exists)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_charges (
                charge_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                created REAL NOT NULL DEFAULT (strftime('%s', 'now'))
            )
        """)
        await self._conn.commit()

    async def _execute_write(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        async with self._write_lock:
            cursor = await self._conn.execute(sql, params)
            await self._conn.commit()
            return cursor

    async def _execute_transaction(self, statements: List[Tuple[str, tuple]]) -> None:
        async with self._write_lock:
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                for sql, params in statements:
                    await self._conn.execute(sql, params)
                await self._conn.commit()
            except Exception:
                await self._conn.execute("ROLLBACK")
                raise

    async def fetch_one(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        async with self._conn.execute(sql, params) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row else None

    async def fetch_all(self, sql: str, params: tuple = ()) -> List[Dict]:
        async with self._conn.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_user_total(self, user_id: int) -> int:
        row = await self.fetch_one("SELECT total FROM top WHERE user_id = ?", (user_id,))
        return row["total"] if row else 0

    async def update_user_total(self, user_id: int, delta: int) -> None:
        await self._execute_write(
            "INSERT INTO top (user_id, total) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET total = total + ?",
            (user_id, delta, delta),
        )
        await self._log_finance(user_id, delta, "balance_change",
                                f"{'credit' if delta > 0 else 'debit'} of {delta}")

    async def get_top_players(self, limit: int, offset: int) -> List[Tuple[int, int]]:
        rows = await self.fetch_all(
            "SELECT user_id, total FROM top ORDER BY total DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )
        return [(r["user_id"], r["total"]) for r in rows]

    async def get_total_players(self) -> int:
        row = await self.fetch_one("SELECT COUNT(*) AS cnt FROM top")
        return row["cnt"] if row else 0

    async def _log_finance(self, user_id: int, amount: int, type_: str, description: str = ""):
        await self._execute_write(
            "INSERT INTO finance_log (user_id, amount, type, description) VALUES (?, ?, ?, ?)",
            (user_id, amount, type_, description)
        )

    async def add_invoice(self, payload: str, user_id: int, bet: int, mines: int,
                          field_size: int, origin_chat_id: int, prepaid: int = 0) -> None:
        await self._execute_write(
            "INSERT INTO invoices (payload, user_id, bet, mines, field_size, created, status, origin_chat_id, prepaid) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (payload, user_id, bet, mines, field_size, time.time(), InvoiceStatus.PENDING, origin_chat_id, prepaid),
        )

    async def set_invoice_charge_id(self, payload: str, charge_id: str) -> None:
        await self._execute_write(
            "UPDATE invoices SET charge_id = ? WHERE payload = ?", (charge_id, payload)
        )

    async def set_invoice_paid(self, payload: str) -> None:
        await self._execute_write(
            "UPDATE invoices SET paid = 1, status = ? WHERE payload = ?",
            (InvoiceStatus.PAID, payload),
        )

    async def set_invoice_status(self, payload: str, status: str) -> None:
        await self._execute_write(
            "UPDATE invoices SET status = ? WHERE payload = ?", (status, payload)
        )

    async def set_invoice_message(self, payload: str, chat_id: int, message_id: int) -> None:
        await self._execute_write(
            "UPDATE invoices SET chat_id = ?, message_id = ? WHERE payload = ?",
            (chat_id, message_id, payload),
        )

    async def get_invoice(self, payload: str) -> Optional[Dict]:
        return await self.fetch_one("SELECT * FROM invoices WHERE payload = ?", (payload,))

    async def get_pending_paid_invoice(self, user_id: int) -> Optional[Dict]:
        return await self.fetch_one(
            "SELECT * FROM invoices WHERE user_id = ? AND status IN (?, ?) "
            "ORDER BY created DESC LIMIT 1",
            (user_id, InvoiceStatus.PAID, InvoiceStatus.QUEUED),
        )

    async def has_active_or_pending_invoice(self, user_id: int) -> bool:
        row = await self.fetch_one(
            "SELECT 1 FROM invoices WHERE user_id = ? AND status IN (?, ?, ?) LIMIT 1",
            (user_id, InvoiceStatus.PAID, InvoiceStatus.QUEUED, InvoiceStatus.ACTIVE),
        )
        return row is not None

    async def delete_invoice(self, payload: str) -> None:
        await self._execute_write("DELETE FROM invoices WHERE payload = ?", (payload,))

    async def get_invoices_by_status(self, status: str) -> List[Dict]:
        return await self.fetch_all(
            "SELECT * FROM invoices WHERE status = ?", (status,)
        )

    async def cleanup_expired_invoices(self, ttl: int, game_mgr) -> None:
        now = time.time()
        expired = await self.fetch_all(
            "SELECT * FROM invoices WHERE status = ? AND ? - created > ?",
            (InvoiceStatus.PENDING, now, ttl),
        )
        for inv in expired:
            user_id = inv["user_id"]
            if inv["prepaid"] > 0:
                await self.update_user_total(user_id, inv["prepaid"])
                await self._execute_write("DELETE FROM invoices WHERE payload = ?", (inv["payload"],))
            elif inv.get("charge_id"):
                success = await game_mgr.refund_stars(user_id, inv["charge_id"])
                if success:
                    await self._execute_write("DELETE FROM invoices WHERE payload = ?", (inv["payload"],))
                else:
                    await game_mgr.notify_owner(
                        f"⚠️ Не удался возврат {inv['bet']} ⭐ для {user_id} (charge_id={inv['charge_id']}). "
                        f"Требуется ручной возврат. Инвойс оставлен."
                    )
            else:
                await game_mgr.notify_owner(
                    f"⚠️ Просрочен неоплаченный инвойс {inv['payload']} для {user_id} на {inv['bet']} ⭐ (prepaid=0). Ручной возврат."
                )
                await self._execute_write("DELETE FROM invoices WHERE payload = ?", (inv["payload"],))

    async def get_queued_invoices(self) -> List[Dict]:
        return await self.fetch_all(
            "SELECT * FROM invoices WHERE status = ? ORDER BY created ASC",
            (InvoiceStatus.QUEUED,),
        )

    async def save_game(self, game: Dict) -> None:
        await self._execute_write(
            "INSERT OR REPLACE INTO active_games "
            "(user_id, bet, prepaid, mines, field_size, board, opened, step, active, message_id, chat_id, origin_chat_id, invoice_payload, created) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                game["user_id"], game["bet"], game["prepaid"], game["mines"], game["field_size"],
                json.dumps(game["board"]), json.dumps(game["opened"]), game["step"],
                1 if game["active"] else 0, game.get("message_id"), game.get("chat_id"),
                game.get("origin_chat_id"), game.get("invoice_payload"), game.get("created", time.time()),
            ),
        )

    async def deactivate_game(self, user_id: int) -> None:
        """Помечает игру как неактивную (завершена)."""
        await self._execute_write(
            "UPDATE active_games SET active = 0 WHERE user_id = ?", (user_id,)
        )

    async def delete_game(self, user_id: int) -> None:
        await self._execute_write("DELETE FROM active_games WHERE user_id = ?", (user_id,))

    async def load_active_games(self) -> List[Dict]:
        rows = await self.fetch_all("SELECT * FROM active_games WHERE active = 1")
        games = []
        for row in rows:
            try:
                g = dict(row)
                g["board"] = json.loads(g["board"])
                g["opened"] = json.loads(g["opened"])
                g["active"] = bool(g["active"])
                g["field_size"] = g.get("field_size", 3)
                g["prepaid"] = g.get("prepaid", 0)
                games.append(g)
            except Exception:
                logger.error(f"Corrupt game record for user {row['user_id']}, deleting.")
                await self.delete_game(row["user_id"])
        return games

    async def get_stale_games(self, timeout: int) -> List[Dict]:
        now = time.time()
        return await self.fetch_all(
            "SELECT * FROM active_games WHERE active = 1 AND ? - created > ?",
            (now, timeout),
        )

    async def clean_orphan_invoices(self, timeout: int, game_mgr) -> None:
        now = time.time()
        orphans = await self.fetch_all(
            "SELECT * FROM invoices WHERE status IN (?, ?) "
            "AND ? - created > ? AND payload NOT IN (SELECT invoice_payload FROM active_games WHERE active = 1)",
            (InvoiceStatus.PAID, InvoiceStatus.ACTIVE, now, timeout),
        )
        for inv in orphans:
            user_id = inv["user_id"]
            if inv["prepaid"] > 0:
                await self.update_user_total(user_id, inv["prepaid"])
                await self._execute_write("DELETE FROM invoices WHERE payload = ?", (inv["payload"],))
            elif inv.get("charge_id"):
                success = await game_mgr.refund_stars(user_id, inv["charge_id"])
                if success:
                    await self._execute_write("DELETE FROM invoices WHERE payload = ?", (inv["payload"],))
                else:
                    await game_mgr.notify_owner(
                        f"⚠️ Осиротевший инвойс {inv['payload']} для {user_id} на {inv['bet']} ⭐. "
                        f"Не удалось вернуть автоматически, требуется ручной возврат."
                    )
            else:
                await game_mgr.notify_owner(
                    f"⚠️ Осиротевший оплаченный инвойс {inv['payload']} для {user_id} на {inv['bet']} ⭐. Ручной возврат."
                )
                await self._execute_write("DELETE FROM invoices WHERE payload = ?", (inv["payload"],))

    # ---------- Промокоды ----------
    async def create_promo(self, code: str, stars: int, max_activations: int) -> None:
        await self._execute_write(
            "INSERT INTO promos (code, stars, max_activations) VALUES (?, ?, ?)",
            (code.upper(), stars, max_activations),
        )

    async def get_promo(self, code: str) -> Optional[Dict]:
        return await self.fetch_one("SELECT * FROM promos WHERE code = ?", (code.upper(),))

    async def activate_promo(self, user_id: int, code: str) -> Tuple[bool, str, bool]:
        async with self._write_lock:
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                promo = await self.fetch_one("SELECT * FROM promos WHERE code = ?", (code.upper(),))
                if not promo:
                    await self._conn.execute("ROLLBACK")
                    return False, "Промокод не найден.", False
                if promo["used_count"] >= promo["max_activations"]:
                    await self._conn.execute("ROLLBACK")
                    return False, "Промокод больше не действителен (лимит исчерпан).", False

                used = await self.fetch_one(
                    "SELECT 1 FROM promo_activations WHERE user_id = ? AND promo_code = ?",
                    (user_id, code.upper()),
                )
                if used:
                    await self._conn.execute("ROLLBACK")
                    return False, "Вы уже активировали этот промокод.", False

                await self._conn.execute(
                    "INSERT INTO top (user_id, total) VALUES (?, ?) "
                    "ON CONFLICT(user_id) DO UPDATE SET total = total + ?",
                    (user_id, promo["stars"], promo["stars"]),
                )
                await self._conn.execute(
                    "INSERT INTO finance_log (user_id, amount, type, description) VALUES (?, ?, ?, ?)",
                    (user_id, promo["stars"], "promo_activation", f"Code {code.upper()}")
                )
                await self._conn.execute(
                    "UPDATE promos SET used_count = used_count + 1 WHERE code = ?",
                    (code.upper(),),
                )
                await self._conn.execute(
                    "INSERT INTO promo_activations (user_id, promo_code) VALUES (?, ?)",
                    (user_id, code.upper()),
                )
                new_used = promo["used_count"] + 1
                exhausted = False
                if new_used >= promo["max_activations"]:
                    await self._conn.execute("DELETE FROM promos WHERE code = ?", (code.upper(),))
                    exhausted = True
                await self._conn.commit()
                return True, f"Промокод активирован! Вам начислено {promo['stars']} ⭐.", exhausted
            except Exception as e:
                logger.error(f"Promo activation error: {e}")
                await self._conn.execute("ROLLBACK")
                return False, "Ошибка сервера. Попробуйте позже.", False

    # ---------- Выводы ----------
    async def has_pending_withdrawal(self, user_id: int) -> bool:
        row = await self.fetch_one("SELECT 1 FROM withdrawals WHERE user_id = ? AND status = 'pending' LIMIT 1", (user_id,))
        return row is not None

    async def add_withdrawal(self, user_id: int, amount: int) -> Optional[int]:
        now = time.time()
        cursor = await self._execute_write(
            "INSERT INTO withdrawals (user_id, amount, created, status) VALUES (?, ?, ?, 'pending')",
            (user_id, amount, now)
        )
        return cursor.lastrowid

    async def get_withdrawal(self, withdraw_id: int) -> Optional[Dict]:
        return await self.fetch_one("SELECT * FROM withdrawals WHERE id = ?", (withdraw_id,))

    async def confirm_withdrawal(self, withdraw_id: int) -> bool:
        async with self._write_lock:
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                w = await self.fetch_one("SELECT * FROM withdrawals WHERE id = ?", (withdraw_id,))
                if not w or w["status"] != "pending":
                    await self._conn.execute("ROLLBACK")
                    return False

                total_row = await self.fetch_one("SELECT total FROM top WHERE user_id = ?", (w["user_id"],))
                if not total_row or total_row["total"] < w["amount"]:
                    await self._conn.execute("ROLLBACK")
                    logger.warning(f"Withdrawal {withdraw_id} failed: insufficient balance.")
                    return False

                await self._conn.execute("UPDATE top SET total = total - ? WHERE user_id = ?", (w["amount"], w["user_id"]))
                await self._conn.execute("UPDATE withdrawals SET status = 'confirmed' WHERE id = ?", (withdraw_id,))
                await self._conn.execute(
                    "INSERT INTO finance_log (user_id, amount, type, description) VALUES (?, ?, ?, ?)",
                    (w["user_id"], -w["amount"], "withdrawal_confirmed", f"Withdrawal #{withdraw_id}")
                )
                await self._conn.commit()
                return True
            except Exception as e:
                logger.error(f"Confirm withdrawal error: {e}")
                await self._conn.execute("ROLLBACK")
                return False

    async def try_mark_charge_processed(self, charge_id: str, user_id: int, amount: int) -> bool:
        """Атомарно пытается вставить charge_id. Возвращает True, если вставка произошла (первый раз)."""
        async with self._write_lock:
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = await self._conn.execute(
                    "INSERT INTO processed_charges (charge_id, user_id, amount) VALUES (?, ?, ?)",
                    (charge_id, user_id, amount)
                )
                if cursor.rowcount == 1:
                    await self._conn.commit()
                    return True
                else:
                    await self._conn.execute("ROLLBACK")
                    return False
            except aiosqlite.IntegrityError:
                await self._conn.execute("ROLLBACK")
                return False
            except Exception:
                await self._conn.execute("ROLLBACK")
                raise
            # ---------- Игровой менеджер ----------
class GameManager:
    def __init__(self, db: Database):
        self.db = db
        self.games: Dict[int, Dict] = {}
        self.user_state: Dict[int, Optional[UserState]] = {}
        self.user_temp_data: Dict[int, Dict] = {}
        self._locks: Dict[int, asyncio.Lock] = {}
        self._lock_creation = asyncio.Lock()
        self._last_cb_time: Dict[int, float] = {}
        self._rate_limit_lock = asyncio.Lock()
        self._name_cache: Dict[int, Dict] = {}
        self._name_cache_lock = asyncio.Lock()
        self.bot = None
        self.owner_chat_id: Optional[int] = None

    def set_bot(self, bot):
        self.bot = bot

    async def get_lock(self, user_id: int) -> asyncio.Lock:
        async with self._lock_creation:
            if user_id not in self._locks:
                self._locks[user_id] = asyncio.Lock()
        return self._locks[user_id]

    async def cleanup_unused_locks(self):
        async with self._lock_creation:
            removable = [
                uid for uid in list(self._locks.keys())
                if uid not in self.games and uid not in self.user_state
                and not await self.db.has_active_or_pending_invoice(uid)
            ]
            for uid in removable:
                del self._locks[uid]

    async def check_rate_limit(self, user_id: int) -> bool:
        async with self._rate_limit_lock:
            now = time.time()
            last = self._last_cb_time.get(user_id, 0)
            if now - last < config.RATE_LIMIT_SEC:
                return False
            self._last_cb_time[user_id] = now
            return True

    def create_game(self, user_id: int, bet: int, field_size: int, mines: int,
                    origin_chat_id: int, prepaid: int = 0,
                    invoice_payload: Optional[str] = None) -> Dict:
        total_cells = config.FIELD_SIZES[field_size]
        board = self._generate_board(field_size, mines)
        return {
            "user_id": user_id, "bet": bet, "prepaid": prepaid,
            "field_size": field_size, "mines": mines,
            "board": board, "opened": [False] * total_cells, "step": 0,
            "active": True, "message_id": None, "chat_id": None,
            "origin_chat_id": origin_chat_id, "invoice_payload": invoice_payload,
            "created": time.time(),
        }

    def _generate_board(self, field_size: int, mines: int) -> List[bool]:
        total_cells = config.FIELD_SIZES[field_size]
        idx = secrets.SystemRandom().sample(range(total_cells), mines)
        board = [False] * total_cells
        for i in idx:
            board[i] = True
        return board

    def current_multiplier(self, field_size: int, mines: int, step: int) -> float:
        seq = config.MULTIPLIERS[field_size][mines]
        return seq[min(step - 1, len(seq) - 1)]

    def build_field_markup(self, game: Dict, show_cashout: bool = True) -> InlineKeyboardMarkup:
        fs = game["field_size"]
        total = config.FIELD_SIZES[fs]
        buttons = []
        for i in range(0, total, fs):
            row = []
            for j in range(fs):
                idx = i + j
                if idx >= total:
                    break
                if game["opened"][idx]:
                    symbol = "💣" if game["board"][idx] else "💎"
                    row.append(InlineKeyboardButton(symbol, callback_data=f"dead_{game['user_id']}"))
                else:
                    row.append(InlineKeyboardButton("🟦", callback_data=f"cell_{idx}_{game['user_id']}"))
            buttons.append(row)
        if show_cashout and game["active"] and game["step"] > 0:
            buttons.append([InlineKeyboardButton("Забрать кэш и выйти", callback_data=f"cashout_{game['user_id']}")])
        return InlineKeyboardMarkup(buttons)

    async def get_user_name(self, context: ContextTypes.DEFAULT_TYPE, user_id: int) -> str:
        async with self._name_cache_lock:
            cached = self._name_cache.get(user_id)
            if cached and (time.time() - cached["ts"]) < config.NAME_CACHE_TTL:
                return cached["name"]
        try:
            chat = await context.bot.get_chat(user_id)
            name = chat.first_name or ""
            if chat.last_name:
                name += f" {chat.last_name}"
            if not name:
                name = f"player {user_id}"
        except Exception:
            name = f"player {user_id}"
        async with self._name_cache_lock:
            self._name_cache[user_id] = {"name": name, "ts": time.time()}
        return name

    async def notify_owner(self, text: str, reply_markup=None):
        if not self.bot:
            logger.error("Bot instance not set in GameManager")
            return
        if not self.owner_chat_id:
            await self._resolve_owner_chat_id()
        if not self.owner_chat_id:
            logger.error("Cannot resolve owner chat id, notification skipped")
            return
        try:
            await self.bot.send_message(chat_id=self.owner_chat_id, text=text, reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"Failed to notify owner: {e}")

    async def _resolve_owner_chat_id(self):
        if config.OWNER_USER_ID:
            self.owner_chat_id = config.OWNER_USER_ID
            return
        try:
            owner = await self.bot.get_chat(f"@{config.OWNER_USERNAME}")
            self.owner_chat_id = owner.id
        except Exception as e:
            logger.error(f"Failed to get owner chat_id: {e}")

    async def refund_stars(self, user_id: int, telegram_payment_charge_id: str) -> bool:
        try:
            await self.bot.refund_star_payment(
                user_id=user_id,
                telegram_payment_charge_id=telegram_payment_charge_id,
            )
            logger.info(f"Успешный возврат звёзд для {user_id} (charge_id={telegram_payment_charge_id})")
            return True
        except TelegramError as e:
            logger.error(f"Ошибка возврата звёзд для {user_id}: {e}")
            return False

    async def force_close_game(self, user_id: int) -> None:
        lock = await self.get_lock(user_id)
        async with lock:
            game = self.games.pop(user_id, None)
            if not game:
                return
            self.user_state.pop(user_id, None)

            # Помечаем игру неактивной в БД и удалим
            await self.db.deactivate_game(user_id)
            await self.db.delete_game(user_id)

            if game["prepaid"] > 0:
                await self.db.update_user_total(user_id, game["prepaid"])
            else:
                inv_payload = game.get("invoice_payload")
                if inv_payload:
                    inv = await self.db.get_invoice(inv_payload)
                    if inv and inv.get("charge_id"):
                        success = await self.refund_stars(user_id, inv["charge_id"])
                        if not success:
                            await self.notify_owner(
                                f"⚠️ Не удалось автоматически вернуть звёзды за игру {user_id}. "
                                f"charge_id={inv['charge_id']}, сумма {inv['bet']} ⭐."
                            )
                        await self.db.delete_invoice(inv_payload)
                else:
                    await self.notify_owner(
                        f"⚠️ Закрыта просроченная игра {user_id} на {game['bet']} ⭐ (prepaid=0). Ручной возврат."
                    )

            chat_id = game.get("chat_id")
            message_id = game.get("message_id")
            if chat_id and message_id:
                try:
                    await self.bot.delete_message(chat_id, message_id)
                except Exception:
                    pass
            try:
                if game["prepaid"] > 0:
                    msg = "Ваша игра была закрыта из-за длительного бездействия. Предоплаченная часть возвращена на баланс."
                else:
                    msg = "Ваша игра была закрыта. Обратитесь к администратору для возврата звёзд."
                await self.bot.send_message(user_id, msg)
            except Exception:
                pass

    async def launch_game(self, user_id: int, bet: int, fs: int, mines: int,
                          origin_chat_id: int, prepaid: int,
                          invoice_payload: Optional[str]) -> bool:
        """
        Создаёт игру: сначала отправляет сообщение, потом сохраняет в БД.
        При ошибке полностью откатывает финансовые операции и уведомляет владельца для возврата charge_id, если нужно.
        """
        game = self.create_game(user_id, bet, fs, mines, origin_chat_id, prepaid, invoice_payload)
        # 1. Отправляем сообщение
        try:
            markup = self.build_field_markup(game, show_cashout=False)
            msg = await asyncio.wait_for(
                self.bot.send_message(
                    chat_id=origin_chat_id,
                    text=f"Поле {fs}×{fs} | Доход: 0 ⭐",
                    reply_markup=markup
                ),
                timeout=config.REQUEST_TIMEOUT
            )
        except Exception as e:
            logger.error(f"Failed to send initial game message for {user_id}: {e}")
            # Откат: возврат prepaid + удаление инвойса (если есть) + уведомление владельца
            if prepaid > 0:
                await self.db.update_user_total(user_id, prepaid)
            if invoice_payload:
                inv = await self.db.get_invoice(invoice_payload)
                if inv and inv.get("charge_id"):
                    success = await self.refund_stars(user_id, inv["charge_id"])
                    if not success:
                        await self.notify_owner(
                            f"⚠️ КРИТИЧЕСКАЯ ОШИБКА: Не удалось отправить сообщение для игры пользователя {user_id}. "
                            f"Доплата (charge_id={inv['charge_id']}) не возвращена автоматически. "
                            f"Требуется ручной возврат {inv['bet'] - inv['prepaid']} ⭐."
                        )
                await self.db.delete_invoice(invoice_payload)
            elif prepaid == 0:
                await self.notify_owner(
                    f"⚠️ Не удалось запустить игру для {user_id}: ошибка отправки сообщения. prepaid=0, требуется ручной возврат."
                )
            return False

        game["message_id"] = msg.message_id
        game["chat_id"] = origin_chat_id

        # 2. Атомарно сохраняем игру в БД
        try:
            async with self.db._write_lock:
                await self.db._conn.execute("BEGIN IMMEDIATE")
                try:
                    await self.db._conn.execute(
                        "INSERT OR REPLACE INTO active_games "
                        "(user_id, bet, prepaid, mines, field_size, board, opened, step, active, message_id, chat_id, origin_chat_id, invoice_payload, created) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (user_id, bet, prepaid, mines, fs,
                         json.dumps(game["board"]), json.dumps(game["opened"]),
                         0, 1, msg.message_id, origin_chat_id, origin_chat_id, invoice_payload, game["created"]),
                    )
                    if invoice_payload:
                        await self.db._conn.execute(
                            "UPDATE invoices SET status = ? WHERE payload = ?",
                            (InvoiceStatus.ACTIVE, invoice_payload),
                        )
                    await self.db._conn.commit()
                except Exception:
                    await self.db._conn.execute("ROLLBACK")
                    raise
        except Exception as e:
            logger.error(f"DB error while launching game for {user_id}: {e}")
            # Удаляем отправленное сообщение
            try:
                await self.bot.delete_message(origin_chat_id, msg.message_id)
            except Exception:
                pass
            # Откат: возврат prepaid + удаление инвойса + уведомление владельца
            if prepaid > 0:
                await self.db.update_user_total(user_id, prepaid)
            if invoice_payload:
                inv = await self.db.get_invoice(invoice_payload)
                if inv and inv.get("charge_id"):
                    success = await self.refund_stars(user_id, inv["charge_id"])
                    if not success:
                        await self.notify_owner(
                            f"⚠️ КРИТИЧЕСКАЯ ОШИБКА: Ошибка БД при запуске игры для {user_id}. "
                            f"charge_id={inv['charge_id']} не возвращён автоматически. "
                            f"Требуется ручной возврат {inv['bet'] - inv['prepaid']} ⭐."
                        )
                await self.db.delete_invoice(invoice_payload)
            elif prepaid == 0:
                await self.notify_owner(
                    f"⚠️ Ошибка БД при запуске игры для {user_id}. prepaid=0, ручной возврат."
                )
            return False

        self.games[user_id] = game
        self.user_state[user_id] = UserState.PLAYING
        return True

    async def cleanup_caches(self):
        now = time.time()
        async with self._name_cache_lock:
            expired = [uid for uid, data in self._name_cache.items() if now - data["ts"] > config.NAME_CACHE_TTL]
            for uid in expired:
                del self._name_cache[uid]
        stale_uids = [uid for uid, temp in self.user_temp_data.items() if now - temp.get("ts", 0) > 300]
        for uid in stale_uids:
            lock = await self.get_lock(uid)
            async with lock:
                if uid in self.user_temp_data and now - self.user_temp_data[uid].get("ts", 0) > 300:
                    self.user_temp_data.pop(uid, None)
                    if self.user_state.get(uid) in (UserState.AWAITING_BET, UserState.AWAITING_FIELD, UserState.CHOOSING_MINES, UserState.READY):
                        self.user_state[uid] = None

# ---------- UI контроллер ----------
class UIController:
    @staticmethod
    async def safe_edit_or_resend(bot, game: Dict, text: str, markup: InlineKeyboardMarkup) -> None:
        chat_id = game.get("chat_id")
        message_id = game.get("message_id")
        if not chat_id or not message_id:
            await UIController._send_new_game_message(bot, game, text, markup)
            return
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=markup)
        except Exception as e:
            logger.warning(f"Edit failed for user {game['user_id']}: {e}, resending.")
            await UIController._resend(bot, game, text, markup)

    @staticmethod
    async def _send_new_game_message(bot, game: Dict, text: str, markup: InlineKeyboardMarkup):
        try:
            msg = await bot.send_message(chat_id=game["origin_chat_id"], text=text, reply_markup=markup)
            game["message_id"] = msg.message_id
            game["chat_id"] = msg.chat_id
        except Exception as e:
            logger.error(f"Cannot send game message for user {game['user_id']}: {e}")
            raise

    @staticmethod
    async def _resend(bot, game: Dict, text: str, markup: InlineKeyboardMarkup):
        try:
            await bot.delete_message(chat_id=game["chat_id"], message_id=game["message_id"])
        except Exception:
            pass
        await UIController._send_new_game_message(bot, game, text, markup)

# ---------- Игровые действия ----------
async def process_cell(game_mgr: GameManager, db: Database, game: Dict, idx: int) -> None:
    user_id = game["user_id"]
    if game["opened"][idx]:
        return
    game["opened"][idx] = True
    fs = game["field_size"]
    total = config.FIELD_SIZES[fs]

    if game["board"][idx]:  # мина
        game["active"] = False
        # Сначала помечаем игру неактивной в БД, затем удалим
        await db.deactivate_game(user_id)
        for i in range(total):
            if game["board"][i]:
                game["opened"][i] = True
        await db.delete_game(user_id)
        invoice_payload = game.pop("invoice_payload", None)
        game_mgr.games.pop(user_id, None)
        game_mgr.user_state.pop(user_id, None)
        if invoice_payload:
            await db.delete_invoice(invoice_payload)

        markup = game_mgr.build_field_markup(game, show_cashout=False)
        try:
            await UIController.safe_edit_or_resend(game_mgr.bot, game, "💥 Ты попал в мину!", markup)
        except Exception as e:
            logger.error(f"UI update failed during mine hit for {user_id}: {e}")
        try:
            await game_mgr.bot.send_message(user_id, "ахаххах спасибо за звездочки мабой, заходи еще",
                                           reply_markup=InlineKeyboardMarkup([
                                               [InlineKeyboardButton("Вернуться в меню", callback_data="back_to_hub")]
                                           ]))
        except Exception as e:
            logger.error(f"Lose notify error for {user_id}: {e}")
        await _launch_queued_if_any(game_mgr, db, user_id)
    else:
        game["step"] += 1
        await db.save_game(game)
        safe_cells = total - game["mines"]
        if game["step"] == safe_cells:  # победа
            profit = int(game["bet"] * game_mgr.current_multiplier(fs, game["mines"], game["step"]))
            game["active"] = False
            await db.deactivate_game(user_id)
            await db.update_user_total(user_id, profit)
            await db.delete_game(user_id)
            invoice_payload = game.pop("invoice_payload", None)
            game_mgr.games.pop(user_id, None)
            game_mgr.user_state.pop(user_id, None)
            if invoice_payload:
                await db.delete_invoice(invoice_payload)

            markup = game_mgr.build_field_markup(game, show_cashout=False)
            try:
                await UIController.safe_edit_or_resend(game_mgr.bot, game, f"Ваш доход: {profit} ⭐", markup)
            except Exception as e:
                logger.error(f"UI update failed during win for {user_id}: {e}")
            try:
                await game_mgr.bot.send_message(user_id, f"Поздравляю, вы ограбили Илюшу на {profit} звезд",
                                               reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Вернуться в меню", callback_data="back_to_hub")]]))
            except Exception as e:
                logger.error(f"Win notify error for {user_id}: {e}")
            await _launch_queued_if_any(game_mgr, db, user_id)
        else:
            profit = int(game["bet"] * game_mgr.current_multiplier(fs, game["mines"], game["step"]))
            text = f"Поле {fs}×{fs} | Доход: {profit} ⭐"
            markup = game_mgr.build_field_markup(game)
            try:
                await UIController.safe_edit_or_resend(game_mgr.bot, game, text, markup)
            except Exception as e:
                logger.error(f"UI update failed during game progress for {user_id}: {e}")

async def process_cashout(game_mgr: GameManager, db: Database, game: Dict) -> None:
    user_id = game["user_id"]
    if game["step"] == 0:
        return
    fs = game["field_size"]
    profit = int(game["bet"] * game_mgr.current_multiplier(fs, game["mines"], game["step"]))
    game["active"] = False
    await db.deactivate_game(user_id)
    await db.update_user_total(user_id, profit)
    await db.delete_game(user_id)
    invoice_payload = game.pop("invoice_payload", None)
    game_mgr.games.pop(user_id, None)
    game_mgr.user_state.pop(user_id, None)
    if invoice_payload:
        await db.delete_invoice(invoice_payload)

    markup = game_mgr.build_field_markup(game, show_cashout=False)
    try:
        await UIController.safe_edit_or_resend(game_mgr.bot, game, f"Вы забрали {profit} ⭐", markup)
    except Exception as e:
        logger.error(f"UI update failed during cashout for {user_id}: {e}")
    try:
        await game_mgr.bot.send_message(user_id, f"Вы успешно кэшнули {profit} звезд",
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Вернуться в меню", callback_data="back_to_hub")]]))
    except Exception as e:
        logger.error(f"Cashout notify error for {user_id}: {e}")
    await _launch_queued_if_any(game_mgr, db, user_id)

async def _launch_queued_if_any(game_mgr: GameManager, db: Database, user_id: int):
    inv = await db.get_pending_paid_invoice(user_id)
    if not inv or inv["status"] not in (InvoiceStatus.PAID, InvoiceStatus.QUEUED):
        return
    if user_id in game_mgr.games and game_mgr.games[user_id].get("active"):
        return
    if game_mgr.user_state.get(user_id) in (UserState.AWAITING_BET, UserState.AWAITING_FIELD, UserState.CHOOSING_MINES, UserState.READY):
        return

    bet = inv["bet"]
    mines = inv["mines"]
    fs = inv["field_size"]
    origin = inv.get("origin_chat_id") or user_id
    prepaid = inv["prepaid"]
    payload = inv["payload"]

    success = await game_mgr.launch_game(user_id, bet, fs, mines, origin, prepaid, payload)
    if not success:
        try:
            await game_mgr.bot.send_message(user_id, "Не удалось запустить отложенную игру. "
                                                    "Предоплаченная часть возвращена на баланс.")
        except Exception:
            pass

# ---------- Обработчики команд ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Используй /hub, чтобы войти в игровое лобби.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🎮 <b>Медовые мины</b> — игра на Telegram Stars.\n\n"
        "• <b>/hub</b> — главное меню\n"
        "• Выберите размер поля (3x3, 4x4, 5x5), количество мин и сделайте ставку.\n"
        "• Открывайте ячейки, избегая мин. Можно забрать выигрыш досрочно.\n"
        "• Баланс пополняется через игру и промокоды.\n"
        "• Вывод звёзд — через заявку (подтверждается владельцем).\n"
        "• <b>/cancel</b> — отменить создание игры (до оплаты).\n"
        "• <b>/promo КОД</b> — активировать промокод.\n"
    )
    await update.message.reply_text(text, parse_mode="HTML")

async def hub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    game_mgr: GameManager = context.bot_data["game_mgr"]
    db: Database = context.bot_data["db"]
    lock = await game_mgr.get_lock(user_id)

    async with lock:
        if update.callback_query:
            try:
                await update.callback_query.message.delete()
            except Exception:
                pass

        if user_id in game_mgr.games and game_mgr.games[user_id].get("active"):
            game = game_mgr.games[user_id]
            if game.get("origin_chat_id") != chat_id:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Перенести игру сюда", callback_data=f"transfer_game_{user_id}")],
                    [InlineKeyboardButton("Оставить как есть", callback_data="back_to_hub")],
                ])
                if update.callback_query:
                    await update.callback_query.edit_message_text(
                        f"У вас уже идёт игра в чате {game['origin_chat_id']}. Перенести?", reply_markup=kb
                    )
                    await update.callback_query.answer()
                else:
                    await update.message.reply_text(
                        f"У вас уже идёт игра в чате {game['origin_chat_id']}. Перенести?", reply_markup=kb
                    )
                return
            fs = game["field_size"]
            if not game.get("message_id"):
                profit = int(game["bet"] * game_mgr.current_multiplier(fs, game["mines"], game["step"])) if game["step"] else 0
                markup = game_mgr.build_field_markup(game)
                msg = await context.bot.send_message(chat_id=chat_id, text=f"Поле {fs}×{fs} | Доход: {profit} ⭐", reply_markup=markup)
                game["message_id"] = msg.message_id
                game["chat_id"] = chat_id
                await db.save_game(game)
            else:
                try:
                    await context.bot.edit_message_reply_markup(
                        chat_id=game["chat_id"], message_id=game["message_id"],
                        reply_markup=game_mgr.build_field_markup(game)
                    )
                except Exception:
                    text = f"Поле {fs}×{fs} | Доход: {int(game['bet'] * game_mgr.current_multiplier(fs, game['mines'], game['step'])) if game['step'] else 0} ⭐"
                    msg = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=game_mgr.build_field_markup(game))
                    game["message_id"] = msg.message_id
                    game["chat_id"] = chat_id
                    await db.save_game(game)
            game_mgr.user_state[user_id] = UserState.PLAYING
            if update.callback_query:
                await update.callback_query.answer()
            return

        pending_inv = await db.get_pending_paid_invoice(user_id)
        if pending_inv:
            if time.time() - pending_inv["created"] > config.INVOICE_TTL * 2:
                if pending_inv["prepaid"] > 0:
                    await db.update_user_total(user_id, pending_inv["prepaid"])
                elif pending_inv.get("charge_id"):
                    await game_mgr.refund_stars(user_id, pending_inv["charge_id"])
                else:
                    await game_mgr.notify_owner(
                        f"⚠️ Устаревший оплаченный инвойс {pending_inv['payload']} для {user_id}. Ручной возврат."
                    )
                await db.delete_invoice(pending_inv["payload"])
                await context.bot.send_message(chat_id, "Ваш старый платёж устарел. Начните новую игру.")
                return

            bet = pending_inv["bet"]
            mines = pending_inv["mines"]
            fs = pending_inv["field_size"]
            origin = pending_inv.get("origin_chat_id") or user_id
            prepaid = pending_inv["prepaid"]
            payload = pending_inv["payload"]

            success = await game_mgr.launch_game(user_id, bet, fs, mines, origin, prepaid, payload)
            if success:
                if update.callback_query:
                    await update.callback_query.answer()
            else:
                if update.callback_query:
                    await update.callback_query.answer("Ошибка запуска. Попробуйте /hub позже.")
            return

        game_mgr.user_state[user_id] = None
        game_mgr.user_temp_data.pop(user_id, None)

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Залудить хату", callback_data="start_game")],
            [InlineKeyboardButton("Топ ебланов", callback_data="show_top")],
            [InlineKeyboardButton("Мой баланс", callback_data="my_balance")],
            [InlineKeyboardButton("Наш канал (@dollarpunch)", url=config.CHANNEL_LINK)],
            [InlineKeyboardButton("Наш чат (@honeyholder)", url=config.CHAT_LINK)],
        ])
        caption = "Добро Пожаловать в игровое лобби, путник! Это тестовая версия бота, выбирай что тебе нужно"

        try:
            await context.bot.send_photo(chat_id=chat_id, photo=config.HUB_PHOTO_ID, caption=caption, reply_markup=kb)
        except Exception as e:
            logger.error(f"Failed to send photo: {e}, sending text fallback")
            await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=kb)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    game_mgr: GameManager = context.bot_data["game_mgr"]
    db: Database = context.bot_data["db"]
    lock = await game_mgr.get_lock(user_id)
    async with lock:
        if user_id in game_mgr.games and game_mgr.games[user_id].get("active"):
            await update.message.reply_text("Нельзя отменить игру во время игры. Закончите её.")
            return

        paid_inv = await db.get_pending_paid_invoice(user_id)
        if paid_inv:
            await update.message.reply_text(
                "У вас есть оплаченная игра, ожидающая запуска. Отменить её невозможно, "
                "используйте /hub, чтобы начать."
            )
            return

        pending_inv = await db.fetch_one(
            "SELECT * FROM invoices WHERE user_id = ? AND status = ?",
            (user_id, InvoiceStatus.PENDING),
        )
        if pending_inv:
            if pending_inv["prepaid"] > 0:
                await db.update_user_total(user_id, pending_inv["prepaid"])
            else:
                await game_mgr.notify_owner(
                    f"⚠️ Отменён неоплаченный инвойс {pending_inv['payload']} для {user_id} на {pending_inv['bet']} ⭐ (prepaid=0)."
                )
            await db.delete_invoice(pending_inv["payload"])
            game_mgr.user_temp_data.pop(user_id, None)
            if game_mgr.user_state.get(user_id) in (UserState.AWAITING_BET, UserState.AWAITING_FIELD, UserState.CHOOSING_MINES, UserState.READY):
                game_mgr.user_state[user_id] = None
            await update.message.reply_text("Ожидающий платёж отменён. /hub")
            # Запускаем queued инвойс, если есть
            await _launch_queued_if_any(game_mgr, db, user_id)
            return

        if game_mgr.user_state.get(user_id) in (UserState.AWAITING_BET, UserState.AWAITING_FIELD, UserState.CHOOSING_MINES, UserState.READY):
            game_mgr.user_state[user_id] = None
            game_mgr.user_temp_data.pop(user_id, None)
            await update.message.reply_text("Выбор сброшен. /hub")
            await _launch_queued_if_any(game_mgr, db, user_id)
            return

        await update.message.reply_text("Нет активных действий. /hub")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    game_mgr: GameManager = context.bot_data["game_mgr"]
    db: Database = context.bot_data["db"]

    if not await game_mgr.check_rate_limit(user_id):
        await query.answer("Слишком частые нажатия!", show_alert=False)
        return

    if data == "back_to_hub":
        await hub(update, context)
        return

    if data.startswith("top_page_"):
        try:
            page = int(data.split("_")[2])
        except (IndexError, ValueError):
            await query.answer("Некорректная страница")
            return
        top_players = await db.get_top_players(config.TOP_PAGE_SIZE, (page - 1) * config.TOP_PAGE_SIZE)
        total = await db.get_total_players()
        lines = []
        for i, (uid, stars) in enumerate(top_players, start=(page - 1) * config.TOP_PAGE_SIZE + 1):
            name = await game_mgr.get_user_name(context, uid)
            lines.append(f'{i}. <a href="tg://user?id={uid}">{html.escape(name)}</a> — {stars} ⭐')
        text = "Топ 10 солнечных лудиков проекта\n" + "\n".join(lines) if lines else "Пока никого нет."
        buttons = []
        if page > 1:
            buttons.append(InlineKeyboardButton("◀ Назад", callback_data=f"top_page_{page - 1}"))
        if (page * config.TOP_PAGE_SIZE) < total:
            buttons.append(InlineKeyboardButton("Вперёд ▶", callback_data=f"top_page_{page + 1}"))
        buttons.append(InlineKeyboardButton("В меню", callback_data="back_to_hub"))
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([buttons]), parse_mode="HTML")
        await query.answer()
        return

    async with await game_mgr.get_lock(user_id):
        if data == "start_game":
            if user_id in game_mgr.games and game_mgr.games[user_id].get("active"):
                await query.answer("Ты уже в игре!", show_alert=True)
                return
            if await db.has_active_or_pending_invoice(user_id):
                await query.answer("У тебя уже есть оплаченная игра.", show_alert=True)
                return
            game_mgr.user_state[user_id] = UserState.AWAITING_BET
            game_mgr.user_temp_data.pop(user_id, None)
            await query.answer()
            await query.message.reply_text("Введи сумму ставки (1-2500 ⭐):",
                                           reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data="cancel_bet")]]))
            return

        if data == "cancel_bet":
            if game_mgr.user_state.get(user_id) == UserState.AWAITING_BET:
                game_mgr.user_state[user_id] = None
                game_mgr.user_temp_data.pop(user_id, None)
                await query.message.reply_text("Выбор ставки отменён. /hub")
                await _launch_queued_if_any(game_mgr, db, user_id)
                await query.answer()
                return
            await query.answer("Нечего отменять.")
            return

        if data == "show_top":
            await query.answer()
            top_players = await db.get_top_players(config.TOP_PAGE_SIZE, 0)
            total = await db.get_total_players()
            lines = []
            for i, (uid, stars) in enumerate(top_players, start=1):
                name = await game_mgr.get_user_name(context, uid)
                lines.append(f'{i}. <a href="tg://user?id={uid}">{html.escape(name)}</a> — {stars} ⭐')
            text = "Топ 10 солнечных лудиков проекта\n" + "\n".join(lines) if lines else "Пока никого нет."
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("В меню", callback_data="back_to_hub")]
            ])
            await query.message.reply_text(text, reply_markup=kb, parse_mode="HTML")
            return

        if data == "my_balance":
            total = await db.get_user_total(user_id)
            text = f"Ваш баланс: {total} ⭐"
            if await db.has_pending_withdrawal(user_id):
                text += "\nУ вас уже есть активная заявка на вывод (ожидает подтверждения)."
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Вывод", callback_data="withdraw")],
                [InlineKeyboardButton("В меню", callback_data="back_to_hub")]
            ])
            await query.answer()
            if query.message:
                await query.message.reply_text(text, reply_markup=kb)
            return

        if data == "withdraw":
            await query.answer()
            total = await db.get_user_total(user_id)
            if total <= 0:
                await query.answer("Нет доступных для вывода звёзд.", show_alert=True)
                return
            if await db.has_pending_withdrawal(user_id):
                await query.answer("У вас уже есть заявка на вывод. Дождитесь подтверждения.", show_alert=True)
                return
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("50 звезд", callback_data="withdraw_50"),
                 InlineKeyboardButton("100 звезд", callback_data="withdraw_100"),
                 InlineKeyboardButton("1000 звезд", callback_data="withdraw_1000")],
                [InlineKeyboardButton("Назад", callback_data="my_balance")]
            ])
            if query.message:
                await query.message.reply_text("Выберите сумму вывода:", reply_markup=kb)
            return

        if data.startswith("withdraw_"):
            try:
                amount = int(data.split("_")[1])
            except (IndexError, ValueError):
                await query.answer("Некорректная сумма")
                return
            total = await db.get_user_total(user_id)
            if total < amount:
                await query.answer("Недостаточно звёзд для вывода.", show_alert=True)
                return
            if await db.has_pending_withdrawal(user_id):
                await query.answer("У вас уже есть заявка на вывод.", show_alert=True)
                return
            withdraw_id = await db.add_withdrawal(user_id, amount)
            user = query.from_user
            user_name = f"@{user.username}" if user.username else user.full_name
            owner_text = f"📤 {user_name} оформил вывод на {amount} звезд"
            owner_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Подтвердить", callback_data=f"confirm_withdraw_{withdraw_id}")]
            ])
            await game_mgr.notify_owner(owner_text, reply_markup=owner_kb)
            await query.answer("Заявка на вывод создана и отправлена на подтверждение.", show_alert=True)
            if query.message:
                await query.message.reply_text(f"Заявка на вывод {amount} ⭐ ожидает подтверждения.")
            return

        if data.startswith("confirm_withdraw_"):
            # Проверка владельца по ID или username
            is_owner = False
            if config.OWNER_USER_ID and query.from_user.id == config.OWNER_USER_ID:
                is_owner = True
            elif query.from_user.username and query.from_user.username.lower() == config.OWNER_USERNAME.lower():
                is_owner = True
            if not is_owner:
                await query.answer("Только владелец может подтверждать вывод.", show_alert=True)
                return
            try:
                w_id = int(data.split("_")[2])
            except (IndexError, ValueError):
                await query.answer("Неверный идентификатор заявки")
                return
            success = await db.confirm_withdrawal(w_id)
            if success:
                w = await db.get_withdrawal(w_id)
                try:
                    await game_mgr.bot.send_message(
                        chat_id=w["user_id"],
                        text=f"✅ Ваш вывод на {w['amount']} ⭐ успешно подтверждён и выполнен!"
                    )
                except Exception as e:
                    logger.error(f"Failed to notify user {w['user_id']} about withdrawal: {e}")
                await query.answer("Вывод подтверждён.", show_alert=True)
                await query.edit_message_text(
                    text=f"✅ Вывод {w['amount']} ⭐ для пользователя {w['user_id']} подтверждён.",
                    reply_markup=None
                )
            else:
                await query.answer("Ошибка подтверждения (возможно, недостаточно средств).", show_alert=True)
            return

        if data.startswith("field_"):
            if game_mgr.user_state.get(user_id) != UserState.AWAITING_FIELD:
                await query.answer("Сейчас не выбор поля", show_alert=True)
                return
            try:
                fs = int(data.split("_")[1])
            except (IndexError, ValueError):
                await query.answer("Некорректный размер")
                return
            temp = game_mgr.user_temp_data.get(user_id, {})
            bet = temp.get("bet")
            if not bet:
                await query.answer("Ставка не найдена", show_alert=True)
                game_mgr.user_state[user_id] = None
                return
            temp["field_size"] = fs
            temp["ts"] = time.time()
            game_mgr.user_temp_data[user_id] = temp
            game_mgr.user_state[user_id] = UserState.CHOOSING_MINES
            mines_list = config.MINE_OPTIONS[fs]
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{m} мин", callback_data=f"mines_{m}") for m in mines_list]
            ])
            await query.message.reply_text(f"Ставка: {bet} ⭐, поле {fs}×{fs}. Выбери количество мин:", reply_markup=kb)
            await query.answer()
            return

        if data.startswith("mines_"):
            if game_mgr.user_state.get(user_id) != UserState.CHOOSING_MINES:
                await query.answer("Сначала выбери поле и ставку!", show_alert=True)
                return
            mines = int(data.split("_")[1])
            temp = game_mgr.user_temp_data.get(user_id, {})
            bet = temp.get("bet")
            fs = temp.get("field_size")
            if not bet or not fs:
                await query.answer("Данные утеряны, начни заново /hub", show_alert=True)
                game_mgr.user_state[user_id] = None
                return
            if mines not in config.MINE_OPTIONS.get(fs, []):
                await query.answer("Недопустимое число мин", show_alert=True)
                return
            game_mgr.user_state[user_id] = UserState.READY
            temp["mines"] = mines
            temp["ts"] = time.time()
            game_mgr.user_temp_data[user_id] = temp
            await query.message.reply_text(
                f"Ставка: {bet} ⭐, поле {fs}×{fs}, {mines} мин(ы). Готов?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Начать игру!", callback_data="start_payment")],
                    [InlineKeyboardButton("Назад", callback_data="back_to_bet")],
                ])
            )
            await query.answer()
            return

        if data == "back_to_bet":
            if game_mgr.user_state.get(user_id) != UserState.READY:
                await query.answer("Не сейчас", show_alert=True)
                return
            game_mgr.user_state[user_id] = None
            game_mgr.user_temp_data.pop(user_id, None)
            await query.message.reply_text("Выбор сброшен. Используйте /hub для новой ставки.")
            await _launch_queued_if_any(game_mgr, db, user_id)
            await query.answer()
            return

        if data == "start_payment":
            if game_mgr.user_state.get(user_id) != UserState.READY:
                await query.answer("Сначала выбери ставку, поле и мины!", show_alert=True)
                return
            temp = game_mgr.user_temp_data.pop(user_id, {})
            bet = temp.get("bet")
            fs = temp.get("field_size")
            mines = temp.get("mines")
            if not bet or not fs or not mines:
                await query.answer("Ошибка данных. /hub", show_alert=True)
                game_mgr.user_state[user_id] = None
                return
            if await db.has_active_or_pending_invoice(user_id):
                await query.answer("Уже есть неоплаченный счёт или игра.", show_alert=True)
                return

            total = await db.get_user_total(user_id)
            origin_chat_id = update.effective_chat.id

            prepaid = min(total, bet)
            if prepaid > 0:
                remaining = bet - prepaid
                payload = str(uuid4()) if remaining > 0 else None
                # Атомарное списание и создание инвойса
                try:
                    async with db._write_lock:
                        await db._conn.execute("BEGIN IMMEDIATE")
                        try:
                            await db._conn.execute(
                                "UPDATE top SET total = total - ? WHERE user_id = ?",
                                (prepaid, user_id)
                            )
                            await db._conn.execute(
                                "INSERT INTO finance_log (user_id, amount, type, description) VALUES (?, ?, ?, ?)",
                                (user_id, -prepaid, "prepay_debit", f"Prepay for bet {bet}, prepaid={prepaid}")
                            )
                            if remaining > 0:
                                await db._conn.execute(
                                    "INSERT INTO invoices (payload, user_id, bet, mines, field_size, created, status, origin_chat_id, prepaid) "
                                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                    (payload, user_id, bet, mines, fs, time.time(), InvoiceStatus.PENDING, origin_chat_id, prepaid)
                                )
                            await db._conn.commit()
                        except Exception:
                            await db._conn.execute("ROLLBACK")
                            raise

                    if remaining == 0:
                        success = await game_mgr.launch_game(user_id, bet, fs, mines, origin_chat_id, prepaid, None)
                        if not success:
                            # Откат при ошибке запуска игры
                            await db.update_user_total(user_id, prepaid)
                            await db._log_finance(user_id, prepaid, "prepay_refund", "Refund after failed game launch")
                            await query.answer("Ошибка запуска игры, ставка возвращена.", show_alert=True)
                        else:
                            await query.answer("Игра началась за счёт вашего баланса!", show_alert=True)
                        return
                    else:
                        try:
                            prices = [LabeledPrice("Остаток ставки", int(remaining))]
                            msg = await asyncio.wait_for(
                                context.bot.send_invoice(
                                    chat_id=user_id,
                                    title="Медовые мины",
                                    description=f"Доплата {remaining} ⭐ (списано {prepaid} ⭐ с баланса)",
                                    payload=payload,
                                    provider_token="",
                                    currency="XTR",
                                    prices=prices,
                                ),
                                timeout=config.REQUEST_TIMEOUT
                            )
                            await db.set_invoice_message(payload, msg.chat_id, msg.message_id)
                            game_mgr.user_state[user_id] = None
                            await query.answer("Счёт на остаток суммы выставлен. После оплаты игра начнётся.", show_alert=True)
                        except Exception as e:
                            logger.error(f"Invoice send error {user_id}: {e}")
                            # Откат: удаляем инвойс, возвращаем prepaid
                            async with db._write_lock:
                                await db._conn.execute("BEGIN IMMEDIATE")
                                try:
                                    await db._conn.execute("DELETE FROM invoices WHERE payload = ?", (payload,))
                                    await db._conn.execute("UPDATE top SET total = total + ? WHERE user_id = ?", (prepaid, user_id))
                                    await db._conn.execute(
                                        "INSERT INTO finance_log (user_id, amount, type, description) VALUES (?, ?, ?, ?)",
                                        (user_id, prepaid, "prepay_refund", f"Refund after failed invoice send")
                                    )
                                    await db._conn.commit()
                                except Exception:
                                    await db._conn.execute("ROLLBACK")
                                    raise
                            await query.answer("Не удалось выставить счёт.", show_alert=True)
                        return
                except Exception as e:
                    logger.error(f"Payment preparation error: {e}")
                    await query.answer("Ошибка при подготовке платежа.", show_alert=True)
                    return
            else:
                payload = str(uuid4())
                await db.add_invoice(payload, user_id, bet, mines, fs, origin_chat_id, prepaid=0)
                try:
                    prices = [LabeledPrice("Ставка", int(bet))]
                    msg = await asyncio.wait_for(
                        context.bot.send_invoice(
                            chat_id=user_id,
                            title="Медовые мины",
                            description=f"Ставка {bet} ⭐, {mines} мин, поле {fs}×{fs}",
                            payload=payload,
                            provider_token="",
                            currency="XTR",
                            prices=prices,
                        ),
                        timeout=config.REQUEST_TIMEOUT
                    )
                    await db.set_invoice_message(payload, msg.chat_id, msg.message_id)
                    game_mgr.user_state[user_id] = None
                    await query.answer("Счёт выставлен. После оплаты игра начнётся в этом чате.", show_alert=True)
                except Exception as e:
                    logger.error(f"Invoice send error {user_id}: {e}")
                    await db.delete_invoice(payload)
                    await query.answer("Не удалось выставить счёт.", show_alert=True)
                return

        if data.startswith("transfer_game_"):
            target_uid = int(data.split("_")[2])
            if user_id != target_uid:
                await query.answer("Не ваша кнопка", show_alert=True)
                return
            game = game_mgr.games.get(user_id)
            if not game or not game["active"]:
                await query.answer("Нет активной игры", show_alert=True)
                return
            old_chat = game.get("origin_chat_id")
            new_chat = update.effective_chat.id
            game["origin_chat_id"] = new_chat
            game["chat_id"] = new_chat
            if game.get("message_id") and old_chat:
                try:
                    await context.bot.delete_message(old_chat, game["message_id"])
                except Exception:
                    pass
            fs = game["field_size"]
            profit = int(game["bet"] * game_mgr.current_multiplier(fs, game["mines"], game["step"])) if game["step"] else 0
            markup = game_mgr.build_field_markup(game)
            msg = await context.bot.send_message(chat_id=new_chat, text=f"Игра перенесена. Поле {fs}×{fs} | Доход: {profit} ⭐", reply_markup=markup)
            game["message_id"] = msg.message_id
            await db.save_game(game)
            await query.answer("Игра перенесена!", show_alert=True)
            try:
                await query.message.delete()
            except Exception:
                pass
            return

        if data.startswith("cell_"):
            parts = data.split("_")
            if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
                await query.answer("Некорректные данные")
                return
            owner_id = int(parts[2])
            if user_id != owner_id:
                await query.answer("Это не ваша игра!", show_alert=True)
                return
            game = game_mgr.games.get(user_id)
            if not game or not game["active"]:
                await query.answer("Игра не активна", show_alert=True)
                return
            idx = int(parts[1])
            await process_cell(game_mgr, db, game, idx)
            await query.answer()
            return

        if data.startswith("cashout_"):
            owner_id = int(data.split("_")[1])
            if user_id != owner_id:
                await query.answer("Это не ваша игра!", show_alert=True)
                return
            game = game_mgr.games.get(user_id)
            if not game or not game["active"]:
                await query.answer("Игра не активна", show_alert=True)
                return
            await process_cashout(game_mgr, db, game)
            await query.answer()
            return

        if data.startswith("dead_"):
            await query.answer("Ячейка уже открыта!")
            return

        await query.answer("Неизвестное действие.")

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    game_mgr: GameManager = context.bot_data["game_mgr"]
    db: Database = context.bot_data["db"]
    lock = await game_mgr.get_lock(user_id)
    async with lock:
        if game_mgr.user_state.get(user_id) != UserState.AWAITING_BET:
            return
        text = update.message.text.strip()
        if not text.isdigit():
            await update.message.reply_text("Целое число от 1 до 2500.")
            return
        bet = int(text)
        if bet < config.MIN_BET or bet > config.MAX_BET:
            await update.message.reply_text(f"Ставка от {config.MIN_BET} до {config.MAX_BET} звезд.")
            return
        game_mgr.user_temp_data[user_id] = {"bet": bet, "ts": time.time()}
        game_mgr.user_state[user_id] = UserState.AWAITING_FIELD
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{s}×{s}", callback_data=f"field_{s}") for s in [3, 4, 5]]
        ])
        await update.message.reply_text("Выбери размер поля:", reply_markup=kb)

async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    payload = query.invoice_payload
    user_id = query.from_user.id
    db: Database = context.bot_data["db"]
    game_mgr: GameManager = context.bot_data["game_mgr"]

    inv = await db.get_invoice(payload)
    if not inv:
        await query.answer(ok=False, error_message="Счёт не найден.")
        return
    if inv["user_id"] != user_id:
        await query.answer(ok=False, error_message="Этот счёт не ваш.")
        return
    if time.time() - inv["created"] > config.INVOICE_TTL:
        if inv["prepaid"] > 0:
            await db.update_user_total(user_id, inv["prepaid"])
            await db.delete_invoice(payload)
        elif inv.get("charge_id"):
            success = await game_mgr.refund_stars(user_id, inv["charge_id"])
            if success:
                await db.delete_invoice(payload)
            else:
                await game_mgr.notify_owner(
                    f"⚠️ Просрочен платёж {payload} для {user_id}, не удалось вернуть автоматически."
                )
        else:
            await game_mgr.notify_owner(
                f"⚠️ Просрочен платёж {payload} для {user_id}. Ручной возврат."
            )
            await db.delete_invoice(payload)
        await query.answer(ok=False, error_message="Время оплаты истекло.")
        return
    if inv["paid"]:
        await query.answer(ok=False, error_message="Счёт уже оплачен.")
        return
    lock = await game_mgr.get_lock(user_id)
    async with lock:
        if user_id in game_mgr.games and game_mgr.games[user_id].get("active"):
            await query.answer(ok=False, error_message="У вас уже есть активная игра.")
            return
    await query.answer(ok=True)

async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    payment = update.message.successful_payment
    payload = payment.invoice_payload
    total_amount = payment.total_amount
    charge_id = payment.telegram_payment_charge_id
    db: Database = context.bot_data["db"]
    game_mgr: GameManager = context.bot_data["game_mgr"]
    lock = await game_mgr.get_lock(user_id)

    async with lock:
        # Атомарная защита от дублирования платежа
        is_new = await db.try_mark_charge_processed(charge_id, user_id, total_amount)
        if not is_new:
            logger.warning(f"Duplicate successful_payment for charge_id {charge_id}, ignoring.")
            await update.message.reply_text("Этот платёж уже был обработан.")
            return

        inv = await db.get_invoice(payload)
        if not inv:
            logger.warning(f"Payment without invoice for {user_id}")
            await update.message.reply_text("Ошибка: платёж не найден.")
            return
        if inv["paid"]:
            await update.message.reply_text("Этот счёт уже был оплачен ранее.")
            return
        expected_paid = inv["bet"] - inv["prepaid"]
        if total_amount != expected_paid:
            logger.error(f"Amount mismatch {total_amount} vs {expected_paid}")
            if inv["prepaid"] > 0:
                await db.update_user_total(user_id, inv["prepaid"])
            else:
                await game_mgr.notify_owner(
                    f"⚠️ Несовпадение суммы платежа {payload} для {user_id}. Ручной возврат."
                )
            await db.delete_invoice(payload)
            await update.message.reply_text("Ошибка суммы платежа. Средства возвращены на баланс (если предоплата была).")
            return

        await db.set_invoice_charge_id(payload, charge_id)
        await db.set_invoice_paid(payload)

        full_bet = inv["bet"]
        mines = inv["mines"]
        fs = inv["field_size"]
        origin = inv.get("origin_chat_id") or user_id
        prepaid = inv["prepaid"]

        if game_mgr.user_state.get(user_id) in (UserState.AWAITING_BET, UserState.AWAITING_FIELD, UserState.CHOOSING_MINES, UserState.READY) or \
           (user_id in game_mgr.games and game_mgr.games[user_id].get("active")):
            await db.set_invoice_status(payload, InvoiceStatus.QUEUED)
            await update.message.reply_text("Платёж получен, игра начнётся после завершения текущих действий.")
            return

        success = await game_mgr.launch_game(user_id, full_bet, fs, mines, origin, prepaid, payload)
        if success:
            await update.message.reply_text(f"Игра запущена в чате {origin}.")
        else:
            await update.message.reply_text("Платёж получен, но не удалось запустить игру. "
                                           "Обратитесь к администратору.")

# ---------- Promo handlers ----------
async def setpromo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    is_owner = False
    if config.OWNER_USER_ID and user.id == config.OWNER_USER_ID:
        is_owner = True
    elif user.username and user.username.lower() == config.OWNER_USERNAME.lower():
        is_owner = True
    if not is_owner:
        await update.message.reply_text("Только владелец бота может создавать промокоды.")
        return
    args = context.args
    if len(args) != 3:
        await update.message.reply_text("Использование: /setpromo <код> <звёзды> <макс_активаций>")
        return
    code = args[0].upper()
    try:
        stars = int(args[1])
        max_use = int(args[2])
    except ValueError:
        await update.message.reply_text("Звёзды и количество активаций должны быть целыми числами.")
        return
    if stars <= 0 or max_use <= 0:
        await update.message.reply_text("Значения должны быть положительными.")
        return
    db: Database = context.bot_data["db"]
    await db.create_promo(code, stars, max_use)
    await update.message.reply_text(
        f"Промокод {code} создан: {stars} ⭐, можно активировать {max_use} раз(а)."
    )

async def promo_activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = update.effective_user
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Использование: /promo <код>")
        return
    code = args[0].upper()
    db: Database = context.bot_data["db"]
    game_mgr: GameManager = context.bot_data["game_mgr"]
    success, msg, exhausted = await db.activate_promo(user_id, code)
    await update.message.reply_text(msg)

    if success:
        user_name = f"{user.full_name} (@{user.username})" if user.username else user.full_name
        await game_mgr.notify_owner(
            f"📥 Промокод {code} активирован пользователем {user_name} (ID {user_id}) "
            f"в {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}"
        )
        if exhausted:
            await game_mgr.notify_owner(
                f"🚫 Промокод {code} исчерпал лимит активаций и был удалён."
            )

# ---------- Периодические задачи ----------
async def cleanup_job(context: ContextTypes.DEFAULT_TYPE):
    db: Database = context.bot_data["db"]
    game_mgr: GameManager = context.bot_data["game_mgr"]
    await db.cleanup_expired_invoices(config.INVOICE_TTL, game_mgr)
    stale_games = await db.get_stale_games(config.STALE_GAME_TIMEOUT)
    for g in stale_games:
        await game_mgr.force_close_game(g["user_id"])
    await db.clean_orphan_invoices(config.ORPHAN_INVOICE_TIMEOUT, game_mgr)

async def cache_cleanup_job(context: ContextTypes.DEFAULT_TYPE):
    game_mgr: GameManager = context.bot_data["game_mgr"]
    await game_mgr.cleanup_caches()
    await game_mgr.cleanup_unused_locks()

async def on_startup(app):
    db: Database = app.bot_data["db"]
    game_mgr: GameManager = app.bot_data["game_mgr"]
    game_mgr.set_bot(app.bot)
    await game_mgr._resolve_owner_chat_id()

    # Безопасное преобразование статусов ACTIVE->QUEUED только для орфанов
    active_invoices = await db.get_invoices_by_status(InvoiceStatus.ACTIVE)
    for inv in active_invoices:
        game_exists = await db.fetch_one(
            "SELECT 1 FROM active_games WHERE invoice_payload = ? AND active = 1",
            (inv["payload"],)
        )
        if not game_exists:
            await db.set_invoice_status(inv["payload"], InvoiceStatus.QUEUED)
            logger.info(f"Orphan active invoice {inv['payload']} moved to queued on startup.")

    active_games = await db.load_active_games()
    for g in active_games:
        game_mgr.games[g["user_id"]] = g
        game_mgr.user_state[g["user_id"]] = UserState.PLAYING
        if g.get("invoice_payload"):
            await db.set_invoice_status(g["invoice_payload"], InvoiceStatus.ACTIVE)

    queued = await db.get_queued_invoices()
    for inv in queued:
        user_id = inv["user_id"]
        if user_id in game_mgr.games and game_mgr.games[user_id].get("active"):
            continue
        bet = inv["bet"]
        fs = inv["field_size"]
        mines = inv["mines"]
        origin = inv.get("origin_chat_id") or user_id
        prepaid = inv["prepaid"]
        payload = inv["payload"]
        success = await game_mgr.launch_game(user_id, bet, fs, mines, origin, prepaid, payload)
        if not success:
            logger.error(f"Startup queued launch fail for {user_id}")

async def main():
    db = Database(config.DB_FILE)
    await db.connect()
    game_mgr = GameManager(db)

    app = ApplicationBuilder().token(config.BOT_TOKEN).build()
    app.bot_data["db"] = db
    app.bot_data["game_mgr"] = game_mgr

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("hub", hub))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("setpromo", setpromo))
    app.add_handler(CommandHandler("promo", promo_activate))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(PreCheckoutQueryHandler(pre_checkout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))

    if app.job_queue:
        app.job_queue.run_repeating(cleanup_job, interval=300, first=10)
        app.job_queue.run_repeating(cache_cleanup_job, interval=600, first=30)

    app.post_init = on_startup

    logger.info("Бот запущен с повышенной надёжностью.")
    await app.run_polling()
    await db.close()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
