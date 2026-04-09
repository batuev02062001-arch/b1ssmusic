import asyncio
import logging
import os
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    BufferedInputFile
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.storage.base import StorageKey, BaseStorage, StateType
from typing import Any, Dict, Optional, cast
import json, pathlib


class SimpleFileStorage(BaseStorage):
    """Простое FSM-хранилище на основе JSON-файла — переживает перезапуски."""

    def __init__(self, path: str = "fsm_storage.json"):
        self._path = pathlib.Path(path)
        self._data: Dict[str, Any] = {}
        if self._path.exists():
            try:
                self._data = json.loads(self._path.read_text())
            except Exception:
                self._data = {}

    def _key(self, key: StorageKey) -> str:
        return f"{key.bot_id}:{key.chat_id}:{key.user_id}"

    def _save(self):
        self._path.write_text(json.dumps(self._data))

    async def set_state(self, key: StorageKey, state: StateType = None):
        k = self._key(key)
        if k not in self._data:
            self._data[k] = {"state": None, "data": {}}
        self._data[k]["state"] = state.state if hasattr(state, "state") else state
        self._save()

    async def get_state(self, key: StorageKey) -> Optional[str]:
        return self._data.get(self._key(key), {}).get("state")

    async def set_data(self, key: StorageKey, data: Dict[str, Any]):
        k = self._key(key)
        if k not in self._data:
            self._data[k] = {"state": None, "data": {}}
        self._data[k]["data"] = data
        self._save()

    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        return self._data.get(self._key(key), {}).get("data", {})

    async def close(self):
        pass

from database import Database, LIBRARY_LIMIT, PLAYLIST_LIMIT, PLAYLIST_TRACKS_LIMIT
from search import search_soundcloud, download_track

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# ── ВСТАВЬ СВОИ ДАННЫЕ ──────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID  = int(os.getenv("OWNER_ID", "0"))   # твой Telegram user_id
# ────────────────────────────────────────────────────────

bot     = Bot(token=BOT_TOKEN)
storage = SimpleFileStorage("fsm_storage.json")
dp      = Dispatcher(storage=storage)
db      = Database("library.db")

# Режим технических работ (True = бот отключён для всех кроме владельца)
maintenance_mode = False


# ══════════════════════════════════════════
#  STATES
# ══════════════════════════════════════════

class States(StatesGroup):
    searching              = State()
    playlist_naming        = State()
    playlist_select_tracks = State()
    playlist_renaming      = State()
    support_writing        = State()
    owner_replying         = State()
    broadcast_waiting      = State()


# ══════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════

def is_owner(user_id: int) -> bool:
    return OWNER_ID != 0 and user_id == OWNER_ID


RESERVED_TEXTS = {
    "📚 Библиотека", "🎵 Плейлисты", "🎲 Случайный трек",
    "ℹ️ Помощь", "🔍 Поиск", "🆘 Поддержка", "👑 Панель владельца",
    "🔴 Выключить бота", "🟢 Включить бота"
}

INPUT_STATES = {
    States.playlist_naming.state,
    States.playlist_renaming.state,
    States.support_writing.state,
    States.owner_replying.state,
    States.broadcast_waiting.state,
}


# ══════════════════════════════════════════
#  KEYBOARDS
# ══════════════════════════════════════════

def kb_main():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 Поиск"),        KeyboardButton(text="📚 Библиотека")],
            [KeyboardButton(text="🎵 Плейлисты"),    KeyboardButton(text="🎲 Случайный трек")],
            [KeyboardButton(text="🆘 Поддержка"),    KeyboardButton(text="ℹ️ Помощь")],
        ],
        resize_keyboard=True
    )


def kb_owner_main():
    status = "🔴 Выключить бота" if not maintenance_mode else "🟢 Включить бота"
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 Поиск"),        KeyboardButton(text="📚 Библиотека")],
            [KeyboardButton(text="🎵 Плейлисты"),    KeyboardButton(text="🎲 Случайный трек")],
            [KeyboardButton(text="🆘 Поддержка"),    KeyboardButton(text="ℹ️ Помощь")],
            [KeyboardButton(text="👑 Панель владельца"), KeyboardButton(text=status)],
        ],
        resize_keyboard=True
    )


def kb_results(tracks: list, query: str):
    buttons = []
    for i, t in enumerate(tracks):
        label = f"{t['artist']} — {t['title']}"
        if len(label) > 50: label = label[:47] + "..."
        buttons.append([InlineKeyboardButton(
            text=f"▶️ {label}", callback_data=f"play:{i}:{query[:30]}"
        )])
    buttons.append([InlineKeyboardButton(text="❌ Закрыть", callback_data="close")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_track_actions(track_id: str, in_library: bool):
    lib_btn = (
        InlineKeyboardButton(text="🗑 Убрать из библиотеки", callback_data=f"remove:{track_id}")
        if in_library else
        InlineKeyboardButton(text="💾 В библиотеку", callback_data=f"save:{track_id}")
    )
    return InlineKeyboardMarkup(inline_keyboard=[[lib_btn]])


def kb_library(tracks: list, page: int = 0, page_size: int = 5):
    total = len(tracks)
    start = page * page_size
    end   = min(start + page_size, total)
    buttons = []
    for t in tracks[start:end]:
        label = f"🎵 {t['artist']} — {t['title']}"
        if len(label) > 52: label = label[:49] + "..."
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"libplay:{t['track_id']}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"libpage:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"libpage:{page+1}"))
    if nav: buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="🗑 Очистить всё", callback_data="lib_clear")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_playlists(playlists: list, page: int = 0, page_size: int = 6):
    total = len(playlists)
    start = page * page_size
    end   = min(start + page_size, total)
    buttons = []
    for pl in playlists[start:end]:
        label = f"🎶 {pl['name']} ({pl['track_count']} тр.)"
        if len(label) > 52: label = label[:49] + "..."
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"pl_open:{pl['playlist_id']}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"plpage:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"plpage:{page+1}"))
    if nav: buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="➕ Новый плейлист", callback_data="pl_create")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_playlist_detail(playlist_id: int, tracks: list, page: int = 0, page_size: int = 5):
    total = len(tracks)
    start = page * page_size
    end   = min(start + page_size, total)
    buttons = []
    for t in tracks[start:end]:
        label = f"▶️ {t['artist']} — {t['title']}"
        if len(label) > 52: label = label[:49] + "..."
        buttons.append([
            InlineKeyboardButton(text=label,  callback_data=f"plplay:{t['track_id']}"),
            InlineKeyboardButton(text="✖️",   callback_data=f"pl_rmtrack:{playlist_id}:{t['track_id']}"),
        ])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"pltrpage:{playlist_id}:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"pltrpage:{playlist_id}:{page+1}"))
    if nav: buttons.append(nav)
    buttons.append([
        InlineKeyboardButton(text="➕ Добавить треки", callback_data=f"pl_addtracks:{playlist_id}"),
        InlineKeyboardButton(text="✏️ Переименовать",  callback_data=f"pl_rename:{playlist_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="🗑 Удалить плейлист", callback_data=f"pl_delete:{playlist_id}"),
        InlineKeyboardButton(text="« Назад",             callback_data="pl_back"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_select_tracks(library_tracks: list, selected_ids: set,
                     playlist_id: int, page: int = 0, page_size: int = 6):
    total = len(library_tracks)
    start = page * page_size
    end   = min(start + page_size, total)
    buttons = []
    for t in library_tracks[start:end]:
        tid   = t["track_id"]
        check = "✅" if tid in selected_ids else "⬜"
        label = f"{check} {t['artist']} — {t['title']}"
        if len(label) > 52: label = label[:49] + "..."
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"sel_toggle:{tid}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"sel_page:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"sel_page:{page+1}"))
    if nav: buttons.append(nav)
    count_label = f"✔️ Добавить ({len(selected_ids)})" if selected_ids else "✔️ Добавить"
    buttons.append([
        InlineKeyboardButton(text=count_label, callback_data="sel_confirm"),
        InlineKeyboardButton(text="❌ Отмена",  callback_data="sel_cancel"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_reply_to_user(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💬 Ответить", callback_data=f"owner_reply:{user_id}")
    ]])


# ══════════════════════════════════════════
#  SEND AUDIO
# ══════════════════════════════════════════

async def send_audio_track(chat_id: int, track: dict, in_library: bool = False):
    track_id   = track.get("track_id") or track.get("url", "").split("/")[-1]
    status_msg = await bot.send_message(chat_id, "⏳ Загружаю трек...")
    audio_bytes = await download_track(track)
    if audio_bytes is None:
        await status_msg.edit_text(
            f"😔 Не удалось загрузить *{track['artist']} — {track['title']}*\nПопробуй другой.",
            parse_mode="Markdown"
        )
        return
    await status_msg.delete()
    await bot.send_audio(
        chat_id=chat_id,
        audio=BufferedInputFile(audio_bytes, filename=f"{track['artist']} - {track['title']}.mp3"),
        title=track["title"],
        performer=track["artist"],
        duration=track.get("duration_sec"),
        caption=f"🎵 *{track['artist']}* — {track['title']}",
        parse_mode="Markdown",
        reply_markup=kb_track_actions(track_id, in_library)
    )


# ══════════════════════════════════════════
#  BAN MIDDLEWARE
# ══════════════════════════════════════════

@dp.message.outer_middleware()
async def ban_check_middleware(handler, event: Message, data: dict):
    user_id = event.from_user.id
    # Бан
    if not is_owner(user_id) and db.is_banned(user_id):
        await event.answer("🚫 Вы заблокированы в этом боте.")
        return
    # Технические работы
    if maintenance_mode and not is_owner(user_id):
        await event.answer(
            "🔧 *Технические работы*\n\nБот временно недоступен. Попробуй позже.",
            parse_mode="Markdown"
        )
        return
    return await handler(event, data)


# ══════════════════════════════════════════
#  START / HELP / CANCEL
# ══════════════════════════════════════════

@dp.message(CommandStart())
async def cmd_start(message: Message):
    db.ensure_user(message.from_user.id)
    kb = kb_owner_main() if is_owner(message.from_user.id) else kb_main()
    await message.answer(
        f"👋 Привет, *{message.from_user.first_name}*!\n\n"
        "🎵 Я музыкальный бот — ищу треки на SoundCloud и отправляю прямо сюда.\n\n"
        "💾 Понравился трек? Сохраняй в *библиотеку* и создавай *плейлисты*.\n\n"
        "Просто напиши название трека или исполнителя 👇",
        parse_mode="Markdown",
        reply_markup=kb
    )


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current = await state.get_state()
    if current:
        await state.clear()
        kb = kb_owner_main() if is_owner(message.from_user.id) else kb_main()
        await message.answer("❌ Действие отменено.", reply_markup=kb)
    else:
        await message.answer("Нечего отменять.")


@dp.message(Command("help"))
@dp.message(F.text == "ℹ️ Помощь")
async def cmd_help(message: Message):
    kb = kb_owner_main() if is_owner(message.from_user.id) else kb_main()
    await message.answer(
        "📖 *Как пользоваться:*\n\n"
        "🔍 *Поиск* — введи название трека или исполнителя\n"
        "▶️ Нажми на трек из списка — бот пришлёт аудио\n"
        f"💾 *В библиотеку* — сохранить трек (макс. {LIBRARY_LIMIT})\n"
        "📚 *Библиотека* — все сохранённые треки\n"
        "🎵 *Плейлисты* — создавай и управляй плейлистами\n"
        f"   └ Макс. {PLAYLIST_LIMIT} плейлистов, по {PLAYLIST_TRACKS_LIMIT} треков\n"
        "🎲 *Случайный трек* — случайный из библиотеки\n"
        "🆘 *Поддержка* — написать нам\n\n"
        "*Команды:*\n"
        "/start — главное меню\n"
        "/help — эта справка\n"
        "/cancel — отменить текущее действие",
        parse_mode="Markdown",
        reply_markup=kb
    )


# ══════════════════════════════════════════
#  SEARCH
# ══════════════════════════════════════════

@dp.message(F.text == "🔍 Поиск")
async def btn_search(message: Message, state: FSMContext):
    await state.set_state(States.searching)
    await message.answer("🔍 Введите название трека или исполнителя:")


@dp.message(States.searching)
async def do_search(message: Message, state: FSMContext):
    await state.clear()
    await process_query(message, message.text.strip())


@dp.message(States.support_writing, F.text | F.photo | F.video | F.document)
async def support_send_message(message: Message, state: FSMContext):
    await state.clear()
    user     = message.from_user
    username = f"@{user.username}" if user.username else "нет username"

    if OWNER_ID == 0:
        await message.answer("⚠️ Поддержка временно недоступна.")
        return

    await bot.send_message(
        OWNER_ID,
        f"🆘 *Новое обращение в поддержку*\n"
        f"👤 {user.full_name} ({username})\n"
        f"🆔 `{user.id}`\n"
        f"{'─' * 25}",
        parse_mode="Markdown"
    )
    await message.forward(OWNER_ID)
    await bot.send_message(
        OWNER_ID,
        f"👆 Сообщение от `{user.id}`",
        parse_mode="Markdown",
        reply_markup=kb_reply_to_user(user.id)
    )
    await message.answer("✅ Обращение отправлено! Мы ответим в ближайшее время. 🙏")


@dp.message(States.owner_replying, F.text | F.photo | F.video | F.document)
async def owner_send_reply_early(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    data      = await state.get_data()
    target_id = data.get("reply_target")
    await state.clear()
    try:
        await bot.send_message(target_id, "📩 *Ответ от поддержки:*", parse_mode="Markdown")
        await message.copy_to(target_id)
        await message.answer(f"✅ Ответ отправлен пользователю `{target_id}`.", parse_mode="Markdown")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить: `{e}`", parse_mode="Markdown")


@dp.message(F.text & ~F.text.startswith("/") & ~F.text.in_(RESERVED_TEXTS))
async def auto_search(message: Message, state: FSMContext):
    current = await state.get_state()
    if current is not None:
        return
    await process_query(message, message.text.strip())


async def process_query(message: Message, query: str):
    if len(query) < 2:
        return
    db.ensure_user(message.from_user.id)
    msg = await message.answer(f"🔍 Ищу: *{query}*...", parse_mode="Markdown")
    tracks = await search_soundcloud(query, limit=8)
    if not tracks:
        await msg.edit_text("😔 Ничего не найдено. Попробуй другой запрос.")
        return
    for t in tracks:
        db.upsert_track(t)
    await msg.edit_text(
        f"🎵 Найдено *{len(tracks)}* треков по запросу «{query}»:",
        parse_mode="Markdown",
        reply_markup=kb_results(tracks, query)
    )


@dp.callback_query(F.data.startswith("play:"))
async def callback_play(callback: CallbackQuery):
    _, idx_str, query = callback.data.split(":", 2)
    idx = int(idx_str)
    await callback.answer("⏳ Загружаю...")
    await callback.message.edit_reply_markup(reply_markup=None)
    tracks = await search_soundcloud(query, limit=8)
    if idx >= len(tracks):
        await callback.message.answer("❌ Трек не найден, попробуй снова.")
        return
    track = tracks[idx]
    db.upsert_track(track)
    track_id   = track.get("track_id") or track["url"].split("/")[-1]
    in_library = db.is_in_library(callback.from_user.id, track_id)
    await send_audio_track(callback.from_user.id, track, in_library)


# ══════════════════════════════════════════
#  LIBRARY
# ══════════════════════════════════════════

@dp.message(F.text == "📚 Библиотека")
async def show_library(message: Message):
    db.ensure_user(message.from_user.id)
    tracks = db.get_library(message.from_user.id)
    if not tracks:
        await message.answer(
            "📚 Твоя библиотека пуста.\n\nНайди трек и нажми *💾 В библиотеку*.",
            parse_mode="Markdown"
        )
        return
    await message.answer(
        f"📚 *Твоя библиотека* — {len(tracks)}/{LIBRARY_LIMIT} треков:",
        parse_mode="Markdown",
        reply_markup=kb_library(tracks)
    )


@dp.callback_query(F.data.startswith("libpage:"))
async def library_page(callback: CallbackQuery):
    page   = int(callback.data.split(":")[1])
    tracks = db.get_library(callback.from_user.id)
    await callback.message.edit_reply_markup(reply_markup=kb_library(tracks, page))
    await callback.answer()


@dp.callback_query(F.data.startswith("libplay:"))
async def library_play(callback: CallbackQuery):
    track_id = callback.data.split(":", 1)[1]
    track    = db.get_track(track_id)
    if not track:
        await callback.answer("❌ Трек не найден в базе.", show_alert=True)
        return
    await callback.answer("⏳ Загружаю...")
    await send_audio_track(callback.from_user.id, track, in_library=True)


@dp.callback_query(F.data.startswith("save:"))
async def save_to_library(callback: CallbackQuery):
    track_id = callback.data.split(":", 1)[1]
    track    = db.get_track(track_id)
    if not track:
        await callback.answer("❌ Трек не найден.", show_alert=True)
        return
    ok, status = db.add_to_library(callback.from_user.id, track_id)
    if status == "limit":
        await callback.answer(f"⛔ Библиотека заполнена! Максимум {LIBRARY_LIMIT} треков.", show_alert=True)
        return
    if status == "already":
        await callback.answer("ℹ️ Трек уже в библиотеке.")
        return
    count = db.library_count(callback.from_user.id)
    await callback.answer(f"✅ Добавлено! В библиотеке {count}/{LIBRARY_LIMIT}")
    await callback.message.edit_reply_markup(reply_markup=kb_track_actions(track_id, in_library=True))


@dp.callback_query(F.data.startswith("remove:"))
async def remove_from_library(callback: CallbackQuery):
    track_id = callback.data.split(":", 1)[1]
    db.remove_from_library(callback.from_user.id, track_id)
    await callback.answer("🗑 Удалено из библиотеки.")
    await callback.message.edit_reply_markup(reply_markup=kb_track_actions(track_id, in_library=False))


@dp.callback_query(F.data == "lib_clear")
async def clear_library(callback: CallbackQuery):
    db.clear_library(callback.from_user.id)
    await callback.message.edit_text("🗑 Библиотека очищена.")
    await callback.answer()


# ══════════════════════════════════════════
#  RANDOM TRACK
# ══════════════════════════════════════════

@dp.message(F.text == "🎲 Случайный трек")
async def random_track(message: Message):
    track = db.get_random_track(message.from_user.id)
    if not track:
        await message.answer("📚 Библиотека пуста — сначала добавь треки!")
        return
    await send_audio_track(message.from_user.id, track, in_library=True)


# ══════════════════════════════════════════
#  PLAYLISTS
# ══════════════════════════════════════════

@dp.message(F.text == "🎵 Плейлисты")
async def show_playlists(message: Message):
    db.ensure_user(message.from_user.id)
    playlists = db.get_playlists(message.from_user.id)
    if not playlists:
        await message.answer(
            f"🎵 У тебя пока нет плейлистов.\nМожно создать до *{PLAYLIST_LIMIT}*.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="➕ Новый плейлист", callback_data="pl_create")
            ]])
        )
        return
    await message.answer(
        f"🎵 *Твои плейлисты* — {len(playlists)}/{PLAYLIST_LIMIT}:",
        parse_mode="Markdown",
        reply_markup=kb_playlists(playlists)
    )


@dp.callback_query(F.data.startswith("plpage:"))
async def playlists_page(callback: CallbackQuery):
    page      = int(callback.data.split(":")[1])
    playlists = db.get_playlists(callback.from_user.id)
    await callback.message.edit_reply_markup(reply_markup=kb_playlists(playlists, page))
    await callback.answer()


@dp.callback_query(F.data == "pl_back")
async def playlist_back(callback: CallbackQuery):
    playlists = db.get_playlists(callback.from_user.id)
    if not playlists:
        await callback.message.edit_text(
            "🎵 У тебя нет плейлистов.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="➕ Новый плейлист", callback_data="pl_create")
            ]])
        )
    else:
        await callback.message.edit_text(
            f"🎵 *Твои плейлисты* — {len(playlists)}/{PLAYLIST_LIMIT}:",
            parse_mode="Markdown",
            reply_markup=kb_playlists(playlists)
        )
    await callback.answer()


@dp.callback_query(F.data == "pl_create")
async def pl_create_start(callback: CallbackQuery, state: FSMContext):
    count = db.playlist_count(callback.from_user.id)
    if count >= PLAYLIST_LIMIT:
        await callback.answer(f"⛔ Максимум {PLAYLIST_LIMIT} плейлистов.", show_alert=True)
        return
    await state.set_state(States.playlist_naming)
    await callback.message.answer(
        f"✏️ Введи название для нового плейлиста (макс. 64 символа):\n"
        f"Плейлистов: {count}/{PLAYLIST_LIMIT}"
    )
    await callback.answer()


@dp.message(States.playlist_naming)
async def pl_set_name(message: Message, state: FSMContext):
    name    = message.text.strip()
    user_id = message.from_user.id
    pl_id, status = db.create_playlist(user_id, name)
    if status == "limit":
        await state.clear()
        await message.answer(f"⛔ Достигнут лимит в {PLAYLIST_LIMIT} плейлистов.")
        return
    if status == "empty_name":
        await message.answer("❌ Название не может быть пустым. Попробуй ещё раз:")
        return
    if status == "long_name":
        await message.answer("❌ Название слишком длинное (макс. 64 символа). Попробуй ещё раз:")
        return
    library = db.get_library(user_id)
    await state.set_state(States.playlist_select_tracks)
    await state.update_data(playlist_id=pl_id, selected_ids=[], sel_page=0)
    if not library:
        await state.clear()
        await message.answer(
            f"✅ Плейлист *{name}* создан!\n\n📭 Библиотека пуста — добавь треки и наполни плейлист.",
            parse_mode="Markdown"
        )
        return
    await message.answer(
        f"✅ Плейлист *{name}* создан!\n\nВыбери треки из библиотеки:",
        parse_mode="Markdown",
        reply_markup=kb_select_tracks(library, set(), pl_id)
    )


@dp.callback_query(F.data.startswith("sel_toggle:"))
async def sel_toggle(callback: CallbackQuery, state: FSMContext):
    tid      = callback.data.split(":", 1)[1]
    data     = await state.get_data()
    selected = set(data.get("selected_ids", []))
    pl_id    = data.get("playlist_id")
    page     = data.get("sel_page", 0)
    if tid in selected:
        selected.discard(tid)
    else:
        selected.add(tid)
    await state.update_data(selected_ids=list(selected))
    library = db.get_library(callback.from_user.id)
    await callback.message.edit_reply_markup(
        reply_markup=kb_select_tracks(library, selected, pl_id, page)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("sel_page:"))
async def sel_page_turn(callback: CallbackQuery, state: FSMContext):
    page     = int(callback.data.split(":")[1])
    data     = await state.get_data()
    selected = set(data.get("selected_ids", []))
    pl_id    = data.get("playlist_id")
    await state.update_data(sel_page=page)
    library = db.get_library(callback.from_user.id)
    await callback.message.edit_reply_markup(
        reply_markup=kb_select_tracks(library, selected, pl_id, page)
    )
    await callback.answer()


@dp.callback_query(F.data == "sel_confirm")
async def sel_confirm(callback: CallbackQuery, state: FSMContext):
    data     = await state.get_data()
    selected = list(data.get("selected_ids", []))
    pl_id    = data.get("playlist_id")
    await state.clear()
    if not selected:
        await callback.answer("⚠️ Не выбрано ни одного трека.", show_alert=True)
        return
    added, status = db.add_tracks_to_playlist_bulk(pl_id, selected)
    pl      = db.get_playlist(pl_id, callback.from_user.id)
    pl_name = pl["name"] if pl else "плейлист"
    msg = (
        f"⛔ Плейлист переполнен (макс. {PLAYLIST_TRACKS_LIMIT} треков)."
        if status == "limit"
        else f"✅ Добавлено *{added}* треков в плейлист *{pl_name}*!"
    )
    await callback.message.edit_text(msg, parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "sel_cancel")
async def sel_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Выбор отменён.")
    await callback.answer()


@dp.callback_query(F.data.startswith("pl_open:"))
async def pl_open(callback: CallbackQuery):
    pl_id = int(callback.data.split(":")[1])
    pl    = db.get_playlist(pl_id, callback.from_user.id)
    if not pl:
        await callback.answer("❌ Плейлист не найден.", show_alert=True)
        return
    tracks = db.get_playlist_tracks(pl_id)
    text = (
        f"🎶 *{pl['name']}*\nТреков: {len(tracks)}/{PLAYLIST_TRACKS_LIMIT}\n\n"
        + ("▶️ Нажми трек чтобы слушать. ✖️ — удалить." if tracks else "📭 Плейлист пуст.")
    )
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=kb_playlist_detail(pl_id, tracks)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("pltrpage:"))
async def playlist_tracks_page(callback: CallbackQuery):
    _, pl_id_str, page_str = callback.data.split(":")
    pl_id, page = int(pl_id_str), int(page_str)
    pl = db.get_playlist(pl_id, callback.from_user.id)
    if not pl:
        await callback.answer("❌ Плейлист не найден.", show_alert=True)
        return
    tracks = db.get_playlist_tracks(pl_id)
    await callback.message.edit_reply_markup(reply_markup=kb_playlist_detail(pl_id, tracks, page))
    await callback.answer()


@dp.callback_query(F.data.startswith("plplay:"))
async def playlist_play_track(callback: CallbackQuery):
    track_id = callback.data.split(":", 1)[1]
    track    = db.get_track(track_id)
    if not track:
        await callback.answer("❌ Трек не найден.", show_alert=True)
        return
    await callback.answer("⏳ Загружаю...")
    in_lib = db.is_in_library(callback.from_user.id, track_id)
    await send_audio_track(callback.from_user.id, track, in_library=in_lib)


@dp.callback_query(F.data.startswith("pl_rmtrack:"))
async def pl_remove_track(callback: CallbackQuery):
    parts              = callback.data.split(":")
    pl_id, track_id   = int(parts[1]), parts[2]
    pl = db.get_playlist(pl_id, callback.from_user.id)
    if not pl:
        await callback.answer("❌ Плейлист не найден.", show_alert=True)
        return
    db.remove_track_from_playlist(pl_id, track_id)
    await callback.answer("🗑 Трек удалён из плейлиста.")
    tracks = db.get_playlist_tracks(pl_id)
    text = (
        f"🎶 *{pl['name']}*\nТреков: {len(tracks)}/{PLAYLIST_TRACKS_LIMIT}\n\n"
        + ("▶️ Нажми трек чтобы слушать. ✖️ — удалить." if tracks else "📭 Плейлист пуст.")
    )
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=kb_playlist_detail(pl_id, tracks)
    )


@dp.callback_query(F.data.startswith("pl_addtracks:"))
async def pl_add_tracks_start(callback: CallbackQuery, state: FSMContext):
    pl_id   = int(callback.data.split(":")[1])
    pl      = db.get_playlist(pl_id, callback.from_user.id)
    if not pl:
        await callback.answer("❌ Плейлист не найден.", show_alert=True)
        return
    library = db.get_library(callback.from_user.id)
    if not library:
        await callback.answer("📭 Библиотека пуста.", show_alert=True)
        return
    current_count = db.playlist_track_count(pl_id)
    if current_count >= PLAYLIST_TRACKS_LIMIT:
        await callback.answer(f"⛔ Плейлист заполнен (макс. {PLAYLIST_TRACKS_LIMIT}).", show_alert=True)
        return
    await state.set_state(States.playlist_select_tracks)
    await state.update_data(playlist_id=pl_id, selected_ids=[], sel_page=0)
    await callback.message.answer(
        f"➕ Добавление треков в *{pl['name']}*\nУже: {current_count}/{PLAYLIST_TRACKS_LIMIT}",
        parse_mode="Markdown",
        reply_markup=kb_select_tracks(library, set(), pl_id)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("pl_rename:"))
async def pl_rename_start(callback: CallbackQuery, state: FSMContext):
    pl_id = int(callback.data.split(":")[1])
    pl    = db.get_playlist(pl_id, callback.from_user.id)
    if not pl:
        await callback.answer("❌ Плейлист не найден.", show_alert=True)
        return
    await state.set_state(States.playlist_renaming)
    await state.update_data(rename_playlist_id=pl_id)
    await callback.message.answer(
        f"✏️ Введи новое название для *{pl['name']}*:",
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.message(States.playlist_renaming)
async def pl_do_rename(message: Message, state: FSMContext):
    data    = await state.get_data()
    pl_id   = data.get("rename_playlist_id")
    new_name = message.text.strip()
    if not new_name:
        await message.answer("❌ Название не может быть пустым.")
        return
    if len(new_name) > 64:
        await message.answer("❌ Слишком длинное (макс. 64 символа).")
        return
    ok = db.rename_playlist(pl_id, message.from_user.id, new_name)
    await state.clear()
    await message.answer(
        f"✅ Переименован в *{new_name}*!" if ok else "❌ Не удалось переименовать.",
        parse_mode="Markdown"
    )


@dp.callback_query(F.data.startswith("pl_delete:"))
async def pl_delete(callback: CallbackQuery):
    pl_id = int(callback.data.split(":")[1])
    pl    = db.get_playlist(pl_id, callback.from_user.id)
    if not pl:
        await callback.answer("❌ Плейлист не найден.", show_alert=True)
        return
    db.delete_playlist(pl_id, callback.from_user.id)
    playlists = db.get_playlists(callback.from_user.id)
    if not playlists:
        await callback.message.edit_text(
            f"🗑 Плейлист *{pl['name']}* удалён.\nУ тебя больше нет плейлистов.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="➕ Новый плейлист", callback_data="pl_create")
            ]])
        )
    else:
        await callback.message.edit_text(
            f"🗑 Плейлист *{pl['name']}* удалён.\n\n🎵 *Твои плейлисты* — {len(playlists)}/{PLAYLIST_LIMIT}:",
            parse_mode="Markdown",
            reply_markup=kb_playlists(playlists)
        )
    await callback.answer()


# ══════════════════════════════════════════
#  SUPPORT — ПОЛЬЗОВАТЕЛИ
# ══════════════════════════════════════════

@dp.message(F.text == "🆘 Поддержка")
async def support_start(message: Message, state: FSMContext):
    if is_owner(message.from_user.id):
        await message.answer(
            "👑 Ты владелец — обращения приходят автоматически.\n"
            "Нажми *💬 Ответить* под сообщением пользователя.",
            parse_mode="Markdown"
        )
        return
    await state.set_state(States.support_writing)
    await message.answer(
        "🆘 *Поддержка*\n\n"
        "Опиши свою проблему или вопрос — мы ответим как можно скорее.\n\n"
        "Можно отправить *текст, фото или видео*.\n"
        "Напиши /cancel чтобы отменить.",
        parse_mode="Markdown"
    )


# ══════════════════════════════════════════
#  SUPPORT — ОТВЕТ ВЛАДЕЛЬЦА
# ══════════════════════════════════════════

@dp.callback_query(F.data.startswith("owner_reply:"))
async def owner_reply_start(callback: CallbackQuery, state: FSMContext):
    if not is_owner(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.", show_alert=True)
        return
    target_id = int(callback.data.split(":")[1])
    await state.set_state(States.owner_replying)
    await state.update_data(reply_target=target_id)
    await callback.message.answer(
        f"💬 Пишешь ответ пользователю `{target_id}`.\n"
        "Можно отправить *текст, фото или видео*.\n"
        "/cancel — отменить.",
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.message(States.owner_replying)
async def owner_send_reply(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    data      = await state.get_data()
    target_id = data.get("reply_target")
    await state.clear()
    try:
        await bot.send_message(target_id, "📩 *Ответ от поддержки:*", parse_mode="Markdown")
        await message.copy_to(target_id)
        await message.answer(f"✅ Ответ отправлен пользователю `{target_id}`.", parse_mode="Markdown")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить: `{e}`", parse_mode="Markdown")


@dp.message(F.text.in_({"🔴 Выключить бота", "🟢 Включить бота"}))
async def toggle_maintenance(message: Message):
    if not is_owner(message.from_user.id):
        return
    global maintenance_mode
    maintenance_mode = not maintenance_mode
    if maintenance_mode:
        await message.answer(
            "🔴 *Технические работы включены.*\n\nБот недоступен для пользователей.",
            parse_mode="Markdown",
            reply_markup=kb_owner_main()
        )
    else:
        await message.answer(
            "🟢 *Технические работы отключены.*\n\nБот снова доступен для всех.",
            parse_mode="Markdown",
            reply_markup=kb_owner_main()
        )


# ══════════════════════════════════════════
#  OWNER PANEL
# ══════════════════════════════════════════

@dp.message(F.text == "👑 Панель владельца")
async def owner_panel(message: Message):
    if not is_owner(message.from_user.id):
        return
    status = "🔴 Включены" if maintenance_mode else "🟢 Выключены"
    await message.answer(
        "👑 *Панель владельца*\n\n"
        "📊 *Статистика:*\n"
        f"  👥 Пользователей: *{db.get_users_count()}*\n"
        f"  🚫 Заблокировано: *{db.get_banned_count()}*\n"
        f"  🎵 Треков в каталоге: *{db.get_tracks_count()}*\n"
        f"  💾 Сохранений в библиотеках: *{db.get_library_total()}*\n"
        f"  🎶 Плейлистов создано: *{db.get_playlists_total()}*\n\n"
        f"🔧 *Техработы:* {status}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📋 *Команды:*\n\n"
        "/stats — подробная статистика\n"
        "/users — список пользователей\n"
        "/broadcast — рассылка\n"
        "/ban `<user_id>` — заблокировать\n"
        "/unban `<user_id>` — разблокировать\n"
        "/cancel — отменить действие",
        parse_mode="Markdown",
        reply_markup=kb_owner_main()
    )


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not is_owner(message.from_user.id): return
    await message.answer(
        "📊 *Статистика бота*\n\n"
        f"👥 Пользователей: *{db.get_users_count()}*\n"
        f"🚫 Заблокировано: *{db.get_banned_count()}*\n"
        f"🎵 Треков в каталоге: *{db.get_tracks_count()}*\n"
        f"💾 Сохранений в библиотеках: *{db.get_library_total()}*\n"
        f"🎶 Плейлистов создано: *{db.get_playlists_total()}*",
        parse_mode="Markdown"
    )


@dp.message(Command("users"))
async def cmd_users(message: Message):
    if not is_owner(message.from_user.id): return
    users = db.get_all_users()
    if not users:
        await message.answer("👥 Пользователей нет.")
        return
    lines = []
    for u in users[:50]:
        banned = " 🚫" if u.get("is_banned") else ""
        uname  = f" @{u['username']}" if u.get("username") else ""
        lines.append(f"• `{u['user_id']}`{uname}{banned} — {str(u['created_at'])[:10]}")
    text = f"👥 *Пользователи* ({len(users)} всего):\n\n" + "\n".join(lines)
    if len(users) > 50:
        text += f"\n\n...и ещё {len(users) - 50}"
    await message.answer(text, parse_mode="Markdown")


@dp.message(Command("ban"))
async def cmd_ban(message: Message):
    if not is_owner(message.from_user.id): return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /ban `<user_id>`", parse_mode="Markdown")
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный user_id.")
        return
    if target_id == OWNER_ID:
        await message.answer("❌ Нельзя заблокировать самого себя.")
        return
    db.ban_user(target_id)
    await message.answer(f"🚫 Пользователь `{target_id}` заблокирован.", parse_mode="Markdown")
    try:
        await bot.send_message(target_id, "🚫 Вы заблокированы в этом боте.")
    except Exception:
        pass


@dp.message(Command("unban"))
async def cmd_unban(message: Message):
    if not is_owner(message.from_user.id): return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /unban `<user_id>`", parse_mode="Markdown")
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный user_id.")
        return
    db.unban_user(target_id)
    await message.answer(f"✅ Пользователь `{target_id}` разблокирован.", parse_mode="Markdown")
    try:
        await bot.send_message(target_id, "✅ Ваш доступ к боту восстановлен.")
    except Exception:
        pass


# ══════════════════════════════════════════
#  BROADCAST
# ══════════════════════════════════════════

@dp.message(Command("broadcast"))
async def cmd_broadcast_start(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id): return
    users_count  = db.get_users_count()
    banned_count = db.get_banned_count()
    await state.set_state(States.broadcast_waiting)
    await message.answer(
        f"📢 *Рассылка*\n\n"
        f"Активных получателей: *{users_count - banned_count}*\n\n"
        "Отправь сообщение для рассылки (текст, фото, видео).\n"
        "/cancel — отменить.",
        parse_mode="Markdown"
    )


@dp.message(States.broadcast_waiting)
async def cmd_broadcast_send(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id): return
    await state.clear()
    users  = db.get_all_users()
    active = [u for u in users if not u.get("is_banned")]

    status_msg = await message.answer(f"📤 Начинаю рассылку... 0/{len(active)}")
    success, failed = 0, 0

    for i, user in enumerate(active):
        try:
            await message.copy_to(user["user_id"])
            success += 1
        except Exception:
            failed += 1
        if (i + 1) % 10 == 0:
            try:
                await status_msg.edit_text(f"📤 Рассылка... {i+1}/{len(active)}")
            except Exception:
                pass
        await asyncio.sleep(0.05)

    await status_msg.edit_text(
        f"✅ *Рассылка завершена!*\n\n"
        f"📨 Отправлено: *{success}*\n"
        f"❌ Ошибок: *{failed}*",
        parse_mode="Markdown"
    )


# ══════════════════════════════════════════
#  MISC
# ══════════════════════════════════════════

@dp.callback_query(F.data == "close")
async def close_msg(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()


async def main():
    logger.info("Bot started!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
