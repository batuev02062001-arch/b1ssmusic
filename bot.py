import asyncio, logging, os, json, pathlib
from typing import Any, Dict, Optional
from datetime import datetime, timezone, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    BufferedInputFile,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import StorageKey, BaseStorage, StateType
from aiogram.client.session.aiohttp import AiohttpSession

from database import Database, LIBRARY_LIMIT, PLAYLIST_LIMIT, PLAYLIST_TRACKS_LIMIT, SUPPORTED_LANGS
from search import search_soundcloud, download_track

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID  = int(os.getenv("OWNER_ID", "0"))
BOT_LINK  = "https://t.me/b1ssmusic_bot"

DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════
#  PERSISTENT FSM STORAGE
# ══════════════════════════════════════════

class SimpleFileStorage(BaseStorage):
    def __init__(self, path: str):
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
            self._data[k] = {}
        self._data[k]["state"] = state.state if hasattr(state, "state") else state
        self._save()

    async def get_state(self, key: StorageKey) -> Optional[str]:
        return self._data.get(self._key(key), {}).get("state")

    async def set_data(self, key: StorageKey, data: Dict[str, Any]):
        k = self._key(key)
        if k not in self._data:
            self._data[k] = {}
        self._data[k]["data"] = data
        self._save()

    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        return self._data.get(self._key(key), {}).get("data", {})

    async def close(self):
        self._save()


# ══════════════════════════════════════════
#  INIT
# ══════════════════════════════════════════

_session = AiohttpSession(timeout=120)
bot      = Bot(token=BOT_TOKEN, session=_session)
storage  = SimpleFileStorage(str(DATA_DIR / "fsm_storage.json"))
dp       = Dispatcher(storage=storage)
db       = Database(str(DATA_DIR / "library.db"))

maintenance_mode = False


# ══════════════════════════════════════════
#  STATES
# ══════════════════════════════════════════

class S(StatesGroup):
    lang_select            = State()
    searching              = State()
    playlist_naming        = State()
    playlist_select_tracks = State()
    playlist_renaming      = State()
    playlist_join          = State()
    support_writing        = State()
    appeal_writing         = State()
    owner_replying         = State()
    broadcast_waiting      = State()
    admin_msg_id           = State()
    admin_msg_text         = State()
    tempban_id             = State()
    tempban_duration       = State()


# ══════════════════════════════════════════
#  I18N
# ══════════════════════════════════════════

T = {
    "ru": {
        "welcome": "👋 Привет, *{name}*!\n\n🎵 Я музыкальный бот — ищу треки на SoundCloud.\n💾 Сохраняй в *библиотеку*, создавай *плейлисты*.\n\nНажми *🔍 Поиск* чтобы найти трек 👇",
        "search_prompt": "🔍 Введи название трека или исполнителя:",
        "searching": "🔍 Ищу: *{q}*...",
        "not_found": "😔 Ничего не найдено. Попробуй другой запрос.",
        "found": "🎵 Найдено *{n}* треков по запросу «{q}»:",
        "loading": "⏳ Загружаю трек...",
        "load_fail": "😔 Не удалось загрузить трек. Попробуй другой.",
        "saved": "✅ Добавлено! В библиотеке {n}/{lim}",
        "already": "ℹ️ Трек уже в библиотеке.",
        "lib_full": "⛔ Библиотека заполнена! Максимум {lim} треков.",
        "removed": "🗑 Удалено из библиотеки.",
        "lib_empty": "📚 Библиотека пуста.\nНайди трек и нажми *💾 В библиотеку*.",
        "lib_title": "📚 *Библиотека* — {n}/{lim} треков:",
        "lib_cleared": "🗑 Библиотека очищена.",
        "no_playlists": "🎵 У тебя нет плейлистов.\nМожно создать до *{lim}*.",
        "playlists_title": "🎵 *Плейлисты* — {n}/{lim}:",
        "pl_name_prompt": "✏️ Введи название нового плейлиста:",
        "pl_created": "✅ Плейлист *{name}* создан!\n\nВыбери треки из библиотеки:",
        "pl_created_empty": "✅ Плейлист *{name}* создан!\n\n📭 Библиотека пуста — добавь треки и наполни плейлист.",
        "pl_full": "⛔ Максимум {lim} плейлистов.",
        "pl_renamed": "✅ Переименован в *{name}*!",
        "pl_deleted": "🗑 Плейлист *{name}* удалён.",
        "pl_tracks_added": "✅ Добавлено *{n}* треков в плейлист *{name}*!",
        "pl_code": "🔗 Код плейлиста: `{code}`\nПоделись им — другие смогут добавить его себе командой /joinplaylist",
        "pl_join_prompt": "🔗 Введи 6-значный код плейлиста:",
        "pl_join_ok": "✅ Плейлист *{name}* добавлен к тебе!",
        "pl_join_fail": "❌ Плейлист с таким кодом не найден.",
        "random_empty": "📚 Библиотека пуста — сначала добавь треки!",
        "top_empty": "📊 Пока нет данных о прослушиваниях за эту неделю.",
        "top_title": "🏆 *Топ треков недели* ({week}):\n\n",
        "support_prompt": "🆘 *Поддержка*\n\nОпиши проблему или вопрос.\nМожно отправить текст, фото или видео.\n/cancel — отменить.",
        "support_sent": "✅ Обращение отправлено! Ответим в ближайшее время. 🙏",
        "banned": "🚫 Вы заблокированы в этом боте.",
        "banned_until": "🚫 Вы временно заблокированы до {until}.",
        "appeal_prompt": "📝 Напиши причину для разблокировки:",
        "appeal_sent": "✅ Апелляция отправлена. Ожидай решения.",
        "appeal_already": "⚠️ У тебя уже есть ожидающая апелляция.",
        "maintenance": "🔧 *Технические работы*\n\nБот временно недоступен. Попробуй позже.",
        "cancelled": "❌ Отменено.",
        "nothing_to_cancel": "Нечего отменять.",
        "help": (
            "📖 *Как пользоваться:*\n\n"
            "🔍 *Поиск* — нажми кнопку или введи запрос\n"
            "▶️ Нажми трек — бот пришлёт аудио\n"
            f"💾 *В библиотеку* — сохранить трек (макс. {LIBRARY_LIMIT})\n"
            "📚 *Библиотека* — все сохранённые треки\n"
            f"🎵 *Плейлисты* — до {PLAYLIST_LIMIT} шт., по {PLAYLIST_TRACKS_LIMIT} треков\n"
            "🎲 *Случайный трек* — из библиотеки\n"
            "🏆 *Топ недели* — самые слушаемые треки\n"
            "🆘 *Поддержка* — написать нам\n\n"
            "*Команды:*\n"
            "/start — главное меню\n"
            "/lang — сменить язык\n"
            "/joinplaylist — добавить плейлист по коду\n"
            "/cancel — отменить действие"
        ),
        "btn_search": "🔍 Поиск",
        "btn_library": "📚 Библиотека",
        "btn_playlists": "🎵 Плейлисты",
        "btn_random": "🎲 Случайный трек",
        "btn_top": "🏆 Топ недели",
        "btn_support": "🆘 Поддержка",
        "btn_help": "ℹ️ Помощь",
        "btn_owner": "👑 Панель владельца",
        "btn_maint_on": "🔴 Выключить бота",
        "btn_maint_off": "🟢 Включить бота",
        "share_caption": "🎵 *{artist}* — {title}\n\n🤖 Найди больше треков: {link}",
    },
    "en": {
        "welcome": "👋 Hi, *{name}*!\n\n🎵 I'm a music bot — I find tracks on SoundCloud.\n💾 Save to *library*, create *playlists*.\n\nPress *🔍 Search* to find a track 👇",
        "search_prompt": "🔍 Enter track name or artist:",
        "searching": "🔍 Searching: *{q}*...",
        "not_found": "😔 Nothing found. Try another query.",
        "found": "🎵 Found *{n}* tracks for «{q}»:",
        "loading": "⏳ Loading track...",
        "load_fail": "😔 Failed to load track. Try another.",
        "saved": "✅ Added! Library {n}/{lim}",
        "already": "ℹ️ Track already in library.",
        "lib_full": "⛔ Library full! Max {lim} tracks.",
        "removed": "🗑 Removed from library.",
        "lib_empty": "📚 Library is empty.\nFind a track and press *💾 To library*.",
        "lib_title": "📚 *Library* — {n}/{lim} tracks:",
        "lib_cleared": "🗑 Library cleared.",
        "no_playlists": "🎵 You have no playlists.\nYou can create up to *{lim}*.",
        "playlists_title": "🎵 *Playlists* — {n}/{lim}:",
        "pl_name_prompt": "✏️ Enter name for the new playlist:",
        "pl_created": "✅ Playlist *{name}* created!\n\nSelect tracks from library:",
        "pl_created_empty": "✅ Playlist *{name}* created!\n\n📭 Library is empty — add tracks first.",
        "pl_full": "⛔ Max {lim} playlists.",
        "pl_renamed": "✅ Renamed to *{name}*!",
        "pl_deleted": "🗑 Playlist *{name}* deleted.",
        "pl_tracks_added": "✅ Added *{n}* tracks to *{name}*!",
        "pl_code": "🔗 Playlist code: `{code}`\nShare it — others can add it with /joinplaylist",
        "pl_join_prompt": "🔗 Enter the 6-character playlist code:",
        "pl_join_ok": "✅ Playlist *{name}* added!",
        "pl_join_fail": "❌ No playlist found with this code.",
        "random_empty": "📚 Library is empty — add tracks first!",
        "top_empty": "📊 No listening data for this week yet.",
        "top_title": "🏆 *Top tracks this week* ({week}):\n\n",
        "support_prompt": "🆘 *Support*\n\nDescribe your issue.\nText, photo or video accepted.\n/cancel to cancel.",
        "support_sent": "✅ Request sent! We'll reply soon. 🙏",
        "banned": "🚫 You are banned from this bot.",
        "banned_until": "🚫 You are temporarily banned until {until}.",
        "appeal_prompt": "📝 Write your reason for unban:",
        "appeal_sent": "✅ Appeal submitted. Await decision.",
        "appeal_already": "⚠️ You already have a pending appeal.",
        "maintenance": "🔧 *Maintenance*\n\nBot is temporarily unavailable.",
        "cancelled": "❌ Cancelled.",
        "nothing_to_cancel": "Nothing to cancel.",
        "help": (
            "📖 *How to use:*\n\n"
            "🔍 *Search* — press button or type query\n"
            "▶️ Tap a track — bot sends audio\n"
            f"💾 *To library* — save track (max {LIBRARY_LIMIT})\n"
            "📚 *Library* — all saved tracks\n"
            f"🎵 *Playlists* — up to {PLAYLIST_LIMIT}, {PLAYLIST_TRACKS_LIMIT} tracks each\n"
            "🎲 *Random track* — from your library\n"
            "🏆 *Top of the week* — most played tracks\n"
            "🆘 *Support* — contact us\n\n"
            "*Commands:*\n"
            "/start — main menu\n"
            "/joinplaylist — join playlist by code\n"
            "/cancel — cancel current action"
        ),
        "btn_search": "🔍 Search",
        "btn_library": "📚 Library",
        "btn_playlists": "🎵 Playlists",
        "btn_random": "🎲 Random track",
        "btn_top": "🏆 Top of week",
        "btn_support": "🆘 Support",
        "btn_help": "ℹ️ Help",
        "btn_owner": "👑 Owner panel",
        "btn_maint_on": "🔴 Disable bot",
        "btn_maint_off": "🟢 Enable bot",
        "share_caption": "🎵 *{artist}* — {title}\n\n🤖 Find more tracks: {link}",
    },
    "be": {
        "welcome": "👋 Прывітанне, *{name}*!\n\n🎵 Я музычны бот — шукаю трэкі на SoundCloud.\n💾 Захоўвай у *бібліятэку*, стварай *плэйлісты*.\n\nНацісні *🔍 Пошук* каб знайсці трэк 👇",
        "search_prompt": "🔍 Увядзі назву трэка або выканаўцы:",
        "searching": "🔍 Шукаю: *{q}*...",
        "not_found": "😔 Нічога не знойдзена. Паспрабуй іншы запыт.",
        "found": "🎵 Знойдзена *{n}* трэкаў па запыце «{q}»:",
        "loading": "⏳ Загружаю трэк...",
        "load_fail": "😔 Не ўдалося загрузіць трэк. Паспрабуй іншы.",
        "saved": "✅ Дадана! У бібліятэцы {n}/{lim}",
        "already": "ℹ️ Трэк ужо ў бібліятэцы.",
        "lib_full": "⛔ Бібліятэка поўная! Максімум {lim} трэкаў.",
        "removed": "🗑 Выдалена з бібліятэкі.",
        "lib_empty": "📚 Бібліятэка пустая.\nЗнайдзі трэк і націсні *💾 У бібліятэку*.",
        "lib_title": "📚 *Бібліятэка* — {n}/{lim} трэкаў:",
        "lib_cleared": "🗑 Бібліятэка ачышчана.",
        "no_playlists": "🎵 У цябе няма плэйлістаў.\nМожна стварыць да *{lim}*.",
        "playlists_title": "🎵 *Плэйлісты* — {n}/{lim}:",
        "pl_name_prompt": "✏️ Увядзі назву новага плэйліста:",
        "pl_created": "✅ Плэйліст *{name}* створаны!\n\nВыбяры трэкі з бібліятэкі:",
        "pl_created_empty": "✅ Плэйліст *{name}* створаны!\n\n📭 Бібліятэка пустая.",
        "pl_full": "⛔ Максімум {lim} плэйлістаў.",
        "pl_renamed": "✅ Перайменаваны ў *{name}*!",
        "pl_deleted": "🗑 Плэйліст *{name}* выдалены.",
        "pl_tracks_added": "✅ Дадана *{n}* трэкаў у *{name}*!",
        "pl_code": "🔗 Код плэйліста: `{code}`\nПадзяліся ім — іншыя змогуць дадаць яго сабе праз /joinplaylist",
        "pl_join_prompt": "🔗 Увядзі 6-значны код плэйліста:",
        "pl_join_ok": "✅ Плэйліст *{name}* дададзены!",
        "pl_join_fail": "❌ Плэйліст з такім кодам не знойдзены.",
        "random_empty": "📚 Бібліятэка пустая — спачатку дадай трэкі!",
        "top_empty": "📊 Пакуль няма дадзеных за гэты тыдзень.",
        "top_title": "🏆 *Топ трэкаў тыдня* ({week}):\n\n",
        "support_prompt": "🆘 *Падтрымка*\n\nАпішы сваю праблему.\n/cancel — адмяніць.",
        "support_sent": "✅ Зварот адпраўлены! 🙏",
        "banned": "🚫 Вы заблакіраваны ў гэтым боце.",
        "banned_until": "🚫 Вы часова заблакіраваны да {until}.",
        "appeal_prompt": "📝 Напішы прычыну для разблакіроўкі:",
        "appeal_sent": "✅ Апеляцыя адпраўлена.",
        "appeal_already": "⚠️ У цябе ўжо ёсць апеляцыя на разглядзе.",
        "maintenance": "🔧 *Тэхнічныя работы*\n\nБот часова недаступны.",
        "cancelled": "❌ Адменена.",
        "nothing_to_cancel": "Няма чаго адмяняць.",
        "help": "📖 Выкарыстоўвай кнопкі ніжэй для навігацыі.",
        "btn_search": "🔍 Пошук",
        "btn_library": "📚 Бібліятэка",
        "btn_playlists": "🎵 Плэйлісты",
        "btn_random": "🎲 Выпадковы трэк",
        "btn_top": "🏆 Топ тыдня",
        "btn_support": "🆘 Падтрымка",
        "btn_help": "ℹ️ Даведка",
        "btn_owner": "👑 Панэль уладальніка",
        "btn_maint_on": "🔴 Адключыць бот",
        "btn_maint_off": "🟢 Уключыць бот",
        "share_caption": "🎵 *{artist}* — {title}\n\n🤖 Знайдзі больш трэкаў: {link}",
    },
    "kk": {
        "welcome": "👋 Сәлем, *{name}*!\n\n🎵 Мен музыкалық бот — SoundCloud-тан треклер іздеймін.\n💾 *Кітапханаға* сақта, *ойнату тізімдер* жаса.\n\nТрек іздеу үшін *🔍 Іздеу* басыңыз 👇",
        "search_prompt": "🔍 Трек немесе орындаушы атын енгізіңіз:",
        "searching": "🔍 Іздеуде: *{q}*...",
        "not_found": "😔 Ештеңе табылмады. Басқа сұраныс жасап көр.",
        "found": "🎵 «{q}» сұранысы бойынша *{n}* трек табылды:",
        "loading": "⏳ Трек жүктелуде...",
        "load_fail": "😔 Тректі жүктеу мүмкін болмады. Басқасын көріңіз.",
        "saved": "✅ Қосылды! Кітапханада {n}/{lim}",
        "already": "ℹ️ Трек кітапханада бар.",
        "lib_full": "⛔ Кітапхана толы! Максимум {lim} трек.",
        "removed": "🗑 Кітапханадан жойылды.",
        "lib_empty": "📚 Кітапхана бос.\nТрек тауып *💾 Кітапханаға* басыңыз.",
        "lib_title": "📚 *Кітапхана* — {n}/{lim} трек:",
        "lib_cleared": "🗑 Кітапхана тазаланды.",
        "no_playlists": "🎵 Сізде ойнату тізімдер жоқ.\n*{lim}* тізімге дейін жасауға болады.",
        "playlists_title": "🎵 *Ойнату тізімдер* — {n}/{lim}:",
        "pl_name_prompt": "✏️ Жаңа ойнату тізімінің атын енгізіңіз:",
        "pl_created": "✅ *{name}* тізімі жасалды!\n\nКітапханадан трек таңдаңыз:",
        "pl_created_empty": "✅ *{name}* тізімі жасалды!\n\n📭 Кітапхана бос.",
        "pl_full": "⛔ Максимум {lim} тізім.",
        "pl_renamed": "✅ *{name}* деп өзгертілді!",
        "pl_deleted": "🗑 *{name}* тізімі жойылды.",
        "pl_tracks_added": "✅ *{name}* тізіміне *{n}* трек қосылды!",
        "pl_code": "🔗 Тізім коды: `{code}`\nБөлісіңіз — басқалар /joinplaylist арқылы қоса алады",
        "pl_join_prompt": "🔗 6 таңбалы тізім кодын енгізіңіз:",
        "pl_join_ok": "✅ *{name}* тізімі қосылды!",
        "pl_join_fail": "❌ Бұл кодпен тізім табылмады.",
        "random_empty": "📚 Кітапхана бос — алдымен трек қосыңыз!",
        "top_empty": "📊 Бұл аптада тыңдау деректері әлі жоқ.",
        "top_title": "🏆 *Аптаның үздік треклері* ({week}):\n\n",
        "support_prompt": "🆘 *Қолдау*\n\nМәселені сипаттаңыз.\n/cancel — болдырмау.",
        "support_sent": "✅ Өтініш жіберілді! 🙏",
        "banned": "🚫 Сіз осы ботта бұғатталдыңыз.",
        "banned_until": "🚫 Сіз {until} дейін уақытша бұғатталдыңыз.",
        "appeal_prompt": "📝 Бұғаттан шығару себебін жазыңыз:",
        "appeal_sent": "✅ Шағым жіберілді.",
        "appeal_already": "⚠️ Сізде күтілетін шағым бар.",
        "maintenance": "🔧 *Техникалық жұмыстар*\n\nБот уақытша қол жетімсіз.",
        "cancelled": "❌ Болдырылмады.",
        "nothing_to_cancel": "Болдырмайтын ештеңе жоқ.",
        "help": "📖 Навигация үшін төмендегі түймелерді пайдаланыңыз.",
        "btn_search": "🔍 Іздеу",
        "btn_library": "📚 Кітапхана",
        "btn_playlists": "🎵 Тізімдер",
        "btn_random": "🎲 Кездейсоқ трек",
        "btn_top": "🏆 Апта топы",
        "btn_support": "🆘 Қолдау",
        "btn_help": "ℹ️ Анықтама",
        "btn_owner": "👑 Иесінің панелі",
        "btn_maint_on": "🔴 Ботты өшіру",
        "btn_maint_off": "🟢 Ботты қосу",
        "share_caption": "🎵 *{artist}* — {title}\n\n🤖 Көбірек трек табу: {link}",
    },
}


def t(user_id: int, key: str, **kwargs) -> str:
    lang = db.get_lang(user_id)
    text = T.get(lang, T["ru"]).get(key, T["ru"].get(key, key))
    return text.format(**kwargs) if kwargs else text


# ══════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════

def is_owner(uid: int) -> bool:
    return OWNER_ID != 0 and uid == OWNER_ID

def is_admin(uid: int) -> bool:
    return is_owner(uid) or db.is_admin(uid)

def get_all_btn_texts() -> set:
    result = set()
    for lang_dict in T.values():
        for k, v in lang_dict.items():
            if k.startswith("btn_"):
                result.add(v)
    return result

ALL_BTN_TEXTS = None  # lazy init after T is defined


def _all_btn_texts() -> set:
    global ALL_BTN_TEXTS
    if ALL_BTN_TEXTS is None:
        ALL_BTN_TEXTS = get_all_btn_texts()
    return ALL_BTN_TEXTS


# ══════════════════════════════════════════
#  KEYBOARDS
# ══════════════════════════════════════════

def kb_lang():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=v, callback_data=f"setlang:{k}")]
        for k, v in SUPPORTED_LANGS.items()
    ])


def kb_main(uid: int):
    lang = db.get_lang(uid)
    tl = T.get(lang, T["ru"])
    rows = [
        [KeyboardButton(text=tl["btn_search"]),   KeyboardButton(text=tl["btn_library"])],
        [KeyboardButton(text=tl["btn_playlists"]), KeyboardButton(text=tl["btn_random"])],
        [KeyboardButton(text=tl["btn_top"]),       KeyboardButton(text=tl["btn_support"])],
        [KeyboardButton(text=tl["btn_help"])],
    ]
    if is_owner(uid):
        status_key = "btn_maint_on" if not maintenance_mode else "btn_maint_off"
        rows.append([KeyboardButton(text=tl["btn_owner"]),
                     KeyboardButton(text=tl[status_key])])
    elif is_admin(uid):
        rows.append([KeyboardButton(text="👮 Панель админа")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def kb_results(tracks: list, query: str):
    btns = []
    for i, t in enumerate(tracks):
        label = f"{t['artist']} — {t['title']}"
        if len(label) > 50: label = label[:47] + "..."
        btns.append([InlineKeyboardButton(
            text=f"▶️ {label}", callback_data=f"play:{i}:{query[:30]}"
        )])
    btns.append([InlineKeyboardButton(text="❌ Закрыть / Close", callback_data="close")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def kb_track_actions(track_id: str, in_library: bool, uid: int):
    lang = db.get_lang(uid)
    tl = T.get(lang, T["ru"])
    lib_btn = (
        InlineKeyboardButton(text="🗑 " + ("Убрать" if lang == "ru" else "Remove"),
                             callback_data=f"remove:{track_id}")
        if in_library else
        InlineKeyboardButton(text="💾 " + ("В библиотеку" if lang == "ru" else "Save"),
                             callback_data=f"save:{track_id}")
    )
    return InlineKeyboardMarkup(inline_keyboard=[[lib_btn]])


def kb_library(tracks: list, uid: int, page: int = 0, page_size: int = 5):
    total = len(tracks)
    start = page * page_size
    end   = min(start + page_size, total)
    btns  = []
    for tr in tracks[start:end]:
        label = f"🎵 {tr['artist']} — {tr['title']}"
        if len(label) > 52: label = label[:49] + "..."
        btns.append([InlineKeyboardButton(text=label, callback_data=f"libplay:{tr['track_id']}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"libpage:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"libpage:{page+1}"))
    if nav: btns.append(nav)
    btns.append([InlineKeyboardButton(text="🗑 Очистить / Clear", callback_data="lib_clear")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def kb_playlists(playlists: list, page: int = 0, page_size: int = 6):
    total = len(playlists)
    start = page * page_size
    end   = min(start + page_size, total)
    btns  = []
    for pl in playlists[start:end]:
        label = f"🎶 {pl['name']} ({pl['track_count']} тр.)"
        if len(label) > 52: label = label[:49] + "..."
        btns.append([InlineKeyboardButton(text=label, callback_data=f"pl_open:{pl['playlist_id']}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"plpage:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"plpage:{page+1}"))
    if nav: btns.append(nav)
    btns.append([
        InlineKeyboardButton(text="➕ Новый / New", callback_data="pl_create"),
        InlineKeyboardButton(text="🔗 По коду / By code", callback_data="pl_join_start"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def kb_playlist_detail(pl_id: int, tracks: list, share_code: str, page: int = 0, page_size: int = 5):
    total = len(tracks)
    start = page * page_size
    end   = min(start + page_size, total)
    btns  = []
    for tr in tracks[start:end]:
        label = f"▶️ {tr['artist']} — {tr['title']}"
        if len(label) > 52: label = label[:49] + "..."
        btns.append([
            InlineKeyboardButton(text=label, callback_data=f"plplay:{tr['track_id']}"),
            InlineKeyboardButton(text="✖️", callback_data=f"pl_rmtrack:{pl_id}:{tr['track_id']}"),
        ])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"pltrpage:{pl_id}:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"pltrpage:{pl_id}:{page+1}"))
    if nav: btns.append(nav)
    btns.append([
        InlineKeyboardButton(text="➕ Добавить треки", callback_data=f"pl_addtracks:{pl_id}"),
        InlineKeyboardButton(text="✏️ Переименовать",  callback_data=f"pl_rename:{pl_id}"),
    ])
    btns.append([
        InlineKeyboardButton(text=f"🔗 Код: {share_code}", callback_data=f"pl_sharecode:{pl_id}"),
        InlineKeyboardButton(text="🗑 Удалить",            callback_data=f"pl_delete:{pl_id}"),
    ])
    btns.append([InlineKeyboardButton(text="« Назад", callback_data="pl_back")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def kb_select_tracks(library_tracks: list, selected_ids: set,
                     pl_id: int, page: int = 0, page_size: int = 6):
    total = len(library_tracks)
    start = page * page_size
    end   = min(start + page_size, total)
    btns  = []
    for tr in library_tracks[start:end]:
        tid   = tr["track_id"]
        check = "✅" if tid in selected_ids else "⬜"
        label = f"{check} {tr['artist']} — {tr['title']}"
        if len(label) > 52: label = label[:49] + "..."
        btns.append([InlineKeyboardButton(text=label, callback_data=f"sel_toggle:{tid}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"sel_page:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"sel_page:{page+1}"))
    if nav: btns.append(nav)
    count_label = f"✔️ Добавить ({len(selected_ids)})" if selected_ids else "✔️ Добавить"
    btns.append([
        InlineKeyboardButton(text=count_label, callback_data="sel_confirm"),
        InlineKeyboardButton(text="❌ Отмена",  callback_data="sel_cancel"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def kb_reply_to_user(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💬 Ответить", callback_data=f"owner_reply:{user_id}")
    ]])


def kb_appeal(appeal_id: int, user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Одобрить",  callback_data=f"appeal_ok:{appeal_id}:{user_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"appeal_no:{appeal_id}:{user_id}"),
    ]])


# ══════════════════════════════════════════
#  SEND AUDIO
# ══════════════════════════════════════════

async def send_audio_track(chat_id: int, track: dict, in_library: bool,
                           uid: int = None):
    from search import _get_stream_url
    track_id = track.get("track_id", "")
    sc_id    = track.get("_sc_id") or track.get("sc_id")
    if not sc_id:
        saved = db.get_track(track_id)
        if saved:
            sc_id = saved.get("_sc_id") or saved.get("sc_id")

    artist  = track.get("artist", "")
    title   = track.get("title", "")
    caption = f"🎵 {artist} — {title}\n\n🤖 {BOT_LINK}"
    safe_name = "".join(c for c in f"{artist} - {title}.mp3" if c not in r'\/:*?"<>|')

    status = await bot.send_message(chat_id, t(uid or chat_id, "loading"))

    # Способ 1: прямая ссылка — Telegram скачивает сам (быстро)
    if sc_id:
        stream_url = await _get_stream_url(int(sc_id))
        if stream_url:
            try:
                await status.delete()
                db.record_play(track_id)
                await bot.send_audio(
                    chat_id=chat_id,
                    audio=stream_url,
                    title=title,
                    performer=artist,
                    duration=track.get("duration_sec"),
                    caption=caption,
                    reply_markup=kb_track_actions(track_id, in_library, uid or chat_id),
                )
                return
            except Exception:
                pass  # fallback to download

    # Способ 2: скачиваем сами и отправляем файлом
    audio_bytes = await download_track(track)
    await status.delete()
    if audio_bytes is None:
        await bot.send_message(chat_id, t(uid or chat_id, "load_fail"))
        return
    db.record_play(track_id)
    await bot.send_audio(
        chat_id=chat_id,
        audio=BufferedInputFile(audio_bytes, filename=safe_name),
        title=title,
        performer=artist,
        duration=track.get("duration_sec"),
        caption=caption,
        reply_markup=kb_track_actions(track_id, in_library, uid or chat_id),
    )


# ══════════════════════════════════════════
#  MIDDLEWARE
# ══════════════════════════════════════════

@dp.message.outer_middleware()
async def gate_middleware(handler, event: Message, data: dict):
    uid = event.from_user.id
    if is_owner(uid):
        return await handler(event, data)
    if db.is_banned(uid):
        user      = db.get_user(uid)
        ban_until = user.get("ban_until") if user else None
        # Разрешаем /start и апелляцию даже забаненным
        txt = (event.text or "").strip()
        is_start   = txt == "/start" or txt.startswith("/start ")
        is_support = txt in _BTN_VALUES and any(
            v == txt for lang in T.values() for k, v in lang.items() if k == "btn_support"
        )
        state: FSMContext = data.get("state")
        cur = await state.get_state() if state else None
        is_appeal_state = cur == S.appeal_writing.state

        if not (is_start or is_support or is_appeal_state):
            if ban_until:
                until_dt  = datetime.fromisoformat(ban_until)
                until_str = until_dt.strftime("%d.%m.%Y %H:%M UTC")
                await event.answer(t(uid, "banned_until", until=until_str))
            else:
                await event.answer(t(uid, "banned"))
            return
    if maintenance_mode and not is_admin(uid):
        await event.answer(t(uid, "maintenance"), parse_mode="Markdown")
        return
    return await handler(event, data)


@dp.message.middleware()
async def fsm_command_reset_middleware(handler, event: Message, data: dict):
    """Сбрасывает FSM при командах и нажатии кнопок меню."""
    if event.text:
        txt = event.text.strip()
        is_cmd = txt.startswith("/") and not txt.startswith("/cancel")
        is_menu_btn = txt in _get_btn_values() or txt == "👮 Панель админа"
        if is_cmd or is_menu_btn:
            state: FSMContext = data.get("state")
            if state:
                cur = await state.get_state()
                if cur is not None:
                    await state.clear()
    return await handler(event, data)


# ══════════════════════════════════════════
#  FSM HANDLERS BEFORE AUTO-SEARCH
# ══════════════════════════════════════════

@dp.message(S.playlist_naming, F.text)
async def pl_set_name(message: Message, state: FSMContext):
    txt = message.text.strip()
    if txt in _get_btn_values():
        await state.clear()
        await btn_router(message, state)
        return
    name    = message.text.strip()
    uid     = message.from_user.id
    pl_id, status = db.create_playlist(uid, name)
    if status == "limit":
        await state.clear()
        await message.answer(t(uid, "pl_full", lim=PLAYLIST_LIMIT))
        return
    if status == "empty_name":
        await message.answer("❌ Название не может быть пустым.")
        return
    if status == "long_name":
        await message.answer("❌ Слишком длинное (макс. 64 символа).")
        return
    library = db.get_library(uid)
    await state.set_state(S.playlist_select_tracks)
    await state.update_data(playlist_id=pl_id, selected_ids=[], sel_page=0)
    if not library:
        await state.clear()
        await message.answer(t(uid, "pl_created_empty", name=name), parse_mode="Markdown")
        return
    await message.answer(t(uid, "pl_created", name=name), parse_mode="Markdown",
                         reply_markup=kb_select_tracks(library, set(), pl_id))


@dp.message(S.playlist_renaming, F.text)
async def pl_do_rename(message: Message, state: FSMContext):
    txt = message.text.strip()
    if txt in _get_btn_values():
        await state.clear()
        await btn_router(message, state)
        return
    data     = await state.get_data()
    pl_id    = data.get("rename_playlist_id")
    new_name = message.text.strip()
    uid      = message.from_user.id
    if not new_name or len(new_name) > 64:
        await message.answer("❌ Название от 1 до 64 символов.")
        return
    ok = db.rename_playlist(pl_id, uid, new_name)
    await state.clear()
    await message.answer(
        t(uid, "pl_renamed", name=new_name) if ok else "❌ Не удалось переименовать.",
        parse_mode="Markdown"
    )


@dp.message(S.playlist_join, F.text)
async def pl_join_by_code(message: Message, state: FSMContext):
    txt = message.text.strip()
    uid = message.from_user.id
    if txt in _BTN_VALUES:
        await state.clear()
        await btn_router(message, state)
        return
    code = txt.upper()
    await state.clear()
    pl = db.get_playlist_by_code(code)
    if not pl:
        await message.answer(t(uid, "pl_join_fail"))
        return
    new_id, status = db.copy_playlist_to_user(pl["playlist_id"], uid)
    if status == "ok":
        await message.answer(t(uid, "pl_join_ok", name=pl["name"]), parse_mode="Markdown")
    elif status == "limit":
        await message.answer(t(uid, "pl_full", lim=PLAYLIST_LIMIT))
    else:
        await message.answer("❌ Ошибка при добавлении плейлиста.")


@dp.message(S.support_writing, F.text | F.photo | F.video | F.document)
async def support_send(message: Message, state: FSMContext):
    await state.clear()
    uid      = message.from_user.id
    user     = message.from_user
    username = f"@{user.username}" if user.username else "нет username"
    if OWNER_ID == 0:
        await message.answer("⚠️ Поддержка недоступна.")
        return
    await bot.send_message(OWNER_ID,
        f"🆘 *Обращение*\n👤 {user.full_name} ({username})\n🆔 `{uid}`\n{'─'*25}",
        parse_mode="Markdown")
    await message.forward(OWNER_ID)
    await bot.send_message(OWNER_ID, f"👆 от `{uid}`",
                           parse_mode="Markdown",
                           reply_markup=kb_reply_to_user(uid))
    await message.answer(t(uid, "support_sent"))


@dp.message(S.appeal_writing, F.text)
async def appeal_send(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if db.has_pending_appeal(uid):
        await message.answer(t(uid, "appeal_already"))
        return
    appeal_id = db.submit_appeal(uid, message.text.strip())
    user = message.from_user
    username = f"@{user.username}" if user.username else "нет"
    if OWNER_ID != 0:
        await bot.send_message(OWNER_ID,
            f"📝 *Апелляция #{appeal_id}*\n👤 {user.full_name} ({username})\n🆔 `{uid}`\n\n{message.text}",
            parse_mode="Markdown",
            reply_markup=kb_appeal(appeal_id, uid))
    await message.answer(t(uid, "appeal_sent"))


@dp.message(S.owner_replying, F.text | F.photo | F.video | F.document)
async def owner_reply_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data      = await state.get_data()
    target_id = data.get("reply_target")
    await state.clear()
    try:
        await bot.send_message(target_id, "📩 *Ответ от поддержки:*", parse_mode="Markdown")
        await message.copy_to(target_id)
        await message.answer(f"✅ Отправлено пользователю `{target_id}`.", parse_mode="Markdown")
    except Exception as e:
        await message.answer(f"❌ Ошибка: `{e}`", parse_mode="Markdown")


@dp.message(S.broadcast_waiting, F.text | F.photo | F.video | F.document)
async def broadcast_send(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    await state.clear()
    users  = db.get_all_users()
    active = [u for u in users if not u.get("is_banned")]
    status_msg = await message.answer(f"📤 Рассылка... 0/{len(active)}")
    success, failed = 0, 0
    for i, u in enumerate(active):
        try:
            await message.copy_to(u["user_id"])
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
        f"✅ *Рассылка завершена!*\n📨 Отправлено: *{success}*\n❌ Ошибок: *{failed}*",
        parse_mode="Markdown"
    )


@dp.message(S.admin_msg_id, F.text)
async def admin_msg_get_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    txt = message.text.strip()
    if txt in _get_btn_values():
        await state.clear()
        await btn_router(message, state)
        return
    try:
        target_id = int(txt)
    except ValueError:
        await message.answer("❌ Некорректный ID.")
        return
    await state.update_data(admin_msg_target=target_id)
    await state.set_state(S.admin_msg_text)
    await message.answer(f"✉️ Введи сообщение для пользователя `{target_id}`:",
                         parse_mode="Markdown")


@dp.message(S.admin_msg_text, F.text | F.photo | F.video | F.document)
async def admin_msg_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    data      = await state.get_data()
    target_id = data.get("admin_msg_target")
    await state.clear()
    try:
        await message.copy_to(target_id)
        await message.answer(f"✅ Сообщение отправлено пользователю `{target_id}`.",
                             parse_mode="Markdown")
    except Exception as e:
        await message.answer(f"❌ Ошибка: `{e}`", parse_mode="Markdown")


@dp.message(S.tempban_id, F.text)
async def tempban_get_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    txt = message.text.strip()
    if txt in _get_btn_values():
        await state.clear()
        await btn_router(message, state)
        return
    try:
        target_id = int(txt)
    except ValueError:
        await message.answer("❌ Некорректный ID.")
        return
    await state.update_data(tempban_target=target_id)
    await state.set_state(S.tempban_duration)
    await message.answer(
        f"⏱ Укажи длительность бана для `{target_id}`.\n"
        "Формат: `1h`, `24h`, `7d`, `30d`",
        parse_mode="Markdown"
    )


@dp.message(S.tempban_duration, F.text)
async def tempban_apply(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    data      = await state.get_data()
    target_id = data.get("tempban_target")
    await state.clear()
    text = message.text.strip().lower()
    try:
        if text.endswith("h"):
            delta = timedelta(hours=int(text[:-1]))
        elif text.endswith("d"):
            delta = timedelta(days=int(text[:-1]))
        else:
            raise ValueError
    except ValueError:
        await message.answer("❌ Неверный формат. Используй: 1h, 24h, 7d")
        return
    until = (datetime.now(timezone.utc) + delta).isoformat()
    db.ban_user(target_id, until=until)
    until_str = (datetime.now(timezone.utc) + delta).strftime("%d.%m.%Y %H:%M UTC")
    await message.answer(
        f"⏱ Пользователь `{target_id}` временно забанен до *{until_str}*.",
        parse_mode="Markdown"
    )
    try:
        await bot.send_message(target_id,
            t(target_id, "banned_until", until=until_str))
    except Exception:
        pass


# ══════════════════════════════════════════
#  TRANSLITERATION
# ══════════════════════════════════════════

_TRANSLIT = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'yo','ж':'zh',
    'з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o',
    'п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'h','ц':'ts',
    'ч':'ch','ш':'sh','щ':'sch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu',
    'я':'ya',
}

def _translit(text: str) -> str:
    result = []
    for ch in text.lower():
        result.append(_TRANSLIT.get(ch, ch))
    return "".join(result)

def _has_cyrillic(text: str) -> bool:
    return any('\u0400' <= c <= '\u04FF' for c in text)


# ══════════════════════════════════════════
#  AUTO SEARCH (must be after all FSM)
# ══════════════════════════════════════════

async def _do_search(message: Message, query: str):
    uid = message.from_user.id
    db.ensure_user(uid, message.from_user.username)

    # Easter egg
    if "милан" in query.lower() and "хамет" in query.lower():
        db.ban_user(uid)
        await message.answer("ИДИ НАХУЙ")
        return
    msg = await message.answer(t(uid, "searching", q=query), parse_mode="Markdown")

    tracks = await search_soundcloud(query, limit=8)

    # Если запрос на кириллице — ищем транслитом параллельно
    if _has_cyrillic(query):
        translit_query = _translit(query)
        tracks, translit_tracks = await asyncio.gather(
            search_soundcloud(query, limit=6),
            search_soundcloud(translit_query, limit=6)
        )
        seen = {tr["track_id"] for tr in tracks}
        for tr in translit_tracks:
            if tr["track_id"] not in seen:
                tracks.append(tr)
                seen.add(tr["track_id"])
        tracks = tracks[:8]

    if not tracks:
        await msg.edit_text(t(uid, "not_found"))
        return
    for tr in tracks:
        db.upsert_track(tr)
    await msg.edit_text(
        t(uid, "found", n=len(tracks), q=query),
        parse_mode="Markdown",
        reply_markup=kb_results(tracks, query)
    )


# ══════════════════════════════════════════
#  COMMANDS
# ══════════════════════════════════════════

@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    uid = message.from_user.id
    db.ensure_user(uid, message.from_user.username)
    await state.clear()

    # Забаненный — показываем статус и кнопку апелляции
    if db.is_banned(uid) and not is_owner(uid):
        user      = db.get_user(uid)
        ban_until = user.get("ban_until") if user else None
        if ban_until:
            until_dt  = datetime.fromisoformat(ban_until)
            until_str = until_dt.strftime("%d.%m.%Y %H:%M UTC")
            ban_text  = t(uid, "banned_until", until=until_str)
        else:
            ban_text = t(uid, "banned")

        if db.has_pending_appeal(uid):
            await message.answer(
                f"{ban_text}\n\n📝 Твоя апелляция уже на рассмотрении."
            )
        else:
            await message.answer(
                f"{ban_text}\n\nЕсли считаешь это ошибкой — подай апелляцию:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="📝 Подать апелляцию", callback_data="start_appeal")
                ]])
            )
        return

    await message.answer(
        "🌍 Выбери язык / Choose language / Выберы мову / Тілді таңдаңыз:",
        reply_markup=kb_lang()
    )
    await state.set_state(S.lang_select)


@dp.callback_query(F.data == "start_appeal")
async def cb_start_appeal(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    if db.has_pending_appeal(uid):
        await callback.answer(t(uid, "appeal_already"), show_alert=True)
        return
    await state.set_state(S.appeal_writing)
    await callback.message.answer(t(uid, "appeal_prompt"))
    await callback.answer()


@dp.callback_query(F.data.startswith("setlang:"))
async def cb_setlang(callback: CallbackQuery, state: FSMContext):
    lang = callback.data.split(":")[1]
    uid  = callback.from_user.id
    db.set_lang(uid, lang)
    await state.clear()
    await callback.message.edit_text(
        f"✅ {SUPPORTED_LANGS[lang]}"
    )
    await callback.message.answer(
        t(uid, "welcome", name=callback.from_user.first_name),
        parse_mode="Markdown",
        reply_markup=kb_main(uid)
    )
    await callback.answer()


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    uid     = message.from_user.id
    current = await state.get_state()
    await state.clear()
    if current:
        await message.answer(t(uid, "cancelled"), reply_markup=kb_main(uid))
    else:
        await message.answer(t(uid, "nothing_to_cancel"))


@dp.message(Command("lang"))
async def cmd_lang(message: Message, state: FSMContext):
    await state.set_state(S.lang_select)
    await message.answer(
        "🌍 Выбери язык / Choose language / Выберы мову / Тілді таңдаңыз:",
        reply_markup=kb_lang()
    )
    uid = message.from_user.id
    await message.answer(t(uid, "help"), parse_mode="Markdown", reply_markup=kb_main(uid))


@dp.message(Command("joinplaylist"))
async def cmd_joinplaylist(message: Message, state: FSMContext):
    uid  = message.from_user.id
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        code = args[1].strip().upper()
        pl   = db.get_playlist_by_code(code)
        if not pl:
            await message.answer(t(uid, "pl_join_fail"))
            return
        new_id, status = db.copy_playlist_to_user(pl["playlist_id"], uid)
        if status == "ok":
            await message.answer(t(uid, "pl_join_ok", name=pl["name"]), parse_mode="Markdown")
        else:
            await message.answer(t(uid, "pl_full", lim=PLAYLIST_LIMIT))
        return
    await state.set_state(S.playlist_join)
    await message.answer(t(uid, "pl_join_prompt"))


# ══════════════════════════════════════════
#  BUTTON HANDLERS
# ══════════════════════════════════════════

def _is_btn(uid: int, key: str, text: str) -> bool:
    for lang_dict in T.values():
        val = lang_dict.get(key, "")
        if val and val.strip() == text.strip():
            return True
    return False


# Собираем все тексты кнопок в один set для быстрой проверки
def _all_btn_values() -> set:
    result = set()
    for lang_dict in T.values():
        for k, v in lang_dict.items():
            if k.startswith("btn_") and v:
                result.add(v.strip())
    result.add("👮 Панель админа")
    return result

# Инициализируем сразу, не лениво
_BTN_VALUES: set = _all_btn_values()

def _get_btn_values() -> set:
    return _BTN_VALUES


@dp.message(S.searching)
async def do_search_state(message: Message, state: FSMContext):
    txt = message.text.strip() if message.text else ""
    # Если написали кнопку меню — выходим из поиска и выполняем кнопку
    if txt in _get_btn_values() or txt == "👮 Панель админа":
        await state.clear()
        await btn_router(message, state)
        return
    await state.clear()
    await _do_search(message, txt)


@dp.message(F.text & ~F.text.startswith("/"))
async def btn_router(message: Message, state: FSMContext):
    uid  = message.from_user.id
    txt  = message.text.strip()
    cur  = await state.get_state()
    if cur is not None:
        return

    if _is_btn(uid, "btn_search", txt):
        await state.set_state(S.searching)
        await message.answer(t(uid, "search_prompt"))

    elif _is_btn(uid, "btn_library", txt):
        await show_library(message)

    elif _is_btn(uid, "btn_playlists", txt):
        await show_playlists(message)

    elif _is_btn(uid, "btn_random", txt):
        await random_track(message)

    elif _is_btn(uid, "btn_top", txt):
        await show_top(message)

    elif _is_btn(uid, "btn_support", txt):
        await support_start(message, state)

    elif _is_btn(uid, "btn_help", txt):
        await message.answer(t(uid, "help"), parse_mode="Markdown")

    elif _is_btn(uid, "btn_owner", txt) and is_owner(uid):
        await owner_panel(message)

    elif _is_btn(uid, "btn_maint_on", txt) and is_owner(uid):
        await toggle_maintenance(message)

    elif _is_btn(uid, "btn_maint_off", txt) and is_owner(uid):
        await toggle_maintenance(message)

    elif txt == "👮 Панель админа" and is_admin(uid):
        await admin_panel(message)

    # Поиск только если текст НЕ является кнопкой ни на одном языке
    elif txt not in _get_btn_values() and len(txt) >= 2:
        await _do_search(message, txt)


# ══════════════════════════════════════════
#  LIBRARY
# ══════════════════════════════════════════

async def show_library(message: Message):
    uid    = message.from_user.id
    tracks = db.get_library(uid)
    if not tracks:
        await message.answer(t(uid, "lib_empty"), parse_mode="Markdown")
        return
    await message.answer(
        t(uid, "lib_title", n=len(tracks), lim=LIBRARY_LIMIT),
        parse_mode="Markdown",
        reply_markup=kb_library(tracks, uid)
    )


@dp.callback_query(F.data.startswith("libpage:"))
async def cb_libpage(callback: CallbackQuery):
    page   = int(callback.data.split(":")[1])
    uid    = callback.from_user.id
    tracks = db.get_library(uid)
    await callback.message.edit_reply_markup(reply_markup=kb_library(tracks, uid, page))
    await callback.answer()


@dp.callback_query(F.data.startswith("libplay:"))
async def cb_libplay(callback: CallbackQuery):
    track_id = callback.data.split(":", 1)[1]
    track    = db.get_track(track_id)
    uid      = callback.from_user.id
    if not track:
        await callback.answer("❌ Трек не найден.", show_alert=True)
        return
    await callback.answer("⏳")
    await send_audio_track(uid, track, in_library=True, uid=uid)


@dp.callback_query(F.data.startswith("save:"))
async def cb_save(callback: CallbackQuery):
    track_id = callback.data.split(":", 1)[1]
    uid      = callback.from_user.id
    track    = db.get_track(track_id)
    if not track:
        await callback.answer("❌ Трек не найден.", show_alert=True)
        return
    ok, status = db.add_to_library(uid, track_id)
    if status == "limit":
        await callback.answer(t(uid, "lib_full", lim=LIBRARY_LIMIT), show_alert=True)
        return
    if status == "already":
        await callback.answer(t(uid, "already"))
        return
    count = db.library_count(uid)
    await callback.answer(t(uid, "saved", n=count, lim=LIBRARY_LIMIT))
    await callback.message.edit_reply_markup(
        reply_markup=kb_track_actions(track_id, True, uid)
    )


@dp.callback_query(F.data.startswith("remove:"))
async def cb_remove(callback: CallbackQuery):
    track_id = callback.data.split(":", 1)[1]
    uid      = callback.from_user.id
    db.remove_from_library(uid, track_id)
    await callback.answer(t(uid, "removed"))
    await callback.message.edit_reply_markup(
        reply_markup=kb_track_actions(track_id, False, uid)
    )


@dp.callback_query(F.data == "lib_clear")
async def cb_lib_clear(callback: CallbackQuery):
    uid = callback.from_user.id
    db.clear_library(uid)
    await callback.message.edit_text(t(uid, "lib_cleared"))
    await callback.answer()


# ══════════════════════════════════════════
#  RANDOM / TOP
# ══════════════════════════════════════════

async def random_track(message: Message):
    uid   = message.from_user.id
    track = db.get_random_track(uid)
    if not track:
        await message.answer(t(uid, "random_empty"))
        return
    await send_audio_track(uid, track, in_library=True, uid=uid)


async def show_top(message: Message):
    uid    = message.from_user.id
    week   = db.get_last_week_str()
    tracks = db.get_top_tracks(week=week)
    if not tracks:
        week   = db._current_week()
        tracks = db.get_top_tracks(week=week)
    if not tracks:
        await message.answer(t(uid, "top_empty"))
        return
    lines = []
    for i, tr in enumerate(tracks, 1):
        lines.append(f"{i}. *{tr['artist']}* — {tr['title']} _{tr['count']} plays_")
    await message.answer(
        t(uid, "top_title", week=week) + "\n".join(lines),
        parse_mode="Markdown"
    )


# ══════════════════════════════════════════
#  PLAY CALLBACKS
# ══════════════════════════════════════════

@dp.callback_query(F.data.startswith("play:"))
async def cb_play(callback: CallbackQuery):
    _, idx_str, query = callback.data.split(":", 2)
    uid = callback.from_user.id
    await callback.answer("⏳")
    await callback.message.edit_reply_markup(reply_markup=None)
    tracks = await search_soundcloud(query, limit=8)
    idx    = int(idx_str)
    if idx >= len(tracks):
        await callback.message.answer("❌ Трек не найден.")
        return
    track = tracks[idx]
    db.upsert_track(track)
    in_lib = db.is_in_library(uid, track.get("track_id",""))
    await send_audio_track(uid, track, in_lib, uid=uid)


# ══════════════════════════════════════════
#  PLAYLISTS
# ══════════════════════════════════════════

async def show_playlists(message: Message):
    uid = message.from_user.id
    db.ensure_user(uid, message.from_user.username)
    pls = db.get_playlists(uid)
    if not pls:
        await message.answer(
            t(uid, "no_playlists", lim=PLAYLIST_LIMIT),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="➕ Новый / New", callback_data="pl_create"),
                InlineKeyboardButton(text="🔗 По коду / By code", callback_data="pl_join_start"),
            ]])
        )
        return
    await message.answer(
        t(uid, "playlists_title", n=len(pls), lim=PLAYLIST_LIMIT),
        parse_mode="Markdown",
        reply_markup=kb_playlists(pls)
    )


@dp.callback_query(F.data.startswith("plpage:"))
async def cb_plpage(callback: CallbackQuery):
    page = int(callback.data.split(":")[1])
    pls  = db.get_playlists(callback.from_user.id)
    await callback.message.edit_reply_markup(reply_markup=kb_playlists(pls, page))
    await callback.answer()


@dp.callback_query(F.data == "pl_back")
async def cb_pl_back(callback: CallbackQuery):
    uid = callback.from_user.id
    pls = db.get_playlists(uid)
    if not pls:
        await callback.message.edit_text(
            t(uid, "no_playlists", lim=PLAYLIST_LIMIT),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="➕ Новый / New", callback_data="pl_create"),
                InlineKeyboardButton(text="🔗 По коду / By code", callback_data="pl_join_start"),
            ]])
        )
    else:
        await callback.message.edit_text(
            t(uid, "playlists_title", n=len(pls), lim=PLAYLIST_LIMIT),
            parse_mode="Markdown",
            reply_markup=kb_playlists(pls)
        )
    await callback.answer()


@dp.callback_query(F.data == "pl_join_start")
async def cb_pl_join_start(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    await state.set_state(S.playlist_join)
    await callback.message.answer(t(uid, "pl_join_prompt"))
    await callback.answer()


@dp.callback_query(F.data == "pl_create")
async def cb_pl_create(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    if db.playlist_count(uid) >= PLAYLIST_LIMIT:
        await callback.answer(t(uid, "pl_full", lim=PLAYLIST_LIMIT), show_alert=True)
        return
    await state.set_state(S.playlist_naming)
    await callback.message.answer(t(uid, "pl_name_prompt"))
    await callback.answer()


@dp.callback_query(F.data.startswith("pl_open:"))
async def cb_pl_open(callback: CallbackQuery):
    pl_id  = int(callback.data.split(":")[1])
    uid    = callback.from_user.id
    pl     = db.get_playlist(pl_id, uid)
    if not pl:
        await callback.answer("❌ Плейлист не найден.", show_alert=True)
        return
    tracks = db.get_playlist_tracks(pl_id)
    text   = (f"🎶 *{pl['name']}*\n"
              f"Треков: {len(tracks)}/{PLAYLIST_TRACKS_LIMIT}\n"
              + ("▶️ Нажми трек. ✖️ — удалить." if tracks else "📭 Плейлист пуст."))
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=kb_playlist_detail(pl_id, tracks, pl.get("share_code","????"))
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("pltrpage:"))
async def cb_pltrpage(callback: CallbackQuery):
    _, pl_id_s, page_s = callback.data.split(":")
    pl_id, page = int(pl_id_s), int(page_s)
    uid = callback.from_user.id
    pl  = db.get_playlist(pl_id, uid)
    if not pl:
        await callback.answer("❌"); return
    tracks = db.get_playlist_tracks(pl_id)
    await callback.message.edit_reply_markup(
        reply_markup=kb_playlist_detail(pl_id, tracks, pl.get("share_code","????"), page)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("plplay:"))
async def cb_plplay(callback: CallbackQuery):
    track_id = callback.data.split(":", 1)[1]
    uid      = callback.from_user.id
    track    = db.get_track(track_id)
    if not track:
        await callback.answer("❌ Трек не найден.", show_alert=True)
        return
    await callback.answer("⏳")
    in_lib = db.is_in_library(uid, track_id)
    await send_audio_track(uid, track, in_lib, uid=uid)


@dp.callback_query(F.data.startswith("pl_rmtrack:"))
async def cb_pl_rmtrack(callback: CallbackQuery):
    parts = callback.data.split(":")
    pl_id, track_id = int(parts[1]), parts[2]
    uid = callback.from_user.id
    pl  = db.get_playlist(pl_id, uid)
    if not pl:
        await callback.answer("❌"); return
    db.remove_track_from_playlist(pl_id, track_id)
    tracks = db.get_playlist_tracks(pl_id)
    text = (f"🎶 *{pl['name']}*\nТреков: {len(tracks)}/{PLAYLIST_TRACKS_LIMIT}\n"
            + ("▶️ Нажми трек. ✖️ — удалить." if tracks else "📭 Плейлист пуст."))
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=kb_playlist_detail(pl_id, tracks, pl.get("share_code","????"))
    )
    await callback.answer("🗑 Удалено")


@dp.callback_query(F.data.startswith("pl_addtracks:"))
async def cb_pl_addtracks(callback: CallbackQuery, state: FSMContext):
    pl_id = int(callback.data.split(":")[1])
    uid   = callback.from_user.id
    pl    = db.get_playlist(pl_id, uid)
    if not pl:
        await callback.answer("❌"); return
    library = db.get_library(uid)
    if not library:
        await callback.answer("📭 Библиотека пуста.", show_alert=True); return
    if db.playlist_track_count(pl_id) >= PLAYLIST_TRACKS_LIMIT:
        await callback.answer(f"⛔ Плейлист полон ({PLAYLIST_TRACKS_LIMIT}).", show_alert=True); return
    await state.set_state(S.playlist_select_tracks)
    await state.update_data(playlist_id=pl_id, selected_ids=[], sel_page=0)
    await callback.message.answer(
        f"➕ Добавление в *{pl['name']}*",
        parse_mode="Markdown",
        reply_markup=kb_select_tracks(library, set(), pl_id)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("sel_toggle:"))
async def cb_sel_toggle(callback: CallbackQuery, state: FSMContext):
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
async def cb_sel_page(callback: CallbackQuery, state: FSMContext):
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
async def cb_sel_confirm(callback: CallbackQuery, state: FSMContext):
    data     = await state.get_data()
    selected = list(data.get("selected_ids", []))
    pl_id    = data.get("playlist_id")
    uid      = callback.from_user.id
    await state.clear()
    if not selected:
        await callback.answer("⚠️ Не выбрано ни одного трека.", show_alert=True)
        return
    added, status = db.add_tracks_to_playlist_bulk(pl_id, selected)
    pl = db.get_playlist(pl_id, uid)
    pl_name = pl["name"] if pl else "плейлист"
    msg = (f"⛔ Плейлист полон (макс. {PLAYLIST_TRACKS_LIMIT})."
           if status == "limit"
           else t(uid, "pl_tracks_added", n=added, name=pl_name))
    await callback.message.edit_text(msg, parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "sel_cancel")
async def cb_sel_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(t(callback.from_user.id, "cancelled"))
    await callback.answer()


@dp.callback_query(F.data.startswith("pl_rename:"))
async def cb_pl_rename(callback: CallbackQuery, state: FSMContext):
    pl_id = int(callback.data.split(":")[1])
    uid   = callback.from_user.id
    pl    = db.get_playlist(pl_id, uid)
    if not pl:
        await callback.answer("❌"); return
    await state.set_state(S.playlist_renaming)
    await state.update_data(rename_playlist_id=pl_id)
    await callback.message.answer(f"✏️ Новое название для *{pl['name']}*:",
                                  parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data.startswith("pl_delete:"))
async def cb_pl_delete(callback: CallbackQuery):
    pl_id = int(callback.data.split(":")[1])
    uid   = callback.from_user.id
    pl    = db.get_playlist(pl_id, uid)
    if not pl:
        await callback.answer("❌"); return
    db.delete_playlist(pl_id, uid)
    pls = db.get_playlists(uid)
    if pls:
        await callback.message.edit_text(
            t(uid, "pl_deleted", name=pl["name"]) + "\n\n" +
            t(uid, "playlists_title", n=len(pls), lim=PLAYLIST_LIMIT),
            parse_mode="Markdown",
            reply_markup=kb_playlists(pls)
        )
    else:
        await callback.message.edit_text(
            t(uid, "pl_deleted", name=pl["name"]), parse_mode="Markdown"
        )
    await callback.answer()


@dp.callback_query(F.data.startswith("pl_sharecode:"))
async def cb_pl_sharecode(callback: CallbackQuery):
    pl_id = int(callback.data.split(":")[1])
    uid   = callback.from_user.id
    pl    = db.get_playlist(pl_id, uid)
    if not pl:
        await callback.answer("❌"); return
    code = pl.get("share_code","????")
    await callback.answer(f"Код: {code}", show_alert=True)
    await callback.message.answer(
        t(uid, "pl_code", code=code),
        parse_mode="Markdown"
    )


# ══════════════════════════════════════════
#  SUPPORT
# ══════════════════════════════════════════

async def support_start(message: Message, state: FSMContext):
    uid = message.from_user.id
    if is_admin(uid):
        await message.answer("👑 Обращения приходят автоматически.")
        return
    if db.is_banned(uid):
        # Banned — offer appeal instead
        if db.has_pending_appeal(uid):
            await message.answer(t(uid, "appeal_already"))
        else:
            await state.set_state(S.appeal_writing)
            await message.answer(t(uid, "appeal_prompt"))
        return
    await state.set_state(S.support_writing)
    await message.answer(t(uid, "support_prompt"), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("owner_reply:"))
async def cb_owner_reply(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔", show_alert=True); return
    target_id = int(callback.data.split(":")[1])
    await state.set_state(S.owner_replying)
    await state.update_data(reply_target=target_id)
    await callback.message.answer(
        f"💬 Ответ пользователю `{target_id}`.\n/cancel — отменить.",
        parse_mode="Markdown"
    )
    await callback.answer()


# ══════════════════════════════════════════
#  APPEALS
# ══════════════════════════════════════════

@dp.callback_query(F.data.startswith("appeal_ok:"))
async def cb_appeal_ok(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔", show_alert=True); return
    _, appeal_id_s, user_id_s = callback.data.split(":")
    uid = db.resolve_appeal(int(appeal_id_s), "approved")
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(f"✅ Апелляция одобрена. Пользователь `{uid}` разблокирован.",
                                  parse_mode="Markdown")
    try:
        await bot.send_message(uid, "✅ Ваша апелляция одобрена. Вы разблокированы!")
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data.startswith("appeal_no:"))
async def cb_appeal_no(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔", show_alert=True); return
    _, appeal_id_s, user_id_s = callback.data.split(":")
    uid = db.resolve_appeal(int(appeal_id_s), "rejected")
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(f"❌ Апелляция отклонена для `{uid}`.", parse_mode="Markdown")
    try:
        await bot.send_message(uid, "❌ Ваша апелляция отклонена.")
    except Exception:
        pass
    await callback.answer()


# ══════════════════════════════════════════
#  ADMIN PANEL
# ══════════════════════════════════════════

async def admin_panel(message: Message):
    uid = message.from_user.id
    if not is_admin(uid): return
    appeals = db.get_pending_appeals()
    appeal_note = f"\n📝 Апелляций на рассмотрении: *{len(appeals)}*" if appeals else ""
    await message.answer(
        "👮 *Панель админа*\n\n"
        f"👥 Пользователей: *{db.get_users_count()}*\n"
        f"🚫 Заблокировано: *{db.get_banned_count()}*{appeal_note}\n\n"
        "━━━━━━━━━━━━━━━━\n"
        "*Команды:*\n"
        "/users — список пользователей\n"
        "/ban `<id>` — перманентный бан\n"
        "/unban `<id>` — разбан\n"
        "/tempban — временный бан\n"
        "/appeals — список апелляций\n"
        "/msguser — написать пользователю\n"
        "/viewlib `<id>` — библиотека пользователя\n"
        "/viewplaylists `<id>` — плейлисты пользователя"
        + ("\n/addadmin `<id>` — назначить админа\n/removeadmin `<id>` — снять админа\n/broadcast — рассылка"
           if is_owner(uid) else ""),
        parse_mode="Markdown"
    )


# ══════════════════════════════════════════
#  OWNER PANEL
# ══════════════════════════════════════════

async def owner_panel(message: Message):
    if not is_owner(message.from_user.id): return
    uid    = message.from_user.id
    status = "🔴 Включены" if maintenance_mode else "🟢 Выключены"
    appeals = db.get_pending_appeals()
    appeal_note = f"\n📝 Апелляций на рассмотрении: *{len(appeals)}*" if appeals else ""
    await message.answer(
        "👑 *Панель владельца*\n\n"
        "📊 *Статистика:*\n"
        f"  👥 Пользователей: *{db.get_users_count()}*\n"
        f"  🚫 Заблокировано: *{db.get_banned_count()}*\n"
        f"  🎵 Треков в каталоге: *{db.get_tracks_count()}*\n"
        f"  💾 Сохранений в библиотеках: *{db.get_library_total()}*\n"
        f"  🎶 Плейлистов создано: *{db.get_playlists_total()}*\n"
        f"  🔧 Техработы: {status}{appeal_note}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📋 *Все команды:*\n\n"
        "👥 *Пользователи:*\n"
        "/users — список всех пользователей\n"
        "/ban `<id>` — перманентный бан\n"
        "/unban `<id>` — разбан\n"
        "/tempban — временный бан (1h/7d/30d)\n"
        "/appeals — апелляции на рассмотрении\n\n"
        "💬 *Сообщения:*\n"
        "/msguser — написать пользователю по ID\n"
        "/broadcast — рассылка всем пользователям\n\n"
        "📚 *Просмотр данных:*\n"
        "/viewlib `<id>` — библиотека пользователя\n"
        "/viewplaylists `<id>` — плейлисты пользователя\n"
        "/stats — подробная статистика\n\n"
        "👮 *Администраторы (только владелец):*\n"
        "/addadmin `<id>` — назначить администратора\n"
        "/removeadmin `<id>` — снять администратора\n\n"
        "🔧 *Управление ботом:*\n"
        "Кнопка 🔴/🟢 — включить/выключить техработы",
        parse_mode="Markdown",
        reply_markup=kb_main(uid)
    )


async def toggle_maintenance(message: Message):
    if not is_owner(message.from_user.id): return
    global maintenance_mode
    maintenance_mode = not maintenance_mode
    uid = message.from_user.id
    if maintenance_mode:
        await message.answer("🔴 Технические работы включены.", reply_markup=kb_main(uid))
    else:
        await message.answer("🟢 Бот снова доступен.", reply_markup=kb_main(uid))


# ══════════════════════════════════════════
#  ADMIN COMMANDS
# ══════════════════════════════════════════

@dp.message(Command("users"))
async def cmd_users(message: Message):
    if not is_admin(message.from_user.id): return
    users = db.get_all_users()
    if not users:
        await message.answer("👥 Пользователей нет."); return
    lines = []
    for u in users[:50]:
        banned = " 🚫" if u.get("is_banned") else ""
        admin  = " 👮" if u.get("is_admin") else ""
        uname  = f" @{u['username']}" if u.get("username") else ""
        lines.append(f"• `{u['user_id']}`{uname}{admin}{banned}")
    text = f"👥 *Пользователи* ({len(users)}):\n\n" + "\n".join(lines)
    if len(users) > 50:
        text += f"\n...и ещё {len(users)-50}"
    await message.answer(text, parse_mode="Markdown")


@dp.message(Command("ban"))
async def cmd_ban(message: Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /ban `<id>`", parse_mode="Markdown"); return
    try:
        tid = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный ID."); return
    if tid == OWNER_ID:
        await message.answer("❌ Нельзя забанить владельца."); return
    db.ban_user(tid)
    await message.answer(f"🚫 Пользователь `{tid}` забанен.", parse_mode="Markdown")
    try:
        await bot.send_message(tid, t(tid, "banned"))
    except Exception:
        pass


@dp.message(Command("unban"))
async def cmd_unban(message: Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /unban `<id>`", parse_mode="Markdown"); return
    try:
        tid = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный ID."); return
    db.unban_user(tid)
    await message.answer(f"✅ Пользователь `{tid}` разблокирован.", parse_mode="Markdown")
    try:
        await bot.send_message(tid, "✅ Вы разблокированы!")
    except Exception:
        pass


@dp.message(Command("tempban"))
async def cmd_tempban(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.set_state(S.tempban_id)
    await message.answer("🆔 Введи ID пользователя для временного бана:")


@dp.message(Command("appeals"))
async def cmd_appeals(message: Message):
    if not is_admin(message.from_user.id): return
    appeals = db.get_pending_appeals()
    if not appeals:
        await message.answer("📝 Нет апелляций на рассмотрении."); return
    for ap in appeals[:10]:
        uname = f"@{ap['username']}" if ap.get("username") else "нет"
        await message.answer(
            f"📝 *Апелляция #{ap['appeal_id']}*\n"
            f"👤 {uname} | `{ap['user_id']}`\n\n"
            f"{ap['text']}",
            parse_mode="Markdown",
            reply_markup=kb_appeal(ap["appeal_id"], ap["user_id"])
        )


@dp.message(Command("msguser"))
async def cmd_msguser(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.set_state(S.admin_msg_id)
    await message.answer("🆔 Введи ID пользователя:")


@dp.message(Command("viewlib"))
async def cmd_viewlib(message: Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /viewlib `<id>`", parse_mode="Markdown"); return
    try:
        tid = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный ID."); return
    tracks = db.get_library(tid)
    if not tracks:
        await message.answer(f"📚 Библиотека пользователя `{tid}` пуста.", parse_mode="Markdown"); return
    lines = [f"{i}. *{tr['artist']}* — {tr['title']}" for i, tr in enumerate(tracks[:30], 1)]
    await message.answer(
        f"📚 *Библиотека* `{tid}` ({len(tracks)} треков):\n\n" + "\n".join(lines),
        parse_mode="Markdown"
    )


@dp.message(Command("viewplaylists"))
async def cmd_viewplaylists(message: Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /viewplaylists `<id>`", parse_mode="Markdown"); return
    try:
        tid = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный ID."); return
    pls = db.get_playlists(tid)
    if not pls:
        await message.answer(f"🎵 У пользователя `{tid}` нет плейлистов.", parse_mode="Markdown"); return
    lines = [f"• *{pl['name']}* — {pl['track_count']} тр. | код: `{pl.get('share_code','?')}`"
             for pl in pls]
    await message.answer(
        f"🎵 *Плейлисты* `{tid}` ({len(pls)}):\n\n" + "\n".join(lines),
        parse_mode="Markdown"
    )


@dp.message(Command("addadmin"))
async def cmd_addadmin(message: Message):
    if not is_owner(message.from_user.id): return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /addadmin `<id>`", parse_mode="Markdown"); return
    try:
        tid = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный ID."); return
    db.set_admin(tid, True)
    await message.answer(f"👮 Пользователь `{tid}` назначен админом.", parse_mode="Markdown")
    try:
        await bot.send_message(tid, "👮 Вы назначены администратором бота!")
    except Exception:
        pass


@dp.message(Command("removeadmin"))
async def cmd_removeadmin(message: Message):
    if not is_owner(message.from_user.id): return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /removeadmin `<id>`", parse_mode="Markdown"); return
    try:
        tid = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный ID."); return
    db.set_admin(tid, False)
    await message.answer(f"✅ Права админа сняты с `{tid}`.", parse_mode="Markdown")


@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id): return
    active = db.get_users_count() - db.get_banned_count()
    await state.set_state(S.broadcast_waiting)
    await message.answer(
        f"📢 *Рассылка*\nПолучателей: *{active}*\n\nОтправь сообщение.\n/cancel — отменить.",
        parse_mode="Markdown"
    )


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not is_admin(message.from_user.id): return
    await message.answer(
        f"📊 *Статистика*\n\n"
        f"👥 Пользователей: *{db.get_users_count()}*\n"
        f"🚫 Заблокировано: *{db.get_banned_count()}*\n"
        f"🎵 Треков: *{db.get_tracks_count()}*\n"
        f"💾 Сохранений: *{db.get_library_total()}*\n"
        f"🎶 Плейлистов: *{db.get_playlists_total()}*",
        parse_mode="Markdown"
    )


# ══════════════════════════════════════════
#  MISC
# ══════════════════════════════════════════

@dp.callback_query(F.data == "close")
async def cb_close(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()


@dp.callback_query(F.data == "noop")
async def cb_noop(callback: CallbackQuery):
    await callback.answer()


# ══════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════

async def main():
    logger.info("Bot starting...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
