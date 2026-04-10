"""
database.py — полная база данных музыкального бота.
Новое: admins, temp_ban, appeals, play_counts, playlist share codes, languages.
"""
import sqlite3, json, random, string, logging
from typing import Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

LIBRARY_LIMIT         = 100
PLAYLIST_LIMIT        = 25
PLAYLIST_TRACKS_LIMIT = 50

SUPPORTED_LANGS = {"ru": "🇷🇺 Русский", "en": "🇬🇧 English",
                   "be": "🇧🇾 Беларуская", "kk": "🇰🇿 Қазақша"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, path: str = "library.db"):
        self.path = path
        self._init_db()

    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id    INTEGER PRIMARY KEY,
                    username   TEXT,
                    lang       TEXT NOT NULL DEFAULT 'ru',
                    is_banned  INTEGER NOT NULL DEFAULT 0,
                    ban_until  TEXT,
                    is_admin   INTEGER NOT NULL DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS tracks (
                    track_id     TEXT PRIMARY KEY,
                    title        TEXT NOT NULL,
                    artist       TEXT NOT NULL,
                    url          TEXT,
                    duration_sec INTEGER,
                    duration_fmt TEXT,
                    artwork_url  TEXT,
                    sc_id        INTEGER,
                    extra        TEXT
                );

                CREATE TABLE IF NOT EXISTS library (
                    user_id   INTEGER NOT NULL,
                    track_id  TEXT    NOT NULL,
                    added_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, track_id),
                    FOREIGN KEY (user_id)  REFERENCES users(user_id)  ON DELETE CASCADE,
                    FOREIGN KEY (track_id) REFERENCES tracks(track_id)
                );

                CREATE TABLE IF NOT EXISTS playlists (
                    playlist_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id     INTEGER NOT NULL,
                    name        TEXT    NOT NULL,
                    share_code  TEXT    UNIQUE,
                    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS playlist_tracks (
                    playlist_id INTEGER NOT NULL,
                    track_id    TEXT    NOT NULL,
                    position    INTEGER NOT NULL DEFAULT 0,
                    added_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (playlist_id, track_id),
                    FOREIGN KEY (playlist_id) REFERENCES playlists(playlist_id) ON DELETE CASCADE,
                    FOREIGN KEY (track_id)    REFERENCES tracks(track_id)
                );

                CREATE TABLE IF NOT EXISTS play_counts (
                    track_id   TEXT    NOT NULL,
                    week       TEXT    NOT NULL,
                    count      INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (track_id, week),
                    FOREIGN KEY (track_id) REFERENCES tracks(track_id)
                );

                CREATE TABLE IF NOT EXISTS appeals (
                    appeal_id  INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id    INTEGER NOT NULL,
                    text       TEXT    NOT NULL,
                    status     TEXT    NOT NULL DEFAULT 'pending',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );
            """)
            # Миграции для старых БД
            for col, definition in [
                ("lang",      "TEXT NOT NULL DEFAULT 'ru'"),
                ("ban_until", "TEXT"),
                ("is_admin",  "INTEGER NOT NULL DEFAULT 0"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE users ADD COLUMN {col} {definition}")
                except Exception:
                    pass
            for col, definition in [
                ("share_code", "TEXT UNIQUE"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE playlists ADD COLUMN {col} {definition}")
                except Exception:
                    pass
        logger.info("Database ready: %s", self.path)

    # ─── Internal ────────────────────────────

    def _row_to_track(self, t: dict) -> dict:
        extra = json.loads(t.pop("extra") or "{}")
        t["_sc_id"] = t.get("sc_id") or extra.get("_sc_id")
        return t

    def _rows_to_tracks(self, rows) -> list:
        return [self._row_to_track(dict(r)) for r in rows]

    def _current_week(self) -> str:
        now = datetime.now(timezone.utc)
        return f"{now.year}-W{now.strftime('%V')}"

    # ─── Users ───────────────────────────────

    def ensure_user(self, user_id: int, username: str = None):
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO users (user_id, username) VALUES (?,?)",
                (user_id, username)
            )
            if username:
                conn.execute(
                    "UPDATE users SET username=? WHERE user_id=?",
                    (username, user_id)
                )

    def get_user(self, user_id: int) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
        return dict(row) if row else None

    def set_lang(self, user_id: int, lang: str):
        with self._conn() as conn:
            conn.execute("UPDATE users SET lang=? WHERE user_id=?", (lang, user_id))

    def get_lang(self, user_id: int) -> str:
        with self._conn() as conn:
            row = conn.execute("SELECT lang FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row["lang"] if row else "ru"

    def has_lang_set(self, user_id: int) -> bool:
        u = self.get_user(user_id)
        return bool(u and u.get("lang"))

    # ─── Ban / Admin ──────────────────────────

    def is_banned(self, user_id: int) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT is_banned, ban_until FROM users WHERE user_id=?", (user_id,)
            ).fetchone()
        if not row:
            return False
        if not row["is_banned"]:
            return False
        if row["ban_until"]:
            until = datetime.fromisoformat(row["ban_until"])
            if datetime.now(timezone.utc) >= until:
                self.unban_user(user_id)
                return False
        return True

    def ban_user(self, user_id: int, until: Optional[str] = None):
        """until — ISO datetime строка или None (перманентный бан)."""
        self.ensure_user(user_id)
        with self._conn() as conn:
            conn.execute(
                "UPDATE users SET is_banned=1, ban_until=? WHERE user_id=?",
                (until, user_id)
            )

    def unban_user(self, user_id: int):
        with self._conn() as conn:
            conn.execute(
                "UPDATE users SET is_banned=0, ban_until=NULL WHERE user_id=?", (user_id,)
            )

    def is_admin(self, user_id: int) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT is_admin FROM users WHERE user_id=?", (user_id,)
            ).fetchone()
        return bool(row["is_admin"]) if row else False

    def set_admin(self, user_id: int, value: bool):
        self.ensure_user(user_id)
        with self._conn() as conn:
            conn.execute(
                "UPDATE users SET is_admin=? WHERE user_id=?", (int(value), user_id)
            )

    def get_all_users(self) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM users ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_users_count(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]

    def get_banned_count(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM users WHERE is_banned=1").fetchone()[0]

    def get_admins(self) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM users WHERE is_admin=1"
            ).fetchall()
        return [dict(r) for r in rows]

    # ─── Appeals ─────────────────────────────

    def submit_appeal(self, user_id: int, text: str) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO appeals (user_id, text) VALUES (?,?)", (user_id, text)
            )
        return cur.lastrowid

    def get_pending_appeals(self) -> list:
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT a.*, u.username FROM appeals a
                LEFT JOIN users u ON a.user_id = u.user_id
                WHERE a.status='pending'
                ORDER BY a.created_at ASC
            """).fetchall()
        return [dict(r) for r in rows]

    def resolve_appeal(self, appeal_id: int, status: str):
        """status: 'approved' | 'rejected'"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT user_id FROM appeals WHERE appeal_id=?", (appeal_id,)
            ).fetchone()
            conn.execute(
                "UPDATE appeals SET status=? WHERE appeal_id=?", (status, appeal_id)
            )
        if status == "approved" and row:
            self.unban_user(row["user_id"])
        return row["user_id"] if row else None

    def has_pending_appeal(self, user_id: int) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM appeals WHERE user_id=? AND status='pending'", (user_id,)
            ).fetchone()
        return row is not None

    # ─── Tracks ──────────────────────────────

    def upsert_track(self, track: dict):
        extra = {k: v for k, v in track.items()
                 if k not in ("track_id","title","artist","url","duration_sec",
                              "duration_fmt","artwork_url","sc_id","_sc_id")}
        with self._conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO tracks
                    (track_id, title, artist, url, duration_sec, duration_fmt,
                     artwork_url, sc_id, extra)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                track.get("track_id",""),
                track.get("title","Unknown"),
                track.get("artist","Unknown"),
                track.get("url",""),
                track.get("duration_sec"),
                track.get("duration_fmt",""),
                track.get("artwork_url",""),
                track.get("_sc_id") or track.get("sc_id"),
                json.dumps(extra),
            ))

    def save_track(self, track: dict):
        self.upsert_track(track)

    def get_track(self, track_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM tracks WHERE track_id=?", (track_id,)
            ).fetchone()
        return self._row_to_track(dict(row)) if row else None

    def get_tracks_count(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]

    # ─── Play counts ─────────────────────────

    def record_play(self, track_id: str):
        week = self._current_week()
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO play_counts (track_id, week, count) VALUES (?,?,1)
                ON CONFLICT(track_id, week) DO UPDATE SET count = count + 1
            """, (track_id, week))

    def get_top_tracks(self, week: Optional[str] = None, limit: int = 10) -> list:
        if week is None:
            week = self._current_week()
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT t.title, t.artist, t.track_id, pc.count
                FROM play_counts pc
                JOIN tracks t ON t.track_id = pc.track_id
                WHERE pc.week=?
                ORDER BY pc.count DESC
                LIMIT ?
            """, (week, limit)).fetchall()
        return [dict(r) for r in rows]

    def get_last_week_str(self) -> str:
        now = datetime.now(timezone.utc)
        year, week, _ = (now.isocalendar()[0],
                         now.isocalendar()[1] - 1,
                         now.isocalendar()[2])
        if week == 0:
            year -= 1
            week = 52
        return f"{year}-W{week:02d}"

    # ─── Library ─────────────────────────────

    def library_count(self, user_id: int) -> int:
        with self._conn() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM library WHERE user_id=?", (user_id,)
            ).fetchone()[0]

    def add_to_library(self, user_id: int, track_id: str) -> tuple:
        self.ensure_user(user_id)
        if self.is_in_library(user_id, track_id):
            return False, "already"
        if self.library_count(user_id) >= LIBRARY_LIMIT:
            return False, "limit"
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO library (user_id, track_id) VALUES (?,?)",
                (user_id, track_id)
            )
        return True, "ok"

    def remove_from_library(self, user_id: int, track_id: str):
        with self._conn() as conn:
            for pl in conn.execute(
                "SELECT playlist_id FROM playlists WHERE user_id=?", (user_id,)
            ).fetchall():
                conn.execute(
                    "DELETE FROM playlist_tracks WHERE playlist_id=? AND track_id=?",
                    (pl["playlist_id"], track_id)
                )
            conn.execute(
                "DELETE FROM library WHERE user_id=? AND track_id=?",
                (user_id, track_id)
            )

    def is_in_library(self, user_id: int, track_id: str) -> bool:
        with self._conn() as conn:
            return conn.execute(
                "SELECT 1 FROM library WHERE user_id=? AND track_id=?",
                (user_id, track_id)
            ).fetchone() is not None

    def get_library(self, user_id: int) -> list:
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT t.* FROM tracks t
                JOIN library l ON t.track_id = l.track_id
                WHERE l.user_id=?
                ORDER BY l.added_at DESC
            """, (user_id,)).fetchall()
        return self._rows_to_tracks(rows)

    def get_random_track(self, user_id: int) -> Optional[dict]:
        tracks = self.get_library(user_id)
        return random.choice(tracks) if tracks else None

    def clear_library(self, user_id: int):
        with self._conn() as conn:
            conn.execute("DELETE FROM library WHERE user_id=?", (user_id,))

    def get_library_total(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM library").fetchone()[0]

    # ─── Playlists ───────────────────────────

    def _gen_share_code(self) -> str:
        chars = string.ascii_uppercase + string.digits
        while True:
            code = "".join(random.choices(chars, k=6))
            with self._conn() as conn:
                if not conn.execute(
                    "SELECT 1 FROM playlists WHERE share_code=?", (code,)
                ).fetchone():
                    return code

    def playlist_count(self, user_id: int) -> int:
        with self._conn() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM playlists WHERE user_id=?", (user_id,)
            ).fetchone()[0]

    def create_playlist(self, user_id: int, name: str) -> tuple:
        self.ensure_user(user_id)
        if not name or not name.strip():
            return None, "empty_name"
        if len(name) > 64:
            return None, "long_name"
        if self.playlist_count(user_id) >= PLAYLIST_LIMIT:
            return None, "limit"
        code = self._gen_share_code()
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO playlists (user_id, name, share_code) VALUES (?,?,?)",
                (user_id, name.strip(), code)
            )
        return cur.lastrowid, "ok"

    def get_playlists(self, user_id: int) -> list:
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT p.playlist_id, p.name, p.share_code, p.created_at,
                       COUNT(pt.track_id) as track_count
                FROM playlists p
                LEFT JOIN playlist_tracks pt ON p.playlist_id = pt.playlist_id
                WHERE p.user_id=?
                GROUP BY p.playlist_id
                ORDER BY p.created_at DESC
            """, (user_id,)).fetchall()
        return [dict(r) for r in rows]

    def get_playlist(self, playlist_id: int, user_id: int) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM playlists WHERE playlist_id=? AND user_id=?",
                (playlist_id, user_id)
            ).fetchone()
        return dict(row) if row else None

    def get_playlist_by_code(self, code: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM playlists WHERE share_code=?", (code.upper(),)
            ).fetchone()
        return dict(row) if row else None

    def get_playlist_tracks(self, playlist_id: int) -> list:
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT t.* FROM tracks t
                JOIN playlist_tracks pt ON t.track_id = pt.track_id
                WHERE pt.playlist_id=?
                ORDER BY pt.position, pt.added_at
            """, (playlist_id,)).fetchall()
        return self._rows_to_tracks(rows)

    def playlist_track_count(self, playlist_id: int) -> int:
        with self._conn() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM playlist_tracks WHERE playlist_id=?", (playlist_id,)
            ).fetchone()[0]

    def add_tracks_to_playlist_bulk(self, playlist_id: int, track_ids: list) -> tuple:
        current = self.playlist_track_count(playlist_id)
        added = 0
        with self._conn() as conn:
            for track_id in track_ids:
                if current + added >= PLAYLIST_TRACKS_LIMIT:
                    return added, "limit"
                if conn.execute(
                    "SELECT 1 FROM playlist_tracks WHERE playlist_id=? AND track_id=?",
                    (playlist_id, track_id)
                ).fetchone():
                    continue
                pos = conn.execute(
                    "SELECT COALESCE(MAX(position),0)+1 FROM playlist_tracks WHERE playlist_id=?",
                    (playlist_id,)
                ).fetchone()[0]
                conn.execute(
                    "INSERT INTO playlist_tracks (playlist_id, track_id, position) VALUES (?,?,?)",
                    (playlist_id, track_id, pos)
                )
                added += 1
        return added, "ok"

    def copy_playlist_to_user(self, src_playlist_id: int, dst_user_id: int) -> tuple:
        """Скопировать чужой плейлист себе. Возвращает (new_playlist_id, status)."""
        with self._conn() as conn:
            src = conn.execute(
                "SELECT * FROM playlists WHERE playlist_id=?", (src_playlist_id,)
            ).fetchone()
        if not src:
            return None, "not_found"
        new_id, status = self.create_playlist(dst_user_id, src["name"])
        if status != "ok":
            return None, status
        tracks = self.get_playlist_tracks(src_playlist_id)
        # Треки не обязательно в библиотеке получателя — просто добавляем в плейлист напрямую
        with self._conn() as conn:
            for i, t in enumerate(tracks[:PLAYLIST_TRACKS_LIMIT]):
                conn.execute(
                    "INSERT OR IGNORE INTO playlist_tracks (playlist_id, track_id, position) VALUES (?,?,?)",
                    (new_id, t["track_id"], i)
                )
        return new_id, "ok"

    def remove_track_from_playlist(self, playlist_id: int, track_id: str):
        with self._conn() as conn:
            conn.execute(
                "DELETE FROM playlist_tracks WHERE playlist_id=? AND track_id=?",
                (playlist_id, track_id)
            )

    def rename_playlist(self, playlist_id: int, user_id: int, new_name: str) -> bool:
        with self._conn() as conn:
            r = conn.execute(
                "UPDATE playlists SET name=? WHERE playlist_id=? AND user_id=?",
                (new_name, playlist_id, user_id)
            )
        return r.rowcount > 0

    def delete_playlist(self, playlist_id: int, user_id: int) -> bool:
        with self._conn() as conn:
            r = conn.execute(
                "DELETE FROM playlists WHERE playlist_id=? AND user_id=?",
                (playlist_id, user_id)
            )
        return r.rowcount > 0

    def get_playlists_total(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM playlists").fetchone()[0]

    def get_stats(self, user_id: int) -> dict:
        return {
            "library":    self.library_count(user_id),
            "playlists":  self.playlist_count(user_id),
            "lib_limit":  LIBRARY_LIMIT,
            "pl_limit":   PLAYLIST_LIMIT,
        }
