"""
SQLite база данных для музыкального бота.
Файл library.db хранится рядом с ботом — данные не зависят от истории Telegram.

Лимиты:
  LIBRARY_LIMIT        = 100  треков в библиотеке
  PLAYLIST_LIMIT       = 25   плейлистов
  PLAYLIST_TRACKS_LIMIT= 50   треков в одном плейлисте
"""

import sqlite3
import json
import random
import logging
from typing import Optional

logger = logging.getLogger(__name__)

LIBRARY_LIMIT         = 100
PLAYLIST_LIMIT        = 25
PLAYLIST_TRACKS_LIMIT = 50


class Database:
    def __init__(self, path: str = "library.db"):
        self.path = path
        self._init_db()

    # ─────────────────────────────────────────
    #  INTERNAL
    # ─────────────────────────────────────────

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
                    is_banned  INTEGER NOT NULL DEFAULT 0,
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
            """)
            # Миграция: добавить is_banned если таблица уже существовала без него
            try:
                conn.execute("ALTER TABLE users ADD COLUMN is_banned INTEGER NOT NULL DEFAULT 0")
            except Exception:
                pass
        logger.info("Database ready: %s", self.path)

    def _row_to_track(self, t: dict) -> dict:
        extra = json.loads(t.pop("extra") or "{}")
        t["_sc_id"] = t.get("sc_id") or extra.get("_sc_id")
        return t

    def _rows_to_tracks(self, rows) -> list:
        return [self._row_to_track(dict(r)) for r in rows]

    # ─────────────────────────────────────────
    #  USERS
    # ─────────────────────────────────────────

    def ensure_user(self, user_id: int, username: str = None):
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)",
                (user_id, username)
            )
            if username:
                conn.execute(
                    "UPDATE users SET username=? WHERE user_id=?",
                    (username, user_id)
                )

    def is_banned(self, user_id: int) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT is_banned FROM users WHERE user_id=?", (user_id,)
            ).fetchone()
        return bool(row["is_banned"]) if row else False

    def ban_user(self, user_id: int):
        self.ensure_user(user_id)
        with self._conn() as conn:
            conn.execute(
                "UPDATE users SET is_banned=1 WHERE user_id=?", (user_id,)
            )

    def unban_user(self, user_id: int):
        with self._conn() as conn:
            conn.execute(
                "UPDATE users SET is_banned=0 WHERE user_id=?", (user_id,)
            )

    def get_all_users(self) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT user_id, username, is_banned, created_at FROM users ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_users_count(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]

    def get_banned_count(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM users WHERE is_banned=1").fetchone()[0]

    # ─────────────────────────────────────────
    #  TRACKS (global catalog)
    # ─────────────────────────────────────────

    def upsert_track(self, track: dict):
        """Сохранить/обновить трек в каталоге. Алиас: save_track."""
        extra = {k: v for k, v in track.items()
                 if k not in ("track_id", "title", "artist", "url", "duration_sec",
                              "duration_fmt", "artwork_url", "sc_id", "_sc_id")}
        with self._conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO tracks
                    (track_id, title, artist, url, duration_sec, duration_fmt,
                     artwork_url, sc_id, extra)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                track.get("track_id", ""),
                track.get("title", "Unknown"),
                track.get("artist", "Unknown"),
                track.get("url", ""),
                track.get("duration_sec"),
                track.get("duration_fmt", ""),
                track.get("artwork_url", ""),
                track.get("_sc_id") or track.get("sc_id"),
                json.dumps(extra),
            ))

    # Алиас для обратной совместимости
    def save_track(self, track: dict):
        self.upsert_track(track)

    def get_track(self, track_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM tracks WHERE track_id=?", (track_id,)
            ).fetchone()
        if not row:
            return None
        return self._row_to_track(dict(row))

    def get_tracks_count(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]

    # ─────────────────────────────────────────
    #  LIBRARY
    # ─────────────────────────────────────────

    def library_count(self, user_id: int) -> int:
        with self._conn() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM library WHERE user_id=?", (user_id,)
            ).fetchone()[0]

    def add_to_library(self, user_id: int, track_id: str) -> tuple:
        """
        Возвращает (bool, status).
        status: 'ok' | 'limit' | 'already'
        """
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
        """Общее кол-во записей в библиотеках всех пользователей."""
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM library").fetchone()[0]

    # ─────────────────────────────────────────
    #  PLAYLISTS
    # ─────────────────────────────────────────

    def playlist_count(self, user_id: int) -> int:
        with self._conn() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM playlists WHERE user_id=?", (user_id,)
            ).fetchone()[0]

    def create_playlist(self, user_id: int, name: str) -> tuple:
        """
        Создать плейлист.
        Возвращает (playlist_id | None, status).
        status: 'ok' | 'limit' | 'empty_name' | 'long_name'
        """
        self.ensure_user(user_id)
        if not name or not name.strip():
            return None, "empty_name"
        if len(name) > 64:
            return None, "long_name"
        if self.playlist_count(user_id) >= PLAYLIST_LIMIT:
            return None, "limit"
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO playlists (user_id, name) VALUES (?,?)",
                (user_id, name.strip())
            )
        return cur.lastrowid, "ok"

    def get_playlists(self, user_id: int) -> list:
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT p.playlist_id, p.name, p.created_at,
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
                "SELECT COUNT(*) FROM playlist_tracks WHERE playlist_id=?",
                (playlist_id,)
            ).fetchone()[0]

    def add_tracks_to_playlist_bulk(self, playlist_id: int, track_ids: list) -> tuple:
        """
        Добавить несколько треков в плейлист.
        Возвращает (added_count, status).
        status: 'ok' | 'limit'
        """
        current = self.playlist_track_count(playlist_id)
        added = 0
        with self._conn() as conn:
            for track_id in track_ids:
                if current + added >= PLAYLIST_TRACKS_LIMIT:
                    return added, "limit"
                already = conn.execute(
                    "SELECT 1 FROM playlist_tracks WHERE playlist_id=? AND track_id=?",
                    (playlist_id, track_id)
                ).fetchone()
                if already:
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
        """Общее кол-во плейлистов всех пользователей."""
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM playlists").fetchone()[0]
