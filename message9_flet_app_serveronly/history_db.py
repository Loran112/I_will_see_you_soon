import sqlite3
from datetime import datetime


class HistoryDB:
    """Server-only local store: no JSON configs, no account folders, no embedded users."""

    def __init__(self):
        self._settings = {}
        self._profiles = ({}, {}, {})
        self._con = sqlite3.connect(":memory:", check_same_thread=False)
        self._init_db()

    def _init_db(self):
        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                chat_id TEXT,
                sender TEXT,
                text TEXT
            )
            """
        )
        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_meta (
                chat_id TEXT PRIMARY KEY,
                title TEXT
            )
            """
        )
        self._con.commit()

    def use_account(self, _username):
        return

    def set_setting(self, key, value):
        self._settings[key] = value

    def get_setting(self, key):
        return self._settings.get(key)

    def ensure_chat(self, chat_id, title):
        self._con.execute("INSERT OR IGNORE INTO chat_meta(chat_id, title) VALUES(?, ?)", (chat_id, title))
        self._con.execute("UPDATE chat_meta SET title=? WHERE chat_id=?", (title, chat_id))
        self._con.commit()

    def add_message(self, chat_id, sender, text):
        ts = datetime.now().strftime("%H:%M")
        self._con.execute(
            "INSERT INTO messages(ts, chat_id, sender, text) VALUES (?, ?, ?, ?)",
            (ts, chat_id, sender, text),
        )
        self._con.commit()
        return ts

    def get_messages(self, chat_id):
        rows = self._con.execute(
            "SELECT ts, sender, text FROM messages WHERE chat_id=? ORDER BY id ASC",
            (chat_id,),
        ).fetchall()
        return rows

    def get_chats(self):
        return self._con.execute("SELECT chat_id, title FROM chat_meta ORDER BY chat_id ASC").fetchall()

    def delete_chat(self, chat_id):
        self._con.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
        self._con.execute("DELETE FROM chat_meta WHERE chat_id = ?", (chat_id,))
        self._con.commit()

    def load_profile_cache(self):
        return self._profiles

    def save_profile_cache(self, profiles, user_ids, avatars):
        self._profiles = (dict(profiles), dict(user_ids), dict(avatars))
