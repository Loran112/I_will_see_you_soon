import base64
import importlib.util
import sys
import os
import secrets
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

HOST = os.getenv("CHAT_BIND_HOST", "26.9.179.68")
PUBLIC_HOST = os.getenv("CHAT_PUBLIC_HOST", os.getenv("CHAT_SERVER_HOST", "26.9.179.68"))
PORT = int(os.getenv("CHAT_SERVER_PORT", "5050"))
ASSETS_DIR = Path(os.getenv("CHAT_ASSETS_DIR", str(Path(__file__).parent))).resolve()
UPLOADS_DIR = ASSETS_DIR / "uploads"
SYSTEM_AVATAR_FILE = os.getenv("CHAT_SYSTEM_AVATAR", "system_avatar.jpg")
DB_PATH = Path(os.getenv("CHAT_DB_PATH", str(Path(__file__).with_name("server_chat.db"))))

# value: (user_id, username, nickname, token)
clients: Dict[WebSocket, Tuple[int, str, str, str]] = {}
clients_lock = threading.Lock()
db_lock = threading.Lock()


app = FastAPI(title="Message9 FastAPI Server")
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")


def now_ts() -> str:
    return datetime.now().strftime("%H:%M")


def now_dt() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER,
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            nickname TEXT NOT NULL,
            avatar_url TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            device_id TEXT,
            created_at TEXT NOT NULL,
            last_seen TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_user_id INTEGER,
            sender_username TEXT NOT NULL,
            sender_nickname TEXT NOT NULL,
            text TEXT NOT NULL,
            ts TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS private_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_user_id INTEGER NOT NULL,
            sender_username TEXT NOT NULL,
            sender_nickname TEXT NOT NULL,
            recipient_user_id INTEGER NOT NULL,
            recipient_username TEXT NOT NULL,
            text TEXT NOT NULL,
            ts TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS hidden_private_chats (
            owner_user_id INTEGER NOT NULL,
            peer_user_id INTEGER NOT NULL,
            hidden_after_id INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (owner_user_id, peer_user_id)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS groups (
            group_id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            owner_user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS group_members (
            group_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            added_at TEXT NOT NULL,
            PRIMARY KEY (group_id, user_id)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS group_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            sender_user_id INTEGER NOT NULL,
            sender_username TEXT NOT NULL,
            sender_nickname TEXT NOT NULL,
            text TEXT NOT NULL,
            ts TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS vpn_networks (
            vpn_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            virtual_ip TEXT UNIQUE,
            password TEXT NOT NULL,
            owner_user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS vpn_members (
            vpn_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL,
            PRIMARY KEY (vpn_id, user_id)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS vpn_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vpn_id INTEGER NOT NULL,
            sender_user_id INTEGER NOT NULL,
            sender_username TEXT NOT NULL,
            sender_nickname TEXT NOT NULL,
            text TEXT NOT NULL,
            ts TEXT NOT NULL
        )
        """
    )

    cols_users = {r[1] for r in con.execute("PRAGMA table_info(users)").fetchall()}
    if "user_id" not in cols_users:
        con.execute("ALTER TABLE users ADD COLUMN user_id INTEGER")

    con.execute("UPDATE users SET user_id = rowid WHERE user_id IS NULL")
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id)")

    cols_msg = {r[1] for r in con.execute("PRAGMA table_info(messages)").fetchall()}
    if "sender_user_id" not in cols_msg:
        con.execute("ALTER TABLE messages ADD COLUMN sender_user_id INTEGER")

    con.execute(
        """
        UPDATE messages
        SET sender_user_id = (
            SELECT u.user_id FROM users u WHERE u.username = messages.sender_username
        )
        WHERE sender_user_id IS NULL
        """
    )

    cols_vpn = {r[1] for r in con.execute("PRAGMA table_info(vpn_networks)").fetchall()}
    if "virtual_ip" not in cols_vpn:
        con.execute("ALTER TABLE vpn_networks ADD COLUMN virtual_ip TEXT")

    vpn_rows = con.execute("SELECT vpn_id, virtual_ip FROM vpn_networks ORDER BY vpn_id ASC").fetchall()
    for vpn_id, virtual_ip in vpn_rows:
        if not virtual_ip:
            octet = int(vpn_id) + 1
            con.execute("UPDATE vpn_networks SET virtual_ip = ? WHERE vpn_id = ?", (f"10.77.0.{octet}", int(vpn_id)))

    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_vpn_virtual_ip ON vpn_networks(virtual_ip)")

    con.commit()
    return con


def parse_cmd(raw: str):
    return raw.split("|")


def register_user(username: str, password: str, nickname: str) -> Tuple[bool, str]:
    with db_lock:
        with get_db() as con:
            row = con.execute("SELECT 1 FROM users WHERE username=?", (username,)).fetchone()
            if row:
                return False, "username_taken"
            next_id_row = con.execute("SELECT COALESCE(MAX(user_id), 0) + 1 FROM users").fetchone()
            next_user_id = int(next_id_row[0]) if next_id_row else 1
            con.execute(
                "INSERT INTO users(user_id, username, password, nickname, created_at) VALUES(?,?,?,?,?)",
                (next_user_id, username, password, nickname, now_dt()),
            )
    return True, "ok"


def get_profile(username: str) -> Optional[Tuple[int, str, str]]:
    with db_lock:
        with get_db() as con:
            row = con.execute(
                "SELECT user_id, nickname, COALESCE(avatar_url,'') FROM users WHERE username=?",
                (username,),
            ).fetchone()
    if not row:
        return None
    return int(row[0]), row[1], row[2]


def login_user(username: str, password: str, device_id: str) -> Tuple[bool, str, str, int, str, str]:
    with db_lock:
        with get_db() as con:
            row = con.execute(
                "SELECT user_id, username, password, nickname, COALESCE(avatar_url,'') FROM users WHERE username=?",
                (username,),
            ).fetchone()
            if not row or row[2] != password:
                return False, "invalid_credentials", "", 0, "", ""
            token = secrets.token_hex(24)
            now = now_dt()
            con.execute(
                "INSERT INTO sessions(token, username, device_id, created_at, last_seen) VALUES(?,?,?,?,?)",
                (token, username, device_id, now, now),
            )
            return True, "", token, int(row[0]), row[3], row[4]


def touch_session(token: str) -> None:
    with db_lock:
        with get_db() as con:
            con.execute("UPDATE sessions SET last_seen=? WHERE token=?", (now_dt(), token))


def resolve_token(token: str) -> Optional[Tuple[int, str, str, str]]:
    with db_lock:
        with get_db() as con:
            row = con.execute(
                """
                SELECT u.user_id, s.username, u.nickname, COALESCE(u.avatar_url,'')
                FROM sessions s
                JOIN users u ON u.username=s.username
                WHERE s.token=?
                """,
                (token,),
            ).fetchone()
    if not row:
        return None
    return int(row[0]), row[1], row[2], row[3]


def set_avatar(username: str, avatar_url: str) -> None:
    with db_lock:
        with get_db() as con:
            con.execute("UPDATE users SET avatar_url=? WHERE username=?", (avatar_url, username))


def update_nickname(username: str, nickname: str) -> None:
    with db_lock:
        with get_db() as con:
            con.execute("UPDATE users SET nickname=? WHERE username=?", (nickname, username))


def update_username(old_username: str, new_username: str) -> Tuple[bool, str]:
    with db_lock:
        with get_db() as con:
            exists = con.execute("SELECT 1 FROM users WHERE username=?", (new_username,)).fetchone()
            if exists:
                return False, "username_taken"
            con.execute("UPDATE users SET username=? WHERE username=?", (new_username, old_username))
            con.execute("UPDATE sessions SET username=? WHERE username=?", (new_username, old_username))
            con.execute("UPDATE messages SET sender_username=? WHERE sender_username=?", (new_username, old_username))
    return True, "ok"


def save_message(sender_user_id: int, sender_username: str, sender_nickname: str, text: str) -> Tuple[int, str]:
    ts = now_ts()
    with db_lock:
        with get_db() as con:
            cur = con.execute(
                "INSERT INTO messages(sender_user_id, sender_username, sender_nickname, text, ts) VALUES(?,?,?,?,?)",
                (sender_user_id, sender_username, sender_nickname, text, ts),
            )
            msg_id = int(cur.lastrowid)
    return msg_id, ts


def save_private_message(
    sender_user_id: int,
    sender_username: str,
    sender_nickname: str,
    recipient_user_id: int,
    recipient_username: str,
    text: str,
) -> Tuple[int, str]:
    ts = now_ts()
    with db_lock:
        with get_db() as con:
            cur = con.execute(
                """
                INSERT INTO private_messages(
                    sender_user_id, sender_username, sender_nickname,
                    recipient_user_id, recipient_username, text, ts
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (sender_user_id, sender_username, sender_nickname, recipient_user_id, recipient_username, text, ts),
            )
            msg_id = int(cur.lastrowid)
    return msg_id, ts


def get_recent_messages(limit: int = 120):
    with db_lock:
        with get_db() as con:
            rows = con.execute(
                "SELECT id, sender_user_id, sender_username, sender_nickname, text, ts FROM messages ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
    rows.reverse()
    return rows


def get_recent_private_messages(user_id: int, limit: int = 200):
    with db_lock:
        with get_db() as con:
            rows = con.execute(
                """
                SELECT id, sender_user_id, sender_username, sender_nickname,
                       recipient_user_id, recipient_username, text, ts
                FROM private_messages
                WHERE sender_user_id = ? OR recipient_user_id = ?
                ORDER BY id DESC LIMIT ?
                """,
                (user_id, user_id, limit),
            ).fetchall()
            hidden_rows = con.execute(
                "SELECT peer_user_id, hidden_after_id FROM hidden_private_chats WHERE owner_user_id = ?",
                (user_id,),
            ).fetchall()
    hidden_map = {int(peer_id): int(hidden_after_id) for peer_id, hidden_after_id in hidden_rows}
    rows.reverse()
    filtered = []
    for row in rows:
        msg_id, sender_user_id, _su, _sn, recipient_user_id, _ru, _text, _ts = row
        peer_user_id = recipient_user_id if int(sender_user_id) == int(user_id) else sender_user_id
        hidden_after_id = hidden_map.get(int(peer_user_id), 0)
        if int(msg_id) > int(hidden_after_id):
            filtered.append(row)
    return filtered


def hide_private_chat(owner_user_id: int, peer_user_id: int) -> bool:
    with db_lock:
        with get_db() as con:
            peer_exists = con.execute("SELECT 1 FROM users WHERE user_id = ?", (peer_user_id,)).fetchone()
            if not peer_exists:
                return False
            row = con.execute(
                """
                SELECT COALESCE(MAX(id), 0)
                FROM private_messages
                WHERE (sender_user_id = ? AND recipient_user_id = ?)
                   OR (sender_user_id = ? AND recipient_user_id = ?)
                """,
                (owner_user_id, peer_user_id, peer_user_id, owner_user_id),
            ).fetchone()
            hidden_after_id = int(row[0]) if row else 0
            con.execute(
                """
                INSERT INTO hidden_private_chats(owner_user_id, peer_user_id, hidden_after_id)
                VALUES(?, ?, ?)
                ON CONFLICT(owner_user_id, peer_user_id)
                DO UPDATE SET hidden_after_id = excluded.hidden_after_id
                """,
                (owner_user_id, peer_user_id, hidden_after_id),
            )
    return True


def get_user_by_id(user_id: int) -> Optional[Tuple[int, str, str, str]]:
    with db_lock:
        with get_db() as con:
            row = con.execute(
                "SELECT user_id, username, nickname, COALESCE(avatar_url,'') FROM users WHERE user_id=?",
                (user_id,),
            ).fetchone()
    if not row:
        return None
    return int(row[0]), row[1], row[2], row[3]


def get_all_profiles() -> Dict[str, Tuple[int, str, str]]:
    with db_lock:
        with get_db() as con:
            rows = con.execute("SELECT username, user_id, nickname, COALESCE(avatar_url,'') FROM users").fetchall()
    return {u: (int(uid), n, a) for u, uid, n, a in rows}


def get_group_member_ids(group_id: int):
    with db_lock:
        with get_db() as con:
            rows = con.execute(
                "SELECT user_id FROM group_members WHERE group_id = ? ORDER BY user_id ASC",
                (group_id,),
            ).fetchall()
    return [int(row[0]) for row in rows]


def get_groups_for_user(user_id: int):
    with db_lock:
        with get_db() as con:
            rows = con.execute(
                """
                SELECT g.group_id, g.title
                FROM groups g
                JOIN group_members gm ON gm.group_id = g.group_id
                WHERE gm.user_id = ?
                ORDER BY g.group_id ASC
                """,
                (user_id,),
            ).fetchall()
    return [(int(group_id), title) for group_id, title in rows]


def get_group_messages_for_user(user_id: int, limit: int = 300):
    with db_lock:
        with get_db() as con:
            rows = con.execute(
                """
                SELECT gm.group_id, gm.id, gm.sender_user_id, gm.sender_username, gm.sender_nickname, gm.text, gm.ts
                FROM group_messages gm
                JOIN group_members members ON members.group_id = gm.group_id
                WHERE members.user_id = ?
                ORDER BY gm.id DESC LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
    rows.reverse()
    return rows


def create_group_chat(owner_user_id: int, title: str, member_ids):
    unique_ids = sorted({int(owner_user_id), *[int(user_id) for user_id in member_ids]})
    with db_lock:
        with get_db() as con:
            found_rows = con.execute(
                f"SELECT user_id FROM users WHERE user_id IN ({','.join('?' for _ in unique_ids)})",
                tuple(unique_ids),
            ).fetchall()
            found_ids = {int(row[0]) for row in found_rows}
            if found_ids != set(unique_ids):
                return None
            cur = con.execute(
                "INSERT INTO groups(title, owner_user_id, created_at) VALUES(?,?,?)",
                (title, owner_user_id, now_dt()),
            )
            group_id = int(cur.lastrowid)
            for member_id in unique_ids:
                con.execute(
                    "INSERT OR IGNORE INTO group_members(group_id, user_id, added_at) VALUES(?,?,?)",
                    (group_id, member_id, now_dt()),
                )
    return group_id


def add_group_members(group_id: int, member_ids):
    member_ids = sorted({int(user_id) for user_id in member_ids})
    with db_lock:
        with get_db() as con:
            group_exists = con.execute("SELECT title FROM groups WHERE group_id = ?", (group_id,)).fetchone()
            if not group_exists:
                return None
            if member_ids:
                found_rows = con.execute(
                    f"SELECT user_id FROM users WHERE user_id IN ({','.join('?' for _ in member_ids)})",
                    tuple(member_ids),
                ).fetchall()
                found_ids = {int(row[0]) for row in found_rows}
                if found_ids != set(member_ids):
                    return None
            for member_id in member_ids:
                con.execute(
                    "INSERT OR IGNORE INTO group_members(group_id, user_id, added_at) VALUES(?,?,?)",
                    (group_id, member_id, now_dt()),
                )
    return True


def save_group_message(group_id: int, sender_user_id: int, sender_username: str, sender_nickname: str, text: str):
    ts = now_ts()
    with db_lock:
        with get_db() as con:
            cur = con.execute(
                "INSERT INTO group_messages(group_id, sender_user_id, sender_username, sender_nickname, text, ts) VALUES(?,?,?,?,?,?)",
                (group_id, sender_user_id, sender_username, sender_nickname, text, ts),
            )
            msg_id = int(cur.lastrowid)
    return msg_id, ts


def save_group_system_message(group_id: int, text: str):
    return save_group_message(group_id, 0, "system", "System", text)


def get_vpn_member_ids(vpn_id: int):
    with db_lock:
        with get_db() as con:
            rows = con.execute(
                "SELECT user_id FROM vpn_members WHERE vpn_id = ? ORDER BY user_id ASC",
                (vpn_id,),
            ).fetchall()
    return [int(row[0]) for row in rows]


def get_vpn_by_id(vpn_id: int):
    with db_lock:
        with get_db() as con:
            row = con.execute(
                "SELECT vpn_id, name, virtual_ip FROM vpn_networks WHERE vpn_id = ?",
                (vpn_id,),
            ).fetchone()
    if not row:
        return None
    return int(row[0]), row[1], row[2]


def build_vpn_route(virtual_ip: str) -> str:
    return f"http://{PUBLIC_HOST}:{PORT}/vpn/{virtual_ip}"


def create_vpn_network(owner_user_id: int, name: str, password: str):
    network_name = name.strip()
    if not network_name or not password:
        return None, None, "bad_input"
    with db_lock:
        with get_db() as con:
            exists = con.execute("SELECT vpn_id FROM vpn_networks WHERE LOWER(name) = LOWER(?)", (network_name,)).fetchone()
            if exists:
                return None, None, "name_taken"
            max_row = con.execute("SELECT COALESCE(MAX(vpn_id), 0) FROM vpn_networks").fetchone()
            next_num = int(max_row[0]) + 1 if max_row else 1
            virtual_ip = f"10.77.0.{next_num + 1}"
            cur = con.execute(
                "INSERT INTO vpn_networks(name, virtual_ip, password, owner_user_id, created_at) VALUES(?,?,?,?,?)",
                (network_name, virtual_ip, password, owner_user_id, now_dt()),
            )
            vpn_id = int(cur.lastrowid)
            con.execute(
                "INSERT OR IGNORE INTO vpn_members(vpn_id, user_id, joined_at) VALUES(?,?,?)",
                (vpn_id, owner_user_id, now_dt()),
            )
    return vpn_id, virtual_ip, "ok"


def join_vpn_network(user_id: int, virtual_ip: str, password: str):
    ip = virtual_ip.strip()
    if not ip:
        return None, None, "bad_ip"
    with db_lock:
        with get_db() as con:
            row = con.execute(
                "SELECT vpn_id, name, password FROM vpn_networks WHERE virtual_ip = ?",
                (ip,),
            ).fetchone()
            if not row:
                return None, None, "not_found"
            vpn_id, vpn_name, real_password = int(row[0]), row[1], row[2]
            if real_password != password:
                return None, None, "bad_password"
            con.execute(
                "INSERT OR IGNORE INTO vpn_members(vpn_id, user_id, joined_at) VALUES(?,?,?)",
                (vpn_id, user_id, now_dt()),
            )
    return vpn_id, vpn_name, "ok"


async def ws_send(ws: WebSocket, text: str) -> bool:
    try:
        await ws.send_text(text)
        return True
    except Exception:
        return False


async def ws_broadcast(text: str, skip: Optional[WebSocket] = None) -> None:
    with clients_lock:
        targets = list(clients.keys())
    dead = []
    for ws in targets:
        if ws is skip:
            continue
        ok = await ws_send(ws, text)
        if not ok:
            dead.append(ws)
    if dead:
        with clients_lock:
            for ws in dead:
                clients.pop(ws, None)


@app.post("/auth")
async def auth(payload: dict):
    mode = (payload.get("mode") or "").upper()
    username = (payload.get("username") or "").strip()
    password = (payload.get("password") or "").strip()
    if not username or not password:
        return JSONResponse({"status": "error", "reason": "bad_credentials"}, status_code=400)

    if mode == "REGISTER":
        nickname = (payload.get("nickname") or username).strip() or username
        device_id = (payload.get("device_id") or "device").strip()
        ok, err = register_user(username, password, nickname)
        if not ok:
            if err == "username_taken":
                prof = get_profile(username)
                if prof:
                    uid, nick, avatar = prof
                    return {"status": "exists", "user_id": uid, "username": username, "nickname": nick, "avatar": avatar}
            return JSONResponse({"status": "error", "reason": err}, status_code=400)
        ok, err, token, uid, nick, avatar = login_user(username, password, device_id)
        if not ok:
            return JSONResponse({"status": "error", "reason": err}, status_code=400)
        return {"status": "ok", "token": token, "user_id": uid, "username": username, "nickname": nick, "avatar": avatar}

    if mode == "LOGIN":
        device_id = (payload.get("device_id") or "device").strip()
        ok, err, token, uid, nick, avatar = login_user(username, password, device_id)
        if not ok:
            return JSONResponse({"status": "error", "reason": err}, status_code=400)
        return {"status": "ok", "token": token, "user_id": uid, "username": username, "nickname": nick, "avatar": avatar}

    return JSONResponse({"status": "error", "reason": "bad_mode"}, status_code=400)


@app.websocket("/ws")
async def ws_chat(websocket: WebSocket, token: str, device_id: str = "device"):
    await websocket.accept()
    resolved = resolve_token(token)
    if not resolved:
        await ws_send(websocket, "@connect|error|bad_token")
        await websocket.close(code=1008)
        return

    user_id, username, nickname, _avatar = resolved
    touch_session(token)
    with clients_lock:
        clients[websocket] = (user_id, username, nickname, token)

    await ws_send(websocket, f"@me|{user_id}|{username}|{nickname}")

    profiles = get_all_profiles()
    for u, (uid, n, a) in profiles.items():
        await ws_send(websocket, f"@profile|{uid}|{u}|{n}")
        if a:
            await ws_send(websocket, f"@avatar|{uid}|{u}|{a}")

    for mid, suid, su, sn, text, ts in get_recent_messages():
        text_b64 = base64.b64encode(text.encode("utf-8")).decode("ascii")
        await ws_send(websocket, f"@history|{mid}|{suid}|{su}|{sn}|{ts}|{text_b64}")

    for mid, suid, su, sn, ruid, ru, text, ts in get_recent_private_messages(user_id):
        text_b64 = base64.b64encode(text.encode("utf-8")).decode("ascii")
        await ws_send(websocket, f"@dmhistory|{mid}|{suid}|{su}|{sn}|{ruid}|{ru}|{ts}|{text_b64}")

    for group_id, group_title in get_groups_for_user(user_id):
        title_b64 = base64.b64encode(group_title.encode("utf-8")).decode("ascii")
        members_csv = ",".join(str(member_id) for member_id in get_group_member_ids(group_id))
        await ws_send(websocket, f"@group|{group_id}|{title_b64}|{members_csv}")

    for group_id, mid, suid, su, sn, text, ts in get_group_messages_for_user(user_id):
        text_b64 = base64.b64encode(text.encode("utf-8")).decode("ascii")
        await ws_send(websocket, f"@grouphistory|{group_id}|{mid}|{suid}|{su}|{sn}|{ts}|{text_b64}")

    await ws_broadcast(f"@profile|{user_id}|{username}|{nickname}", skip=websocket)

    try:
        while True:
            msg = await websocket.receive_text()
            msg = msg.strip()
            if not msg:
                continue
            parts = parse_cmd(msg)
            cmd = parts[0].upper()

            if cmd == "SEND" and len(parts) >= 3:
                client_id = parts[1]
                try:
                    text = base64.b64decode(parts[2].encode("ascii")).decode("utf-8")
                except Exception:
                    continue
                msg_id, ts = save_message(user_id, username, nickname, text)
                await ws_send(websocket, f"@ack|{client_id}|{msg_id}|{ts}")

                text_b64 = base64.b64encode(text.encode("utf-8")).decode("ascii")
                payload = f"@msg|{msg_id}|{user_id}|{username}|{nickname}|{ts}|{text_b64}"
                await ws_broadcast(payload)

                with clients_lock:
                    online_usernames = {v[1] for ws, v in clients.items() if ws is not websocket}
                if online_usernames:
                    await ws_send(websocket, f"@status|{msg_id}|delivered")
                continue

            if cmd == "DELETEDM" and len(parts) >= 2:
                try:
                    target_user_id = int(parts[1])
                except ValueError:
                    await ws_send(websocket, "@error|dmdelete|bad_target")
                    continue
                if not hide_private_chat(user_id, target_user_id):
                    await ws_send(websocket, "@error|dmdelete|not_found")
                    continue
                await ws_send(websocket, f"@deletedm|{target_user_id}")
                continue

            if cmd == "SENDDM" and len(parts) >= 4:
                try:
                    target_user_id = int(parts[1])
                except ValueError:
                    await ws_send(websocket, "@error|dm|bad_target")
                    continue
                client_id = parts[2]
                try:
                    text = base64.b64decode(parts[3].encode("ascii")).decode("utf-8")
                except Exception:
                    continue
                target = get_user_by_id(target_user_id)
                if not target:
                    await ws_send(websocket, "@error|dm|not_found")
                    continue
                _target_uid, target_username, _target_nickname, _target_avatar = target
                msg_id, ts = save_private_message(user_id, username, nickname, target_user_id, target_username, text)
                await ws_send(websocket, f"@ackdm|{client_id}|{msg_id}|{ts}|{target_user_id}")
                text_b64 = base64.b64encode(text.encode("utf-8")).decode("ascii")
                payload = f"@dm|{msg_id}|{user_id}|{username}|{nickname}|{target_user_id}|{target_username}|{ts}|{text_b64}"
                await ws_send(websocket, payload)
                with clients_lock:
                    targets = [(ws, data) for ws, data in clients.items() if data[0] == target_user_id]
                for target_ws, _data in targets:
                    await ws_send(target_ws, payload)
                continue

            if cmd == "CREATEGROUP" and len(parts) >= 4:
                client_id = parts[1]
                member_csv = parts[2].strip()
                try:
                    title = base64.b64decode(parts[3].encode("ascii")).decode("utf-8").strip()
                except Exception:
                    await ws_send(websocket, "@error|group|bad_title")
                    continue
                if not title:
                    title = f"Group {user_id}"
                try:
                    member_ids = [int(item) for item in member_csv.split(",") if item.strip()]
                except ValueError:
                    await ws_send(websocket, "@error|group|bad_member")
                    continue
                group_id = create_group_chat(user_id, title, member_ids)
                if not group_id:
                    await ws_send(websocket, "@error|group|not_found")
                    continue
                for member_id in member_ids:
                    member_profile = get_user_by_id(member_id)
                    if member_profile:
                        _mid, _muser, member_nickname, _mavatar = member_profile
                        save_group_system_message(group_id, f"{member_nickname} joined the group")
                members = get_group_member_ids(group_id)
                title_b64 = base64.b64encode(title.encode("utf-8")).decode("ascii")
                members_csv = ",".join(str(member_id) for member_id in members)
                payload = f"@group|{group_id}|{title_b64}|{members_csv}"
                await ws_send(websocket, f"@ackgroupcreate|{client_id}|{group_id}")
                with clients_lock:
                    targets = [(ws, data) for ws, data in clients.items() if data[0] in members]
                sent = set()
                for target_ws, data in targets:
                    if target_ws in sent:
                        continue
                    sent.add(target_ws)
                    await ws_send(target_ws, payload)
                continue

            if cmd == "ADDGROUP" and len(parts) >= 3:
                try:
                    group_id = int(parts[1])
                    member_ids = [int(item) for item in parts[2].split(",") if item.strip()]
                except ValueError:
                    await ws_send(websocket, "@error|group|bad_member")
                    continue
                if not add_group_members(group_id, member_ids):
                    await ws_send(websocket, "@error|group|not_found")
                    continue
                for member_id in member_ids:
                    member_profile = get_user_by_id(member_id)
                    if member_profile:
                        _mid, _muser, member_nickname, _mavatar = member_profile
                        save_group_system_message(group_id, f"{member_nickname} joined the group")
                group_rows = get_groups_for_user(user_id)
                title = next((group_title for gid, group_title in group_rows if gid == group_id), None)
                if not title:
                    with db_lock:
                        with get_db() as con:
                            row = con.execute("SELECT title FROM groups WHERE group_id = ?", (group_id,)).fetchone()
                    title = row[0] if row else f"Group {group_id}"
                members = get_group_member_ids(group_id)
                title_b64 = base64.b64encode(title.encode("utf-8")).decode("ascii")
                members_csv = ",".join(str(member_id) for member_id in members)
                payload = f"@group|{group_id}|{title_b64}|{members_csv}"
                with clients_lock:
                    targets = [(ws, data) for ws, data in clients.items() if data[0] in members]
                sent = set()
                for target_ws, data in targets:
                    if target_ws in sent:
                        continue
                    sent.add(target_ws)
                    await ws_send(target_ws, payload)
                    target_user_id = data[0]
                    if target_user_id in member_ids:
                        for g_group_id, mid, suid, su, sn, history_text, history_ts in get_group_messages_for_user(target_user_id):
                            if g_group_id != group_id:
                                continue
                            hist_b64 = base64.b64encode(history_text.encode("utf-8")).decode("ascii")
                            await ws_send(target_ws, f"@grouphistory|{group_id}|{mid}|{suid}|{su}|{sn}|{history_ts}|{hist_b64}")
                continue

            if cmd == "SENDGROUP" and len(parts) >= 4:
                try:
                    group_id = int(parts[1])
                except ValueError:
                    await ws_send(websocket, "@error|group|bad_group")
                    continue
                client_id = parts[2]
                try:
                    text_value = base64.b64decode(parts[3].encode("ascii")).decode("utf-8")
                except Exception:
                    continue
                member_ids = get_group_member_ids(group_id)
                if user_id not in member_ids:
                    await ws_send(websocket, "@error|group|forbidden")
                    continue
                msg_id, ts = save_group_message(group_id, user_id, username, nickname, text_value)
                await ws_send(websocket, f"@ackgroup|{client_id}|{group_id}|{msg_id}|{ts}")
                text_b64 = base64.b64encode(text_value.encode("utf-8")).decode("ascii")
                payload = f"@groupmsg|{group_id}|{msg_id}|{user_id}|{username}|{nickname}|{ts}|{text_b64}"
                with clients_lock:
                    targets = [(ws, data) for ws, data in clients.items() if data[0] in member_ids]
                for target_ws, _data in targets:
                    await ws_send(target_ws, payload)
                continue

            if cmd == "CREATEVPN" and len(parts) >= 4:
                client_id = parts[1]
                try:
                    vpn_name = base64.b64decode(parts[2].encode("ascii")).decode("utf-8").strip()
                    vpn_password = base64.b64decode(parts[3].encode("ascii")).decode("utf-8")
                except Exception:
                    await ws_send(websocket, "@error|vpn|bad_input")
                    continue
                vpn_id, virtual_ip, status = create_vpn_network(user_id, vpn_name, vpn_password)
                if not vpn_id:
                    await ws_send(websocket, f"@error|vpn|{status}")
                    continue
                route = build_vpn_route(virtual_ip)
                name_b64 = base64.b64encode(vpn_name.encode("utf-8")).decode("ascii")
                route_b64 = base64.b64encode(route.encode("utf-8")).decode("ascii")
                await ws_send(websocket, f"@vpncreated|{client_id}|{vpn_id}|{name_b64}|{virtual_ip}|{route_b64}")
                continue

            if cmd == "JOINVPN" and len(parts) >= 4:
                client_id = parts[1]
                vpn_ip = parts[2].strip()
                try:
                    vpn_password = base64.b64decode(parts[3].encode("ascii")).decode("utf-8")
                except Exception:
                    await ws_send(websocket, "@error|vpn|bad_input")
                    continue
                vpn_id, vpn_name, status = join_vpn_network(user_id, vpn_ip, vpn_password)
                if not vpn_id:
                    await ws_send(websocket, f"@error|vpn|{status}")
                    continue
                route = build_vpn_route(vpn_ip)
                name_b64 = base64.b64encode(vpn_name.encode("utf-8")).decode("ascii")
                route_b64 = base64.b64encode(route.encode("utf-8")).decode("ascii")
                await ws_send(websocket, f"@vpnjoined|{client_id}|{vpn_id}|{name_b64}|{vpn_ip}|{route_b64}")
                continue

            if cmd == "READ" and len(parts) >= 2:
                msg_id = parts[1]
                with db_lock:
                    with get_db() as con:
                        row = con.execute("SELECT sender_username FROM messages WHERE id=?", (msg_id,)).fetchone()
                if row and row[0] != username:
                    sender_username = row[0]
                    with clients_lock:
                        targets = [(ws, u) for ws, (_uid, u, _n, _t) in clients.items()]
                    for ws, u in targets:
                        if u == sender_username:
                            await ws_send(ws, f"@status|{msg_id}|read")
                continue

            if cmd == "PROFILE" and len(parts) >= 2:
                new_nick = parts[1].strip()
                if new_nick:
                    nickname = new_nick
                    update_nickname(username, nickname)
                    with clients_lock:
                        if websocket in clients:
                            _uid, _u, _n, _t = clients[websocket]
                            clients[websocket] = (_uid, _u, nickname, token)
                    await ws_broadcast(f"@profile|{user_id}|{username}|{nickname}")
                continue

            if cmd == "USERNAME" and len(parts) >= 2:
                new_username = parts[1].strip()
                if new_username and new_username != username:
                    ok, err = update_username(username, new_username)
                    if not ok:
                        await ws_send(websocket, f"@error|username|{err}")
                    else:
                        username = new_username
                        with clients_lock:
                            if websocket in clients:
                                _uid, _u, _n, _t = clients[websocket]
                                clients[websocket] = (user_id, username, nickname, token)
                        await ws_send(websocket, f"@profile|{user_id}|{username}|{nickname}")
                        with db_lock:
                            with get_db() as con:
                                arow = con.execute(
                                    "SELECT COALESCE(avatar_url,'') FROM users WHERE username=?",
                                    (username,),
                                ).fetchone()
                        avatar_url = arow[0] if arow else ""
                        if avatar_url:
                            await ws_broadcast(f"@avatar|{user_id}|{username}|{avatar_url}")
                        await ws_broadcast(f"@profile|{user_id}|{username}|{nickname}")
                continue

            if cmd == "AVATAR" and len(parts) >= 2:
                avatar_url = parts[1].strip()
                if avatar_url:
                    set_avatar(username, avatar_url)
                    await ws_broadcast(f"@avatar|{user_id}|{username}|{avatar_url}")
                continue

            if cmd == "UPLOAD" and len(parts) >= 3:
                filename = Path(parts[1]).name
                data_b64 = parts[2]
                try:
                    blob = base64.b64decode(data_b64.encode("ascii"))
                except Exception:
                    await ws_send(websocket, "@uploaded|error")
                    continue
                if len(blob) > 8 * 1024 * 1024:
                    await ws_send(websocket, "@uploaded|too_large")
                    continue
                UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
                safe_name = f"{username}_{int(datetime.now().timestamp())}_{filename}"
                path = UPLOADS_DIR / safe_name
                with open(path, "wb") as f:
                    f.write(blob)
                url = f"http://{PUBLIC_HOST}:{PORT}/assets/uploads/{safe_name}"
                await ws_send(websocket, f"@uploaded|{url}")
                continue

            # legacy fallback
            text = msg
            msg_id, ts = save_message(user_id, username, nickname, text)
            text_b64 = base64.b64encode(text.encode("utf-8")).decode("ascii")
            payload = f"@msg|{msg_id}|{user_id}|{username}|{nickname}|{ts}|{text_b64}"
            await ws_broadcast(payload)

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        with clients_lock:
            clients.pop(websocket, None)


@app.get("/")
async def health():
    return {
        "ok": True,
        "host": PUBLIC_HOST,
        "port": PORT,
        "assets": f"http://{PUBLIC_HOST}:{PORT}/assets/{SYSTEM_AVATAR_FILE}",
    }

def _has_websocket_backend() -> bool:
    return importlib.util.find_spec("websockets") is not None or importlib.util.find_spec("wsproto") is not None

def main() -> None:
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    if not _has_websocket_backend():
        print("ERROR: WebSocket backend is missing. Install one of: pip install websockets  OR  pip install wsproto  OR  pip install 'uvicorn[standard]'")
        sys.exit(1)
    if not (ASSETS_DIR / SYSTEM_AVATAR_FILE).exists():
        print(f"Warning: system avatar file not found: {ASSETS_DIR / SYSTEM_AVATAR_FILE}")
    print(f"FastAPI chat server listening on {HOST}:{PORT} (public host {PUBLIC_HOST})")
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()






