from __future__ import annotations

import asyncio
import base64
import queue
import secrets
import threading
from pathlib import Path
import tkinter as tk
from tkinter import filedialog
from typing import Dict, List
from urllib.parse import quote

import flet as ft
import requests
import websocket

import os
from .history_db import HistoryDB

SERVER_HOST = os.getenv("CHAT_SERVER_HOST", "26.9.179.68")
SERVER_PORT = int(os.getenv("CHAT_SERVER_PORT", "5050"))


class FletTelegramClient:
    def __init__(self, page: ft.Page):
        self.page = page
        self.db = HistoryDB()
        self.events = queue.Queue()
        self.ws = None
        self.ws_thread = None
        self.connected = False
        self.device_id = self.db.get_setting("device_id") or secrets.token_hex(8)
        self.db.set_setting("device_id", self.device_id)
        saved_host = (self.db.get_setting("server_host") or "").strip()
        if saved_host.lower() in {"", "127.0.0.1", "localhost"}:
            saved_host = SERVER_HOST
            self.db.set_setting("server_host", saved_host)
        self.host = (saved_host or SERVER_HOST).strip()
        self.port = str(self.db.get_setting("server_port") or SERVER_PORT or "5050")
        self.auth_token = ""
        self.current_user_id = 0
        self.current_username = ""
        self.current_nickname = ""
        self.current_chat = "public"
        self.profiles: Dict[str, dict] = {}
        self.usernames_by_id: Dict[int, str] = {}
        self.group_titles: Dict[int, str] = {}
        self.group_members: Dict[int, List[int]] = {}
        self.pending_echoes = set()
        self.sidebar_width = 300
        self.sidebar_compact = False
        self._app_alive = True
        self.upload_target = ""
        self.image_prefix = "[img]"
        self.max_window_width = 1320
        self.max_window_height = 820
        self._load_local_state()
        self._setup_page()
        self._build_ui()
        self._update_sidebar_layout()
        self.page.run_task(self._event_pump)
        if self.current_username and self.auth_token:
            self.show_main()
            self.load_cached_chats()
            self.connect_ws()
        else:
            self.show_login()

    def _load_local_state(self):
        last_username = (self.db.get_setting("last_username") or "").strip()
        if last_username:
            self.db.use_account(last_username)
            self.current_username = last_username
            self.current_nickname = (self.db.get_setting("last_nickname") or last_username).strip() or last_username
            self.auth_token = (self.db.get_setting("auth_token") or "").strip()
            profiles, user_ids, avatars = self.db.load_profile_cache()
            for username, profile in profiles.items():
                if isinstance(profile, dict):
                    nickname = profile.get("nickname") or username
                    avatar_url = avatars.get(username, profile.get("avatar_url", ""))
                    user_id = int(user_ids.get(username) or profile.get("user_id") or 0)
                else:
                    nickname = str(profile or username)
                    avatar_url = avatars.get(username, "")
                    user_id = int(user_ids.get(username) or 0)
                self.profiles[username] = {
                    "nickname": nickname,
                    "avatar_url": avatar_url,
                    "user_id": user_id,
                }
                if user_id:
                    self.usernames_by_id[user_id] = username

    def _setup_page(self):
        self.page.title = "Miramsg"
        self.page.theme_mode = ft.ThemeMode.DARK
        self.page.theme = ft.Theme(visual_density=ft.VisualDensity.STANDARD)
        self.page.padding = 0
        self.page.spacing = 0
        self.page.bgcolor = "#0f1722"
        self.page.window_width = 1180
        self.page.window_height = 760
        self.page.window_min_width = 980
        self.page.window_min_height = 640
        try:
            self.page.on_resized = self._on_page_resized
            self.page.on_close = self._on_page_close
            self.page.on_disconnect = self._on_page_close
        except Exception:
            pass

    def _on_page_resized(self, _e=None):
        try:
            if getattr(self.page, "width", 0) and self.page.width > self.max_window_width:
                self.page.window_width = self.max_window_width
            if getattr(self.page, "height", 0) and self.page.height > self.max_window_height:
                self.page.window_height = self.max_window_height
        except Exception:
            pass
        self._update_sidebar_layout()
        self.refresh_chats()

    def _safe_update(self):
        if not self._app_alive:
            return
        try:
            self.page.update()
        except Exception:
            pass

    def _scroll_messages_to_bottom(self):
        try:
            self.messages.scroll_to(offset=-1, duration=0)
        except Exception:
            pass

    def _shutdown(self):
        if not self._app_alive:
            return
        self._app_alive = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        self.ws = None
        self.ws_thread = None
        self.connected = False

    def _on_page_close(self, _e=None):
        self._shutdown()

    def _update_sidebar_layout(self):
        page_width = getattr(self.page, "width", 1180) or 1180
        forced_compact = page_width <= 1020
        effective_width = 92 if forced_compact else self.sidebar_width
        self.sidebar_compact = effective_width <= 120
        try:
            self.sidebar_container.width = effective_width
            self.sidebar_header_row.visible = not self.sidebar_compact
            self.search_field.visible = not self.sidebar_compact
            self.sidebar_profile_row.visible = not self.sidebar_compact
            self.sidebar_action_row.visible = not self.sidebar_compact
            self.sidebar_divider.visible = not self.sidebar_compact
        except Exception:
            pass

    def _profile_by_username(self, username):
        return self.profiles.get(username, {}) if username else {}

    def _avatar_url_for_chat(self, chat_id):
        if chat_id.startswith("dm:"):
            user_id = int(chat_id.split(":", 1)[1])
            username = self.usernames_by_id.get(user_id)
            return self._profile_by_username(username).get("avatar_url", "")
        return ""

    def _field(self, **kwargs):
        return ft.TextField(
            dense=True,
            filled=True,
            bgcolor="#1b2635",
            border=ft.InputBorder.NONE,
            border_width=0,
            content_padding=ft.padding.symmetric(horizontal=16, vertical=14),
            border_radius=10,
            text_size=15,
            **kwargs,
        )

    def _build_ui(self):
        self.host_field = self._field(label="IP", value=self.host, expand=True)
        self.port_field = self._field(label="Port", value=self.port, width=120)
        self.username_field = self._field(label="Username", value=self.current_username, expand=True)
        self.password_field = self._field(label="Password", password=True, can_reveal_password=True, expand=True)
        self.nickname_field = self._field(label="Nickname", value=self.current_nickname, expand=True)
        self.login_status = ft.Text(color="#93a4b8")
        self.search_field = self._field(hint_text="Search chats", on_change=lambda e: self.refresh_chats())
        self.chat_list = ft.Column(scroll=ft.ScrollMode.AUTO, spacing=6, expand=True)
        self.messages = ft.Column(scroll=ft.ScrollMode.AUTO, spacing=12, expand=True)
        self.chat_title = ft.Text("General Chat", size=20, weight=ft.FontWeight.BOLD)
        self.chat_subtitle = ft.Text("Offline cache", size=12, color="#8ea0b5")
        self.conn_text = ft.Text("Offline", size=12, color="#f59e0b")
        self.message_input = self._field(hint_text="Write a message...", expand=True, on_submit=self.send_message)

        self.login_view = ft.Container(
            expand=True,
            content=ft.Row(
                expand=True,
                alignment=ft.MainAxisAlignment.CENTER,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Container(
                        width=450,
                        bgcolor="#17212b",
                        border_radius=16,
                        padding=24,
                        content=ft.Column(
                            tight=True,
                            spacing=16,
                            horizontal_alignment=ft.CrossAxisAlignment.STRETCH,
                            controls=[
                                ft.Text("Login or register", size=28, weight=ft.FontWeight.BOLD),
                                ft.Text("Separate Flet copy. The Tkinter sample stays untouched.", color="#8ea0b5"),
                                ft.Container(margin=ft.margin.only(bottom=2), content=ft.Row([self.host_field, self.port_field], spacing=12)),
                                ft.Container(margin=ft.margin.only(bottom=2), content=self.username_field),
                                ft.Container(margin=ft.margin.only(bottom=2), content=self.password_field),
                                ft.Container(margin=ft.margin.only(bottom=2), content=self.nickname_field),
                                ft.Row(
                                    spacing=10,
                                    controls=[
                                        ft.ElevatedButton("Register", on_click=self.register, bgcolor="#2563eb", color="#ffffff", expand=True, height=38),
                                        ft.OutlinedButton("Login", on_click=self.login, expand=True, height=38),
                                    ],
                                ),
                                self.login_status,
                            ],
                        ),
                    )
                ],
            ),
        )

        self.sidebar_header_row = ft.Row([ft.Text("Chats", size=22, weight=ft.FontWeight.BOLD), self.conn_text], alignment=ft.MainAxisAlignment.SPACE_BETWEEN)
        self.sidebar_profile_row = ft.Row([ft.TextButton("My Profile", on_click=self.show_profile), ft.TextButton("Open DM by ID", on_click=self.open_dm)], wrap=True)
        self.sidebar_action_row = ft.Row([
            ft.TextButton("New Group", on_click=self.new_group),
            ft.TextButton("Logout", on_click=self.logout),
        ], wrap=True)
        self.sidebar_divider = ft.Divider(color="#223041", height=1)
        self.sidebar_column = ft.Column(expand=True, spacing=14, controls=[
            self.sidebar_header_row,
            self.search_field,
            self.sidebar_profile_row,
            self.sidebar_action_row,
            self.sidebar_divider,
            self.chat_list,
        ])
        self.sidebar_container = ft.Container(
            width=300,
            bgcolor="#17212b",
            padding=ft.padding.only(left=14, right=14, top=16, bottom=14),
            content=self.sidebar_column,
        )
        self.main_row = ft.Row(spacing=0, expand=True, controls=[
                self.sidebar_container,
                ft.VerticalDivider(width=1, color="#223041"),
                ft.Container(
                    expand=True,
                    content=ft.Column(expand=True, spacing=0, controls=[
                        ft.Container(
                            bgcolor="#17212b",
                            padding=ft.padding.symmetric(horizontal=14, vertical=4),
                            on_click=self.show_chat_info,
                            content=ft.Row(
                                spacing=12,
                                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                                controls=[
                                    ft.Column(
                                        spacing=1,
                                        tight=True,
                                        controls=[
                                            self.chat_title,
                                            self.chat_subtitle,
                                        ],
                                    ),
                                ],
                            ),
                        ),
                        ft.Container(expand=True, padding=16, content=self.messages),
                        ft.Container(bgcolor="#17212b", padding=ft.padding.symmetric(horizontal=14, vertical=12), content=ft.Row([
                            ft.IconButton(icon=ft.Icons.PHOTO, tooltip="Upload photo", on_click=self.attach_message_photo),
                            self.message_input,
                            ft.ElevatedButton("Send", icon=ft.Icons.INSERT_EMOTICON, on_click=self.send_message, bgcolor="#22c1dc", color="#0f1722", height=38),
                        ])),
                    ]),
                ),
            ])
        self.main_view = ft.Container(
            expand=True,
            visible=False,
            content=self.main_row,
        )
        self.page.add(self.login_view, self.main_view)

    def show_login(self):
        self.login_view.visible = True
        self.main_view.visible = False
        self._safe_update()

    def show_main(self):
        self.login_view.visible = False
        self.main_view.visible = True
        self._safe_update()

    def http_url(self):
        return f"http://{self.host}:{self.port}"

    def ws_url(self):
        return f"ws://{self.host}:{self.port}/ws?token={quote(self.auth_token)}&device_id={quote(self.device_id)}"

    def validate_auth(self):
        self.host = self.host_field.value.strip() or SERVER_HOST
        self.port = self.port_field.value.strip() or "5050"
        self.db.set_setting("server_host", self.host)
        self.db.set_setting("server_port", self.port)
        int(self.port)
        username = self.username_field.value.strip()
        password = self.password_field.value.strip()
        nickname = self.nickname_field.value.strip() or username
        if not username or not password:
            raise ValueError("Username and password are required.")
        return username, password, nickname

    def register(self, _e=None):
        try:
            username, password, nickname = self.validate_auth()
        except Exception as exc:
            self.set_login_status(str(exc), True)
            return
        self.auth_request({"mode": "REGISTER", "username": username, "password": password, "nickname": nickname, "device_id": self.device_id})

    def login(self, _e=None):
        try:
            username, password, _nickname = self.validate_auth()
        except Exception as exc:
            self.set_login_status(str(exc), True)
            return
        self.auth_request({"mode": "LOGIN", "username": username, "password": password, "device_id": self.device_id})

    def auth_request(self, payload):
        try:
            response = requests.post(f"{self.http_url()}/auth", json=payload, timeout=8)
            data = response.json()
        except requests.RequestException:
            self.set_login_status("Server is unavailable. Check IP, port, and whether server.py is running.", True)
            return
        if response.ok and data.get("status") == "ok":
            self.apply_session(data)
            self.show_main()
            self.load_cached_chats()
            self.connect_ws()
            self.set_login_status("", False)
            return
        if data.get("status") == "exists":
            self.set_login_status("This username already exists. Use Login to open that profile.", True)
            return
        self.set_login_status(data.get("reason", "Authentication failed."), True)

    def apply_session(self, data):
        self.current_username = (data.get("username") or self.username_field.value or "").strip()
        self.current_nickname = (data.get("nickname") or self.current_username).strip() or self.current_username
        self.current_user_id = int(data.get("user_id") or 0)
        self.auth_token = (data.get("token") or "").strip()
        self.db.set_setting("last_username", self.current_username)
        self.db.use_account(self.current_username)
        self.db.set_setting("last_nickname", self.current_nickname)
        self.db.set_setting("auth_token", self.auth_token)
        self.profiles[self.current_username] = {"nickname": self.current_nickname, "avatar_url": data.get("avatar", ""), "user_id": self.current_user_id}
        if self.current_user_id:
            self.usernames_by_id[self.current_user_id] = self.current_username
        self.save_profiles()

    def set_login_status(self, text, error=False):
        self.login_status.value = text
        self.login_status.color = "#f87171" if error else "#93a4b8"
        self._safe_update()

    def save_profiles(self):
        profiles = {u: {"nickname": p.get("nickname") or u, "avatar_url": p.get("avatar_url", ""), "user_id": p.get("user_id", 0)} for u, p in self.profiles.items()}
        user_ids = {u: p.get("user_id", 0) for u, p in self.profiles.items() if p.get("user_id")}
        avatars = {u: p.get("avatar_url", "") for u, p in self.profiles.items() if p.get("avatar_url")}
        self.db.save_profile_cache(profiles, user_ids, avatars)

    def load_cached_chats(self):
        self.db.ensure_chat("public", "General Chat")
        self.refresh_chats()
        self.switch_chat(self.current_chat)

    def connect_ws(self):
        if not self.auth_token or self.ws_thread:
            return
        def on_message(_ws, message): self.events.put(("message", message))
        def on_open(_ws): self.events.put(("connected", None))
        def on_close(_ws, _code, _msg): self.events.put(("disconnected", None))
        def on_error(_ws, error): self.events.put(("error", str(error)))
        self.ws = websocket.WebSocketApp(self.ws_url(), on_message=on_message, on_open=on_open, on_close=on_close, on_error=on_error)
        self.ws_thread = threading.Thread(target=lambda: self.ws.run_forever(ping_interval=25, ping_timeout=10), daemon=True)
        self.ws_thread.start()

    async def _event_pump(self):
        while self._app_alive:
            while not self.events.empty():
                event, payload = self.events.get()
                self.handle_event(event, payload)
            await asyncio.sleep(0.1)

    def handle_event(self, event, payload):
        if not self._app_alive:
            return
        if event == "connected":
            self.connected = True
            self.conn_text.value = "Online"
            self.conn_text.color = "#22c55e"
            self._safe_update()
            return
        if event in {"disconnected", "error"}:
            self.connected = False
            self.conn_text.value = "Offline"
            self.conn_text.color = "#f59e0b"
            self._safe_update()
            return
        self.handle_ws_message(payload)

    def b64(self, text):
        return base64.b64encode(text.encode("utf-8")).decode("ascii")

    def unb64(self, text):
        try: return base64.b64decode(text.encode("ascii")).decode("utf-8")
        except Exception: return ""

    def handle_ws_message(self, raw):
        if raw.startswith("@me|"):
            _, uid, username, nickname = raw.split("|", 3)
            self.current_user_id = int(uid)
            self.current_username = username
            self.current_nickname = nickname
            return
        if raw.startswith("@profile|"):
            _, uid, username, nickname = raw.split("|", 3)
            self.profiles[username] = {**self.profiles.get(username, {}), "nickname": nickname, "user_id": int(uid)}
            self.usernames_by_id[int(uid)] = username
            self.save_profiles(); self.refresh_chats(); return
        if raw.startswith("@avatar|"):
            _, uid, username, avatar = raw.split("|", 3)
            self.profiles[username] = {**self.profiles.get(username, {}), "avatar_url": avatar, "user_id": int(uid)}
            self.usernames_by_id[int(uid)] = username
            self.save_profiles(); self.refresh_chats(); return
        if raw.startswith("@uploaded|"):
            uploaded_url = raw.split("|", 1)[1]
            if uploaded_url and uploaded_url != "error" and uploaded_url != "too_large":
                if self.upload_target == "avatar":
                    self.send_ws(f"AVATAR|{uploaded_url}")
                    if self.current_username:
                        self.profiles[self.current_username] = {**self.profiles.get(self.current_username, {}), "avatar_url": uploaded_url, "user_id": self.current_user_id}
                        self.save_profiles()
                        self.refresh_chats()
                        self.snack("Avatar updated.")
                else:
                    self.send_image_message(uploaded_url)
            else:
                self.snack("Upload failed.")
            self.upload_target = ""
            return
        if raw.startswith("@history|"):
            _, _, _, _, nickname, ts, text_b64 = raw.split("|", 6)
            self.store_message("public", nickname, self.unb64(text_b64), ts, True, False)
            self.refresh_if_current("public"); return
        if raw.startswith("@msg|"):
            _, _, _, username, nickname, ts, text_b64 = raw.split("|", 6)
            text = self.unb64(text_b64)
            if username == self.current_username and ("public", nickname, text) in self.pending_echoes:
                self.pending_echoes.discard(("public", nickname, text))
                return
            self.store_message("public", nickname, text, ts, username != self.current_username, True); return
        if raw.startswith("@dmhistory|"):
            _, _, suid, suser, snick, ruid, ruser, ts, text_b64 = raw.split("|", 8)
            self.handle_dm(int(suid), suser, snick, int(ruid), ruser, self.unb64(text_b64), ts, False); return
        if raw.startswith("@dm|"):
            _, _, suid, suser, snick, ruid, ruser, ts, text_b64 = raw.split("|", 8)
            text = self.unb64(text_b64)
            peer_chat = f"dm:{int(ruid) if suser == self.current_username else int(suid)}"
            if suser == self.current_username and (peer_chat, snick, text) in self.pending_echoes:
                self.pending_echoes.discard((peer_chat, snick, text))
                return
            self.handle_dm(int(suid), suser, snick, int(ruid), ruser, text, ts, True); return
        if raw.startswith("@deletedm|"):
            chat_id = f"dm:{int(raw.split('|',1)[1])}"
            self.db.delete_chat(chat_id)
            if self.current_chat == chat_id: self.switch_chat("public")
            self.refresh_chats(); return
        if raw.startswith("@group|"):
            _, gid, title_b64, members_csv = raw.split("|", 3)
            group_id = int(gid)
            self.group_titles[group_id] = self.unb64(title_b64)
            self.group_members[group_id] = [int(x) for x in members_csv.split(",") if x.strip()]
            self.db.ensure_chat(f"group:{group_id}", self.group_titles[group_id])
            self.refresh_chats(); self.refresh_header(); return
        if raw.startswith("@grouphistory|"):
            _, gid, _, _, _, sender_nick, ts, text_b64 = raw.split("|", 7)
            self.store_message(f"group:{int(gid)}", sender_nick, self.unb64(text_b64), ts, True, False)
            self.refresh_if_current(self.current_chat); return
        if raw.startswith("@groupmsg|"):
            _, gid, _, _, suser, snick, ts, text_b64 = raw.split("|", 7)
            text = self.unb64(text_b64)
            group_chat = f"group:{int(gid)}"
            if suser == self.current_username and (group_chat, snick, text) in self.pending_echoes:
                self.pending_echoes.discard((group_chat, snick, text))
                return
            self.store_message(group_chat, snick, text, ts, suser != self.current_username, True); return
        if raw.startswith("@vpncreated|"):
            _, client_id, vpn_id, name_b64, vpn_ip, route_b64 = raw.split("|", 5)
            name = self.unb64(name_b64) or f"VPN {vpn_id}"
            route = self.unb64(route_b64)
            dialog = ft.AlertDialog(
                modal=True,
                title=ft.Text("VPN network created"),
                content=ft.Column(
                    tight=True,
                    spacing=10,
                    controls=[
                        ft.Text(f"Network: {name}"),
                        ft.Text(f"Virtual IP: {vpn_ip}"),
                        ft.Text(f"Route: {route}"),
                    ],
                ),
                actions=[ft.TextButton("Close", on_click=lambda e: self.page.pop_dialog())],
            )
            self.page.show_dialog(dialog)
            return
        if raw.startswith("@vpnjoined|"):
            _, client_id, vpn_id, name_b64, vpn_ip, route_b64 = raw.split("|", 5)
            name = self.unb64(name_b64) or f"VPN {vpn_id}"
            route = self.unb64(route_b64)
            dialog = ft.AlertDialog(
                modal=True,
                title=ft.Text("VPN connected"),
                content=ft.Column(
                    tight=True,
                    spacing=10,
                    controls=[
                        ft.Text(f"Network: {name}"),
                        ft.Text(f"Virtual IP: {vpn_ip}"),
                        ft.Text(f"Route: {route}"),
                    ],
                ),
                actions=[ft.TextButton("Close", on_click=lambda e: self.page.pop_dialog())],
            )
            self.page.show_dialog(dialog)
            return
        if raw.startswith("@error|vpn|"):
            reason = raw.split("|", 2)[2]
            messages = {
                "bad_input": "VPN: invalid name or password.",
                "name_taken": "VPN: this network name is already in use.",
                "not_found": "VPN: network not found.",
                "bad_password": "VPN: wrong password.",
                "bad_ip": "VPN: invalid virtual IP format.",
                "forbidden": "VPN: access denied.",
            }
            self.snack(messages.get(reason, f"VPN error: {reason}"))
            return
        if raw.startswith("@error|"):
            self.snack(raw.replace("@error|", "Server error: "))

    def handle_dm(self, sender_uid, sender_username, sender_nick, recipient_uid, recipient_username, text, ts, refresh):
        peer_uid = recipient_uid if sender_username == self.current_username else sender_uid
        peer_username = recipient_username if sender_username == self.current_username else sender_username
        self.profiles[peer_username] = {**self.profiles.get(peer_username, {}), "nickname": self.profiles.get(peer_username, {}).get("nickname", sender_nick if sender_username != self.current_username else peer_username), "user_id": peer_uid}
        self.usernames_by_id[peer_uid] = peer_username
        self.save_profiles()
        self.store_message(f"dm:{peer_uid}", sender_nick, text, ts, sender_username != self.current_username, refresh)

    def store_message(self, chat_id, sender, text, ts, save, refresh):
        self.db.ensure_chat(chat_id, self.chat_title_for(chat_id))
        if save:
            rows = self.db.get_messages(chat_id)
            if not rows or rows[-1] != (ts, sender, text):
                self.db.add_message(chat_id, sender, text)
        if refresh and self.current_chat == chat_id:
            self.messages.controls.append(self._message_control(ts, sender, text))
            self.refresh_chats()
            self._safe_update()
            self._scroll_messages_to_bottom()
        else:
            self.refresh_chats()

    def _avatar_url_for_sender(self, sender: str) -> str:
        if sender in {"System", "system"}:
            return ""
        if self.current_username and sender == self.current_nickname:
            return self._profile_by_username(self.current_username).get("avatar_url", "")
        for username, profile in self.profiles.items():
            if profile.get("nickname") == sender:
                return profile.get("avatar_url", "")
        if sender in self.profiles:
            return self.profiles.get(sender, {}).get("avatar_url", "")
        return ""

    def _chat_avatar(self, chat_id, size):
        return self.avatar(self._chat_avatar_label(chat_id), size, self._avatar_url_for_chat(chat_id))

    def _chat_avatar_label(self, chat_id):
        if chat_id == "public":
            return "General Chat"
        if chat_id.startswith("dm:"):
            return self.chat_title_for(chat_id)
        if chat_id.startswith("group:"):
            return self.chat_title_for(chat_id)
        return chat_id

    def chat_title_for(self, chat_id):
        if chat_id == "public": return "General Chat"
        if chat_id.startswith("dm:"):
            user_id = int(chat_id.split(":",1)[1])
            username = self.usernames_by_id.get(user_id)
            return self.profiles.get(username, {}).get("nickname", username or f"DM {user_id}")
        if chat_id.startswith("group:"):
            return self.group_titles.get(int(chat_id.split(":",1)[1]), chat_id)
        return chat_id

    def subtitle_for(self, chat_id):
        if chat_id == "public": return "Everyone connected to the server"
        if chat_id.startswith("dm:"):
            user_id = int(chat_id.split(":",1)[1])
            return f"User ID {user_id}  @{self.usernames_by_id.get(user_id, 'unknown')}"
        if chat_id.startswith("group:"):
            return f"{len(self.group_members.get(int(chat_id.split(':',1)[1]), []))} members"
        return ""

    def refresh_chats(self, update=True):
        query = (self.search_field.value or "").strip().lower() if not self.sidebar_compact else ""
        controls = []
        for chat_id, _title in self.db.get_chats():
            title = self.chat_title_for(chat_id)
            if query and query not in title.lower():
                continue
            content = self._chat_avatar(chat_id, 38) if self.sidebar_compact else ft.Row(
                spacing=10,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    self._chat_avatar(chat_id, 34),
                    ft.Column(
                        spacing=2,
                        expand=True,
                        controls=[
                            ft.Text(title, weight=ft.FontWeight.W_600, size=14),
                            ft.Text(self.subtitle_for(chat_id), size=11, color="#7f8ea3"),
                        ],
                    ),
                ],
            )
            controls.append(
                ft.GestureDetector(
                    on_tap=lambda e, cid=chat_id: self.switch_chat(cid),
                    on_secondary_tap=lambda e, cid=chat_id: self.chat_actions(cid),
                    content=ft.Container(
                        bgcolor="#2563eb" if chat_id == self.current_chat else "#1b2635",
                        border_radius=8,
                        padding=ft.padding.symmetric(horizontal=10, vertical=8),
                        content=content,
                    ),
                )
            )
        self.chat_list.controls = controls
        if update:
            self._safe_update()

    def _is_image_message(self, text: str) -> bool:
        return isinstance(text, str) and text.startswith(self.image_prefix)

    def _image_url_from_text(self, text: str) -> str:
        if not self._is_image_message(text):
            return ""
        return text[len(self.image_prefix):].strip()

    def send_image_message(self, image_url: str):
        image_url = (image_url or "").strip()
        if not image_url:
            self.snack("Upload returned empty URL.")
            return
        msg_text = f"{self.image_prefix} {image_url}"
        cid = secrets.token_hex(8)
        pending_key = (self.current_chat, self.current_nickname, msg_text)
        if self.current_chat == "public":
            ok = self.send_ws(f"SEND|{cid}|{self.b64(msg_text)}")
        elif self.current_chat.startswith("dm:"):
            ok = self.send_ws(f"SENDDM|{int(self.current_chat.split(':',1)[1])}|{cid}|{self.b64(msg_text)}")
        elif self.current_chat.startswith("group:"):
            ok = self.send_ws(f"SENDGROUP|{int(self.current_chat.split(':',1)[1])}|{cid}|{self.b64(msg_text)}")
        else:
            ok = False
        if ok:
            self.pending_echoes.add(pending_key)
            self.store_message(self.current_chat, self.current_nickname, msg_text, self._now_ts(), True, True)
            self.snack("Photo sent.")
            self._safe_update()

    def _message_control(self, ts, sender, text):
        mine = {self.current_username, self.current_nickname}
        is_me = sender in mine
        avatar_control = self.avatar(sender, 34, self._avatar_url_for_sender(sender))
        body_controls = [ft.Text(f"{sender}  {ts}", size=11, color="#9fb0c4")]
        image_url = self._image_url_from_text(text)
        if image_url:
            body_controls.append(
                ft.Image(
                    src=image_url,
                    width=320,
                    fit=ft.BoxFit.CONTAIN,
                    border_radius=8,
                )
            )
        else:
            body_controls.append(ft.Text(text, selectable=True, size=14))
        bubble = ft.Container(
            width=500,
            bgcolor="#2563eb" if is_me else "#1b2635",
            border_radius=12,
            padding=12,
            content=ft.Column(
                spacing=5,
                tight=True,
                controls=body_controls,
            ),
        )
        row_controls = [bubble, avatar_control] if is_me else [avatar_control, bubble]
        return ft.Row(
            alignment=ft.MainAxisAlignment.END if is_me else ft.MainAxisAlignment.START,
            spacing=8,
            controls=row_controls,
        )

    def switch_chat(self, chat_id, force=False):
        if self.current_chat == chat_id and not force:
            return
        self.current_chat = chat_id
        self.refresh_header()
        self.messages.controls = [self._message_control(ts, sender, text) for ts, sender, text in self.db.get_messages(chat_id)]
        self.refresh_chats(update=False)
        self._safe_update()
        self._scroll_messages_to_bottom()

    def refresh_if_current(self, chat_id):
        if self.current_chat == chat_id:
            self.switch_chat(chat_id, force=True)
        else:
            self.refresh_chats()

    def refresh_header(self):
        self.chat_title.value = self.chat_title_for(self.current_chat)
        self.chat_subtitle.value = self.subtitle_for(self.current_chat)

    def send_ws(self, payload):
        if not self.ws or not self.connected:
            self.snack("No active server connection.")
            return False
        try:
            self.ws.send(payload)
            return True
        except Exception:
            self.snack("Failed to send data to server.")
            return False

    def send_message(self, _e=None):
        text = (self.message_input.value or "").strip()
        if not text:
            return
        cid = secrets.token_hex(8)
        pending_key = (self.current_chat, self.current_nickname, text)
        if self.current_chat == "public":
            ok = self.send_ws(f"SEND|{cid}|{self.b64(text)}")
        elif self.current_chat.startswith("dm:"):
            ok = self.send_ws(f"SENDDM|{int(self.current_chat.split(':',1)[1])}|{cid}|{self.b64(text)}")
        elif self.current_chat.startswith("group:"):
            ok = self.send_ws(f"SENDGROUP|{int(self.current_chat.split(':',1)[1])}|{cid}|{self.b64(text)}")
        else:
            ok = False
        if ok:
            self.pending_echoes.add(pending_key)
            self.store_message(self.current_chat, self.current_nickname, text, self._now_ts(), True, True)
            self.message_input.value = ""
            self._safe_update()

    def open_dm(self, _e=None):
        field = self._field(label="User ID", autofocus=True)
        dialog = ft.AlertDialog(modal=True, title=ft.Text("Open direct messages"), content=field)
        def submit(_):
            try: user_id = int((field.value or "").strip())
            except ValueError: self.snack("User ID must be a number."); return
            if self.current_user_id and user_id == self.current_user_id: self.snack("You cannot open a direct message with yourself."); return
            if user_id not in self.usernames_by_id: self.snack("No user exists with that ID."); return
            self.page.pop_dialog(); self.db.ensure_chat(f"dm:{user_id}", self.chat_title_for(f"dm:{user_id}")); self.switch_chat(f"dm:{user_id}")
        dialog.actions = [ft.TextButton("Cancel", on_click=lambda e: self.page.pop_dialog()), ft.FilledButton("Open", on_click=submit)]
        self.page.show_dialog(dialog)

    def new_group(self, _e=None):
        title = self._field(label="Group title", autofocus=True)
        members = self._field(label="Member IDs (comma-separated)")
        dialog = ft.AlertDialog(modal=True, title=ft.Text("Create group"), content=ft.Column([title, members], tight=True, width=380))
        def submit(_):
            ids = []
            if (members.value or "").strip():
                try: ids = [int(x.strip()) for x in members.value.split(",") if x.strip()]
                except ValueError: self.snack("Member IDs must be numbers."); return
            ok = self.send_ws(f"CREATEGROUP|{secrets.token_hex(8)}|{','.join(str(x) for x in ids)}|{self.b64((title.value or 'New Group').strip() or 'New Group')}")
            if ok: self.page.pop_dialog()
        dialog.actions = [ft.TextButton("Cancel", on_click=lambda e: self.page.pop_dialog()), ft.FilledButton("Create", on_click=submit)]
        self.page.show_dialog(dialog)

    def chat_actions(self, chat_id):
        items = []
        if chat_id.startswith("dm:"):
            items = [ft.TextButton("View profile", on_click=lambda e: self.show_profile_for_dm(chat_id)), ft.TextButton("Create group with user", on_click=lambda e: self.group_from_dm(chat_id)), ft.TextButton("Delete chat", on_click=lambda e: self.delete_dm(chat_id))]
        elif chat_id.startswith("group:"):
            items = [ft.TextButton("View group", on_click=lambda e: self.show_chat_info(chat_id)), ft.TextButton("Add user", on_click=lambda e: self.add_group_user(chat_id))]
        if not items:
            return
        content_controls = list(items)
        if chat_id.startswith("group:"):
            content_controls = [
                ft.Row(
                    spacing=10,
                    controls=[
                        self.avatar(self.chat_title_for(chat_id), 38),
                        ft.Text(self.chat_title_for(chat_id), weight=ft.FontWeight.W_600),
                    ],
                ),
                ft.Divider(),
                *items,
            ]
        dialog = ft.AlertDialog(modal=True, title=ft.Text(self.chat_title_for(chat_id)), content=ft.Column(content_controls, tight=True))
        for item in items:
            fn = item.on_click
            item.on_click = lambda e, f=fn, d=dialog: (self.page.pop_dialog(), f(e))
        self.page.show_dialog(dialog)

    def group_from_dm(self, chat_id):
        user_id = int(chat_id.split(":",1)[1])
        title = self._field(label="Group title", value=f"{self.chat_title_for(chat_id)} Group", autofocus=True)
        dialog = ft.AlertDialog(modal=True, title=ft.Text("Create group with this user"), content=title)
        dialog.actions = [ft.TextButton("Cancel", on_click=lambda e: self.page.pop_dialog()), ft.FilledButton("Create", on_click=lambda e: (self.send_ws(f"CREATEGROUP|{secrets.token_hex(8)}|{user_id}|{self.b64((title.value or 'New Group').strip() or 'New Group') }"), self.page.pop_dialog()))]
        self.page.show_dialog(dialog)

    def add_group_user(self, chat_id):
        group_id = int(chat_id.split(":",1)[1])
        field = self._field(label="User ID", autofocus=True)
        dialog = ft.AlertDialog(modal=True, title=ft.Text("Add user to group"), content=field)
        def submit(_):
            try: user_id = int((field.value or "").strip())
            except ValueError: self.snack("User ID must be a number."); return
            self.send_ws(f"ADDGROUP|{group_id}|{user_id}")
            self.page.pop_dialog()
        dialog.actions = [ft.TextButton("Cancel", on_click=lambda e: self.page.pop_dialog()), ft.FilledButton("Add", on_click=submit)]
        self.page.show_dialog(dialog)

    def delete_dm(self, chat_id):
        self.send_ws(f"DELETEDM|{int(chat_id.split(':',1)[1])}")

    def show_profile_for_dm(self, chat_id):
        self.show_profile(user_id=int(chat_id.split(":",1)[1]))

    def show_profile(self, _e=None, user_id=None):
        user_id = user_id or self.current_user_id
        username = self.usernames_by_id.get(user_id, self.current_username or "unknown")
        profile = self._profile_by_username(username)
        nick = profile.get("nickname", username)
        avatar_url = profile.get("avatar_url", "")
        actions = [ft.TextButton("Close", on_click=lambda e: self.page.pop_dialog())]
        if user_id == self.current_user_id:
            actions.insert(0, ft.FilledButton("Change avatar", on_click=self.change_profile_avatar))
        dialog = ft.AlertDialog(
            modal=True,
            title=ft.Text(nick),
            content=ft.Column(
                tight=True,
                spacing=12,
                controls=[
                    self.avatar(nick, 92, avatar_url),
                    ft.Text(f"Username: {username}"),
                    ft.Text(f"User ID: {user_id}"),
                    ft.Text(f"Nickname: {nick}"),
                ],
            ),
            actions=actions,
        )
        self.page.show_dialog(dialog)

    def attach_message_photo(self, _e=None):
        if not self.connected:
            self.snack("No active server connection.")
            return
        picker_root = tk.Tk()
        picker_root.withdraw()
        picker_root.attributes("-topmost", True)
        file_path = filedialog.askopenfilename(
            title="Select photo",
            filetypes=[("Images", "*.png;*.jpg;*.jpeg;*.webp;*.gif"), ("All files", "*.*")],
        )
        try:
            picker_root.destroy()
        except Exception:
            pass
        if not file_path:
            return
        try:
            blob = Path(file_path).read_bytes()
        except Exception:
            self.snack("Unable to read selected image.")
            return
        self.upload_target = "message"
        payload = base64.b64encode(blob).decode("ascii")
        self.send_ws(f"UPLOAD|{Path(file_path).name}|{payload}")

    def change_profile_avatar(self, _e=None):
        if not self.connected:
            self.snack("No active server connection.")
            return
        picker_root = tk.Tk()
        picker_root.withdraw()
        picker_root.attributes("-topmost", True)
        file_path = filedialog.askopenfilename(
            title="Select avatar",
            filetypes=[("Images", "*.png;*.jpg;*.jpeg;*.webp;*.gif"), ("All files", "*.*")],
        )
        try:
            picker_root.destroy()
        except Exception:
            pass
        if not file_path:
            return
        try:
            blob = Path(file_path).read_bytes()
        except Exception:
            self.snack("Unable to read selected image.")
            return
        self.upload_target = "avatar"
        payload = base64.b64encode(blob).decode("ascii")
        self.send_ws(f"UPLOAD|{Path(file_path).name}|{payload}")

    def show_chat_info(self, chat_id=None, _e=None):
        if chat_id is not None and not isinstance(chat_id, str):
            chat_id = None
        chat_id = chat_id or self.current_chat
        if chat_id.startswith("dm:"):
            self.show_profile(user_id=int(chat_id.split(":",1)[1]))
            return
        if chat_id.startswith("group:"):
            gid = int(chat_id.split(":",1)[1])
            members = self.group_members.get(gid, [])
            lines = []
            for user_id in members:
                username = self.usernames_by_id.get(user_id, "unknown")
                nickname = self.profiles.get(username, {}).get("nickname", username)
                lines.append(ft.Text(f"{nickname}  (@{username})  ID {user_id}"))
            dialog = ft.AlertDialog(
                modal=True,
                title=ft.Text(self.group_titles.get(gid, f"Group {gid}")),
                content=ft.Column(
                    tight=True,
                    spacing=10,
                    controls=[
                        ft.Row(spacing=10, controls=[self.avatar(self.group_titles.get(gid, f"Group {gid}"), 48), ft.Text(self.group_titles.get(gid, f"Group {gid}"), weight=ft.FontWeight.W_600)]),
                        ft.Text(f"Group ID: {gid}"),
                        ft.Text(f"Members: {len(members)}"),
                        ft.Divider(),
                        *lines,
                    ],
                ),
                actions=[ft.TextButton("Close", on_click=lambda e: self.page.pop_dialog())],
            )
            self.page.show_dialog(dialog)

    def logout(self, _e=None):
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        self.ws = None
        self.ws_thread = None
        self.connected = False
        self.db.set_setting("auth_token", "")
        self.auth_token = ""
        self.current_user_id = 0
        self.current_username = ""
        self.current_nickname = ""
        self.current_chat = "public"
        self.chat_list.controls = []
        self.messages.controls = []
        self.username_field.value = ""
        self.password_field.value = ""
        self.nickname_field.value = ""
        self.show_login()
        self.set_login_status("Logged out.")

    def avatar(self, text, size, avatar_url=""):
        initials = ''.join([part[:1] for part in (text or '?').split()[:2]]).upper() or '?'
        if avatar_url:
            return ft.Container(
                width=size,
                height=size,
                border_radius=size,
                clip_behavior=ft.ClipBehavior.HARD_EDGE,
                content=ft.Image(src=avatar_url, width=size, height=size, fit=ft.BoxFit.COVER),
            )
        return ft.Container(
            width=size,
            height=size,
            border_radius=size,
            bgcolor="#2ec7c1",
            content=ft.Row(
                expand=True,
                alignment=ft.MainAxisAlignment.CENTER,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[ft.Text(initials, size=max(16, int(size * 0.34)), weight=ft.FontWeight.W_500)],
            ),
        )

    def snack(self, text):
        # Keep notification bubble width close to text length.
        width = max(180, min(640, 28 + len(text) * 8))
        self.page.snack_bar = ft.SnackBar(
            ft.Text(text),
            behavior=ft.SnackBarBehavior.FLOATING,
            width=width,
            padding=ft.padding.symmetric(horizontal=14, vertical=10),
            open=True,
        )
        self._safe_update()

    def _now_ts(self):
        from datetime import datetime
        return datetime.now().strftime("%H:%M")


def main(page: ft.Page):
    FletTelegramClient(page)




