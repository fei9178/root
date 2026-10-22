#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Termux 聊天室后端 (单文件 + 内嵌前端)
依赖: 仅标准库
用法: python chat_server.py
环境变量:
  PORT        监听端口 (默认 8080)
  DB_DIR      数据库目录 (默认 ./data, Render 部署请设为 /data)
"""
import os, sys, json, re, sqlite3, threading, time, hashlib, base64, secrets, ipaddress, traceback
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

try:
    import resource
    HAVE_RESOURCE = True
except ImportError:
    HAVE_RESOURCE = False

DB_DIR = os.environ.get("DB_DIR") or os.environ.get("DATA_DIR") or "./data"
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "chat.db")

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT") or "8080")

# ---------- 全局状态 ----------
G = {
    "peers": {},          # username -> {handler, nickname, authed, room}
    "lock": threading.Lock(),
    "start_ts": time.time(),
    "req_count": 0,
}

# ---------- 工具函数 ----------
def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def short_ts():
    return datetime.now().strftime("%m-%d %H:%M")

def pwhash(pwd):
    return hashlib.sha256(("chat_salt_2026_" + pwd).encode("utf-8")).hexdigest()

def uid():
    return secrets.token_hex(8)

def is_safe_room(rid):
    return bool(re.fullmatch(r"[A-Za-z0-9_\-]+", rid or ""))

# ---------- 数据库 ----------
class DB:
    def __init__(self, path):
        self.path = path
        self.lk = threading.Lock()
        self.conn = self._connect(path)
        self._init()

    @staticmethod
    def _connect(path):
        """兼容 virtiofs / 网络挂载等不支持 WAL 的存储"""
        for mode in ("WAL", "DELETE", "MEMORY"):
            try:
                if mode == "MEMORY":
                    conn = sqlite3.connect(":memory:", check_same_thread=False)
                    sys.stderr.write("[DB] 警告: 磁盘不可写, 使用内存模式(重启数据丢失)\n")
                else:
                    conn = sqlite3.connect(path, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=" + mode)
                conn.execute("PRAGMA foreign_keys=ON")
                conn.execute("CREATE TABLE IF NOT EXISTS _probe(x)")
                conn.execute("INSERT INTO _probe VALUES(1)")
                conn.execute("DELETE FROM _probe")
                conn.commit()
                sys.stderr.write("[DB] 日志模式: %s, 路径: %s\n" % (mode, path))
                return conn
            except Exception as e:
                sys.stderr.write("[DB] %s 模式失败: %s\n" % (mode, e))
                continue
        raise RuntimeError("数据库不可用: 请检查 DB_DIR 指向的目录是否有写入权限")

    def _init(self):
        c = self.conn
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            nickname TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            banned INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rooms(
            room_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            owner TEXT NOT NULL,
            password_hash TEXT,
            is_default INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            FOREIGN KEY(owner) REFERENCES users(username)
        );
        CREATE TABLE IF NOT EXISTS room_members(
            room_id TEXT NOT NULL,
            username TEXT NOT NULL,
            joined_at TEXT NOT NULL,
            PRIMARY KEY(room_id, username),
            FOREIGN KEY(room_id) REFERENCES rooms(room_id) ON DELETE CASCADE,
            FOREIGN KEY(username) REFERENCES users(username) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mtype TEXT NOT NULL,
            from_user TEXT NOT NULL,
            to_target TEXT NOT NULL,
            content TEXT NOT NULL,
            time TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_msg_target ON messages(mtype, to_target, id);
        CREATE INDEX IF NOT EXISTS idx_msg_from ON messages(from_user, id);

        CREATE TABLE IF NOT EXISTS friends(
            user_a TEXT NOT NULL,
            user_b TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            since TEXT NOT NULL,
            PRIMARY KEY(user_a, user_b),
            FOREIGN KEY(user_a) REFERENCES users(username) ON DELETE CASCADE,
            FOREIGN KEY(user_b) REFERENCES users(username) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_friend_b ON friends(user_b, status);

        CREATE TABLE IF NOT EXISTS announcements(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL,
            pinned INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS motd(
            id INTEGER PRIMARY KEY CHECK (id=1),
            content TEXT NOT NULL,
            updated_by TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reports(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reporter TEXT NOT NULL,
            target TEXT NOT NULL,
            reason TEXT NOT NULL,
            room TEXT,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open'
        );
        """)
        # 默认公共大厅 (system 用户不入库, owner 用 NULL 兼容外键)
        c.execute("PRAGMA foreign_keys=OFF")
        c.execute("INSERT OR IGNORE INTO rooms(room_id,name,owner,is_default,created_at) VALUES(?,?,?,1,?)",
                  ("public", "公共大厅", "system", now_iso()))
        c.execute("PRAGMA foreign_keys=ON")
        # 默认管理员
        try:
            c.execute("INSERT INTO users(username,password_hash,nickname,is_admin,created_at) VALUES(?,?,?,1,?)",
                      ("admin", pwhash("admin123"), "管理员", now_iso()))
        except sqlite3.IntegrityError:
            pass
        c.commit()

    def get(self, sql, args=()):
        with self.lk:
            r = self.conn.execute(sql, args).fetchone()
            return dict(r) if r else None

    def query(self, sql, args=()):
        with self.lk:
            return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def execute(self, sql, args=()):
        with self.lk:
            cur = self.conn.execute(sql, args)
            self.conn.commit()
            return cur.lastrowid

    def val(self, sql, args=()):
        with self.lk:
            r = self.conn.execute(sql, args).fetchone()
            return r[0] if r else None

DB = DB(DB_PATH)

# ---------- 用户/房间 辅助 ----------
def get_user(username):
    return DB.get("SELECT * FROM users WHERE username=?", (username,))

def room_exists(rid):
    return DB.get("SELECT * FROM rooms WHERE room_id=?", (rid,))

def room_member_count(rid):
    return DB.val("SELECT COUNT(*) FROM room_members WHERE room_id=?", (rid,)) or 0

def is_member(rid, username):
    return bool(DB.get("SELECT 1 FROM room_members WHERE room_id=? AND username=?",
                      (rid, username)))

def is_friend(a, b):
    if not a or not b or a == b:
        return False
    u, v = sorted([a, b])
    r = DB.get("SELECT status FROM friends WHERE user_a=? AND user_b=?", (u, v))
    return bool(r and r["status"] == "accepted")

def public_rooms_list():
    """返回房间列表, 含真实成员数"""
    rows = DB.query("SELECT * FROM rooms ORDER BY is_default DESC, created_at ASC")
    out = []
    for r in rows:
        d = dict(r)
        d["members"] = room_member_count(d["room_id"])
        d["has_password"] = bool(d.get("password_hash"))
        out.append(d)
    return out

def online_usernames():
    with G["lock"]:
        return [u for u, p in G["peers"].items() if p.get("authed")]

def peer_count(room_id):
    with G["lock"]:
        return sum(1 for p in G["peers"].values()
                   if p.get("authed") and p.get("room") == room_id)

# ---------- 推送 ----------
def peers_in_room(room_id):
    with G["lock"]:
        return [p for p in G["peers"].values()
                if p.get("authed") and p.get("room") == room_id]

def peer_by_user(username):
    with G["lock"]:
        return G["peers"].get(username)

def broadcast(room_id, event):
    data = json.dumps(event, ensure_ascii=False)
    for p in peers_in_room(room_id):
        p.get("queue") and p["queue"].append(data)

def user_event(username, event):
    p = peer_by_user(username)
    if p and p.get("queue"):
        p["queue"].append(json.dumps(event, ensure_ascii=False))

def broadcast_all(event):
    data = json.dumps(event, ensure_ascii=False)
    with G["lock"]:
        for p in G["peers"].values():
            if p.get("queue"):
                p["queue"].append(data)

def push_online():
    users = []
    with G["lock"]:
        for u, p in G["peers"].items():
            if p.get("authed"):
                users.append({"username": u, "nickname": p.get("nickname", "")})
    broadcast_all({"t": "online", "online": users, "count": len(users)})

def is_admin(username):
    if not username:
        return False
    u = get_user(username)
    return bool(u and u.get("is_admin") and not u.get("banned"))

# ---------- 消息处理 ----------
def handle_message(p, msg):
    mtype = msg.get("mtype") or msg.get("type")
    user = p.get("username")
    if not user:
        return
    if mtype == "join":
        rid = msg.get("room")
        if not is_safe_room(rid):
            return
        if not room_exists(rid):
            return
        old = p.get("room")
        with G["lock"]:
            p["room"] = rid
        if old and old != rid:
            broadcast(old, {"t": "leave", "user": user, "nickname": p.get("nickname"),
                            "room": old})
            broadcast(old, {"t": "system", "text": "{} 离开了房间".format(p.get("nickname")),
                            "room": old})
        if not is_member(rid, user):
            DB.execute("INSERT OR IGNORE INTO room_members(room_id,username,joined_at) VALUES(?,?,?)",
                       (rid, user, now_iso()))
        broadcast(rid, {"t": "join", "user": user, "nickname": p.get("nickname"), "room": rid})
        broadcast(rid, {"t": "system", "text": "{} 进入了房间".format(p.get("nickname")), "room": rid})
        broadcast(rid, {"t": "rooms", "rooms": public_rooms_list()})
        push_online()
    elif mtype in ("group", "msg"):
        rid = msg.get("room") or p.get("room") or "public"
        text = (msg.get("text") or "").strip()
        if not text or len(text) > 4000:
            return
        if not is_member(rid, user):
            DB.execute("INSERT OR IGNORE INTO room_members(room_id,username,joined_at) VALUES(?,?,?)",
                       (rid, user, now_iso()))
        DB.execute("INSERT INTO messages(mtype,from_user,to_target,content,time) VALUES(?,?,?,?,?)",
                   ("group", user, rid, text, short_ts()))
        broadcast(rid, {"t": "msg", "mtype": "group", "user": user, "nickname": p.get("nickname"),
                        "room": rid, "text": text, "time": short_ts()})
    elif mtype == "pm":
        to = msg.get("to")
        text = (msg.get("text") or "").strip()
        if not to or not text or len(text) > 4000:
            return
        DB.execute("INSERT INTO messages(mtype,from_user,to_target,content,time) VALUES(?,?,?,?,?)",
                   ("pm", user, to, text, short_ts()))
        ev = {"t": "msg", "mtype": "pm", "user": user, "nickname": p.get("nickname"),
              "to": to, "text": text, "time": short_ts()}
        user_event(to, ev)
        if peer_by_user(user) and peer_by_user(user).get("queue"):
            peer_by_user(user)["queue"].append(json.dumps(ev, ensure_ascii=False))
    elif mtype == "typing":
        rid = msg.get("room") or p.get("room")
        if rid:
            broadcast(rid, {"t": "typing", "user": user, "nickname": p.get("nickname"), "room": rid})

def on_disconnect(p):
    user = p.get("username")
    room = p.get("room")
    if user and room:
        broadcast(room, {"t": "leave", "user": user, "nickname": p.get("nickname"), "room": room})
        broadcast(room, {"t": "system", "text": "{} 已下线".format(p.get("nickname")), "room": room})
    if user:
        with G["lock"]:
            G["peers"].pop(user, None)
    push_online()

# ---------- HTTP ----------
class Handler(BaseHTTPRequestHandler):
    server_version = "TermuxChat/2.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("[%s] %s\n" % (now_iso(), fmt % args))

    def _send(self, code, ctype, body, extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if extra:
            for k, v in extra.items():
                self.send_header(k, v)
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self._send(code, "application/json; charset=utf-8", body)

    def _ok(self, **kw):
        self._json({"ok": True, **kw})

    def _err(self, msg, code=200):
        self._json({"ok": False, "msg": msg}, code)

    def _read_json(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
            if n <= 0:
                return {}
            raw = self.rfile.read(n)
            return json.loads(raw.decode("utf-8")) or {}
        except Exception:
            return {}

    # ---- 鉴权 ----
    def _auth(self):
        u = self.headers.get("X-User")
        t = self.headers.get("X-Token")
        if not u or not t:
            return None
        p = peer_by_user(u)
        if not p or p.get("token") != t:
            return None
        return get_user(u)

    def _user_from_body(self, d):
        """从请求体取 user 字段, 若该用户已登录(存在 token 对)则视为合法"""
        username = (d or {}).get("user")
        if not username:
            return None
        p = peer_by_user(username)
        if not p or not p.get("token"):
            return None
        return get_user(username)

    def _admin(self):
        u = self._auth()
        if not u or not u.get("is_admin") or u.get("banned"):
            return None
        return u

    # ---- 路由 ----
    def do_GET(self):
        G["req_count"] += 1
        up = urlparse(self.path)
        path = up.path
        q = {k: v[0] for k, v in parse_qs(up.query).items()}
        try:
            if path in ("/", "/index.html"):
                return self._send(200, "text/html; charset=utf-8", HTML_INDEX.encode("utf-8"))
            if path == "/admin" or path == "/admin.html":
                return self._send(200, "text/html; charset=utf-8", HTML_ADMIN.encode("utf-8"))
            if path == "/app.js":
                return self._send(200, "application/javascript; charset=utf-8",
                                  JS_APP.encode("utf-8"),
                                  extra={"Cache-Control": "no-cache"})
            if path == "/api/online":
                users = online_usernames()
                return self._json({"ok": True, "online": users, "count": len(users)})
            if path == "/api/health":
                return self._json({"ok": True, "uptime": int(time.time() - G["start_ts"]),
                                   "req": G["req_count"], "peers": len(G["peers"]),
                                   "time": now_iso()})
            if path == "/api/announcements":
                rows = DB.query("SELECT * FROM announcements WHERE pinned=1 ORDER BY id DESC LIMIT 5")
                return self._json({"ok": True, "announcements": [dict(r) for r in rows]})
            if path == "/api/motd":
                m = DB.get("SELECT * FROM motd WHERE id=1")
                return self._json({"ok": True, "motd": m["content"] if m else "",
                                   "updated_at": m["updated_at"] if m else "",
                                   "updated_by": m["updated_by"] if m else ""})
            if path == "/api/stream":
                return self._sse(q.get("user"), q.get("token"))
            if path == "/api/rooms":
                user = q.get("user")
                me = peer_by_user(user)
                cu = me.get("username") if me else None
                rows = public_rooms_list()
                out = []
                for r in rows:
                    d = {"id": r["room_id"], "name": r["name"], "owner": r["owner"],
                         "is_default": r["is_default"], "members": r["members"],
                         "online": peer_count(r["room_id"]),
                         "has_password": bool(r.get("password_hash")),
                         "joined": is_member(r["room_id"], cu) if cu else False}
                    out.append(d)
                return self._json({"ok": True, "rooms": out})
            if path == "/api/rooms/search":
                kw = (q.get("kw") or "").strip()
                if len(kw) < 1:
                    return self._json({"ok": True, "rooms": []})
                like = "%" + kw + "%"
                rows = DB.query("SELECT * FROM rooms WHERE name LIKE ? OR room_id LIKE ? "
                                "ORDER BY is_default DESC, name ASC LIMIT 30", (like, like))
                out = []
                for r in rows:
                    out.append({"id": r["room_id"], "name": r["name"], "owner": r["owner"],
                                "is_default": r["is_default"],
                                "members": room_member_count(r["room_id"]),
                                "online": peer_count(r["room_id"]),
                                "has_password": bool(r.get("password_hash"))})
                return self._json({"ok": True, "rooms": out})
            if path == "/api/users/search":
                u = self._auth()
                if not u:
                    return self._json({"ok": False, "msg": "未登录", "debug": "no_auth",
                                      "headers": str(dict(self.headers))[:200]})
                kw = (q.get("kw") or "").strip()
                if len(kw) < 1:
                    return self._json({"ok": True, "users": []})
                like = "%" + kw + "%"
                rows = DB.query("SELECT username, nickname, is_admin, banned FROM users "
                                "WHERE username LIKE ? OR nickname LIKE ? "
                                "ORDER BY nickname LIMIT 20", (like, like))
                return self._json({"ok": True, "users": [dict(r) for r in rows], "count": len(rows)})
            if path == "/api/history":
                user = q.get("user")
                room = q.get("room") or "public"
                limit = int(q.get("limit") or 100)
                if limit > 500:
                    limit = 500
                me = peer_by_user(user)
                cu = me.get("username") if me else None
                if not cu or not is_member(room, cu):
                    return self._err("无权访问该房间")
                rows = DB.query("SELECT mtype,from_user AS 'from',to_target AS 'to',content,time "
                                "FROM messages WHERE mtype='group' AND to_target=? "
                                "ORDER BY id DESC LIMIT ?", (room, limit))
                rows.reverse()
                return self._json({"ok": True, "room": room,
                                   "messages": [dict(r) for r in rows]})
            if path == "/api/pm/history":
                d = self._read_json()
                me = self._user_from_body(d) or peer_by_user(q.get("user"))
                if not me:
                    return self._err("未登录")
                with G["lock"]:
                    cu = me.get("username")
                other = q.get("with")
                if not other:
                    return self._err("参数错误")
                rows = DB.query("SELECT mtype,from_user AS 'from',to_target AS 'to',content,time "
                                "FROM messages WHERE mtype='pm' AND "
                                "((from_user=? AND to_target=?) OR (from_user=? AND to_target=?)) "
                                "ORDER BY id DESC LIMIT 200",
                                (cu, other, other, cu))
                rows.reverse()
                return self._json({"ok": True, "with": other,
                                   "messages": [dict(r) for r in rows]})
            if path == "/api/friends":
                d = self._read_json()
                me = peer_by_user(d.get("user")) or peer_by_user(q.get("user"))
                if not me:
                    return self._err("未登录")
                with G["lock"]:
                    cu = me.get("username")
                rows = DB.query("SELECT * FROM friends WHERE user_a=? OR user_b=?", (cu, cu))
                friends, requests_in, requests_out = [], [], []
                for r in rows:
                    a, b = r["user_a"], r["user_b"]
                    if a != cu and b != cu:
                        continue
                    other = b if a == cu else a
                    ou = get_user(other)
                    if not ou:
                        continue
                    item = {"username": other, "nickname": ou["nickname"],
                            "is_admin": ou["is_admin"], "online": bool(peer_by_user(other))}
                    st = r["status"]
                    if st == "accepted":
                        friends.append(item)
                    elif st == "pending":
                        if a == cu:
                            requests_out.append(item)
                        else:
                            requests_in.append(item)
                return self._json({"ok": True, "friends": friends, "requests_in": requests_in,
                                   "requests_out": requests_out})
            if path == "/api/admin/users":
                a = self._admin()
                if not a:
                    return self._err("需要管理员权限")
                rows = DB.query("SELECT username,nickname,is_admin,banned,created_at FROM users "
                                "ORDER BY banned ASC, is_admin DESC, created_at ASC")
                online = set(online_usernames())
                return self._json({"ok": True,
                                  "users": [{"username": r["username"], "nickname": r["nickname"],
                                             "is_admin": r["is_admin"], "banned": r["banned"],
                                             "online": r["username"] in online,
                                             "created_at": r["created_at"]} for r in rows],
                                  "online": list(online)})
            if path == "/api/admin/rooms":
                a = self._admin()
                if not a:
                    return self._err("需要管理员权限")
                rows = public_rooms_list()
                return self._json({"ok": True, "rooms": [
                    {"id": r["room_id"], "name": r["name"], "owner": r["owner"],
                     "is_default": r["is_default"], "members": r["members"],
                     "online": peer_count(r["room_id"]),
                     "has_password": bool(r.get("password_hash")),
                     "created_at": r["created_at"]} for r in rows]})
            if path == "/api/admin/stats":
                a = self._admin()
                if not a:
                    return self._err("需要管理员权限")
                return self._json({"ok": True, "stats": {
                    "users": DB.val("SELECT COUNT(*) FROM users") or 0,
                    "online": len(G["peers"]),
                    "banned": DB.val("SELECT COUNT(*) FROM users WHERE banned=1") or 0,
                    "rooms": DB.val("SELECT COUNT(*) FROM rooms") or 0,
                    "messages": DB.val("SELECT COUNT(*) FROM messages") or 0,
                    "friends": DB.val("SELECT COUNT(*) FROM friends WHERE status='accepted'") or 0,
                    "reports": DB.val("SELECT COUNT(*) FROM reports WHERE status='open'") or 0,
                    "uptime": int(time.time() - G["start_ts"])}})
            if path == "/api/admin/reports":
                a = self._admin()
                if not a:
                    return self._err("需要管理员权限")
                rows = DB.query("SELECT * FROM reports ORDER BY id DESC LIMIT 100")
                return self._json({"ok": True, "reports": [dict(r) for r in rows]})
            if path == "/api/admin/pm":
                a = self._admin()
                if not a:
                    return self._err("需要管理员权限")
                u1, u2 = q.get("a"), q.get("b")
                if not u1 or not u2:
                    return self._err("参数错误")
                rows = DB.query("SELECT * FROM messages WHERE mtype='pm' AND "
                                "((from_user=? AND to_target=?) OR (from_user=? AND to_target=?)) "
                                "ORDER BY id DESC LIMIT 500", (u1, u2, u2, u1))
                return self._json({"ok": True, "messages": [dict(r) for r in rows]})
            return self._send(404, "text/plain; charset=utf-8", "404 Not Found".encode("utf-8"))
        except Exception as e:
            self.log_error("GET %s: %s\n%s", path, e, traceback.format_exc())
            return self._err("服务器内部错误: " + str(e), 500)

    def do_POST(self):
        G["req_count"] += 1
        path = urlparse(self.path).path
        d = self._read_json()
        try:
            if path == "/api/register":
                username = re.sub(r"\s+", "", (d.get("username") or "").strip())
                password = d.get("password") or ""
                nickname = (d.get("nickname") or "").strip() or username
                if not re.fullmatch(r"[A-Za-z0-9_]{3,20}", username):
                    return self._err("账号需 3-20 位字母/数字/下划线")
                if len(password) < 4:
                    return self._err("密码至少 4 位")
                if len(nickname) > 20:
                    return self._err("昵称过长")
                if get_user(username):
                    return self._err("账号已存在")
                DB.execute("INSERT INTO users(username,password_hash,nickname,created_at) VALUES(?,?,?,?)",
                           (username, pwhash(password), nickname, now_iso()))
                return self._ok(msg="注册成功")

            if path == "/api/login":
                username = (d.get("username") or "").strip()
                password = d.get("password") or ""
                u = get_user(username)
                if not u:
                    return self._err("账号不存在")
                if u["banned"]:
                    return self._err("该账号已被封禁, 请联系管理员")
                if u["password_hash"] != pwhash(password):
                    return self._err("密码错误")
                token = secrets.token_hex(16)
                with G["lock"]:
                    old = G["peers"].pop(username, None)
                    if old and old is not None:
                        pass
                    p = {"username": username, "nickname": u["nickname"], "token": token,
                         "authed": True, "room": "public", "queue": [], "handler": self}
                    G["peers"][username] = p
                    if not is_member("public", username):
                        DB.execute("INSERT OR IGNORE INTO room_members(room_id,username,joined_at) VALUES(?,?,?)",
                                   ("public", username, now_iso()))
                push_online()
                return self._json({"ok": True, "user": {"username": username,
                                                       "nickname": u["nickname"],
                                                       "is_admin": bool(u["is_admin"]),
                                                       "banned": bool(u["banned"]),
                                                       "token": token}})

            if path == "/api/admin/login":
                # 普通用户已登录后, 用管理员密码二次验证
                user = d.get("user") or ""
                admin_pass = d.get("admin_password") or ""
                u = get_user(user)
                if not u:
                    return self._err("请先登录普通账号")
                if not u.get("is_admin"):
                    return self._err("该账号没有管理员权限")
                if u["banned"]:
                    return self._err("账号已被封禁")
                if u["password_hash"] != pwhash(admin_pass):
                    return self._err("管理员密码错误")
                return self._ok(msg="验证成功")

            if path == "/api/logout":
                u = d.get("user") or ""
                with G["lock"]:
                    p = G["peers"].pop(u, None)
                push_online()
                return self._ok()

            # ---------- 发送群消息 ----------
            if path == "/api/send":
                me = self._user_from_body(d) or peer_by_user(d.get("user"))
                if not me:
                    return self._err("未登录")
                user = me["username"]
                rid = d.get("room") or me.get("room") or "public"
                text = (d.get("text") or "").strip()
                if not text:
                    return self._err("消息不能为空")
                if len(text) > 4000:
                    return self._err("消息过长")
                if not is_member(rid, user):
                    DB.execute("INSERT OR IGNORE INTO room_members(room_id,username,joined_at) VALUES(?,?,?)",
                               (rid, user, now_iso()))
                with G["lock"]:
                    me["room"] = rid
                DB.execute("INSERT INTO messages(mtype,from_user,to_target,content,time) VALUES(?,?,?,?,?)",
                           ("group", user, rid, text, short_ts()))
                broadcast(rid, {"t": "msg", "mtype": "group", "user": user,
                                "nickname": me.get("nickname"), "room": rid,
                                "text": text, "time": short_ts()})
                return self._ok()

            # ---------- 发送私信 ----------
            if path == "/api/pm":
                me = self._user_from_body(d) or peer_by_user(d.get("user"))
                if not me:
                    return self._err("未登录")
                user = me["username"]
                to = d.get("to") or ""
                text = (d.get("text") or "").strip()
                if not to or not text:
                    return self._err("参数错误")
                if len(text) > 4000:
                    return self._err("消息过长")
                if not get_user(to):
                    return self._err("对方账号不存在")
                if to == user:
                    return self._err("不能发私信给自己")
                DB.execute("INSERT INTO messages(mtype,from_user,to_target,content,time) VALUES(?,?,?,?,?)",
                           ("pm", user, to, text, short_ts()))
                ev = {"t": "msg", "mtype": "pm", "user": user,
                      "nickname": me.get("nickname"), "to": to,
                      "text": text, "time": short_ts()}
                user_event(to, ev)
                user_event(user, ev)
                return self._ok()

            if path == "/api/nickname":
                me = peer_by_user(d.get("user"))
                if not me:
                    return self._err("未登录")
                nn = (d.get("nickname") or "").strip()
                if not nn or len(nn) > 20:
                    return self._err("昵称 1-20 字")
                with G["lock"]:
                    me["nickname"] = nn
                u = me["username"]
                DB.execute("UPDATE users SET nickname=? WHERE username=?", (nn, u))
                broadcast_all({"t": "nickname", "user": u, "nickname": nn})
                return self._ok()

            if path == "/api/password":
                me = peer_by_user(d.get("user"))
                if not me:
                    return self._err("未登录")
                oldp = d.get("old_password") or ""
                newp = d.get("new_password") or ""
                if len(newp) < 4:
                    return self._err("新密码至少 4 位")
                u = me["username"]
                usr = get_user(u)
                if not usr or usr["password_hash"] != pwhash(oldp):
                    return self._err("原密码错误")
                DB.execute("UPDATE users SET password_hash=? WHERE username=?",
                           (pwhash(newp), u))
                return self._ok(msg="密码已修改")

            if path == "/api/room/create":
                me = self._user_from_body(d) or peer_by_user(d.get("user"))
                if not me:
                    return self._err("未登录")
                name = (d.get("name") or "").strip()
                if not name or len(name) > 30:
                    return self._err("房间名 1-30 字")
                password = (d.get("password") or "").strip()
                rid = "r" + uid()
                attempt = 0
                while room_exists(rid) and attempt < 5:
                    rid = "r" + uid()
                    attempt += 1
                if room_exists(rid):
                    rid = "r" + str(int(time.time() * 1000)) + uid()[:4]
                u = me["username"]
                DB.execute("INSERT INTO rooms(room_id,name,owner,password_hash,created_at) VALUES(?,?,?,?,?)",
                           (rid, name, u, pwhash(password) if password else None, now_iso()))
                DB.execute("INSERT OR IGNORE INTO room_members(room_id,username,joined_at) VALUES(?,?,?)",
                           (rid, u, now_iso()))
                broadcast_all({"t": "rooms", "rooms": public_rooms_list()})
                return self._json({"ok": True, "room": rid, "id": rid, "name": name,
                                   "has_password": bool(password)})

            if path == "/api/room/join":
                me = self._user_from_body(d) or peer_by_user(d.get("user"))
                if not me:
                    return self._err("未登录")
                rid = d.get("room") or ""
                if not is_safe_room(rid) or not room_exists(rid):
                    return self._err("房间不存在")
                room = room_exists(rid)
                u = me["username"]
                password = (d.get("password") or "").strip()
                already = is_member(rid, u)
                if room.get("password_hash") and room["password_hash"] != pwhash(password) and not already:
                    return self._json({"ok": False, "msg": "房间密码错误", "need_password": True})
                old_room = me.get("room")
                DB.execute("INSERT OR IGNORE INTO room_members(room_id,username,joined_at) VALUES(?,?,?)",
                           (rid, u, now_iso()))
                if old_room and old_room != rid:
                    broadcast(old_room, {"t": "leave", "user": u, "nickname": me.get("nickname"),
                                         "room": old_room})
                    broadcast(old_room, {"t": "system", "text": me.get("nickname") + " 离开了房间",
                                         "room": old_room})
                with G["lock"]:
                    me["room"] = rid
                broadcast(rid, {"t": "join", "user": u, "nickname": me.get("nickname"), "room": rid})
                broadcast(rid, {"t": "system", "text": me.get("nickname") + " 进入了房间", "room": rid})
                broadcast_all({"t": "rooms", "rooms": public_rooms_list()})
                return self._json({"ok": True, "room": rid})

            if path == "/api/room/leave":
                me = self._user_from_body(d) or peer_by_user(d.get("user"))
                if not me:
                    return self._err("未登录")
                rid = d.get("room") or ""
                if rid == "public":
                    return self._err("不能退出公共大厅")
                DB.execute("DELETE FROM room_members WHERE room_id=? AND username=?",
                           (rid, me["username"]))
                return self._ok()

            if path == "/api/friend/add":
                me = self._user_from_body(d) or peer_by_user(d.get("user"))
                if not me:
                    return self._err("未登录")
                target = (d.get("to") or "").strip()
                cu = me["username"]
                if not get_user(target):
                    return self._err("用户不存在")
                if target == cu:
                    return self._err("不能加自己")
                u, v = sorted([cu, target])
                exist = DB.get("SELECT status FROM friends WHERE user_a=? AND user_b=?", (u, v))
                if exist and exist["status"] == "accepted":
                    return self._err("已是好友")
                if exist and exist["status"] == "pending":
                    return self._err("请求已发送, 等待对方同意")
                DB.execute("INSERT OR REPLACE INTO friends(user_a,user_b,status,since) VALUES(?,?,?,?)",
                           (u, v, "pending", now_iso()))
                user_event(target, {"t": "friend_request", "from": cu,
                                    "nickname": me.get("nickname"), "time": now_iso()})
                return self._ok(msg="好友请求已发送")

            if path == "/api/friend/respond":
                me = self._user_from_body(d) or peer_by_user(d.get("user"))
                if not me:
                    return self._err("未登录")
                cu = me["username"]
                target = (d.get("to") or "").strip()
                action = d.get("action")  # accept / reject
                u, v = sorted([cu, target])
                exist = DB.get("SELECT status FROM friends WHERE user_a=? AND user_b=?", (u, v))
                if not exist or exist["status"] != "pending":
                    return self._err("请求不存在")
                if action == "accept":
                    DB.execute("UPDATE friends SET status='accepted' WHERE user_a=? AND user_b=?",
                               (u, v))
                    user_event(target, {"t": "friend_accepted", "from": cu,
                                        "nickname": me.get("nickname")})
                    return self._ok(msg="已添加为好友")
                else:
                    DB.execute("DELETE FROM friends WHERE user_a=? AND user_b=?", (u, v))
                    return self._ok(msg="已拒绝")

            if path == "/api/friend/remove":
                me = self._user_from_body(d) or peer_by_user(d.get("user"))
                if not me:
                    return self._err("未登录")
                cu = me["username"]
                target = (d.get("to") or "").strip()
                u, v = sorted([cu, target])
                DB.execute("DELETE FROM friends WHERE user_a=? AND user_b=?", (u, v))
                return self._ok()

            if path == "/api/report":
                me = self._user_from_body(d) or peer_by_user(d.get("user"))
                if not me:
                    return self._err("未登录")
                target = (d.get("to") or "").strip()
                reason = (d.get("reason") or "").strip()
                if not target or not reason:
                    return self._err("请填写举报对象与理由")
                DB.execute("INSERT INTO reports(reporter,target,reason,room,created_at) VALUES(?,?,?,?,?)",
                           (me["username"], target, reason, d.get("room") or "", now_iso()))
                return self._ok(msg="举报已提交, 管理员会处理")

            # ---------- 管理员操作 ----------
            a = self._admin()
            if not a:
                return self._err("需要管理员权限")
            aname = a["username"]

            if path == "/api/admin/ban":
                target = d.get("target")
                ban = bool(d.get("ban"))
                if not get_user(target):
                    return self._err("用户不存在")
                if target == aname:
                    return self._err("不能封禁自己")
                DB.execute("UPDATE users SET banned=? WHERE username=?",
                           (1 if ban else 0, target))
                if ban:
                    with G["lock"]:
                        p = G["peers"].pop(target, None)
                    push_online()
                return self._ok(msg="已" + ("封禁" if ban else "解封"))

            if path == "/api/admin/delete":
                target = d.get("target")
                if target == aname:
                    return self._err("不能注销自己")
                if target == "admin":
                    return self._err("内置管理员账号不可注销")
                DB.execute("DELETE FROM users WHERE username=?", (target,))
                with G["lock"]:
                    G["peers"].pop(target, None)
                push_online()
                return self._ok(msg="账号已注销")

            if path == "/api/admin/reset_password":
                target = d.get("target")
                newp = d.get("new_password") or "123456"
                if len(newp) < 4:
                    return self._err("新密码至少 4 位")
                if not get_user(target):
                    return self._err("用户不存在")
                DB.execute("UPDATE users SET password_hash=? WHERE username=?",
                           (pwhash(newp), target))
                return self._ok(msg="密码已重置为 " + newp)

            if path == "/api/admin/room/delete":
                rid = d.get("room")
                room = room_exists(rid)
                if not room:
                    return self._err("房间不存在")
                if room["is_default"]:
                    return self._err("公共大厅不可删除")
                DB.execute("DELETE FROM rooms WHERE room_id=?", (rid,))
                broadcast_all({"t": "rooms", "rooms": public_rooms_list()})
                return self._ok(msg="房间已解散")

            if path == "/api/admin/room/clearmsg":
                rid = d.get("room")
                DB.execute("DELETE FROM messages WHERE mtype='group' AND to_target=?", (rid,))
                if rid and rid != "all":
                    broadcast(rid, {"t": "cleared", "room": rid,
                                    "text": "本房间聊天记录已被管理员清空"})
                broadcast_all({"t": "system", "text": "全站聊天记录已被管理员清空"})
                return self._ok(msg="已清空")

            if path == "/api/admin/pm/clear":
                u1, u2 = d.get("a"), d.get("b")
                if not u1 or not u2:
                    return self._err("参数错误")
                DB.execute("DELETE FROM messages WHERE mtype='pm' AND "
                           "((from_user=? AND to_target=?) OR (from_user=? AND to_target=?))",
                           (u1, u2, u2, u1))
                return self._ok(msg="私信记录已清除")

            if path == "/api/admin/announce":
                content = (d.get("content") or "").strip()
                if not content or len(content) > 500:
                    return self._err("公告 1-500 字")
                DB.execute("INSERT INTO announcements(admin,content,created_at) VALUES(?,?,?)",
                           (aname, content, now_iso()))
                broadcast_all({"t": "announce", "content": content,
                               "admin": aname, "time": now_iso()})
                return self._ok(msg="公告已发布")

            if path == "/api/admin/announce/pin":
                aid = int(d.get("id") or 0)
                pin = int(d.get("pin") if d.get("pin") is not None else 1)
                DB.execute("UPDATE announcements SET pinned=? WHERE id=?", (1 if pin else 0, aid))
                return self._ok()

            if path == "/api/admin/announce/delete":
                aid = int(d.get("id") or 0)
                DB.execute("DELETE FROM announcements WHERE id=?", (aid,))
                return self._ok()

            if path == "/api/admin/motd":
                content = (d.get("content") or "").strip()
                if len(content) > 200:
                    return self._err("欢迎语过长")
                DB.execute("INSERT OR REPLACE INTO motd(id,content,updated_by,updated_at) VALUES(1,?,?,?)",
                           (content, aname, now_iso()))
                broadcast_all({"t": "motd", "content": content, "updated_by": aname})
                return self._ok(msg="已更新")

            if path == "/api/admin/report/resolve":
                rid = int(d.get("id") or 0)
                DB.execute("UPDATE reports SET status='resolved' WHERE id=?", (rid,))
                return self._ok()

            return self._err("未知接口", 404)
        except Exception as e:
            self.log_error("POST %s: %s\n%s", path, e, traceback.format_exc())
            return self._err("服务器内部错误: " + str(e), 500)

    # ---- SSE ----
    def _sse(self, username, token):
        if not username:
            return self._err("未登录")
        p = peer_by_user(username)
        if not p or p.get("token") != token:
            return self._err("登录已过期, 请重新登录")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        q = p.setdefault("queue", [])
        # 初始状态
        rooms = public_rooms_list()
        onl = [{"username": u, "nickname": G["peers"][u]["nickname"]}
               for u in online_usernames() if u in G["peers"]]
        motd = DB.get("SELECT content FROM motd WHERE id=1")
        anns = DB.query("SELECT * FROM announcements WHERE pinned=1 ORDER BY id DESC LIMIT 5")
        self._ev({"t": "hello", "user": username, "nickname": p.get("nickname"),
                  "rooms": rooms, "online": onl, "count": len(onl),
                  "motd": motd["content"] if motd else "",
                  "announcements": [dict(a) for a in anns]})
        last = time.time()
        try:
            while True:
                if q:
                    while q:
                        self._ev(json.loads(q.pop(0)))
                if time.time() - last > 20:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    last = time.time()
                time.sleep(0.2)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            on_disconnect(p)

    def _ev(self, obj):
        self.wfile.write(("data: " + json.dumps(obj, ensure_ascii=False) + "\n\n").encode("utf-8"))
        self.wfile.flush()

# ---------- 前端: 登录页 + 聊天界面 ----------
HTML_INDEX = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover, maximum-scale=1.0">
<title>Termux 聊天室</title>
<style>
:root{
--bg:#0d1117;--panel:#161b22;--panel2:#1c2430;--panel3:#232d3b;
--line:#2b3444;--txt:#e6edf3;--sub:#8b949e;--accent:#4493f8;--accent2:#a371f7;
--ok:#3fb950;--warn:#f85149;--gold:#d29922;
}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent;}
html,body{height:100%;}
body{font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif;
background:var(--bg);color:var(--txt);overflow:hidden;}
button,input,textarea{font-family:inherit;}
.hidden{display:none!important;}
::-webkit-scrollbar{width:6px;height:6px;}
::-webkit-scrollbar-thumb{background:var(--line);border-radius:3px;}
/* ===== 登录 ===== */
#screen-login{height:100%;display:flex;align-items:center;justify-content:center;padding:20px;
background:radial-gradient(1200px 600px at 50% -10%, rgba(68,147,248,.15), transparent);}
.auth{width:100%;max-width:380px;background:var(--panel);border:1px solid var(--line);
border-radius:22px;padding:28px 22px;box-shadow:0 24px 60px rgba(0,0,0,.5);}
.logo{text-align:center;margin-bottom:22px;}
.logo .badge{font-size:40px;}
.logo h1{font-size:26px;background:linear-gradient(90deg,var(--accent),var(--accent2));
-webkit-background-clip:text;background-clip:text;color:transparent;}
.logo p{color:var(--sub);font-size:13px;margin-top:6px;}
.tabs{display:flex;gap:6px;background:var(--panel2);padding:5px;border-radius:12px;margin-bottom:16px;}
.tabs button{flex:1;border:none;background:none;color:var(--sub);padding:10px;border-radius:9px;
font-size:14px;cursor:pointer;transition:.15s;font-weight:600;}
.tabs button.on{background:linear-gradient(90deg,var(--accent),var(--accent2));color:#fff;}
.auth input{width:100%;padding:13px 14px;background:var(--panel2);border:1px solid var(--line);
border-radius:12px;color:var(--txt);font-size:15px;margin-bottom:10px;}
.auth input:focus{outline:none;border-color:var(--accent);}
.submit{width:100%;padding:13px;border:none;border-radius:12px;font-size:15px;font-weight:700;
background:linear-gradient(90deg,var(--accent),var(--accent2));color:#fff;cursor:pointer;margin-top:6px;}
.submit:active{transform:scale(.98);}
.msg-line{text-align:center;font-size:13px;min-height:18px;margin-top:12px;}
.msg-line.err{color:var(--warn);}
.msg-line.ok{color:var(--ok);}
.motd{margin-top:18px;padding:12px 14px;background:var(--panel2);border:1px solid var(--line);
border-radius:12px;font-size:12px;color:var(--sub);line-height:1.6;}
.motd strong{color:var(--gold);}
.hint{font-size:11px;color:var(--sub);text-align:center;margin-top:14px;}
.hint code{background:var(--panel2);padding:2px 6px;border-radius:5px;color:var(--accent);}
.ann-box{background:var(--panel2);border:1px solid var(--gold);border-radius:14px;padding:16px;margin:20px auto;
max-width:340px;text-align:center;}
.ann-box h4{font-size:15px;margin-bottom:8px;color:var(--gold);}
.ann-box p{font-size:13px;color:var(--sub);margin-bottom:12px;}
.ann-box input{width:100%;padding:10px 12px;background:var(--bg);border:1px solid var(--line);
border-radius:10px;color:var(--txt);font-size:14px;text-align:center;margin-bottom:12px;}
.ann-box .row{display:flex;gap:10px;}
.ann-box .row button{flex:1;padding:10px;border:none;border-radius:10px;font-size:14px;cursor:pointer;}
.ann-box .row .ok{background:var(--accent);color:#fff;}
.ann-box .row .no{background:var(--panel);color:var(--sub);}
/* ===== 聊天主界面 ===== */
#screen-chat{height:100%;display:flex;flex-direction:column;}
header{display:flex;align-items:center;gap:10px;padding:11px 14px;background:var(--panel);
border-bottom:1px solid var(--line);}
header .back{flex:1;min-width:0;}
header .title{font-weight:600;font-size:16px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
header .sub{font-size:11px;color:var(--sub);}
.icon-btn{border:none;background:var(--panel2);color:var(--txt);width:38px;height:38px;
border-radius:10px;cursor:pointer;font-size:16px;flex-shrink:0;display:flex;align-items:center;
justify-content:center;position:relative;}
.icon-btn:active{background:var(--line);}
.badge i{position:absolute;top:-1px;right:-1px;width:8px;height:8px;background:var(--warn);
border-radius:50%;display:none;}
.badge.on i{display:block;}
/* 房间标签 */
#room-tabs{display:flex;gap:8px;padding:10px 12px;overflow-x:auto;background:var(--panel);
border-bottom:1px solid var(--line);scrollbar-width:none;}
#room-tabs::-webkit-scrollbar{display:none;}
.room-tab{flex-shrink:0;padding:7px 13px;border-radius:20px;background:var(--panel2);
border:1px solid var(--line);color:var(--sub);font-size:13px;cursor:pointer;white-space:nowrap;
display:flex;align-items:center;gap:4px;}
.room-tab.on{background:var(--accent);color:#fff;border-color:var(--accent);}
.room-tab .lock{font-size:11px;}
/* 公告条 */
#ann-bar{background:rgba(210,153,34,.12);border-bottom:1px solid rgba(210,153,34,.3);
padding:8px 14px;font-size:12px;color:var(--gold);display:flex;gap:8px;align-items:flex-start;}
#ann-bar .x{border:none;background:none;color:var(--sub);cursor:pointer;font-size:14px;flex-shrink:0;}
/* 消息区 */
#messages{flex:1;overflow-y:auto;padding:14px 12px;scroll-behavior:smooth;}
.msg{display:flex;margin-bottom:12px;gap:8px;align-items:flex-end;}
.msg.me{flex-direction:row-reverse;}
.avatar{width:34px;height:34px;border-radius:50%;background:linear-gradient(135deg,var(--accent),var(--accent2));
color:#fff;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;flex-shrink:0;}
.bubble{max-width:72%;padding:9px 13px;border-radius:14px;font-size:15px;line-height:1.5;
word-break:break-word;background:var(--panel2);}
.msg.me .bubble{background:var(--accent);}
.msg.pm .bubble{border:1px solid var(--accent2);}
.msg .who{font-size:11px;color:var(--sub);margin-bottom:3px;}
.msg .time{font-size:10px;color:var(--sub);opacity:.7;margin:0 4px;}
.sys{text-align:center;color:var(--sub);font-size:12px;margin:8px 0;}
.sys.ann{color:var(--gold);}
.ann-tag{background:rgba(210,153,34,.15);color:var(--gold);font-size:11px;padding:6px 12px;
border-radius:10px;margin:8px auto;max-width:90%;text-align:center;line-height:1.5;}
/* 输入区 */
#composer{display:flex;gap:8px;padding:10px 12px;background:var(--panel);
border-top:1px solid var(--line);align-items:center;}
#input{flex:1;padding:11px 14px;background:var(--panel2);border:1px solid var(--line);
border-radius:22px;color:var(--txt);font-size:15px;}
#input:focus{outline:none;border-color:var(--accent);}
#send{border:none;background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;
width:44px;height:44px;border-radius:50%;cursor:pointer;font-size:18px;flex-shrink:0;}
#send:active{transform:scale(.92);}
/* ===== 抽屉 ===== */
.drawer{position:fixed;inset:0;z-index:50;display:none;}
.drawer.on{display:block;}
.mask{position:absolute;inset:0;background:rgba(0,0,0,.55);}
.panel{position:absolute;right:0;top:0;bottom:0;width:86%;max-width:340px;background:var(--panel);
display:flex;flex-direction:column;animation:slide .2s ease;}
@keyframes slide{from{transform:translateX(100%);}to{transform:translateX(0);}}
.panel-h{display:flex;align-items:center;justify-content:space-between;padding:14px;
border-bottom:1px solid var(--line);}
.panel-h h3{font-size:16px;}
.panel-h button{border:none;background:none;color:var(--sub);font-size:22px;cursor:pointer;}
.panel-b{flex:1;overflow-y:auto;padding:10px;}
.panel-f{padding:12px;border-top:1px solid var(--line);}
.user-item{display:flex;align-items:center;gap:10px;padding:10px;border-radius:12px;}
.user-item.tap{cursor:pointer;}
.user-item.tap:active{background:var(--panel2);}
.user-item .dot{width:9px;height:9px;border-radius:50%;background:var(--ok);flex-shrink:0;}
.user-item .dot.off{background:var(--line);}
.user-item .un{flex:1;min-width:0;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.user-item .nk{font-size:11px;color:var(--sub);}
.user-item .tag{font-size:10px;padding:2px 6px;border-radius:6px;background:var(--panel2);color:var(--sub);flex-shrink:0;}
.user-item .tag.admin{color:var(--accent);}
.tag.gold{color:var(--gold);}
.mini-btn{border:1px solid var(--line);background:var(--panel2);color:var(--txt);font-size:12px;
padding:5px 10px;border-radius:8px;cursor:pointer;flex-shrink:0;}
.mini-btn:active{opacity:.7;}
.mini-btn.pri{background:var(--accent);border-color:var(--accent);}
.mini-btn.warn{color:var(--warn);border-color:rgba(248,81,73,.3);}
.sec-title{font-size:12px;color:var(--sub);padding:14px 8px 6px;font-weight:700;letter-spacing:.5px;}
.room-add{display:flex;gap:8px;margin-top:8px;align-items:center;}
.room-add input{flex:1;padding:9px 12px;background:var(--panel2);border:1px solid var(--line);
border-radius:10px;color:var(--txt);font-size:14px;}
.room-add button{padding:9px 14px;border:none;border-radius:10px;background:var(--accent);color:#fff;font-size:14px;}
.room-add .lock-chk{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--sub);margin-top:8px;}
.btn-block{width:100%;padding:12px;border-radius:12px;border:1px solid var(--line);background:var(--panel2);
color:var(--txt);font-size:14px;cursor:pointer;margin-bottom:8px;text-align:left;display:flex;
align-items:center;gap:10px;}
.btn-block:active{background:var(--line);}
.btn-block .ic{font-size:16px;}
.btn-block.danger{color:var(--warn);border-color:rgba(248,81,73,.3);}
.btn-block.gold{color:var(--gold);border-color:rgba(210,153,34,.3);}
.me-card{display:flex;align-items:center;gap:12px;padding:6px 0 14px;}
.me-card .info .n{font-weight:600;}
.me-card .info .u{font-size:12px;color:var(--sub);}
.nick-edit{display:flex;gap:8px;}
.nick-edit input{flex:1;padding:10px 12px;background:var(--panel2);border:1px solid var(--line);
border-radius:10px;color:var(--txt);font-size:14px;}
.nick-edit button{padding:10px 16px;border:none;border-radius:10px;background:var(--accent);color:#fff;font-size:14px;}
.pass-edit{display:flex;flex-direction:column;gap:8px;}
.pass-edit input{padding:10px 12px;background:var(--panel2);border:1px solid var(--line);
border-radius:10px;color:var(--txt);font-size:14px;}
.logout{width:100%;padding:11px;border-radius:12px;border:1px solid var(--line);background:transparent;
color:var(--warn);font-size:14px;cursor:pointer;margin-top:10px;}
.empty{padding:24px 0;text-align:center;color:var(--sub);font-size:13px;}
.search-row{display:flex;gap:8px;margin-bottom:10px;}
.search-row input{flex:1;padding:9px 12px;background:var(--panel2);border:1px solid var(--line);
border-radius:10px;color:var(--txt);font-size:14px;}
.search-row button{padding:9px 14px;border:none;border-radius:10px;background:var(--panel3);color:var(--txt);font-size:14px;}
.room-result{display:flex;align-items:center;gap:8px;padding:9px 10px;border-radius:10px;
border:1px solid var(--line);background:var(--panel2);margin-bottom:6px;}
.room-result .rn{flex:1;min-width:0;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.room-result .rn small{display:block;color:var(--sub);font-size:11px;}
.pwd-prompt{position:fixed;inset:0;z-index:80;display:none;align-items:center;justify-content:center;
background:rgba(0,0,0,.6);padding:20px;}
.pwd-prompt.on{display:flex;}
.pwd-prompt .box{width:100%;max-width:320px;background:var(--panel);border:1px solid var(--line);
border-radius:16px;padding:20px;}
.pwd-prompt h4{font-size:15px;margin-bottom:6px;}
.pwd-prompt p{font-size:12px;color:var(--sub);margin-bottom:12px;}
.pwd-prompt input{width:100%;padding:11px 12px;background:var(--panel2);border:1px solid var(--line);
border-radius:10px;color:var(--txt);font-size:15px;margin-bottom:12px;text-align:center;}
.pwd-prompt .row{display:flex;gap:10px;}
.pwd-prompt .row button{flex:1;padding:10px;border:none;border-radius:10px;font-size:14px;cursor:pointer;}
.pwd-prompt .row .ok{background:var(--accent);color:#fff;}
.pwd-prompt .row .no{background:var(--panel);color:var(--sub);}
.toast{position:fixed;left:50%;bottom:80px;transform:translateX(-50%);padding:10px 18px;
background:rgba(0,0,0,.88);color:#fff;border-radius:22px;font-size:13px;z-index:200;
transition:opacity .25s;max-width:80%;text-align:center;}
/* 私信 */
.pm-head{display:flex;align-items:center;gap:10px;padding:12px 14px;background:var(--panel);
border-bottom:1px solid var(--line);}
.pm-head .info{flex:1;min-width:0;}
.pm-head .info .n{font-size:15px;font-weight:600;}
.pm-head .info .s{font-size:11px;color:var(--ok);}
.pm-head button{border:none;background:var(--panel2);color:var(--sub);width:36px;height:36px;
border-radius:10px;cursor:pointer;font-size:18px;}
.pm-msgs{flex:1;overflow-y:auto;padding:14px 12px;}
.pm-comp{flex-shrink:0;display:flex;gap:8px;padding:10px 12px;background:var(--panel);
border-top:1px solid var(--line);}
.pm-comp input{flex:1;padding:11px 14px;background:var(--panel2);border:1px solid var(--line);
border-radius:22px;color:var(--txt);font-size:15px;}
.pm-comp button{border:none;background:var(--accent2);color:#fff;width:44px;height:44px;
border-radius:50%;cursor:pointer;font-size:18px;}
</style>
</head>
<body>
<!-- 登录 -->
<div id="screen-login">
<div class="auth">
<div class="logo">
<div class="badge">💬</div>
<h1>Termux 聊天室</h1>
<p>局域网多人在线群聊 · 私聊 · 房间</p>
</div>
<div class="tabs">
<button id="tab-login" class="on">登录</button>
<button id="tab-register">注册</button>
</div>
<div id="view-login">
<input id="login-user" placeholder="账号" autocomplete="off" autocapitalize="off" spellcheck="false">
<input id="login-pass" type="password" placeholder="密码">
<button class="submit" id="btn-login">进入聊天室</button>
</div>
<div id="view-register" class="hidden">
<input id="reg-user" placeholder="账号 (3-20位字母/数字/下划线)" autocomplete="off" autocapitalize="off" spellcheck="false">
<input id="reg-nick" placeholder="昵称 (可选)" autocomplete="off" maxlength="20">
<input id="reg-pass" type="password" placeholder="密码 (至少4位)">
<button class="submit" id="btn-register">注册并登录</button>
</div>
<div class="msg-line" id="auth-msg"></div>
<div class="motd" id="motd-box" style="display:none;">
<strong>📢 <span id="motd-text"></span></strong>
</div>
<div class="ann-box hidden" id="ann-box">
<h4>📢 全站公告</h4>
<p id="ann-content"></p>
<button class="ok" onclick="document.getElementById('ann-box').classList.add('hidden')">我知道了</button>
</div>
<p class="hint">默认管理员 <code>admin / admin123</code><br>登录后可在菜单进入管理后台</p>
</div>
</div>
<!-- 聊天 -->
<div id="screen-chat" class="hidden">
<header>
<div class="back">
<div class="title" id="cur-title">公共大厅</div>
<div class="sub" id="cur-sub">群聊</div>
</div>
<button class="icon-btn badge" id="btn-users" title="在线用户">👥<i></i></button>
<button class="icon-btn" id="btn-menu" title="菜单">☰</button>
</header>
<div id="ann-bar" class="hidden">
<span id="ann-bar-text"></span>
<button class="x" id="ann-bar-x">✕</button>
</div>
<div id="room-tabs"></div>
<div id="messages"></div>
<div id="composer">
<input id="input" placeholder="说点什么…" autocomplete="off" maxlength="4000">
<button id="send">➤</button>
</div>
</div>
<!-- 在线用户 -->
<div class="drawer" id="drawer-users">
<div class="mask" data-close="drawer-users"></div>
<div class="panel">
<div class="panel-h"><h3>在线用户</h3><button data-close="drawer-users">✕</button></div>
<div class="panel-b" id="user-list"></div>
</div>
</div>
<!-- 菜单 -->
<div class="drawer" id="drawer-menu">
<div class="mask" data-close="drawer-menu"></div>
<div class="panel">
<div class="panel-h"><h3>菜单</h3><button data-close="drawer-menu">✕</button></div>
<div class="panel-b">
<div class="me-card">
<div class="avatar" id="me-avatar">?</div>
<div class="info">
<div class="n" id="me-nick"></div>
<div class="u" id="me-user"></div>
</div>
</div>
<div class="sec-title">个人</div>
<div class="nick-edit">
<input id="nick-input" placeholder="新昵称" maxlength="20">
<button id="btn-nick">保存</button>
</div>
<div class="sec-title">安全</div>
<div class="pass-edit">
<input id="pass-old" type="password" placeholder="当前密码">
<input id="pass-new" type="password" placeholder="新密码 (至少4位)">
<button class="btn-block" id="btn-pass">🔑 修改密码</button>
</div>
<div class="sec-title">好友</div>
<div id="friend-list"></div>
<button class="btn-block" id="btn-add-friend">➕ 添加好友</button>
<div class="sec-title">搜索房间</div>
<div class="search-row">
<input id="search-room" placeholder="输入房间名…" autocomplete="off">
<button id="btn-search-room">搜索</button>
</div>
<div id="search-result"></div>
<div class="sec-title">创建房间</div>
<div class="room-add">
<input id="new-room" placeholder="房间名" maxlength="30">
<button id="btn-new-room">＋</button>
</div>
<div class="room-add lock-chk">
<input type="checkbox" id="new-room-pwd-chk">
<input id="new-room-pwd" placeholder="密码" style="flex:1;padding:9px 12px;background:var(--panel2);
border:1px solid var(--line);border-radius:10px;color:var(--txt);font-size:14px;">
<label for="new-room-pwd-chk">🔒 设密码</label>
</div>
<div class="sec-title">后台管理</div>
<button class="btn-block" id="btn-admin">👑 管理员登录</button>
<button class="btn-block" id="btn-admin-direct" style="display:none;">👑 进入管理后台</button>
</div>
<div class="panel-f">
<button class="logout" id="btn-logout">退出登录</button>
</div>
</div>
</div>
<!-- 密码输入弹窗 -->
<div class="pwd-prompt" id="pwd-prompt">
<div class="box">
<h4 id="pwd-title">🔒 该房间需要密码</h4>
<p id="pwd-desc">输入房间密码以加入</p>
<input id="pwd-input" type="password" placeholder="房间密码" autocomplete="off">
<div class="row">
<button class="no" id="pwd-cancel">取消</button>
<button class="ok" id="pwd-ok">加入</button>
</div>
</div>
</div>
<script>
/* __APP_JS__ */
</script>
</body>
</html>"""

# ---------- 前端: 管理员后台 ----------
HTML_ADMIN = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>聊天室管理后台</title>
<style>
:root{--bg:#0d1117;--panel:#161b22;--panel2:#1c2430;--panel3:#232d3b;--line:#2b3444;
--txt:#e6edf3;--sub:#8b949e;--accent:#4493f8;--accent2:#a371f7;--ok:#3fb950;--warn:#f85149;--gold:#d29922;}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent;}
body{font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif;
background:var(--bg);color:var(--txt);min-height:100vh;padding:16px;}
.wrap{max-width:900px;margin:0 auto;}
h1{font-size:20px;margin-bottom:4px;}
.sub{color:var(--sub);font-size:13px;margin-bottom:16px;}
.login{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:24px;max-width:360px;margin:60px auto;}
.login input{width:100%;padding:12px;background:var(--panel2);border:1px solid var(--line);
border-radius:10px;color:var(--txt);font-size:15px;margin-bottom:12px;}
.login button{width:100%;padding:12px;border:none;border-radius:10px;background:var(--accent);color:#fff;font-size:15px;font-weight:700;}
.login .switch{text-align:center;margin-top:14px;font-size:13px;color:var(--sub);}
.login .switch a{color:var(--accent);text-decoration:none;}
.login .switch a.on{font-weight:700;color:var(--txt);}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:10px;margin-bottom:16px;}
.stat{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:14px;text-align:center;}
.stat .n{font-size:24px;font-weight:700;}
.stat .l{font-size:11px;color:var(--sub);margin-top:4px;}
.card{background:var(--panel);border:1px solid var(--line);border-radius:16px;overflow:hidden;margin-bottom:16px;}
.card h2{font-size:15px;padding:14px;border-bottom:1px solid var(--line);display:flex;
align-items:center;gap:8px;}
.card h2 .act{margin-left:auto;display:flex;gap:6px;}
.card .pad{padding:14px;}
table{width:100%;border-collapse:collapse;font-size:13px;}
th,td{padding:10px 12px;text-align:left;border-bottom:1px solid var(--line);}
th{color:var(--sub);font-size:11px;font-weight:700;white-space:nowrap;}
td.online{color:var(--ok);} td.offline{color:var(--sub);}
.mini{padding:5px 10px;border:1px solid var(--line);background:var(--panel2);color:var(--txt);
border-radius:8px;font-size:12px;cursor:pointer;margin:2px 4px 2px 0;}
.mini.danger{border-color:rgba(248,81,73,.3);color:var(--warn);}
.mini.pri{background:var(--accent);border-color:var(--accent);}
.mini.gold{background:rgba(210,153,34,.15);border-color:rgba(210,153,34,.3);color:var(--gold);}
.mini:active{opacity:.7;}
.hidden{display:none!important;}
.empty{color:var(--sub);font-size:13px;padding:20px;text-align:center;}
.head{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;flex-wrap:wrap;gap:10px;}
.head .left{display:flex;align-items:center;gap:12px;}
.head .left button,.head .left a{background:none;border:none;color:var(--accent);font-size:13px;cursor:pointer;text-decoration:none;}
.tabs{display:flex;gap:4px;background:var(--panel);padding:4px;border-radius:10px;margin-bottom:16px;
flex-wrap:wrap;}
.tabs button{border:none;background:none;color:var(--sub);padding:8px 14px;border-radius:8px;
font-size:13px;cursor:pointer;font-weight:600;}
.tabs button.on{background:var(--accent);color:#fff;}
.ann-input{display:flex;gap:8px;margin-bottom:12px;}
.ann-input input{flex:1;padding:10px 12px;background:var(--panel2);border:1px solid var(--line);
border-radius:10px;color:var(--txt);font-size:14px;}
.ann-input button{padding:10px 18px;border:none;border-radius:10px;background:var(--gold);color:#000;font-size:14px;font-weight:700;}
.motd-input{display:flex;gap:8px;}
.motd-input input{flex:1;padding:10px 12px;background:var(--panel2);border:1px solid var(--line);
border-radius:10px;color:var(--txt);font-size:14px;}
.motd-input button{padding:10px 18px;border:none;border-radius:10px;background:var(--accent);color:#fff;font-size:14px;}
.toast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%);padding:10px 18px;
background:rgba(0,0,0,.9);color:#fff;border-radius:20px;font-size:13px;z-index:100;}
.pill{font-size:10px;padding:2px 7px;border-radius:10px;background:var(--panel3);color:var(--sub);}
.pill.red{background:rgba(248,81,73,.15);color:var(--warn);}
.pill.green{background:rgba(63,185,80,.15);color:var(--ok);}
.pill.gold{background:rgba(210,153,34,.15);color:var(--gold);}
</style>
</head>
<body>
<div class="wrap">
<!-- 登录 -->
<div id="login-box" class="login">
<h1 style="text-align:center;margin-bottom:6px;">🛡️ 管理后台</h1>
<p class="sub" style="text-align:center;margin-bottom:18px;">仅管理员可访问</p>
<div class="switch" style="margin-bottom:16px;">
<a href="#" id="sw-normal" class="on">管理员登录</a>
</div>
<input id="adm-user" placeholder="管理员账号" value="admin">
<input id="adm-pass" type="password" placeholder="管理员密码" value="admin123">
<button id="adm-go">进入后台</button>
<p class="sub" style="margin-top:14px;text-align:center;">默认 admin / admin123<br>请登录后尽快修改密码</p>
</div>
<!-- 后台 -->
<div id="admin-box" class="hidden">
<div class="head">
<div class="left">
<button id="adm-back">← 返回聊天室</button>
</div>
<div class="left">
<span style="color:var(--sub);font-size:13px;" id="adm-name"></span>
<button id="adm-out">退出</button>
</div>
</div>
<h1>🛡️ 聊天室管理后台</h1>
<p class="sub">管理用户、房间、聊天记录与全站公告</p>
<div class="stats" id="stats"></div>
<div class="tabs">
<button data-tab="users" class="on">用户管理</button>
<button data-tab="rooms">房间管理</button>
<button data-tab="ann">公告与欢迎语</button>
<button data-tab="reports">举报</button>
<button data-tab="pm">私信记录</button>
</div>
<!-- 用户 -->
<div class="tab-pane" data-pane="users">
<div class="card">
<h2>用户列表</h2>
<div class="pad">
<div class="ann-input">
<input id="adm-search-user" placeholder="搜索账号或昵称…">
<button class="mini pri" id="adm-search-user-btn">搜索</button>
</div>
</div>
<div style="overflow-x:auto;">
<table>
<thead><tr><th>账号</th><th>昵称</th><th>身份</th><th>状态</th><th>操作</th></tr></thead>
<tbody id="user-tbody"></tbody>
</table>
</div>
</div>
</div>
<!-- 房间 -->
<div class="tab-pane hidden" data-pane="rooms">
<div class="card">
<h2>房间列表
<div class="act"><button class="mini pri" id="btn-refresh-rooms">刷新</button></div></h2>
<div style="overflow-x:auto;">
<table>
<thead><tr><th>房间</th><th>创建者</th><th>成员</th><th>在线</th><th>类型</th><th>操作</th></tr></thead>
<tbody id="room-tbody"></tbody>
</table>
</div>
</div>
<div class="card">
<h2>清空聊天记录</h2>
<div class="pad">
<div class="ann-input">
<input id="adm-clear-room" placeholder="输入房间 ID (留空或 all = 全站)">
<button class="mini danger" id="btn-clear-msg">清空记录</button>
</div>
</div>
</div>
</div>
<!-- 公告 -->
<div class="tab-pane hidden" data-pane="ann">
<div class="card">
<h2>发布全站公告</h2>
<div class="pad">
<div class="ann-input">
<input id="adm-ann" placeholder="公告内容 (全员实时弹出, 最长500字)">
<button id="btn-ann">发布</button>
</div>
<div id="ann-list"></div>
</div>
</div>
<div class="card">
<h2>登录页欢迎语</h2>
<div class="pad">
<div class="motd-input">
<input id="adm-motd" placeholder="显示在登录页的欢迎语 (留空则不显示)">
<button id="btn-motd">保存</button>
</div>
</div>
</div>
</div>
<!-- 举报 -->
<div class="tab-pane hidden" data-pane="reports">
<div class="card">
<h2>用户举报
<div class="act"><button class="mini" id="btn-refresh-reports">刷新</button></div></h2>
<div style="overflow-x:auto;">
<table>
<thead><tr><th>时间</th><th>举报人</th><th>对象</th><th>理由</th><th>房间</th><th>状态</th><th>操作</th></tr></thead>
<tbody id="report-tbody"></tbody>
</table>
</div>
</div>
</div>
<!-- 私信 -->
<div class="tab-pane hidden" data-pane="pm">
<div class="card">
<h2>查看私信记录</h2>
<div class="pad">
<div class="ann-input">
<input id="adm-pm-a" placeholder="用户A">
<input id="adm-pm-b" placeholder="用户B">
<button class="mini pri" id="btn-pm-query">查询</button>
</div>
<div id="pm-result"></div>
</div>
</div>
</div>
</div>
</div>
<script>
/* __ADMIN_JS__ */
</script>
</body>
</html>"""

# ---------- 前端: 聊天界面 JS ----------
JS_APP = r"""
(function () {
"use strict";
var API = "";
var S = { user:null, pass:"", nickname:"", isAdmin:false, curRoom:"public", rooms:[], evSource:null,
          friends:{}, typingTimer:null, searchKw:"" };
function $(s){return document.querySelector(s);}
function el(html){var t=document.createElement("template");t.innerHTML=html.trim();return t.content.firstChild;}
function esc(s){return String(s).replace(/[&<>"']/g,function(c){return{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c];});}
function ini(n){return (n&&n.charAt(0).toUpperCase())||"?";}
function toast(msg){
  var o=$("#toast"); if(o)o.remove();
  var d=el('<div class="toast">'+esc(msg)+'</div>');
  document.body.appendChild(d);
  setTimeout(function(){d.style.opacity="0";},1800);
  setTimeout(function(){d.remove();},2200);
}
function post(path,data,cb){
  fetch(API+path,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data||{})})
  .then(function(r){return r.json();}).then(function(j){cb&&cb(j);})
  .catch(function(){cb&&cb({ok:false,msg:"网络错误, 请检查后端是否运行"});});
}
function showAuthMsg(m,cls){var d=$("#auth-msg");d.textContent=m;d.className="msg-line"+(cls?" "+cls:"");}

/* ===== 登录/注册 ===== */
$("#tab-login").onclick=function(){sw("login");};
$("#tab-register").onclick=function(){sw("register");};
function sw(v){
  $("#tab-login").classList.toggle("on",v==="login");
  $("#tab-register").classList.toggle("on",v==="register");
  $("#view-login").classList.toggle("hidden",v!=="login");
  $("#view-register").classList.toggle("hidden",v!=="register");
  showAuthMsg("");
}
function setMotd(t){if(t){$("#motd-box").style.display="block";$("#motd-text").textContent=t;}}
$("#btn-login").onclick=function(){
  var u=$("#login-user").value.trim(),p=$("#login-pass").value;
  if(!u||!p)return showAuthMsg("请填写账号和密码","err");
  post("/api/login",{username:u,password:p},function(j){
    if(j.ok)enter(j.user,p);else showAuthMsg(j.msg,"err");
  });
};
$("#btn-register").onclick=function(){
  var u=$("#reg-user").value.trim(),n=$("#reg-nick").value.trim(),p=$("#reg-pass").value;
  if(!u||!p)return showAuthMsg("请填写账号和密码","err");
  post("/api/register",{username:u,password:p,nickname:n},function(j){
    if(j.ok){showAuthMsg("注册成功, 正在登录…","ok");
      post("/api/login",{username:u,password:p},function(k){if(k.ok)enter(k.user,p);else showAuthMsg(k.msg,"err");});
    }else showAuthMsg(j.msg,"err");
  });
};
function enter(user,pass){
  S.user=user.username;S.pass=pass;S.nickname=user.nickname;S.isAdmin=!!user.is_admin;
  localStorage.setItem("chat_user",user.username);
  localStorage.setItem("chat_pass",pass);
  $("#me-avatar").textContent=ini(user.nickname);
  $("#me-nick").textContent=user.nickname;
  $("#me-user").textContent="@"+user.username+(user.is_admin?" · 管理员":"");
  $("#btn-admin-direct").style.display=user.is_admin?"block":"none";
  $("#screen-login").classList.add("hidden");
  $("#screen-chat").classList.remove("hidden");
  openStream();
  loadRooms();
}
function tryAuto(){
  var u=localStorage.getItem("chat_user"),p=localStorage.getItem("chat_pass");
  if(u&&p)post("/api/login",{username:u,password:p},function(j){if(j.ok)enter(j.user,p);});
}
$("#btn-logout").onclick=function(){
  localStorage.removeItem("chat_pass");
  if(S.evSource)S.evSource.close();
  location.reload();
};
/* ===== SSE ===== */
function openStream(){
  if(S.evSource)S.evSource.close();
  S.evSource=new EventSource("/api/stream?user="+encodeURIComponent(S.user)+"&token="+encodeURIComponent("x"));
  S.evSource.onmessage=function(e){try{handle(JSON.parse(e.data));}catch(err){}};
  S.evSource.onerror=function(){setTimeout(openStream,3000);};
}
function handle(ev){
  switch(ev.t){
    case "hello":
      S.rooms=ev.rooms||[];renderRoomTabs();
      switchRoom(S.curRoom);renderOnline(ev.online||[]);
      setMotd(ev.motd);renderAnnBar(ev.announcements||[]);
      break;
    case "rooms":S.rooms=ev.rooms||[];renderRoomTabs();break;
    case "online":renderOnline(ev.online||[]);break;
    case "announce":
      renderAnnBar([{content:ev.content,admin:ev.admin,created_at:ev.time}]);
      $("#ann-box").classList.remove("hidden");
      $("#ann-content").textContent=ev.content;
      toast("📢 "+ev.content);break;
    case "motd":setMotd(ev.content);break;
    case "msg":
      if(ev.mtype==="group"){appendMsg(ev,ev.room===S.curRoom);if(ev.room===S.curRoom)scrollBottom();}
      else if(ev.mtype==="pm")handlePm(ev);
      break;
    case "join":case "leave":case "cleared":
      if(S.curRoom===ev.room)loadHistory(S.curRoom);
      break;
    case "system":
      if(ev.room===S.curRoom)appendSys(ev.text);
      else if(ev.text)toast(ev.text);
      break;
    case "friend_request":toast("📩 "+ev.nickname+" 请求加你为好友");loadFriends();break;
    case "friend_accepted":toast("✅ "+ev.nickname+" 已同意你的好友请求");loadFriends();break;
    case "nickname":
      if(ev.user===S.user){S.nickname=ev.nickname;$("#me-nick").textContent=ev.nickname;}
      break;
  }
}
/* ===== 房间 ===== */
function loadRooms(){
  fetch("/api/rooms?user="+encodeURIComponent(S.user)).then(function(r){return r.json();})
  .then(function(j){if(j.ok){S.rooms=j.rooms;renderRoomTabs();}});
}
function renderRoomTabs(){
  var box=$("#room-tabs");box.innerHTML="";
  (S.rooms||[]).forEach(function(r){
    var b=el('<button class="room-tab'+(r.id===S.curRoom?" on":"")+'">'+
      (r.has_password?'<span class="lock">🔒</span>':"")+esc(r.name)+'</button>');
    b.onclick=function(){joinRoom(r);};
    box.appendChild(b);
  });
}
function switchRoom(id){
  S.curRoom=id;
  var r=(S.rooms||[]).filter(function(x){return x.id===id;})[0]||{name:"公共大厅"};
  $("#cur-title").textContent=r.name;
  $("#cur-sub").textContent=(r.has_password?"🔒 ":"")+"群聊 · "+id;
  Array.prototype.forEach.call($$("#room-tabs .room-tab"),function(b,i){
    b.classList.toggle("on",S.rooms[i]&&S.rooms[i].id===id);});
  $("#messages").innerHTML="";loadHistory(id);
}
function joinRoom(r){
  var doJoin=function(pwd){
    post("/api/room/join",{user:S.user,room:r.id,password:pwd||""},function(j){
      if(j.ok){switchRoom(r.id);loadRooms();}
      else if(j.need_password){askPwd(r);}
      else toast(j.msg||"加入失败");
    });
  };
  if(r.has_password&&!r.joined)askPwd(r,doJoin);
  else doJoin("");
}
var _pwdCb=null;
function askPwd(r,cb){
  _pwdCb=cb;
  $("#pwd-title").textContent="🔒 "+r.name;
  $("#pwd-desc").textContent="该房间已设密码, 仅成员或持密码者可进入";
  $("#pwd-input").value="";
  $("#pwd-prompt").classList.add("on");
  setTimeout(function(){$("#pwd-input").focus();},100);
}
$("#pwd-ok").onclick=function(){
  var v=$("#pwd-input").value;
  $("#pwd-prompt").classList.remove("on");
  if(_pwdCb){_pwdCb(v);_pwdCb=null;}else joinRoomById($("#pwd-room-id").value,v);
};
$("#pwd-cancel").onclick=function(){$("#pwd-prompt").classList.remove("on");_pwdCb=null;};
$("#pwd-input").addEventListener("keydown",function(e){if(e.key==="Enter"){$("#pwd-ok").click();}});
function joinRoomById(rid,pwd){
  post("/api/room/join",{user:S.user,room:rid,password:pwd||""},function(j){
    if(j.ok){toast("已加入");loadRooms();}
    else if(j.need_password)toast("密码错误");else toast(j.msg||"加入失败");
  });
}
function loadHistory(room){
  fetch("/api/history?user="+encodeURIComponent(S.user)+"&room="+encodeURIComponent(room))
  .then(function(r){return r.json();}).then(function(j){
    if(!j.ok)return;
    $("#messages").innerHTML="";
    j.messages.forEach(function(m){appendMsg({mtype:"group",user:m.from,room:m.to,text:m.content,time:m.time},true);});
    scrollBottom();
  });
}
function appendMsg(m,visible){
  if(!visible)return;
  var mine=m.user===S.user,who=mine?"我":m.user;
  var d=el('<div class="msg '+(mine?"me":"")+(m.mtype==="pm"?" pm":"")+'">'+
    '<div class="avatar">'+(mine?ini(S.nickname):ini(who))+'</div>'+
    '<div><div class="who">'+esc(who)+'<span class="time">'+(m.time||"")+'</span></div>'+
    '<div class="bubble">'+esc(m.text)+'</div></div></div>');
  $("#messages").appendChild(d);
}
function appendSys(t){
  var d=el('<div class="sys">'+esc(t)+'</div>');
  $("#messages").appendChild(d);
}
function scrollBottom(){var m=$("#messages");m.scrollTop=m.scrollHeight;}
function send(){
  var v=$("#input").value.trim();if(!v)return;
  post("/api/send",{user:S.user,room:S.curRoom,text:v});
  $("#input").value="";
}
$("#send").onclick=send;
$("#input").addEventListener("keydown",function(e){if(e.key==="Enter"&&!e.shiftKey){e.preventDefault();send();}});
/* ===== 在线用户 ===== */
function renderOnline(list){
  var box=$("#user-list");box.innerHTML="";
  if(!list.length){box.innerHTML='<div class="empty">当前没有人在线</div>';}
  var friends={};
  (S.friends||[]).forEach(function(f){friends[f.username]=f;});
  list.forEach(function(u){
    var isF=!!friends[u.username];
    var it=el('<div class="user-item tap">'+
      '<span class="dot"></span>'+
      '<div class="avatar" style="width:36px;height:36px;font-size:13px;">'+ini(u.nickname)+'</div>'+
      '<div class="un">'+esc(u.nickname)+'<div class="nk">@'+esc(u.username)+
      (isF?' · <span style="color:var(--ok)">好友</span>':'')+'</div></div>'+
      (isF?'<button class="mini-btn">私信</button>':'<button class="mini-btn pri">加好友</button>')+
      '</div>');
    it.onclick=function(e){
      if(e.target.classList.contains("mini-btn")){
        e.stopPropagation();
        if(isF)openPm(u.username,u.nickname);
        else post("/api/friend/add",{user:S.user,to:u.username},function(j){toast(j.msg||(j.ok?"已发送":"操作失败"));});
      }else openPm(u.username,u.nickname);
    };
    box.appendChild(it);
  });
  $("#btn-users").classList.toggle("on",list.length>0);
}
/* ===== 好友 ===== */
function loadFriends(){
  fetch("/api/friends?user="+encodeURIComponent(S.user)).then(function(r){return r.json();})
  .then(function(j){if(!j.ok)return;
    S.friends=j.friends||[];
    var box=$("#friend-list");box.innerHTML="";
    if(j.requests_in&&j.requests_in.length){
      var h=el('<div class="sec-title">新请求</div>');box.appendChild(h);
      j.requests_in.forEach(function(u){
        var d=el('<div class="user-item"><div class="avatar" style="width:36px;height:36px;font-size:13px;">'+
          ini(u.nickname)+'</div><div class="un">'+esc(u.nickname)+'<div class="nk">@'+esc(u.username)+
          '</div></div><button class="mini-btn pri">同意</button><button class="mini-btn warn">拒绝</button></div>');
        d.querySelectorAll(".mini-btn")[0].onclick=function(){respond(u.username,"accept");};
        d.querySelectorAll(".mini-btn")[1].onclick=function(){respond(u.username,"reject");};
        box.appendChild(d);
      });
    }
    if(!S.friends.length){box.innerHTML+='<div class="empty">还没有好友, 去在线列表添加吧</div>';}
    S.friends.forEach(function(u){
      var d=el('<div class="user-item tap"><div class="avatar" style="width:36px;height:36px;font-size:13px;">'+
        ini(u.nickname)+'</div><div class="un">'+esc(u.nickname)+'<div class="nk">@'+esc(u.username)+
        '</div></div><span class="tag" style="color:var(--ok)">●</span><button class="mini-btn">私信</button></div>');
      d.onclick=function(){openPm(u.username,u.nickname);};
      d.querySelector(".mini-btn").onclick=function(e){e.stopPropagation();openPm(u.username,u.nickname);};
      box.appendChild(d);
    });
  });
}
function respond(t,act){
  post("/api/friend/respond",{user:S.user,to:t,action:act},function(j){
    toast(j.msg||"已处理");loadFriends();renderOnline(onlineCache);
  });
}
$("#btn-add-friend").onclick=function(){
  var n=prompt("输入对方账号");
  if(!n)return;
  post("/api/friend/add",{user:S.user,to:n.trim()},function(j){toast(j.msg||(j.ok?"已发送":"操作失败"));});
};
/* ===== 搜索房间 ===== */
$("#btn-search-room").onclick=doSearch;
$("#search-room").addEventListener("keydown",function(e){if(e.key==="Enter")doSearch();});
function doSearch(){
  var kw=$("#search-room").value.trim();
  var box=$("#search-result");
  if(!kw){box.innerHTML='<div class="empty">输入关键词开始搜索</div>';return;}
  box.innerHTML='<div class="empty">搜索中…</div>';
  fetch("/api/rooms/search?kw="+encodeURIComponent(kw)).then(function(r){return r.json();})
  .then(function(j){
    box.innerHTML="";
    if(!j.ok||!j.rooms.length){box.innerHTML='<div class="empty">没有找到匹配的房间</div>';return;}
    j.rooms.forEach(function(r){
      var joined=S.friends?false:false;
      var d=el('<div class="room-result"><span class="rn">'+esc(r.name)+
        (r.has_password?' 🔒 ':'')+'<small>@'+esc(r.id)+' · '+r.members+' 成员 · '+r.online+
        ' 在线 · 房主 @'+esc(r.owner)+'</small></span>'+
        '<button class="mini-btn pri">'+(r.joined?"进入":"加入")+'</button></div>');
      d.querySelector(".mini-btn").onclick=function(){joinRoom(r);};
      box.appendChild(d);
    });
  });
}
/* ===== 创建房间 ===== */
$("#btn-new-room").onclick=function(){
  var n=$("#new-room").value.trim();
  var pwd=$("#new-room-pwd-chk").checked?$("#new-room-pwd").value:"";
  if(!n)return toast("请输入房间名");
  post("/api/room/create",{user:S.user,name:n,password:pwd},function(j){
    if(j.ok){$("#new-room").value="";$("#new-room-pwd").value="";$("#new-room-pwd-chk").checked=false;
      toast("房间已创建");loadRooms();switchRoom(j.room);}
    else toast(j.msg||"创建失败");
  });
};
/* ===== 管理员登录 ===== */
$("#btn-admin").onclick=function(){
  closeDrawer("drawer-menu");
  if(S.isAdmin){window.location.href="/admin";return;}
  var p=prompt("请输入管理员密码");
  if(!p)return;
  post("/api/admin/login",{user:S.user,admin_password:p},function(j){
    if(j.ok){toast("验证成功, 正在进入…","ok");setTimeout(function(){window.location.href="/admin";},600);}
    else toast(j.msg||"验证失败");
  });
};
$("#btn-admin-direct").onclick=function(){closeDrawer("drawer-menu");window.location.href="/admin";};
/* ===== 公告条 ===== */
var _annList=[];
function renderAnnBar(list){
  if(!list||!list.length){$("#ann-bar").classList.add("hidden");_annList=[];return;}
  _annList=list;
  $("#ann-bar-text").textContent="📢 "+list[0].content;
  $("#ann-bar").classList.remove("hidden");
}
$("#ann-bar-x").onclick=function(){
  _annList.shift();
  if(_annList.length)$("#ann-bar-text").textContent="📢 "+_annList[0].content;
  else $("#ann-bar").classList.add("hidden");
};
/* ===== 昵称/密码 ===== */
$("#btn-nick").onclick=function(){
  var n=$("#nick-input").value.trim();
  if(!n)return toast("昵称不能为空","err");
  post("/api/nickname",{user:S.user,nickname:n},function(j){
    if(j.ok){S.nickname=n;$("#me-nick").textContent=n;toast("昵称已更新","ok");}else toast(j.msg,"err");});
};
$("#btn-pass").onclick=function(){
  var o=$("#pass-old").value,n=$("#pass-new").value;
  if(!o||!n)return toast("请填写完整","err");
  if(n.length<4)return toast("新密码至少4位","err");
  post("/api/password",{user:S.user,old_password:o,new_password:n},function(j){
    if(j.ok){$("#pass-old").value="";$("#pass-new").value="";toast("密码已修改, 请重新登录","ok");
      setTimeout(function(){location.reload();},1200);}
    else toast(j.msg,"err");
  });
};
/* ===== 私信 ===== */
var pm={to:null,toNick:"",box:null};
function openPm(uname,nick){
  pm.to=uname;pm.toNick=nick||uname;pm.box=null;
  $("#screen-chat").insertAdjacentHTML("afterend",
    '<div id="pm-overlay" style="position:fixed;inset:0;z-index:60;display:flex;flex-direction:column;'+
    'background:var(--bg);">'+
    '<div class="pm-head"><button id="pm-close">‹</button>'+
    '<div class="info"><div class="n" id="pm-title">'+esc(nick||uname)+'</div>'+
    '<div class="s" id="pm-status">私信</div></div></div>'+
    '<div class="pm-msgs" id="pm-msgs"></div>'+
    '<div class="pm-comp"><input id="pm-input" placeholder="发私信…" autocomplete="off" maxlength="4000">'+
    '<button id="pm-send">➤</button></div></div>');
  pm.box=$("#pm-msgs");
  $("#pm-close").onclick=function(){var o=$("#pm-overlay");if(o)o.remove();};
  $("#pm-send").onclick=pmSend;
  $("#pm-input").addEventListener("keydown",function(e){if(e.key==="Enter")pmSend();});
  loadPm();
}
function loadPm(){
  fetch("/api/pm/history?user="+encodeURIComponent(S.user)+"&with="+encodeURIComponent(pm.to))
  .then(function(r){return r.json();}).then(function(j){
    if(!j.ok||!pm.box)return;
    pm.box.innerHTML="";
    j.messages.forEach(function(m){pmAppend(m.from===S.user,"我",m.from===S.user?S.nickname:pm.toNick,m.content,m.time);});
    pmScroll();
  });
}
function pmAppend(mine,who,nick,text,time){
  var d=el('<div class="msg '+(mine?"me":"")+' pm">'+
    '<div class="avatar">'+ini(mine?S.nickname:pm.toNick)+'</div>'+
    '<div><div class="who">'+esc(who)+'<span class="time">'+(time||"")+'</span></div>'+
    '<div class="bubble">'+esc(text)+'</div></div></div>');
  if(pm.box)pm.box.appendChild(d);
}
function pmScroll(){if(pm.box)pm.box.scrollTop=pm.box.scrollHeight;}
function pmSend(){
  var v=$("#pm-input").value.trim();if(!v||!pm.to)return;
  post("/api/send",{user:S.user,to:pm.to,text:v,mtype:"pm"});
  pmAppend(true,"我",S.nickname,v,nowHM());
  $("#pm-input").value="";pmScroll();
}
function nowHM(){var d=new Date();return String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0")+" "+String(d.getHours()).padStart(2,"0")+":"+String(d.getMinutes()).padStart(2,"0");}
/* ===== 抽屉 ===== */
function openDrawer(id){$("#"+id).classList.add("on");}
function closeDrawer(id){$("#"+id).classList.remove("on");}
document.addEventListener("click",function(e){
  var c=e.target.getAttribute&&e.target.getAttribute("data-close");
  if(c)closeDrawer(c);
});
$("#btn-users").onclick=function(){loadOnline();openDrawer("drawer-users");};
$("#btn-menu").onclick=function(){loadFriends();openDrawer("drawer-menu");};
var onlineCache=[];
function loadOnline(){
  fetch("/api/online").then(function(r){return r.json();}).then(function(j){
    if(j.ok){onlineCache=j.online||[];renderOnline(onlineCache);}});
}
tryAuto();
if(location.hostname&&location.hostname!=="localhost"&&location.hostname!=="127.0.0.1"){
  $("#hint-ip").textContent=location.host;
}
})();
"""

# ---------- 前端: 后台 JS ----------
JS_ADMIN = r"""
(function(){
var user=null, pass="", token="";
function $(s){return document.querySelector(s);}
function el(h){var t=document.createElement("template");t.innerHTML=h.trim();return t.content.firstChild;}
function esc(s){return String(s).replace(/[&<>"']/g,function(c){return{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c];});}
function toast(m){
  var o=$("#toast");if(o)o.remove();
  var d=el('<div class="toast">'+esc(m)+'</div>');document.body.appendChild(d);
  setTimeout(function(){d.style.opacity="0";},1800);setTimeout(function(){d.remove();},2200);
}
function post(p,d,cb){
  fetch(p,{method:"POST",headers:{"Content-Type":"application/json","X-User":user,"X-Token":token},
  body:JSON.stringify(d||{})}).then(function(r){return r.json();}).then(function(j){cb&&cb(j);})
  .catch(function(){cb&&cb({ok:false,msg:"网络错误"});});
}
function need(){if(!user){location.href="/";return false;}return true;}
/* 登录 */
$("#adm-go").onclick=function(){
  var u=$("#adm-user").value.trim(),pw=$("#adm-pass").value;
  if(!u||!pw)return toast("请填写完整");
  fetch("/api/login",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({username:u,password:pw})}).then(function(r){return r.json();}).then(function(j){
    if(!j.ok){toast(j.msg||"登录失败");return;}
    if(!j.user.is_admin){toast("该账号不是管理员");return;}
    user=j.user.username;pass=pw;token=j.user.token;
    $("#login-box").classList.add("hidden");$("#admin-box").classList.remove("hidden");
    $("#adm-name").textContent="@"+user+" · 管理员";
    loadAll();
  });
};
$("#sw-normal").onclick=function(e){e.preventDefault();location.href="/";};
$("#adm-out").onclick=function(){location.reload();};
$("#adm-back").onclick=function(){location.href="/";};
/* 标签页 */
document.querySelectorAll(".tabs button").forEach(function(b){
  b.onclick=function(){
    document.querySelectorAll(".tabs button").forEach(function(x){x.classList.remove("on");});
    b.classList.add("on");
    var t=b.dataset.tab;
    document.querySelectorAll(".tab-pane").forEach(function(p){
      p.classList.toggle("hidden",p.dataset.pane!==t);});
    if(t==="users")loadUsers();
    if(t==="rooms")loadRooms();
    if(t==="ann")loadAnns();
    if(t==="reports")loadReports();
  };
});
/* 统计 */
function loadAll(){loadStats();loadUsers();}
function loadStats(){
  post("/api/admin/stats",{},function(j){
    if(!j.ok)return;var s=j.stats;
    $("#stats").innerHTML=
      card(s.users,"注册用户")+card(s.online,"在线")+card(s.banned,"已封禁")+
      card(s.rooms,"房间")+card(s.messages,"消息")+card(s.friends,"好友关系")+
      card(s.reports,"待处理举报")+card(s.uptime,"运行秒");
  });
}
function card(n,l){return '<div class="stat"><div class="n">'+n+'</div><div class="l">'+l+'</div></div>';}
/* 用户 */
function loadUsers(kw){
  var url="/api/admin/users";
  if(kw)url="/api/users/search?user="+encodeURIComponent(user)+"&kw="+encodeURIComponent(kw);
  else url="/api/admin/users";
  fetch(url,{headers:{"X-User":user,"X-Token":token}}).then(function(r){return r.json();}).then(function(j){
    if(!j.ok){toast(j.msg||"加载失败");return;}
    var rows=kw?j.users:j.users;
    var tb=$("#user-tbody");tb.innerHTML="";
    rows.forEach(function(u){
      var me=u.username===user;
      var tr=document.createElement("tr");
      tr.innerHTML="<td><strong>"+esc(u.username)+"</strong></td>"+
        "<td>"+esc(u.nickname)+"</td>"+
        "<td>"+(u.is_admin?'<span class="pill gold">管理员</span>':'<span class="pill">普通</span>')+"</td>"+
        "<td class="+(u.online?"online":"offline")+">"+(u.online?"🟢 在线":"⚪ 离线")+"</td>"+
        "<td>"+
        (u.is_admin?'<span class="pill">—</span>':
          '<button class="mini '+(u.banned?"":"danger")+'" data-u="'+esc(u.username)+'" data-ban="'+(!u.banned)+'">'+
          (u.banned?"解封":"封禁")+"</button>")+
        (u.is_admin||u.username==="admin"?'':'<button class="mini danger" data-del="'+esc(u.username)+'">注销</button>')+
        (u.is_admin||u.username==="admin"?'':'<button class="mini" data-reset="'+esc(u.username)+'">重置密码</button>')+
        "</td>";
      tb.appendChild(tr);
    });
  });
}
$("#adm-search-user-btn").onclick=function(){loadUsers($("#adm-search-user").value.trim());};
$("#adm-search-user").addEventListener("keydown",function(e){if(e.key==="Enter")$("#adm-search-user-btn").click();});
/* 房间 */
function loadRooms(){
  post("/api/admin/rooms",{},function(j){
    if(!j.ok){toast(j.msg||"加载失败");return;}
    var tb=$("#room-tbody");tb.innerHTML="";
    j.rooms.forEach(function(r){
      var tr=document.createElement("tr");
      tr.innerHTML="<td><strong>"+esc(r.name)+(r.has_password?" 🔒":"")+"</strong><br><small style='color:var(--sub)'>@"+esc(r.id)+"</small></td>"+
        "<td>@"+esc(r.owner)+"</td><td>"+r.members+"</td><td class='online'>"+r.online+"</td>"+
        "<td>"+(r.is_default?'<span class="pill gold">默认大厅</span>':'<span class="pill">普通</span>')+"</td>"+
        "<td>"+(r.is_default?'<span class="pill">—</span>':'<button class="mini danger" data-rm="'+esc(r.id)+'">解散</button>')+"</td>";
      tb.appendChild(tr);
    });
  });
}
$("#btn-refresh-rooms").onclick=function(){loadRooms();loadStats();};
$("#btn-clear-msg").onclick=function(){
  var rid=$("#adm-clear-room").value.trim()||"all";
  if(rid==="all"&&!confirm("确定清空全站所有聊天记录? 此操作不可恢复"))return;
  if(rid!=="all"&&!confirm("确定清空房间 "+rid+" 的聊天记录?"))return;
  post("/api/admin/room/clearmsg",{room:rid},function(j){toast(j.msg||"已清空");loadStats();});
};
/* 公告 */
function loadAnns(){
  fetch("/api/announcements").then(function(r){return r.json();}).then(function(j){
    var box=$("#ann-list");box.innerHTML="";
    if(!j.announcements||!j.announcements.length){box.innerHTML='<div class="empty">暂无公告</div>';return;}
    j.announcements.forEach(function(a){
      var d=el('<div style="padding:10px 0;border-bottom:1px solid var(--line);">'+
        '<div style="font-size:14px;">'+esc(a.content)+'</div>'+
        '<div style="font-size:11px;color:var(--sub);margin-top:4px;">@'+esc(a.admin)+' · '+esc(a.created_at)+'</div>'+
        '</div>');
      box.appendChild(d);
    });
  });
  fetch("/api/motd").then(function(r){return r.json();}).then(function(j){
    if(j.ok)$("#adm-motd").value=j.motd||"";
  });
}
$("#btn-ann").onclick=function(){
  var c=$("#adm-ann").value.trim();if(!c)return toast("内容为空");
  post("/api/admin/announce",{content:c},function(j){toast(j.msg||"已发布");$("#adm-ann").value="";loadAnns();loadStats();});
};
$("#btn-motd").onclick=function(){
  post("/api/admin/motd",{content:$("#adm-motd").value},function(j){toast(j.msg||"已保存");});
};
/* 举报 */
function loadReports(){
  post("/api/admin/reports",{},function(j){
    if(!j.ok)return;
    var tb=$("#report-tbody");tb.innerHTML="";
    if(!j.reports.length){tb.innerHTML='<tr><td colspan="7" class="empty">暂无举报</td></tr>';return;}
    j.reports.forEach(function(r){
      var tr=document.createElement("tr");
      tr.innerHTML="<td>"+esc(r.created_at)+"</td><td>@"+esc(r.reporter)+"</td><td>@"+esc(r.target)+"</td>"+
        "<td>"+esc(r.reason)+"</td><td>"+(r.room||"—")+"</td>"+
        "<td>"+(r.status==="open"?'<span class="pill red">待处理</span>':'<span class="pill green">已处理</span>')+"</td>"+
        "<td>"+(r.status==="open"?'<button class="mini pri" data-rid="'+r.id+'">标记处理</button>':'—')+"</td>";
      tb.appendChild(tr);
    });
  });
}
$("#btn-refresh-reports").onclick=function(){loadReports();};
/* 私信记录 */
$("#btn-pm-query").onclick=function(){
  var a=$("#adm-pm-a").value.trim(),b=$("#adm-pm-b").value.trim();
  if(!a||!b)return toast("请输入两个账号");
  post("/api/admin/pm",{a:a,b:b},function(j){
    var box=$("#pm-result");box.innerHTML="";
    if(!j.ok||!j.messages||!j.messages.length){box.innerHTML='<div class="empty">没有私信记录</div>';return;}
    j.messages.forEach(function(m){
      box.appendChild(el('<div style="padding:8px 0;border-bottom:1px solid var(--line);font-size:13px;">'+
        '<span style="color:var(--sub);font-size:11px;">['+esc(m.time)+']</span> '+
        '<strong>'+esc(m.from_user)+'</strong> → '+esc(m.to_target)+'：'+esc(m.content)+'</div>'));
    });
  });
};
/* 事件委托 */
document.addEventListener("click",function(e){
  var b=e.target;
  if(b.dataset&&b.dataset.ban!==undefined){
    var ban=b.dataset.ban==="true";
    post("/api/admin/ban",{target:b.dataset.u,ban:ban},function(j){toast(j.msg||"已更新");loadUsers();loadStats();});
  }
  if(b.dataset&&b.dataset.del){
    if(!confirm("确定注销账号 @"+b.dataset.del+"? 此操作不可恢复"))return;
    post("/api/admin/delete",{target:b.dataset.del},function(j){toast(j.msg||"已注销");loadUsers();loadStats();});
  }
  if(b.dataset&&b.dataset.reset){
    var np=prompt("为 @"+b.dataset.reset+" 设置新密码 (留空默认 123456)","");
    if(np===null)return;
    post("/api/admin/reset_password",{target:b.dataset.reset,new_password:np||"123456"},
      function(j){toast(j.msg||"已重置");});
  }
  if(b.dataset&&b.dataset.rm){
    if(!confirm("确定解散房间 "+b.dataset.rm+"?"))return;
    post("/api/admin/room/delete",{room:b.dataset.rm},function(j){toast(j.msg||"已解散");loadRooms();loadStats();});
  }
  if(b.dataset&&b.dataset.rid){
    post("/api/admin/report/resolve",{id:parseInt(b.dataset.rid)},function(j){toast("已标记处理");loadReports();});
  }
});
/* 自动刷新 */
setInterval(function(){
  if($("#admin-box").classList.contains("hidden"))return;
  loadStats();
  var t=$(".tabs button.on");
  if(t&&t.dataset.tab==="users")loadUsers();
  if(t&&t.dataset.tab==="rooms")loadRooms();
},8000);
})();
"""

# ---------- 注入 JS ----------
HTML_INDEX = HTML_INDEX.replace("/* __APP_JS__ */", JS_APP)
HTML_ADMIN = HTML_ADMIN.replace("/* __ADMIN_JS__ */", JS_ADMIN)

# ---------- 启动 ----------
def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    sys.stderr.write("=" * 50 + "\n")
    sys.stderr.write("Termux 聊天室 已启动\n")
    sys.stderr.write("  监听: http://%s:%d\n" % (HOST, PORT))
    sys.stderr.write("  数据库: %s\n" % DB_PATH)
    sys.stderr.write("  管理后台: /admin\n")
    sys.stderr.write("  默认管理员: admin / admin123\n")
    sys.stderr.write("=" * 50 + "\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        sys.stderr.write("\n正在关闭...\n")
        server.shutdown()

if __name__ == "__main__":
    main()
