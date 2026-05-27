"""
社区聊天室 - 服务器端
===========================
基于 WebSocket 的实时聊天应用

主要功能：
- 用户注册/登录 (Cookie + Token 双重认证)
- 实时消息广播
- 文件/图片上传
- 聊天历史记录
"""

import asyncio
import json
import sqlite3
import hashlib
import uuid
import re
from datetime import datetime
from pathlib import Path

from aiohttp import web
import aiohttp

# ==================== 配置 ====================
BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

DB_PATH = BASE_DIR / "chat.db"
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
COOKIE_NAME = "chat_token"
COOKIE_MAX_AGE = 7 * 24 * 60 * 60  # 7天

# 支持的图片格式
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}

# ==================== 网络配置（内网穿透用）====================
# 本地监听地址和端口
HOST = "0.0.0.0"
PORT = 8080

# 外部访问地址（用于 cPolar 等内网穿透）
# ⚠️ 如果使用内网穿透，请在这里填入你的代理地址
# 例如：PUBLIC_BASE_URL = "http://your-cpolar-url.xxx.com"
#       PUBLIC_WS_URL = "ws://your-cpolar-url.xxx.com/ws"
# 如果留空，则使用本地地址
PUBLIC_BASE_URL = "https://5a12bdd6.r23.cpolar.top"  # 外部 HTTP 地址（cpolar 免费版 https 会自动重定向）
PUBLIC_WS_URL = "wss://5a12bdd6.r23.cpolar.top/ws"  # ⚠️ wss:// 对应 https，ws:// 对应 http


# ==================== 数据库操作 ====================

def init_db():
    """初始化数据库表"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 用户表
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            nickname TEXT NOT NULL,
            avatar TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 聊天记录表
    c.execute("""
        CREATE TABLE IF NOT EXISTS chat_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            msg_type TEXT NOT NULL,
            content TEXT NOT NULL,
            time TEXT NOT NULL
        )
    """)
    
    # 私聊消息表
    c.execute("""
        CREATE TABLE IF NOT EXISTS private_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT NOT NULL,
            receiver TEXT NOT NULL,
            content TEXT NOT NULL,
            time TEXT NOT NULL,
            is_read INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 帖子表
    c.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            author TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            time TEXT NOT NULL,
            likes INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 帖子评论表
    c.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            author TEXT NOT NULL,
            content TEXT NOT NULL,
            time TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
        )
    """)
    
    # 帖子点赞表
    c.execute("""
        CREATE TABLE IF NOT EXISTS post_likes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            user TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(post_id, user)
        )
    """)
    
    conn.commit()
    conn.close()


def hash_password(password: str) -> str:
    """给密码加盐并哈希"""
    salt = uuid.uuid4().hex[:8]
    hashed = hashlib.sha256(f"{password}{salt}".encode()).hexdigest()
    return f"{hashed}:{salt}"


def verify_password(password: str, stored: str) -> bool:
    """验证密码是否正确"""
    try:
        hash_value, salt = stored.split(":")
        return hashlib.sha256(f"{password}{salt}".encode()).hexdigest() == hash_value
    except (ValueError, AttributeError):
        return False


def save_message(username: str, msg_type: str, content: str, time_str: str):
    """保存消息到数据库"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO chat_history (username, msg_type, content, time) VALUES (?, ?, ?, ?)",
        (username, msg_type, content, time_str)
    )
    conn.commit()
    conn.close()


def get_user_info(username: str):
    """获取用户信息"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT username, nickname, avatar FROM users WHERE username = ?", (username,))
    user = c.fetchone()
    conn.close()
    return user


# ==================== 私聊消息操作 ====================

def save_private_message(sender: str, receiver: str, content: str, time_str: str):
    """保存私聊消息"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO private_messages (sender, receiver, content, time) VALUES (?, ?, ?, ?)",
        (sender, receiver, content, time_str)
    )
    conn.commit()
    conn.close()


def get_private_messages(username: str, other: str, limit: int = 50):
    """获取两个用户之间的私聊记录"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT id, sender, receiver, content, time, is_read
        FROM private_messages
        WHERE (sender = ? AND receiver = ?) OR (sender = ? AND receiver = ?)
        ORDER BY id DESC LIMIT ?
    """, (username, other, other, username, limit))
    messages = c.fetchall()
    conn.close()
    return messages


def get_chat_list(username: str):
    """获取用户的聊天列表（和谁聊过）"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT 
            CASE 
                WHEN sender = ? THEN receiver 
                ELSE sender 
            END as other_user,
            (SELECT content FROM private_messages 
             WHERE (sender = ? AND receiver = CASE WHEN sender = ? THEN receiver ELSE sender END)
                OR (receiver = ? AND sender = CASE WHEN sender = ? THEN receiver ELSE sender END)
             ORDER BY id DESC LIMIT 1) as last_msg,
            (SELECT time FROM private_messages 
             WHERE (sender = ? AND receiver = CASE WHEN sender = ? THEN receiver ELSE sender END)
                OR (receiver = ? AND sender = CASE WHEN sender = ? THEN receiver ELSE sender END)
             ORDER BY id DESC LIMIT 1) as last_time,
            (SELECT COUNT(*) FROM private_messages 
             WHERE sender = CASE WHEN sender = ? THEN receiver ELSE sender END 
             AND receiver = ? AND is_read = 0) as unread_count
        FROM private_messages
        WHERE sender = ? OR receiver = ?
        ORDER BY (SELECT MAX(id) FROM private_messages p2 
                  WHERE (p2.sender = ? AND p2.receiver = CASE WHEN private_messages.sender = ? THEN private_messages.receiver ELSE private_messages.sender END)
                     OR (p2.receiver = ? AND p2.sender = CASE WHEN private_messages.sender = ? THEN private_messages.receiver ELSE private_messages.sender END)) DESC
    """, (username, username, username, username, username, username, username, username, username, username, username, username, username, username, username, username, username, username))
    chats = c.fetchall()
    conn.close()
    return chats


def mark_messages_read(username: str, from_user: str):
    """标记从某用户收到的消息为已读"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "UPDATE private_messages SET is_read = 1 WHERE sender = ? AND receiver = ?",
        (from_user, username)
    )
    conn.commit()
    conn.close()


# ==================== 帖子操作 ====================

def create_post(author: str, title: str, content: str, time_str: str):
    """创建帖子"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO posts (author, title, content, time) VALUES (?, ?, ?, ?)",
        (author, title, content, time_str)
    )
    post_id = c.lastrowid
    conn.commit()
    conn.close()
    return post_id


def get_posts(page: int = 1, per_page: int = 20):
    """获取帖子列表"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    offset = (page - 1) * per_page
    c.execute("""
        SELECT p.id, p.author, p.title, p.content, p.time, p.likes,
               (SELECT COUNT(*) FROM comments WHERE post_id = p.id) as comment_count
        FROM posts p
        ORDER BY p.id DESC
        LIMIT ? OFFSET ?
    """, (per_page, offset))
    posts = c.fetchall()
    conn.close()
    return posts


def get_post(post_id: int):
    """获取单个帖子"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT p.id, p.author, p.title, p.content, p.time, p.likes,
               (SELECT COUNT(*) FROM comments WHERE post_id = p.id) as comment_count
        FROM posts p
        WHERE p.id = ?
    """, (post_id,))
    post = c.fetchone()
    conn.close()
    return post


def like_post(post_id: int, user: str):
    """点赞帖子"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute(
            "INSERT INTO post_likes (post_id, user) VALUES (?, ?)",
            (post_id, user)
        )
        c.execute("UPDATE posts SET likes = likes + 1 WHERE id = ?", (post_id,))
        conn.commit()
        liked = True
    except sqlite3.IntegrityError:
        # 已经点过赞，取消点赞
        c.execute("DELETE FROM post_likes WHERE post_id = ? AND user = ?", (post_id, user))
        c.execute("UPDATE posts SET likes = likes - 1 WHERE id = ?", (post_id,))
        conn.commit()
        liked = False
    finally:
        conn.close()
    return liked


def is_post_liked(post_id: int, user: str):
    """检查用户是否已点赞"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM post_likes WHERE post_id = ? AND user = ?", (post_id, user))
    result = c.fetchone()
    conn.close()
    return result is not None


# ==================== 评论操作 ====================

def add_comment(post_id: int, author: str, content: str, time_str: str):
    """添加评论"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO comments (post_id, author, content, time) VALUES (?, ?, ?, ?)",
        (post_id, author, content, time_str)
    )
    comment_id = c.lastrowid
    conn.commit()
    conn.close()
    return comment_id


def get_comments(post_id: int):
    """获取帖子的所有评论"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT id, author, content, time
        FROM comments
        WHERE post_id = ?
        ORDER BY id ASC
    """, (post_id,))
    comments = c.fetchall()
    conn.close()
    return comments


# ==================== 聊天室管理器 ====================

class ChatRoom:
    """管理所有在线用户和消息广播"""
    
    def __init__(self):
        self.clients = set()      # 所有 WebSocket 连接
        self.user_map = {}         # websocket -> username
        self.tokens = {}           # token -> username
    
    async def broadcast(self, message: str):
        """向所有在线用户广播消息"""
        disconnected = []
        
        for client in self.clients:
            try:
                if not client.closed:
                    await client.send_str(message)
            except Exception as e:
                print(f"发送消息失败: {e}")
                disconnected.append(client)
        
        # 清理断开的连接
        for client in disconnected:
            await self.remove_client(client)
    
    async def add_client(self, websocket, username: str):
        """添加新用户"""
        self.clients.add(websocket)
        self.user_map[websocket] = username
    
    async def remove_client(self, websocket):
        """移除用户"""
        username = self.user_map.pop(websocket, None)
        self.clients.discard(websocket)
        return username
    
    @property
    def online_count(self) -> int:
        return len(self.clients)


# 全局聊天室实例
chat_room = ChatRoom()


# ==================== Cookie 辅助函数 ====================

def make_cookie_response(response_data, cookie_value=None):
    """创建带 Cookie 的响应"""
    response = web.json_response(response_data)
    if cookie_value:
        response.set_cookie(
            COOKIE_NAME,
            cookie_value,
            max_age=COOKIE_MAX_AGE,
            path="/",
            httponly=True,
            samesite="Lax"
        )
    return response


def get_auth_user(request) -> str:
    """从请求中获取认证用户"""
    # 优先从 Cookie 获取
    token = request.cookies.get(COOKIE_NAME, "")
    if token and token in chat_room.tokens:
        return chat_room.tokens[token]
    
    # 其次从 Header 获取
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        if token in chat_room.tokens:
            return chat_room.tokens[token]
    
    return None


# ==================== HTTP API 接口 ====================

async def api_register(request):
    """用户注册"""
    try:
        data = await request.json()
        username = data.get("username", "").strip()
        password = data.get("password", "")
        nickname = data.get("nickname", username)
        
        # 验证输入
        if not username or not password:
            return web.json_response({"code": 400, "msg": "用户名和密码不能为空"})
        if len(username) < 3 or len(username) > 20:
            return web.json_response({"code": 400, "msg": "用户名长度需在 3-20 位"})
        if len(password) < 6:
            return web.json_response({"code": 400, "msg": "密码至少 6 位"})
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # 检查用户名是否已存在
        c.execute("SELECT id FROM users WHERE username = ?", (username,))
        if c.fetchone():
            conn.close()
            return web.json_response({"code": 400, "msg": "用户名已被注册"})
        
        # 创建用户
        hashed = hash_password(password)
        c.execute(
            "INSERT INTO users (username, password, nickname) VALUES (?, ?, ?)",
            (username, hashed, nickname)
        )
        conn.commit()
        conn.close()
        
        return web.json_response({"code": 200, "msg": "注册成功"})
        
    except Exception as e:
        print(f"注册错误: {e}")
        return web.json_response({"code": 500, "msg": f"服务器错误"})


async def api_login(request):
    """用户登录"""
    try:
        data = await request.json()
        username = data.get("username", "").strip()
        password = data.get("password", "")
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "SELECT id, password, nickname, avatar FROM users WHERE username = ?",
            (username,)
        )
        user = c.fetchone()
        conn.close()
        
        # 验证用户名和密码
        if not user or not verify_password(password, user[1]):
            return web.json_response({"code": 401, "msg": "用户名或密码错误"})
        
        # 生成登录凭证
        token = str(uuid.uuid4())
        chat_room.tokens[token] = username
        
        # 返回带 Cookie 的响应
        return make_cookie_response({
            "code": 200,
            "msg": "登录成功",
            "data": {
                "token": token,
                "username": username,
                "nickname": user[2],
                "avatar": user[3]
            }
        }, cookie_value=token)
        
    except Exception as e:
        print(f"登录错误: {e}")
        return web.json_response({"code": 500, "msg": f"服务器错误"})


async def api_logout(request):
    """用户退出"""
    username = get_auth_user(request)
    if username:
        # 清理所有该用户的 token
        tokens_to_remove = [t for t, u in chat_room.tokens.items() if u == username]
        for t in tokens_to_remove:
            del chat_room.tokens[t]
    
    response = web.json_response({"code": 200, "msg": "已退出"})
    response.del_cookie(COOKIE_NAME, path="/")
    return response


async def api_check_login(request):
    """检查登录状态"""
    token = request.cookies.get(COOKIE_NAME, "")
    username = None
    
    if token and token in chat_room.tokens:
        username = chat_room.tokens[token]
    
    if username:
        user = get_user_info(username)
        if user:
            return web.json_response({
                "code": 200,
                "data": {
                    "logged_in": True,
                    "username": user[0],
                    "nickname": user[1],
                    "avatar": user[2],
                    "token": token,
                    "public_base_url": PUBLIC_BASE_URL,
                    "public_ws_url": PUBLIC_WS_URL
                }
            })
    
    return web.json_response({
        "code": 200,
        "data": {"logged_in": False}
    })


async def api_server_info(request):
    """获取服务器地址信息（供前端连接外部代理）"""
    return web.json_response({
        "code": 200,
        "data": {
            "base_url": PUBLIC_BASE_URL or f"http://localhost:{PORT}",
            "ws_url": PUBLIC_WS_URL or f"ws://localhost:{PORT}/ws"
        }
    })


# ==================== 私聊 API ====================

async def api_send_private(request):
    """发送私聊消息"""
    username = get_auth_user(request)
    if not username:
        return web.json_response({"code": 401, "msg": "未登录"})
    
    data = await request.json()
    receiver = data.get("receiver", "").strip()
    content = data.get("content", "").strip()
    
    if not receiver or not content:
        return web.json_response({"code": 400, "msg": "参数不完整"})
    
    if receiver == username:
        return web.json_response({"code": 400, "msg": "不能给自己发消息"})
    
    # 检查接收者是否存在
    user = get_user_info(receiver)
    if not user:
        return web.json_response({"code": 404, "msg": "用户不存在"})
    
    time_str = datetime.now().strftime("%H:%M:%S")
    save_private_message(username, receiver, content, time_str)
    
    # 如果接收者在线，通过 WebSocket 发送
    await send_private_ws(username, receiver, content, time_str)
    
    return web.json_response({"code": 200, "msg": "发送成功"})


async def send_private_ws(sender: str, receiver: str, content: str, time_str: str):
    """通过 WebSocket 发送私聊消息给接收者（如果在线）"""
    msg_json = json.dumps({
        "type": "private",
        "sender": sender,
        "receiver": receiver,
        "content": content,
        "time": time_str
    })
    
    for ws, user in chat_room.user_map.items():
        if user == receiver:
            try:
                await ws.send_str(msg_json)
            except Exception:
                pass


async def api_get_private_history(request):
    """获取私聊记录"""
    username = get_auth_user(request)
    if not username:
        return web.json_response({"code": 401, "msg": "未登录"})
    
    other = request.query.get("user", "")
    if not other:
        return web.json_response({"code": 400, "msg": "缺少参数"})
    
    messages = get_private_messages(username, other)
    # 标记消息为已读
    mark_messages_read(username, other)
    
    result = [{
        "id": m[0],
        "sender": m[1],
        "receiver": m[2],
        "content": m[3],
        "time": m[4],
        "is_read": m[5]
    } for m in messages]
    
    return web.json_response({"code": 200, "data": result})


async def api_get_chat_list(request):
    """获取聊天列表"""
    username = get_auth_user(request)
    if not username:
        return web.json_response({"code": 401, "msg": "未登录"})
    
    chats = get_chat_list(username)
    result = []
    for chat in chats:
        other_user = chat[0]
        user_info = get_user_info(other_user)
        result.append({
            "username": other_user,
            "nickname": user_info[1] if user_info else other_user,
            "avatar": user_info[2] if user_info else "",
            "last_msg": chat[1] or "",
            "last_time": chat[2] or "",
            "unread": chat[3] or 0
        })
    
    return web.json_response({"code": 200, "data": result})


async def api_get_all_users(request):
    """获取所有用户列表（用于私聊选择）"""
    username = get_auth_user(request)
    if not username:
        return web.json_response({"code": 401, "msg": "未登录"})
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT username, nickname, avatar FROM users WHERE username != ?", (username,))
    users = c.fetchall()
    conn.close()
    
    result = [{
        "username": u[0],
        "nickname": u[1],
        "avatar": u[2],
        "online": u[0] in chat_room.user_map.values()
    } for u in users]
    
    return web.json_response({"code": 200, "data": result})


# ==================== 帖子 API ====================

async def api_create_post(request):
    """发布帖子"""
    username = get_auth_user(request)
    if not username:
        return web.json_response({"code": 401, "msg": "未登录"})
    
    data = await request.json()
    title = data.get("title", "").strip()
    content = data.get("content", "").strip()
    
    if not title or not content:
        return web.json_response({"code": 400, "msg": "标题和内容不能为空"})
    
    if len(title) > 100:
        return web.json_response({"code": 400, "msg": "标题太长"})
    
    time_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    post_id = create_post(username, title, content, time_str)
    
    return web.json_response({"code": 200, "msg": "发布成功", "post_id": post_id})


async def api_get_posts(request):
    """获取帖子列表"""
    username = get_auth_user(request)
    page = int(request.query.get("page", 1))
    
    posts = get_posts(page)
    result = []
    for p in posts:
        liked = is_post_liked(p[0], username) if username else False
        result.append({
            "id": p[0],
            "author": p[1],
            "title": p[2],
            "content": p[3],
            "time": p[4],
            "likes": p[5],
            "comment_count": p[6],
            "liked": liked
        })
    
    return web.json_response({"code": 200, "data": result})


async def api_get_post_detail(request):
    """获取帖子详情（含评论）"""
    username = get_auth_user(request)
    post_id = int(request.query.get("id", 0))
    
    if not post_id:
        return web.json_response({"code": 400, "msg": "缺少帖子ID"})
    
    post = get_post(post_id)
    if not post:
        return web.json_response({"code": 404, "msg": "帖子不存在"})
    
    comments = get_comments(post_id)
    liked = is_post_liked(post_id, username) if username else False
    
    return web.json_response({
        "code": 200,
        "data": {
            "post": {
                "id": post[0],
                "author": post[1],
                "title": post[2],
                "content": post[3],
                "time": post[4],
                "likes": post[5],
                "comment_count": post[6],
                "liked": liked
            },
            "comments": [{
                "id": c[0],
                "author": c[1],
                "content": c[2],
                "time": c[3]
            } for c in comments]
        }
    })


async def api_like_post(request):
    """点赞帖子"""
    username = get_auth_user(request)
    if not username:
        return web.json_response({"code": 401, "msg": "未登录"})
    
    data = await request.json()
    post_id = int(data.get("post_id", 0))
    
    if not post_id:
        return web.json_response({"code": 400, "msg": "缺少帖子ID"})
    
    liked = like_post(post_id, username)
    return web.json_response({"code": 200, "liked": liked})


# ==================== 评论 API ====================

async def api_add_comment(request):
    """添加评论"""
    username = get_auth_user(request)
    if not username:
        return web.json_response({"code": 401, "msg": "未登录"})
    
    data = await request.json()
    post_id = int(data.get("post_id", 0))
    content = data.get("content", "").strip()
    
    if not post_id or not content:
        return web.json_response({"code": 400, "msg": "参数不完整"})
    
    if len(content) > 500:
        return web.json_response({"code": 400, "msg": "评论太长"})
    
    # 检查帖子是否存在
    post = get_post(post_id)
    if not post:
        return web.json_response({"code": 404, "msg": "帖子不存在"})
    
    time_str = datetime.now().strftime("%H:%M")
    comment_id = add_comment(post_id, username, content, time_str)
    
    # 广播评论通知给在线用户
    await chat_room.broadcast(json.dumps({
        "type": "comment_notification",
        "post_id": post_id,
        "post_title": post[2],
        "author": username,
        "content": content[:50],
        "time": time_str
    }))
    
    return web.json_response({"code": 200, "msg": "评论成功", "comment_id": comment_id})


def get_client_ip(request):
    """获取客户端真实IP地址（支持 cpolar 等内网穿透）"""
    # 打印所有请求头（调试用）
    print(f"[调试] 所有请求头: {dict(request.headers)}")
    
    # 1. 优先从 X-Forwarded-For 获取（cpolar HTTP 模式会传递此头）
    forwarded = request.headers.get('X-Forwarded-For', '')
    if forwarded:
        # X-Forwarded-For 格式: "真实IP, 代理1, 代理2..."，取第一个
        client_ip = forwarded.split(',')[0].strip()
        print(f"[调试] X-Forwarded-For = {forwarded} -> 真实IP = {client_ip}")
        return client_ip
    
    # 2. 尝试 X-Real-IP（nginx 常用）
    real_ip = request.headers.get('X-Real-IP', '')
    if real_ip:
        print(f"[调试] X-Real-IP = {real_ip}")
        return real_ip.strip()
    
    # 3. 最后尝试直接连接地址（可能只是代理服务器IP）
    if request.remote:
        print(f"[调试] request.remote = {request.remote} (可能是代理IP)")
        return request.remote
    
    return 'unknown'


async def api_client_info(request):
    """获取客户端信息（IP等）- 用于测试IP获取"""
    ip = get_client_ip(request)
    
    # 打印到服务器控制台（带来源说明）
    xff = request.headers.get('X-Forwarded-For', '')
    xri = request.headers.get('X-Real-IP', '')
    remote = request.remote
    
    print(f"\n{'='*50}")
    print(f"[IP信息] 真实客户端IP: {ip}")
    print(f"  X-Forwarded-For: {xff or '(无)'}")
    print(f"  X-Real-IP: {xri or '(无)'}")
    print(f"  request.remote: {remote}")
    print(f"{'='*50}\n")
    
    return web.json_response({
        "code": 200,
        "data": {
            "ip": ip,
            "xff": xff,
            "xri": xri,
            "remote": remote,
            "user_agent": request.headers.get('User-Agent', 'unknown')
        }
    })


async def api_upload(request):
    """文件上传"""
    try:
        # 验证登录
        username = get_auth_user(request)
        if not username:
            return web.json_response({"code": 401, "msg": "请先登录"}, status=401)
        
        # 读取 multipart 数据
        reader = await request.multipart()
        
        # 获取文件字段
        field = await reader.next()
        if not field or field.name != "file":
            return web.json_response({"code": 400, "msg": "缺少文件字段"})
        
        filename = field.filename
        if not filename:
            return web.json_response({"code": 400, "msg": "文件名无效"})
        
        # 读取文件内容
        file_data = b""
        chunk_size = 64 * 1024  # 64KB 每块
        while True:
            chunk = await field.read_chunk(chunk_size)
            if not chunk:
                break
            file_data += chunk
            
            # 检查文件大小
            if len(file_data) > MAX_FILE_SIZE:
                return web.json_response({"code": 413, "msg": "文件过大（最大 10MB）"})
        
        # 获取文件后缀并保存
        ext = Path(filename).suffix.lower()
        unique_name = f"{uuid.uuid4().hex}{ext}"
        file_path = UPLOAD_DIR / unique_name
        
        # 保存文件
        with open(file_path, "wb") as f:
            f.write(file_data)
        
        # 根据后缀判断文件类型
        msg_type = "image" if ext in IMAGE_EXTS else "file"
        file_url = f"/uploads/{unique_name}"
        
        return web.json_response({
            "code": 200,
            "data": {
                "type": msg_type,
                "url": file_url,
                "filename": filename,
                "size": len(file_data)
            }
        })
        
    except Exception as e:
        print(f"上传错误: {e}")
        import traceback
        traceback.print_exc()
        return web.json_response({"code": 500, "msg": f"上传失败: {str(e)}"})


async def api_history(request):
    """获取聊天历史"""
    try:
        limit = int(request.query.get("limit", 50))
        offset = int(request.query.get("offset", 0))
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "SELECT username, msg_type, content, time FROM chat_history ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset)
        )
        rows = c.fetchall()
        conn.close()
        
        # 反转顺序（按时间正序返回）
        history = [
            {"username": r[0], "type": r[1], "content": r[2], "time": r[3]}
            for r in reversed(rows)
        ]
        
        return web.json_response({"code": 200, "data": history})
        
    except Exception as e:
        print(f"历史记录错误: {e}")
        return web.json_response({"code": 500, "msg": f"服务器错误"})


async def web_index(request):
    """返回主页面"""
    html_path = BASE_DIR / "index.html"
    if not html_path.exists():
        return web.Response(text="index.html not found", status=404)
    
    with open(html_path, "r", encoding="utf-8") as f:
        return web.Response(text=f.read(), content_type="text/html")


# ==================== WebSocket 聊天服务 ====================

def get_ws_client_ip(websocket):
    """从 WebSocket 连接获取客户端 IP"""
    try:
        # 获取连接的远程地址
        transport = websocket._writer.transport
        if transport:
            peername = transport.get_extra_info('peername')
            if peername:
                return peername[0]
    except Exception:
        pass
    return 'unknown'


async def handle_chat(websocket, client_ip='unknown'):
    """处理 WebSocket 连接和消息"""
    username = None
    
    try:
        # --- 1. 认证阶段：等待客户端发送 token ---
        try:
            # aiohttp WebSocketResponse 用 receive() 而不是 recv()
            first_msg = await asyncio.wait_for(websocket.receive(), timeout=30)
            
            if first_msg.type != web.WSMsgType.TEXT:
                print(f"[WS] 非文本消息，断开 (IP: {client_ip})")
                return
            
            data = json.loads(first_msg.data)
            
            token = data.get("token", "")
            print(f"[WS] 收到认证请求 (IP: {client_ip})")
            
            username = chat_room.tokens.get(token)
            
            if not username:
                print(f"[WS] 认证失败 (IP: {client_ip})")
                await websocket.send_str(json.dumps({
                    "type": "error",
                    "content": "认证失败，请重新登录"
                }))
                return
                
        except asyncio.TimeoutError:
            print(f"[WS] 认证超时 (IP: {client_ip})")
            return
        
        # --- 2. 加入聊天室 ---
        await chat_room.add_client(websocket, username)
        print(f"[+] {username} 加入聊天室 (IP: {client_ip})，当前在线: {chat_room.online_count}")
        
        # 广播用户加入消息
        await chat_room.broadcast(json.dumps({
            "type": "system",
            "content": f"{username} 进入聊天室",
            "time": datetime.now().strftime("%H:%M:%S"),
            "online_count": chat_room.online_count
        }))
        
        # --- 3. 消息循环：接收并处理消息 ---
        async for raw_msg in websocket:
            try:
                # aiohttp WebSocketResponse 返回的是 WSMessage 对象
                if raw_msg.type != web.WSMsgType.TEXT:
                    if raw_msg.type == web.WSMsgType.CLOSE or raw_msg.type == web.WSMsgType.ERROR:
                        break
                    continue
                
                msg_data = json.loads(raw_msg.data)
                msg_type = msg_data.get("type", "text")
                content = msg_data.get("content", "").strip()
                time_str = datetime.now().strftime("%H:%M:%S")
                
                # 打印消息到控制台（带 IP）
                content_preview = content[:30] + "..." if len(content) > 30 else content
                print(f"[MSG] {username} (IP: {client_ip}): [{msg_type}] {content_preview}")
                
                # 跳过空文本消息
                if not content and msg_type == "text":
                    continue
                
                # 广播消息给所有人
                msg_json = json.dumps({
                    "type": msg_type,
                    "username": username,
                    "content": content,
                    "filename": msg_data.get("filename", ""),
                    "time": time_str
                })
                await chat_room.broadcast(msg_json)
                
                # 保存到数据库
                save_message(username, msg_type, content, time_str)
                
            except json.JSONDecodeError:
                continue  # 忽略无效的 JSON
    
    except Exception as e:
        print(f"WebSocket 错误: {e}")
    finally:
        # --- 4. 用户离开 ---
        if username:
            leave_user = await chat_room.remove_client(websocket)
            if leave_user:
                print(f"[-] {leave_user} 离开 (IP: {client_ip})，当前在线: {chat_room.online_count}")
                await chat_room.broadcast(json.dumps({
                    "type": "system",
                    "content": f"{leave_user} 离开聊天室",
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "online_count": chat_room.online_count
                }))


# ==================== 启动服务 ====================

async def websocket_handler(request):
    """WebSocket 升级处理"""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    # 获取客户端 IP（支持 cpolar 等代理）
    client_ip = get_client_ip(request)
    print(f"[连接] 新的 WebSocket 连接 (IP: {client_ip})")
    
    await handle_chat(ws, client_ip)
    return ws


async def main():
    """启动 HTTP 和 WebSocket 服务"""
    # 初始化数据库
    init_db()
    print("数据库初始化完成")
    
    # 创建 HTTP 应用
    app = web.Application()
    
    # 配置 CORS（允许跨域）
    # app.router.add_route('*', '/ws', websocket_handler)
    
    # 配置路由
    app.router.add_get("/", web_index)
    app.router.add_post("/api/register", api_register)
    app.router.add_post("/api/login", api_login)
    app.router.add_post("/api/logout", api_logout)
    app.router.add_get("/api/check_login", api_check_login)
    app.router.add_get("/api/server_info", api_server_info)  # 服务器地址信息
    app.router.add_get("/api/client_info", api_client_info)  # 客户端IP信息
    app.router.add_post("/api/upload", api_upload)
    app.router.add_get("/api/history", api_history)
    
    # 私聊 API
    app.router.add_post("/api/private/send", api_send_private)  # 发送私聊
    app.router.add_get("/api/private/history", api_get_private_history)  # 私聊记录
    app.router.add_get("/api/private/chats", api_get_chat_list)  # 聊天列表
    app.router.add_get("/api/users", api_get_all_users)  # 用户列表
    
    # 帖子 API
    app.router.add_post("/api/post/create", api_create_post)  # 发布帖子
    app.router.add_get("/api/posts", api_get_posts)  # 帖子列表
    app.router.add_get("/api/post/detail", api_get_post_detail)  # 帖子详情
    app.router.add_post("/api/post/like", api_like_post)  # 点赞帖子
    
    # 评论 API
    app.router.add_post("/api/comment/add", api_add_comment)  # 添加评论
    
    # WebSocket 路由
    app.router.add_get("/ws", websocket_handler)
    
    # 静态文件服务（上传的文件）
    app.router.add_static("/uploads/", UPLOAD_DIR, show_index=True)
    
    # 启动 HTTP 服务
    runner = web.AppRunner(app)
    await runner.setup()
    
    http_site = web.TCPSite(runner, HOST, PORT)
    await http_site.start()
    
    # 计算外部访问地址
    local_base = f"http://localhost:{PORT}"
    local_ws = f"ws://localhost:{PORT}/ws"
    
    public_base = PUBLIC_BASE_URL or local_base
    public_ws = PUBLIC_WS_URL or local_ws
    
    print("=" * 50)
    print("  社区聊天室启动成功！")
    print("-" * 50)
    print(f"  本地访问: {local_base}")
    print(f"  WebSocket: {local_ws}")
    if PUBLIC_BASE_URL:
        print("-" * 50)
        print(f"  外部访问: {public_base}")
        print(f"  外部 WS:  {public_ws}")
    print("=" * 50)
    
    # 保持运行
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())