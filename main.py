import asyncio
import logging
import os
import re
import time
import html
import random
import json
import aiohttp
from datetime import datetime, timedelta

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters,
)
from pyrogram import Client, enums
from pyrogram.errors import (
    SessionPasswordNeeded, FloodWait, UsernameInvalid, 
    UsernameNotOccupied, ChannelPrivate, UserAlreadyParticipant,
    ChannelBanned, PeerIdInvalid, BadRequest, ChatAdminRequired,
    InviteHashExpired, InviteHashInvalid, AuthKeyUnregistered 
)
from pyrogram.raw.functions.messages import CheckChatInvite
from pyrogram.raw.types import ChatInviteAlready, ChatInvite

# ─────────────────────────────────────────
#  CONFIG & INITIALIZATION
# ─────────────────────────────────────────
BOT_TOKEN = "8277915856:AAENwF3ByzZ7FKZ7CWLaxiVqCPtmgciEkQ4"
API_ID    = 28980295
API_HASH  = "c378a9631b9adaf795fe9562c95dbd24"

ADMIN_ID  = 8884734704

# Directories and Files
SESSIONS_DIR  = "sessions"
USERS_FILE = "users.txt"
SCRAPER_STATE_FILE = "scraper_state.json"
STORAGE_STATE_FILE = "storage_state.json"  
CONFIG_FILE = "bot_config.json"

os.makedirs(SESSIONS_DIR, exist_ok=True)

# VPS Logging
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# GLOBAL PYROGRAM BOT INSTANCE
# ─────────────────────────────────────────
PYRO_BOT = Client(
    "main_bot_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir=SESSIONS_DIR,
    no_updates=True
)

# ─────────────────────────────────────────
#  DYNAMIC CONFIGURATION SYSTEM
# ─────────────────────────────────────────
DEFAULT_CONFIG = {
    "STORAGE_CHANNEL_ID": -1004448809511,
    "ACTIVE_CHANNEL_ID": -1004458234660,
    "EXPIRED_CHANNEL_ID": -1003934489318,
    "FORWARD_ON_CHANNEL_ID": -1004340697685,
    "CHATTING_ON_CHANNEL_ID": -1003789944143,
    "SKIPPED_CHANNEL_ID": -1003934489318,
    "MEMBERS_LESS_1000_ID": -1004494600592,
    "MEMBERS_1000_2500_ID": -1003701317207,
    "MEMBERS_2500_5000_ID": -1004320671631,
    "MEMBERS_5000_PLUS_ID": -1004320042078,
    "ADD_MEMBER_TEXT_CHAT_ID": -1004334266609,
    "ADD_MEMBER_MEDIA_CHAT_ID": -1004334266609,
    "EXTRACTOR_UPLOAD_ID": ADMIN_ID
}

CONFIG_NAMES = {
    "STORAGE_CHANNEL_ID": "Storage Channel",
    "ACTIVE_CHANNEL_ID": "Active Links",
    "EXPIRED_CHANNEL_ID": "Expired Links",
    "FORWARD_ON_CHANNEL_ID": "Forward ON",
    "CHATTING_ON_CHANNEL_ID": "Chatting ON",
    "SKIPPED_CHANNEL_ID": "Skipped Links",
    "MEMBERS_LESS_1000_ID": "Members < 1000",
    "MEMBERS_1000_2500_ID": "Members 1K - 2.5K",
    "MEMBERS_2500_5000_ID": "Members 2.5K - 5K",
    "MEMBERS_5000_PLUS_ID": "Members 5000+",
    "ADD_MEMBER_TEXT_CHAT_ID": "Add Member + Text",
    "ADD_MEMBER_MEDIA_CHAT_ID": "Add Member + Media",
    "EXTRACTOR_UPLOAD_ID": "Extractor Upload Target"
}

def load_bot_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                loaded = json.load(f)
                for k, v in DEFAULT_CONFIG.items():
                    if k not in loaded:
                        loaded[k] = v
                return loaded
        except Exception as e:
            logger.error(f"Error loading config: {e}")
    return DEFAULT_CONFIG.copy()

def save_bot_config(config_data):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(config_data, f, indent=4)
    except Exception as e:
        logger.error(f"Error saving config: {e}")

def get_conf(key: str) -> int:
    cfg = load_bot_config()
    return cfg.get(key, DEFAULT_CONFIG.get(key))

# ─────────────────────────────────────────
#  STATE & LOCKS & QUEUES & MEMORY
# ─────────────────────────────────────────
TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
LOGIN_STATE = {} 
CHECKING_LOCKS = {}
USER_QUEUES = {}      
QUEUE_CONTROL = {}    
USER_DELAYS = {}      

SCRAPER_DUPLICATES = {}  
CHECKER_DUPLICATES = {}  

CHECKER_STATE = {}
SCRAPER_TASKS = {}
EXTRACTOR_TASKS = {} 

# ─────────────────────────────────────────
#  JSON STATE LOADERS
# ─────────────────────────────────────────
def load_scraper_state(uid: int) -> dict:
    default_state = {"targets": {}, "auto_run": False, "last_run": 0, "daily_stats": 0}
    try:
        if os.path.exists(SCRAPER_STATE_FILE):
            with open(SCRAPER_STATE_FILE, "r") as f:
                data = json.load(f)
                state = data.get(str(uid), default_state)
                if isinstance(state.get("targets"), list):
                    new_targets = {str(t): 0 for t in state["targets"]}
                    state["targets"] = new_targets
                if "auto_run" not in state: state["auto_run"] = False
                if "last_run" not in state: state["last_run"] = 0
                if "daily_stats" not in state: state["daily_stats"] = 0
                return state
    except Exception as e: 
        logger.error(f"Error loading scraper state: {e}")
    return default_state

def save_scraper_state(uid: int, state: dict):
    try:
        data = {}
        if os.path.exists(SCRAPER_STATE_FILE):
            with open(SCRAPER_STATE_FILE, "r") as f: data = json.load(f)
        data[str(uid)] = state
        with open(SCRAPER_STATE_FILE, "w") as f: json.dump(data, f)
    except Exception as e:
        logger.error(f"Error saving scraper state: {e}")

def load_storage_state(uid: int) -> int:
    try:
        if os.path.exists(STORAGE_STATE_FILE):
            with open(STORAGE_STATE_FILE, "r") as f:
                data = json.load(f)
                return data.get(str(uid), 1)
    except: pass
    return 1

def save_storage_state(uid: int, msg_id: int):
    try:
        data = {}
        if os.path.exists(STORAGE_STATE_FILE):
            with open(STORAGE_STATE_FILE, "r") as f: data = json.load(f)
        data[str(uid)] = msg_id
        with open(STORAGE_STATE_FILE, "w") as f: json.dump(data, f)
    except: pass

def clean_html_text(text: str) -> str:
    if not text: return "Unknown"
    return html.escape(str(text))

def get_user_sessions(uid: int, session_type="checker") -> list:
    sessions = []
    prefix = f"u{uid}_" if session_type == "checker" else f"scraper_{uid}_"
    try:
        for file in os.listdir(SESSIONS_DIR):
            if file.startswith(prefix) and file.endswith(".session"):
                sessions.append(os.path.join(SESSIONS_DIR, file.replace(".session", "")))
    except: pass
    sessions = sorted(sessions, key=lambda x: int(x.split('_')[-1]) if '_' in x else 0)
    return sessions

def get_next_slot(uid: int, session_type="checker") -> int:
    sessions = get_user_sessions(uid, session_type)
    if not sessions: return 1
    slots = []
    for s in sessions:
        try: slots.append(int(s.split('_')[-1]))
        except: pass
    return max(slots) + 1 if slots else 1

async def cleanup_login_state(uid: int):
    if uid in LOGIN_STATE:
        try: await LOGIN_STATE[uid]["app"].disconnect()
        except: pass
        del LOGIN_STATE[uid]

def track_user(uid: int):
    try:
        if not os.path.exists(USERS_FILE): open(USERS_FILE, "w").close()
        with open(USERS_FILE, "r") as f: users = set(f.read().splitlines())
        if str(uid) not in users:
            with open(USERS_FILE, "a") as f: f.write(f"{uid}\n")
    except: pass

def extract_links(text: str) -> list:
    raw = re.findall(r"(?:https?://)?t\.me/(?:joinchat/|\+)?[a-zA-Z0-9_\-+]+", text)
    out = []
    seen = set()
    for lnk in raw:
        lnk = lnk.rstrip("-.,_ \n\t*`~")
        if not lnk.startswith("http"): lnk = "https://" + lnk
        if lnk not in seen and "t.me/" in lnk:
            seen.add(lnk)
            out.append(lnk)
    return out

def parse_link(link: str) -> tuple:
    link = link.strip().rstrip("-.,_ \n\t*`~")
    m = re.search(r"t\.me/(?:joinchat/|\+)([A-Za-z0-9_\-]+)", link)
    if m: return True, m.group(1).rstrip("-")
    m = re.search(r"t\.me/([a-zA-Z0-9_]+)", link)
    if m: return False, m.group(1)
    return False, link

def parse_msg_link(link: str):
    link = link.strip().rstrip("/")
    try:
        if "/c/" in link:
            parts = link.split("/c/")[1].split("/")
            chat_id = int("-100" + parts[0])
            msg_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 1
            return chat_id, msg_id
        elif "t.me/" in link:
            url_part = link.split("t.me/")[1]
            parts = url_part.split("/")
            if parts[0] == "joinchat" or parts[0].startswith("+"):
                return None, None
            chat_username = parts[0].split("?")[0]
            msg_id = 0
            if len(parts) > 1:
                maybe_id = parts[1].split("?")[0]
                if maybe_id.isdigit():
                    msg_id = int(maybe_id)
            return chat_username, msg_id
    except Exception:
        pass
    return None, None

# ─────────────────────────────────────────
#  HTTP LINK CHECKER (FIXED & ULTRA-STRICT METHOD)
# ─────────────────────────────────────────
async def check_public_via_http(link: str, attempt=1) -> dict:
    is_private, _ = parse_link(link)
    result = {"link": link, "status": "unknown", "title": "Unknown", "members": "N/A"}
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }
        async with aiohttp.ClientSession() as sess:
            async with sess.get(link, headers=headers, timeout=10, allow_redirects=True) as r:
                if r.status in [404, 400]:
                    result["status"] = "expired"; result["title"] = "Expired / Not Found"
                    return result
                if r.status == 429:
                    result["status"] = "error"; result["title"] = "Rate Limited"
                    return result
                html_text = await r.text(errors="replace")

        lower_text = html_text.lower()
        
        # 1. BANNED / EXPIRED SIGNATURES (Broad check for dead links)
        dead_phrases = [
            "invite link is invalid", "link is invalid", "has expired", 
            "no longer valid", "not valid", "invalid invite",
            "not found", "user does not exist", "doesn't exist",
            "tgme_page_icon_error", "violated", "copyright", 
            "cannot be displayed", "banned", "inaccessible", 
            "pornographic", "terms of service", "this channel is unavailable"
        ]
        
        if any(phrase in lower_text for phrase in dead_phrases):
            result["status"] = "expired"; result["title"] = "Expired / Invalid"
            return result
        
        # 2. Extract meta title to verify if it's just a default Telegram page
        m_title = re.search(r'<meta property="og:title"\s+content="([^"]+)"', html_text)
        title = m_title.group(1).strip() if m_title else ""
        
        # If title is generic, the link didn't resolve to a real chat/group and redirected
        generic_titles = {"telegram", "telegram messenger", "join group chat on telegram", "join channel on telegram"}
        
        if not title or title.lower() in generic_titles:
            result["status"] = "expired"; result["title"] = "Expired / Invalid"
            return result

        # 3. IF IT PASSED THE ABOVE, CHECK FOR ACTIVE BUTTONS/CONTENT
        if is_private:
            # Additional strict check for private links
            if not bool(re.search(r'tgme_action_button|btn_join|"Join\s+(Group|Channel|Chat)"', html_text, re.I)):
                result["status"] = "expired"; result["title"] = "Expired Link"
            else:
                result["status"] = "active"
                result["title"] = title if title else "Private Chat"
        else:
            m_desc = re.search(r'<meta property="og:description"\s+content="([^"]+)"', html_text)
            desc = m_desc.group(1).strip() if m_desc else ""
            
            mc = re.search(r'class="tgme_page_extra"[^>]*>\s*([\d\s,.\xa0KMB]+)\s*(?:members?|subscribers?)', html_text, re.I)
            if not mc: mc = re.search(r"([\d\s,.\xa0KMB]+)\s*(?:members?|subscribers?)", desc, re.I)
            
            result["status"] = "active"
            result["title"] = title
            if mc: result["members"] = mc.group(1).replace('\xa0', ' ').strip()
            else: result["members"] = "N/A"
        
    except Exception as e:
        result["status"] = "error"; result["title"] = str(e)[:60]
        
    # Double Check Logic for network errors
    if result["status"] in ["error", "unknown"] and attempt == 1:
        await asyncio.sleep(2.0) 
        return await check_public_via_http(link, attempt=2)
        
    return result

async def try_check_link(app: Client, link: str):
    is_private, ref = parse_link(link)
    result = {
        "link": link, "status": "skipped", "title": "Unknown", "username": "N/A",
        "members": "N/A", "videos": "N/A", "photos": "N/A", "forward": "✅ On", 
        "chatting": "❌ Off", "add_member": "❌ Off", "media_only": False
    }
    
    if not is_private: result["username"] = f"@{ref}"
    chat = None
    joined_now = False

    try:
        if not is_private:
            chat = await app.get_chat(ref)
            if getattr(chat, 'type', None) not in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP, enums.ChatType.CHANNEL]:
                raise UsernameInvalid("Not a group or channel")
        else:
            inv = await app.invoke(CheckChatInvite(hash=ref))
            
            if hasattr(inv, 'title'): 
                result["title"] = clean_html_text(inv.title)
            elif hasattr(inv, 'chat') and hasattr(inv.chat, 'title'):
                result["title"] = clean_html_text(inv.chat.title)
                
            if hasattr(inv, 'participants_count'): 
                result["members"] = str(inv.participants_count)
            elif hasattr(inv, 'chat') and hasattr(inv.chat, 'participants_count'):
                result["members"] = str(inv.chat.participants_count)

            if isinstance(inv, ChatInviteAlready):
                try: chat = await app.get_chat(inv.chat.id)
                except: 
                    try: chat = await app.get_chat(int(f"-100{inv.chat.id}"))
                    except: pass
            elif isinstance(inv, ChatInvite):
                retry_count = 0
                while retry_count <= 2:
                    try:
                        chat = await app.join_chat(link)
                        joined_now = True
                        await asyncio.sleep(2) 
                        try: chat = await app.get_chat(chat.id)
                        except: pass
                        break
                    except UserAlreadyParticipant:
                        try: chat = await app.get_chat(link)
                        except: pass
                        break
                    except Exception as inner_e:
                        err_msg = str(inner_e).lower()
                        if "invite_request_sent" in err_msg:
                            result["title"] = result["title"] if result["title"] != "Unknown" else "Admin Approval Required"
                            await asyncio.sleep(1)
                            try:
                                chat = await app.get_chat(link)
                                joined_now = True
                            except Exception:
                                pass
                            break
                        else:
                            retry_count += 1
                            if retry_count == 1:
                                await asyncio.sleep(2)
                            elif retry_count == 2:
                                await asyncio.sleep(random.uniform(2, 5))
                            else:
                                raise inner_e

        result["status"] = "active"
        
        if chat:
            # ─────────────────────────────────────────
            # FIX: STRICT CHECK FOR BANNED/RESTRICTED CHATS
            # ─────────────────────────────────────────
            if getattr(chat, 'is_restricted', False):
                result["status"] = "expired"
                result["title"] = "Banned / Terms of Service"
                return result, False, 0
                
            raw_title = getattr(chat, 'title', None) or getattr(chat, 'first_name', None)
            if raw_title:
                result["title"] = clean_html_text(raw_title)
            
            mem_count = getattr(chat, 'members_count', None)
            if mem_count is None:
                try: mem_count = await app.get_chat_members_count(chat.id)
                except: pass
            if mem_count: result["members"] = str(mem_count)
            
            has_protected = getattr(chat, 'has_protected_content', False)
            result["forward"] = "❌ Off" if has_protected else "✅ On"
            
            if getattr(chat, 'type', None) in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                if chat.permissions:
                    can_txt = chat.permissions.can_send_messages
                    can_med = chat.permissions.can_send_media_messages
                    can_inv = chat.permissions.can_invite_users
                    
                    result["chatting"] = "✅ On" if can_txt else "❌ Off"
                    result["add_member"] = "✅ On" if can_inv else "❌ Off"
                    
                    if can_med and not can_txt:
                        result["media_only"] = True
                else:
                    result["chatting"] = "✅ On" 
                    result["add_member"] = "✅ On"
            elif getattr(chat, 'type', None) == enums.ChatType.CHANNEL:
                result["chatting"] = "❌ Off (Channel)"
                result["add_member"] = "❌ Off (Channel)"

            if joined_now:
                await asyncio.sleep(2) 
                
            for _ in range(2): 
                try: 
                    result["videos"] = str(await app.search_messages_count(chat.id, filter=enums.MessagesFilter.VIDEO))
                    break
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 1)
                except: 
                    await asyncio.sleep(0.5)
                    
            for _ in range(2):
                try: 
                    result["photos"] = str(await app.search_messages_count(chat.id, filter=enums.MessagesFilter.PHOTO))
                    break
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 1)
                except: 
                    await asyncio.sleep(0.5)

        if joined_now and chat:
            await asyncio.sleep(1)
            try: await app.leave_chat(chat.id)
            except: pass

        return result, False, 0

    except FloodWait as e:
        wait_time = getattr(e, 'value', 30)
        return None, True, wait_time
    # ─────────────────────────────────────────
    # FIX: DIRECTLY ROUTE BANNED ERRORS TO EXPIRED
    # ─────────────────────────────────────────
    except ChannelBanned:
        result["status"] = "expired"
        result["title"] = "Banned / Terms of Service"
        return result, False, 0
    except (PeerIdInvalid, ChannelPrivate):
        return None, True, 0
    except (InviteHashExpired, InviteHashInvalid, UsernameInvalid, UsernameNotOccupied):
        result["status"] = "expired"
        result["title"] = "Expired / Invalid"
        return result, False, 0
    except Exception as e:
        err_msg = str(e).lower()
        # FIX: ADDED BAN KEYWORDS TO CATCH ALL VIOLATION EXCEPTIONS
        if "expire" in err_msg or "invalid" in err_msg or "not_occupied" in err_msg or "not a group" in err_msg or "banned" in err_msg or "violated" in err_msg or "restricted" in err_msg:
            result["status"] = "expired"
            result["title"] = "Expired / Invalid"
            return result, False, 0
        elif "invite_request_sent" in err_msg:
            result["status"] = "active"
            result["title"] = result["title"] if result["title"] != "Unknown" else "Admin Approval Required"
            return result, False, 0
        else:
            result["status"] = "skipped"
            result["title"] = f"Error / Skipped"
            return result, True, 0

# ─────────────────────────────────────────
#  INSTANT SENDER & ROUTING
# ─────────────────────────────────────────
async def _send_raw(chat_id: int, text: str, keyboard=None, retries=3):
    if not chat_id or chat_id == 0: return False
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True, "parse_mode": "HTML"}
    if keyboard: payload["reply_markup"] = {"inline_keyboard": keyboard}
    
    async with aiohttp.ClientSession() as s:
        for attempt in range(retries):
            try:
                async with s.post(f"{TG_API}/sendMessage", json=payload) as resp:
                    if resp.status == 200: return await resp.json()
                    elif resp.status == 429:
                        data = await resp.json()
                        await asyncio.sleep(data.get("parameters", {}).get("retry_after", 3) + 0.5)
                    else: await asyncio.sleep(1)
            except: await asyncio.sleep(1)
    return False

async def _pin_message(chat_id: int, message_id: int):
    payload = {"chat_id": chat_id, "message_id": message_id, "disable_notification": True}
    async with aiohttp.ClientSession() as s: await s.post(f"{TG_API}/pinChatMessage", json=payload)

def format_single_message(r: dict) -> str:
    if r['status'] == 'active':
        msg = f"✅ <b>{r.get('title')}</b>\n"
        if r.get('username') != 'N/A': msg += f"👤 <b>Username:</b> {r.get('username')}\n"
        msg += f"👥 <b>Members:</b> <code>{r.get('members')}</code>\n"
        msg += f"🎬 <b>Videos:</b> <code>{r.get('videos')}</code> | 🖼 <b>Photos:</b> <code>{r.get('photos')}</code>\n"
        msg += f"📤 <b>Forward:</b> {r.get('forward')} | 💬 <b>Chat:</b> {r.get('chatting')}\n"
        msg += f"➕ <b>Add Member:</b> {r.get('add_member')}\n🔗 <b>Link:</b> {r['link']}\n"
        return msg
    elif r['status'] == 'skipped':
        return f"⚠️ <b>{r.get('title')}</b>\n👤 <b>Username:</b> {r.get('username')}\n⚠️ <b>Status:</b> Skipped\n🔗 <b>Link:</b> {r['link']}\n"
    else:
        return f"❌ <b>{r.get('title','Expired')}</b>\n👤 <b>Username:</b> {r.get('username')}\n⚠️ <b>Status:</b> <code>Expired</code>\n🔗 <b>Link:</b> {r['link']}\n"

async def dispatch_result(r: dict, stats_tracker: dict):
    msg = format_single_message(r)
    
    if r["status"] == "active":
        await _send_raw(get_conf("ACTIVE_CHANNEL_ID"), f"<b>✅ ACTIVE LINK</b>\n━━━━━━━━━━\n{msg}")
        
        if "✅" in r.get("forward", ""):
            stats_tracker["fwd"] += 1
            await _send_raw(get_conf("FORWARD_ON_CHANNEL_ID"), f"<b>✅ FORWARD ON LINK</b>\n━━━━━━━━━━\n{msg}")
            
        is_chat_on = "✅" in r.get("chatting", "")
        is_add_on = "✅" in r.get("add_member", "")
        is_media_only = r.get("media_only", False)
        
        if is_chat_on:
            stats_tracker["chat"] += 1
            await _send_raw(get_conf("CHATTING_ON_CHANNEL_ID"), f"<b>💬 CHATTING ON LINK</b>\n━━━━━━━━━━\n{msg}")
            try:
                m_count = int(r.get("members", 0)) if r.get("members") != "N/A" else 0
                if m_count < 1000:
                    await _send_raw(get_conf("MEMBERS_LESS_1000_ID"), f"<b>👥 < 1000 MEMBERS (CHAT ON)</b>\n━━━━━━━━━━\n{msg}")
                elif 1000 <= m_count <= 2500:
                    await _send_raw(get_conf("MEMBERS_1000_2500_ID"), f"<b>👥 1000-2500 MEMBERS (CHAT ON)</b>\n━━━━━━━━━━\n{msg}")
                elif 2500 < m_count <= 5000:
                    await _send_raw(get_conf("MEMBERS_2500_5000_ID"), f"<b>👥 2500-5000 MEMBERS (CHAT ON)</b>\n━━━━━━━━━━\n{msg}")
                elif m_count > 5000:
                    await _send_raw(get_conf("MEMBERS_5000_PLUS_ID"), f"<b>👥 5000+ MEMBERS (CHAT ON)</b>\n━━━━━━━━━━\n{msg}")
            except Exception:
                pass
                
        if is_add_on:
            if is_chat_on:
                stats_tracker["add_chat"] += 1
                await _send_raw(get_conf("ADD_MEMBER_TEXT_CHAT_ID"), f"<b>➕ ADD MEMBER & TEXT CHAT ON</b>\n━━━━━━━━━━\n{msg}")
            elif is_media_only:
                stats_tracker.setdefault("add_media", 0)
                stats_tracker["add_media"] += 1
                await _send_raw(get_conf("ADD_MEMBER_MEDIA_CHAT_ID"), f"<b>➕ ADD MEMBER & MEDIA ONLY ON</b>\n━━━━━━━━━━\n{msg}")
                
    elif r["status"] == "expired":
        await _send_raw(get_conf("EXPIRED_CHANNEL_ID"), f"<b>❌ EXPIRED LINK</b>\n━━━━━━━━━━\n{msg}")
    elif r["status"] == "skipped":
        await _send_raw(get_conf("SKIPPED_CHANNEL_ID"), f"<b>⚠️ SKIPPED LINK</b>\n━━━━━━━━━━\n{msg}")

# ─────────────────────────────────────────
#  MEDIA EXTRACTOR
# ─────────────────────────────────────────
async def process_and_send_media(app, bot_uploader, msg, dest_id, cid, status_msg_id=None):
    if not msg or msg.empty: return False
    
    caption = msg.caption or ""
    if not caption and msg.text:
        caption = msg.text
        
    def get_media_property(m):
        for attr in ['video', 'document', 'photo', 'audio', 'animation', 'voice']:
            val = getattr(m, attr, None)
            if val: return val
        return None
        
    media_obj = get_media_property(msg)
            
    if not media_obj:
        if caption:
            try: 
                await bot_uploader.send_message(dest_id, text=caption)
                return True
            except PeerIdInvalid:
                logger.warning("PeerIdInvalid caught for text. Falling back to direct raw API...")
                safe_caption = html.escape(caption)
                await _send_raw(dest_id, safe_caption)
                return True
            except Exception as e: 
                logger.error(f"Bot failed to send text: {e}")
        return False
        
    if status_msg_id: 
        await _edit_raw(cid, status_msg_id, "📥 <b>Downloading media to VPS...</b>\n<i>Please wait...</i>")
    
    file_path = None
    try: 
        file_path = await app.download_media(msg)
    except Exception as e: 
        logger.error(f"Download Error: {e}")
        pass
        
    if not file_path or not os.path.exists(file_path):
        return False
        
    if status_msg_id: 
        await _edit_raw(cid, status_msg_id, "📤 <b>Uploading media to destination via Bot...</b>")
    
    success = False
    try:
        if getattr(msg, 'video', None): await bot_uploader.send_video(dest_id, video=file_path, caption=caption)
        elif getattr(msg, 'document', None): await bot_uploader.send_document(dest_id, document=file_path, caption=caption)
        elif getattr(msg, 'photo', None): await bot_uploader.send_photo(dest_id, photo=file_path, caption=caption)
        elif getattr(msg, 'audio', None): await bot_uploader.send_audio(dest_id, audio=file_path, caption=caption)
        elif getattr(msg, 'animation', None): await bot_uploader.send_animation(dest_id, animation=file_path, caption=caption)
        elif getattr(msg, 'voice', None): await bot_uploader.send_voice(dest_id, voice=file_path, caption=caption)
        else: await bot_uploader.send_document(dest_id, document=file_path, caption=caption)
        success = True
    except Exception as e:
        err_str = str(e).lower()
        if "is_premium" in err_str or "nonetype" in err_str:
            logger.warning(f"Ignored Telegram 'is_premium' bug. File uploaded successfully.")
            success = True
        else:
            logger.error(f"Bot upload failed: {e}")
            success = False
        
    if os.path.exists(file_path):
        try: os.remove(file_path)
        except: pass
        
    return success

async def _run_media_extractor(uid: int, cid: int, target_link: str, mode: str, dest_id: int):
    scraper_sessions = get_user_sessions(uid, "scraper")
    if not scraper_sessions:
        await _send_raw(cid, "❌ <b>No Scraper ID logged in!</b>\nPlease add an account in Link Pro -> Scraper Menu first.")
        return

    chat_id, start_msg_id = parse_msg_link(target_link)
    if not chat_id:
        await _send_raw(cid, "❌ <b>Invalid Post Link!</b>\nPlease provide a direct link to a message or bot (e.g., t.me/c/12345/67 or Bot Link)")
        return

    app = Client(scraper_sessions[0], api_id=API_ID, api_hash=API_HASH, no_updates=True)
    
    prog_resp = await _send_raw(cid, "🔄 <b>Connecting Scraper ID to extract media...</b>")
    status_msg_id = prog_resp.get("result", {}).get("message_id") if isinstance(prog_resp, dict) else None

    try:
        try:
            await app.connect()
        except AuthKeyUnregistered:
            await _edit_raw(cid, status_msg_id, "❌ <b>Scraper Session Expired!</b>\nYour Scraper ID was logged out or revoked by Telegram. Please delete it and login again.")
            try: os.remove(scraper_sessions[0] + ".session")
            except: pass
            EXTRACTOR_TASKS[uid] = False
            return

        try:
            await PYRO_BOT.get_chat(dest_id)
        except Exception as cache_err:
            logger.warning(f"Could not pre-fetch dest_id {dest_id} for bot caching: {cache_err}. Direct raw API will handle texts.")

        try:
            chat = await app.get_chat(chat_id)
        except Exception as e:
            await _edit_raw(cid, status_msg_id, f"❌ <b>Scraper ID cannot access this chat!</b>\nError: {e}\n<i>Make sure the Scraper ID has joined the group/channel/bot.</i>")
            return

        if "?start=" in target_link:
            try:
                payload = target_link.split("?start=")[1].split("&")[0]
                sent_msg = await app.send_message(chat.id, f"/start {payload}")
                await _edit_raw(cid, status_msg_id, f"🤖 <b>Bot Deep Link Detected!</b>\nSent <code>/start {payload}</code> to {chat.first_name or chat.username}.\n<i>Waiting for bot to load media...</i>")
                await asyncio.sleep(8) 
                start_msg_id = sent_msg.id
                mode = "bulk" 
            except Exception as e:
                logger.error(f"Failed to send start payload: {e}")

        if mode == "single":
            try:
                msg = await app.get_messages(chat.id, start_msg_id)
            except Exception as single_e:
                await _edit_raw(cid, status_msg_id, f"❌ <b>Error fetching message:</b> {single_e}")
                return
                
            success = await process_and_send_media(app, PYRO_BOT, msg, dest_id, cid, status_msg_id)
            if success:
                await _edit_raw(cid, status_msg_id, f"✅ <b>Successfully Extracted Single Post!</b>\nSent to Target ID: <code>{dest_id}</code>")
            else:
                await _edit_raw(cid, status_msg_id, "❌ <b>Failed to extract media.</b> (Bot must be Admin in target channel, or message is unsupported)")

        elif mode == "bulk":
            await _edit_raw(cid, status_msg_id, f"🔄 <b>Starting Bulk Extraction...</b>\nFrom Message ID: <code>{start_msg_id}</code> onwards.\n<i>Sending to: {dest_id}</i>")
            
            extracted_count = 0
            current_msg_id = start_msg_id
            empty_patches = 0
            chunk_size = 20
            
            while True:
                try:
                    msgs = []
                    try:
                        msgs = await app.get_messages(chat.id, list(range(current_msg_id, current_msg_id + chunk_size)))
                    except Exception as chunk_e:
                        err_str = str(chunk_e).lower()
                        if "is_premium" in err_str or "nonetype" in err_str:
                            for single_id in range(current_msg_id, current_msg_id + chunk_size):
                                try:
                                    m = await app.get_messages(chat.id, single_id)
                                    if m: msgs.append(m)
                                except: pass
                        else:
                            raise chunk_e

                    valid_msgs = [m for m in msgs if m and not m.empty]
                    
                    if not valid_msgs:
                        empty_patches += 1
                        if empty_patches > 20: 
                            break
                    else:
                        empty_patches = 0

                    for msg in valid_msgs:
                        if msg.id >= current_msg_id:
                            if msg.text and msg.text.startswith("/start"): continue
                            success = await process_and_send_media(app, PYRO_BOT, msg, dest_id, cid, None)
                            if success:
                                extracted_count += 1
                                if extracted_count % 3 == 0:
                                    await _edit_raw(cid, status_msg_id, f"🔄 <b>Bulk Extraction in Progress...</b>\nExtracted: <code>{extracted_count}</code> posts so far.")
                            await asyncio.sleep(2.5) 
                    
                    current_msg_id += chunk_size
                    
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 5)
                except Exception as e:
                    break

            await _edit_raw(cid, status_msg_id, f"✅ <b>Bulk Extraction Complete!</b>\nTotal Extracted: <code>{extracted_count}</code> posts.\nSent to: <code>{dest_id}</code>")

    except Exception as e:
        if status_msg_id: await _edit_raw(cid, status_msg_id, f"❌ <b>Extraction Crashed:</b> {e}")
        logger.error(f"Extractor Error: {e}")
    finally:
        try: await app.disconnect()
        except: pass
        EXTRACTOR_TASKS[uid] = False

# ─────────────────────────────────────────
#  SCRAPER & AUTO-UPDATES
# ─────────────────────────────────────────
async def _run_daily_scraper_task(uid: int, cid: int, state: dict, manual=True):
    scraper_sessions = get_user_sessions(uid, "scraper")
    if not scraper_sessions:
        if manual: await _send_raw(cid, "❌ No Scraper IDs logged in! Please add an account in Scraper Menu.")
        return

    targets = state.get("targets", {})
    if not targets:
        if manual: await _send_raw(cid, "❌ No Targets set! Please Add Target first.")
        return

    scraper_path = scraper_sessions[0]
    app = Client(scraper_path, api_id=API_ID, api_hash=API_HASH, no_updates=True)
    
    try:
        print(f"[{datetime.now()}] 🚀 Connecting Scraper for Deep Scrape...")
        try:
            await app.connect()
        except AuthKeyUnregistered:
            if manual: await _send_raw(cid, "❌ <b>Scraper Session Expired!</b>\nYour ID was logged out by Telegram. Please delete it and login again.")
            try: os.remove(scraper_path + ".session")
            except: pass
            SCRAPER_TASKS[uid] = "stopped"
            return
            
        total_extracted = 0
        if uid not in SCRAPER_DUPLICATES: SCRAPER_DUPLICATES[uid] = set()
        SCRAPER_DUPLICATES[uid].clear() 

        if manual:
            prog_resp = await _send_raw(cid, f"🔄 <b>Starting Deep Scrape from {len(targets)} Targets...</b>\n<i>(Extracting links, tracking progress)</i>")
            prog_msg_id = prog_resp.get("result", {}).get("message_id") if isinstance(prog_resp, dict) else None
        
        for target, last_msg_id in targets.items():
            print(f"[{datetime.now()}] 📡 Resolving & Scraping target: {target}")
            try:
                chat_target = target
                try: 
                    chat_target = int(target)
                except ValueError: 
                    pass

                chat = None
                if isinstance(chat_target, str) and ("t.me/+" in chat_target or "joinchat" in chat_target):
                    try:
                        chat = await app.join_chat(chat_target)
                    except UserAlreadyParticipant:
                        chat = await app.get_chat(chat_target)
                elif isinstance(chat_target, str) and "t.me/" in chat_target:
                    if "/c/" in chat_target:
                        parts = chat_target.split("/c/")[1].split("/")
                        chat_id = int("-100" + parts[0])
                        chat = await app.get_chat(chat_id)
                    else:
                        username = chat_target.split("t.me/")[1].split("/")[0].split("?")[0]
                        chat = await app.get_chat(username)
                else:
                    chat = await app.get_chat(chat_target)

                if not chat:
                    print(f"[{datetime.now()}] ⚠️ Could not resolve chat for target {target}")
                    continue

                chunk_links = []
                new_last_id = last_msg_id
                
                async for msg in app.get_chat_history(chat.id):
                    if msg.id <= last_msg_id:
                        break
                    
                    if msg.id > new_last_id:
                        new_last_id = msg.id
                        
                    text = (msg.text or msg.caption or "")
                    links = extract_links(text)
                    
                    if links:
                        for l in links:
                            if l not in SCRAPER_DUPLICATES[uid]:
                                chunk_links.append(l)
                                SCRAPER_DUPLICATES[uid].add(l)
                                total_extracted += 1
                
                targets[target] = new_last_id
                
                if chunk_links:
                    for i in range(0, len(chunk_links), 50): 
                        send_chunk = chunk_links[i:i+50]
                        text_to_send = "\n".join(send_chunk)
                        try:
                            await _send_raw(get_conf("STORAGE_CHANNEL_ID"), text_to_send)
                        except: pass
                        await asyncio.sleep(1)
                        
            except FloodWait as e:
                print(f"[{datetime.now()}] ⚠️ FloodWait: Sleeping for {e.value}s")
                await asyncio.sleep(e.value + 2)
            except Exception as e:
                logger.error(f"Scraper error on target {target}: {e}")

        await app.disconnect()
        
        state["last_run"] = time.time()
        state["daily_stats"] += total_extracted
        state["targets"] = targets
        save_scraper_state(uid, state)

        print(f"[{datetime.now()}] ✅ Scraping completed! Total: {total_extracted}")
        msg_done = f"✅ <b>Scraping Complete!</b>\nExtracted <code>{total_extracted}</code> links.\n\n"
        
        checker_sessions = get_user_sessions(uid, "checker")
        if checker_sessions and total_extracted > 0:
            msg_done += "🤖 <i>Auto-starting Link Processing Queue from Storage...</i>"
            if manual: await _send_raw(cid, msg_done)
            
            if not CHECKING_LOCKS.get(uid):
                CHECKING_LOCKS[uid] = True
                asyncio.create_task(_run_bulk_check(uid, cid, checker_sessions, auto_storage=True))
        else:
            if total_extracted == 0:
                msg_done += "No new links found since last check."
            else:
                msg_done += "⚠️ No Checker Accounts in Bank! Please login to Checker Bank to process them."
            if manual: await _send_raw(cid, msg_done)

    except Exception as e:
        print(f"[{datetime.now()}] ❌ Scraper crashed: {e}")
        if manual: await _send_raw(cid, f"❌ Scraper crashed: {e}")
        try: await app.disconnect()
        except: pass
        
    SCRAPER_TASKS[uid] = "stopped"

async def auto_scraper_loop():
    while True:
        try:
            if os.path.exists(USERS_FILE):
                with open(USERS_FILE, "r") as f:
                    users = f.read().splitlines()
                
                for uid_str in users:
                    uid = int(uid_str)
                    state = load_scraper_state(uid)
                    
                    if state.get("auto_run", False):
                        last_run = state.get("last_run", 0)
                        if (time.time() - last_run) >= (12 * 3600): 
                            if SCRAPER_TASKS.get(uid) != "running":
                                print(f"[{datetime.now()}] 🔄 Starting Auto-Scrape for UID: {uid}")
                                SCRAPER_TASKS[uid] = "running"
                                asyncio.create_task(_run_daily_scraper_task(uid, uid, state, manual=False))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Auto Scraper Loop Error: {e}")
            
        await asyncio.sleep(3600)

# ─────────────────────────────────────────
#  NON-BLOCKING DASHBOARD UPDATER
# ─────────────────────────────────────────
async def _update_dashboard_if_needed(uid: int, force=False):
    state = CHECKER_STATE.get(uid)
    if not state or not state.get("dash_msg_id"): return
    
    now = time.time()
    if not force and (now - state["last_update"] < 5.0):
        return
        
    state["last_update"] = now
    stats = state["stats"]
    queue_left = len(USER_QUEUES.get(uid, []))
    
    elapsed = now - state["start_time"]
    if stats['processed'] > 0 and queue_left > 0:
        avg_time_per_link = elapsed / stats['processed']
        eta_seconds = int(avg_time_per_link * queue_left)
        m, s = divmod(eta_seconds, 60)
        h, m = divmod(m, 60)
        eta_str = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"
    elif queue_left == 0: eta_str = "Fetching/Done"
    else: eta_str = "Calculating..."

    perf_strs = []
    for c_key, c_data in state["clients"].items():
        if not c_data["enabled"]: status = "🔴 (Off)"
        elif c_data["ready_at"] > now:
            fw_left = int(c_data["ready_at"] - now)
            status = f"⏳ FW({fw_left}s)"
        else: status = "🟢"
        perf_strs.append(f"{c_data['name']}: {c_data['checks']} {status}")
    
    perf_text = "\n".join(perf_strs)
    last_res = state.get("last_result", {})
    last_checked_text = f"<i>{last_res.get('title', 'None')}</i>" if last_res else "<i>Starting...</i>"

    dash_text = (
        f"<b>⚡ LIVE QUEUE DASHBOARD ⚡</b>\n"
        f"📊 <b>Processed:</b> <code>{stats['processed']}</code> | <b>In Memory Queue:</b> <code>{queue_left}</code>\n"
        f"⏳ <b>Estimated Time Left:</b> <code>{eta_str}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Active: <code>{stats['active']}</code>\n"
        f"❌ Expired: <code>{stats['expired']}</code>\n"
        f"⚠️ Skipped: <code>{stats['skipped']}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📤 Forward On: <code>{stats['fwd']}</code> | 💬 Chat On: <code>{stats['chat']}</code>\n"
        f"➕ Add Mem(Txt): <code>{stats['add_chat']}</code> | Add Mem(Media): <code>{stats.get('add_media', 0)}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📱 <b>ID Status & Performance:</b>\n<code>{perf_text}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>Just Checked:</b>\n{last_checked_text}\n"
    )

    kb = []
    row = []
    for c_key, c_data in state["clients"].items():
        btn_icon = "🟢" if c_data["enabled"] else "🔴"
        row.append({"text": f"{btn_icon} {c_data['name']}", "callback_data": f"tog_id_{c_key}"})
        if len(row) == 2:
            kb.append(row)
            row = []
    if row: kb.append(row)

    if QUEUE_CONTROL.get(uid) == "running":
        kb.append([{"text": "⏸️ Pause", "callback_data": "queue_pause"}, {"text": "🛑 Stop", "callback_data": "queue_stop"}])
    else:
        kb.append([{"text": "▶️ Resume", "callback_data": "queue_resume"}, {"text": "🛑 Stop", "callback_data": "queue_stop"}])

    try:
        payload = {"chat_id": state["cid"], "message_id": state["dash_msg_id"], "text": dash_text, "parse_mode": "HTML", "disable_web_page_preview": True, "reply_markup": {"inline_keyboard": kb}}
        async with aiohttp.ClientSession() as s: await s.post(f"{TG_API}/editMessageText", json=payload)
    except: pass

# ─────────────────────────────────────────
#  BULK RUNNER WITH QUEUE 
# ─────────────────────────────────────────
async def _run_bulk_check(uid: int, cid: int, sessions: list, auto_storage=False):
    QUEUE_CONTROL[uid] = "running"
    print(f"[{datetime.now()}] 🔄 Initializing Bulk Check Queue for UID: {uid}")
    
    clients_dict = {}
    for idx, s_path in enumerate(sessions):
        try:
            app = Client(s_path, api_id=API_ID, api_hash=API_HASH, no_updates=True)
            try:
                await app.connect()
            except AuthKeyUnregistered:
                print(f"[{datetime.now()}] ❌ Checker ID {s_path} is Unregistered/Expired. Removing it.")
                try: os.remove(s_path + ".session")
                except: pass
                continue
                
            slot = str(s_path.split('_')[-1] if '_' in s_path else idx + 1)
            clients_dict[slot] = {"app": app, "ready_at": 0, "name": f"ID {slot}", "checks": 0, "enabled": True}
        except Exception as e: 
            print(f"[{datetime.now()}] ❌ Failed to connect Checker ID {s_path}: {e}")

    if not clients_dict:
        await _send_raw(cid, "❌ Failed to connect any of your logged-in IDs. Please check Account Bank.")
        CHECKING_LOCKS[uid] = False
        return

    CHECKER_STATE[uid] = {
        "clients": clients_dict,
        "stats": {"active": 0, "expired": 0, "skipped": 0, "processed": 0, "fwd": 0, "chat": 0, "add_chat": 0, "add_media": 0},
        "start_time": time.time(),
        "last_update": 0,
        "dash_msg_id": None,
        "cid": cid,
        "last_result": None
    }

    dash_resp = await _send_raw(cid, "⏳ <b>Starting Live Processing Queue...</b>")
    dash_msg_id = dash_resp.get("result", {}).get("message_id") if isinstance(dash_resp, dict) else None
    CHECKER_STATE[uid]["dash_msg_id"] = dash_msg_id

    current_pinned_msg_id = None  
    client_keys = list(clients_dict.keys())
    client_idx = 0

    if uid not in USER_QUEUES: USER_QUEUES[uid] = []
    if uid not in CHECKER_DUPLICATES: CHECKER_DUPLICATES[uid] = set()

    storage_last_msg_id = load_storage_state(uid) if auto_storage else 1
    empty_storage_batches = 0

    while True:
        try:
            if QUEUE_CONTROL.get(uid) == "stopped": break
            if QUEUE_CONTROL.get(uid) == "paused":
                await _update_dashboard_if_needed(uid)
                await asyncio.sleep(1)
                continue

            if not USER_QUEUES.get(uid):
                if not auto_storage: 
                    break 
                else:
                    fetched_msg = None
                    messages_received = 0
                    try:
                        c_app = CHECKER_STATE[uid]["clients"][client_keys[0]]["app"]
                        msg_ids_to_fetch = list(range(storage_last_msg_id, storage_last_msg_id + 50))
                        messages = await c_app.get_messages(get_conf("STORAGE_CHANNEL_ID"), msg_ids_to_fetch)
                        
                        links_found_in_batch = False
                        
                        for msg in messages:
                            if not msg or msg.empty: continue
                            messages_received += 1
                            links = extract_links(msg.text or msg.caption or "")
                            
                            for l in links:
                                if l not in CHECKER_DUPLICATES[uid]:
                                    USER_QUEUES[uid].append({"link": l, "message_id": msg.id, "chat_id": get_conf("STORAGE_CHANNEL_ID")})
                                    CHECKER_DUPLICATES[uid].add(l)
                                    links_found_in_batch = True
                                    if not fetched_msg: 
                                        fetched_msg = msg
                        
                        storage_last_msg_id += 50
                        save_storage_state(uid, storage_last_msg_id)
                        
                        if links_found_in_batch:
                            empty_storage_batches = 0
                            if fetched_msg:
                                current_pinned_msg_id = fetched_msg.id
                                await _pin_message(get_conf("STORAGE_CHANNEL_ID"), fetched_msg.id)
                        else:
                            empty_storage_batches += 1
                            
                    except Exception as e:
                        pass
                    
                    if not USER_QUEUES.get(uid):
                        await _update_dashboard_if_needed(uid, force=True)
                        if empty_storage_batches > 20: 
                            await _send_raw(cid, "✅ <b>Storage Checking Paused/Finished.</b>\nReached the end of available messages in Storage. Will re-check soon if resumed.")
                            break
                        
                        if messages_received == 0:
                            await asyncio.sleep(5) 
                        else:
                            await asyncio.sleep(0.1)
                        continue

            item = USER_QUEUES[uid].pop(0)
            lnk = item["link"]
            msg_id = item.get("message_id")
            chat_id = item.get("chat_id")
            
            print(f"[{datetime.now()}] 🔍 Checking link: {lnk}")

            if not auto_storage and msg_id and msg_id != current_pinned_msg_id:
                if chat_id: await _pin_message(chat_id, msg_id)
                else: await _pin_message(cid, msg_id)
                current_pinned_msg_id = msg_id

            fast_checked_expired = False
            http_res = await check_public_via_http(lnk)
            
            if http_res.get("status") in ["expired", "error"]:
                final_result = {
                    "link": lnk, "status": "expired", "title": http_res.get("title", "Expired / Invalid"),
                    "username": "N/A", "members": "N/A", "videos": "N/A", "photos": "N/A",
                    "forward": "N/A", "chatting": "❌ Off", "add_member": "❌ Off", "media_only": False
                }
                fast_checked_expired = True
            else:
                current_time = time.time()
                selected_key = None
                
                for _ in range(len(client_keys)):
                    k = client_keys[client_idx % len(client_keys)]
                    client_idx += 1
                    c_data = CHECKER_STATE[uid]["clients"][k]
                    if c_data["enabled"] and c_data["ready_at"] <= current_time:
                        selected_key = k
                        break
                
                if not selected_key:
                    USER_QUEUES[uid].insert(0, item) 
                    await _update_dashboard_if_needed(uid)
                    await asyncio.sleep(1) 
                    continue

                c_data = CHECKER_STATE[uid]["clients"][selected_key]
                res, retry_needed, fw_time = await try_check_link(c_data["app"], lnk)
                
                if fw_time > 0:
                    c_data["ready_at"] = time.time() + fw_time
                    USER_QUEUES[uid].insert(0, item) 
                    await _update_dashboard_if_needed(uid)
                    continue 
                    
                final_result = res if res else {"link": lnk, "status": "skipped", "title": "Unknown Error"}
                if final_result["status"] == "active":
                    c_data["checks"] += 1
            
            stats = CHECKER_STATE[uid]["stats"]
            stats["processed"] += 1
            if final_result["status"] == "active": stats["active"] += 1
            elif final_result["status"] == "expired": stats["expired"] += 1
            else: stats["skipped"] += 1
            
            CHECKER_STATE[uid]["last_result"] = final_result
            asyncio.create_task(dispatch_result(final_result, stats))
            await _update_dashboard_if_needed(uid)

            if final_result["status"] == "active" and not fast_checked_expired:
                min_del, max_del = USER_DELAYS.get(uid, (10.0, 15.0))
                delay = random.uniform(min_del, max_del)
                start_delay = time.time()
                
                while time.time() - start_delay < delay:
                    if QUEUE_CONTROL.get(uid) in ["stopped", "paused"]: break
                    await _update_dashboard_if_needed(uid)
                    await asyncio.sleep(1) 

        except Exception as e:
            logger.error(f"Error in queue loop: {e}")
            await asyncio.sleep(1) 

    for c_data in CHECKER_STATE[uid]["clients"].values():
        try: await c_data["app"].disconnect()
        except: pass

    if not auto_storage and uid in CHECKER_DUPLICATES:
        CHECKER_DUPLICATES[uid].clear()

    stats = CHECKER_STATE[uid]["stats"]
    perf_strs = [f"{v['name']}: {v['checks']}" for v in CHECKER_STATE[uid]["clients"].values()]
    perf_text = " | ".join(perf_strs)

    status_title = "🛑 QUEUE STOPPED BY USER" if QUEUE_CONTROL.get(uid) == "stopped" else "✨ QUEUE PROCESSING COMPLETED ✨"
    
    final_msg = (f"<b>{status_title}</b>\n\n"
                 f"📊 Total Checked: {stats['processed']}\n"
                 f"✅ Active: <code>{stats['active']}</code> | ❌ Expired: <code>{stats['expired']}</code> | ⚠️ Skipped: <code>{stats['skipped']}</code>\n"
                 f"📤 Fwd On: <code>{stats['fwd']}</code> | 💬 Chat On: <code>{stats['chat']}</code> | ➕ Add Mem(Txt): <code>{stats['add_chat']}</code> | Add Mem(Media): <code>{stats.get('add_media', 0)}</code>\n\n"
                 f"📱 <b>Final ID Performance:</b>\n<code>{perf_text}</code>")
    
    keyboard = [[{"text": "🔙 Main Menu", "callback_data": "menu_pro"}]]
    if dash_msg_id:
        try:
            payload = {"chat_id": cid, "message_id": dash_msg_id, "text": final_msg, "parse_mode": "HTML", "reply_markup": {"inline_keyboard": keyboard}}
            async with aiohttp.ClientSession() as s: await s.post(f"{TG_API}/editMessageText", json=payload)
        except: await _send_raw(cid, final_msg, keyboard)
    else:
        await _send_raw(cid, final_msg, keyboard)

    CHECKING_LOCKS[uid] = False
    print(f"[{datetime.now()}] ✅ Bulk Check Queue Completed/Stopped for UID: {uid}")

# ─────────────────────────────────────────
#  UI & UTILS
# ─────────────────────────────────────────
async def _edit_raw(chat_id, message_id, text, keyboard=None):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if keyboard: payload["reply_markup"] = {"inline_keyboard": keyboard}
    async with aiohttp.ClientSession() as s: await s.post(f"{TG_API}/editMessageText", json=payload)

def START_KB():
    return [
        [{"text": "🔍 Normal Link Checker", "callback_data": "menu_guest"}],
        [{"text": "👑 Link Checker Pro (Advanced)", "callback_data": "menu_pro"}],
        [{"text": "🎥 Media Extractor (Bot Recover)", "callback_data": "menu_extractor_main"}]
    ]

def PRO_KB(uid):
    checker_sessions = get_user_sessions(uid, "checker")
    scraper_sessions = get_user_sessions(uid, "scraper")
    return [
        [{"text": f"🏦 Checker Bank ({len(checker_sessions)} Active)", "callback_data": "menu_accounts"}],
        [{"text": f"🕷️ Scraper Accounts & Targets ({len(scraper_sessions)} Active)", "callback_data": "menu_scraper"}],
        [{"text": "📥 Trigger Smart Scrape Now", "callback_data": "scraper_today"}],
        [{"text": "🔗 Check Links (Manual Mode)", "callback_data": "menu_check"}],
        [{"text": "⚙️ Checker Settings (Delay)", "callback_data": "menu_settings"}],
        [{"text": "⚙️ Channel Configurations", "callback_data": "menu_config_checker"}],
        [{"text": "🔙 Back", "callback_data": "back_start"}]
    ]

def EXTRACTOR_KB():
    return [
        [{"text": "📌 Extract Single Post", "callback_data": "ext_single"}],
        [{"text": "📚 Extract Bulk (All below link)", "callback_data": "ext_bulk"}],
        [{"text": "⚙️ Set Target/Upload Channel", "callback_data": "menu_config_extractor"}],
        [{"text": "🔙 Back", "callback_data": "back_start"}]
    ]

# ─────────────────────────────────────────
#  HANDLERS
# ─────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    track_user(uid)
    ctx.user_data["mode"] = ""
    await cleanup_login_state(uid)
    text = f"👋 <b>Welcome {update.effective_user.first_name}</b>\n\nAdvanced Link Checker & Extractor Bot.\nChoose an option below:"
    await _send_raw(update.effective_chat.id, text, START_KB())

async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; d = q.data; uid = q.from_user.id; cid = q.message.chat.id; mid = q.message.message_id
    track_user(uid)
    
    try:
        async with aiohttp.ClientSession() as s: 
            await s.post(f"{TG_API}/answerCallbackQuery", json={"callback_query_id": q.id})
    except: pass

    if d == "back_start":
        await cleanup_login_state(uid)
        ctx.user_data["mode"] = ""
        await _edit_raw(cid, mid, "👋 <b>Welcome!</b>\nChoose an option below:", START_KB())

    elif d == "menu_guest":
        ctx.user_data["mode"] = "guest_check"
        await _edit_raw(cid, mid, "🔍 <b>Guest Mode (No Account Needed)</b>\n\nJust send me any links here. I will do a strict scan and tell you if they are Active or Expired.\n\n<i>Note: This mode doesn't check Members, Photos, Chat features etc.</i>", [[{"text": "🔙 Back", "callback_data": "back_start"}]])

    elif d == "menu_pro":
        ctx.user_data["mode"] = ""
        state = load_scraper_state(uid)
        daily_stats = state.get("daily_stats", 0)
        await _edit_raw(cid, mid, f"👑 <b>Link Checker Pro</b>\n\n📊 <b>Scraping Status Today:</b> {daily_stats} Links Extracted\n\nManage Checkers, Scrapers and Channels here.", PRO_KB(uid))

    elif d == "menu_extractor_main":
        ctx.user_data["mode"] = ""
        await _edit_raw(cid, mid, "🎥 <b>Restricted Media Extractor</b>\n\nUse this to download restricted media from Bots, Groups or Channels and upload them to your set Target Channel.\n\n<i>Requires Scraper ID to be logged in (via Link Pro menu).</i>", EXTRACTOR_KB())

    elif d == "ext_single":
        ctx.user_data["mode"] = "ext_single_wait"
        await _edit_raw(cid, mid, "📌 <b>Single Post Extraction</b>\n\nSend the exact message link (e.g. `t.me/c/123456/789`).\n\n<i>Make sure your Scraper ID has joined the target group/bot!</i>", [[{"text": "🔙 Cancel", "callback_data": "menu_extractor_main"}]])

    elif d == "ext_bulk":
        ctx.user_data["mode"] = "ext_bulk_wait"
        await _edit_raw(cid, mid, "📚 <b>Bulk Post Extraction</b>\n\nSend the STARTING message link.\nBot will extract that message and ALL messages posted after it sequentially.\n\n<i>Make sure your Scraper ID has joined!</i>", [[{"text": "🔙 Cancel", "callback_data": "menu_extractor_main"}]])

    elif d == "menu_config_checker":
        kb = []
        cfg = load_bot_config()
        checker_keys = [k for k in CONFIG_NAMES.keys() if k != "EXTRACTOR_UPLOAD_ID"]
        for k in checker_keys:
            val_str = str(cfg.get(k, DEFAULT_CONFIG[k]))
            if len(val_str) > 10: val_str = val_str[:4] + ".." + val_str[-4:]
            name = CONFIG_NAMES[k]
            kb.append([{"text": f"{name}: {val_str}", "callback_data": f"setcfg_{k}"}])
        kb.append([{"text": "🔙 Back", "callback_data": "menu_pro"}])
        await _edit_raw(cid, mid, "⚙️ <b>Channel Configurations (Checker)</b>\n\nClick any button below to change its Channel ID.", kb)

    elif d == "menu_config_extractor":
        cfg = load_bot_config()
        val = cfg.get("EXTRACTOR_UPLOAD_ID", DEFAULT_CONFIG["EXTRACTOR_UPLOAD_ID"])
        kb = [
            [{"text": f"Change Target (Current: {val})", "callback_data": "setcfg_EXTRACTOR_UPLOAD_ID"}],
            [{"text": "🔙 Back", "callback_data": "menu_extractor_main"}]
        ]
        await _edit_raw(cid, mid, "⚙️ <b>Extractor Channel Config</b>\n\nSet the Channel/Group ID where Extracted Media will be uploaded.", kb)

    elif d.startswith("setcfg_"):
        cfg_key = d.split("setcfg_")[1]
        ctx.user_data["mode"] = f"waiting_cfg_{cfg_key}"
        cfg_name = CONFIG_NAMES.get(cfg_key, cfg_key)
        await _edit_raw(cid, mid, f"✍️ <b>Update {cfg_name}</b>\n\nPlease send the new Channel/Group ID (must include -100 if it's a channel).\n\n<i>Send /cancel to abort.</i>")

    elif d.startswith("tog_id_"):
        c_key = d.split("tog_id_")[1]
        if uid in CHECKER_STATE and c_key in CHECKER_STATE[uid]["clients"]:
            current_status = CHECKER_STATE[uid]["clients"][c_key]["enabled"]
            CHECKER_STATE[uid]["clients"][c_key]["enabled"] = not current_status
            await _update_dashboard_if_needed(uid, force=True)

    elif d == "queue_pause":
        QUEUE_CONTROL[uid] = "paused"
        await _update_dashboard_if_needed(uid, force=True)

    elif d == "queue_resume":
        QUEUE_CONTROL[uid] = "running"
        await _update_dashboard_if_needed(uid, force=True)

    elif d == "queue_stop":
        QUEUE_CONTROL[uid] = "stopped"
        SCRAPER_TASKS[uid] = "stopped"

    elif d == "menu_settings":
        min_d, max_d = USER_DELAYS.get(uid, (10.0, 15.0))
        kb = [[{"text": "⏱️ Set Custom Delay", "callback_data": "set_delay"}], [{"text": "🔙 Back", "callback_data": "menu_pro"}]]
        await _edit_raw(cid, mid, f"⚙️ <b>Settings Panel</b>\n\n⏱️ <b>Current Delay:</b> {min_d}s to {max_d}s\n\n*(Change the delay carefully to avoid getting your IDs banned)*", kb)

    elif d == "set_delay":
        ctx.user_data["mode"] = "setting_delay"
        await _edit_raw(cid, mid, "⏱️ <b>Send your custom delay in seconds.</b>\n\nExample: `5 10` (for a random delay between 5 to 10 seconds)", [[{"text": "🔙 Cancel", "callback_data": "menu_settings"}]])

    elif d == "menu_scraper":
        state = load_scraper_state(uid)
        scraper_sessions = get_user_sessions(uid, "scraper")
        auto_stat = "✅ ON" if state.get("auto_run") else "❌ OFF"
        targets = state.get("targets", {})
        
        kb = [
            [{"text": "➕ Login New Scraper ID", "callback_data": "scraper_login_tog"}],
        ]
        for s in scraper_sessions:
            base_name = os.path.basename(s)
            kb.append([{"text": f"🗑 Logout Scraper: {base_name}", "callback_data": f"logout_s_{base_name}"}])
            
        kb.extend([
            [{"text": "🎯 Add Target", "callback_data": "scraper_add_target"}, {"text": "🗑 Remove Target", "callback_data": "scraper_rem_target"}],
            [{"text": f"🔄 12-Hour Auto: {auto_stat}", "callback_data": "scraper_tog_auto"}],
            [{"text": "🔙 Back", "callback_data": "menu_pro"}]
        ])
        
        t_list = "\n".join([f"• <code>{t}</code> (Last Msg: {m_id})" for t, m_id in targets.items()]) if targets else "None"
        
        text = (f"🕷️ <b>Scraper Accounts & Target Manager</b>\n\n"
                f"👤 <b>Active Scrapers:</b> {len(scraper_sessions)}\n"
                f"🎯 <b>Active Targets ({len(targets)}):</b>\n{t_list}\n\n"
                f"<i>(Note: When adding a new target, the Bot will extract all links and process them again even if seen before, as per your request.)</i>")
        await _edit_raw(cid, mid, text, kb)

    elif d == "scraper_tog_auto":
        state = load_scraper_state(uid)
        state["auto_run"] = not state.get("auto_run", False)
        save_scraper_state(uid, state)
        await on_callback(Update(update.update_id, callback_query=update.callback_query._replace(data="menu_scraper")), ctx)

    elif d == "scraper_login_tog":
        ctx.user_data["mode"] = "login_phone"
        ctx.user_data["login_type"] = "scraper"
        ctx.user_data["slot"] = get_next_slot(uid, "scraper")
        await _edit_raw(cid, mid, "📱 Send your Telegram Phone Number with country code for **SCRAPER ID**.\nExample: <code>+919876543210</code>\n\n⚠️ <i>Important: OTP will most likely arrive in your Telegram App, not SMS.</i>", [[{"text": "🔙 Cancel", "callback_data": "menu_scraper"}]])

    elif d.startswith("logout_s_"):
        session_name = d.replace("logout_s_", "")
        path = os.path.join(SESSIONS_DIR, session_name + ".session")
        try: os.remove(path)
        except: pass
        await on_callback(Update(update.update_id, callback_query=update.callback_query._replace(data="menu_scraper")), ctx)

    elif d == "scraper_add_target":
        ctx.user_data["mode"] = "scraper_target"
        await _edit_raw(cid, mid, "🎯 <b>Add New Scraper Target</b>\n\nYou can:\n1. Forward any message from the Group/Channel here.\n2. Send the Chat ID (e.g. `-10012345678`)\n3. Paste a Message Link (e.g. `t.me/c/12345/67` or `t.me/joinchat/...`)\n\n<i>(अगर आप पहले से एडेड टारगेट को दोबारा भेजते हैं, तो वह टारगेट रिसेट हो जाएगा और शुरू से सारे लिंक दोबारा स्क्रैप करेगा)</i>", [[{"text": "🔙 Cancel", "callback_data": "menu_scraper"}]])

    elif d == "scraper_rem_target":
        state = load_scraper_state(uid)
        targets = state.get("targets", {})
        if not targets:
            await _edit_raw(cid, mid, "❌ No targets to remove.", [[{"text": "🔙 Back", "callback_data": "menu_scraper"}]])
            return
        
        kb = []
        for t in targets.keys():
            kb.append([{"text": f"❌ {t}", "callback_data": f"rem_t_{t}"}])
        kb.append([{"text": "🔙 Cancel", "callback_data": "menu_scraper"}])
        await _edit_raw(cid, mid, "🗑 <b>Select Target to Remove:</b>", kb)

    elif d.startswith("rem_t_"):
        t_id = d.split("rem_t_")[1]
        state = load_scraper_state(uid)
        if t_id in state["targets"]: 
            del state["targets"][t_id]
        save_scraper_state(uid, state)
        await on_callback(Update(update.update_id, callback_query=update.callback_query._replace(data="menu_scraper")), ctx)

    elif d == "scraper_today":
        if SCRAPER_TASKS.get(uid) == "running":
            await _edit_raw(cid, mid, "⚠️ Scraper is already running!", [[{"text": "🔙 Back", "callback_data": "menu_pro"}]])
            return
        
        state = load_scraper_state(uid)
        SCRAPER_TASKS[uid] = "running"
        asyncio.create_task(_run_daily_scraper_task(uid, cid, state, manual=True))
        await _edit_raw(cid, mid, "✅ Initiating Smart Scrape...\nExtracting all target links.", [[{"text": "🔙 Menu Pro", "callback_data": "menu_pro"}]])

    elif d == "menu_accounts":
        sessions = get_user_sessions(uid, "checker")
        kb = [[{"text": "➕ Login New Checker ID", "callback_data": "login_new"}], [{"text": "🩺 Check Accounts Status", "callback_data": "check_health"}]]
        for s in sessions:
            base_name = os.path.basename(s)
            kb.append([{"text": f"🗑 Logout ID: {base_name}", "callback_data": f"logout_c_{base_name}"}])
        if len(sessions) > 1: kb.append([{"text": "🗑 Logout All IDs", "callback_data": "logout_all"}])
        kb.append([{"text": "🔙 Back", "callback_data": "menu_pro"}])
        await _edit_raw(cid, mid, f"🏦 <b>Checker Bank Manager</b>\n\nLogged in IDs: <b>{len(sessions)}</b>\n\nYou can logout specific IDs, add new ones (10-15 recommended), or check their health.", kb)

    elif d == "check_health":
        await _edit_raw(cid, mid, "⏳ <b>Checking health of all logged-in Checker IDs...</b>\n\n<i>This might take a moment.</i>")
        sessions = get_user_sessions(uid, "checker")
        working_count = dead_count = 0
        for s in sessions:
            try:
                app = Client(s, api_id=API_ID, api_hash=API_HASH, no_updates=True)
                await app.connect()
                if await app.get_me(): working_count += 1
                await app.disconnect()
            except Exception: dead_count += 1
        
        kb = [[{"text": "🔙 Back to Account Bank", "callback_data": "menu_accounts"}]]
        await _edit_raw(cid, mid, f"🩺 <b>Account Status Report</b>\n\n✅ <b>Working IDs:</b> {working_count}\n❌ <b>Dead/Logged Out:</b> {dead_count}\n\n<i>(If you have dead IDs, please find and logout them manually to save resources)</i>", kb)

    elif d.startswith("logout_c_"):
        session_name = d.replace("logout_c_", "")
        path = os.path.join(SESSIONS_DIR, session_name + ".session")
        try: os.remove(path)
        except: pass
        await on_callback(Update(update.update_id, callback_query=update.callback_query._replace(data="menu_accounts")), ctx)

    elif d == "logout_all":
        for s in get_user_sessions(uid, "checker"):
            try: os.remove(s + ".session")
            except: pass
        await _edit_raw(cid, mid, "✅ All Checker IDs Logged Out.", [[{"text": "🔙 Back", "callback_data": "menu_accounts"}]])

    elif d == "login_new":
        ctx.user_data["mode"] = "login_phone"
        ctx.user_data["login_type"] = "checker"
        ctx.user_data["slot"] = get_next_slot(uid, "checker")
        await _edit_raw(cid, mid, "📱 Send your Telegram Phone Number with country code.\nExample: <code>+919876543210</code>\n\n⚠️ <i>Note: OTP usually arrives in your main Telegram App messages.</i>", [[{"text": "🔙 Cancel", "callback_data": "menu_accounts"}]])

    elif d == "menu_check":
        sessions = get_user_sessions(uid, "checker")
        if not sessions:
            await _edit_raw(cid, mid, "❌ <b>No IDs Found!</b>\n\nPlease go to 'Account Bank' and login at least 1 account before checking links.", [[{"text": "🔙 Back", "callback_data": "menu_pro"}]])
            return
        ctx.user_data["mode"] = "checking_links"
        
        if uid in CHECKER_DUPLICATES:
            CHECKER_DUPLICATES[uid].clear()
            
        await _edit_raw(cid, mid, f"🔗 <b>SEND LINKS NOW (MANUAL MODE)</b>\n\nSend up to unlimited links (Forward chunks smoothly).\nBot will process them securely using your {len(sessions)} logged-in IDs in the Account Bank.", [[{"text": "🔙 Back", "callback_data": "menu_pro"}]])

async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    uid = update.effective_user.id; cid = update.effective_chat.id
    text = (update.message.text or update.message.caption or "").strip()
    mode = ctx.user_data.get("mode", "")

    if text == "/start" or text == "/cancel": 
        ctx.user_data["mode"] = ""
        await update.message.reply_text("Action cancelled. Use /start again.")
        return 

    if mode.startswith("waiting_cfg_"):
        cfg_key = mode.replace("waiting_cfg_", "")
        try:
            new_id = int(text)
            config_data = load_bot_config()
            config_data[cfg_key] = new_id
            save_bot_config(config_data)
            
            ctx.user_data["mode"] = ""
            cfg_name = CONFIG_NAMES.get(cfg_key, cfg_key)
            
            if cfg_key == "EXTRACTOR_UPLOAD_ID":
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Config", callback_data="menu_config_extractor")]])
            else:
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Config", callback_data="menu_config_checker")]])
            
            await update.message.reply_text(f"✅ <b>Success!</b>\n\n{cfg_name} has been updated to:\n<code>{new_id}</code>", parse_mode="HTML", reply_markup=kb)
        except ValueError:
            await update.message.reply_text("❌ Invalid ID format. Please send a valid numeric Channel/Group ID (e.g. -1001234567).")
        return

    if mode in ["ext_single_wait", "ext_bulk_wait"]:
        if EXTRACTOR_TASKS.get(uid):
            await update.message.reply_text("⚠️ Extraction already running. Please wait.")
            return
        
        if "t.me/" not in text:
            await update.message.reply_text("❌ Please provide a valid Telegram message or bot link.")
            return
            
        EXTRACTOR_TASKS[uid] = True
        ext_mode = "single" if mode == "ext_single_wait" else "bulk"
        ctx.user_data["mode"] = "" 
        
        target_upload_id = get_conf("EXTRACTOR_UPLOAD_ID")
        asyncio.create_task(_run_media_extractor(uid, cid, text, ext_mode, target_upload_id))
        return

    elif mode == "guest_check":
        links = extract_links(text)
        if not links:
            await update.message.reply_text("❌ No valid Telegram links found.", parse_mode="HTML")
            return
        
        msg = await update.message.reply_text(f"⏳ <b>Checking {len(links)} links without account...</b>\n<i>Please wait, ensuring 100% accuracy...</i>", parse_mode="HTML")
        
        active_links = []
        expired_links = []
        unknown_links = []
        
        for l in links:
            res = await check_public_via_http(l)
            status = res.get("status")
            if status == "active":
                active_links.append(l)
            elif status in ["expired", "error"]:
                expired_links.append(l)
            else:
                unknown_links.append(l)
                
        def chunk_message(title, link_list, icon):
            if not link_list: return []
            chunks = []
            current_chunk = f"<b>{title} ({len(link_list)}):</b>\n\n"
            for l in link_list:
                line = f"{icon} {l}\n"
                if len(current_chunk) + len(line) > 3800:
                    chunks.append(current_chunk)
                    current_chunk = f"<b>{title} (Continued):</b>\n\n"
                current_chunk += line
            if current_chunk: chunks.append(current_chunk)
            return chunks

        all_chunks = []
        if active_links: all_chunks.extend(chunk_message("✅ ACTIVE LINKS", active_links, "✅"))
        if expired_links: all_chunks.extend(chunk_message("❌ EXPIRED LINKS", expired_links, "❌"))
        if unknown_links: all_chunks.extend(chunk_message("⚠️ UNKNOWN LINKS", unknown_links, "⚠️"))

        if not all_chunks:
            await msg.edit_text("No results.", parse_mode="HTML")
            return
            
        await msg.edit_text(all_chunks[0], disable_web_page_preview=True, parse_mode="HTML")
        
        for chunk in all_chunks[1:]:
            await update.message.reply_text(chunk, disable_web_page_preview=True, parse_mode="HTML")

    elif mode == "scraper_target":
        state = load_scraper_state(uid)
        target_val = None
        msg_id_to_save = 0
        
        forward_origin = getattr(update.message, 'forward_origin', None)
        
        if forward_origin:
            if hasattr(forward_origin, 'chat') and forward_origin.chat:
                target_val = str(forward_origin.chat.id)
            elif hasattr(forward_origin, 'sender_chat') and forward_origin.sender_chat:
                target_val = str(forward_origin.sender_chat.id)
        elif getattr(update.message, 'forward_from_chat', None):
            target_val = str(update.message.forward_from_chat.id)
        else:
            text_val = text.strip()
            chat_val, m_id = parse_msg_link(text_val)
            if chat_val:
                target_val = str(chat_val)
                msg_id_to_save = m_id if m_id else 0
            elif "t.me/" in text_val or text_val.startswith("-100") or text_val.startswith("@") or text_val.isdigit():
                target_val = text_val
            
        if target_val:
            targets = state.get("targets", {})
            targets[target_val] = msg_id_to_save
            state["targets"] = targets
            save_scraper_state(uid, state)
            await update.message.reply_text(f"✅ <b>Target Successfully Added/Updated:</b> <code>{target_val}</code>\n<i>Bot will start scraping from message ID {msg_id_to_save} onwards. It will extract and process ALL links found as per settings.</i>", parse_mode="HTML")
            ctx.user_data["mode"] = ""
        else:
            await update.message.reply_text("❌ <b>Invalid input.</b>\nPlease send a valid Chat ID, Username, or Message Link.")

    elif mode == "setting_delay":
        try:
            parts = text.split()
            if len(parts) == 2:
                min_d, max_d = float(parts[0]), float(parts[1])
                if min_d >= 0 and max_d >= min_d:
                    USER_DELAYS[uid] = (min_d, max_d)
                    await update.message.reply_text(f"✅ <b>Delay Updated successfully!</b>\nNew Delay: {min_d}s - {max_d}s", parse_mode="HTML")
                    ctx.user_data["mode"] = ""
                    return
            await update.message.reply_text("❌ <b>Invalid Input.</b>\nEnsure you send two numbers separated by space.", parse_mode="Markdown")
        except: await update.message.reply_text("❌ <b>Invalid Format.</b>", parse_mode="Markdown")

    elif mode == "login_phone":
        if not text.startswith("+") or len(text) < 10:
            await update.message.reply_text("❌ Invalid format. Use +CountryCode Number")
            return
        msg = await update.message.reply_text("⏳ Sending OTP...\n<i>Please check your main Telegram App for the code (Not SMS).</i>", parse_mode="HTML")
        try:
            ltype = ctx.user_data.get("login_type", "checker")
            if ltype == "scraper":
                s_name = f"scraper_{uid}_{ctx.user_data['slot']}"
            else:
                s_name = f"u{uid}_{ctx.user_data['slot']}"
                
            print(f"[{datetime.now()}] 📱 Attempting to send OTP to {text}")
            
            app = Client(
                os.path.join(SESSIONS_DIR, s_name), 
                api_id=API_ID, 
                api_hash=API_HASH,
                device_model="Windows 11 PC",
                system_version="Windows 11",
                app_version="4.14.9",
                lang_code="en"
            )
            
            await app.connect()
            sent = await app.send_code(text)
            
            LOGIN_STATE[uid] = {"app": app, "phone": text, "hash": sent.phone_code_hash}
            ctx.user_data["mode"] = "login_otp"
            
            print(f"[{datetime.now()}] ✅ OTP successfully sent to {text}")
            await msg.edit_text("📩 OTP Sent to your Telegram App! Please send the OTP here.\n*(e.g., send `12345` or space-separated `1 2 3 4 5`)*", parse_mode="Markdown")
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Failed to send OTP to {text}: {e}")
            await msg.edit_text(f"❌ Error: {e}\n\n<i>If FloodWait occurs, try again later or use another number.</i>", parse_mode="HTML")
            ctx.user_data["mode"] = ""

    elif mode == "login_otp":
        otp = text.replace(" ", "")
        if uid not in LOGIN_STATE: return
        data = LOGIN_STATE[uid]; app = data["app"]
        msg = await update.message.reply_text("⏳ Verifying OTP...")
        try:
            await app.sign_in(data["phone"], data["hash"], otp)
            await app.disconnect()
            del LOGIN_STATE[uid]; ctx.user_data["mode"] = ""
            print(f"[{datetime.now()}] ✅ Account successfully logged in via OTP!")
            await msg.edit_text("✅ Login Successful!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Menu', callback_data='back_start')]]))
        except SessionPasswordNeeded:
            ctx.user_data["mode"] = "login_pwd"
            print(f"[{datetime.now()}] 🔐 Two-Step Verification required for {data['phone']}")
            await msg.edit_text("🔐 Two-Step Verification is ON. Send your Password:")
        except Exception as e:
            print(f"[{datetime.now()}] ❌ OTP Error: {e}")
            await msg.edit_text(f"❌ Error: {e}"); 
            try: await app.disconnect() 
            except: pass
            ctx.user_data["mode"] = ""

    elif mode == "login_pwd":
        if uid not in LOGIN_STATE: return
        app = LOGIN_STATE[uid]["app"]
        msg = await update.message.reply_text("⏳ Verifying Password...")
        try:
            await app.check_password(text)
            await app.disconnect()
            del LOGIN_STATE[uid]; ctx.user_data["mode"] = ""
            print(f"[{datetime.now()}] ✅ Account successfully logged in via Password!")
            await msg.edit_text("✅ Login Successful!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Menu', callback_data='back_start')]]))
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Password Error: {e}")
            await msg.edit_text(f"❌ Error: {e}"); 
            try: await app.disconnect() 
            except: pass
            ctx.user_data["mode"] = ""

    elif mode == "checking_links":
        links = extract_links(text)
        if not links: return
            
        sessions = get_user_sessions(uid, "checker")
        if not sessions:
            await update.message.reply_text("❌ Please login a checker first.")
            return

        if uid not in USER_QUEUES: USER_QUEUES[uid] = []
        if uid not in CHECKER_DUPLICATES: CHECKER_DUPLICATES[uid] = set()
            
        bunch_msg_id = update.message.message_id
        added_count = duplicate_count = 0

        for l in links:
            if l not in CHECKER_DUPLICATES[uid]:
                USER_QUEUES[uid].append({"link": l, "message_id": bunch_msg_id, "chat_id": cid})
                CHECKER_DUPLICATES[uid].add(l)
                added_count += 1
            else: duplicate_count += 1

        if added_count == 0 and duplicate_count > 0:
            msg = await update.message.reply_text(f"⚠️ <b>Skipped!</b> All {duplicate_count} links were duplicates.", parse_mode="HTML")
            await asyncio.sleep(3)
            try: await msg.delete()
            except: pass
            return

        if CHECKING_LOCKS.get(uid):
            msg_text = f"✅ Added {added_count} new links to Queue."
            if duplicate_count > 0: msg_text += f"\n🗑 Skipped {duplicate_count} duplicate links in current queue session."
            msg_text += f"\nTotal in Queue: {len(USER_QUEUES[uid])}"
            msg = await update.message.reply_text(msg_text)
            await asyncio.sleep(3)
            try: await msg.delete()
            except: pass
            return

        CHECKING_LOCKS[uid] = True
        asyncio.create_task(_run_bulk_check(uid, cid, sessions, auto_storage=False))

# ─────────────────────────────────────────
#  POST-INIT HOOK FOR BACKGROUND TASKS
# ─────────────────────────────────────────
async def start_background_tasks(application: Application):
    global PYRO_BOT
    try:
        await PYRO_BOT.start()
        print(f"[{datetime.now()}] 🟢 Global Pyrogram Bot Uploader Started Successfully!")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Failed to start Global Pyrogram Bot: {e}")

    asyncio.create_task(auto_scraper_loop())
    print(f"[{datetime.now()}] 🟢 Background Auto-Scraper Task Started Successfully!")

def main():
    app = Application.builder().token(BOT_TOKEN).post_init(start_background_tasks).build()
    
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    
    print(f"[{datetime.now()}] 🟢 Bot is running with Updated Scraper & Advanced Checker Memory...")
    
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
