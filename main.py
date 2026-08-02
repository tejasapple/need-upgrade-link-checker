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
    InviteHashExpired, InviteHashInvalid 
)
from pyrogram.raw.functions.messages import CheckChatInvite
from pyrogram.raw.types import ChatInviteAlready, ChatInvite

# ─────────────────────────────────────────
#  CONFIG 
# ─────────────────────────────────────────
BOT_TOKEN = "8277915856:AAENwF3ByzZ7FKZ7CWLaxiVqCPtmgciEkQ4"
API_ID    = 32003552
API_HASH  = "18e677db0dc3bb8cf89c574a6f460cc3"

ADMIN_ID  = 8884734704

# स्टोरेज चैनल/ग्रुप ID 
STORAGE_CHANNEL_ID = -1004448809511   

# बेसिक चैनल्स
ACTIVE_CHANNEL_ID  = -1004458234660
EXPIRED_CHANNEL_ID = -1003934489318
FORWARD_ON_CHANNEL_ID = -1004340697685
CHATTING_ON_CHANNEL_ID = -1003789944143
SKIPPED_CHANNEL_ID = -1003934489318

# मेंबर्स के अकॉर्डिंग चैनल्स
MEMBERS_LESS_1000_ID = -1004494600592
MEMBERS_1000_2500_ID = -1003701317207
MEMBERS_2500_5000_ID = -1004320671631
MEMBERS_5000_PLUS_ID = -1004320042078

# ऐड मेंबर + चैटिंग/मीडिया चैनल्स
ADD_MEMBER_TEXT_CHAT_ID = -1004334266609    
ADD_MEMBER_MEDIA_CHAT_ID = -1004334266609  

SESSIONS_DIR  = "sessions"
USERS_FILE = "users.txt"
SCRAPER_STATE_FILE = "scraper_state.json"
STORAGE_STATE_FILE = "storage_state.json"  

os.makedirs(SESSIONS_DIR, exist_ok=True)

# VPS Logging Upgraded to INFO so you can see live status on Terminal
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  STATE & LOCKS & QUEUES
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

def extract_links(text: str) -> list[str]:
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

def parse_link(link: str) -> tuple[bool, str]:
    link = link.strip().rstrip("-.,_ \n\t*`~")
    m = re.search(r"t\.me/(?:joinchat/|\+)([A-Za-z0-9_\-]+)", link)
    if m: return True, m.group(1).rstrip("-")
    m = re.search(r"t\.me/([a-zA-Z0-9_]+)", link)
    if m: return False, m.group(1)
    return False, link

async def fast_http_link_check(link: str) -> str:
    link = link.strip().rstrip("-.,_ \n\t*`~")
    for _ in range(1):
        try:
            async with aiohttp.ClientSession() as s:
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                async with s.get(link, timeout=3, headers=headers) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if any(x in text for x in ["Invite link is invalid", "Link is invalid", "has expired"]):
                            return "expired" 
                        if "If you have Telegram, you can contact" in text and "@" in text:
                            if "Join Channel" not in text and "Send Message" not in text and "View in Telegram" not in text:
                                return "unknown"
                        if any(x in text for x in ["Join Group", "Join Channel", "View in Telegram", "View Channel"]):
                            return "active"
                        return "unknown"
                    elif resp.status == 404:
                        return "unknown" 
                    elif resp.status == 429:
                        await asyncio.sleep(0.5)
        except:
            await asyncio.sleep(0.1)
    return "unknown"

async def try_check_link(app: Client, link: str):
    is_private, ref = parse_link(link)
    result = {
        "link": link, "status": "skipped", "title": "Unknown", "username": "N/A",
        "members": "N/A", "videos": "N/A", "photos": "N/A", "forward": "N/A", 
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
            if isinstance(inv, ChatInviteAlready):
                try: chat = await app.get_chat(inv.chat.id)
                except: chat = await app.get_chat(int(f"-100{inv.chat.id}"))
            elif isinstance(inv, ChatInvite):
                try:
                    chat = await app.join_chat(link)
                    joined_now = True
                    await asyncio.sleep(2) 
                    try: chat = await app.get_chat(chat.id)
                    except: pass
                except UserAlreadyParticipant:
                    chat = await app.get_chat(link)
                except Exception as inner_e:
                    err_msg = str(inner_e).lower()
                    if "invite_request_sent" in err_msg:
                        await asyncio.sleep(1)
                        try:
                            chat = await app.get_chat(link)
                            joined_now = True
                        except Exception:
                            raise inner_e 
                    else:
                        raise inner_e

        result["status"] = "active"
        
        if chat:
            raw_title = getattr(chat, 'title', None) or getattr(chat, 'first_name', "Unknown")
            result["title"] = clean_html_text(raw_title)
            result["members"] = str(getattr(chat, 'members_count', 'N/A'))
            
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
    except (ChannelBanned, PeerIdInvalid, ChannelPrivate):
        return None, True, 0
    except (InviteHashExpired, InviteHashInvalid, UsernameInvalid, UsernameNotOccupied):
        result["status"] = "expired"
        result["title"] = "Expired / Invalid"
        return result, False, 0
    except Exception as e:
        err_msg = str(e).lower()
        if "expire" in err_msg or "invalid" in err_msg or "not_occupied" in err_msg or "not a group" in err_msg:
            result["status"] = "expired"
            result["title"] = "Expired / Invalid"
            return result, False, 0
        elif "invite_request_sent" in err_msg:
            result["status"] = "active"
            result["title"] = "Admin Approval Required"
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
        await _send_raw(ACTIVE_CHANNEL_ID, f"<b>✅ ACTIVE LINK</b>\n━━━━━━━━━━\n{msg}")
        
        if "✅" in r.get("forward", ""):
            stats_tracker["fwd"] += 1
            await _send_raw(FORWARD_ON_CHANNEL_ID, f"<b>✅ FORWARD ON LINK</b>\n━━━━━━━━━━\n{msg}")
            
        is_chat_on = "✅" in r.get("chatting", "")
        is_add_on = "✅" in r.get("add_member", "")
        is_media_only = r.get("media_only", False)
        
        if is_chat_on:
            stats_tracker["chat"] += 1
            await _send_raw(CHATTING_ON_CHANNEL_ID, f"<b>💬 CHATTING ON LINK</b>\n━━━━━━━━━━\n{msg}")
            try:
                m_count = int(r.get("members", 0)) if r.get("members") != "N/A" else 0
                if m_count < 1000:
                    await _send_raw(MEMBERS_LESS_1000_ID, f"<b>👥 < 1000 MEMBERS (CHAT ON)</b>\n━━━━━━━━━━\n{msg}")
                elif 1000 <= m_count <= 2500:
                    await _send_raw(MEMBERS_1000_2500_ID, f"<b>👥 1000-2500 MEMBERS (CHAT ON)</b>\n━━━━━━━━━━\n{msg}")
                elif 2500 < m_count <= 5000:
                    await _send_raw(MEMBERS_2500_5000_ID, f"<b>👥 2500-5000 MEMBERS (CHAT ON)</b>\n━━━━━━━━━━\n{msg}")
                elif m_count > 5000:
                    await _send_raw(MEMBERS_5000_PLUS_ID, f"<b>👥 5000+ MEMBERS (CHAT ON)</b>\n━━━━━━━━━━\n{msg}")
            except Exception:
                pass
                
        if is_add_on:
            if is_chat_on:
                stats_tracker["add_chat"] += 1
                await _send_raw(ADD_MEMBER_TEXT_CHAT_ID, f"<b>➕ ADD MEMBER & TEXT CHAT ON</b>\n━━━━━━━━━━\n{msg}")
            elif is_media_only:
                stats_tracker.setdefault("add_media", 0)
                stats_tracker["add_media"] += 1
                await _send_raw(ADD_MEMBER_MEDIA_CHAT_ID, f"<b>➕ ADD MEMBER & MEDIA ONLY ON</b>\n━━━━━━━━━━\n{msg}")
                
    elif r["status"] == "expired":
        await _send_raw(EXPIRED_CHANNEL_ID, f"<b>❌ EXPIRED LINK</b>\n━━━━━━━━━━\n{msg}")
    elif r["status"] == "skipped":
        await _send_raw(SKIPPED_CHANNEL_ID, f"<b>⚠️ SKIPPED LINK</b>\n━━━━━━━━━━\n{msg}")

# ─────────────────────────────────────────
#  SCRAPER & AUTO-UPDATES (SMART MEMORY)
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
        await app.connect()
        total_extracted = 0
        if uid not in SCRAPER_DUPLICATES: SCRAPER_DUPLICATES[uid] = set()

        if manual:
            prog_resp = await _send_raw(cid, f"🔄 <b>Starting Deep Scrape from {len(targets)} Targets...</b>\n<i>(Extracting only links, tracking progress)</i>")
            prog_msg_id = prog_resp.get("result", {}).get("message_id") if isinstance(prog_resp, dict) else None
        
        for target, last_msg_id in targets.items():
            print(f"[{datetime.now()}] 📡 Scraping target: {target}")
            try:
                chat = await app.get_chat(target)
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
                            await _send_raw(STORAGE_CHANNEL_ID, text_to_send)
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
        msg_done = f"✅ <b>Scraping Complete!</b>\nExtracted <code>{total_extracted}</code> fresh links.\n\n"
        
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
#  BULK RUNNER WITH QUEUE (AUTO STORAGE PULL)
# ─────────────────────────────────────────
async def _run_bulk_check(uid: int, cid: int, sessions: list, auto_storage=False):
    QUEUE_CONTROL[uid] = "running"
    print(f"[{datetime.now()}] 🔄 Initializing Bulk Check Queue for UID: {uid}")
    
    clients_dict = {}
    for idx, s_path in enumerate(sessions):
        try:
            app = Client(s_path, api_id=API_ID, api_hash=API_HASH, no_updates=True)
            await app.connect()
            slot = str(s_path.split('_')[-1] if '_' in s_path else idx + 1)
            clients_dict[slot] = {"app": app, "ready_at": 0, "name": f"ID {slot}", "checks": 0, "enabled": True}
        except Exception as e: 
            print(f"[{datetime.now()}] ❌ Failed to connect Checker ID {s_path}: {e}")

    if not clients_dict:
        await _send_raw(cid, "❌ Failed to connect any of your logged-in IDs.")
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
                        messages = await c_app.get_messages(STORAGE_CHANNEL_ID, msg_ids_to_fetch)
                        
                        links_found_in_batch = False
                        
                        for msg in messages:
                            if not msg or msg.empty: continue
                            messages_received += 1
                            links = extract_links(msg.text or msg.caption or "")
                            
                            for l in links:
                                if l not in CHECKER_DUPLICATES[uid]:
                                    USER_QUEUES[uid].append({"link": l, "message_id": msg.id, "chat_id": STORAGE_CHANNEL_ID})
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
                                await _pin_message(STORAGE_CHANNEL_ID, fetched_msg.id)
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
            http_res = await fast_http_link_check(lnk)
            
            if http_res == "expired":
                final_result = {
                    "link": lnk, "status": "expired", "title": "Expired / Invalid",
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
        [{"text": "🔍 Link Without Account (Fast Check)", "callback_data": "menu_guest"}],
        [{"text": "👑 Link Pro (Advanced Dashboard)", "callback_data": "menu_pro"}]
    ]

def PRO_KB(uid):
    checker_sessions = get_user_sessions(uid, "checker")
    scraper_sessions = get_user_sessions(uid, "scraper")
    return [
        [{"text": f"🏦 Checker Bank ({len(checker_sessions)} Active)", "callback_data": "menu_accounts"}],
        [{"text": f"🕷️ Scraper Accounts & Targets ({len(scraper_sessions)} Active)", "callback_data": "menu_scraper"}],
        [{"text": "📥 Trigger Smart Scrape Now", "callback_data": "scraper_today"}],
        [{"text": "🔗 Check Links (Manual)", "callback_data": "menu_check"}],
        [{"text": "⚙️ Settings", "callback_data": "menu_settings"}],
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
    text = f"👋 <b>Welcome {update.effective_user.first_name}</b>\n\nAdvanced Link Checker & Scraper Bot.\nChoose an option below:"
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
        await _edit_raw(cid, mid, "🔍 <b>Guest Mode (No Account Needed)</b>\n\nJust send me any links here. I will do a basic scan and tell you if they are Active or Expired.\n\n<i>Note: This mode doesn't check Members, Photos, Chat features etc.</i>", [[{"text": "🔙 Back", "callback_data": "back_start"}]])

    elif d == "menu_pro":
        ctx.user_data["mode"] = ""
        state = load_scraper_state(uid)
        daily_stats = state.get("daily_stats", 0)
        await _edit_raw(cid, mid, f"👑 <b>Link Pro Dashboard</b>\n\n📊 <b>Scraping Status Today:</b> {daily_stats} Links Extracted\n\nWelcome to the advanced menu. Automate your work seamlessly.", PRO_KB(uid))

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
                f"<i>(When adding a new target, Bot will extract all links. On next run, it will only extract NEW links.)</i>")
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
        await _edit_raw(cid, mid, "🎯 <b>Add New Scraper Target</b>\n\nYou can:\n1. Forward any message from the Group/Channel here.\n2. Or Send the Chat ID (e.g. `-10012345678`)", [[{"text": "🔙 Cancel", "callback_data": "menu_scraper"}]])

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
        await _edit_raw(cid, mid, "✅ Initiating Smart Scrape...\nOnly fetching links & tracking history.", [[{"text": "🔙 Menu Pro", "callback_data": "menu_pro"}]])

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

    if text == "/start": return 

    if mode == "guest_check":
        links = extract_links(text)
        if not links:
            await update.message.reply_text("❌ No valid Telegram links found.", parse_mode="HTML")
            return
        
        msg = await update.message.reply_text("⏳ <b>Checking links without account...</b>", parse_mode="HTML")
        res_text = "🔍 <b>Guest Check Results</b>\n━━━━━━━━━━\n"
        
        for l in links:
            status = await fast_http_link_check(l)
            if status == "expired":
                res_text += f"❌ {l} - Expired/Invalid\n"
            elif status == "active":
                res_text += f"✅ {l} - Active Link\n"
            else:
                res_text += f"⚠️ {l} - Unknown/Requires Login\n"
                
        res_text += "\n<i>(For advanced stats, login via Link Pro)</i>"
        await msg.edit_text(res_text, disable_web_page_preview=True, parse_mode="HTML")

    elif mode == "scraper_target":
        state = load_scraper_state(uid)
        target_val = None
        
        if update.message.forward_from_chat:
            target_val = str(update.message.forward_from_chat.id)
        else:
            target_val = text.strip()
            
        if target_val:
            targets = state.get("targets", {})
            if target_val not in targets:
                targets[target_val] = 0
                state["targets"] = targets
                save_scraper_state(uid, state)
                await update.message.reply_text(f"✅ Target Successfully Added: <code>{target_val}</code>\n\n<i>Note: On first run, it will extract ALL historical links. On next runs, it will only extract new ones.</i>", parse_mode="HTML")
            else:
                await update.message.reply_text("⚠️ Target is already added.")
            ctx.user_data["mode"] = ""
        else:
            await update.message.reply_text("❌ Invalid input.")

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
            
            # --- THE FIX: ADDED DEVICE DETAILS TO FORCE OTP IN APP ---
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
            if duplicate_count > 0: msg_text += f"\n🗑 Skipped {duplicate_count} duplicate links."
            msg_text += f"\nTotal in Queue: {len(USER_QUEUES[uid])}"
            msg = await update.message.reply_text(msg_text)
            await asyncio.sleep(3)
            try: await msg.delete()
            except: pass
            return

        CHECKING_LOCKS[uid] = True
        asyncio.create_task(_run_bulk_check(uid, cid, sessions, auto_storage=False))

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    
    print(f"[{datetime.now()}] 🟢 Bot is running with Multi-Scraper & Smart Memory System...")
    
    loop = asyncio.get_event_loop()
    loop.create_task(auto_scraper_loop())
    
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
