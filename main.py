# main.py
import logging
import os
import json
import asyncio
import httpx
import secrets
import csv
import tempfile
import shutil
import re
from datetime import datetime, timedelta
from threading import Thread

from flask import Flask
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

import aiosqlite
import config
from database import (
    init_db, add_user, get_user, update_credits,
    create_redeem_code, redeem_code_db, get_all_users,
    set_ban_status, get_bot_stats, get_users_in_range,
    add_admin, remove_admin, get_all_admins, is_admin,
    get_expired_codes, delete_redeem_code, get_top_referrers,
    deactivate_code, get_all_codes, parse_time_string,
    get_user_by_username, get_user_stats,
    get_recent_users, get_active_codes, get_inactive_codes,
    delete_user, reset_user_credits,
    search_users, log_lookup,
    get_total_lookups, get_user_lookups,
    get_low_credit_users, get_inactive_users,
    update_last_active, get_leaderboard,
    bulk_update_credits, set_user_premium, remove_user_premium, is_user_premium,
    get_plan_price, update_plan_price,
    create_discount_code, redeem_discount_code,
    get_premium_users, get_users_with_min_credits, get_daily_stats, get_code_usage_stats,
    get_discount_by_code
)

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("❌ BOT_TOKEN environment variable not set!")

OWNER_ID = config.OWNER_ID
ADMIN_IDS = config.ADMIN_IDS
CHANNELS = config.CHANNELS
CHANNEL_LINKS = config.CHANNEL_LINKS
LOG_CHANNELS = config.LOG_CHANNELS
APIS = config.APIS
BACKUP_CHANNEL = config.BACKUP_CHANNEL
DEV_USERNAME = config.DEV_USERNAME
POWERED_BY = config.POWERED_BY

bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
logging.basicConfig(level=logging.INFO)

# --- Flask Keep-Alive for Render ---
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running!"

def run():
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run)
    t.daemon = True
    t.start()

# --- FSM States ---
class Form(StatesGroup):
    waiting_for_redeem = State()
    waiting_for_broadcast = State()
    waiting_for_dm_user = State()
    waiting_for_dm_content = State()
    waiting_for_custom_code = State()
    waiting_for_stats_range = State()
    waiting_for_code_deactivate = State()
    waiting_for_api_input = State()
    waiting_for_username = State()
    waiting_for_delete_user = State()
    waiting_for_reset_credits = State()
    waiting_for_bulk_gift = State()
    waiting_for_user_search = State()
    waiting_for_settings = State()
    waiting_for_offer_code = State()
    waiting_for_bulk_dm_users = State()
    waiting_for_bulk_dm_content = State()
    waiting_for_add_premium = State()
    waiting_for_remove_premium = State()
    waiting_for_plan_price = State()
    waiting_for_offer_details = State()
    waiting_for_bulk_file = State()
    waiting_for_code_stats = State()
    waiting_for_user_lookups = State()
    waiting_for_gift_user = State()
    waiting_for_gift_amount = State()
    waiting_for_removecredits_user = State()
    waiting_for_removecredits_amount = State()
    waiting_for_ban_id = State()
    waiting_for_unban_id = State()
    waiting_for_recent_days = State()
    waiting_for_inactive_days = State()
    waiting_for_gencode_amount = State()
    waiting_for_gencode_uses = State()
    waiting_for_gencode_expiry = State()
    waiting_for_dailystats_days = State()
    waiting_for_topref_limit = State()
    waiting_for_addadmin_id = State()
    waiting_for_removeadmin_id = State()

# --- Helper Functions ---
def get_branding():
    return {"developer": DEV_USERNAME, "powered_by": POWERED_BY}

def clean_api_response(data, extra_blacklist=None):
    if extra_blacklist is None:
        extra_blacklist = []
    if isinstance(data, dict):
        cleaned = {}
        for key, value in data.items():
            if any(unwanted.lower() in key.lower() for unwanted in extra_blacklist):
                continue
            if isinstance(value, str):
                if any(unwanted.lower() in value.lower() for unwanted in extra_blacklist):
                    continue
                if 'credit' in value.lower() and 'nullprotocol' not in value.lower():
                    continue
                cleaned[key] = value
            elif isinstance(value, dict):
                cleaned[key] = clean_api_response(value, extra_blacklist)
            elif isinstance(value, list):
                cleaned[key] = [clean_api_response(item, extra_blacklist) if isinstance(item, dict) else item for item in value]
            else:
                cleaned[key] = value
        return cleaned
    elif isinstance(data, list):
        return [clean_api_response(item, extra_blacklist) if isinstance(item, dict) else item for item in data]
    return data

def format_json_for_display(data, max_length=3500):
    formatted = json.dumps(data, indent=4, ensure_ascii=False)
    if len(formatted) > max_length:
        truncated = formatted[:max_length]
        truncated += f"\n\n... [Data truncated, {len(formatted)-max_length} characters more]"
        return truncated, True
    return formatted, False

def create_readable_txt_file(raw_data, api_type, input_data):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        f.write(f"🔍 {api_type.upper()} Lookup Results\n")
        f.write(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"🔎 Input: {input_data}\n")
        f.write("="*50 + "\n\n")
        def write_readable(obj, indent=0):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    f.write("  " * indent + f"• {key}: ")
                    if isinstance(value, (dict, list)):
                        f.write("\n")
                        write_readable(value, indent + 1)
                    else:
                        f.write(f"{value}\n")
            elif isinstance(obj, list):
                for i, item in enumerate(obj, 1):
                    f.write("  " * indent + f"{i}. ")
                    if isinstance(item, (dict, list)):
                        f.write("\n")
                        write_readable(item, indent + 1)
                    else:
                        f.write(f"{item}\n")
            else:
                f.write(f"{obj}\n")
        write_readable(raw_data)
        f.write("\n" + "="*50 + "\n")
        f.write(f"👨‍💻 Developer: {DEV_USERNAME}\n")
        f.write(f"⚡ Powered by: {POWERED_BY}\n")
        return f.name

async def is_user_owner(user_id):
    return user_id == OWNER_ID

async def is_user_admin(user_id):
    if user_id == OWNER_ID:
        return 'owner'
    if user_id in ADMIN_IDS:
        return 'admin'
    db_admin = await is_admin(user_id)
    return db_admin

async def is_user_banned(user_id):
    user = await get_user(user_id)
    return user[5] == 1 if user else False

async def check_membership(user_id):
    admin_level = await is_user_admin(user_id)
    if admin_level:
        return True
    if await is_user_premium(user_id):
        return True
    try:
        for channel_id in CHANNELS:
            member = await bot.get_chat_member(channel_id, user_id)
            if member.status in ['left', 'kicked', 'restricted']:
                return False
        return True
    except:
        return False

def get_join_keyboard():
    buttons = []
    for i, link in enumerate(CHANNEL_LINKS):
        buttons.append([InlineKeyboardButton(text=f"📢 Join Channel {i+1}", url=link)])
    buttons.append([InlineKeyboardButton(text="✅ Verify Join", callback_data="check_join")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- MAIN MENU (Exactly as requested) ---
def get_main_menu(user_id):
    keyboard = [
        # Row 1 - 📱 Number  📱 Aadhaar
        [
            InlineKeyboardButton(text="📱 Number", callback_data="api_num"),
            InlineKeyboardButton(text="📱 Aadhaar", callback_data="api_aadhar")
        ],
        # Row 2 - 📱 Ration  🆔 TG2Num
        [
            InlineKeyboardButton(text="📱 Ration", callback_data="api_ration"),
            InlineKeyboardButton(text="🆔 TG2Num", callback_data="api_tg2num")
        ],
        # Row 3 - 🚗 Vehicle  🚗 Challan
        [
            InlineKeyboardButton(text="🚗 Vehicle", callback_data="api_vehicle"),
            InlineKeyboardButton(text="🚗 Challan", callback_data="api_vehicle_chalan")
        ],
        # Row 4 - 📧 Email  🏦 IFSC
        [
            InlineKeyboardButton(text="📧 Email", callback_data="api_email"),
            InlineKeyboardButton(text="🏦 IFSC", callback_data="api_ifsc")
        ],
        # Row 5 - 🚗 Vehicle to Number (single button)
        [
            InlineKeyboardButton(text="🚗 Vehicle to Number", callback_data="api_vehicle_to_number")
        ],
        # Row 6 - 📮 Pincode  📋 GST
        [
            InlineKeyboardButton(text="📮 Pincode", callback_data="api_pincode"),
            InlineKeyboardButton(text="📋 GST", callback_data="api_gst")
        ],
        # Row 7 - 🌐 IP Lookup  🇵🇰 Pakistan
        [
            InlineKeyboardButton(text="🌐 IP Lookup", callback_data="api_ip"),
            InlineKeyboardButton(text="🇵🇰 Pakistan", callback_data="api_pakistan")
        ],
        # Row 8 - 📷 Instagram  🐱 GitHub
        [
            InlineKeyboardButton(text="📷 Instagram", callback_data="api_instagram"),
            InlineKeyboardButton(text="🐱 GitHub", callback_data="api_github")
        ],
        # Row 9 - 🎁 Redeem  🔗 Refer & earn
        [
            InlineKeyboardButton(text="🎁 Redeem", callback_data="redeem"),
            InlineKeyboardButton(text="🔗 Refer & earn", callback_data="refer_earn")
        ],
        # Row 10 - 👤 Profile  💳 Buy Credits (Buy Credits is URL button)
        [
            InlineKeyboardButton(text="👤 Profile", callback_data="profile"),
            InlineKeyboardButton(text="💳 Buy Credits", url="https://t.me/Nullprotocol_X")
        ],
        # Row 11 - ⭐ Premium Plans (single button)
        [
            InlineKeyboardButton(text="⭐ Premium Plans", callback_data="premium_plans")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def fetch_api_data(api_type, input_data):
    api_info = APIS.get(api_type)
    if not api_info or not api_info.get('url'):
        logging.error(f"API not configured: {api_type}")
        return {"error": "API not configured", **get_branding()}
    
    url_template = api_info['url']
    if '{}' in url_template:
        url = url_template.format(input_data)
    else:
        url = url_template + input_data
    
    try:
        async with httpx.AsyncClient() as client:
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = await client.get(url, headers=headers, timeout=30)
            if resp.status_code != 200:
                raise Exception(f"API Error {resp.status_code}")
            
            # 🔥 Special handling for vehicle_to_number (always raw text)
            if api_type == 'vehicle_to_number':
                raw_text = resp.text
                return {"raw_text": raw_text, **get_branding()}
            
            # For all other APIs, try to parse JSON
            try:
                raw_data = resp.json()
                extra_blacklist = api_info.get('extra_blacklist', [])
                raw_data = clean_api_response(raw_data, extra_blacklist)
                if isinstance(raw_data, dict):
                    raw_data.update(get_branding())
                elif isinstance(raw_data, list):
                    raw_data = {"results": raw_data, **get_branding()}
                else:
                    raw_data = {"data": str(raw_data), **get_branding()}
                return raw_data
            except:
                # Fallback to raw text if JSON parsing fails
                raw_text = resp.text
                return {"raw_text": raw_text, **get_branding()}
                
    except Exception as e:
        logging.error(f"API fetch error {api_type}: {e}")
        return {"error": "Server Error", "details": str(e)[:200], **get_branding()}

async def process_api_call(message: types.Message, api_type: str, input_data: str):
    user_id = message.from_user.id
    if await is_user_banned(user_id):
        return
    user = await get_user(user_id)
    if not user:
        await message.reply("❌ <b>User not found!</b>", parse_mode="HTML")
        return
    admin_level = await is_user_admin(user_id)
    is_premium = await is_user_premium(user_id)

    if not admin_level and not is_premium:
        if user[2] < 1:
            await message.reply("❌ <b>Insufficient Credits!</b>", parse_mode="HTML")
            return
        else:
            await update_credits(user_id, -1)

    status_msg = await message.reply("🔄 <b>Fetching Data...</b>", parse_mode="HTML")
    raw_data = await fetch_api_data(api_type, input_data)
    await status_msg.delete()

    # 🔥 Check if it's raw text response (for vehicle_to_number)
    if isinstance(raw_data, dict) and 'raw_text' in raw_data:
        # Raw text response
        raw_text = raw_data['raw_text']
        
        # Prepare message
        msg = f"🔍 <b>{api_type.upper()} Lookup Results</b>\n\n"
        msg += f"📊 <b>Input:</b> <code>{input_data}</code>\n"
        msg += f"📅 <b>Date:</b> {datetime.now().strftime('%d-%m-%Y %H:%M')}\n\n"
        msg += f"<pre>{raw_text}</pre>\n\n"
        msg += f"👨‍💻 <b>Developer:</b> {DEV_USERNAME}\n"
        msg += f"⚡ <b>Powered by:</b> {POWERED_BY}"
        
        # Check if message too long
        if len(msg) > 4096:
            # Send as file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
                f.write(f"API Response for {api_type} - {input_data}\n\n")
                f.write(raw_text)
                f.write(f"\n\n👨‍💻 Developer: {DEV_USERNAME}\n⚡ Powered by: {POWERED_BY}")
                temp_file = f.name
            await message.reply_document(
                FSInputFile(temp_file, filename=f"{api_type}_{input_data}.txt"),
                caption=f"🔍 {api_type.upper()} result for {input_data}",
                parse_mode="HTML"
            )
            os.unlink(temp_file)
        else:
            await message.reply(msg, parse_mode="HTML")
        
        # Log to channel (raw text)
        log_channel = LOG_CHANNELS.get(api_type)
        if log_channel and log_channel != "-1000000000000":
            try:
                username = message.from_user.username or 'N/A'
                user_info = f"👤 User: {user_id} (@{username})"
                log_msg = f"📊 <b>Lookup Log - {api_type.upper()}</b>\n\n{user_info}\n🔎 Type: {api_type}\n⌨️ Input: {input_data}\n📅 Date: {datetime.now().strftime('%d-%m-%Y %H:%M')}\n\n📄 Result:\n<pre>{raw_text[:1500]}</pre>"
                if len(raw_text) > 1500:
                    log_msg += "\n... [truncated]"
                await bot.send_message(int(log_channel), log_msg, parse_mode="HTML")
            except Exception as e:
                logging.error(f"Log channel error: {e}")
        
        # Log to database
        await log_lookup(user_id, api_type, input_data, raw_text[:1000])
        await update_last_active(user_id)
        return  # ✅ Important: yahan return karo, taaki aage ka JSON code na chale

    # --- JSON handling for all other APIs ---
    formatted_json, is_truncated = format_json_for_display(raw_data, 3500)
    formatted_json = formatted_json.replace('<', '&lt;').replace('>', '&gt;')
    json_size = len(json.dumps(raw_data, ensure_ascii=False))
    should_send_as_file = json_size > 3000 or (isinstance(raw_data, dict) and any(isinstance(v, list) and len(v) > 10 for v in raw_data.values()))

    temp_file = None
    txt_file = None

    if should_send_as_file:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(raw_data, f, indent=4, ensure_ascii=False)
            temp_file = f.name
        txt_file = create_readable_txt_file(raw_data, api_type, input_data)
        try:
            await message.reply_document(
                FSInputFile(temp_file, filename=f"{api_type}_{input_data}.json"),
                caption=(
                    f"🔍 <b>{api_type.upper()} Lookup Results</b>\n\n"
                    f"📊 <b>Input:</b> <code>{input_data}</code>\n"
                    f"📅 <b>Date:</b> {datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
                    f"📄 <b>File Type:</b> JSON\n\n"
                    f"📝 <i>Data saved as file for better readability</i>\n\n"
                    f"👨‍💻 <b>Developer:</b> {DEV_USERNAME}\n"
                    f"⚡ <b>Powered by:</b> {POWERED_BY}"
                ),
                parse_mode="HTML"
            )
            await message.reply_document(
                FSInputFile(txt_file, filename=f"{api_type}_{input_data}_readable.txt"),
                caption="📄 <b>Readable Text Format</b>\n\n<i>Alternative format for easy reading on mobile</i>",
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"File send error to user: {e}")
            short_msg = (
                f"🔍 <b>{api_type.upper()} Lookup Results</b>\n\n"
                f"📊 <b>Input:</b> <code>{input_data}</code>\n"
                f"📅 <b>Date:</b> {datetime.now().strftime('%d-%m-%Y %H:%M')}\n\n"
                f"⚠️ <b>Data too large for message</b>\n"
                f"📄 <i>Attempted to send as file but failed</i>\n\n"
                f"👨‍💻 <b>Developer:</b> {DEV_USERNAME}\n"
                f"⚡ <b>Powered by:</b> {POWERED_BY}"
            )
            await message.reply(short_msg, parse_mode="HTML")
    else:
        colored = (
            f"🔍 <b>{api_type.upper()} Lookup Results</b>\n\n"
            f"📊 <b>Input:</b> <code>{input_data}</code>\n"
            f"📅 <b>Date:</b> {datetime.now().strftime('%d-%m-%Y %H:%M')}\n\n"
        )
        if is_truncated:
            colored += "⚠️ <i>Response truncated for display</i>\n\n"
        colored += f"<pre><code class=\"language-json\">{formatted_json}</code></pre>\n\n"
        colored += (
            f"📝 <b>Note:</b> Data is for informational purposes only\n"
            f"👨‍💻 <b>Developer:</b> {DEV_USERNAME}\n"
            f"⚡ <b>Powered by:</b> {POWERED_BY}"
        )
        await message.reply(colored, parse_mode="HTML")

    log_data = raw_data.copy()
    if isinstance(log_data, dict) and json_size > 10000:
        for key in log_data:
            if isinstance(log_data[key], list) and len(log_data[key]) > 5:
                log_data[key] = log_data[key][:5] + [f"... truncated, {len(raw_data[key])-5} more"]
            elif isinstance(log_data[key], str) and len(log_data[key]) > 500:
                log_data[key] = log_data[key][:500] + "... [truncated]"
    await log_lookup(user_id, api_type, input_data, json.dumps(log_data, indent=2))
    await update_last_active(user_id)

    log_channel = LOG_CHANNELS.get(api_type)
    if log_channel and log_channel != "-1000000000000":
        try:
            username = message.from_user.username or 'N/A'
            user_info = f"👤 User: {user_id} (@{username})"
            
            if should_send_as_file and temp_file and os.path.exists(temp_file):
                await bot.send_document(
                    chat_id=int(log_channel),
                    document=FSInputFile(temp_file, filename=f"{api_type}_{input_data}.json"),
                    caption=(
                        f"📊 <b>Lookup Log - {api_type.upper()}</b>\n\n"
                        f"{user_info}\n"
                        f"🔎 Type: {api_type}\n"
                        f"⌨️ Input: <code>{input_data}</code>\n"
                        f"📅 Date: {datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
                        f"📊 Size: {json_size} characters\n"
                        f"📄 Format: JSON File"
                    ),
                    parse_mode="HTML"
                )
                if txt_file and os.path.exists(txt_file):
                    await bot.send_document(
                        chat_id=int(log_channel),
                        document=FSInputFile(txt_file, filename=f"{api_type}_{input_data}_readable.txt"),
                        caption="📄 Readable Text Format"
                    )
            else:
                log_message = (
                    f"📊 <b>Lookup Log - {api_type.upper()}</b>\n\n"
                    f"{user_info}\n"
                    f"🔎 Type: {api_type}\n"
                    f"⌨️ Input: <code>{input_data}</code>\n"
                    f"📅 Date: {datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
                    f"📊 Size: {json_size} characters\n\n"
                    f"📄 Result:\n<pre>{formatted_json[:1500]}</pre>"
                )
                if len(formatted_json) > 1500:
                    log_message += "\n... [⚡ Powered by: NULL PROTOCOL]"
                await bot.send_message(int(log_channel), log_message, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Log channel error: {e}")
            try:
                await bot.send_message(
                    int(log_channel),
                    f"📊 <b>Lookup Failed to Log</b>\n\n"
                    f"👤 User: {user_id}\n"
                    f"🔎 Type: {api_type}\n"
                    f"⌨️ Input: {input_data}\n"
                    f"📅 Date: {datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
                    f"❌ Error: {str(e)[:200]}",
                    parse_mode="HTML"
                )
            except:
                pass

    if temp_file and os.path.exists(temp_file):
        try:
            os.unlink(temp_file)
        except:
            pass
    if txt_file and os.path.exists(txt_file):
        try:
            os.unlink(txt_file)
        except:
            pass

# --- START COMMAND ---
@dp.message(CommandStart())
async def start_command(message: types.Message, command: CommandObject):
    user_id = message.from_user.id
    if await is_user_banned(user_id):
        await message.answer("🚫 <b>You are BANNED from using this bot.</b>", parse_mode="HTML")
        return

    existing = await get_user(user_id)
    if not existing:
        referrer = None
        args = command.args
        if args and args.startswith("ref_"):
            try:
                referrer = int(args.split("_")[1])
                if referrer == user_id:
                    referrer = None
            except:
                pass
        await add_user(user_id, message.from_user.username, referrer)
        if referrer:
            await update_credits(referrer, 3)
            try:
                await bot.send_message(referrer, "🎉 <b>Referral +3 Credits!</b>", parse_mode="HTML")
            except:
                pass

    if not await check_membership(user_id):
        await message.answer(
            "👋 <b>Welcome to OSINT FATHER</b>\n\n"
            "⚠️ <b>Bot use karne ke liye channels join karein:</b>",
            reply_markup=get_join_keyboard(),
            parse_mode="HTML"
        )
        return

    welcome_msg = f"""
🔓 <b>Access Granted!</b>

Welcome <b>{message.from_user.first_name}</b>,

<b>OSINT FATHER</b> - Premium Lookup Services
Select a service from menu below:
"""
    await message.answer(welcome_msg, reply_markup=get_main_menu(user_id), parse_mode="HTML")
    await update_last_active(user_id)

@dp.callback_query(F.data == "check_join")
async def verify_join(callback: types.CallbackQuery):
    if await check_membership(callback.from_user.id):
        await callback.message.delete()
        await callback.message.answer("✅ <b>Verified!</b>", reply_markup=get_main_menu(callback.from_user.id), parse_mode="HTML")
    else:
        await callback.answer("❌ Abhi bhi kuch channels join nahi kiye!", show_alert=True)

# --- PROFILE (with recent lookup history) ---
@dp.callback_query(F.data == "profile")
async def show_profile(callback: types.CallbackQuery):
    user_data = await get_user(callback.from_user.id)
    if not user_data:
        return
    admin_level = await is_user_admin(callback.from_user.id)
    is_premium = await is_user_premium(callback.from_user.id)
    credits = "♾️ Unlimited" if (admin_level or is_premium) else user_data[2]
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=ref_{user_data[0]}"
    stats = await get_user_stats(callback.from_user.id)
    referrals = stats[0] if stats else 0
    codes_claimed = stats[1] if stats else 0
    total_from_codes = stats[2] if stats else 0
    lookups = await get_user_lookups(callback.from_user.id, limit=5)
    msg = (f"👤 <b>User Profile</b>\n\n"
           f"🆔 <b>ID:</b> <code>{user_data[0]}</code>\n"
           f"👤 <b>Username:</b> @{user_data[1] or 'N/A'}\n"
           f"💰 <b>Credits:</b> {credits}\n"
           f"📊 <b>Total Earned:</b> {user_data[6]}\n"
           f"👥 <b>Referrals:</b> {referrals}\n"
           f"🎫 <b>Codes Claimed:</b> {codes_claimed}\n"
           f"📅 <b>Joined:</b> {datetime.fromtimestamp(float(user_data[3])).strftime('%d-%m-%Y')}\n"
           f"🔗 <b>Referral Link:</b>\n<code>{link}</code>\n\n"
           f"📋 <b>Recent Lookups:</b>\n")
    if lookups:
        for i, (api_type, inp, date) in enumerate(lookups, 1):
            dstr = datetime.fromisoformat(date).strftime('%d/%m %H:%M')
            msg += f"{i}. {api_type.upper()}: <code>{inp}</code> - {dstr}\n"
    else:
        msg += "No lookups yet."
    await callback.message.edit_text(msg, parse_mode="HTML", reply_markup=get_main_menu(callback.from_user.id))

# --- REFER & EARN ---
@dp.callback_query(F.data == "refer_earn")
async def refer_earn_handler(callback: types.CallbackQuery):
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=ref_{callback.from_user.id}"
    msg = (
        "🔗 <b>Refer & Earn Program</b>\n\n"
        "Apne dosto ko invite karein aur free credits paayein!\n"
        "Per Referral: <b>+3 Credits</b>\n\n"
        "👇 <b>Your Link:</b>\n"
        f"<code>{link}</code>\n\n"
        "📊 <b>How it works:</b>\n"
        "1. Apna link share karein\n"
        "2. Jo bhi is link se join karega\n"
        "3. Aapko milenge <b>3 credits</b>"
    )
    back_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back", callback_data="back_home")]])
    await callback.message.edit_text(msg, parse_mode="HTML", reply_markup=back_kb)

@dp.callback_query(F.data == "back_home")
async def go_home(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(discount_percent=0, discount_code=None)
    await callback.message.edit_text("🔓 <b>Main Menu</b>", reply_markup=get_main_menu(callback.from_user.id), parse_mode="HTML")

# --- REDEEM CODE ---
@dp.callback_query(F.data == "redeem")
async def redeem_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "🎁 <b>Redeem Code</b>\n\n"
        "Enter your redeem code below:\n\n"
        "📌 <i>Note: Each code can be used only once per user</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_redeem")]]),
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_redeem)
    await callback.answer()

@dp.callback_query(F.data == "cancel_redeem")
async def cancel_redeem(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer("❌ Operation Cancelled.", reply_markup=get_main_menu(callback.from_user.id))

@dp.message(Form.waiting_for_redeem)
async def process_redeem(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    result = await redeem_code_db(message.from_user.id, code)
    user_data = await get_user(message.from_user.id)
    if isinstance(result, int):
        new_balance = user_data[2] + result if user_data else result
        await message.answer(
            f"✅ <b>Code Redeemed Successfully!</b>\n"
            f"➕ <b>{result} Credits</b> added to your account.\n\n"
            f"💰 <b>New Balance:</b> {new_balance}",
            parse_mode="HTML",
            reply_markup=get_main_menu(message.from_user.id)
        )
    elif result == "already_claimed":
        await message.answer(
            "❌ <b>You have already claimed this code!</b>\n"
            "Each user can claim a code only once.",
            parse_mode="HTML",
            reply_markup=get_main_menu(message.from_user.id)
        )
    elif result == "invalid":
        await message.answer(
            "❌ <b>Invalid Code!</b>\n"
            "Please check the code and try again.",
            parse_mode="HTML",
            reply_markup=get_main_menu(message.from_user.id)
        )
    elif result == "inactive":
        await message.answer(
            "❌ <b>Code is Inactive!</b>\n"
            "This code has been deactivated by admin.",
            parse_mode="HTML",
            reply_markup=get_main_menu(message.from_user.id)
        )
    elif result == "limit_reached":
        await message.answer(
            "❌ <b>Code Limit Reached!</b>\n"
            "This code has been used by maximum users.",
            parse_mode="HTML",
            reply_markup=get_main_menu(message.from_user.id)
        )
    elif result == "expired":
        await message.answer(
            "❌ <b>Code Expired!</b>\n"
            "This code is no longer valid.",
            parse_mode="HTML",
            reply_markup=get_main_menu(message.from_user.id)
        )
    else:
        await message.answer(
            "❌ <b>Error processing code!</b>\n"
            "Please try again later.",
            parse_mode="HTML",
            reply_markup=get_main_menu(message.from_user.id)
        )
    await state.clear()

# --- API INPUT HANDLERS ---
@dp.callback_query(F.data.startswith("api_"))
async def ask_api_input(callback: types.CallbackQuery, state: FSMContext):
    if await is_user_banned(callback.from_user.id):
        return
    if not await check_membership(callback.from_user.id):
        await callback.answer("❌ Join channels first!", show_alert=True)
        return
    
    # Extract api_type correctly for multi-part names
    api_type = callback.data.replace('api_', '', 1)
    
    if api_type not in APIS or not APIS[api_type].get('url'):
        await callback.answer("❌ This service is temporarily unavailable", show_alert=True)
        return
    await state.set_state(Form.waiting_for_api_input)
    await state.update_data(api_type=api_type)
    prompts = {
        'num': "📱 Enter Mobile Number (10 digits)",
        'ifsc': "🏦 Enter IFSC Code (11 characters)",
        'email': "📧 Enter Email Address",
        'gst': "📋 Enter GST Number (15 characters)",
        'vehicle': "🚗 Enter Vehicle RC Number",
        'vehicle_chalan': "🚗 Enter Vehicle RC Number for Challan Info",
        'vehicle_to_number': "🚗 Enter Vehicle RC Number to Get Owner Number",
        'pincode': "📮 Enter Pincode (6 digits)",
        'instagram': "📷 Enter Instagram Username (without @)",
        'github': "🐱 Enter GitHub Username",
        'pakistan': "🇵🇰 Enter Pakistan Mobile Number (with country code)",
        'ip': "🌐 Enter IP Address",
        'aadhar': "📱 Enter Aadhaar Number (12 digits)",
        'ration': "📱 Enter Aadhaar Number for Ration Info (12 digits)",
        'tg2num': "🆔 Enter Telegram User ID",
    }
    instructions = prompts.get(api_type, "Enter input")
    await callback.message.answer(
        f"<b>{instructions}</b>\n\n"
        f"<i>Type /cancel to cancel</i>\n\n"
        f"📄 <i>Note: Large responses will be sent as files</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_api")]])
    )

@dp.callback_query(F.data == "cancel_api")
async def cancel_api(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer("❌ Operation Cancelled.", reply_markup=get_main_menu(callback.from_user.id))

@dp.message(Form.waiting_for_api_input)
async def handle_api_input(message: types.Message, state: FSMContext):
    data = await state.get_data()
    api_type = data.get('api_type')
    if api_type:
        await process_api_call(message, api_type, message.text.strip())
    await state.clear()

# --- PREMIUM PLANS with Discount System ---
@dp.callback_query(F.data == "premium_plans")
async def show_premium_plans(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = await state.get_data()
    discount = data.get('discount_percent', 0)
    discount_code = data.get('discount_code', None)
    
    if await is_user_premium(user_id):
        await callback.message.edit_text(
            "⭐ <b>You are already a Premium User!</b>\n\n✅ Unlimited searches\n✅ No channel join",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back", callback_data="back_home")]])
        )
        return
    
    weekly_price = await get_plan_price('weekly') or 69
    monthly_price = await get_plan_price('monthly') or 199
    
    weekly_discounted = int(weekly_price * (100 - discount) / 100)
    monthly_discounted = int(monthly_price * (100 - discount) / 100)
    
    if discount > 0:
        price_text = (f"📅 Weekly Plan: ~~₹{weekly_price}~~ ➜ **₹{weekly_discounted}** ({discount}% off)\n"
                      f"📆 Monthly Plan: ~~₹{monthly_price}~~ ➜ **₹{monthly_discounted}** ({discount}% off)\n\n"
                      f"🎟️ Applied code: <code>{discount_code}</code>")
        extra_buttons = [[InlineKeyboardButton(text="❌ Remove Discount", callback_data="remove_discount")]]
    else:
        price_text = f"📅 Weekly Plan – ₹{weekly_price}\n📆 Monthly Plan – ₹{monthly_price}\n\n"
        extra_buttons = []
    
    text = (
        f"⭐ <b>Premium Plans</b>\n\n"
        f"{price_text}"
        f"💳 <b>How to Buy:</b>\n"
        f"Contact @Nullprotocol_X to purchase.\n"
        f"After payment, admin will activate your premium."
    )
    
    keyboard = [
        [InlineKeyboardButton(text=f"📅 Buy Weekly (₹{weekly_discounted})", callback_data="buy_weekly")],
        [InlineKeyboardButton(text=f"📆 Buy Monthly (₹{monthly_discounted})", callback_data="buy_monthly")],
        [InlineKeyboardButton(text="🎟️ Redeem Offer Code", callback_data="redeem_offer")],
    ] + extra_buttons + [[InlineKeyboardButton(text="🔙 Back", callback_data="back_home")]]
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))

@dp.callback_query(F.data.startswith("buy_"))
async def buy_plan_handler(callback: types.CallbackQuery, state: FSMContext):
    plan = callback.data.split("_")[1]  # weekly or monthly
    data = await state.get_data()
    discount = data.get('discount_percent', 0)
    
    base_price = await get_plan_price(plan) or (69 if plan == "weekly" else 199)
    final_price = int(base_price * (100 - discount) / 100)
    
    text = (
        f"🛒 <b>Purchase {plan.capitalize()} Plan</b>\n\n"
        f"{'Original Price: ₹' + str(base_price) + '\n' if discount > 0 else ''}"
        f"Final Amount: ₹{final_price}\n\n"
        "📲 <b>Payment Instructions:</b>\n"
        "1. Send payment to [UPI ID / QR code]\n"
        "2. Take a screenshot\n"
        "3. Forward screenshot to @Nullprotocol_X\n"
        "4. Your premium will be activated within 24 hours\n\n"
        "Or click below to contact admin directly:"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Contact Admin", url="https://t.me/Nullprotocol_X")],
        [InlineKeyboardButton(text="🔙 Back to Plans", callback_data="premium_plans")]
    ])
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(F.data == "redeem_offer")
async def redeem_offer_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "🎟️ <b>Redeem Offer Code</b>\n\n"
        "Enter your discount code:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_redeem_offer")]]),
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_offer_code)
    await callback.answer()

@dp.callback_query(F.data == "cancel_redeem_offer")
async def cancel_offer_redeem(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer("❌ Offer redemption cancelled.", reply_markup=get_main_menu(callback.from_user.id))

@dp.message(Form.waiting_for_offer_code)
async def process_offer_code(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    discount_info = await get_discount_by_code(code)
    if not discount_info:
        await message.answer("❌ Invalid or expired offer code.")
        await state.clear()
        return
    discount_percent, plan_id, max_uses, current_uses, expiry_minutes, created_date, is_active = discount_info
    if not is_active or current_uses >= max_uses:
        await message.answer("❌ Offer code is no longer valid.")
        await state.clear()
        return
    if expiry_minutes:
        created_dt = datetime.fromisoformat(created_date)
        if datetime.now() > created_dt + timedelta(minutes=expiry_minutes):
            await message.answer("❌ Offer code has expired.")
            await state.clear()
            return
    
    await state.update_data(discount_percent=discount_percent, discount_code=code)
    await message.answer(
        f"✅ Offer code accepted! You got {discount_percent}% discount.\n"
        f"Click below to view discounted plans.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⭐ View Premium Plans", callback_data="premium_plans")]
        ])
    )
    await state.set_state(None)

@dp.callback_query(F.data == "remove_discount")
async def remove_discount(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(discount_percent=0, discount_code=None)
    await callback.answer("Discount removed.")
    await show_premium_plans(callback, state)

# ========== ADMIN PANEL (Complete) ==========
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return

    text = "🛠 <b>ADMIN CONTROL PANEL</b>\n\nChoose a category:"
    buttons = [
        [InlineKeyboardButton(text="📊 User Management", callback_data="admin_user_mgmt")],
        [InlineKeyboardButton(text="🎫 Code Management", callback_data="admin_code_mgmt")],
        [InlineKeyboardButton(text="📈 Statistics", callback_data="admin_stats")],
    ]
    if admin_level == 'owner':
        buttons.append([InlineKeyboardButton(text="👑 Owner Commands", callback_data="admin_owner")])
    buttons.append([InlineKeyboardButton(text="❌ Close", callback_data="close_panel")])

    await message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(F.data == "close_panel")
async def close_panel(callback: types.CallbackQuery):
    await callback.message.delete()

# --- User Management Submenu ---
@dp.callback_query(F.data == "admin_user_mgmt")
async def admin_user_mgmt(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        await callback.answer("Unauthorized", show_alert=True)
        return

    text = "📊 <b>User Management</b>\n\nSelect an action:"
    buttons = [
        [InlineKeyboardButton(text="📢 Broadcast", callback_data="broadcast_now"),
         InlineKeyboardButton(text="📨 Direct Message", callback_data="dm_now")],
        [InlineKeyboardButton(text="🎁 Gift Credits", callback_data="admin_gift"),
         InlineKeyboardButton(text="🎁 Bulk Gift", callback_data="bulk_gift")],
        [InlineKeyboardButton(text="📉 Remove Credits", callback_data="admin_removecredits"),
         InlineKeyboardButton(text="🔄 Reset Credits", callback_data="admin_resetcredits")],
        [InlineKeyboardButton(text="🚫 Ban User", callback_data="admin_ban"),
         InlineKeyboardButton(text="🟢 Unban User", callback_data="admin_unban")],
        [InlineKeyboardButton(text="🗑 Delete User", callback_data="admin_deleteuser"),
         InlineKeyboardButton(text="🔍 Search User", callback_data="admin_searchuser")],
        [InlineKeyboardButton(text="👥 List Users", callback_data="admin_users"),
         InlineKeyboardButton(text="📈 Recent Users", callback_data="admin_recentusers")],
        [InlineKeyboardButton(text="📊 User Lookups", callback_data="admin_userlookups"),
         InlineKeyboardButton(text="🏆 Leaderboard", callback_data="admin_leaderboard")],
        [InlineKeyboardButton(text="💰 Premium Users (100+)", callback_data="admin_premiumusers"),
         InlineKeyboardButton(text="📉 Low Credit Users", callback_data="admin_lowcredit")],
        [InlineKeyboardButton(text="⏰ Inactive Users", callback_data="admin_inactiveusers"),
         InlineKeyboardButton(text="⭐ Add Premium", callback_data="add_premium")],
        [InlineKeyboardButton(text="➖ Remove Premium", callback_data="remove_premium")],
        [InlineKeyboardButton(text="🔙 Back to Admin", callback_data="admin_back")]
    ]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

# --- Code Management Submenu ---
@dp.callback_query(F.data == "admin_code_mgmt")
async def admin_code_mgmt(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    text = "🎫 <b>Code Management</b>\n\nSelect an action:"
    buttons = [
        [InlineKeyboardButton(text="🎲 Generate Random Code", callback_data="admin_gencode"),
         InlineKeyboardButton(text="🎫 Custom Code", callback_data="admin_customcode")],
        [InlineKeyboardButton(text="📋 List All Codes", callback_data="admin_listcodes"),
         InlineKeyboardButton(text="✅ Active Codes", callback_data="admin_activecodes")],
        [InlineKeyboardButton(text="❌ Inactive Codes", callback_data="admin_inactivecodes"),
         InlineKeyboardButton(text="🚫 Deactivate Code", callback_data="admin_deactivatecode")],
        [InlineKeyboardButton(text="📊 Code Stats", callback_data="admin_codestats"),
         InlineKeyboardButton(text="⌛️ Check Expired", callback_data="admin_checkexpired")],
        [InlineKeyboardButton(text="🧹 Clean Expired", callback_data="admin_cleanexpired")],
        [InlineKeyboardButton(text="💰 Set Plan Price", callback_data="set_plan_price"),
         InlineKeyboardButton(text="🎟️ Create Offer", callback_data="create_offer")],
        [InlineKeyboardButton(text="🔙 Back to Admin", callback_data="admin_back")]
    ]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

# --- Statistics Submenu ---
@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    text = "📈 <b>Statistics</b>\n\nSelect an action:"
    buttons = [
        [InlineKeyboardButton(text="📊 Bot Stats", callback_data="admin_stats_general"),
         InlineKeyboardButton(text="📅 Daily Stats", callback_data="admin_dailystats")],
        [InlineKeyboardButton(text="🔍 Lookup Stats", callback_data="admin_lookupstats"),
         InlineKeyboardButton(text="💾 Backup User Data", callback_data="admin_backup")],
        [InlineKeyboardButton(text="🏆 Top Referrers", callback_data="admin_topref")],
        [InlineKeyboardButton(text="🔙 Back to Admin", callback_data="admin_back")]
    ]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

# --- Owner Commands Submenu ---
@dp.callback_query(F.data == "admin_owner")
async def admin_owner(callback: types.CallbackQuery):
    if not await is_user_owner(callback.from_user.id):
        await callback.answer("Owner only!", show_alert=True)
        return
    text = "👑 <b>Owner Commands</b>\n\nSelect an action:"
    buttons = [
        [InlineKeyboardButton(text="➕ Add Admin", callback_data="admin_addadmin"),
         InlineKeyboardButton(text="➖ Remove Admin", callback_data="admin_removeadmin")],
        [InlineKeyboardButton(text="👥 List Admins", callback_data="admin_listadmins"),
         InlineKeyboardButton(text="⚙️ Settings", callback_data="admin_settings")],
        [InlineKeyboardButton(text="💾 Full DB Backup", callback_data="admin_fulldbbackup")],
        [InlineKeyboardButton(text="🔙 Back to Admin", callback_data="admin_back")]
    ]
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: types.CallbackQuery):
    await admin_panel(callback.message)

# --- BROADCAST ---
@dp.callback_query(F.data == "broadcast_now")
async def broadcast_now(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("📢 <b>Send message to broadcast</b> (text, photo, video, etc.):", parse_mode="HTML")
    await state.set_state(Form.waiting_for_broadcast)
    await callback.answer()

@dp.message(Form.waiting_for_broadcast)
async def broadcast_handler(message: types.Message, state: FSMContext):
    users = await get_all_users()
    sent = 0
    failed = 0
    total = len(users)
    status = await message.answer(f"🚀 Broadcasting to {total} users...\n\nSent: 0\nFailed: 0")
    for uid in users:
        try:
            await message.copy_to(uid)
            sent += 1
            if sent % 20 == 0:
                await status.edit_text(f"🚀 Broadcasting...\n✅ Sent: {sent}\n❌ Failed: {failed}\n📊 Progress: {((sent+failed)/total*100):.1f}%")
            await asyncio.sleep(0.05)
        except:
            failed += 1
    await status.edit_text(f"✅ <b>Broadcast Complete!</b>\n\n✅ Sent: {sent}\n❌ Failed: {failed}\n👥 Total: {total}", parse_mode="HTML")
    await state.clear()

# --- DIRECT MESSAGE ---
@dp.callback_query(F.data == "dm_now")
async def dm_now(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("👤 <b>Enter user ID to send message:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_dm_user)
    await callback.answer()

@dp.message(Form.waiting_for_dm_user)
async def dm_user_handler(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        await state.update_data(dm_user_id=uid)
        await message.answer("📨 Now send the message:")
        await state.set_state(Form.waiting_for_dm_content)
    except:
        await message.answer("❌ Invalid user ID. Please enter a numeric ID.")

@dp.message(Form.waiting_for_dm_content)
async def dm_content_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    uid = data.get('dm_user_id')
    try:
        await message.copy_to(uid)
        await message.answer(f"✅ Message sent to user {uid}")
    except Exception as e:
        await message.answer(f"❌ Failed: {str(e)}")
    await state.clear()

# --- GIFT CREDITS ---
@dp.callback_query(F.data == "admin_gift")
async def admin_gift_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🎁 <b>Gift Credits</b>\n\nEnter user ID:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_gift_user)
    await callback.answer()

@dp.message(Form.waiting_for_gift_user)
async def gift_user_handler(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        await state.update_data(gift_user_id=uid)
        await message.answer("Enter amount of credits to add:")
        await state.set_state(Form.waiting_for_gift_amount)
    except:
        await message.answer("❌ Invalid user ID. Please enter a numeric ID.")

@dp.message(Form.waiting_for_gift_amount)
async def gift_amount_handler(message: types.Message, state: FSMContext):
    try:
        amount = int(message.text)
        data = await state.get_data()
        uid = data['gift_user_id']
        await update_credits(uid, amount)
        await message.answer(f"✅ Added {amount} credits to user {uid}")
        try:
            await bot.send_message(uid, f"🎁 <b>Admin Gifted You {amount} Credits!</b>", parse_mode="HTML")
        except:
            pass
    except:
        await message.answer("❌ Invalid amount. Please enter a number.")
    await state.clear()

# --- BULK GIFT ---
@dp.callback_query(F.data == "bulk_gift")
async def bulk_gift_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "🎁 <b>Bulk Gift Credits</b>\n\n"
        "Send in format: <code>AMOUNT USERID1 USERID2 ...</code>\n"
        "Example: <code>50 123456 789012 345678</code>",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_bulk_gift)
    await callback.answer()

@dp.message(Form.waiting_for_bulk_gift)
async def bulk_gift_handler(message: types.Message, state: FSMContext):
    try:
        parts = message.text.split()
        amount = int(parts[0])
        user_ids = [int(uid) for uid in parts[1:]]
        await bulk_update_credits(user_ids, amount)
        msg = f"✅ Gifted {amount} credits to {len(user_ids)} users:\n"
        for uid in user_ids[:10]:
            msg += f"• <code>{uid}</code>\n"
        if len(user_ids) > 10:
            msg += f"... and {len(user_ids)-10} more"
        await message.answer(msg, parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Error: {e}")
    await state.clear()

# --- REMOVE CREDITS ---
@dp.callback_query(F.data == "admin_removecredits")
async def admin_removecredits_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("📉 <b>Remove Credits</b>\n\nEnter user ID:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_removecredits_user)
    await callback.answer()

@dp.message(Form.waiting_for_removecredits_user)
async def removecredits_user_handler(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        await state.update_data(removecredits_user_id=uid)
        await message.answer("Enter amount of credits to remove:")
        await state.set_state(Form.waiting_for_removecredits_amount)
    except:
        await message.answer("❌ Invalid user ID.")

@dp.message(Form.waiting_for_removecredits_amount)
async def removecredits_amount_handler(message: types.Message, state: FSMContext):
    try:
        amount = int(message.text)
        data = await state.get_data()
        uid = data['removecredits_user_id']
        await update_credits(uid, -amount)
        await message.answer(f"✅ Removed {amount} credits from user {uid}")
        try:
            await bot.send_message(uid, f"⚠️ <b>Admin Removed {amount} Credits From Your Account!</b>", parse_mode="HTML")
        except:
            pass
    except:
        await message.answer("❌ Invalid amount.")
    await state.clear()

# --- RESET CREDITS ---
@dp.callback_query(F.data == "admin_resetcredits")
async def admin_resetcredits_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🔄 <b>Reset Credits</b>\n\nEnter user ID:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_reset_credits)
    await callback.answer()

@dp.message(Form.waiting_for_reset_credits)
async def reset_credits_handler(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        await reset_user_credits(uid)
        await message.answer(f"✅ Credits reset for user {uid}")
    except:
        await message.answer("❌ Invalid user ID.")
    await state.clear()

# --- BAN USER ---
@dp.callback_query(F.data == "admin_ban")
async def admin_ban_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🚫 <b>Ban User</b>\n\nEnter user ID:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_ban_id)
    await callback.answer()

@dp.message(Form.waiting_for_ban_id)
async def ban_handler(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        await set_ban_status(uid, 1)
        await message.answer(f"🚫 User {uid} banned.")
        try:
            await bot.send_message(uid, "🚫 <b>You have been banned from using this bot.</b>", parse_mode="HTML")
        except:
            pass
    except:
        await message.answer("❌ Invalid user ID.")
    await state.clear()

# --- UNBAN USER ---
@dp.callback_query(F.data == "admin_unban")
async def admin_unban_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🟢 <b>Unban User</b>\n\nEnter user ID:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_unban_id)
    await callback.answer()

@dp.message(Form.waiting_for_unban_id)
async def unban_handler(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        await set_ban_status(uid, 0)
        await message.answer(f"🟢 User {uid} unbanned.")
        try:
            await bot.send_message(uid, "✅ <b>You have been unbanned. You can now use the bot again.</b>", parse_mode="HTML")
        except:
            pass
    except:
        await message.answer("❌ Invalid user ID.")
    await state.clear()

# --- DELETE USER ---
@dp.callback_query(F.data == "admin_deleteuser")
async def admin_deleteuser_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🗑 <b>Delete User</b>\n\nEnter user ID:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_delete_user)
    await callback.answer()

@dp.message(Form.waiting_for_delete_user)
async def delete_user_handler(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        await delete_user(uid)
        await message.answer(f"✅ User {uid} deleted.")
    except:
        await message.answer("❌ Invalid user ID.")
    await state.clear()

# --- SEARCH USER ---
@dp.callback_query(F.data == "admin_searchuser")
async def admin_searchuser_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🔍 <b>Search User</b>\n\nEnter username or user ID to search:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_user_search)
    await callback.answer()

@dp.message(Form.waiting_for_user_search)
async def search_user_handler(message: types.Message, state: FSMContext):
    query = message.text.strip()
    users = await search_users(query)
    if not users:
        await message.answer("❌ No users found.")
    else:
        text = f"🔍 <b>Search Results for '{query}'</b>\n\n"
        for uid, username, credits in users[:15]:
            text += f"🆔 <code>{uid}</code> - @{username or 'N/A'} - {credits} credits\n"
        if len(users) > 15:
            text += f"\n... and {len(users)-15} more results"
        await message.answer(text, parse_mode="HTML")
    await state.clear()

# --- LIST USERS ---
@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: types.CallbackQuery):
    users = await get_all_users()
    total_users = len(users)
    page = 1
    per_page = 10
    total_pages = (total_users + per_page - 1) // per_page
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    text = f"👥 <b>Users List (Page {page}/{total_pages})</b>\n\n"
    for i, user_id in enumerate(users[start_idx:end_idx], start=start_idx+1):
        user_data = await get_user(user_id)
        if user_data:
            text += f"{i}. <code>{user_data[0]}</code> - @{user_data[1] or 'N/A'} - {user_data[2]} credits\n"
    text += f"\nTotal Users: {total_users}"
    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"users_{page-1}"))
    if page < total_pages:
        buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"users_{page+1}"))
    await callback.message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[buttons]) if buttons else None)

# --- RECENT USERS ---
@dp.callback_query(F.data == "admin_recentusers")
async def admin_recentusers_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("📈 <b>Recent Users</b>\n\nEnter number of days:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_recent_days)
    await callback.answer()

@dp.message(Form.waiting_for_recent_days)
async def recent_users_days_handler(message: types.Message, state: FSMContext):
    try:
        days = int(message.text.strip())
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        users = await get_users_in_range(start_date.timestamp(), end_date.timestamp())
        if not users:
            await message.answer(f"❌ No users found in last {days} days.")
        else:
            text = f"📅 <b>Recent Users (Last {days} days)</b>\n\n"
            for user in users[:20]:
                join_date = datetime.fromtimestamp(float(user[3])).strftime('%d-%m-%Y')
                text += f"• <code>{user[0]}</code> - @{user[1] or 'N/A'} - {join_date}\n"
            if len(users) > 20:
                text += f"\n... and {len(users)-20} more"
            await message.answer(text, parse_mode="HTML")
    except:
        await message.answer("❌ Invalid number of days.")
    await state.clear()

# --- USER LOOKUPS ---
@dp.callback_query(F.data == "admin_userlookups")
async def admin_userlookups_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("📊 <b>User Lookup History</b>\n\nEnter user ID:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_user_lookups)
    await callback.answer()

@dp.message(Form.waiting_for_user_lookups)
async def user_lookups_handler(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        lookups = await get_user_lookups(uid, 20)
        if not lookups:
            await message.answer(f"❌ No lookups found for user {uid}.")
            return
        text = f"📊 <b>Recent Lookups for User {uid}</b>\n\n"
        for i, (api_type, input_data, lookup_date) in enumerate(lookups, 1):
            date_str = datetime.fromisoformat(lookup_date).strftime('%d/%m %H:%M')
            text += f"{i}. {api_type.upper()}: {input_data} - {date_str}\n"
        if len(text) > 4000:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
                f.write(text)
                temp_file = f.name
            await message.reply_document(FSInputFile(temp_file), caption=f"Lookup history for user {uid}")
            os.unlink(temp_file)
        else:
            await message.answer(text, parse_mode="HTML")
    except:
        await message.answer("❌ Invalid user ID.")
    await state.clear()

# --- LEADERBOARD ---
@dp.callback_query(F.data == "admin_leaderboard")
async def admin_leaderboard(callback: types.CallbackQuery):
    leaderboard = await get_leaderboard(10)
    if not leaderboard:
        await callback.message.answer("❌ No users found.")
        return
    text = "🏆 <b>Credits Leaderboard</b>\n\n"
    for i, (uid, username, credits) in enumerate(leaderboard, 1):
        medal = "🥇" if i == 1 else ("🥈" if i == 2 else ("🥉" if i == 3 else f"{i}."))
        text += f"{medal} <code>{uid}</code> - @{username or 'N/A'} - {credits} credits\n"
    await callback.message.answer(text, parse_mode="HTML")

# --- PREMIUM USERS (100+ credits) ---
@dp.callback_query(F.data == "admin_premiumusers")
async def admin_premiumusers(callback: types.CallbackQuery):
    users = await get_users_with_min_credits(100)
    if not users:
        await callback.message.answer("❌ No premium users (100+ credits) found.")
        return
    text = "💰 <b>Premium Users (100+ credits)</b>\n\n"
    for uid, username, credits in users[:20]:
        text += f"• <code>{uid}</code> - @{username or 'N/A'} - {credits} credits\n"
    if len(users) > 20:
        text += f"\n... and {len(users)-20} more"
    await callback.message.answer(text, parse_mode="HTML")

# --- LOW CREDIT USERS ---
@dp.callback_query(F.data == "admin_lowcredit")
async def admin_lowcredit(callback: types.CallbackQuery):
    users = await get_low_credit_users()
    if not users:
        await callback.message.answer("✅ No users with low credits.")
        return
    text = "📉 <b>Users with Low Credits (≤5 credits)</b>\n\n"
    for uid, username, credits in users[:20]:
        text += f"• <code>{uid}</code> - @{username or 'N/A'} - {credits} credits\n"
    if len(users) > 20:
        text += f"\n... and {len(users)-20} more"
    await callback.message.answer(text, parse_mode="HTML")

# --- INACTIVE USERS ---
@dp.callback_query(F.data == "admin_inactiveusers")
async def admin_inactiveusers_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("⏰ <b>Inactive Users</b>\n\nEnter number of days (default 30):", parse_mode="HTML")
    await state.set_state(Form.waiting_for_inactive_days)
    await callback.answer()

@dp.message(Form.waiting_for_inactive_days)
async def inactive_users_days_handler(message: types.Message, state: FSMContext):
    try:
        days = int(message.text.strip()) if message.text.strip().isdigit() else 30
        users = await get_inactive_users(days)
        if not users:
            await message.answer(f"✅ No inactive users found (last {days} days).")
            return
        text = f"⏰ <b>Inactive Users (Last {days} days)</b>\n\n"
        for uid, username, last_active in users[:15]:
            last_active_dt = datetime.fromisoformat(last_active)
            days_ago = (datetime.now() - last_active_dt).days
            text += f"• <code>{uid}</code> - @{username or 'N/A'} - {days_ago} days ago\n"
        if len(users) > 15:
            text += f"\n... and {len(users)-15} more inactive users"
        await message.answer(text, parse_mode="HTML")
    except:
        await message.answer("❌ Invalid input.")
    await state.clear()

# --- ADD PREMIUM (via button) ---
@dp.callback_query(F.data == "add_premium")
async def add_premium_callback(callback: types.CallbackQuery, state: FSMContext):
    if not await is_user_owner(callback.from_user.id):
        await callback.answer("Owner only!", show_alert=True)
        return
    await callback.message.answer("➕ Enter user ID and optional days (e.g., 123456 30):")
    await state.set_state(Form.waiting_for_add_premium)
    await callback.answer()

@dp.message(Form.waiting_for_add_premium)
async def add_premium_handler(message: types.Message, state: FSMContext):
    try:
        parts = message.text.split()
        uid = int(parts[0])
        days = int(parts[1]) if len(parts) > 1 else None
        await set_user_premium(uid, days)
        await message.reply(f"✅ Premium added for {uid}" + (f" for {days} days." if days else " permanently."))
        try:
            await bot.send_message(uid, "🎉 You are now a premium user!", parse_mode="HTML")
        except:
            pass
    except Exception as e:
        await message.reply(f"❌ Error: {e}")
    await state.clear()

# --- REMOVE PREMIUM (via button) ---
@dp.callback_query(F.data == "remove_premium")
async def remove_premium_callback(callback: types.CallbackQuery, state: FSMContext):
    if not await is_user_owner(callback.from_user.id):
        await callback.answer("Owner only!", show_alert=True)
        return
    await callback.message.answer("➖ Enter user ID:")
    await state.set_state(Form.waiting_for_remove_premium)
    await callback.answer()

@dp.message(Form.waiting_for_remove_premium)
async def remove_premium_handler(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        await remove_user_premium(uid)
        await message.reply(f"✅ Premium removed from {uid}.")
        try:
            await bot.send_message(uid, "⚠️ Your premium status has been removed.", parse_mode="HTML")
        except:
            pass
    except Exception as e:
        await message.reply(f"❌ Error: {e}")
    await state.clear()

# --- SET PLAN PRICE (via button) ---
@dp.callback_query(F.data == "set_plan_price")
async def set_plan_price_callback(callback: types.CallbackQuery, state: FSMContext):
    if not await is_user_owner(callback.from_user.id):
        await callback.answer("Owner only!", show_alert=True)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Weekly", callback_data="set_price_weekly")],
        [InlineKeyboardButton(text="📆 Monthly", callback_data="set_price_monthly")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="admin_back")]
    ])
    await callback.message.edit_text("💰 Select plan to modify:", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("set_price_"))
async def set_price_input(callback: types.CallbackQuery, state: FSMContext):
    plan = callback.data.split("_")[2]
    await state.update_data(plan_type=plan)
    await callback.message.answer(f"Enter new price for {plan.capitalize()} plan (₹):")
    await state.set_state(Form.waiting_for_plan_price)
    await callback.answer()

@dp.message(Form.waiting_for_plan_price)
async def set_price_handler(message: types.Message, state: FSMContext):
    try:
        price = int(message.text)
        data = await state.get_data()
        plan = data.get('plan_type')
        await update_plan_price(plan, price)
        await message.reply(f"✅ {plan.capitalize()} plan price set to ₹{price}.")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")
    await state.clear()

# --- CREATE OFFER (via button) ---
@dp.callback_query(F.data == "create_offer")
async def create_offer_callback(callback: types.CallbackQuery, state: FSMContext):
    if not await is_user_owner(callback.from_user.id):
        await callback.answer("Owner only!", show_alert=True)
        return
    await callback.message.answer("🎟️ Enter offer details in format: CODE PLAN DISCOUNT% MAX_USES [EXPIRY]\nExample: OFFER10 weekly 10 5 7d")
    await state.set_state(Form.waiting_for_offer_details)
    await callback.answer()

@dp.message(Form.waiting_for_offer_details)
async def create_offer_handler(message: types.Message, state: FSMContext):
    try:
        parts = message.text.split()
        code = parts[0].upper()
        plan = parts[1].lower()
        discount = int(parts[2])
        if discount < 0 or discount > 100:
            await message.reply("❌ Discount must be between 0 and 100.")
            return
        max_uses = int(parts[3])
        expiry = parse_time_string(parts[4]) if len(parts) > 4 else None
        await create_discount_code(code, plan, discount, max_uses, expiry)
        await message.reply(f"✅ Offer code {code} created for {plan} plan with {discount}% off.")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")
    await state.clear()

# --- GENERATE RANDOM CODE ---
@dp.callback_query(F.data == "admin_gencode")
async def admin_gencode_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🎲 <b>Generate Random Code</b>\n\nEnter amount of credits:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_gencode_amount)
    await callback.answer()

@dp.message(Form.waiting_for_gencode_amount)
async def gencode_amount_handler(message: types.Message, state: FSMContext):
    try:
        amount = int(message.text)
        await state.update_data(gencode_amount=amount)
        await message.answer("Enter max number of uses:")
        await state.set_state(Form.waiting_for_gencode_uses)
    except:
        await message.answer("❌ Invalid amount.")

@dp.message(Form.waiting_for_gencode_uses)
async def gencode_uses_handler(message: types.Message, state: FSMContext):
    try:
        uses = int(message.text)
        await state.update_data(gencode_uses=uses)
        await message.answer("Enter expiry time (e.g., 30m, 2h, 1h30m) or send 'none' for no expiry:")
        await state.set_state(Form.waiting_for_gencode_expiry)
    except:
        await message.answer("❌ Invalid number of uses.")

@dp.message(Form.waiting_for_gencode_expiry)
async def gencode_expiry_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    amount = data['gencode_amount']
    uses = data['gencode_uses']
    expiry_input = message.text.strip().lower()
    if expiry_input == 'none':
        expiry_minutes = None
    else:
        expiry_minutes = parse_time_string(expiry_input)
        if expiry_minutes is None:
            await message.answer("❌ Invalid time format. Use like 30m, 2h, or send 'none'.")
            return
    code = f"PRO-{secrets.token_hex(3).upper()}"
    await create_redeem_code(code, amount, uses, expiry_minutes)
    expiry_text = ""
    if expiry_minutes:
        if expiry_minutes < 60:
            expiry_text = f"⏰ Expires in: {expiry_minutes} minutes"
        else:
            hours = expiry_minutes // 60
            mins = expiry_minutes % 60
            expiry_text = f"⏰ Expires in: {hours}h {mins}m"
    else:
        expiry_text = "⏰ No expiry"
    await message.answer(
        f"✅ <b>Random Code Created!</b>\n\n"
        f"🎫 <b>Code:</b> <code>{code}</code>\n"
        f"💰 <b>Amount:</b> {amount} credits\n"
        f"👥 <b>Max Uses:</b> {uses}\n"
        f"{expiry_text}",
        parse_mode="HTML"
    )
    await state.clear()

# --- CUSTOM CODE ---
@dp.callback_query(F.data == "admin_customcode")
async def admin_customcode_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "🎫 <b>Custom Code</b>\n\n"
        "Enter details in format: <code>CODE AMOUNT USES [TIME]</code>\n"
        "Examples:\n"
        "• <code>WELCOME50 50 10</code>\n"
        "• <code>FLASH100 100 5 15m</code>",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_custom_code)
    await callback.answer()

@dp.message(Form.waiting_for_custom_code)
async def custom_code_handler(message: types.Message, state: FSMContext):
    try:
        parts = message.text.strip().split()
        code = parts[0].upper()
        amt = int(parts[1])
        uses = int(parts[2])
        expiry_minutes = parse_time_string(parts[3]) if len(parts) >= 4 else None
        await create_redeem_code(code, amt, uses, expiry_minutes)
        expiry_text = ""
        if expiry_minutes:
            if expiry_minutes < 60:
                expiry_text = f"⏰ Expires in: {expiry_minutes} minutes"
            else:
                hours = expiry_minutes // 60
                mins = expiry_minutes % 60
                expiry_text = f"⏰ Expires in: {hours}h {mins}m"
        else:
            expiry_text = "⏰ No expiry"
        await message.answer(
            f"✅ <b>Custom Code Created!</b>\n\n"
            f"🎫 <b>Code:</b> <code>{code}</code>\n"
            f"💰 <b>Amount:</b> {amt} credits\n"
            f"👥 <b>Max Uses:</b> {uses}\n"
            f"{expiry_text}",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"❌ Error: {e}")
    await state.clear()

# --- LIST CODES ---
@dp.callback_query(F.data == "admin_listcodes")
async def admin_listcodes(callback: types.CallbackQuery):
    codes = await get_all_codes()
    if not codes:
        await callback.message.answer("❌ No redeem codes found.")
        return
    text = "🎫 <b>All Redeem Codes</b>\n\n"
    for code_data in codes:
        code, amount, max_uses, current_uses, expiry_minutes, created_date, is_active = code_data
        status = "✅ Active" if is_active else "❌ Inactive"
        expiry_text = ""
        if expiry_minutes:
            created_dt = datetime.fromisoformat(created_date)
            expiry_dt = created_dt + timedelta(minutes=expiry_minutes)
            if expiry_dt > datetime.now():
                time_left = expiry_dt - datetime.now()
                hours = time_left.seconds // 3600
                minutes = (time_left.seconds % 3600) // 60
                expiry_text = f"⏳ {hours}h {minutes}m left"
            else:
                expiry_text = "⌛️ Expired"
        else:
            expiry_text = "♾️ No expiry"
        text += (
            f"🎟 <b>{code}</b> ({status})\n"
            f"💰 Amount: {amount} | 👥 Uses: {current_uses}/{max_uses}\n"
            f"{expiry_text}\n"
            f"📅 Created: {datetime.fromisoformat(created_date).strftime('%d/%m/%y %H:%M')}\n"
            f"{'-'*30}\n"
        )
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await callback.message.answer(part, parse_mode="HTML")
    else:
        await callback.message.answer(text, parse_mode="HTML")

# --- ACTIVE CODES ---
@dp.callback_query(F.data == "admin_activecodes")
async def admin_activecodes(callback: types.CallbackQuery):
    codes = await get_active_codes()
    if not codes:
        await callback.message.answer("✅ No active codes found.")
        return
    text = "✅ <b>Active Codes</b>\n\n"
    for code, amount, max_uses, current_uses in codes[:10]:
        text += f"🎟 <code>{code}</code> - {amount} credits ({current_uses}/{max_uses})\n"
    if len(codes) > 10:
        text += f"\n... and {len(codes)-10} more active codes"
    await callback.message.answer(text, parse_mode="HTML")

# --- INACTIVE CODES ---
@dp.callback_query(F.data == "admin_inactivecodes")
async def admin_inactivecodes(callback: types.CallbackQuery):
    codes = await get_inactive_codes()
    if not codes:
        await callback.message.answer("❌ No inactive codes found.")
        return
    text = "❌ <b>Inactive Codes</b>\n\n"
    for code, amount, max_uses, current_uses in codes[:10]:
        text += f"🎟 <code>{code}</code> - {amount} credits ({current_uses}/{max_uses})\n"
    if len(codes) > 10:
        text += f"\n... and {len(codes)-10} more inactive codes"
    await callback.message.answer(text, parse_mode="HTML")

# --- DEACTIVATE CODE ---
@dp.callback_query(F.data == "admin_deactivatecode")
async def admin_deactivatecode_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🚫 <b>Deactivate Code</b>\n\nEnter code to deactivate:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_code_deactivate)
    await callback.answer()

@dp.message(Form.waiting_for_code_deactivate)
async def deactivate_code_handler(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    await deactivate_code(code)
    await message.answer(f"✅ Code <code>{code}</code> has been deactivated.", parse_mode="HTML")
    await state.clear()

# --- CODE STATS ---
@dp.callback_query(F.data == "admin_codestats")
async def admin_codestats_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("📊 <b>Code Statistics</b>\n\nEnter code:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_code_stats)
    await callback.answer()

@dp.message(Form.waiting_for_code_stats)
async def code_stats_handler(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    stats = await get_code_usage_stats(code)
    if stats:
        amount, max_uses, current_uses, unique_users, user_ids = stats
        msg = (f"📊 <b>Code Statistics: {code}</b>\n\n"
               f"💰 <b>Amount:</b> {amount} credits\n"
               f"🎯 <b>Uses:</b> {current_uses}/{max_uses}\n"
               f"👥 <b>Unique Users:</b> {unique_users}\n"
               f"🆔 <b>Users:</b> {user_ids or 'None'}")
        await message.answer(msg, parse_mode="HTML")
    else:
        await message.answer(f"❌ Code {code} not found.")
    await state.clear()

# --- CHECK EXPIRED ---
@dp.callback_query(F.data == "admin_checkexpired")
async def admin_checkexpired(callback: types.CallbackQuery):
    expired = await get_expired_codes()
    if not expired:
        await callback.message.answer("✅ No expired codes found.")
        return
    text = "⌛️ <b>Expired Codes</b>\n\n"
    for code_data in expired:
        code, amount, current_uses, max_uses, expiry_minutes, created_date = code_data
        created_dt = datetime.fromisoformat(created_date)
        expiry_dt = created_dt + timedelta(minutes=expiry_minutes)
        text += (
            f"🎟 <code>{code}</code>\n"
            f"💰 Amount: {amount} | 👥 Used: {current_uses}/{max_uses}\n"
            f"⏰ Expired on: {expiry_dt.strftime('%d/%m/%y %H:%M')}\n"
            f"{'-'*20}\n"
        )
    text += f"\nTotal: {len(expired)} expired codes"
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await callback.message.answer(part, parse_mode="HTML")
    else:
        await callback.message.answer(text, parse_mode="HTML")

# --- CLEAN EXPIRED ---
@dp.callback_query(F.data == "admin_cleanexpired")
async def admin_cleanexpired(callback: types.CallbackQuery):
    if not await is_user_owner(callback.from_user.id):
        await callback.answer("Owner only!", show_alert=True)
        return
    expired = await get_expired_codes()
    if not expired:
        await callback.message.answer("✅ No expired codes found.")
        return
    deleted = 0
    for code_data in expired:
        await delete_redeem_code(code_data[0])
        deleted += 1
    await callback.message.answer(f"🧹 Cleaned {deleted} expired codes.")

# --- BOT STATS ---
@dp.callback_query(F.data == "admin_stats_general")
async def admin_stats_general(callback: types.CallbackQuery):
    stats = await get_bot_stats()
    top_ref = await get_top_referrers(5)
    total_lookups = await get_total_lookups()
    text = f"📊 <b>Bot Statistics</b>\n\n"
    text += f"👥 <b>Total Users:</b> {stats['total_users']}\n"
    text += f"📈 <b>Active Users:</b> {stats['active_users']}\n"
    text += f"💰 <b>Total Credits in System:</b> {stats['total_credits']}\n"
    text += f"🎁 <b>Credits Distributed:</b> {stats['credits_distributed']}\n"
    text += f"🔍 <b>Total Lookups:</b> {total_lookups}\n\n"
    if top_ref:
        text += "🏆 <b>Top 5 Referrers:</b>\n"
        for i, (ref_id, count) in enumerate(top_ref, 1):
            text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
    await callback.message.edit_text(text, parse_mode="HTML")

# --- DAILY STATS ---
@dp.callback_query(F.data == "admin_dailystats")
async def admin_dailystats_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("📅 <b>Daily Statistics</b>\n\nEnter number of days (default 7):", parse_mode="HTML")
    await state.set_state(Form.waiting_for_dailystats_days)
    await callback.answer()

@dp.message(Form.waiting_for_dailystats_days)
async def dailystats_handler(message: types.Message, state: FSMContext):
    try:
        days = int(message.text.strip()) if message.text.strip().isdigit() else 7
        stats = await get_daily_stats(days)
        text = f"📈 <b>Daily Statistics (Last {days} days)</b>\n\n"
        if not stats:
            text += "No statistics available."
        else:
            for date, new_users, lookups in stats:
                text += f"📅 {date}: +{new_users} users, {lookups} lookups\n"
        await message.answer(text, parse_mode="HTML")
    except:
        await message.answer("❌ Invalid input.")
    await state.clear()

# --- LOOKUP STATS ---
@dp.callback_query(F.data == "admin_lookupstats")
async def admin_lookupstats(callback: types.CallbackQuery):
    total_lookups = await get_total_lookups()
    api_stats = await get_lookup_stats()
    text = f"🔍 <b>Lookup Statistics</b>\n\n"
    text += f"📊 <b>Total Lookups:</b> {total_lookups}\n\n"
    if api_stats:
        text += "<b>By API Type:</b>\n"
        for api_type, count in api_stats:
            text += f"• {api_type.upper()}: {count} lookups\n"
    await callback.message.edit_text(text, parse_mode="HTML")

# --- BACKUP USER DATA ---
@dp.callback_query(F.data == "admin_backup")
async def admin_backup_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("💾 <b>Backup User Data</b>\n\nEnter number of days (0 for all data):", parse_mode="HTML")
    await state.set_state(Form.waiting_for_stats_range)
    await callback.answer()

@dp.message(Form.waiting_for_stats_range)
async def backup_handler(message: types.Message, state: FSMContext):
    try:
        days = int(message.text.strip())
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days) if days > 0 else datetime.fromtimestamp(0)
        users = await get_users_in_range(start_date.timestamp(), end_date.timestamp())
        if not users:
            await message.answer(f"❌ No users found for given range.")
            return
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['User ID', 'Username', 'Credits', 'Join Date'])
            for user in users:
                join_date = datetime.fromtimestamp(float(user[3])).strftime('%Y-%m-%d %H:%M:%S')
                writer.writerow([user[0], user[1] or 'N/A', user[2], join_date])
            temp_file = f.name
        await message.reply_document(FSInputFile(temp_file), caption=f"📊 Users data for last {days} days\nTotal users: {len(users)}")
        os.unlink(temp_file)
    except Exception as e:
        await message.answer(f"❌ Error: {e}")
    await state.clear()

# --- TOP REFERRERS ---
@dp.callback_query(F.data == "admin_topref")
async def admin_topref_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🏆 <b>Top Referrers</b>\n\nEnter limit (default 10):", parse_mode="HTML")
    await state.set_state(Form.waiting_for_topref_limit)
    await callback.answer()

@dp.message(Form.waiting_for_topref_limit)
async def topref_handler(message: types.Message, state: FSMContext):
    try:
        limit = int(message.text.strip()) if message.text.strip().isdigit() else 10
        top_ref = await get_top_referrers(limit)
        if not top_ref:
            await message.answer("❌ No referrals yet.")
            return
        text = f"🏆 <b>Top {limit} Referrers</b>\n\n"
        for i, (ref_id, count) in enumerate(top_ref, 1):
            text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
        await message.answer(text, parse_mode="HTML")
    except:
        await message.answer("❌ Invalid input.")
    await state.clear()

# --- ADD ADMIN ---
@dp.callback_query(F.data == "admin_addadmin")
async def admin_addadmin_start(callback: types.CallbackQuery, state: FSMContext):
    if not await is_user_owner(callback.from_user.id):
        await callback.answer("Owner only!", show_alert=True)
        return
    await callback.message.answer("➕ <b>Add Admin</b>\n\nEnter user ID:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_addadmin_id)
    await callback.answer()

@dp.message(Form.waiting_for_addadmin_id)
async def addadmin_handler(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        await add_admin(uid)
        await message.answer(f"✅ User {uid} added as admin.")
    except:
        await message.answer("❌ Invalid user ID.")
    await state.clear()

# --- REMOVE ADMIN ---
@dp.callback_query(F.data == "admin_removeadmin")
async def admin_removeadmin_start(callback: types.CallbackQuery, state: FSMContext):
    if not await is_user_owner(callback.from_user.id):
        await callback.answer("Owner only!", show_alert=True)
        return
    await callback.message.answer("➖ <b>Remove Admin</b>\n\nEnter user ID:", parse_mode="HTML")
    await state.set_state(Form.waiting_for_removeadmin_id)
    await callback.answer()

@dp.message(Form.waiting_for_removeadmin_id)
async def removeadmin_handler(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text)
        if uid == OWNER_ID:
            await message.answer("❌ Cannot remove owner!")
            return
        await remove_admin(uid)
        await message.answer(f"✅ Admin {uid} removed.")
    except:
        await message.answer("❌ Invalid user ID.")
    await state.clear()

# --- LIST ADMINS ---
@dp.callback_query(F.data == "admin_listadmins")
async def admin_listadmins(callback: types.CallbackQuery):
    admins = await get_all_admins()
    text = "👥 <b>Admin List</b>\n\n"
    text += f"👑 <b>Owner:</b> <code>{OWNER_ID}</code>\n\n"
    text += "⚙️ <b>Static Admins:</b>\n"
    for admin_id in ADMIN_IDS:
        if admin_id != OWNER_ID:
            text += f"• <code>{admin_id}</code>\n"
    if admins:
        text += "\n🗃️ <b>Database Admins:</b>\n"
        for user_id, level in admins:
            text += f"• <code>{user_id}</code> - {level}\n"
    await callback.message.edit_text(text, parse_mode="HTML")

# --- SETTINGS ---
@dp.callback_query(F.data == "admin_settings")
async def admin_settings_start(callback: types.CallbackQuery, state: FSMContext):
    if not await is_user_owner(callback.from_user.id):
        await callback.answer("Owner only!", show_alert=True)
        return
    await callback.message.answer(
        "⚙️ <b>Bot Settings</b>\n\n"
        "1. Change bot name\n"
        "2. Update API endpoints\n"
        "3. Modify channel settings\n"
        "4. Adjust credit settings\n\n"
        "Enter setting number to modify:",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_settings)
    await callback.answer()

@dp.message(Form.waiting_for_settings)
async def settings_handler(message: types.Message, state: FSMContext):
    await message.answer("⚙️ <b>Settings updated!</b> (placeholder)", parse_mode="HTML")
    await state.clear()

# --- FULL DB BACKUP ---
@dp.callback_query(F.data == "admin_fulldbbackup")
async def admin_fulldbbackup(callback: types.CallbackQuery):
    if not await is_user_owner(callback.from_user.id):
        await callback.answer("Owner only!", show_alert=True)
        return
    try:
        backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2("nullprotocol.db", backup_name)
        await callback.message.answer_document(FSInputFile(backup_name), caption="💾 Full database backup")
        os.remove(backup_name)
    except Exception as e:
        await callback.message.answer(f"❌ Backup failed: {e}")

# --- PAGINATION FOR USERS LIST ---
@dp.callback_query(F.data.startswith("users_"))
async def users_pagination(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    page = int(callback.data.split("_")[1])
    users = await get_all_users()
    total_users = len(users)
    per_page = 10
    total_pages = (total_users + per_page - 1) // per_page
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    text = f"👥 <b>Users List (Page {page}/{total_pages})</b>\n\n"
    for i, user_id in enumerate(users[start_idx:end_idx], start=start_idx+1):
        user_data = await get_user(user_id)
        if user_data:
            text += f"{i}. <code>{user_data[0]}</code> - @{user_data[1] or 'N/A'} - {user_data[2]} credits\n"
    text += f"\nTotal Users: {total_users}"
    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"users_{page-1}"))
    if page < total_pages:
        buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"users_{page+1}"))
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[buttons]) if buttons else None)

# --- MANUAL BACKUP ---
@dp.callback_query(F.data == "manual_backup")
async def manual_backup_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        await callback.answer("Unauthorized", show_alert=True)
        return
    await callback.message.edit_text("🔄 Taking backup...")
    await daily_backup()
    await callback.message.edit_text("✅ Backup completed and sent to backup channel.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back", callback_data="admin_back")]]))

# --- BULK LOOKUP (Admin/Premium) ---
@dp.callback_query(F.data == "bulk_lookup_admin")
async def bulk_lookup_admin_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    admin = await is_user_admin(user_id)
    premium = await is_user_premium(user_id)
    if not (admin or premium):
        await callback.answer("❌ Admin/Premium only!", show_alert=True)
        return
    # Show API selection (only those present in APIS)
    keyboard = []
    row = []
    for i, api_name in enumerate(APIS.keys(), 1):
        row.append(InlineKeyboardButton(text=api_name.upper(), callback_data=f"bulk_api_{api_name}"))
        if i % 3 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(text="❌ Cancel", callback_data="admin_back")])
    await callback.message.edit_text("📁 Select API for bulk lookup:", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))

@dp.callback_query(F.data.startswith("bulk_api_"))
async def bulk_api_selected(callback: types.CallbackQuery, state: FSMContext):
    api_type = callback.data.split("_")[2]
    await state.update_data(bulk_api_type=api_type)
    await callback.message.answer(f"📁 Send a text file with one {api_type.upper()} input per line:")
    await state.set_state(Form.waiting_for_bulk_file)
    await callback.answer()

@dp.message(Form.waiting_for_bulk_file, F.document)
async def bulk_file_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    api_type = data.get('bulk_api_type')
    if not api_type:
        await message.answer("Session expired.")
        await state.clear()
        return
    file_id = message.document.file_id
    file = await bot.get_file(file_id)
    file_path = file.file_path
    await bot.download_file(file_path, "temp_bulk.txt")
    with open("temp_bulk.txt", 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
    os.remove("temp_bulk.txt")
    if not lines:
        await message.answer("File is empty.")
        await state.clear()
        return
    status = await message.answer(f"🔄 Processing {len(lines)} lookups...")
    results = []
    for i, inp in enumerate(lines):
        raw = await fetch_api_data(api_type, inp)
        results.append({'input': inp, 'data': raw})
        if (i+1) % 10 == 0:
            await status.edit_text(f"Processed {i+1}/{len(lines)}...")
    out_file = f"bulk_{api_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(out_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Input', 'Result (JSON)'])
        for r in results:
            writer.writerow([r['input'], json.dumps(r['data'], ensure_ascii=False)])
    await message.reply_document(FSInputFile(out_file), caption=f"Bulk lookup results for {api_type}")
    os.remove(out_file)
    await status.delete()
    await state.clear()

# --- DAILY BACKUP FUNCTION (for scheduler) ---
async def daily_backup():
    try:
        db_backup = f"backup_db_{datetime.now().strftime('%Y%m%d')}.db"
        shutil.copy2("nullprotocol.db", db_backup)
        csv_backup = f"backup_users_{datetime.now().strftime('%Y%m%d')}.csv"
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("SELECT * FROM users") as cursor:
                rows = await cursor.fetchall()
                col_names = [description[0] for description in cursor.description]
        with open(csv_backup, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            writer.writerows(rows)
        txt_backup = f"backup_stats_{datetime.now().strftime('%Y%m%d')}.txt"
        stats = await get_bot_stats()
        total_lookups = await get_total_lookups()
        with open(txt_backup, 'w', encoding='utf-8') as f:
            f.write(f"Backup Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Users: {stats['total_users']}\n")
            f.write(f"Active Users: {stats['active_users']}\n")
            f.write(f"Total Credits: {stats['total_credits']}\n")
            f.write(f"Credits Distributed: {stats['credits_distributed']}\n")
            f.write(f"Total Lookups: {total_lookups}\n")
        await bot.send_document(BACKUP_CHANNEL, FSInputFile(db_backup))
        await bot.send_document(BACKUP_CHANNEL, FSInputFile(csv_backup))
        await bot.send_document(BACKUP_CHANNEL, FSInputFile(txt_backup))
        os.remove(db_backup)
        os.remove(csv_backup)
        os.remove(txt_backup)
        logging.info("Daily backup successful.")
    except Exception as e:
        logging.error(f"Backup failed: {e}")

# --- CANCEL COMMAND ---
@dp.message(Command("cancel"))
async def cancel_command(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("❌ No active operation to cancel.")
        return
    await state.clear()
    await message.answer("✅ Operation cancelled.", reply_markup=get_main_menu(message.from_user.id))

# --- MAIN FUNCTION ---
async def main():
    keep_alive()
    await init_db()
    for aid in ADMIN_IDS:
        if aid != OWNER_ID:
            await add_admin(aid)
    scheduler = AsyncIOScheduler()
    scheduler.add_job(daily_backup, CronTrigger(hour=0, minute=0))
    scheduler.start()
    print("🚀 OSINT FATHER Pro Bot Started...")
    print(f"👑 Owner ID: {OWNER_ID}")
    print(f"👥 Static Admins: {ADMIN_IDS}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
