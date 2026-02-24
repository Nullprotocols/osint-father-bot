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
import threading
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, InputMediaPhoto, InputMediaVideo,
    InputMediaAudio, InputMediaDocument, ReplyKeyboardRemove
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
from flask import Flask, jsonify
from database import (
    init_db, add_user, get_user, update_credits,
    create_redeem_code, redeem_code_db, get_all_users,
    set_ban_status, get_bot_stats, get_users_in_range,
    add_admin, remove_admin, get_all_admins, is_admin,
    get_expired_codes, delete_redeem_code, get_top_referrers,
    deactivate_code, get_all_codes, parse_time_string,
    get_user_by_username, update_username, get_user_stats,
    get_recent_users, get_active_codes, get_inactive_codes,
    delete_user, reset_user_credits, get_user_by_id,
    search_users, get_daily_stats, log_lookup,
    get_lookup_stats, get_total_lookups, get_user_lookups,
    get_premium_users, get_low_credit_users, get_inactive_users,
    update_last_active, get_user_activity, get_leaderboard,
    bulk_update_credits, get_code_usage_stats
)

# --- LOAD ENVIRONMENT VARIABLES ---
load_dotenv()

# --- BOT CONFIGURATION ---
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("BOT_TOKEN environment variable is not set!")

OWNER_ID = int(os.getenv("OWNER_ID", "0"))
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x]

# Channels Config
CHANNELS = [int(x) for x in os.getenv("FORCE_JOIN_CHANNELS", "").split(",") if x]
CHANNEL_LINKS = os.getenv("FORCE_JOIN_LINKS", "").split(",")

# Log Channels
LOG_CHANNELS = {
    'num': os.getenv("LOG_CHANNEL_NUM"),
    'ifsc': os.getenv("LOG_CHANNEL_IFSC"),
    'email': os.getenv("LOG_CHANNEL_EMAIL"),
    'gst': os.getenv("LOG_CHANNEL_GST"),
    'vehicle': os.getenv("LOG_CHANNEL_VEHICLE"),
    'pincode': os.getenv("LOG_CHANNEL_PINCODE"),
    'instagram': os.getenv("LOG_CHANNEL_INSTAGRAM"),
    'github': os.getenv("LOG_CHANNEL_GITHUB"),
    'pakistan': os.getenv("LOG_CHANNEL_PAKISTAN"),
    'ip': os.getenv("LOG_CHANNEL_IP"),
    'ff_info': os.getenv("LOG_CHANNEL_FF_INFO"),
    'ff_ban': os.getenv("LOG_CHANNEL_FF_BAN")
}

# APIs
APIS = {
    'num': os.getenv("API_NUM"),
    'ifsc': os.getenv("API_IFSC"),
    'email': os.getenv("API_EMAIL"),
    'gst': os.getenv("API_GST"),
    'vehicle': os.getenv("API_VEHICLE"),
    'pincode': os.getenv("API_PINCODE"),
    'instagram': os.getenv("API_INSTAGRAM"),
    'github': os.getenv("API_GITHUB"),
    'pakistan': os.getenv("API_PAKISTAN"),
    'ip': os.getenv("API_IP"),
    'ff_info': os.getenv("API_FF_INFO"),
    'ff_ban': os.getenv("API_FF_BAN")
}

# --- INITIALIZE BOT ---
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- FLASK APP FOR RENDER HEALTH CHECK ---
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    return jsonify({
        'status': 'Bot is running',
        'bot_name': 'OSINT FATHER Pro',
        'timestamp': datetime.now().isoformat(),
        'developer': '@Nullprotocol_X'
    })

@flask_app.route('/health')
def health():
    return jsonify({'status': 'healthy'})

@flask_app.route('/stats')
async def stats():
    """Quick stats endpoint for monitoring"""
    try:
        stats_data = await get_bot_stats()
        total_lookups = await get_total_lookups()
        return jsonify({
            'total_users': stats_data['total_users'],
            'active_users': stats_data['active_users'],
            'total_lookups': total_lookups,
            'status': 'ok'
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

# --- FSM STATES ---
class Form(StatesGroup):
    waiting_for_redeem = State()
    waiting_for_broadcast = State()
    waiting_for_direct_message = State()
    waiting_for_dm_user = State()
    waiting_for_dm_content = State()
    waiting_for_custom_code = State()
    waiting_for_stats_range = State()
    waiting_for_code_deactivate = State()
    waiting_for_api_input = State()
    waiting_for_api_type = State()
    waiting_for_username = State()
    waiting_for_delete_user = State()
    waiting_for_reset_credits = State()
    waiting_for_bulk_message = State()
    waiting_for_code_stats = State()
    waiting_for_user_lookups = State()
    waiting_for_bulk_gift = State()
    waiting_for_user_search = State()
    waiting_for_settings = State()

# --- HELPER FUNCTIONS ---
def get_branding():
    return {
        "meta": {
            "developer": "@Nullprotocol_X",
            "powered_by": "NULL PROTOCOL",
            "timestamp": datetime.now().isoformat()
        }
    }

def clean_api_response(data):
    """Remove other developer names from API response"""
    if isinstance(data, dict):
        cleaned = {}
        for key, value in data.items():
            if isinstance(value, str):
                # Remove @patelkrish_99 and other unwanted mentions
                if any(unwanted in value.lower() for unwanted in ['@patelkrish_99', 'patelkrish_99', 't.me/anshapi', 'anshapi', '"@Kon_Hu_Mai"', 'Dm to buy access', '"Dm to buy access"', 'Kon_Hu_Mai']):
                    # Skip this value
                    continue
                # Remove credit mentions except ours
                if 'credit' in value.lower() and 'nullprotocol' not in value.lower():
                    continue
                cleaned[key] = value
            elif isinstance(value, dict):
                cleaned[key] = clean_api_response(value)
            elif isinstance(value, list):
                cleaned[key] = [clean_api_response(item) if isinstance(item, dict) else item for item in value]
            else:
                cleaned[key] = value
        return cleaned
    elif isinstance(data, list):
        return [clean_api_response(item) if isinstance(item, dict) else item for item in data]
    return data

def format_json_for_display(data, max_length=3500):
    """Format JSON for display, truncate if too long"""
    formatted_json = json.dumps(data, indent=4, ensure_ascii=False)
    if len(formatted_json) > max_length:
        truncated = formatted_json[:max_length]
        truncated += f"\n\n... [Data truncated, {len(formatted_json) - max_length} characters more]"
        return truncated, True
    return formatted_json, False

def create_readable_txt_file(raw_data, api_type, input_data):
    """Create readable TXT file from data"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        f.write(f"🔍 {api_type.upper()} Lookup Results\n")
        f.write(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"🔎 Input: {input_data}\n")
        f.write("="*50 + "\n\n")
        
        def write_readable(obj, indent=0, file=f):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    file.write("  " * indent + f"• {key}: ")
                    if isinstance(value, (dict, list)):
                        file.write("\n")
                        write_readable(value, indent + 1, file)
                    else:
                        file.write(f"{value}\n")
            elif isinstance(obj, list):
                for i, item in enumerate(obj, 1):
                    file.write("  " * indent + f"{i}. ")
                    if isinstance(item, (dict, list)):
                        file.write("\n")
                        write_readable(item, indent + 1, file)
                    else:
                        file.write(f"{item}\n")
            else:
                file.write(f"{obj}\n")
        
        write_readable(raw_data)
        f.write("\n" + "="*50 + "\n")
        f.write("👨‍💻 Developer: @Nullprotocol_X\n")
        f.write("⚡ Powered by: NULL PROTOCOL\n")
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
    if user and user[5] == 1:
        return True
    return False

async def check_membership(user_id):
    admin_level = await is_user_admin(user_id)
    if admin_level:
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

def get_main_menu(user_id):
    keyboard = [
        [
            InlineKeyboardButton(text="📱 Number", callback_data="api_num"),
            InlineKeyboardButton(text="🏦 IFSC", callback_data="api_ifsc")
        ],
        [
            InlineKeyboardButton(text="📧 Email", callback_data="api_email"),
            InlineKeyboardButton(text="📋 GST", callback_data="api_gst")
        ],
        [
            InlineKeyboardButton(text="🚗 Vehicle", callback_data="api_vehicle"),
            InlineKeyboardButton(text="📮 Pincode", callback_data="api_pincode")
        ],
        [
            InlineKeyboardButton(text="📷 Instagram", callback_data="api_instagram"),
            InlineKeyboardButton(text="🐱 GitHub", callback_data="api_github")
        ],
        [
            InlineKeyboardButton(text="🇵🇰 Pakistan", callback_data="api_pakistan"),
            InlineKeyboardButton(text="🌐 IP Lookup", callback_data="api_ip")
        ],
        [
            InlineKeyboardButton(text="🔥 FF Info", callback_data="api_ff_info"),
            InlineKeyboardButton(text="🚫 FF Ban", callback_data="api_ff_ban")
        ],
        [
            InlineKeyboardButton(text="🎁 Redeem", callback_data="redeem"),
            InlineKeyboardButton(text="🔗 Refer & earn", callback_data="refer_earn")
        ],
        [
            InlineKeyboardButton(text="👤 Profile", callback_data="profile"),
            InlineKeyboardButton(text="💳 Buy Credits", url="https://t.me/Nullprotocol_X")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# --- HANDLERS ---
# (All your existing handlers remain exactly the same from here onwards)

@dp.message(CommandStart())
async def start_command(message: types.Message, command: CommandObject):
    user_id = message.from_user.id
    
    if await is_user_banned(user_id):
        await message.answer("🚫 <b>You are BANNED from using this bot.</b>", parse_mode="HTML")
        return

    existing_user = await get_user(user_id)
    if not existing_user:
        referrer_id = None
        args = command.args
        if args and args.startswith("ref_"):
            try:
                referrer_id = int(args.split("_")[1])
                if referrer_id == user_id: 
                    referrer_id = None
            except: 
                pass
        
        await add_user(user_id, message.from_user.username, referrer_id)
        if referrer_id:
            await update_credits(referrer_id, 3)
            try: 
                await bot.send_message(referrer_id, "🎉 <b>Referral +3 Credits!</b>", parse_mode="HTML")
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
    
    await message.answer(
        welcome_msg,
        reply_markup=get_main_menu(user_id), 
        parse_mode="HTML"
    )
    await update_last_active(user_id)

@dp.callback_query(F.data == "check_join")
async def verify_join(callback: types.CallbackQuery):
    if await check_membership(callback.from_user.id):
        await callback.message.delete()
        await callback.message.answer("✅ <b>Verified!</b>", 
                                    reply_markup=get_main_menu(callback.from_user.id), 
                                    parse_mode="HTML")
    else:
        await callback.answer("❌ Abhi bhi kuch channels join nahi kiye!", show_alert=True)

@dp.callback_query(F.data == "profile")
async def show_profile(callback: types.CallbackQuery):
    user_data = await get_user(callback.from_user.id)
    if not user_data: 
        return
    
    admin_level = await is_user_admin(callback.from_user.id)
    credits = "♾️ Unlimited" if admin_level else user_data[2]
    
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=ref_{user_data[0]}"
    
    stats = await get_user_stats(callback.from_user.id)
    referrals = stats[0] if stats else 0
    codes_claimed = stats[1] if stats else 0
    total_from_codes = stats[2] if stats else 0
    
    msg = (f"👤 <b>User Profile</b>\n\n"
           f"🆔 <b>ID:</b> <code>{user_data[0]}</code>\n"
           f"👤 <b>Username:</b> @{user_data[1] or 'N/A'}\n"
           f"💰 <b>Credits:</b> {credits}\n"
           f"📊 <b>Total Earned:</b> {user_data[6]}\n"
           f"👥 <b>Referrals:</b> {referrals}\n"
           f"🎫 <b>Codes Claimed:</b> {codes_claimed}\n"
           f"📅 <b>Joined:</b> {datetime.fromtimestamp(float(user_data[3])).strftime('%d-%m-%Y')}\n"
           f"🔗 <b>Referral Link:</b>\n<code>{link}</code>")
    
    await callback.message.edit_text(msg, parse_mode="HTML", 
                                   reply_markup=get_main_menu(callback.from_user.id))

@dp.callback_query(F.data == "refer_earn")
async def refer_earn_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
    
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
    
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Back", callback_data="back_home")]
    ])
    await callback.message.edit_text(msg, parse_mode="HTML", reply_markup=back_kb)

@dp.callback_query(F.data == "back_home")
async def go_home(callback: types.CallbackQuery):
    await callback.message.edit_text(
        f"🔓 <b>Main Menu</b>",
        reply_markup=get_main_menu(callback.from_user.id), parse_mode="HTML"
    )

@dp.callback_query(F.data == "redeem")
async def redeem_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "🎁 <b>Redeem Code</b>\n\n"
        "Enter your redeem code below:\n\n"
        "📌 <i>Note: Each code can be used only once per user</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_redeem")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_redeem)
    await callback.answer()

@dp.callback_query(F.data == "cancel_redeem")
async def cancel_redeem_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer("❌ Operation Cancelled.", 
                                reply_markup=get_main_menu(callback.from_user.id))

async def process_api_call(message: types.Message, api_type: str, input_data: str):
    user_id = message.from_user.id
    
    if await is_user_banned(user_id): 
        return

    user = await get_user(user_id)
    admin_level = await is_user_admin(user_id)
    
    if not admin_level and user[2] < 1:
        await message.reply("❌ <b>Insufficient Credits!</b>", parse_mode="HTML")
        return
    
    if not APIS.get(api_type):
        await message.reply("❌ <b>API service is currently unavailable. Please contact admin.</b>", parse_mode="HTML")
        return
    
    if api_type in ['ff_info', 'ff_ban']:
        cleaned_input = ''.join(filter(str.isdigit, input_data))
        if not cleaned_input:
            await message.reply("❌ <b>Invalid UID format! Please enter numeric UID only.</b>", parse_mode="HTML")
            return
        input_data = cleaned_input

    status_msg = await message.reply("🔄 <b>Fetching Data...</b>", parse_mode="HTML")
    
    try:
        async with httpx.AsyncClient() as client:
            if api_type in ['ff_info', 'ff_ban']:
                url_formats = [
                    f"{APIS[api_type]}{input_data}",
                    f"{APIS[api_type]}?uid={input_data}",
                    f"{APIS[api_type]}&uid={input_data}",
                    f"{APIS[api_type]}?query={input_data}"
                ]
                
                response = None
                last_error = None
                
                for url in url_formats:
                    try:
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                            'Accept': 'application/json'
                        }
                        
                        resp = await client.get(url, headers=headers, timeout=15)
                        
                        if resp.status_code == 200:
                            response = resp
                            break
                    except Exception as e:
                        last_error = e
                        continue
                
                if not response:
                    raise Exception(f"All URL formats failed. Last error: {last_error}")
                
                resp = response
            else:
                url = f"{APIS[api_type]}{input_data}"
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                resp = await client.get(url, headers=headers, timeout=30)
            
            if resp.status_code != 200:
                error_text = resp.text[:200] if resp.text else "No error message"
                raise Exception(f"API Error {resp.status_code}: {error_text}")
            
            try:
                raw_data = resp.json()
            except:
                content_type = resp.headers.get('content-type', '').lower()
                
                if 'html' in content_type:
                    html_text = resp.text
                    json_patterns = [
                        r'var\s+data\s*=\s*({.*?});',
                        r'JSON\.parse\(\'({.*?})\'\)',
                        r'({.*?})'
                    ]
                    
                    for pattern in json_patterns:
                        match = re.search(pattern, html_text, re.DOTALL)
                        if match:
                            try:
                                raw_data = json.loads(match.group(1))
                                break
                            except:
                                continue
                    
                    if 'raw_data' not in locals():
                        raw_data = {"html_response": "Data received but not in JSON format", "content": html_text[:500]}
                else:
                    raw_data = {"text_response": resp.text[:500]}
            
            raw_data = clean_api_response(raw_data)
            
            if api_type == 'num':
                if isinstance(raw_data, dict):
                    raw_data.pop('Dm to buy access', None)
                    raw_data.pop('Owner', None)
                    keys_to_remove = [k for k in raw_data.keys() 
                                      if any(x in k.lower() for x in ['dm to buy', 'owner', '@kon_hu_mai', '@Simpleguy444', 'Simpleguy444', 'Ruk ja bhencho itne m kya unlimited request lega?? Paid lena h to bolo 100-400₹ @Simpleguy444'])]
                    for k in keys_to_remove:
                        raw_data.pop(k, None)
            
            if isinstance(raw_data, dict):
                raw_data.update(get_branding())
            elif isinstance(raw_data, list):
                data = {"results": raw_data}
                data.update(get_branding())
                raw_data = data
            else:
                data = {"data": str(raw_data)}
                data.update(get_branding())
                raw_data = data

    except Exception as e:
        logger.error(f"API call failed for {api_type} with input {input_data}: {e}")
        
        if api_type in ['ff_info', 'ff_ban']:
            raw_data = {
                "uid": input_data,
                "player_name": "Test Player",
                "level": "70",
                "rank": "Heroic",
                "guild": "Test Guild",
                "server": "India",
                "last_seen": datetime.now().strftime('%Y-%m-%d'),
                "status": "Active" if api_type == 'ff_info' else "Not Banned",
                "note": "This is test data. Check your API configuration in .env file."
            }
            raw_data.update(get_branding())
        else:
            raw_data = {"error": "Server Error", "details": str(e)[:200]}
            raw_data.update(get_branding())

    await status_msg.delete()
    
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
                    f"👨‍💻 <b>Developer:</b> @Nullprotocol_X\n"
                    f"⚡ <b>Powered by:</b> NULL PROTOCOL"
                ),
                parse_mode="HTML"
            )
            
            await message.reply_document(
                FSInputFile(txt_file, filename=f"{api_type}_{input_data}_readable.txt"),
                caption=(
                    f"📄 <b>Readable Text Format</b>\n\n"
                    f"<i>Alternative format for easy reading on mobile</i>"
                ),
                parse_mode="HTML"
            )
            
        except Exception as e:
            logger.error(f"Error sending file to user: {e}")
            short_msg = (
                f"🔍 <b>{api_type.upper()} Lookup Results</b>\n\n"
                f"📊 <b>Input:</b> <code>{input_data}</code>\n"
                f"📅 <b>Date:</b> {datetime.now().strftime('%d-%m-%Y %H:%M')}\n\n"
                f"⚠️ <b>Data too large for message</b>\n"
                f"📄 <i>Attempted to send as file but failed</i>\n\n"
                f"👨‍💻 <b>Developer:</b> @Nullprotocol_X\n"
                f"⚡ <b>Powered by:</b> NULL PROTOCOL"
            )
            await message.reply(short_msg, parse_mode="HTML")
    
    else:
        colored_json = (
            f"🔍 <b>{api_type.upper()} Lookup Results</b>\n\n"
            f"📊 <b>Input:</b> <code>{input_data}</code>\n"
            f"📅 <b>Date:</b> {datetime.now().strftime('%d-%m-%Y %H:%M')}\n\n"
        )
        
        if is_truncated:
            colored_json += "⚠️ <i>Response truncated for display</i>\n\n"
        
        colored_json += f"<pre><code class=\"language-json\">{formatted_json}</code></pre>\n\n"
        colored_json += (
            f"📝 <b>Note:</b> Data is for informational purposes only\n"
            f"👨‍💻 <b>Developer:</b> @Nullprotocol_X\n"
            f"⚡ <b>Powered by:</b> NULL PROTOCOL"
        )
        
        await message.reply(colored_json, parse_mode="HTML")

    if not admin_level:
        await update_credits(user_id, -1)
    
    log_data = raw_data.copy()
    if isinstance(log_data, dict) and json_size > 10000:
        for key in log_data:
            if isinstance(log_data[key], list) and len(log_data[key]) > 5:
                log_data[key] = log_data[key][:5]
                log_data[key].append(f"... [truncated, {len(raw_data[key]) - 5} more items]")
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
                
                await bot.send_message(
                    int(log_channel),
                    log_message,
                    parse_mode="HTML"
                )
                
        except Exception as e:
            logger.error(f"Failed to log to channel: {e}")
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

@dp.callback_query(F.data.startswith("api_"))
async def ask_api_input(callback: types.CallbackQuery, state: FSMContext):
    if await is_user_banned(callback.from_user.id): 
        return
    if not await check_membership(callback.from_user.id):
        await callback.answer("❌ Join channels first!", show_alert=True)
        return
    
    api_type = callback.data.split('_')[1]
    
    if api_type not in APIS or not APIS[api_type]:
        await callback.answer("❌ This service is temporarily unavailable", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_api_input)
    await state.update_data(api_type=api_type)
    
    api_map = {
        'num': "📱 Enter Mobile Number (10 digits)",
        'ifsc': "🏦 Enter IFSC Code (11 characters)",
        'email': "📧 Enter Email Address",
        'gst': "📋 Enter GST Number (15 characters)",
        'vehicle': "🚗 Enter Vehicle RC Number",
        'pincode': "📮 Enter Pincode (6 digits)",
        'instagram': "📷 Enter Instagram Username (without @)",
        'github': "🐱 Enter GitHub Username",
        'pakistan': "🇵🇰 Enter Pakistan Mobile Number (with country code)",
        'ip': "🌐 Enter IP Address",
        'ff_info': "🔥 Enter Free Fire UID (numbers only, e.g., 1234567890)",
        'ff_ban': "🚫 Enter Free Fire UID for Ban Check (numbers only, e.g., 1234567890)"
    }
    
    if api_type in api_map:
        instructions = api_map[api_type]
        
        if api_type in ['ff_info', 'ff_ban']:
            extra_info = "\n\n⚠️ <i>Note: Only numeric UID accepted. Letters will be automatically removed.</i>"
        else:
            extra_info = ""
        
        await callback.message.answer(
            f"<b>{instructions}</b>{extra_info}\n\n"
            f"<i>Type /cancel to cancel</i>\n\n"
            f"📄 <i>Note: Large responses will be sent as files</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_api")]
            ])
        )

@dp.callback_query(F.data == "cancel_api")
async def cancel_api_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer("❌ Operation Cancelled.", 
                                reply_markup=get_main_menu(callback.from_user.id))

@dp.message(Form.waiting_for_broadcast)
async def broadcast_message(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        await state.clear()
        return
    
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
                await status.edit_text(
                    f"🚀 Broadcasting to {total} users...\n\n"
                    f"✅ Sent: {sent}\n"
                    f"❌ Failed: {failed}\n"
                    f"📊 Progress: {((sent + failed) / total * 100):.1f}%"
                )
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
    
    await status.edit_text(
        f"✅ <b>Broadcast Complete!</b>\n\n"
        f"✅ Sent: <b>{sent}</b>\n"
        f"❌ Failed: <b>{failed}</b>\n"
        f"👥 Total Users: <b>{total}</b>\n"
        f"📅 <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}",
        parse_mode="HTML"
    )
    await state.clear()

@dp.message(F.text & ~F.text.startswith("/"))
async def handle_inputs(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if await is_user_banned(user_id): 
        return
    
    current_state = await state.get_state()
    
    if current_state == Form.waiting_for_api_input.state:
        data = await state.get_data()
        api_type = data.get('api_type')
        if api_type:
            await process_api_call(message, api_type, message.text.strip())
        await state.clear()
        return
    elif current_state == Form.waiting_for_redeem.state:
        code = message.text.strip().upper()
        result = await redeem_code_db(user_id, code)
        
        if isinstance(result, int):
            user_data = await get_user(user_id)
            new_balance = user_data[2] + result if user_data else result
            await message.answer(
                f"✅ <b>Code Redeemed Successfully!</b>\n"
                f"➕ <b>{result} Credits</b> added to your account.\n\n"
                f"💰 <b>New Balance:</b> {new_balance}",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "already_claimed":
            await message.answer(
                "❌ <b>You have already claimed this code!</b>\n"
                "Each user can claim a code only once.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "invalid":
            await message.answer(
                "❌ <b>Invalid Code!</b>\n"
                "Please check the code and try again.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "inactive":
            await message.answer(
                "❌ <b>Code is Inactive!</b>\n"
                "This code has been deactivated by admin.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "limit_reached":
            await message.answer(
                "❌ <b>Code Limit Reached!</b>\n"
                "This code has been used by maximum users.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "expired":
            await message.answer(
                "❌ <b>Code Expired!</b>\n"
                "This code is no longer valid.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        else:
            await message.answer(
                "❌ <b>Error processing code!</b>\n"
                "Please try again later.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        await state.clear()
        return
    elif current_state == Form.waiting_for_dm_user.state:
        try:
            target_id = int(message.text.strip())
            await state.update_data(dm_user_id=target_id)
            await message.answer(f"📨 Now send the message for user {target_id}:")
            await state.set_state(Form.waiting_for_dm_content)
        except:
            await message.answer("❌ Invalid user ID. Please enter a numeric ID.")
        return
    elif current_state == Form.waiting_for_dm_content.state:
        data = await state.get_data()
        target_id = data.get('dm_user_id')
        if target_id:
            try:
                await message.copy_to(target_id)
                await message.answer(f"✅ Message sent to user {target_id}")
            except Exception as e:
                await message.answer(f"❌ Failed to send message: {str(e)}")
        await state.clear()
        return
    elif current_state == Form.waiting_for_custom_code.state:
        try:
            parts = message.text.strip().split()
            if len(parts) < 3:
                raise ValueError("Minimum 3 arguments required")
            
            code = parts[0].upper()
            amt = int(parts[1])
            uses = int(parts[2])
            expiry_minutes = None
            if len(parts) >= 4:
                expiry_minutes = parse_time_string(parts[3])
            
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
                f"✅ <b>Code Created!</b>\n\n"
                f"🎫 <b>Code:</b> <code>{code}</code>\n"
                f"💰 <b>Amount:</b> {amt} credits\n"
                f"👥 <b>Max Uses:</b> {uses}\n"
                f"{expiry_text}\n\n"
                f"📝 <i>Note: Each user can claim only once</i>",
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(
                f"❌ <b>Error:</b> {str(e)}\n\n"
                f"<b>Format:</b> <code>CODE AMOUNT USES [TIME]</code>\n"
                f"<b>Examples:</b>\n"
                f"• <code>WELCOME50 50 10</code>\n"
                f"• <code>FLASH100 100 5 15m</code>\n"
                f"• <code>SPECIAL200 200 3 1h</code>",
                parse_mode="HTML"
            )
        await state.clear()
        return
    elif current_state == Form.waiting_for_stats_range.state:
        try:
            days = int(message.text.strip())
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            users = await get_users_in_range(start_date.timestamp(), end_date.timestamp())
            if not users:
                await message.answer(f"❌ No users found in last {days} days.")
                return
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['User ID', 'Username', 'Credits', 'Join Date'])
                for user in users:
                    join_date = datetime.fromtimestamp(float(user[3])).strftime('%Y-%m-%d %H:%M:%S')
                    writer.writerow([user[0], user[1] or 'N/A', user[2], join_date])
                temp_file = f.name
            await message.reply_document(
                FSInputFile(temp_file),
                caption=f"📊 Users data for last {days} days\nTotal users: {len(users)}"
            )
            os.unlink(temp_file)
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    elif current_state == Form.waiting_for_code_deactivate.state:
        try:
            code = message.text.strip().upper()
            await deactivate_code(code)
            await message.answer(f"✅ Code <code>{code}</code> has been deactivated.", parse_mode="HTML")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    elif current_state == Form.waiting_for_username.state:
        username = message.text.strip()
        user_id = await get_user_by_username(username)
        if user_id:
            user_data = await get_user(user_id)
            msg = (f"👤 <b>User Found</b>\n\n"
                   f"🆔 <b>ID:</b> <code>{user_data[0]}</code>\n"
                   f"👤 <b>Username:</b> @{user_data[1] or 'N/A'}\n"
                   f"💰 <b>Credits:</b> {user_data[2]}\n"
                   f"📊 <b>Total Earned:</b> {user_data[6]}\n"
                   f"🚫 <b>Banned:</b> {'Yes' if user_data[5] else 'No'}")
            await message.answer(msg, parse_mode="HTML")
        else:
            await message.answer("❌ User not found.")
        await state.clear()
        return
    elif current_state == Form.waiting_for_delete_user.state:
        try:
            uid = int(message.text.strip())
            await delete_user(uid)
            await message.answer(f"✅ User {uid} deleted successfully.")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    elif current_state == Form.waiting_for_reset_credits.state:
        try:
            uid = int(message.text.strip())
            await reset_user_credits(uid)
            await message.answer(f"✅ Credits reset for user {uid}.")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    elif current_state == Form.waiting_for_code_stats.state:
        try:
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
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    elif current_state == Form.waiting_for_user_lookups.state:
        try:
            uid = int(message.text.strip())
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
                await message.reply_document(
                    FSInputFile(temp_file),
                    caption=f"Lookup history for user {uid}"
                )
                os.unlink(temp_file)
            else:
                await message.answer(text, parse_mode="HTML")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    elif current_state == Form.waiting_for_bulk_gift.state:
        try:
            parts = message.text.strip().split()
            if len(parts) < 2:
                raise ValueError("Format: AMOUNT USERID1 USERID2 ...")
            amount = int(parts[0])
            user_ids = [int(uid) for uid in parts[1:]]
            await bulk_update_credits(user_ids, amount)
            msg = f"✅ Gifted {amount} credits to {len(user_ids)} users:\n"
            for uid in user_ids[:10]:
                msg += f"• <code>{uid}</code>\n"
            if len(user_ids) > 10:
                msg += f"... and {len(user_ids) - 10} more"
            await message.answer(msg, parse_mode="HTML")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    elif current_state == Form.waiting_for_user_search.state:
        query = message.text.strip()
        users = await search_users(query)
        if not users:
            await message.answer("❌ No users found.")
            return
        text = f"🔍 <b>Search Results for '{query}'</b>\n\n"
        for user_id, username, credits in users[:15]:
            text += f"🆔 <code>{user_id}</code> - @{username or 'N/A'} - {credits} credits\n"
        if len(users) > 15:
            text += f"\n... and {len(users) - 15} more results"
        await message.answer(text, parse_mode="HTML")
        await state.clear()
        return
    elif current_state == Form.waiting_for_settings.state:
        await message.answer("⚙️ <b>Settings updated!</b>", parse_mode="HTML")
        await state.clear()
        return
    else:
        if message.text.strip():
            await message.answer(
                "Please use the menu buttons to select an option.",
                reply_markup=get_main_menu(user_id)
            )

@dp.message(Form.waiting_for_broadcast, F.content_type.in_({'photo', 'video', 'audio', 'document'}))
async def broadcast_media(message: types.Message, state: FSMContext):
    pass

@dp.message(Command("cancel"))
async def cancel_command(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("❌ No active operation to cancel.")
        return
    await state.clear()
    await message.answer("✅ Operation cancelled.", reply_markup=get_main_menu(message.from_user.id))

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    panel_text = "🛠 <b>ADMIN CONTROL PANEL</b>\n\n"
    panel_text += "<b>📊 User Management:</b>\n"
    panel_text += "📢 <code>/broadcast</code> - Send to all users\n"
    panel_text += "📨 <code>/dm</code> - Direct message to user\n"
    panel_text += "🎁 <code>/gift ID AMOUNT</code> - Add credits\n"
    panel_text += "🎁 <code>/bulkgift AMOUNT ID1 ID2...</code> - Bulk gift\n"
    panel_text += "📉 <code>/removecredits ID AMOUNT</code> - Remove credits\n"
    panel_text += "🔄 <code>/resetcredits ID</code> - Reset user credits to 0\n"
    panel_text += "🚫 <code>/ban ID</code> - Ban user\n"
    panel_text += "🟢 <code>/unban ID</code> - Unban user\n"
    panel_text += "🗑 <code>/deleteuser ID</code> - Delete user\n"
    panel_text += "🔍 <code>/searchuser QUERY</code> - Search users\n"
    panel_text += "👥 <code>/users [PAGE]</code> - List users (10 per page)\n"
    panel_text += "📈 <code>/recentusers DAYS</code> - Recent users\n"
    panel_text += "📊 <code>/userlookups ID</code> - User lookup history\n"
    panel_text += "🏆 <code>/leaderboard</code> - Credits leaderboard\n"
    panel_text += "💰 <code>/premiumusers</code> - Premium users (100+ credits)\n"
    panel_text += "📉 <code>/lowcreditusers</code> - Users with low credits\n"
    panel_text += "⏰ <code>/inactiveusers DAYS</code> - Inactive users\n\n"
    panel_text += "<b>🎫 Code Management:</b>\n"
    panel_text += "🎲 <code>/gencode AMOUNT USES [TIME]</code> - Random code\n"
    panel_text += "🎫 <code>/customcode CODE AMOUNT USES [TIME]</code> - Custom code\n"
    panel_text += "📋 <code>/listcodes</code> - List all codes\n"
    panel_text += "✅ <code>/activecodes</code> - List active codes\n"
    panel_text += "❌ <code>/inactivecodes</code> - List inactive codes\n"
    panel_text += "🚫 <code>/deactivatecode CODE</code> - Deactivate code\n"
    panel_text += "📊 <code>/codestats CODE</code> - Code usage statistics\n"
    panel_text += "⌛️ <code>/checkexpired</code> - Check expired codes\n"
    panel_text += "🧹 <code>/cleanexpired</code> - Remove expired codes\n\n"
    panel_text += "<b>📈 Statistics:</b>\n"
    panel_text += "📊 <code>/stats</code> - Bot statistics\n"
    panel_text += "📅 <code>/dailystats DAYS</code> - Daily statistics\n"
    panel_text += "🔍 <code>/lookupstats</code> - Lookup statistics\n"
    panel_text += "💾 <code>/backup DAYS</code> - Download user data\n"
    panel_text += "🏆 <code>/topref [LIMIT]</code> - Top referrers\n\n"
    
    if admin_level == 'owner':
        panel_text += "<b>👑 Owner Commands:</b>\n"
        panel_text += "➕ <code>/addadmin ID</code> - Add admin\n"
        panel_text += "➖ <code>/removeadmin ID</code> - Remove admin\n"
        panel_text += "👥 <code>/listadmins</code> - List all admins\n"
        panel_text += "⚙️ <code>/settings</code> - Bot settings\n"
        panel_text += "💾 <code>/fulldbbackup</code> - Full database backup\n"
    
    panel_text += "\n<b>⏰ Time Formats:</b>\n"
    panel_text += "• <code>30m</code> = 30 minutes\n"
    panel_text += "• <code>2h</code> = 2 hours\n"
    panel_text += "• <code>1h30m</code> = 1.5 hours\n"
    panel_text += "• <code>1d</code> = 24 hours\n"
    
    buttons = [
        [InlineKeyboardButton(text="📊 Quick Stats", callback_data="quick_stats"),
         InlineKeyboardButton(text="👥 Recent Users", callback_data="recent_users")],
        [InlineKeyboardButton(text="🎫 Active Codes", callback_data="active_codes"),
         InlineKeyboardButton(text="🏆 Top Referrers", callback_data="top_ref")],
        [InlineKeyboardButton(text="🚀 Broadcast", callback_data="broadcast_now"),
         InlineKeyboardButton(text="❌ Close", callback_data="close_panel")]
    ]
    
    await message.answer(panel_text, parse_mode="HTML", 
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.message(Command("broadcast"))
async def broadcast_trigger(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    await message.answer(
        "📢 <b>Send message to broadcast</b> (text, photo, video, audio, document, poll, sticker):\n\n"
        "This will be sent to all users.",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_broadcast)

@dp.message(Command("dm"))
async def dm_trigger(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    await message.answer("👤 <b>Enter user ID to send message:</b>")
    await state.set_state(Form.waiting_for_dm_user)

@dp.message(Command("users"))
async def users_list(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    page = 1
    if command.args and command.args.isdigit():
        page = int(command.args)
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
    if buttons:
        await message.answer(text, parse_mode="HTML", 
                           reply_markup=InlineKeyboardMarkup(inline_keyboard=[buttons]))
    else:
        await message.answer(text, parse_mode="HTML")

@dp.message(Command("searchuser"))
async def search_user_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    await message.answer("🔍 <b>Enter username or user ID to search:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_user_search)

@dp.message(Command("deleteuser"))
async def delete_user_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    await message.answer("🗑 <b>Enter user ID to delete:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_delete_user)

@dp.message(Command("resetcredits"))
async def reset_credits_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    await message.answer("🔄 <b>Enter user ID to reset credits:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_reset_credits)

@dp.message(Command("recentusers"))
async def recent_users_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    days = 7
    if command.args and command.args.isdigit():
        days = int(command.args)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    users = await get_users_in_range(start_date.timestamp(), end_date.timestamp())
    text = f"📅 <b>Recent Users (Last {days} days)</b>\n\n"
    if not users:
        text += "No users found."
    else:
        for user in users[:20]:
            join_date = datetime.fromtimestamp(float(user[3])).strftime('%d-%m-%Y')
            text += f"• <code>{user[0]}</code> - @{user[1] or 'N/A'} - {join_date}\n"
        if len(users) > 20:
            text += f"\n... and {len(users) - 20} more"
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("activecodes"))
async def active_codes_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    codes = await get_active_codes()
    if not codes:
        await message.reply("✅ No active codes found.")
        return
    text = "✅ <b>Active Redeem Codes</b>\n\n"
    for code_data in codes[:10]:
        code, amount, max_uses, current_uses = code_data
        text += f"🎟 <code>{code}</code> - {amount} credits ({current_uses}/{max_uses})\n"
    if len(codes) > 10:
        text += f"\n... and {len(codes) - 10} more active codes"
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("inactivecodes"))
async def inactive_codes_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    codes = await get_inactive_codes()
    if not codes:
        await message.reply("❌ No inactive codes found.")
        return
    text = "❌ <b>Inactive Redeem Codes</b>\n\n"
    for code_data in codes[:10]:
        code, amount, max_uses, current_uses = code_data
        text += f"🎟 <code>{code}</code> - {amount} credits ({current_uses}/{max_uses})\n"
    if len(codes) > 10:
        text += f"\n... and {len(codes) - 10} more inactive codes"
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("leaderboard"))
async def leaderboard_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    leaderboard = await get_leaderboard(10)
    if not leaderboard:
        await message.reply("❌ No users found.")
        return
    text = "🏆 <b>Credits Leaderboard</b>\n\n"
    for i, (user_id, username, credits) in enumerate(leaderboard, 1):
        medal = "🥇" if i == 1 else ("🥈" if i == 2 else ("🥉" if i == 3 else f"{i}."))
        text += f"{medal} <code>{user_id}</code> - @{username or 'N/A'} - {credits} credits\n"
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("dailystats"))
async def daily_stats_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    days = 7
    if command.args and command.args.isdigit():
        days = int(command.args)
    stats = await get_daily_stats(days)
    text = f"📈 <b>Daily Statistics (Last {days} days)</b>\n\n"
    if not stats:
        text += "No statistics available."
    else:
        for date, new_users, lookups in stats:
            text += f"📅 {date}: +{new_users} users, {lookups} lookups\n"
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("lookupstats"))
async def lookup_stats_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    total_lookups = await get_total_lookups()
    api_stats = await get_lookup_stats()
    text = f"🔍 <b>Lookup Statistics</b>\n\n"
    text += f"📊 <b>Total Lookups:</b> {total_lookups}\n\n"
    if api_stats:
        text += "<b>By API Type:</b>\n"
        for api_type, count in api_stats:
            text += f"• {api_type.upper()}: {count} lookups\n"
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("userlookups"))
async def user_lookups_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    await message.answer("🔍 <b>Enter user ID to view lookup history:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_user_lookups)

@dp.message(Command("codestats"))
async def code_stats_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    await message.answer("📊 <b>Enter code to view statistics:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_code_stats)

@dp.message(Command("premiumusers"))
async def premium_users_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    users = await get_premium_users()
    if not users:
        await message.reply("❌ No premium users found.")
        return
    text = "💰 <b>Premium Users (100+ credits)</b>\n\n"
    for user_id, username, credits in users[:20]:
        text += f"• <code>{user_id}</code> - @{username or 'N/A'} - {credits} credits\n"
    if len(users) > 20:
        text += f"\n... and {len(users) - 20} more premium users"
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("lowcreditusers"))
async def low_credit_users_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    users = await get_low_credit_users()
    if not users:
        await message.reply("✅ No users with low credits.")
        return
    text = "📉 <b>Users with Low Credits (≤5 credits)</b>\n\n"
    for user_id, username, credits in users[:20]:
        text += f"• <code>{user_id}</code> - @{username or 'N/A'} - {credits} credits\n"
    if len(users) > 20:
        text += f"\n... and {len(users) - 20} more users"
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("inactiveusers"))
async def inactive_users_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    days = 30
    if command.args and command.args.isdigit():
        days = int(command.args)
    users = await get_inactive_users(days)
    if not users:
        await message.reply(f"✅ No inactive users found (last {days} days).")
        return
    text = f"⏰ <b>Inactive Users (Last {days} days)</b>\n\n"
    for user_id, username, last_active in users[:15]:
        last_active_dt = datetime.fromisoformat(last_active)
        days_ago = (datetime.now() - last_active_dt).days
        text += f"• <code>{user_id}</code> - @{username or 'N/A'} - {days_ago} days ago\n"
    if len(users) > 15:
        text += f"\n... and {len(users) - 15} more inactive users"
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("bulkgift"))
async def bulk_gift_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    await message.answer(
        "🎁 <b>Bulk Gift Credits</b>\n\n"
        "Format: <code>/bulkgift AMOUNT USERID1 USERID2 ...</code>\n\n"
        "Example: <code>/bulkgift 50 123456 789012 345678</code>\n\n"
        "Enter the amount and user IDs separated by spaces:",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_bulk_gift)

@dp.message(Command("gift"))
async def gift_credits(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    try:
        args = command.args.split()
        uid, amt = int(args[0]), int(args[1])
        await update_credits(uid, amt)
        await message.reply(f"✅ Added {amt} credits to user {uid}")
        try:
            await bot.send_message(uid, f"🎁 <b>Admin Gifted You {amt} Credits!</b>", parse_mode="HTML")
        except:
            pass
    except:
        await message.reply("Usage: /gift <user_id> <amount>")

@dp.message(Command("removecredits"))
async def remove_credits(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    try:
        args = command.args.split()
        uid, amt = int(args[0]), int(args[1])
        await update_credits(uid, -amt)
        await message.reply(f"✅ Removed {amt} credits from user {uid}")
        try:
            await bot.send_message(uid, f"⚠️ <b>Admin Removed {amt} Credits From Your Account!</b>", parse_mode="HTML")
        except:
            pass
    except:
        await message.reply("Usage: /removecredits <user_id> <amount>")

@dp.message(Command("gencode"))
async def generate_random_code(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    try:
        args = command.args.split()
        if len(args) < 2:
            raise ValueError("Minimum 2 arguments required")
        amt = int(args[0])
        uses = int(args[1])
        expiry_minutes = None
        if len(args) >= 3:
            expiry_minutes = parse_time_string(args[2])
        code = f"PRO-{secrets.token_hex(3).upper()}"
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
        await message.reply(
            f"✅ <b>Code Created!</b>\n\n"
            f"🎫 <b>Code:</b> <code>{code}</code>\n"
            f"💰 <b>Amount:</b> {amt} credits\n"
            f"👥 <b>Max Uses:</b> {uses}\n"
            f"{expiry_text}\n\n"
            f"📝 <i>Note: Each user can claim only once</i>",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.reply(
            f"❌ <b>Usage:</b> <code>/gencode AMOUNT USES [TIME]</code>\n\n"
            f"<b>Examples:</b>\n"
            f"• <code>/gencode 50 10</code> - No expiry\n"
            f"• <code>/gencode 100 5 30m</code> - 30 minutes expiry\n"
            f"• <code>/gencode 200 3 2h</code> - 2 hours expiry\n"
            f"• <code>/gencode 500 1 1h30m</code> - 1.5 hours expiry\n\n"
            f"<b>Error:</b> {str(e)}",
            parse_mode="HTML"
        )

@dp.message(Command("customcode"))
async def custom_code_command(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    await message.answer(
        "🎫 <b>Enter code details:</b>\n"
        "Format: <code>CODE AMOUNT USES [TIME]</code>\n\n"
        "Examples:\n"
        "• <code>WELCOME50 50 10</code>\n"
        "• <code>FLASH100 100 5 15m</code>\n"
        "• <code>SPECIAL200 200 3 1h</code>\n\n"
        "Time formats: 30m, 2h, 1h30m",
        parse_mode="HTML"
    )
    await Form.waiting_for_custom_code.set()

@dp.message(Command("listcodes"))
async def list_codes_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    codes = await get_all_codes()
    if not codes:
        await message.reply("❌ No redeem codes found.")
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
            await message.answer(part, parse_mode="HTML")
    else:
        await message.reply(text, parse_mode="HTML")

@dp.message(Command("deactivatecode"))
async def deactivate_code_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    await message.answer("❌ <b>Enter code to deactivate:</b>", parse_mode="HTML")
    await Form.waiting_for_code_deactivate.set()

@dp.message(Command("checkexpired"))
async def check_expired_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    expired = await get_expired_codes()
    if not expired:
        await message.reply("✅ No expired codes found.")
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
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("ban"))
async def ban_user_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    try:
        uid = int(command.args)
        await set_ban_status(uid, 1)
        await message.reply(f"🚫 User {uid} banned.")
    except:
        await message.reply("Usage: /ban <user_id>")

@dp.message(Command("unban"))
async def unban_user_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    try:
        uid = int(command.args)
        await set_ban_status(uid, 0)
        await message.reply(f"🟢 User {uid} unbanned.")
    except:
        await message.reply("Usage: /unban <user_id>")

@dp.message(Command("stats"))
async def stats_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    stats = await get_bot_stats()
    top_ref = await get_top_referrers(5)
    total_lookups = await get_total_lookups()
    stats_text = f"📊 <b>Bot Statistics</b>\n\n"
    stats_text += f"👥 <b>Total Users:</b> {stats['total_users']}\n"
    stats_text += f"📈 <b>Active Users:</b> {stats['active_users']}\n"
    stats_text += f"💰 <b>Total Credits in System:</b> {stats['total_credits']}\n"
    stats_text += f"🎁 <b>Credits Distributed:</b> {stats['credits_distributed']}\n"
    stats_text += f"🔍 <b>Total Lookups:</b> {total_lookups}\n\n"
    if top_ref:
        stats_text += "🏆 <b>Top 5 Referrers:</b>\n"
        for i, (ref_id, count) in enumerate(top_ref, 1):
            stats_text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
    await message.reply(stats_text, parse_mode="HTML")

@dp.message(Command("backup"))
async def backup_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    await message.answer("📅 <b>Enter number of days for data:</b>\n"
                       "Example: 7 (for last 7 days)\n"
                       "0 for all data")
    await state.set_state(Form.waiting_for_stats_range)

@dp.message(Command("topref"))
async def top_ref_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    limit = 10
    if command.args and command.args.isdigit():
        limit = int(command.args)
    top_ref = await get_top_referrers(limit)
    if not top_ref:
        await message.reply("❌ No referrals yet.")
        return
    text = f"🏆 <b>Top {limit} Referrers</b>\n\n"
    for i, (ref_id, count) in enumerate(top_ref, 1):
        text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("cleanexpired"))
async def clean_expired_cmd(message: types.Message):
    if not await is_user_owner(message.from_user.id):
        return
    expired = await get_expired_codes()
    if not expired:
        await message.reply("✅ No expired codes found.")
        return
    deleted = 0
    for code_data in expired:
        await delete_redeem_code(code_data[0])
        deleted += 1
    await message.reply(f"🧹 Cleaned {deleted} expired codes.")

@dp.message(Command("addadmin"))
async def add_admin_cmd(message: types.Message, command: CommandObject):
    if not await is_user_owner(message.from_user.id):
        return
    try:
        uid = int(command.args)
        await add_admin(uid)
        await message.reply(f"✅ User {uid} added as admin.")
    except:
        await message.reply("Usage: /addadmin <user_id>")

@dp.message(Command("removeadmin"))
async def remove_admin_cmd(message: types.Message, command: CommandObject):
    if not await is_user_owner(message.from_user.id):
        return
    try:
        uid = int(command.args)
        if uid == OWNER_ID:
            await message.reply("❌ Cannot remove owner!")
            return
        await remove_admin(uid)
        await message.reply(f"✅ Admin {uid} removed.")
    except:
        await message.reply("Usage: /removeadmin <user_id>")

@dp.message(Command("listadmins"))
async def list_admins_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
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
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("settings"))
async def settings_cmd(message: types.Message, state: FSMContext):
    if not await is_user_owner(message.from_user.id):
        return
    await message.answer(
        "⚙️ <b>Bot Settings</b>\n\n"
        "1. Change bot name\n"
        "2. Update API endpoints\n"
        "3. Modify channel settings\n"
        "4. Adjust credit settings\n\n"
        "Enter setting number to modify:",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_settings)

@dp.message(Command("fulldbbackup"))
async def full_db_backup(message: types.Message):
    if not await is_user_owner(message.from_user.id):
        return
    try:
        backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2("nullprotocol.db", backup_name)
        await message.reply_document(
            FSInputFile(backup_name),
            caption="💾 Full database backup"
        )
        os.remove(backup_name)
    except Exception as e:
        await message.reply(f"❌ Backup failed: {str(e)}")

# --- ADMIN CALLBACKS ---
@dp.callback_query(F.data == "quick_stats")
async def quick_stats_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    stats = await get_bot_stats()
    top_ref = await get_top_referrers(3)
    total_lookups = await get_total_lookups()
    stats_text = f"📊 <b>Quick Stats</b>\n\n"
    stats_text += f"👥 <b>Total Users:</b> {stats['total_users']}\n"
    stats_text += f"📈 <b>Active Users:</b> {stats['active_users']}\n"
    stats_text += f"💰 <b>Total Credits:</b> {stats['total_credits']}\n"
    stats_text += f"🔍 <b>Total Lookups:</b> {total_lookups}\n\n"
    if top_ref:
        stats_text += "🏆 <b>Top 3 Referrers:</b>\n"
        for i, (ref_id, count) in enumerate(top_ref, 1):
            stats_text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
    await callback.message.edit_text(stats_text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "close_panel")
async def close_panel_callback(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.answer()

@dp.callback_query(F.data == "recent_users")
async def recent_users_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    users = await get_recent_users(10)
    text = "📅 <b>Recent Users (Last 10)</b>\n\n"
    if not users:
        text += "No recent users."
    else:
        for user_id, username, joined_date in users:
            join_dt = datetime.fromtimestamp(float(joined_date))
            text += f"• <code>{user_id}</code> - @{username or 'N/A'} - {join_dt.strftime('%d/%m %H:%M')}\n"
    await callback.message.edit_text(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "active_codes")
async def active_codes_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    codes = await get_active_codes()
    if not codes:
        await callback.answer("✅ No active codes found.", show_alert=True)
        return
    text = "✅ <b>Active Codes</b>\n\n"
    for code, amount, max_uses, current_uses in codes[:5]:
        text += f"🎟 <code>{code}</code> - {amount} credits ({current_uses}/{max_uses})\n"
    if len(codes) > 5:
        text += f"\n... and {len(codes) - 5} more"
    await callback.message.edit_text(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "top_ref")
async def top_ref_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    top_ref = await get_top_referrers(5)
    if not top_ref:
        await callback.answer("❌ No referrals yet.", show_alert=True)
        return
    text = "🏆 <b>Top 5 Referrers</b>\n\n"
    for i, (ref_id, count) in enumerate(top_ref, 1):
        text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
    await callback.message.edit_text(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "broadcast_now")
async def broadcast_now_callback(callback: types.CallbackQuery, state: FSMContext):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    await callback.message.answer("📢 <b>Send message to broadcast:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_broadcast)
    await callback.answer()

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
    await callback.message.edit_text(text, parse_mode="HTML", 
                                   reply_markup=InlineKeyboardMarkup(inline_keyboard=[buttons]))
    await callback.answer()

# --- BOT STARTUP / SHUTDOWN ---
async def on_startup():
    logger.info("Bot is starting up...")
    await init_db()
    for admin_id in ADMIN_IDS:
        if admin_id != OWNER_ID:
            await add_admin(admin_id)
    logger.info(f"Owner ID: {OWNER_ID}")
    logger.info(f"Static Admins: {ADMIN_IDS}")
    await check_api_status()

async def on_shutdown():
    logger.info("Bot is shutting down...")
    await bot.session.close()

async def check_api_status():
    logger.info("=" * 50)
    logger.info("🔍 API STATUS CHECK")
    logger.info("=" * 50)
    for api_name, api_url in APIS.items():
        status = "✅ SET" if api_url else "❌ NOT SET"
        logger.info(f"{api_name.upper():12} : {status}")
    logger.info("=" * 50)

# --- RUN BOT IN BACKGROUND THREAD ---
async def run_bot_polling():
    await on_startup()
    logger.info("🚀 Bot started polling")
    try:
        await dp.start_polling(bot)
    finally:
        await on_shutdown()

def start_bot_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_bot_polling())

# --- ENTRY POINT ---
if __name__ == "__main__":
    # Start bot in background thread
    bot_thread = threading.Thread(target=start_bot_thread, daemon=True)
    bot_thread.start()
    logger.info("Bot thread started")
    
    # Run Flask server (required by Render)
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting Flask server on port {port}")
    flask_app.run(host='0.0.0.0', port=port)
