import os
import re
import aiosqlite
import asyncpg
from datetime import datetime, timedelta

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")  # Render PostgreSQL URL
USING_SQLITE = DATABASE_URL is None

# Connection pool for PostgreSQL
_pg_pool = None

async def get_pg_pool():
    global _pg_pool
    if _pg_pool is None and not USING_SQLITE:
        _pg_pool = await asyncpg.create_pool(DATABASE_URL)
    return _pg_pool

# Helper function (unchanged)
def parse_time_string(time_str):
    """
    Parse time string like: 
    "30m" = 30 minutes
    "2h" = 2 hours (120 minutes)
    "1h30m" = 90 minutes
    "24h" = 1440 minutes
    """
    if not time_str or str(time_str).lower() == 'none':
        return None
    
    time_str = str(time_str).lower()
    total_minutes = 0
    
    hour_match = re.search(r'(\d+)h', time_str)
    if hour_match:
        total_minutes += int(hour_match.group(1)) * 60
    
    minute_match = re.search(r'(\d+)m', time_str)
    if minute_match:
        total_minutes += int(minute_match.group(1))
    
    if not hour_match and not minute_match and time_str.isdigit():
        total_minutes = int(time_str)
    
    return total_minutes if total_minutes > 0 else None

# -------------------- INIT DATABASE --------------------
async def init_db():
    if USING_SQLITE:
        await init_sqlite()
    else:
        await init_postgres()

async def init_sqlite():
    async with aiosqlite.connect("nullprotocol.db") as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                credits INTEGER DEFAULT 5,
                joined_date TEXT,
                referrer_id INTEGER,
                is_banned INTEGER DEFAULT 0,
                total_earned INTEGER DEFAULT 0,
                last_active TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                level TEXT DEFAULT 'admin',
                added_by INTEGER,
                added_date TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS redeem_codes (
                code TEXT PRIMARY KEY,
                amount INTEGER,
                max_uses INTEGER,
                current_uses INTEGER DEFAULT 0,
                expiry_minutes INTEGER,
                created_date TEXT,
                is_active INTEGER DEFAULT 1
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS redeem_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                code TEXT,
                claimed_date TEXT,
                UNIQUE(user_id, code)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS lookup_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                api_type TEXT,
                input_data TEXT,
                result TEXT,
                lookup_date TEXT
            )
        """)
        await db.commit()

async def init_postgres():
    pool = await get_pg_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                credits INTEGER DEFAULT 5,
                joined_date TEXT,
                referrer_id BIGINT,
                is_banned INTEGER DEFAULT 0,
                total_earned INTEGER DEFAULT 0,
                last_active TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                level TEXT DEFAULT 'admin',
                added_by BIGINT,
                added_date TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS redeem_codes (
                code TEXT PRIMARY KEY,
                amount INTEGER,
                max_uses INTEGER,
                current_uses INTEGER DEFAULT 0,
                expiry_minutes INTEGER,
                created_date TEXT,
                is_active INTEGER DEFAULT 1
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS redeem_logs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                code TEXT,
                claimed_date TEXT,
                UNIQUE(user_id, code)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lookup_logs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                api_type TEXT,
                input_data TEXT,
                result TEXT,
                lookup_date TEXT
            )
        """)

# -------------------- USER FUNCTIONS --------------------
async def get_user(user_id):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
                return await cursor.fetchone()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if row:
                # Convert Record to tuple for compatibility with existing code
                return tuple(row)
            return None

async def add_user(user_id, username, referrer_id=None):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            # Check if user exists
            async with db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)) as cursor:
                if await cursor.fetchone():
                    return
            credits = 5
            current_time = str(datetime.now().timestamp())
            await db.execute("""
                INSERT INTO users (user_id, username, credits, joined_date, referrer_id, is_banned, total_earned, last_active) 
                VALUES (?, ?, ?, ?, ?, 0, 0, ?)
            """, (user_id, username, credits, current_time, referrer_id, current_time))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            # Check if user exists
            exists = await conn.fetchval("SELECT user_id FROM users WHERE user_id = $1", user_id)
            if exists:
                return
            credits = 5
            current_time = str(datetime.now().timestamp())
            await conn.execute("""
                INSERT INTO users (user_id, username, credits, joined_date, referrer_id, is_banned, total_earned, last_active) 
                VALUES ($1, $2, $3, $4, $5, 0, 0, $6)
            """, user_id, username, credits, current_time, referrer_id, current_time)

async def update_credits(user_id, amount):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            if amount > 0:
                await db.execute("UPDATE users SET credits = credits + ?, total_earned = total_earned + ? WHERE user_id = ?", 
                               (amount, amount, user_id))
            else:
                await db.execute("UPDATE users SET credits = credits + ? WHERE user_id = ?", (amount, user_id))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            if amount > 0:
                await conn.execute("UPDATE users SET credits = credits + $1, total_earned = total_earned + $1 WHERE user_id = $2", 
                                 amount, user_id)
            else:
                await conn.execute("UPDATE users SET credits = credits + $1 WHERE user_id = $2", amount, user_id)

async def set_ban_status(user_id, status):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            await db.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (status, user_id))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET is_banned = $1 WHERE user_id = $2", status, user_id)

async def get_all_users():
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("SELECT user_id FROM users") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM users")
            return [row['user_id'] for row in rows]

async def get_user_by_username(username):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("SELECT user_id FROM users WHERE username = ?", (username,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT user_id FROM users WHERE username = $1", username)
            return row['user_id'] if row else None

async def get_top_referrers(limit=10):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT referrer_id, COUNT(*) as referrals 
                FROM users 
                WHERE referrer_id IS NOT NULL 
                GROUP BY referrer_id 
                ORDER BY referrals DESC 
                LIMIT ?
            """, (limit,)) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT referrer_id, COUNT(*) as referrals 
                FROM users 
                WHERE referrer_id IS NOT NULL 
                GROUP BY referrer_id 
                ORDER BY referrals DESC 
                LIMIT $1
            """, limit)
            return [(row['referrer_id'], row['referrals']) for row in rows]

async def get_bot_stats():
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("SELECT COUNT(*) FROM users") as cursor:
                total_users = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM users WHERE credits > 0") as cursor:
                active_users = (await cursor.fetchone())[0]
            async with db.execute("SELECT SUM(credits) FROM users") as cursor:
                total_credits = (await cursor.fetchone())[0] or 0
            async with db.execute("SELECT SUM(total_earned) FROM users") as cursor:
                credits_distributed = (await cursor.fetchone())[0] or 0
            return {
                'total_users': total_users,
                'active_users': active_users,
                'total_credits': total_credits,
                'credits_distributed': credits_distributed
            }
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
            active_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE credits > 0")
            total_credits = await conn.fetchval("SELECT COALESCE(SUM(credits), 0) FROM users")
            credits_distributed = await conn.fetchval("SELECT COALESCE(SUM(total_earned), 0) FROM users")
            return {
                'total_users': total_users,
                'active_users': active_users,
                'total_credits': total_credits,
                'credits_distributed': credits_distributed
            }

async def get_users_in_range(start_date, end_date):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT user_id, username, credits, joined_date 
                FROM users 
                WHERE joined_date BETWEEN ? AND ?
            """, (start_date, end_date)) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT user_id, username, credits, joined_date 
                FROM users 
                WHERE joined_date BETWEEN $1 AND $2
            """, start_date, end_date)
            return [(row['user_id'], row['username'], row['credits'], row['joined_date']) for row in rows]

# -------------------- ADMIN FUNCTIONS --------------------
async def add_admin(user_id, level='admin'):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            await db.execute("INSERT OR REPLACE INTO admins (user_id, level) VALUES (?, ?)", (user_id, level))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO admins (user_id, level) VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
            """, user_id, level)

async def remove_admin(user_id):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            await db.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM admins WHERE user_id = $1", user_id)

async def get_all_admins():
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("SELECT user_id, level FROM admins") as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id, level FROM admins")
            return [(row['user_id'], row['level']) for row in rows]

async def is_admin(user_id):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("SELECT level FROM admins WHERE user_id = ?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT level FROM admins WHERE user_id = $1", user_id)
            return row['level'] if row else None

# -------------------- REDEEM CODE FUNCTIONS --------------------
async def create_redeem_code(code, amount, max_uses, expiry_minutes=None):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            await db.execute("""
                INSERT OR REPLACE INTO redeem_codes 
                (code, amount, max_uses, expiry_minutes, created_date, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
            """, (code, amount, max_uses, expiry_minutes, datetime.now().isoformat()))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO redeem_codes (code, amount, max_uses, expiry_minutes, created_date, is_active)
                VALUES ($1, $2, $3, $4, $5, 1)
                ON CONFLICT (code) DO UPDATE SET
                    amount = EXCLUDED.amount,
                    max_uses = EXCLUDED.max_uses,
                    expiry_minutes = EXCLUDED.expiry_minutes,
                    created_date = EXCLUDED.created_date,
                    is_active = EXCLUDED.is_active
            """, code, amount, max_uses, expiry_minutes, datetime.now().isoformat())

async def redeem_code_db(user_id, code):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            # Check if user already claimed this code
            async with db.execute("SELECT 1 FROM redeem_logs WHERE user_id = ? AND code = ?", (user_id, code)) as cursor:
                if await cursor.fetchone():
                    return "already_claimed"
            
            # Get code details
            async with db.execute("""
                SELECT amount, max_uses, current_uses, expiry_minutes, created_date, is_active
                FROM redeem_codes WHERE code = ?
            """, (code,)) as cursor:
                data = await cursor.fetchone()
                
            if not data:
                return "invalid"
            
            amount, max_uses, current_uses, expiry_minutes, created_date, is_active = data
            
            if not is_active:
                return "inactive"
            
            if current_uses >= max_uses:
                return "limit_reached"
            
            if expiry_minutes is not None and expiry_minutes > 0:
                created_dt = datetime.fromisoformat(created_date)
                expiry_dt = created_dt + timedelta(minutes=expiry_minutes)
                if datetime.now() > expiry_dt:
                    return "expired"
            
            try:
                await db.execute("BEGIN TRANSACTION")
                
                await db.execute("UPDATE redeem_codes SET current_uses = current_uses + 1 WHERE code = ?", (code,))
                await db.execute("UPDATE users SET credits = credits + ? WHERE user_id = ?", (amount, user_id))
                await db.execute("UPDATE users SET total_earned = total_earned + ? WHERE user_id = ?", (amount, user_id))
                await db.execute("INSERT OR IGNORE INTO redeem_logs (user_id, code, claimed_date) VALUES (?, ?, ?)",
                               (user_id, code, datetime.now().isoformat()))
                await db.commit()
                return amount
            except Exception as e:
                await db.execute("ROLLBACK")
                return f"error: {str(e)}"
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            # Check if user already claimed this code
            claimed = await conn.fetchval("SELECT 1 FROM redeem_logs WHERE user_id = $1 AND code = $2", user_id, code)
            if claimed:
                return "already_claimed"
            
            # Get code details
            row = await conn.fetchrow("""
                SELECT amount, max_uses, current_uses, expiry_minutes, created_date, is_active
                FROM redeem_codes WHERE code = $1
            """, code)
            
            if not row:
                return "invalid"
            
            amount, max_uses, current_uses, expiry_minutes, created_date, is_active = row
            
            if not is_active:
                return "inactive"
            
            if current_uses >= max_uses:
                return "limit_reached"
            
            if expiry_minutes is not None and expiry_minutes > 0:
                created_dt = datetime.fromisoformat(created_date)
                expiry_dt = created_dt + timedelta(minutes=expiry_minutes)
                if datetime.now() > expiry_dt:
                    return "expired"
            
            async with conn.transaction():
                await conn.execute("UPDATE redeem_codes SET current_uses = current_uses + 1 WHERE code = $1", code)
                await conn.execute("UPDATE users SET credits = credits + $1, total_earned = total_earned + $1 WHERE user_id = $2", amount, user_id)
                await conn.execute("INSERT INTO redeem_logs (user_id, code, claimed_date) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                                 user_id, code, datetime.now().isoformat())
            return amount

async def get_expired_codes():
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT code, amount, current_uses, max_uses, expiry_minutes, created_date 
                FROM redeem_codes 
                WHERE is_active = 1 
                AND expiry_minutes IS NOT NULL 
                AND expiry_minutes > 0
                AND datetime(created_date, '+' || expiry_minutes || ' minutes') < datetime('now')
            """) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT code, amount, current_uses, max_uses, expiry_minutes, created_date 
                FROM redeem_codes 
                WHERE is_active = 1 
                AND expiry_minutes IS NOT NULL 
                AND expiry_minutes > 0
                AND (created_date::timestamptz + (expiry_minutes * interval '1 minute')) < now()
            """)
            return [(row['code'], row['amount'], row['current_uses'], row['max_uses'], row['expiry_minutes'], row['created_date']) for row in rows]

async def delete_redeem_code(code):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            await db.execute("DELETE FROM redeem_codes WHERE code = ?", (code,))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM redeem_codes WHERE code = $1", code)

async def deactivate_code(code):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            await db.execute("UPDATE redeem_codes SET is_active = 0 WHERE code = ?", (code,))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE redeem_codes SET is_active = 0 WHERE code = $1", code)

async def get_all_codes():
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT code, amount, max_uses, current_uses, 
                       expiry_minutes, created_date, is_active
                FROM redeem_codes
                ORDER BY created_date DESC
            """) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT code, amount, max_uses, current_uses, 
                       expiry_minutes, created_date, is_active
                FROM redeem_codes
                ORDER BY created_date DESC
            """)
            return [(row['code'], row['amount'], row['max_uses'], row['current_uses'],
                     row['expiry_minutes'], row['created_date'], row['is_active']) for row in rows]

async def get_active_codes():
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT code, amount, max_uses, current_uses
                FROM redeem_codes
                WHERE is_active = 1
                ORDER BY created_date DESC
            """) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT code, amount, max_uses, current_uses
                FROM redeem_codes
                WHERE is_active = 1
                ORDER BY created_date DESC
            """)
            return [(row['code'], row['amount'], row['max_uses'], row['current_uses']) for row in rows]

async def get_inactive_codes():
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT code, amount, max_uses, current_uses
                FROM redeem_codes
                WHERE is_active = 0
                ORDER BY created_date DESC
            """) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT code, amount, max_uses, current_uses
                FROM redeem_codes
                WHERE is_active = 0
                ORDER BY created_date DESC
            """)
            return [(row['code'], row['amount'], row['max_uses'], row['current_uses']) for row in rows]

async def get_user_redeem_history(user_id):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("SELECT code, claimed_date FROM redeem_logs WHERE user_id = ?", (user_id,)) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT code, claimed_date FROM redeem_logs WHERE user_id = $1", user_id)
            return [(row['code'], row['claimed_date']) for row in rows]

async def get_code_usage_stats(code):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT 
                    rc.amount, rc.max_uses, rc.current_uses,
                    COUNT(DISTINCT rl.user_id) as unique_users,
                    GROUP_CONCAT(DISTINCT rl.user_id) as user_ids
                FROM redeem_codes rc
                LEFT JOIN redeem_logs rl ON rc.code = rl.code
                WHERE rc.code = ?
                GROUP BY rc.code
            """, (code,)) as cursor:
                return await cursor.fetchone()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    rc.amount, rc.max_uses, rc.current_uses,
                    COUNT(DISTINCT rl.user_id) as unique_users,
                    STRING_AGG(DISTINCT rl.user_id::text, ',') as user_ids
                FROM redeem_codes rc
                LEFT JOIN redeem_logs rl ON rc.code = rl.code
                WHERE rc.code = $1
                GROUP BY rc.code
            """, code)
            if row:
                return (row['amount'], row['max_uses'], row['current_uses'], row['unique_users'], row['user_ids'])
            return None

# -------------------- USER STATS FUNCTIONS --------------------
async def get_user_stats(user_id):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM users WHERE referrer_id = ?) as referrals,
                    (SELECT COUNT(*) FROM redeem_logs WHERE user_id = ?) as codes_claimed,
                    (SELECT SUM(amount) FROM redeem_logs rl 
                     JOIN redeem_codes rc ON rl.code = rc.code 
                     WHERE rl.user_id = ?) as total_from_codes
                FROM users WHERE user_id = ?
            """, (user_id, user_id, user_id, user_id)) as cursor:
                return await cursor.fetchone()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    (SELECT COUNT(*) FROM users WHERE referrer_id = $1) as referrals,
                    (SELECT COUNT(*) FROM redeem_logs WHERE user_id = $1) as codes_claimed,
                    (SELECT COALESCE(SUM(amount), 0) FROM redeem_logs rl 
                     JOIN redeem_codes rc ON rl.code = rc.code 
                     WHERE rl.user_id = $1) as total_from_codes
            """, user_id)
            return (row['referrals'], row['codes_claimed'], row['total_from_codes'])

async def get_recent_users(limit=20):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT user_id, username, joined_date 
                FROM users 
                ORDER BY joined_date DESC 
                LIMIT ?
            """, (limit,)) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT user_id, username, joined_date 
                FROM users 
                ORDER BY joined_date DESC 
                LIMIT $1
            """, limit)
            return [(row['user_id'], row['username'], row['joined_date']) for row in rows]

async def delete_user(user_id):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            await db.execute("DELETE FROM redeem_logs WHERE user_id = ?", (user_id,))
            await db.execute("UPDATE users SET referrer_id = NULL WHERE referrer_id = ?", (user_id,))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM users WHERE user_id = $1", user_id)
                await conn.execute("DELETE FROM redeem_logs WHERE user_id = $1", user_id)
                await conn.execute("UPDATE users SET referrer_id = NULL WHERE referrer_id = $1", user_id)

async def reset_user_credits(user_id):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            await db.execute("UPDATE users SET credits = 0 WHERE user_id = ?", (user_id,))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET credits = 0 WHERE user_id = $1", user_id)

async def get_user_by_id(user_id):
    return await get_user(user_id)  # same as get_user

async def search_users(query):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT user_id, username, credits 
                FROM users 
                WHERE username LIKE ? OR user_id = ?
                LIMIT 20
            """, (f"%{query}%", query if query.isdigit() else 0)) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT user_id, username, credits 
                FROM users 
                WHERE username ILIKE $1 OR user_id = $2
                LIMIT 20
            """, f"%{query}%", int(query) if query.isdigit() else 0)
            return [(row['user_id'], row['username'], row['credits']) for row in rows]

async def get_daily_stats(days=7):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT 
                    date(joined_date, 'unixepoch') as join_date,
                    COUNT(*) as new_users,
                    (SELECT COUNT(*) FROM redeem_logs 
                     WHERE date(claimed_date) = date(joined_date, 'unixepoch')) as claims
                FROM users 
                WHERE date(joined_date, 'unixepoch') >= date('now', ? || ' days')
                GROUP BY join_date
                ORDER BY join_date DESC
            """, (f"-{days}",)) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    DATE(joined_date) as join_date,
                    COUNT(*) as new_users,
                    (SELECT COUNT(*) FROM redeem_logs WHERE DATE(claimed_date) = DATE(u.joined_date)) as claims
                FROM users u
                WHERE DATE(joined_date) >= CURRENT_DATE - $1 * INTERVAL '1 day'
                GROUP BY join_date
                ORDER BY join_date DESC
            """, days)
            return [(row['join_date'], row['new_users'], row['claims']) for row in rows]

async def update_username(user_id, username):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (username, user_id))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET username = $1 WHERE user_id = $2", username, user_id)

async def update_last_active(user_id):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            await db.execute("UPDATE users SET last_active = ? WHERE user_id = ?", 
                           (datetime.now().isoformat(), user_id))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET last_active = $1 WHERE user_id = $2", 
                             datetime.now().isoformat(), user_id)

async def get_user_activity(user_id, days=7):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            async with db.execute("""
                SELECT COUNT(*) 
                FROM lookup_logs 
                WHERE user_id = ? AND lookup_date > ?
            """, (user_id, cutoff)) as cursor:
                return (await cursor.fetchone())[0]
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            count = await conn.fetchval("""
                SELECT COUNT(*) 
                FROM lookup_logs 
                WHERE user_id = $1 AND lookup_date > $2
            """, user_id, cutoff)
            return count

async def get_leaderboard(limit=10):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT user_id, username, credits 
                FROM users 
                WHERE is_banned = 0
                ORDER BY credits DESC 
                LIMIT ?
            """, (limit,)) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT user_id, username, credits 
                FROM users 
                WHERE is_banned = 0
                ORDER BY credits DESC 
                LIMIT $1
            """, limit)
            return [(row['user_id'], row['username'], row['credits']) for row in rows]

async def bulk_update_credits(user_ids, amount):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            await db.execute("BEGIN TRANSACTION")
            for user_id in user_ids:
                if amount > 0:
                    await db.execute("UPDATE users SET credits = credits + ?, total_earned = total_earned + ? WHERE user_id = ?", 
                                   (amount, amount, user_id))
                else:
                    await db.execute("UPDATE users SET credits = credits + ? WHERE user_id = ?", (amount, user_id))
            await db.execute("COMMIT")
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                for user_id in user_ids:
                    if amount > 0:
                        await conn.execute("UPDATE users SET credits = credits + $1, total_earned = total_earned + $1 WHERE user_id = $2", 
                                         amount, user_id)
                    else:
                        await conn.execute("UPDATE users SET credits = credits + $1 WHERE user_id = $2", amount, user_id)

# -------------------- LOOKUP LOGS --------------------
async def log_lookup(user_id, api_type, input_data, result):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            await db.execute("""
                INSERT INTO lookup_logs (user_id, api_type, input_data, result, lookup_date)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, api_type, input_data[:500], str(result)[:1000], datetime.now().isoformat()))
            await db.commit()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO lookup_logs (user_id, api_type, input_data, result, lookup_date)
                VALUES ($1, $2, $3, $4, $5)
            """, user_id, api_type, input_data[:500], str(result)[:1000], datetime.now().isoformat())

async def get_lookup_stats(user_id=None):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            if user_id:
                async with db.execute("""
                    SELECT api_type, COUNT(*) as count 
                    FROM lookup_logs 
                    WHERE user_id = ?
                    GROUP BY api_type
                """, (user_id,)) as cursor:
                    return await cursor.fetchall()
            else:
                async with db.execute("""
                    SELECT api_type, COUNT(*) as count 
                    FROM lookup_logs 
                    GROUP BY api_type
                """) as cursor:
                    return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            if user_id:
                rows = await conn.fetch("""
                    SELECT api_type, COUNT(*) as count 
                    FROM lookup_logs 
                    WHERE user_id = $1
                    GROUP BY api_type
                """, user_id)
            else:
                rows = await conn.fetch("""
                    SELECT api_type, COUNT(*) as count 
                    FROM lookup_logs 
                    GROUP BY api_type
                """)
            return [(row['api_type'], row['count']) for row in rows]

async def get_total_lookups():
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("SELECT COUNT(*) FROM lookup_logs") as cursor:
                return (await cursor.fetchone())[0]
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM lookup_logs")

async def get_user_lookups(user_id, limit=50):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT api_type, input_data, lookup_date 
                FROM lookup_logs 
                WHERE user_id = ?
                ORDER BY lookup_date DESC
                LIMIT ?
            """, (user_id, limit)) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT api_type, input_data, lookup_date 
                FROM lookup_logs 
                WHERE user_id = $1
                ORDER BY lookup_date DESC
                LIMIT $2
            """, user_id, limit)
            return [(row['api_type'], row['input_data'], row['lookup_date']) for row in rows]

# -------------------- PREMIUM / LOW CREDIT USERS --------------------
async def get_premium_users():
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT user_id, username, credits 
                FROM users 
                WHERE credits >= 100
                ORDER BY credits DESC
            """) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT user_id, username, credits 
                FROM users 
                WHERE credits >= 100
                ORDER BY credits DESC
            """)
            return [(row['user_id'], row['username'], row['credits']) for row in rows]

async def get_low_credit_users():
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            async with db.execute("""
                SELECT user_id, username, credits 
                FROM users 
                WHERE credits <= 5
                ORDER BY credits ASC
            """) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT user_id, username, credits 
                FROM users 
                WHERE credits <= 5
                ORDER BY credits ASC
            """)
            return [(row['user_id'], row['username'], row['credits']) for row in rows]

async def get_inactive_users(days=30):
    if USING_SQLITE:
        async with aiosqlite.connect("nullprotocol.db") as db:
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            async with db.execute("""
                SELECT user_id, username, last_active 
                FROM users 
                WHERE last_active < ? 
                AND is_banned = 0
                ORDER BY last_active ASC
            """, (cutoff,)) as cursor:
                return await cursor.fetchall()
    else:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            rows = await conn.fetch("""
                SELECT user_id, username, last_active 
                FROM users 
                WHERE last_active < $1 
                AND is_banned = 0
                ORDER BY last_active ASC
            """, cutoff)
            return [(row['user_id'], row['username'], row['last_active']) for row in rows]
