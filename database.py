import os
import logging
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
from datetime import datetime
import pytz

MYANMAR_TIMEZONE = pytz.timezone('Asia/Yangon')

# Database connection
def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT")
    )

# Initialize database tables
def init_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Create user_data table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_data (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL,
                date_key TEXT NOT NULL,
                number INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create break_limits table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS break_limits (
                id SERIAL PRIMARY KEY,
                date_key TEXT NOT NULL UNIQUE,
                limit_amount INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create pnumber_per_date table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pnumber_per_date (
                id SERIAL PRIMARY KEY,
                date_key TEXT NOT NULL UNIQUE,
                power_number INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create all_data table (for com and za)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS all_data (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                com INTEGER NOT NULL,
                za INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        logging.info("Database tables initialized successfully")
    except Exception as e:
        logging.error(f"Error initializing database: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

# User data operations
async def save_user_bet(username, date_key, number, amount):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            "INSERT INTO user_data (username, date_key, number, amount) VALUES (%s, %s, %s, %s)",
            (username, date_key, number, amount)
        )
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error saving user bet: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

async def get_user_bets(username=None, date_key=None):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        query = "SELECT * FROM user_data"
        conditions = []
        params = []
        
        if username:
            conditions.append("username = %s")
            params.append(username)
        if date_key:
            conditions.append("date_key = %s")
            params.append(date_key)
            
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        cur.execute(query, params)
        return cur.fetchall()
    except Exception as e:
        logging.error(f"Error getting user bets: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

async def delete_user_bet(username, date_key, number, amount):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            """
            DELETE FROM user_data 
            WHERE username = %s AND date_key = %s AND number = %s AND amount = %s
            """,
            (username, date_key, number, amount)
        )
        
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        logging.error(f"Error deleting user bet: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

# Break limits operations
async def save_break_limit(date_key, limit_amount):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            """
            INSERT INTO break_limits (date_key, limit_amount) 
            VALUES (%s, %s)
            ON CONFLICT (date_key) 
            DO UPDATE SET limit_amount = EXCLUDED.limit_amount
            """,
            (date_key, limit_amount)
        )
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error saving break limit: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

async def get_break_limit(date_key):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            "SELECT limit_amount FROM break_limits WHERE date_key = %s",
            (date_key,)
        )
        
        result = cur.fetchone()
        return result[0] if result else None
    except Exception as e:
        logging.error(f"Error getting break limit: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

# Power number operations
async def save_power_number(date_key, power_number):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            """
            INSERT INTO pnumber_per_date (date_key, power_number) 
            VALUES (%s, %s)
            ON CONFLICT (date_key) 
            DO UPDATE SET power_number = EXCLUDED.power_number
            """,
            (date_key, power_number)
        )
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error saving power number: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

async def get_power_number(date_key):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            "SELECT power_number FROM pnumber_per_date WHERE date_key = %s",
            (date_key,)
        )
        
        result = cur.fetchone()
        return result[0] if result else None
    except Exception as e:
        logging.error(f"Error getting power number: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

# All data operations (com and za)
async def save_user_com_za(username, com, za):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            """
            INSERT INTO all_data (username, com, za) 
            VALUES (%s, %s, %s)
            ON CONFLICT (username) 
            DO UPDATE SET com = EXCLUDED.com, za = EXCLUDED.za
            """,
            (username, com, za)
        )
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error saving user com/za: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

async def get_user_com_za(username):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            "SELECT com, za FROM all_data WHERE username = %s",
            (username,)
        )
        
        result = cur.fetchone()
        return result if result else (0, 80)  # Default values
    except Exception as e:
        logging.error(f"Error getting user com/za: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

async def get_all_users():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT username FROM all_data")
        return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logging.error(f"Error getting all users: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

# Date operations
async def get_available_dates():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get dates from user_data
        cur.execute("SELECT DISTINCT date_key FROM user_data ORDER BY date_key DESC")
        user_dates = [row[0] for row in cur.fetchall()]
        
        # Get dates from break_limits
        cur.execute("SELECT DISTINCT date_key FROM break_limits ORDER BY date_key DESC")
        break_dates = [row[0] for row in cur.fetchall()]
        
        # Get dates from pnumber_per_date
        cur.execute("SELECT DISTINCT date_key FROM pnumber_per_date ORDER BY date_key DESC")
        pnumber_dates = [row[0] for row in cur.fetchall()]
        
        # Combine and deduplicate
        all_dates = list(set(user_dates + break_dates + pnumber_dates))
        return sorted(all_dates, reverse=True)
    except Exception as e:
        logging.error(f"Error getting available dates: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

async def delete_date_data(date_key):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Delete from all tables
        cur.execute("DELETE FROM user_data WHERE date_key = %s", (date_key,))
        cur.execute("DELETE FROM break_limits WHERE date_key = %s", (date_key,))
        cur.execute("DELETE FROM pnumber_per_date WHERE date_key = %s", (date_key,))
        
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Error deleting date data: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()
