import psycopg2
import psycopg2.extras
import bcrypt
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL connection parameters
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'ipos')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

def get_db_connection():
    """Create a connection to the PostgreSQL database"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn

def init_db():
    """Initialize the database with required tables"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create Users table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Users (
        user_id SERIAL PRIMARY KEY,
        username VARCHAR(255) UNIQUE NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Create IPOs table for scraped IPO data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS IPOs (
        ipo_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        symbol VARCHAR(20) NOT NULL,
        company_name VARCHAR(255),
        offering_price DECIMAL(10, 2),
        total_shares INTEGER,
        ipo_date DATE,
        status VARCHAR(20) DEFAULT 'upcoming',
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Create Ongoing_Watchlist table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Ongoing_Watchlist (
        watchlist_id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        ipo_id INTEGER NOT NULL,
        expiry_date DATE,
        FOREIGN KEY (user_id) REFERENCES Users (user_id),
        FOREIGN KEY (ipo_id) REFERENCES IPOs (ipo_id)
    )
    ''')

    # Create Past_Investments table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Past_Investments (
        investment_id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        ipo_id INTEGER NOT NULL,
        shares_purchased INTEGER NOT NULL,
        purchase_price DECIMAL(10, 2) NOT NULL,
        sold_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(20) DEFAULT 'pending',
        FOREIGN KEY (user_id) REFERENCES Users (user_id),
        FOREIGN KEY (ipo_id) REFERENCES IPOs (ipo_id)
    )
    ''')

    conn.commit()
    conn.close()

# ---------------------------
# User Related Functions
# ---------------------------

def create_user(username, email, password, first_name=None, last_name=None):
    """
    Create a new user with hashed password.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Hash the password
    password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

    try:
        cursor.execute(
            '''
            INSERT INTO Users (username, email, password_hash, first_name, last_name)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING user_id
            ''',
            (username, email, password_hash.decode('utf-8'), first_name, last_name)
        )
        user_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        return user_id
    except psycopg2.IntegrityError:
        # Username or email already exists
        conn.rollback()
        conn.close()
        return None

def authenticate_user(username, password):
    """
    Authenticate a user by username and password.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cursor.execute('SELECT * FROM Users WHERE username = %s', (username,))
    user = cursor.fetchone()

    if user and bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
        conn.close()
        return dict(user)
    
    conn.close()
    return None

# ---------------------------
# IPO Related Functions
# ---------------------------

def store_ipo(ipo_data):
    """
    Store scraped IPO data.
    Expected ipo_data keys: name, symbol, company_name, offering_price, total_shares,
    ipo_date, status, description.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO IPOs (name, symbol, company_name, offering_price, total_shares, ipo_date, status, description)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        ipo_data['name'],
        ipo_data['symbol'],
        ipo_data.get('company_name'),
        ipo_data.get('offering_price'),
        ipo_data.get('total_shares'),
        ipo_data.get('ipo_date'),
        ipo_data.get('status', 'upcoming'),
        ipo_data.get('description')
    ))
    
    conn.commit()
    conn.close()
    return True

def get_ipo(ipo_id):
    """
    Retrieve a single IPO by its ID.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    cursor.execute('SELECT * FROM IPOs WHERE ipo_id = %s', (ipo_id,))
    ipo = cursor.fetchone()
    conn.close()
    
    return dict(ipo) if ipo else None

# ---------------------------
# Ongoing Watchlist Functions
# ---------------------------

def add_to_watchlist(user_id, ipo_id, expiry_date=None):
    """
    Add an IPO to a user's watchlist.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO Ongoing_Watchlist (user_id, ipo_id, expiry_date)
    VALUES (%s, %s, %s)
    RETURNING watchlist_id
    ''', (user_id, ipo_id, expiry_date))
    
    watchlist_id = cursor.fetchone()[0]
    conn.commit()
    conn.close()
    return watchlist_id

def remove_from_watchlist(watchlist_id, user_id):
    """
    Remove an IPO from a user's watchlist.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    DELETE FROM Ongoing_Watchlist
    WHERE watchlist_id = %s AND user_id = %s
    ''', (watchlist_id, user_id))
    
    conn.commit()
    conn.close()
    return True

def get_user_watchlist(user_id):
    """
    Get the watchlist for a given user, including IPO details.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    cursor.execute('''
    SELECT ow.watchlist_id, ow.expiry_date, i.*
    FROM Ongoing_Watchlist AS ow
    JOIN IPOs AS i ON ow.ipo_id = i.ipo_id
    WHERE ow.user_id = %s
    ORDER BY i.ipo_date ASC
    ''', (user_id,))
    
    watchlist = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return watchlist

# ---------------------------
# Past Investments Functions
# ---------------------------

def add_investment(user_id, ipo_id, shares_purchased, purchase_price, sold_date=None, status='pending'):
    """
    Record a new past investment.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO Past_Investments (user_id, ipo_id, shares_purchased, purchase_price, sold_date, status)
    VALUES (%s, %s, %s, %s, %s, %s)
    RETURNING investment_id
    ''', (user_id, ipo_id, shares_purchased, purchase_price, sold_date, status))
    
    investment_id = cursor.fetchone()[0]
    conn.commit()
    conn.close()
    return investment_id

def update_investment_status(investment_id, user_id, status, sold_date=None):
    """
    Update the status (and optionally the sold_date) of an investment.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if sold_date:
        cursor.execute('''
        UPDATE Past_Investments
        SET status = %s, sold_date = %s
        WHERE investment_id = %s AND user_id = %s
        ''', (status, sold_date, investment_id, user_id))
    else:
        cursor.execute('''
        UPDATE Past_Investments
        SET status = %s
        WHERE investment_id = %s AND user_id = %s
        ''', (status, investment_id, user_id))
    
    conn.commit()
    conn.close()
    return True

def get_user_investments(user_id):
    """
    Get all past investments for a given user.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    cursor.execute('''
    SELECT pi.*, i.name AS ipo_name, i.symbol, i.ipo_date
    FROM Past_Investments AS pi
    JOIN IPOs AS i ON pi.ipo_id = i.ipo_id
    WHERE pi.user_id = %s
    ORDER BY pi.sold_date DESC
    ''', (user_id,))
    
    investments = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return investments

