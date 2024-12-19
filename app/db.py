# db.py

import sqlite3
import time

db_file_name = '/app/data/nostr_sync.db'

order_expiration = 1 * 60 * 60

def prepare_db():
    conn = sqlite3.connect(db_file_name)
    cursor = conn.cursor()
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        identifier TEXT NOT NULL,
        first_seen INTEGER NOT NULL,
        iteration INTEGER NOT NULL,
        origin TEXT NOT NULL
    );
    '''
    cursor.execute(create_table_query)
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()

def insert_order(cursor, identifier, first_seen, iteration, origin):
    insert_query = '''
    INSERT INTO orders (identifier, first_seen, iteration, origin)
    VALUES (?, ?, ?, ?);
    '''
    cursor.execute(insert_query, (identifier, first_seen, iteration, origin))

def max_iteration(conn, origin):
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(iteration), 0) FROM orders WHERE origin = ?;", (origin,))
    return cursor.fetchone()[0]

def exists_iteration(conn, identifier, origin):
    cursor = conn.cursor()
    expiration = time.time() - order_expiration
    
    cursor.execute("""
        SELECT EXISTS(
            SELECT 1 
            FROM orders 
            WHERE identifier = ? 
            AND origin = ? 
            AND first_seen >= ?
        );
    """, (identifier, origin, expiration))
    return cursor.fetchone()[0] == 1

def delete_records_by_iteration(cursor, iteration, origin):
    cursor.execute("DELETE FROM orders WHERE iteration <= ? AND origin = ?;", (iteration, origin,))

def update_iteration(cursor, identifier, origin, iteration):
    cursor.execute("UPDATE orders SET iteration = ? WHERE identifier = ? AND origin = ?;", (iteration, identifier, origin))
