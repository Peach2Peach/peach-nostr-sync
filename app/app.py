# app.py
import os
import asyncio
import hashlib
import uuid
import sqlite3
import requests
import time
import json
from datetime import datetime
from nostr_sdk import Keys, Client, EventBuilder, NostrSigner, Kind, Tag

db_file_name = './data/nostr_sync.db'
order_expiration = 2 * 60 * 60

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
    create_index_query = '''
    CREATE INDEX IF NOT EXISTS idx_identifier_origin ON orders (identifier, origin);
    '''
    cursor.execute(create_table_query)
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()

def insert_order(conn, identifier, first_seen, iteration, origin):
    cursor = conn.cursor()
    insert_query = '''
    INSERT INTO orders (identifier, first_seen, iteration, origin)
    VALUES (?, ?, ?, ?);
    '''
    cursor.execute(insert_query, (identifier, first_seen, iteration, origin))
    conn.commit()

def max_iteration(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(iteration), 0) FROM orders;")
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

def delete_records_by_iteration(conn, iteration):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM orders WHERE iteration <= ?;", (iteration,))
    conn.commit()

def update_iteration(conn, identifier, origin, iteration):
    cursor = conn.cursor()
    cursor.execute("UPDATE orders SET iteration = ? WHERE identifier = ? AND origin = ?;", (iteration, identifier, origin))
    conn.commit()

def get_all_orders_by_iteration(conn, iteration):
    cursor = conn.cursor()

    query = '''
    SELECT * FROM orders WHERE iteration = ?;
    '''
    cursor.execute(query, (iteration,))
    return cursor.fetchall()

def fetch_hodlhodl_orders():
    base_url = "https://hodlhodl.com/api/v1/offers"
    offset = 0
    limit = 50
    orders = []

    while True:
        url = f"{base_url}?pagination[limit]={limit}&pagination[offset]={offset}"
        response = requests.get(url)
        
        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code}")
            break
        
        data = response.json()

        if data.get("status") != "success":
            print("Failed to fetch offers.")
            break
        
        orders.extend(data.get("offers", []))
        
        # If the number of offers returned is less than the limit, we are done
        if len(data.get("offers", [])) < limit:
            break
        
        # Increment the offset for the next request
        offset += limit

    
    print(f"Found {len(orders)} HodlHodl orders")

    return orders


async def publish_hodlhodl_to_nostr(orders):
    conn = sqlite3.connect(db_file_name)

    last_iteration = max_iteration(conn)
    origin = 'hodlhodl'

    print(f"Iteration HodlHodl: {last_iteration + 1}")

    # Initialize with coordinator Keys
    keys = Keys.parse(os.environ.get('NOSTR_NSEC'))
    signer = NostrSigner.keys(keys)
    client = Client(signer)

    # Add relays and connect
    await client.add_relay("ws://localhost")
    await client.connect()

    for order in orders:
        identifier = order.get('id')

        if exists_iteration(conn, identifier, origin):
            # keep alive existing orders
            update_iteration(conn, identifier, origin, last_iteration + 1)
            print(f"Iteration: {last_iteration + 1} - Order stay alive: {identifier}")
        else:
            # Publish new orders
            event = parse_hodlhodl_to_nostr(order, keys, "pending")        
            await client.send_event(event)

            print(f"Iteration: {last_iteration + 1} - Nostr event sent: {event.as_json()}")

            insert_order(conn, identifier, str(event.created_at().as_secs()), last_iteration + 1, origin)

    # Remove expired orders
    for orders in get_all_orders_by_iteration(conn, last_iteration):
        event = parse_hodlhodl_to_nostr(order, keys)         
        await client.send_event(event, "canceled")

        print(f"Iteration: {last_iteration + 1} - Order expired: {identifier}")

    delete_records_by_iteration(conn, last_iteration)

def parse_hodlhodl_to_nostr(order, keys, status):
    identifier = order.get('id')

    timestamp_in_2_hours = time.time() + order_expiration

    payment_method_names = []
    for instruction in order.get("payment_method_instructions", []):
        payment_method_names.append(instruction.get("payment_method_name"))

    tags = [
        Tag.parse(["d", identifier]),
        Tag.parse(["name", order.get("trader").get("login")]),
        Tag.parse(["k", order.get("side")]),
        Tag.parse(["f", order.get("currency_code")]),
        Tag.parse(["s", status]),
        Tag.parse(["amt", str(order.get('max_amount_sats'))]),
        Tag.parse(["fa", str(order.get('min_amount')), str(order.get('max_amount'))]),
        Tag.parse(["pm"] + payment_method_names),
        Tag.parse(["source", f"https://hodlhodl.com/offers/{identifier}"]),
        Tag.parse(["rating", json.dumps({
            "total_reviews": order.get("trader").get("trades_count"),
            "total_rating": order.get("trader").get("rating"),
        })]),
        Tag.parse(["network", "mainnet"]),
        Tag.parse(["layer", "onchain"]),
        Tag.parse([
            "expiration",
            str(int(timestamp_in_2_hours))
        ]),
        Tag.parse(["y", "hodlhodl"]),
        Tag.parse(["z", "order"])
    ]

    event = EventBuilder(
        Kind(38383),
        "",
        tags,
    ).to_event(keys)

    return event

async def main():
    print(f"START")
    prepare_db()
    while True:
        orders = fetch_hodlhodl_orders()
        await publish_hodlhodl_to_nostr(orders)
        await asyncio.sleep(300)  # Wait for 5 minutes

if __name__ == "__main__":
    asyncio.run(main())
