# app.py
import os
import sys
import asyncio
import hashlib
import uuid
import sqlite3
import requests
import time
import json
from datetime import datetime
from nostr_sdk import Keys, Client, EventBuilder, NostrSigner, Kind, Tag, NostrSdkError

log_file_path = '/app/log/app.log'
db_file_name = '/app/data/nostr_sync.db'
order_expiration = 1 * 60 * 60

def print_log(*args, **kwargs):
    message = ' '.join(map(str, args))
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"{datetime.now()}: {message}\n")

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
    CREATE INDEX IF NOT EXISTS idx_identifier_origin ON orders (iteration, origin, identifier);
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

def get_all_orders_by_iteration(conn, iteration, origin):
    cursor = conn.cursor()
    query = '''
    SELECT * FROM orders WHERE iteration = ? AND origin = ?;
    '''
    cursor.execute(query, (iteration, origin,))
    return cursor.fetchall()

def fetch_peach_orders():
    base_url = "https://api.peachbitcoin.com/v1/offer/search"
    page = 0
    size = 500
    orders = []
    headers = {
        'Content-Type': 'application/json'
    }

    while True:
        url = f"{base_url}?page={page}&size={size}"
        print_log(f"Fetching {url}")
        response = requests.post(url, headers=headers)
        
        if response.status_code != 200:
            print_log(f"Error fetching data: {response.status_code}")
            break
        
        data = response.json()

        if data.get("offers") is None:
            print_log("Failed to fetch offers.")
            break
        
        orders.extend(data.get("offers", []))
        
        # If the number of offers returned is less than the limit, we are done
        if len(data.get("offers", [])) < size:
            break
        
        # Increment the offset for the next request
        page += 1

    
    print_log(f"Found {len(orders)} Peach orders")

    return orders

def parse_peach_to_nostr(order, keys, status):
    identifier = order.get('id')

    timestamp_in_2_hours = time.time() + order_expiration

    events = []

    tags = [
            Tag.parse(["name", order.get("user").get("id")]),
            Tag.parse(["k", "sell" if order.get("type") == "ask" else "buy"]),
            Tag.parse(["s", status]),
            Tag.parse(["source", f""]), # TODO
            Tag.parse(["rating", json.dumps({
                "total_reviews": order.get("user").get("ratingCount"),
                "total_rating": order.get("user").get("rating"),
            })]),
            Tag.parse(["network", "mainnet"]),
            Tag.parse(["layer", "onchain"]),
            Tag.parse([
                "expiration",
                str(int(timestamp_in_2_hours))
            ]),
            Tag.parse(["y", "peach"]),
            Tag.parse(["z", "order"])
        ]

    amount = order.get("amount")
    if isinstance(amount, list):
        Tag.parse(["amt", str(amount[0]), str(amount[1])])
    else:
        Tag.parse(["amt", str(amount)])

    for currency, methods in order.get("meansOfPayment", {}).items():
        tags.append(Tag.parse(["d", identifier + currency]))
        tags.append(Tag.parse(["pm"] + methods))
        tags.append(Tag.parse(["f", currency]))

        event = EventBuilder(
            Kind(38383),
            "",
            tags,
        ).to_event(keys)
        events.append(event)

    return events

def fetch_hodlhodl_orders():
    base_url = "https://hodlhodl.com/api/v1/offers"
    offset = 0
    limit = 100
    orders = []
    headers = {
        'Content-Type': 'application/json'
    }

    while True:
        url = f"{base_url}?pagination[limit]={limit}&pagination[offset]={offset}"
        print_log(f"Fetching {url}")
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print_log(f"Error fetching data: {response.status_code}")
            break
        
        data = response.json()

        if data.get("status") != "success":
            print_log("Failed to fetch offers.")
            break
        
        orders.extend(data.get("offers", []))
        
        # If the number of offers returned is less than the limit, we are done
        if len(data.get("offers", [])) < limit:
            break
        
        # Increment the offset for the next request
        offset += limit

    
    print_log(f"Found {len(orders)} HodlHodl orders")

    return orders

def parse_hodlhodl_to_nostr(order, keys, status):
    identifier = str(order.get('id'))

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

    return [event]

async def publish_to_nostr(orders, origin, parser, nsec):
    conn = sqlite3.connect(db_file_name)

    last_iteration = max_iteration(conn, origin)

    print_log(f"Iteration {origin}: {last_iteration + 1}")

    # Initialize with coordinator Keys
    keys = Keys.parse(nsec)
    signer = NostrSigner.keys(keys)
    client = Client(signer)

    # Add relays and connect
    await client.add_relay("ws://localhost")
    await client.connect()

    print_log(f"Iteration {origin}: Checking Orders")

    cursor = conn.cursor()
    for order in orders:
        identifier = str(order.get('id'))
        if exists_iteration(conn, identifier, origin):
            # keep alive existing orders
            print_log(f"Iteration {origin}: {last_iteration + 1} - Order stay alive: {identifier}")
            update_iteration(cursor, identifier, origin, last_iteration + 1)
        else:
            # Publish new orders
            try:
                events = parser(order, keys, "pending")
                for event in events:     
                    print_log(f"Iteration {origin}: {last_iteration + 1} - Nostr event sent: {event.as_json()}")
                    try:
                        await client.send_event(event)
                    except NostrSdkError as e:
                        print_log(f"Iteration {origin}: {last_iteration + 1} - Event already published")

                if (len(events) > 0):
                    insert_order(cursor, identifier, str(events[0].created_at().as_secs()), last_iteration + 1, origin)
            except Exception as e:       
                print_log(f"Iteration {origin}: {last_iteration + 1} - Error parsing {e} : {order}")

    conn.commit()

    print_log(f"Iteration {origin}: Cleaning Orders")
    delete_records_by_iteration(conn, last_iteration, origin)
    
    conn.commit()
    conn.close()

async def main():
    print_log(f"START LOOP")
    prepare_db()
    while True:
        print_log(f"HODL HODL")
        hodlhodl_orders = fetch_hodlhodl_orders()
        await publish_to_nostr(hodlhodl_orders, 'hodlhodl', parse_hodlhodl_to_nostr, os.environ.get('HODLHODL_NOSTR_NSEC'))

        print_log(f"PEACH")
        peach_orders = fetch_peach_orders()
        await publish_to_nostr(peach_orders, 'peach', parse_peach_to_nostr, os.environ.get('PEACH_NOSTR_NSEC'))

        print_log(f"DONE GOING TO SLEEP")
        await asyncio.sleep(300)  # Wait for 5 minutes

if __name__ == "__main__":
    print_log(f"START")
    asyncio.run(main())
