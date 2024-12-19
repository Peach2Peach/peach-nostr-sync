# nostr.py

import hashlib
import sqlite3
from logs import print_log
from nostr_sdk import Keys, Client, NostrSigner, NostrSdkError
from db import insert_order, max_iteration, exists_iteration, delete_records_by_iteration, update_iteration, db_file_name

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