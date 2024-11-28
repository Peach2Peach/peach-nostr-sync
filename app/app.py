# app.py
import os
import asyncio
import hashlib
import uuid
import requests
import time
import json
from datetime import datetime
from nostr_sdk import Keys, Client, EventBuilder, NostrSigner, Kind, Tag

def fetch_orders():
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

    return orders


async def transform_api_to_nostr(orders):
    transformed_data = []

    # Initialize with coordinator Keys
    keys = Keys.parse(os.environ.get('NOSTR_NSEC'))
    signer = NostrSigner.keys(keys)
    client = Client(signer)

    # Add relays and connect
    await client.add_relay("ws://localhost")
    await client.connect()

    for order in orders:
        hashed_id = hashlib.md5(
            f"ROBOHODLHODL{order.get('id')}".encode("utf-8")
        ).hexdigest()

        timestamp_in_24_hours = time.time() + 2 * 60 * 60

        payment_method_names = []
        for instruction in order.get("payment_method_instructions", []):
            payment_method_names.append(instruction.get("payment_method_name"))

        tags = [
            Tag.parse(["d", str(uuid.UUID(hashed_id))]),
            Tag.parse(["name", order.get("trader").get("login")]),
            Tag.parse(["k", order.get("side")]),
            Tag.parse(["f", order.get("currency_code")]),
            Tag.parse(["s", "pending"]),
            Tag.parse(["amt", str(order.get('max_amount_sats'))]),
            Tag.parse(["fa", str(order.get('min_amount')), str(order.get('max_amount'))]),
            Tag.parse(["pm"] + payment_method_names),
            Tag.parse(["source", f"https://hodlhodl.com/offers/{order.get('id')}"]),
            Tag.parse(["rating", json.dumps({
                "total_reviews": order.get("trader").get("trades_count"),
                "total_rating": order.get("trader").get("rating"),
            })]),
            Tag.parse(["network", "mainnet"]),
            Tag.parse(["layer", "onchain"]),
            Tag.parse([
                "expiration",
                str(int(timestamp_in_24_hours))
            ]),
            Tag.parse(["y", "hodlhodl"]),
            Tag.parse(["z", "order"])
        ]

        event = EventBuilder(
            Kind(38383),
            "",
            tags,
        ).to_event(keys)
        await client.send_event(event)
        print(f"Nostr event sent: {event.as_json()}")

async def main():
    while True:
        orders = fetch_orders()
        await transform_api_to_nostr(orders)
        await asyncio.sleep(300)  # Wait for 5 minutes

if __name__ == "__main__":
    asyncio.run(main())
