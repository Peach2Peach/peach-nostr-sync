# hodlhodl.py

import os
import asyncio
import requests
import time
import json
from db import prepare_db, db_file_name, order_expiration
from logs import print_log
from nostr import publish_to_nostr
from nostr_sdk import Keys, EventBuilder, Kind, Tag

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

async def main():
    print_log(f"START LOOP")
    prepare_db()
    while True:
        print_log(f"HODL HODL")
        hodlhodl_orders = fetch_hodlhodl_orders()
        await publish_to_nostr(hodlhodl_orders, 'hodlhodl', parse_hodlhodl_to_nostr, os.environ.get('HODLHODL_NOSTR_NSEC'))

        print_log(f"DONE GOING TO SLEEP")
        await asyncio.sleep(300)  # Wait for 5 minutes

if __name__ == "__main__":
    print_log(f"START")
    asyncio.run(main())
