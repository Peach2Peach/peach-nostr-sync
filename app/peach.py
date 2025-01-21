# peach.py

import asyncio
import json
import os
import time

import requests
from db import db_file_name, order_expiration, prepare_db
from logs import print_log
from nostr import publish_to_nostr
from nostr_sdk import EventBuilder, Keys, Kind, Tag


def fetch_peach_orders():
    base_url = "https://api.peachbitcoin.com/v1/offer/search/nostr"
    orders = []
    headers = {
        'Content-Type': 'application/json'
    }

    while True:
        url = f"{base_url}"
        print_log(f"Fetching {url}")
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print_log(f"Error fetching data: {response.status_code}")
            break
        
        data = response.json()

        if data.get("offers") is None:
            print_log("Failed to fetch offers.")
            break
        
        orders.extend(data.get("offers", []))
        
        if len(data.get("offers", [])) < size:
            break

        page += 1

    print_log(f"Found {len(orders)} Peach orders")
    return orders

def parse_peach_to_nostr(order, keys, status):
    identifier = order.get('id')
    timestamp_in_2_hours = int(time.time() + order_expiration)

    order_type = "sell" if order.get("type") == "ask" else "buy"

    rating_data = {
        "total_reviews": order.get("ratingCount", 0),
        "total_rating": order.get("rating", 0)
    }

    amount = order.get("amount", 0)
    premium = order.get("premium", 0)
    prices = order.get("prices", {})

    if isinstance(amount, list):
        # Range: amt = 0, fa = 0 if we dont have info details
        amt = "0"
        fa = ["0"]
    else:
        amt = str(amount)
        if prices:
            first_currency = next(iter(prices))
            fa_value = prices[first_currency]
            fa = [str(fa_value)]
        else:
            fa = ["0"]

    source_url = ""
    network = "mainnet"
    layer = "onchain"
    bond = "0"

    events = []
    means_of_payment = order.get("meansOfPayment", {})
    for currency, methods in means_of_payment.items():
        tags = [
            Tag.parse(["d", str(identifier) + currency]),  # d = order_id + currency
            Tag.parse(["k", order_type]),
            Tag.parse(["s", status]),
            Tag.parse(["amt", amt]),
            Tag.parse(["fa"] + fa),
            Tag.parse(["premium", str(premium)]),
            Tag.parse(["rating", json.dumps(rating_data)]),
            Tag.parse(["source", source_url]),
            Tag.parse(["network", network]),
            Tag.parse(["layer", layer]),
            Tag.parse(["name", order.get("userId", "")]),
            Tag.parse(["bond", bond]),
            Tag.parse(["expiration", str(timestamp_in_2_hours)]),
            Tag.parse(["y", "peach"]),
            Tag.parse(["z", "order"]),
            Tag.parse(["f", currency]),
            Tag.parse(["pm"] + methods)
        ]

        event = EventBuilder(
            Kind(38383),
            "",
            tags
        ).to_event(keys)
        events.append(event)

    return events

async def main():
    print_log("START LOOP")
    prepare_db()
    while True:
        print_log("PEACH")
        peach_orders = fetch_peach_orders()
        await publish_to_nostr(peach_orders, 'peach', parse_peach_to_nostr, os.environ.get('PEACH_NOSTR_NSEC'))
        print_log("DONE GOING TO SLEEP")
        await asyncio.sleep(300)

if __name__ == "__main__":
    print_log("START")
    asyncio.run(main())