key_id = 'DUMMY_KEY_ID'
secret_key = 'DUMMY_SECRET_KEY'

import requests
import asyncio
from polymarket_us import PolymarketUS
import os

def handle_market_data_lite(d):
    print(f"Best Bid: {d['marketDataLite']['bestBid']['value']}, Best Ask: {d['marketDataLite']['bestAsk']['value']}")

def print_markets_from_event(client, event_slug):
    """Fetches and prints all available markets inside a parent event."""
    print(f"\n🔍 Fetching all markets for event: {event_slug}")
    try:
        # Use the SDK to request event data
        event_data = client.events.retrieve_by_slug(event_slug)
        
        # Safely parse the response (handles both wrapped {"event": {...}} and direct payloads)
        event_obj = event_data.get("event", event_data) if isinstance(event_data, dict) else {}
        markets = event_obj.get("markets", [])
        
        if not markets:
            print("⚠️ No markets found for this event.")
            return

        print("================ AVAILABLE MARKETS ================")
        for i, market in enumerate(markets):
            title = market.get("title") or market.get("question") or "No Title"
            print(f"[{i}] Title: {title}")
            print(f"    Slug:  {market.get('slug')}")
            print("-" * 55)
            
    except Exception as e:
        print(f"❌ Error fetching event data: {e}")

# Define your slugs
event_slug = "itfme-borrui-calapo-2026-07-21" # Replace with your target event slug
market_slug = "aec-itfme-borrui-calapo-2026-07-21" # Your target market stream

async def main():
    client = PolymarketUS(
        key_id=key_id,
        secret_key=secret_key,
    )

    # 1. Print all markets from the event container
    print_markets_from_event(client, event_slug)

    # 2. Proceed with WebSocket subscription
    ws = client.ws.markets()

    # Register event handlers
    ws.on("market_data_lite", lambda d: handle_market_data_lite(d))

    print(f"\nConnecting and subscribing to market stream: {market_slug}...")
    await ws.connect()
    await ws.subscribe("prices", "SUBSCRIPTION_TYPE_MARKET_DATA_LITE", [market_slug])

    await asyncio.sleep(3600)
    await ws.close()

if __name__ == "__main__":
    asyncio.run(main())