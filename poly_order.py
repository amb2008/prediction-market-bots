import time
import base64
import requests
import json
# Import the Ed25519 primitive directly instead of serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

# ==========================================
# CONFIGURATION
# ==========================================
# Your API Key ID generated at polymarket.us/developer
API_KEY_ID = 'DUMMY_KEY_ID'

# Path to your poly.txt file containing the raw secret key string
PRIVATE_KEY_PATH = 'DUMMY_PRIVATE_KEY_PATH'

BASE_URL = "https://api.polymarket.us"
ORDER_PATH = "/v1/orders"

def load_private_key(path: str):
    """Loads the Ed25519 private key from the secret key text file string."""
    with open(path, "r") as _fh:
        secret_key_str = _fh.read().strip()
    
    # Polymarket US secret keys are base64 encoded.
    # We decode them and extract the first 32 bytes to build the key object.
    private_bytes = base64.b64decode(secret_key_str)[:32]
    return ed25519.Ed25519PrivateKey.from_private_bytes(private_bytes)

def generate_headers(method: str, path: str, private_key) -> dict:
    """Generates the required Ed25519 signature and authentication headers."""
    # Timestamp must be in milliseconds and within 30 seconds of server time
    timestamp = str(int(time.time() * 1000))
    
    # Message to sign: timestamp + method + path
    message = (timestamp + method + path).encode('utf-8')
    
    # Sign using Ed25519 and base64-encode the result
    signature = private_key.sign(message)
    signature_b64 = base64.b64encode(signature).decode('utf-8')
    
    return {
        "Content-Type": "application/json",
        "X-PM-Access-Key": API_KEY_ID,
        "X-PM-Signature": signature_b64,
        "X-PM-Timestamp": timestamp
    }

def place_order():
    private_key = load_private_key(PRIVATE_KEY_PATH)
    
    # ==========================================
    # ORDER DETAILS
    # ==========================================
    payload = {
        "marketSlug": "aadc-fwc-eng-cod-2026-07-01-to-advance-eng",   # Replace with the actual market slug
        "type": "ORDER_TYPE_LIMIT",                 # ORDER_TYPE_LIMIT or ORDER_TYPE_MARKET
        "action": "ORDER_ACTION_BUY",               # ORDER_ACTION_BUY or ORDER_ACTION_SELL
        "outcomeSide": "OUTCOME_SIDE_YES",          # OUTCOME_SIDE_YES or OUTCOME_SIDE_NO
        "quantity": 1,   
        "price":
            {"value":"0.90",
            "currency": "USD"},
        "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL"     # Expiration policy
    }
    
    print(f"Signing order to {payload['action']} {payload['quantity']} {payload['outcomeSide']}")
    headers = generate_headers("POST", ORDER_PATH, private_key)
    
    try:
        response = requests.post(f"{BASE_URL}{ORDER_PATH}", headers=headers, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Order placed successfully!")
            print(f"Order ID: {data.get('id')}")
            if "executions" in data:
                print(f"Executions: {json.dumps(data['executions'], indent=2)}")
        else:
            print(f"⚠️ Failed to place order. Status Code: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Network or execution error: {e}")

if __name__ == "__main__":
    place_order()