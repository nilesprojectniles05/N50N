import os, requests, datetime

BASE_URL = "https://api.dhan.co/v2"
ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")

HEADERS = {
    "accept": "application/json",
    "access-token": ACCESS_TOKEN
}

# ---------------- INDEX LTP ----------------
def get_index_ltp(symbol):
    url = f"{BASE_URL}/market-quote/quotes"
    payload = {
        "symbols": [symbol]
    }
    r = requests.post(url, json=payload, headers=HEADERS)
    data = r.json()
    return float(data["data"][symbol]["last_price"])

# ---------------- OPTION CHAIN ----------------
def get_option_chain(base_symbol):
    url = f"{BASE_URL}/option-chain"
    payload = {
        "symbol": base_symbol
    }
    r = requests.post(url, json=payload, headers=HEADERS)
    data = r.json()["data"]
    return data

# ---------------- LATEST EXPIRY ----------------
def get_latest_expiry(option_chain_data):
    expiries = sorted(option_chain_data.keys())
    return expiries[0]   # nearest expiry

# ---------------- ATM STRIKE ----------------
def get_atm_strike(ltp, step=50):
    return int(round(ltp / step) * step)
