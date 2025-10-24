# etl_connector.py
import os
import time
import json
import requests
from datetime import datetime

# -----------------------------
# Configuration
# -----------------------------
API_BASE_URL = "https://onionoo.torproject.org"
ENDPOINTS = ["details", "bandwidth", "clients"]  # Three endpoints to fetch
OUTPUT_FOLDER = "output"

# -----------------------------
# ETL Functions
# -----------------------------
def fetch_data(endpoint):
    """Extract: Fetch data from API"""
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        print(f"📡 Fetched data from endpoint: {endpoint}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Error fetching {endpoint}: {e}")
        return None

def transform_data(data, endpoint):
    """Transform: Add metadata and count items"""
    if data is None:
        return None

    # Count items based on response structure
    if "relays" in data:
        count = len(data["relays"])
    elif "clients" in data:
        count = len(data["clients"])
    else:
        count = len(data)

    transformed = {
        "endpoint": endpoint,
        "fetched_at": datetime.utcnow().isoformat(),
        "data_count": count,
        "raw": data
    }
    return transformed

def load_data(doc):
    """Load: Save transformed data to local JSON file"""
    if doc is None:
        return

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    filename = f"{OUTPUT_FOLDER}/{doc['endpoint']}_{int(time.time())}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=4)
    print(f"✅ Saved {doc['endpoint']} data to {filename}")

# -----------------------------
# Main ETL Runner
# -----------------------------
def run_etl():
    print("🚀 Starting ETL for Tor Exit Nodes")
    for endpoint in ENDPOINTS:
        data = fetch_data(endpoint)
        transformed = transform_data(data, endpoint)
        load_data(transformed)
        time.sleep(1)  # small delay between requests
    print("✅ ETL completed successfully!")

# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    run_etl()
