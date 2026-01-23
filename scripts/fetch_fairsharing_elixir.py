#!/usr/bin/env python3
import os
import json
import requests
from pathlib import Path

OUTPUT_DIR = Path("data/fairsharing")
OUTPUT_JSON = OUTPUT_DIR / "fairsharing_5449.json"

FAIRSHARING_LOGIN = os.environ.get("FAIRSHARING_LOGIN")
FAIRSHARING_PWD = os.environ.get("FAIRSHARING_PWD")

FAIRSHARING_RECORD_ID = 5449  # EVORA curated collection


def fetch_jwt() -> str:
    if not FAIRSHARING_LOGIN or not FAIRSHARING_PWD:
        raise SystemExit("❌ FAIRSHARING_LOGIN or FAIRSHARING_PWD not provided.")

    url = "https://api.fairsharing.org/users/sign_in"
    payload = {
        "user": {
            "login": FAIRSHARING_LOGIN,
            "password": FAIRSHARING_PWD
        }
    }

    r = requests.post(url, json=payload, headers={
        "Accept": "application/json",
        "Content-Type": "application/json"
    })

    if r.status_code != 200:
        raise SystemExit(f"❌ FAIRsharing login failed: {r.status_code} {r.text}")

    return r.json().get("jwt")


def fetch_record(jwt: str, rec_id: int) -> dict:
    url = f"https://api.fairsharing.org/fairsharing_records/{rec_id}"

    r = requests.get(url, headers={
        "Accept": "application/json",
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json"
    })

    if r.status_code != 200:
        raise SystemExit(f"❌ Failed to fetch FAIRsharing record {rec_id}: {r.status_code}")

    return r.json()


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("🔑 Logging into FAIRsharing…")
    jwt = fetch_jwt()

    print("📥 Fetching FAIRsharing record 5449…")
    data = fetch_record(jwt, FAIRSHARING_RECORD_ID)

    with OUTPUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ Saved FAIRsharing record to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
