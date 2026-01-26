#!/usr/bin/env python3
import json
import os
import time
from pathlib import Path
from typing import Dict, Any

import requests

BASE_URL = "https://api.fairsharing.org"
COLLECTION_ID = "5449"

OUTPUT_DIR = Path("data/fairsharing")
OUTPUT_JSON = OUTPUT_DIR / "fairsharing_5449.json"


def robust_request(method: str, url: str, *, headers: Dict[str, str], json_payload=None,
                   retries: int = 4, backoff: float = 1.0) -> requests.Response:
    """
    Simple retry wrapper for FAIRsharing calls.
    Raises after retries are exhausted.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(
                method,
                url,
                headers=headers,
                json=json_payload,
                timeout=30,
            )
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"⚠️ FAIRsharing request failed ({attempt}/{retries}) to {url}: {e}")
            if attempt == retries:
                raise
            time.sleep(backoff)
            backoff *= 2


def get_jwt() -> str:
    login = os.environ.get("FAIRSHARING_LOGIN")
    pwd = os.environ.get("FAIRSHARING_PWD")

    if not login or not pwd:
        raise SystemExit("❌ FAIRSHARING_LOGIN or FAIRSHARING_PWD env variable is missing.")

    url = f"{BASE_URL}/users/sign_in"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    payload = {"user": {"login": login, "password": pwd}}

    resp = robust_request("POST", url, headers=headers, json_payload=payload)
    data = resp.json()
    jwt = data.get("jwt")
    if not jwt:
        raise SystemExit("❌ Could not obtain JWT from FAIRsharing response.")
    print("✅ Obtained FAIRsharing JWT.")
    return jwt


def get_record(jwt: str, record_id: str) -> Dict[str, Any]:
    url = f"{BASE_URL}/fairsharing_records/{record_id}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {jwt}",
    }
    resp = robust_request("GET", url, headers=headers)
    return resp.json()


def main() -> int:
    jwt = get_jwt()

    # 1) Fetch the EVORA collection record itself
    print(f"🔎 Fetching FAIRsharing collection {COLLECTION_ID}…")
    collection = get_record(jwt, COLLECTION_ID)

    # 2) Extract linked record IDs from the collection
    coll_data = collection.get("data", {})
    coll_attrs = coll_data.get("attributes", {}) if isinstance(coll_data, dict) else {}
    linked_list = coll_attrs.get("linked_records") or []

    record_ids = sorted({str(lr.get("linked_record_id")) for lr in linked_list if lr.get("linked_record_id")})
    print(f"✅ Collection has {len(record_ids)} linked FAIRsharing records: {', '.join(record_ids) or 'none'}")

    # 3) Fetch each linked record
    records: Dict[str, Any] = {}
    for idx, rid in enumerate(record_ids, start=1):
        print(f"  → Fetching linked record {rid} ({idx}/{len(record_ids)})…")
        records[rid] = get_record(jwt, rid)

    # 4) Save aggregated raw JSON
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "collection": collection,
        "records": records,
    }
    with OUTPUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"✅ Saved FAIRsharing EVORA collection + {len(records)} linked records → {OUTPUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
