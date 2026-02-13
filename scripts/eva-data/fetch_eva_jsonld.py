#!/usr/bin/env python3
import json
import os
import sys
from typing import Any, Dict, List
import itertools
import time
from pathlib import Path


import requests

BASE_URL = (
    "https://www.european-virus-archive.com/"
    "jsonldfeed/evatoevorao/{token}&p={page}"
)

FINAL_DIR = Path("data/eva/pages")
TMP_DIR = Path("data/eva/pages_tmp")
META_FILE = Path("data/eva/.meta.json")

MAX_PAGES = 100  # safety cap


def fetch_page(page: int, token: str, retries: int = 4, base_wait: float = 2.0) -> Dict[str, Any]:
    url = BASE_URL.format(page=page, token=token)
    wait = base_wait
    for attempt in range(1, retries + 1):
        try:
            print(f"🔎 Fetching EVA page {page} (attempt {attempt}/{retries})")
            resp = requests.get(url, timeout=45)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"⚠️  Error fetching EVA page {page}: {e}", file=sys.stderr)
            if attempt == retries:
                print(f"❌ Giving up on page {page} after {retries} attempts.", file=sys.stderr)
                # Return an empty @graph to stop pagination gracefully
                return {"@graph": []}
            time.sleep(wait)
            wait *= 2  # exponential backoff


def main() -> int:
    token = os.environ.get("EVA_FEED_TOKEN")
    if not token:
        raise SystemExit("❌ EVA_FEED_TOKEN env variable is missing.")

    TMP_DIR.mkdir(parents=True, exist_ok=True)

    total_items = 0
    context_seen = False

    for page in itertools.count(1):
        if page > MAX_PAGES:
            print(f"🚨 Reached MAX_PAGES={MAX_PAGES}, stopping.", file=sys.stderr)
            break

        data = fetch_page(page, token)
        graph: List[Dict[str, Any]] = data.get("@graph") or []

        # Stop when graph is empty (only schema/context or fetch failed)
        if not graph:
            print(f"✅ Page {page} has empty @graph (or fetch failed), stopping pagination.")
            break

        if not context_seen and "@context" in data:
            context_seen = True

        page_path = TMP_DIR / f"eva_p{page}.jsonld"
        with open(page_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        total_items += len(graph)
        print(f"📄 Saved page {page} with {len(graph)} items → {page_path}")

    # ---------------- VALIDATION ----------------
    tmp_pages = list(TMP_DIR.glob("eva_p*.jsonld"))
    if not tmp_pages:
        print("❌ No EVA pages downloaded — keeping previous dataset.")
        for f in TMP_DIR.glob("*"):
            f.unlink()
        TMP_DIR.rmdir()
        return 1

    # optional: previous count check
    prev_count = 0
    if META_FILE.exists():
        prev_count = json.loads(META_FILE.read_text()).get("pages", 0)

    new_count = len(tmp_pages)
    print(f"📊 EVA pages: previous={prev_count}, new={new_count}")

    if prev_count and new_count < prev_count * 0.5:
        print("❌ Suspicious shrink — keeping previous dataset.")
        return 1

    # ---------------- ATOMIC SWAP ----------------
    if FINAL_DIR.exists():
        for f in FINAL_DIR.glob("*"):
            f.unlink()
    else:
        FINAL_DIR.mkdir(parents=True)

    for f in TMP_DIR.glob("*"):
        f.rename(FINAL_DIR / f.name)

    TMP_DIR.rmdir()

    META_FILE.write_text(json.dumps({"pages": new_count}))
    print("✅ EVA dataset updated atomically.")
    print(f"🎯 Finished EVA pagination. Total items across pages: {total_items}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
