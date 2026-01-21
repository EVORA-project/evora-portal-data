#!/usr/bin/env python3
import glob
import json
import os
from typing import Any, Dict, List

PAGES_DIR = "data/eva/pages"
MERGED_PATH = "data/eva/eva_merged.jsonld"


def main() -> int:
    page_files = sorted(glob.glob(os.path.join(PAGES_DIR, "eva_p*.jsonld")))
    if not page_files:
        raise SystemExit(f"No page files found under {PAGES_DIR}")

    merged_graph: List[Dict[str, Any]] = []
    context = None

    for path in page_files:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if context is None and "@context" in data:
            context = data["@context"]

        graph = data.get("@graph") or []
        merged_graph.extend(graph)
        print(f"➕ Merging {len(graph)} items from {path}")

    if context is None:
        raise SystemExit("No @context found in any page – aborting merge.")

    os.makedirs(os.path.dirname(MERGED_PATH), exist_ok=True)
    merged = {"@context": context, "@graph": merged_graph}

    with open(MERGED_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote merged EVA JSON-LD → {MERGED_PATH} ({len(merged_graph)} items)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
