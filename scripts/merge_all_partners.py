#!/usr/bin/env python3
import json
from pathlib import Path
from typing import Any, Dict, List

INPUT_FILES = [
    Path("data/fairsharing/fairsharing_elixir_services.jsonld"),
    Path("data/erinha/erinha_services_enriched.jsonld"),
    Path("data/eva/eva_merged_enriched.jsonld"),
]

OUTPUT_PATH = Path("data/portal/evora_portal_all.jsonld")


def main() -> int:
    merged_graph: List[Dict[str, Any]] = []
    context = None

    for path in INPUT_FILES:
        if not path.exists():
            print(f"⚠️ Skipping missing partner file: {path}")
            continue

        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if context is None and "@context" in data:
            context = data["@context"]

        graph = data.get("@graph") or []
        merged_graph.extend(graph)
        print(f"➕ {len(graph)} items from {path}")

    if context is None:
        # Fallback minimal context
        context = {
            "@vocab": "https://w3id.org/evorao/",
            "EVORAO": "https://w3id.org/evorao/",
            "dcterms": "http://purl.org/dc/terms/",
            "dct": "http://purl.org/dc/terms/",
            "dcat": "http://www.w3.org/ns/dcat#",
            "search": "https://w3id.org/evorao/search#",
        }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    out = {"@context": context, "@graph": merged_graph}
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote merged portal data → {OUTPUT_PATH} ({len(merged_graph)} items)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
