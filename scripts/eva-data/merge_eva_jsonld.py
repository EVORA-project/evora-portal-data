#!/usr/bin/env python3
import copy
import glob
import json
import os
from typing import Any, Dict, List

PAGES_DIR = "data/eva/pages"
MERGED_PATH = "data/eva/eva_merged.jsonld"



EVA_METADATA_LICENSE = {
    "@id": "https://creativecommons.org/licenses/by/4.0/",
    "@type": "EVORAO:License",
    "dcterms:title": "CC-BY 4.0",
    "EVORAO:resourceUrl": "https://creativecommons.org/licenses/by/4.0/",
    "EVORAO:licensingOrAttribution": (
        "European Virus Archive - AISBL (EVA). Public catalogue data and metadata; "
        "adapted and enriched for the EVORA Portal. "
        "https://www.european-virus-archive.com/legal-mentions#open-licence-and-data-reuse"
    ),
}


def add_eva_metadata_license(entity):
    """Apply EVA's public catalogue default, preserving specific licence notices.

    This describes the source catalogue metadata, not biological materials,
    products or third-party images. Existing collection names remain unchanged.
    """
    collections = entity.get("EVORAO:collection")
    if not collections:
        collections = {"@type": "EVORAO:Collection", "dcterms:title": "European Virus Archive"}
        entity["EVORAO:collection"] = collections
    for collection in collections if isinstance(collections, list) else [collections]:
        if not isinstance(collection, dict):
            continue
        provider = collection.setdefault("EVORAO:collectionDataProvider", {
            "@type": "EVORAO:DataProvider",
            "dcterms:title": "European Virus Archive",
            "EVORAO:homepage": "https://www.european-virus-archive.com/",
        })
        if not isinstance(provider, dict):
            continue
        specific = collection.get("EVORAO:license") or collection.get("dcterms:license") or entity.get("dcterms:license")
        provider.setdefault("EVORAO:license", copy.deepcopy(specific or EVA_METADATA_LICENSE))


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

    # -------------------------------------------------
    # Ensure no duplicate by @id
    # -------------------------------------------------
    unique = {}
    for e in merged_graph:
        eid = e.get("@id")
        if eid:
            unique[eid] = e   # last occurrence wins
        else:
            unique[id(e)] = e  # fallback safety

    merged_graph = list(unique.values())
    for entity in merged_graph:
        add_eva_metadata_license(entity)

    os.makedirs(os.path.dirname(MERGED_PATH), exist_ok=True)
    merged = {"@context": context, "@graph": merged_graph}

    with open(MERGED_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote merged EVA JSON-LD → {MERGED_PATH} ({len(merged_graph)} items)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
