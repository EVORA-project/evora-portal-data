#!/usr/bin/env python3
import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Any, Optional, Set

from ictv_api import ICTVOLSClient  # make sure ictv_api.py is importable


# ============================================================
# Basic helpers
# ============================================================

def robust_call(func, *args, retries: int = 4, base_wait: float = 0.5, **kwargs):
    """
    Call a function with simple exponential backoff.
    Any exception is caught; after all retries, returns None.
    """
    wait = base_wait
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"⚠️  ICTV call failed (attempt {attempt}/{retries}): {e}", flush=True)
            if attempt == retries:
                print("   → giving up on this label.\n", flush=True)
                return None
            time.sleep(wait)
            wait *= 2


def ensure_list(d: Dict[str, Any], key: str):
    v = d.get(key)
    if v is None:
        d[key] = []
        return d[key]
    if isinstance(v, list):
        return v
    d[key] = [v]
    return d[key]


# ============================================================
# ICTV resolution + caching
# ============================================================

def load_cache(cache_path: Path) -> Dict[str, Any]:
    if cache_path.exists():
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"📂 Loaded ICTV cache from {cache_path} ({len(data)} entries)")
            return data
        except Exception as e:
            print(f"⚠️  Failed to load cache {cache_path}: {e}")
    return {}


def save_cache(cache: Dict[str, Any], cache_path: Path):
    try:
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        print(f"💾 Saved ICTV cache to {cache_path} ({len(cache)} entries)")
    except Exception as e:
        print(f"⚠️  Failed to save cache {cache_path}: {e}")


def collect_labels_for_resolution(graph) -> Set[str]:
    labels: Set[str] = set()
    for node in graph:
        pid = node.get("EVORAO:pathogenIdentification")
        if not isinstance(pid, dict):
            continue

        virus_name = None
        pn = pid.get("EVORAO:pathogenName")
        if isinstance(pn, dict):
            virus_name = pn.get("dcterms:title") or pn.get("dct:title")

        taxon_label = None
        taxon_obj = pid.get("EVORAO:taxon")
        if isinstance(taxon_obj, dict):
            taxon_label = taxon_obj.get("dcterms:title") or taxon_obj.get("dct:title")

        for val in (virus_name, taxon_label):
            if val:
                s = str(val).strip()
                if s:
                    labels.add(s)
    return labels


def resolve_label_once(label: str) -> Optional[Dict[str, Any]]:
    """
    Worker function: create its own ICTV client, resolve a label once.
    Wrapped with robust_call so it never throws.
    """
    client = ICTVOLSClient()
    return robust_call(client.resolveToLatest, label)


def resolve_all_labels(labels: Set[str], cache: Dict[str, Any], max_workers: int = 8) -> Dict[str, Any]:
    """
    Resolve all unique labels using threads + caching.
    Existing cache entries are reused.
    """
    missing = [lbl for lbl in labels if lbl not in cache]
    total_missing = len(missing)

    if total_missing == 0:
        print("✅ All labels already in cache, no new ICTV calls needed.")
        return cache

    print(f"🔎 Resolving {total_missing} ICTV labels (in parallel with {max_workers} workers)...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(resolve_label_once, lbl): lbl for lbl in missing}

        for i, future in enumerate(as_completed(futures), start=1):
            label = futures[future]
            try:
                res = future.result()
            except Exception as e:
                print(f"❌ Unexpected error resolving '{label}': {e}")
                res = None

            # Store raw ICTV resolution result (may be None)
            cache[label] = res

            if i % 20 == 0 or i == total_missing:
                print(f"   → resolved {i}/{total_missing} labels", flush=True)

    return cache


# ============================================================
# Convert ICTV result → EVORAO Taxon
# ============================================================

def pick_best_ictv_entity(res: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(res, dict):
        return None

    status = res.get("status")
    if status == "current":
        return res.get("current")
    if status == "obsolete":
        # If there's a final replacement, prefer it
        if res.get("final"):
            return res["final"]
        return res.get("obsolete")
    return None


def ictv_entity_to_evorao_taxon(ent: Dict[str, Any], original_label: Optional[str]) -> Dict[str, Any]:
    taxon: Dict[str, Any] = {
        "@type": "EVORAO:Taxon",
        "dcterms:title": ent.get("label") or original_label or ""
    }

    if ent.get("ictv_id"):
        taxon["EVORAO:taxonomicId"] = ent["ictv_id"]
    if ent.get("msl"):
        taxon["EVORAO:mslRelease"] = ent["msl"]
    if ent.get("ictv_curie"):
        taxon["EVORAO:ictvCurie"] = ent["ictv_curie"]
    if ent.get("iri"):
        taxon["EVORAO:ictvIri"] = ent["iri"]

    syns = ent.get("synonyms") or []
    if syns:
        taxon["EVORAO:synonym"] = syns

    if ent.get("direct_parent_label"):
        taxon["EVORAO:directParentName"] = ent["direct_parent_label"]

    lineage = ent.get("lineage") or []
    if lineage:
        taxon["EVORAO:lineage"] = lineage

    rank = ent.get("rank") or {}
    if rank.get("label"):
        taxon["EVORAO:taxonomicRank"] = rank["label"]

    if original_label and original_label.strip() and original_label.strip() != taxon["dcterms:title"]:
        taxon["EVORAO:originalTaxonLabel"] = original_label.strip()

    return taxon


def expand_search_fields(entity: Dict[str, Any], taxon_obj: Dict[str, Any], original_label: Optional[str]):
    labels = []

    main_label = taxon_obj.get("dcterms:title")
    if main_label:
        labels.append(main_label)

    if original_label:
        labels.append(original_label)

    for s in taxon_obj.get("EVORAO:synonym", []):
        labels.append(s)

    for l in taxon_obj.get("EVORAO:lineage", []):
        labels.append(l)

    # dedupe, keep non-empty
    labels = list(dict.fromkeys([x for x in labels if x]))

    for field in ["dcat:keyword", "search:keywords", "search:taxon"]:
        arr = ensure_list(entity, field)
        for lbl in labels:
            if lbl not in arr:
                arr.append(lbl)


# ============================================================
# Enrichment of entities using the cache
# ============================================================

def enrich_graph_with_cache(graph: list, cache: Dict[str, Any]):
    total = len(graph)
    print(f"🧪 Enriching {total} entities with ICTV data...")

    for i, node in enumerate(graph, start=1):
        pid = node.get("EVORAO:pathogenIdentification")
        if not isinstance(pid, dict):
            continue

        virus_name = None
        pn = pid.get("EVORAO:pathogenName")
        if isinstance(pn, dict):
            virus_name = pn.get("dcterms:title") or pn.get("dct:title")

        taxon_label = None
        taxon_obj = pid.get("EVORAO:taxon")
        if isinstance(taxon_obj, dict):
            taxon_label = taxon_obj.get("dcterms:title") or taxon_obj.get("dct:title")

        # Choose resolution preference: virus name first, then taxon label
        resolved = None
        used_label = None
        for candidate in (virus_name, taxon_label):
            if not candidate:
                continue
            key = str(candidate).strip()
            if not key:
                continue
            resolved = cache.get(key)
            used_label = key
            if resolved is not None:
                break

        if not resolved:
            # No ICTV info available for this entity
            continue

        ent = pick_best_ictv_entity(resolved)
        if not ent:
            continue

        new_taxon = ictv_entity_to_evorao_taxon(ent, taxon_label)
        pid["EVORAO:taxon"] = new_taxon
        expand_search_fields(node, new_taxon, taxon_label)

        if i % 50 == 0 or i == total:
            print(f"   → enriched {i}/{total}", flush=True)


# ============================================================
# Main file processing
# ============================================================

def enrich_file(input_path: Path, output_path: Path, cache_path: Path):
    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    graph = data.get("@graph") or []
    print(f"📖 Loaded graph with {len(graph)} entities from {input_path}")

    # 1) Collect all labels to resolve
    labels = collect_labels_for_resolution(graph)
    print(f"🔍 Found {len(labels)} unique candidate labels to resolve")

    # 2) Load cache
    cache = load_cache(cache_path)

    # 3) Resolve missing labels in parallel
    cache = resolve_all_labels(labels, cache, max_workers=8)

    # 4) Save cache
    save_cache(cache, cache_path)

    # 5) Enrich all entities using cached results
    enrich_graph_with_cache(graph, cache)

    # 6) Write output JSON-LD
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ Enriched file written to {output_path}")


# ============================================================
# CLI
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Enrich EVORAO JSON-LD with ICTV taxonomy using ICTVOLSClient + caching + parallel requests."
    )
    parser.add_argument("--input", "-i", required=True, help="Input JSON-LD file")
    parser.add_argument("--output", "-o", required=True, help="Output JSON-LD file")
    parser.add_argument("--cache", "-c", help="Cache file path (default: ictv_cache.json next to input)")

    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if args.cache:
        cache_path = Path(args.cache)
    else:
        cache_path = input_path.with_name("ictv_cache.json")

    enrich_file(input_path, output_path, cache_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
