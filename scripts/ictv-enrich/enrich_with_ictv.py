#!/usr/bin/env python3
import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Any, Optional, Set
from datetime import datetime

from ictv_api import ICTVOLSClient  # fetched by workflow into this folder


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
            print(f"⚠️ ICTV call failed (attempt {attempt}/{retries}): {e}")
            if attempt == retries:
                print(" → giving up on this label.\n")
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
            print(f" Loaded ICTV cache from {cache_path} ({len(data)} entries)")
            return data
        except Exception as e:
            print(f"⚠️ Failed to load cache {cache_path}: {e}")
    return {}


def save_cache(cache: Dict[str, Any], cache_path: Path):
    try:
        cache["_fetched_at"] = datetime.utcnow().isoformat()

        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)

        print(f" Saved ICTV cache to {cache_path} ({len(cache)} entries)")
    except Exception as e:
        print(f"⚠️ Failed to save cache {cache_path}: {e}")


def collect_labels_for_resolution(graph) -> Set[str]:
    """
    Collect all candidate labels to resolve:
    - EVORAO:pathogenIdentification / EVORAO:pathogenName / dcterms:title
    - EVORAO:pathogenIdentification / EVORAO:taxon / dcterms:title
    """
    labels: Set[str] = set()

    for node in graph:
        pid = node.get("EVORAO:pathogenIdentification")
        if not isinstance(pid, dict):
            continue

        pn = pid.get("EVORAO:pathogenName")
        if isinstance(pn, dict):
            t = pn.get("dcterms:title") or pn.get("dct:title")
            if t:
                labels.add(str(t).strip())

        tax = pid.get("EVORAO:taxon")
        if isinstance(tax, dict):
            t = tax.get("dcterms:title") or tax.get("dct:title")
            if t:
                labels.add(str(t).strip())

    return labels


def resolve_label_once(label: str) -> Optional[Dict[str, Any]]:
    """
    Worker: each thread uses its own ICTV client.
    Wrapped in robust_call, so it never raises.
    """
    client = ICTVOLSClient()
    return robust_call(client.resolveToLatest, label)


def resolve_all_labels(labels: Set[str], cache: Dict[str, Any], max_workers: int = 8):
    """
    Resolve all unique labels using threads + caching.
    Existing cache entries are reused as-is.
    """
    missing = [lbl for lbl in labels if lbl not in cache]
    total_missing = len(missing)

    if total_missing == 0:
        print("✅ All labels already in cache")
        return cache

    print(f" Resolving {total_missing} ICTV labels (max_workers={max_workers})")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(resolve_label_once, lbl): lbl for lbl in missing}

        for i, future in enumerate(as_completed(futures), start=1):
            label = futures[future]
            try:
                res = future.result()
            except Exception as e:
                print(f"❌ Unexpected error resolving '{label}': {e}")
                res = None

            cache[label] = res

            if i % 20 == 0 or i == total_missing:
                print(f" → resolved {i}/{total_missing}")

    return cache


# ============================================================
# ICTV → EVORAO conversion
# ============================================================

def pick_best_ictv_entity(res: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize the ICTV resolution object to a single chosen entity:
      - if status == "current": res["current"]
      - if status == "obsolete": res["final"] if present else res["obsolete"]
      - else: None
    """
    if not isinstance(res, dict):
        return None

    status = res.get("status")
    if status == "current":
        return res.get("current")
    if status == "obsolete":
        final = res.get("final")
        return final if final else res.get("obsolete")
    return None


def ictv_entity_to_evorao_taxon(
    ent: Dict[str, Any],
    original_label: Optional[str],
    existing_taxon: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build an EVORAO:Taxon structure using ONLY existing EVORAO properties:
      - dcterms:title
      - EVORAO:taxonomicId
      - EVORAO:taxonomy (→ EVORAO:Taxonomy)
      - EVORAO:rank (→ EVORAO:TaxonomicRank)
      - EVORAO:parentTaxon (→ EVORAO:Taxon)
      - EVORAO:alternateName (→ EVORAO:AlternateName)
    """
    label = ent.get("label") or original_label or ""

    taxon: Dict[str, Any] = {
        "@type": "EVORAO:Taxon",
        "dcterms:title": label,
    }

    # Preserve existing @id if present (important for stable references)
    if isinstance(existing_taxon, dict) and "@id" in existing_taxon:
        taxon["@id"] = existing_taxon["@id"]

    # --- taxonomicId (ICTV ID as persistent taxon identifier) ---
    ictv_id = ent.get("ictv_id")
    if ictv_id:
        taxon["EVORAO:taxonomicId"] = str(ictv_id)

    # --- Taxonomy node (ICTV, with MSL as version) ---
    msl = ent.get("msl")
    taxonomy_node: Optional[Dict[str, Any]] = None
    if msl:
        taxonomy_node = {
            "@type": "EVORAO:Taxonomy",
            "dcterms:title": "ICTV",
            "dcat:version": str(msl),
            "EVORAO:versionDataProvider": {
                "@type": "EVORAO:DataProvider",
                "dcterms:title": "ICTV via OLS4",
                "EVORAO:weight": 1,
            },
        }
        taxon["EVORAO:taxonomy"] = taxonomy_node

    # --- Rank as EVORAO:TaxonomicRank ---
    rank_info = ent.get("rank") or {}
    rank_label = (rank_info.get("label") or "").strip()
    if rank_label:
        rank_node: Dict[str, Any] = {
            "@type": "EVORAO:TaxonomicRank",
            "dcterms:title": rank_label,
        }
        if taxonomy_node is not None:
            rank_node["EVORAO:taxonomy"] = taxonomy_node
        taxon["EVORAO:rank"] = rank_node

    # --- Parent taxon (stub, with label only) ---
    parent_label = ent.get("direct_parent_label")
    if parent_label:
        parent_node: Dict[str, Any] = {
            "@type": "EVORAO:Taxon",
            "dcterms:title": parent_label,
        }
        taxon["EVORAO:parentTaxon"] = parent_node

    # --- Alternate names as EVORAO:AlternateName ---
    alt_objs = []

    # ICTV synonyms → AlternateName
    for s in ent.get("synonyms") or []:
        if isinstance(s, str):
            s_clean = s.strip()
            if s_clean:
                alt_objs.append(
                    {
                        "@type": "EVORAO:AlternateName",
                        "dcterms:title": s_clean,
                    }
                )

    # Original local label (if different from ICTV label) → AlternateName
    if original_label:
        orig_clean = original_label.strip()
        if orig_clean and orig_clean != label:
            alt_objs.append(
                {
                    "@type": "EVORAO:AlternateName",
                    "dcterms:title": orig_clean,
                }
            )

    # Merge with any existing AlternateName entries, if present
    if isinstance(existing_taxon, dict):
        existing_alts = existing_taxon.get("EVORAO:alternateName") or []
        if isinstance(existing_alts, list):
            alt_objs.extend([a for a in existing_alts if isinstance(a, dict)])

    # Deduplicate alternate names by title
    if alt_objs:
        seen_titles = set()
        uniq_alts = []
        for a in alt_objs:
            title = (a.get("dcterms:title") or a.get("dct:title") or "").strip()
            if not title:
                continue
            if title in seen_titles:
                continue
            seen_titles.add(title)
            uniq_alts.append(a)
        if uniq_alts:
            taxon["EVORAO:alternateName"] = uniq_alts

    # --- Lineage (flattened, display-oriented, strings only) ---
    lineage_vals = []
    for l in ent.get("lineage") or []:
        if isinstance(l, dict):
            lbl = l.get("label")
            if lbl:
                lineage_vals.append(str(lbl).strip())
        elif isinstance(l, str):
            lineage_vals.append(l.strip())

    # Remove empty + keep order
    lineage_vals = [x for x in lineage_vals if x]

    if lineage_vals:
        taxon["EVORAO:lineage"] = lineage_vals

    return taxon


def expand_search_fields(
    entity: Dict[str, Any],
    taxon_obj: Dict[str, Any],
    original_label: Optional[str],
    ent: Dict[str, Any],
):
    """
    Populate dcat:keyword, and search:taxon
    with:
      - ICTV label (Taxon title)
      - original taxon label (if different)
      - AlternateName titles (synonyms + former labels)
      - lineage labels (ancestors)
      - direct parent label
    """
    labels = []

    # Main ICTV label
    main_label = taxon_obj.get("dcterms:title") or taxon_obj.get("dct:title")
    if main_label:
        entity["search:taxonLabel"] = str(main_label)
        labels.append(str(main_label))

    # Local original label (if distinct)
    if original_label:
        orig_clean = original_label.strip()
        if orig_clean and orig_clean != main_label:
            labels.append(orig_clean)

    # Alternate names (EVORAO:AlternateName)
    for alt in taxon_obj.get("EVORAO:alternateName", []):
        if isinstance(alt, dict):
            t = alt.get("dcterms:title") or alt.get("dct:title")
            if t:
                labels.append(str(t))

    # Direct parent label
    parent_label = ent.get("direct_parent_label")
    if parent_label:
        labels.append(str(parent_label))

    # Lineage (ancestors labels)
    for l in taxon_obj.get("EVORAO:lineage", []):
        labels.append(str(l))

    # Deduplicate, keep order, remove empty
    labels = list(dict.fromkeys([x for x in labels if x]))

    # Push into the search-related fields
    for field in ["dcat:keyword", "search:taxon"]:
        arr = ensure_list(entity, field)
        for lbl in labels:
            if lbl not in arr:
                arr.append(lbl)


# ============================================================
# Enrichment of entities using the cache
# ============================================================

def enrich_graph_with_cache(graph: list, cache: Dict[str, Any]):
    total = len(graph)
    print(f" Enriching {total} entities with ICTV data...")

    for i, node in enumerate(graph, start=1):
        pid = node.get("EVORAO:pathogenIdentification")
        if not isinstance(pid, dict):
            continue

        # 1) Extract virus name and existing taxon label
        virus_name = None
        pn = pid.get("EVORAO:pathogenName")
        if isinstance(pn, dict):
            virus_name = pn.get("dcterms:title") or pn.get("dct:title")

        existing_taxon = pid.get("EVORAO:taxon")
        taxon_label = None
        if isinstance(existing_taxon, dict):
            taxon_label = (
                existing_taxon.get("dcterms:title")
                or existing_taxon.get("dct:title")
            )

        resolved = None
        ent = None

        # 2) Choose resolution preference: virus name first, then taxon label
        for candidate in (virus_name, taxon_label):
            if not candidate:
                continue

            key = str(candidate).strip()
            if not key:
                continue

            resolved = cache.get(key)
            best = pick_best_ictv_entity(resolved) if resolved else None

            if best:
                ent = best
                break  # we found a valid ICTV entity

        # If no valid ICTV entity was found → do not touch the existing taxon
        if not ent:
            continue

        # 3) Build EVORAO-compliant Taxon, preserving @id if possible
        new_taxon = ictv_entity_to_evorao_taxon(
            ent,
            taxon_label,
            existing_taxon=existing_taxon,
        )
        pid["EVORAO:taxon"] = new_taxon

        # 4) Expand search fields (keywords, taxon search)
        expand_search_fields(node, new_taxon, taxon_label, ent)

        if i % 50 == 0 or i == total:
            print(f" → enriched {i}/{total}")


# ============================================================
# Main file processing
# ============================================================

def enrich_file(input_path: Path, output_path: Path, cache_path: Path):
    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    graph = data.get("@graph") or []
    print(f" Loaded graph with {len(graph)} entities from {input_path}")

    # 1) Collect all labels to resolve
    labels = collect_labels_for_resolution(graph)
    print(f" Found {len(labels)} unique candidate labels to resolve")

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
        description=(
            "Enrich EVORAO JSON-LD with ICTV taxonomy using ICTVOLSClient "
            "+ caching + parallel requests."
        )
    )
    parser.add_argument("--input", "-i", required=True, help="Input JSON-LD file")
    parser.add_argument("--output", "-o", required=True, help="Output JSON-LD file")
    parser.add_argument(
        "--cache",
        "-c",
        help="Cache file path (default: ictv_cache.json next to input)",
    )

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
