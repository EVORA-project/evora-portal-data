"""Cache explicit OLS identity annotations for parent links already present in the export."""
import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import tempfile
import time
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


def index_terms(cache):
    terms = defaultdict(set)
    parents = defaultdict(set)
    for name, row in cache.items():
        if name.startswith("_") or not isinstance(row, dict):
            continue
        for field in ("_evora_target", "current", "final", "obsolete"):
            term = row.get(field)
            if not isinstance(term, dict):
                continue
            version, iri, identifier, label = (term.get(k) for k in ("msl", "iri", "ictv_id", "label"))
            if not all(isinstance(v, str) and v for v in (version, iri, identifier, label)):
                continue
            terms[version, iri].add((label, identifier))
            parents[version, identifier, label].add((term.get("direct_parent_iri"), term.get("direct_parent_label")))
    for version, entries in cache.get("_parent_terms", {}).items():
        for iri, term in entries.items():
            if all(isinstance(term.get(k), str) and term[k] for k in ("label", "ictv_id")):
                terms[version, iri].add((term["label"], term["ictv_id"]))
    return terms, parents


def parent_ids_from_cache(cache):
    terms, parents = index_terms(cache)
    result = {}
    for key, links in parents.items():
        if len(links) != 1:
            continue
        parent_iri, parent_label = next(iter(links))
        annotated = terms.get((key[0], parent_iri), set())
        if len(annotated) == 1:
            label, identifier = next(iter(annotated))
            if label == parent_label:
                result[(*key, label)] = identifier
    return result


def ictv_key(taxon):
    taxonomy = taxon.get("EVORAO:taxonomy")
    if not isinstance(taxonomy, dict) or taxonomy.get("dcterms:title") != "ICTV":
        return None
    key = (taxonomy.get("dcat:version"), taxon.get("EVORAO:taxonomicId"), taxon.get("dcterms:title"))
    return key if all(isinstance(v, str) and v.strip() for v in key) else None


def identified_taxa(record):
    identifications = record.get("EVORAO:pathogenIdentification")
    for identification in identifications if isinstance(identifications, list) else [identifications]:
        if not isinstance(identification, dict):
            continue
        taxon = identification.get("EVORAO:taxon")
        if isinstance(taxon, dict):
            key = ictv_key(taxon)
            if key:
                yield identification, taxon, key


def missing_parents(graph, terms, parents):
    requests = set()
    for record in graph:
        for _, taxon, key in identified_taxa(record):
            parent = taxon.get("EVORAO:parentTaxon")
            if not isinstance(parent, dict) or "EVORAO:taxonomicId" in parent:
                continue
            links = parents.get(key, set())
            if len(links) == 1:
                iri, label = next(iter(links))
                if iri and label == parent.get("dcterms:title") and (key[0], iri) not in terms:
                    requests.add((key[0], iri, label))
    return requests


def fetch_annotation(iri, timeout):
    encoded = quote(quote(iri, safe=""), safe="")
    url = "https://www.ebi.ac.uk/ols4/api/v2/ontologies/ictv/entities/" + encoded
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def string_value(value):
    if isinstance(value, list) and len(value) == 1:
        value = value[0]
    return value if isinstance(value, str) and value.strip() else None


def update_parent_terms(cache, requests, fetch=fetch_annotation):
    if not requests:
        return 0
    ordered = sorted(requests)
    # Resume the bounded pass on subsequent rebuilds, including after a 404.
    last = tuple(cache.get("_parent_lookup_cursor", []))
    split = next((i for i, key in enumerate(ordered) if key > last), 0)
    deadline = time.monotonic() + 60
    stored = 0
    for version, iri, label in (ordered[split:] + ordered[:split])[:24]:
        remaining = deadline - time.monotonic()
        if remaining < 1:
            break
        cache["_parent_lookup_cursor"] = [version, iri, label]
        try:
            raw = fetch(iri, min(8, remaining))
        except HTTPError as exc:
            if exc.code in (404, 410):
                continue
            print(f"::warning::OLS parent metadata unavailable (HTTP {exc.code}); retaining known annotations.")
            break
        except (OSError, ValueError):
            print("::warning::OLS parent metadata unavailable; retaining known annotations.")
            break
        if not isinstance(raw, dict):
            continue
        identifier = string_value(raw.get("http://purl.org/dc/terms/identifier"))
        actual_version = string_value(raw.get("http://www.w3.org/2002/07/owl#versionInfo"))
        actual_label = string_value(raw.get("label") or raw.get("http://www.w3.org/2000/01/rdf-schema#label"))
        if raw.get("iri") == iri and actual_label == label and actual_version == version and identifier:
            cache.setdefault("_parent_terms", {}).setdefault(version, {})[iri] = {"label": label, "ictv_id": identifier}
            stored += 1
    return stored


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cache", type=Path)
    parser.add_argument("exports", type=Path, nargs="+")
    args = parser.parse_args()
    cache = json.loads(args.cache.read_bytes())
    terms, parents = index_terms(cache)
    needed = set()
    for source in args.exports:
        needed.update(missing_parents(json.loads(source.read_bytes())["@graph"], terms, parents))
    if not needed:
        print("All referenced parent annotations available or lacking a unique cached reference.")
        return
    count = update_parent_terms(cache, needed)
    print(f"Verified {count} additional parent annotations; {len(needed)} unique references needed checking.")
    target = None
    try:
        with tempfile.NamedTemporaryFile(dir=args.cache.parent, delete=False, mode="w", encoding="utf-8") as stream:
            target = Path(stream.name)
            json.dump(cache, stream, ensure_ascii=False, indent=2)
        os.replace(target, args.cache)
    finally:
        if target is not None:
            target.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
