#!/usr/bin/env python3
import csv
import json
from pathlib import Path
from typing import Dict, Any, List


INPUT_CSV = Path("data/erinha/erinha_catalogue.csv")
OUTPUT_JSONLD = Path("data/erinha/erinha_services.jsonld")

CONTEXT = {
    "@vocab": "https://w3id.org/evorao/",
    "EVORAO": "https://w3id.org/evorao/",
    "dcterms": "http://purl.org/dc/terms/",
    "dct": "http://purl.org/dc/terms/",
    "dcat": "http://www.w3.org/ns/dcat#",
    "search": "https://w3id.org/evorao/search#"
}


# -------------------------------------------------------------------------
# Utilities
# -------------------------------------------------------------------------

def normalize(s: str) -> str:
    """Normalize strings to compare header-like structures."""
    return (s or "").lower().replace(" ", "").replace("_", "")


def is_header_like(row: Dict[str, str], header: List[str]) -> bool:
    """
    Detect ERINHA's second header row:
    It matches ≥ 80% of header fields after normalization.
    """
    header_norm = [normalize(h) for h in header]
    total = len(header_norm)
    matches = 0

    for k, v in row.items():
        if normalize(v) in header_norm:
            matches += 1

    similarity = matches / total if total else 0
    return similarity >= 0.80


def split_keywords(raw: str) -> List[str]:
    if not raw:
        return []
    parts = []
    for chunk in raw.replace(";", ",").split(","):
        w = chunk.strip()
        if w:
            parts.append(w)
    return list(dict.fromkeys(parts))


# -------------------------------------------------------------------------
# Pathogen identification builder
# -------------------------------------------------------------------------

def build_pathogen_identification(row: Dict[str, str]) -> Dict[str, Any]:
    # Priority order defined from real ERINHA CSV
    taxon_label = (
        (row.get("ICTV nomenclature") or "").strip()
        or (row.get("Taxon") or "").strip()
        or (row.get("Pathogen Name") or "").strip()
        or (row.get("Pathogen name") or "").strip()
    )

    pathogen_name = (
        row.get("Pathogen Name")
        or row.get("Pathogen name")
        or ""
    ).strip()

    viral_strain = (
        row.get("Strain")
        or row.get("Viral strain")
        or ""
    ).strip()

    pid = {"@type": "EVORAO:PathogenIdentification"}

    if pathogen_name:
        pid["EVORAO:pathogenName"] = {
            "@type": "EVORAO:VirusName",
            "dcterms:title": pathogen_name
        }

    if viral_strain:
        pid["EVORAO:strainDesignation"] = viral_strain

    if taxon_label:
        pid["EVORAO:taxon"] = {
            "@type": "EVORAO:Taxon",
            "dcterms:title": taxon_label
        }

    return pid


# -------------------------------------------------------------------------
# Convert one ERINHA row → EVORAO Service
# -------------------------------------------------------------------------

def row_to_service(row: Dict[str, str], idx: int) -> Dict[str, Any]:
    ref_sku = (row.get("refSku") or "").strip() or f"ERINHA-{idx}"
    title = (row.get("Title") or "").strip()
    desc = (row.get("Description") or "").strip()
    provider = (row.get("Provider") or "").strip()
    keywords_raw = (row.get("Keywords") or "").strip()
    model_type = (row.get("Model type") or "").strip()
    category = (row.get("Inquiry type") or row.get("Category") or "").strip()
    rm_category = (row.get("Research Model Category") or "").strip()
    access_url = (row.get("accessPointURL") or "").strip()

    # Fix your rule: "TBD" → ERINHA general apply page
    if access_url == "TBD":
        access_url = "https://erinha.eu/apply-for-services/"

    urn = f"urn:evorao:ERINHA:Service:{ref_sku}"

    entity = {
        "@id": urn,
        "@type": "EVORAO:Service",
        "EVORAO:refSku": ref_sku,
        "dcterms:title": title,
        "dcterms:description": desc,
        "dcat:keyword": [],
        "search:keywords": [],
        "search:taxon": []
    }

    if provider:
        entity["EVORAO:provider"] = provider
    if model_type:
        entity["EVORAO:modelType"] = model_type
    if category:
        entity["EVORAO:category"] = category
    if rm_category:
        entity["EVORAO:researchModelCategory"] = rm_category
    if access_url:
        entity["EVORAO:accessPointURL"] = access_url

    # Keywords
    for kw in split_keywords(keywords_raw):
        entity["dcat:keyword"].append(kw)
        entity["search:keywords"].append(kw)

    # Pathogen & Taxon
    pid = build_pathogen_identification(row)
    if pid:
        entity["EVORAO:pathogenIdentification"] = pid

        # taxon label seed
        taxon_obj = pid.get("EVORAO:taxon")
        if taxon_obj:
            t_label = taxon_obj.get("dcterms:title")
            if t_label:
                entity["search:taxon"].append(t_label)
                entity["search:keywords"].append(t_label)
                entity["dcat:keyword"].append(t_label)

        # pathogen name
        pname = pid.get("EVORAO:pathogenName", {}).get("dcterms:title")
        if pname:
            entity["search:keywords"].append(pname)
            entity["dcat:keyword"].append(pname)

    # dedupe
    for key in ["dcat:keyword", "search:keywords", "search:taxon"]:
        entity[key] = list(dict.fromkeys(entity[key]))

    return entity


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------

def main() -> int:
    if not INPUT_CSV.exists():
        raise SystemExit(f"❌ CSV not found: {INPUT_CSV}")

    # Read all rows manually to avoid DictReader guessing issues
    with INPUT_CSV.open(encoding="utf-8") as f:
        raw_rows = list(csv.reader(f))

    # Row 0 = true header
    header = raw_rows[0]
    raw_data_rows = raw_rows[1:]

    # Convert each following row into a dict
    reader = []
    for row in raw_data_rows:
        if not any(c.strip() for c in row):
            continue  # skip empty rows
        reader.append(dict(zip(header, row)))

    graph = []
    idx = 2  # logical numbering

    for row in reader:

        # Skip ERINHA's internal duplicate header row
        if is_header_like(row, header):
            print("⚠️ Skipping ERINHA internal header row")
            continue

        try:
            entity = row_to_service(row, idx)
        except ValueError:
            continue

        graph.append(entity)
        idx += 1

    OUTPUT_JSONLD.parent.mkdir(parents=True, exist_ok=True)

    jsonld = {"@context": CONTEXT, "@graph": graph}
    with OUTPUT_JSONLD.open("w", encoding="utf-8") as f:
        json.dump(jsonld, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote {len(graph)} ERINHA services → {OUTPUT_JSONLD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
