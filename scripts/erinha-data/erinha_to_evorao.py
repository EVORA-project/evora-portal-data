#!/usr/bin/env python3
import csv
import json
from pathlib import Path
from typing import Dict, Any, List

# -------------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------------

INPUT_CSV = Path("data/erinha/erinha_catalogue.csv")
OUTPUT_JSONLD = Path("data/erinha/erinha_services.jsonld")

# -------------------------------------------------------------------------
# JSON-LD context
# -------------------------------------------------------------------------

CONTEXT = {
    "@vocab": "https://w3id.org/evorao/",
    "EVORAO": "https://w3id.org/evorao/",
    "dcterms": "http://purl.org/dc/terms/",
    "dct": "http://purl.org/dc/terms/",
    "dcat": "http://www.w3.org/ns/dcat#",
    "search": "https://w3id.org/evorao/search#",
    "foaf": "http://xmlns.com/foaf/0.1/",
}

# Assay columns: additionalCategory if cell == "x"
TECH_COLUMNS: List[str] = [
    "Virus isolation",
    "Typing",
    "Virus titration",
    "Neutralisation assay",
    "Antiviral assay",
    "Cytotoxicity",
    "Cell Viability",
    "PRNT / microneutralisation",
    "CPE (TCID50)",
    "Focus Forming Assay",
    "RT-qPCR",
    "Serology",
    "Immuno-fluorescence",
    "Confocal microscopy",
    "Electron microscopy",
    "Histo-chemistry",
    "Histo-pathology",
    "ELISA",
    "Bead-based ELISA",
    "ELISPOT / FLUOSPOT",
    "Sequencing",
    "Flow cytometry",
    "Proteomics",
    "Transcriptomics",
]

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

    for v in row.values():
        if normalize(v) in header_norm:
            matches += 1

    similarity = matches / total if total else 0
    return similarity >= 0.80


def split_keywords(raw: str) -> List[str]:
    if not raw:
        return []
    parts: List[str] = []
    for chunk in raw.replace(";", ",").split(","):
        w = chunk.strip()
        if w:
            parts.append(w)
    # De-duplicate, preserve order
    return list(dict.fromkeys(parts))


# -------------------------------------------------------------------------
# Pathogen identification builder
# -------------------------------------------------------------------------


def build_pathogen_identification(row: Dict[str, str]) -> Dict[str, Any]:
    """
    Map:
      Pathogen name        → pathogenName:VirusName:dcterms:title
      ICTV nomenclature    → taxon:Taxon:dcterms:title
      Viral strain         → strain
    """

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
        row.get("Viral strain")
        or row.get("Viral Strain")
        or ""
    ).strip()

    pid: Dict[str, Any] = {"@type": "EVORAO:PathogenIdentification"}

    if pathogen_name:
        pid["EVORAO:pathogenName"] = {
            "@type": "EVORAO:VirusName",
            "dcterms:title": pathogen_name,
        }

    if viral_strain:
        pid["EVORAO:strain"] = viral_strain

    if taxon_label:
        pid["EVORAO:taxon"] = {
            "@type": "EVORAO:Taxon",
            "dcterms:title": taxon_label,
        }

    return pid


# -------------------------------------------------------------------------
# Additional categories
# -------------------------------------------------------------------------


def add_additional_categories(entity: Dict[str, Any], row: Dict[str, str]) -> None:
    """
    EVORAO:Service:additionalCategory:
      - Research Model Category
      - Experimental model name
      - For each TECH_COLUMNS where the cell == "x", add the column title
    """
    cats: List[Dict[str, Any]] = []

    def add_cat(label: str) -> None:
        label = (label or "").strip()
        if not label:
            return
        # Avoid duplicates by title
        for c in cats:
            if c.get("dcterms:title") == label:
                return
        cats.append(
            {
                "@type": "EVORAO:ProductCategory",
                "dcterms:title": label,
            }
        )

    # Research Model Category
    add_cat(row.get("Research Model Category", ""))

    # Experimental model name
    add_cat(row.get("Experimental model name", ""))

    # Assay columns where value is "x"
    for col in TECH_COLUMNS:
        val = (row.get(col) or "").strip()
        if val.lower() == "x":
            add_cat(col)

    if cats:
        entity["EVORAO:additionalCategory"] = cats


# -------------------------------------------------------------------------
# Convert one ERINHA row → EVORAO:Service
# -------------------------------------------------------------------------


def row_to_service(row: Dict[str, str], idx: int) -> Dict[str, Any]:
    ref_sku = (row.get("refSku") or "").strip() or f"ERINHA-{idx}"
    title = (row.get("Title") or "").strip()
    desc = (row.get("Description") or "").strip()
    keywords_raw = (row.get("Keywords") or "").strip()
    collection = (row.get("Collection") or "").strip()
    provider = (row.get("Provider") or "").strip()
    containment = (row.get("Containment level") or "").strip()
    model_type = (row.get("Model type") or "").strip()
    host_species = (row.get("Host species") or "").strip()
    infection_route = (row.get("Animal infection route") or "").strip()
    availability = (row.get("Availability") or "").strip()
    unit_cost_note = (row.get("Unit cost note") or "").strip()
    access_url = (row.get("accessPointURL") or "").strip()
    other_note = (row.get("Other") or "").strip()

    # EVA rule: "TBD" → ERINHA general apply page
    if access_url == "TBD":
        access_url = "https://erinha.eu/apply-for-services/"

    urn = f"urn:evorao:ERINHA:Service:{ref_sku}"

    entity: Dict[str, Any] = {
        "@id": urn,
        "@type": "EVORAO:Service",
        "EVORAO:refSku": ref_sku,
        "dcterms:title": title,
        "dcterms:description": desc,
        "dcat:keyword": [],
        "search:keywords": [],
        "search:taxon": [],
    }

    # Main category: always "service"
    entity["EVORAO:category"] = {
        "@type": "EVORAO:ProductCategory",
        "dcterms:title": "service",
    }

    # Provider → EVORAO:provider:Provider:foaf:name
    if provider:
        entity["EVORAO:provider"] = {
            "@type": "EVORAO:Provider",
            "foaf:name": provider,
        }

    # Collection → EVORAO:collection:Collection:dcterms:title
    if collection:
        entity["EVORAO:collection"] = {
            "@type": "EVORAO:Collection",
            "dcterms:title": collection,
        }

        # Publisher derived from Collection
        # If Collection == ERINHA → fully described ResearchInfrastructure
        if collection == "ERINHA":
            entity["EVORAO:publisher"] = {
                "@type": "EVORAO:ResearchInfrastructure",
                "foaf:name": "European Research Infrastructure on Highly Pathogenic Agents",
                "EVORAO:alternateName": "ERINHA",
                "EVORAO:homepage": "https://www.erinha.eu/",
                "EVORAO:rorId": "https://ror.org/008y8yz21",
            }
        else:
            # Otherwise we only know the name
            entity["EVORAO:publisher"] = {
                "@type": "EVORAO:ResearchInfrastructure",
                "foaf:name": collection,
            }

    # Containment level → biosafetyLevel:BiosafetyLevel:dcterms:title
    if containment:
        entity["EVORAO:biosafetyLevel"] = {
            "@type": "EVORAO:BiosafetyLevel",
            "dcterms:title": containment,
        }

    # Model type → EVORAO:modelType (string)
    if model_type:
        entity["EVORAO:modelType"] = model_type

    # Host species → modelSpecies (string)
    if host_species:
        entity["EVORAO:modelSpecies"] = host_species

    # Animal infection route → technicalRecommendation (prefixed)
    if infection_route:
        entity["EVORAO:technicalRecommendation"] = (
            f"Animal infection route: {infection_route}"
        )

    # Availability → EVORAO:availability
    if availability:
        entity["EVORAO:availability"] = availability

    # Unit cost note → EVORAO:unitCostNote
    if unit_cost_note:
        entity["EVORAO:unitCostNote"] = unit_cost_note

    # accessPointURL → EVORAO:accessPointUrl
    if access_url:
        entity["EVORAO:accessPointUrl"] = access_url

    # Other → EVORAO:note
    if other_note:
        entity["EVORAO:note"] = other_note

    # Keywords:
    #   - EVORAO:keywords (Keyword objects)
    #   - dcat:keyword (strings)
    #   - search:keywords (strings)
    evorao_keywords: List[Dict[str, Any]] = []
    for kw in split_keywords(keywords_raw):
        entity["dcat:keyword"].append(kw)
        entity["search:keywords"].append(kw)
        evorao_keywords.append(
            {
                "@type": "EVORAO:Keyword",
                "dcterms:title": kw,
            }
        )
    if evorao_keywords:
        entity["EVORAO:keywords"] = evorao_keywords

    # Pathogen identification (name, taxon, strain)
    pid = build_pathogen_identification(row)
    if pid:
        entity["EVORAO:pathogenIdentification"] = pid

        # Seed search:taxon & extra keywords with the taxon label
        taxon_obj = pid.get("EVORAO:taxon")
        if isinstance(taxon_obj, dict):
            t_label = taxon_obj.get("dcterms:title")
            if t_label:
                entity["search:taxon"].append(t_label)
                entity["search:keywords"].append(t_label)
                entity["dcat:keyword"].append(t_label)

        # And with the pathogen name
        pname = pid.get("EVORAO:pathogenName", {}).get("dcterms:title")
        if pname:
            entity["search:keywords"].append(pname)
            entity["dcat:keyword"].append(pname)

    # Additional categories
    add_additional_categories(entity, row)

    # De-duplicate keyword / search lists
    for key in ["dcat:keyword", "search:keywords", "search:taxon"]:
        entity[key] = list(dict.fromkeys(entity[key]))

    return entity


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------


def main() -> int:
    if not INPUT_CSV.exists():
        raise SystemExit(f"❌ CSV not found: {INPUT_CSV}")

    # Read all rows
    with INPUT_CSV.open(encoding="utf-8") as f:
        raw_rows = list(csv.reader(f))

    if not raw_rows:
        raise SystemExit("❌ CSV appears to be empty.")

    # Row 0 = true header
    header = raw_rows[0]
    raw_data_rows = raw_rows[1:]

    # Convert each following row into a dict
    dict_rows: List[Dict[str, str]] = []
    for row in raw_data_rows:
        if not any(c.strip() for c in row):
            continue  # skip fully empty rows
        dict_rows.append(dict(zip(header, row)))

    graph: List[Dict[str, Any]] = []
    idx = 2  # logical numbering for fallback refSku

    for row in dict_rows:
        # Skip ERINHA's internal duplicate header row
        if is_header_like(row, header):
            print("⚠️ Skipping ERINHA internal header row")
            continue

        entity = row_to_service(row, idx)
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
