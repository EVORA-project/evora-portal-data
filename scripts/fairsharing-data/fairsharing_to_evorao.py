#!/usr/bin/env python3
import json
from pathlib import Path
from typing import Dict, Any, List, Optional

INPUT_JSON = Path("data/fairsharing/fairsharing_5449.json")
OUTPUT_JSONLD = Path("data/fairsharing/fairsharing_elixir_services.jsonld")

CONTEXT = {
    "@vocab": "https://w3id.org/evorao/",
    "EVORAO": "https://w3id.org/evorao/",
    "dcterms": "http://purl.org/dc/terms/",
    "dcat": "http://www.w3.org/ns/dcat#",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "search": "https://w3id.org/evorao/search#"
}


# -------------------------------
# UTILS
# -------------------------------

def ensure_unique(seq: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in seq:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


# -------------------------------
# TAXON EXTRACTION
# -------------------------------

def build_taxon_from_ncbi_id(ncbi_id: str) -> Dict[str, Any]:
    """Return a minimal EVORAO:Taxon from a known NCBI id."""
    return {
        "@type": "EVORAO:Taxon",
        "EVORAO:taxonomicId": f"NCBI:{ncbi_id}",
        # ICTV enrichment will later try to refine with correct labels, parents, ranks.
    }


def build_taxon_from_label(label: str) -> Dict[str, Any]:
    """Return an EVORAO:Taxon with only a title. ICTV enrichment will replace/complete it."""
    return {
        "@type": "EVORAO:Taxon",
        "dcterms:title": label.strip()
    }


def extract_taxon(fair_attrs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract a taxon from FAIRsharing metadata if available.
    Else fallback to "Viruses" (NCBI:10239).
    """
    # 1. FAIRsharing field "taxonomies"
    tax = fair_attrs.get("taxonomies") or []
    if len(tax) == 1 and isinstance(tax[0], str):
        if tax[0].lower() == "viruses":
            return {
                "@type": "EVORAO:Taxon",
                "dcterms:title": "Viruses",
                "EVORAO:taxonomicId": "NCBI:10239"
            }
        else:
            return build_taxon_from_label(tax[0])

    # 2. FAIRsharing "ncbi_taxonomy_id" field (not always present)
    if "ncbi_taxonomy_id" in fair_attrs and fair_attrs["ncbi_taxonomy_id"]:
        return build_taxon_from_ncbi_id(str(fair_attrs["ncbi_taxonomy_id"]))

    # 3. Fallback: Viruses (NCBI 10239)
    return {
        "@type": "EVORAO:Taxon",
        "dcterms:title": "Viruses",
        "EVORAO:taxonomicId": "NCBI:10239"
    }


# -------------------------------
# MAIN
# -------------------------------

def main() -> int:
    if not INPUT_JSON.exists():
        raise SystemExit(f"❌ FAIRsharing record not found: {INPUT_JSON}")

    with INPUT_JSON.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    data = raw.get("data") or {}
    attrs = data.get("attributes") or {}
    md = attrs.get("metadata") or {}

    collection_name = md.get("name") or attrs.get("name") or ""
    collection_desc = md.get("description") or attrs.get("description") or ""
    collection_id = attrs.get("id") or data.get("id") or "5449"

    linked_records = attrs.get("linked_records") or []

    base_keywords = ensure_unique(
        (attrs.get("subjects") or [])
        + (attrs.get("domains") or [])
        + (attrs.get("taxonomies") or [])
        + (attrs.get("user_defined_tags") or [])
    )

    # Extract ELIXIR-level taxon (applies to all records)
    collection_taxon = extract_taxon(attrs)

    graph: List[Dict[str, Any]] = []

    for lr in linked_records:

        lr_name = (lr.get("linked_record_name") or "").strip()
        lr_id = lr.get("linked_record_id")
        lr_registry = (lr.get("linked_record_registry") or "").strip()
        lr_type = (lr.get("linked_record_type") or "").strip()
        lr_relation = (lr.get("relation") or "").strip()

        if not lr_id:
            continue

        ref_sku = str(lr_id)
        urn = f"urn:evorao:ELIXIR:Service:{ref_sku}"

        service: Dict[str, Any] = {
            "@id": urn,
            "@type": "EVORAO:Service",
            "EVORAO:refSku": ref_sku,
            "dcterms:title": lr_name or f"FAIRsharing record {ref_sku}",
            "dcterms:description": collection_desc,
            "dcat:keyword": [],
            "search:keywords": [],
        }

        # Provider = ELIXIR Europe
        service["EVORAO:provider"] = {
            "@type": "EVORAO:Provider",
            "foaf:name": "ELIXIR Europe",
        }

        # Collection metadata
        service["EVORAO:collection"] = {
            "@type": "EVORAO:Collection",
            "dcterms:title": collection_name,
            "dcterms:description": collection_desc,
            "dcterms:identifier": f"FAIRsharing:{collection_id}",
        }

        # EVORAO:category = "service"
        service["EVORAO:category"] = {
            "@type": "EVORAO:ProductCategory",
            "dcterms:title": "service",
        }

        # AccessPoint = FAIRsharing page
        service["EVORAO:accessPointUrl"] = f"https://fairsharing.org/{ref_sku}"

        # Additional categories for registry/type
        additional = []
        for tag in (lr_registry, lr_type):
            tag = tag.strip()
            if tag:
                additional.append(
                    {
                        "@type": "EVORAO:ProductCategory",
                        "dcterms:title": tag
                    }
                )
        if additional:
            service["EVORAO:additionalCategory"] = additional

        # Keywords
        kw = []
        kw.extend(base_keywords)
        kw.extend([lr_registry, lr_type, lr_relation])
        kw = ensure_unique([x for x in kw if x])

        service["dcat:keyword"] = kw
        service["search:keywords"] = kw
        service["EVORAO:keywords"] = [
            {"@type": "EVORAO:Keyword", "dcterms:title": x} for x in kw
        ]

        # NEW: PathogenIdentification with TAXON
        # --------------------------------------------
        service["EVORAO:pathogenIdentification"] = {
            "@type": "EVORAO:PathogenIdentification",
            "EVORAO:taxon": collection_taxon,
        }

        graph.append(service)

    OUTPUT_JSONLD.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSONLD.open("w", encoding="utf-8") as f:
        json.dump({"@context": CONTEXT, "@graph": graph}, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote {len(graph)} ELIXIR FAIRsharing services → {OUTPUT_JSONLD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
