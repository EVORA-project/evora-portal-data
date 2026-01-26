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


def ensure_list_unique(vals: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for v in vals:
        v = (v or "").strip()
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def extract_keywords(attrs: Dict[str, Any], meta: Dict[str, Any]) -> List[str]:
    kws: List[str] = []
    for key in ("subjects", "domains", "taxonomies", "user_defined_tags", "object_types"):
        val = attrs.get(key) or meta.get(key)
        if isinstance(val, list):
            for item in val:
                s = str(item).strip()
                if s:
                    kws.append(s)
    return ensure_list_unique(kws)


def build_viruses_taxon_and_pid() -> Dict[str, Any]:
    """
    Generic viruses placeholder:

      - Taxon title: "Viruses"
      - EVORAO:taxonomicId: "NCBITaxon:10239"
      - VirusName: "any virus"
    """
    taxon = {
        "@type": "EVORAO:Taxon",
        "dcterms:title": "Viruses",
        "EVORAO:taxonomicId": "NCBITaxon:10239",
    }

    pid = {
        "@type": "EVORAO:PathogenIdentification",
        "EVORAO:pathogenName": {
            "@type": "EVORAO:VirusName",
            "dcterms:title": "any virus",
        },
        "EVORAO:taxon": taxon,
    }

    return pid


def build_collection_block() -> Dict[str, Any]:
    """
    EVORAO:collection + collectionDataProvider + license
    with FAIRsharing CC-BY-SA 4.0 + logo HTML snippet.
    """
    return {
        "@type": "EVORAO:Collection",
        "dcterms:title": "FAIRsharing",
        "EVORAO:collectionDataProvider": {
            "@type": "EVORAO:DataProvider",
            "dcterms:title": "FAIRsharing",
            "EVORAO:homepage": "https://fairsharing.org/",
            "EVORAO:license": {
                "@type": "EVORAO:License",
                "dcterms:title": "CC-BY-SA 4.0",
                "EVORAO:licensingOrAttribution": (
                    "<img src=\"https://api.fairsharing.org/img/fairsharing-attribution.svg\" "
                    "alt=\"FAIRsharing Logo\">"
                ),
            },
        },
    }


def main() -> int:
    if not INPUT_JSON.exists():
        raise SystemExit(f"❌ FAIRsharing raw JSON not found: {INPUT_JSON}")

    with INPUT_JSON.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    collection = raw.get("collection", {})
    records_by_id: Dict[str, Any] = raw.get("records", {})

    coll_data = collection.get("data", {}) if isinstance(collection, dict) else {}
    coll_attrs = coll_data.get("attributes", {}) if isinstance(coll_data, dict) else {}
    linked_list = coll_attrs.get("linked_records") or []

    graph: List[Dict[str, Any]] = []

    for lr in linked_list:
        rid = str(lr.get("linked_record_id"))
        if not rid:
            continue

        record_payload = records_by_id.get(rid)
        if not isinstance(record_payload, dict):
            print(f"⚠️ No fetched record for FAIRsharing ID {rid}, skipping.")
            continue

        data = record_payload.get("data", {})
        attrs = data.get("attributes", {}) if isinstance(data, dict) else {}
        meta = attrs.get("metadata") or {}

        fs_id = data.get("id", rid)

        # Title preference: metadata.name > attributes.name > linked_record_name
        title = (
            (meta.get("name") or "").strip()
            or (attrs.get("name") or "").strip()
            or (lr.get("linked_record_name") or "").strip()
            or f"FAIRsharing record {fs_id}"
        )

        description = (
            (attrs.get("description") or "").strip()
            or (meta.get("description") or "").strip()
        )

        url = (attrs.get("url") or "").strip() or (meta.get("homepage") or "").strip()

        keywords = extract_keywords(attrs, meta)

        urn = f"urn:evorao:FAIRSHARING:Service:{fs_id}"

        service: Dict[str, Any] = {
            "@id": urn,
            "@type": "EVORAO:Service",
            "EVORAO:refSku": str(fs_id),
            "dcterms:title": title,
            "dcterms:description": description,
            "dcat:keyword": keywords.copy(),
            "search:keywords": keywords.copy(),
            "search:taxon": [],
        }

        if url:
            service["EVORAO:accessPointURL"] = url

        # Category is always "service"
        service["EVORAO:category"] = {
            "@type": "EVORAO:ProductCategory",
            "dcterms:title": "service",
        }

        # Additional category = FAIRsharing linked_record_type (e.g. "repository", "knowledgebase", …)
        linked_type = (lr.get("linked_record_type") or "").strip()
        if linked_type:
            service["EVORAO:additionalCategory"] = [
                {
                    "@type": "EVORAO:ProductCategory",
                    "dcterms:title": linked_type,
                }
            ]

        # Provider: ELIXIR Europe as the RI
        service["EVORAO:provider"] = {
            "@type": "EVORAO:ResearchInfrastructure",
            "foaf:name": "ELIXIR Europe",
            "EVORAO:alternateName": [
                {
                    "@type": "EVORAO:AlternateName",
                    "dcterms:title": "ELIXIR",
                }
            ],
        }

        # Collection + FAIRsharing licence / attribution
        service["EVORAO:collection"] = build_collection_block()

        # Generic viruses taxon + pathogen identification
        pid = build_viruses_taxon_and_pid()
        service["EVORAO:pathogenIdentification"] = pid

        # Add "Viruses" to search fields and keywords
        for label in ["Viruses"]:
            if label not in service["dcat:keyword"]:
                service["dcat:keyword"].append(label)
            for field in ("search:keywords", "search:taxon"):
                if label not in service[field]:
                    service[field].append(label)

        # Final de-dup on keyword/search arrays
        service["dcat:keyword"] = ensure_list_unique(service["dcat:keyword"])
        service["search:keywords"] = ensure_list_unique(service["search:keywords"])
        service["search:taxon"] = ensure_list_unique(service["search:taxon"])

        graph.append(service)

    OUTPUT_JSONLD.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSONLD.open("w", encoding="utf-8") as f:
        json.dump({"@context": CONTEXT, "@graph": graph}, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote {len(graph)} ELIXIR FAIRsharing services → {OUTPUT_JSONLD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
