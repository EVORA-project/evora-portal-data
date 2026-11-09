#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import Dict, Any, List, Optional

INPUT_JSON = Path("data/fairsharing/fairsharing_5449.json")
OUTPUT_JSONLD = Path("data/fairsharing/fairsharing_elixir_services.jsonld")

COLLECTIONS = {
    "5449": {"slug": "elixir", "provider_name": "ELIXIR Europe", "provider_label": "ELIXIR", "collection_title": "FAIRsharing", "publisher_label": "FAIRsharing"},
    "7589": {"slug": "evora", "provider_name": "European Viral Outbreak Response Alliance (EVORA)", "provider_label": "EVORA", "collection_title": "EVORA resources (FAIRsharing)", "publisher_label": "EVORA"},
}

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


def build_collection_block(collection_id="5449") -> Dict[str, Any]:
    """
    EVORAO:collection + collectionDataProvider + license
    with FAIRsharing CC-BY-SA 4.0 + logo HTML snippet.
    """
    return {
        "@id": f"https://fairsharing.org/{collection_id}/",
        "@type": "EVORAO:Collection",
        "dcterms:title": COLLECTIONS[collection_id]["collection_title"],
        "EVORAO:collectionDataProvider": {
            "@type": "EVORAO:DataProvider",
            "dcterms:title": "FAIRsharing",
            "EVORAO:homepage": "https://fairsharing.org/",
            "EVORAO:license": {
                "@id": "https://creativecommons.org/licenses/by-sa/4.0/",
                "@type": "EVORAO:License",
                "dcterms:title": "CC-BY-SA 4.0",
                "EVORAO:resourceUrl": "https://creativecommons.org/licenses/by-sa/4.0/",
                "EVORAO:licensingOrAttribution": (
                    "<img src=\"https://api.fairsharing.org/img/fairsharing-attribution.svg\" "
                    "alt=\"FAIRsharing Logo\">"
                ),
            },
        },
    }


def convert_records(raw, collection_id="5449"):
    config = COLLECTIONS[collection_id]
    collection = raw.get("collection", {})
    records_by_id: Dict[str, Any] = raw.get("records", {})

    coll_data = collection.get("data", {}) if isinstance(collection, dict) else {}
    coll_attrs = coll_data.get("attributes", {}) if isinstance(coll_data, dict) else {}
    linked_list = coll_attrs.get("linked_records") or []

    graph: List[Dict[str, Any]] = []

    for lr in linked_list:
        if not lr.get("linked_record_id"):
            continue
        rid = str(lr["linked_record_id"])

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
            (meta.get("description") or "").strip()
            or (attrs.get("description") or "").strip()
        )

        url = (attrs.get("url") or "").strip() or (meta.get("homepage") or "").strip()

        keywords = extract_keywords(attrs, meta)

        # Keep existing ELIXIR identifiers stable and distinguish EVORA curation
        # when a FAIRsharing record appears in both collections.
        namespace = "FAIRSHARING" if collection_id == "5449" else f"FAIRSHARING:{collection_id}"
        urn = f"urn:evorao:{namespace}:Service:{fs_id}"

        service: Dict[str, Any] = {
            "@id": urn,
            "@type": "EVORAO:Service",
            "EVORAO:refSku": str(fs_id),
            "dcterms:source": {"@id": f"https://fairsharing.org/{fs_id}/"},
            "dcterms:title": title,
            "dcterms:description": description,
            "dcat:keyword": keywords.copy(),
            "EVORAO:availability": "online",
            "EVORAO:unitCostNote": "publicly available",
            "search:taxon": [],
        }

        if url:
            service["EVORAO:accessPointUrl"] = url

        # Category is always "service"
        service["EVORAO:category"] = {
            "@type": "EVORAO:ProductCategory",
            "dcterms:title": "service",
        }
        service["search:category"] =  "service"
        
        # Additional category = FAIRsharing linked_record_type (e.g. "repository", "knowledgebase", …)
        linked_type = (lr.get("linked_record_type") or "").strip()
        if linked_type:
            service["EVORAO:additionalCategory"] = [
                {
                    "@type": "EVORAO:ProductCategory",
                    "dcterms:title": linked_type,
                }
            ]
            service["search:additionalCategory"] = [linked_type]

        # Provider: ELIXIR Europe as the RI
        service["EVORAO:provider"] = {
            "@type": "EVORAO:ResearchInfrastructure",
            "foaf:name": config["provider_name"],
            "EVORAO:alternateName": [
                {
                    "@type": "EVORAO:AlternateName",
                    "dcterms:title": config["provider_label"],
                }
            ],
        }
        service["search:providerName"] = config["provider_label"]
        service["search:collectionName"] = config["collection_title"]
        service["search:publisherName"] = config["publisher_label"]
        if collection_id == "7589":
            service["EVORAO:provider"]["EVORAO:homepage"] = "https://evora-project.eu/"
            service["EVORAO:publisher"] = {
                "@type": "EVORAO:ResearchInfrastructure",
                "foaf:name": config["provider_name"],
                "EVORAO:alternateName": "EVORA",
                "EVORAO:homepage": "https://evora-project.eu/",
            }

        # Collection + FAIRsharing licence / attribution
        service["EVORAO:collection"] = build_collection_block(collection_id)

        # Generic viruses taxon + pathogen identification
        pid = build_viruses_taxon_and_pid()
        service["EVORAO:pathogenIdentification"] = pid

        # Add "Viruses" to search fields and keywords
        service["search:taxonLabel"] = "Viruses"
        service["search:pathogenName"] = "any virus"
        for label in ["Viruses"]:
            if label not in service["dcat:keyword"]:
                service["dcat:keyword"].append(label)
            for field in ("dcat:keyword", "search:taxon"):
                if label not in service[field]:
                    service[field].append(label)

        # Final de-dup on keyword/search arrays
        service["dcat:keyword"] = ensure_list_unique(service["dcat:keyword"])
        service["search:taxon"] = ensure_list_unique(service["search:taxon"])

        graph.append(service)

    # -------------------------------------------------
    # Ensure no duplicate by @id
    # -------------------------------------------------
    graph = list({e["@id"]: e for e in graph}.values())

    return {"@context": CONTEXT, "@graph": graph}


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert a curated FAIRsharing collection to EVORAO services.")
    parser.add_argument("--collection-id", choices=tuple(COLLECTIONS), default="5449")
    args = parser.parse_args()
    collection_id = args.collection_id
    input_json = Path(f"data/fairsharing/fairsharing_{collection_id}.json")
    output_jsonld = Path(f"data/fairsharing/fairsharing_{COLLECTIONS[collection_id]['slug']}_services.jsonld")
    if not input_json.exists():
        raise SystemExit(f"FAIRsharing raw JSON not found: {input_json}")
    raw = json.loads(input_json.read_text(encoding="utf-8"))
    output = convert_records(raw, collection_id)
    output_jsonld.parent.mkdir(parents=True, exist_ok=True)
    output_jsonld.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(output['@graph'])} {COLLECTIONS[collection_id]['provider_label']} FAIRsharing services -> {output_jsonld}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
