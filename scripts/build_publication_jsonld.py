"""Produce EVORAO 1.2.0 publication JSON-LD from the merged portal export."""
import hashlib
import json
from pathlib import Path
import sys


PREDICATES = {
    "EVORAO:category": "dcat:theme",
    "EVORAO:license": "dcterms:license",
    "EVORAO:homepage": "foaf:homepage",
    "EVORAO:publisher": "dcterms:publisher",
    "EVORAO:note": "skos:note",
}

# These paths have a single xsd:anyURI datatype in all released shapes.
URI_LITERALS = (
    "EVORAO:iri", "dcat:endpointURL", "foaf:homepage", "EVORAO:accessPointUrl",
    "EVORAO:contentUrl", "EVORAO:loginUrl", "EVORAO:providedEntityType", "EVORAO:resourceUrl",
)


def as_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def convert(value):
    if isinstance(value, list):
        return [convert(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        if key.startswith("search:"):
            continue
        key = PREDICATES.get(key, key)
        if key in URI_LITERALS and isinstance(item, dict) and set(item) == {"@id"}:
            item = item["@id"]
        mapped = convert(item)
        if key in result and result[key] != mapped:
            raise ValueError(f"Conflicting source values for {key} on {value.get('@id', 'embedded node')}")
        result[key] = mapped

    # The legal document is shared. The attribution and logo describe the
    # individual source's licence statement, so give that statement local scope.
    if (result.get("@type") == "EVORAO:License"
            and result.get("EVORAO:licensingOrAttribution")
            and result.get("@id") == result.get("EVORAO:resourceUrl")):
        result.pop("@id", None)
    return result


def add_keyword_nodes(record):
    nodes = list(as_list(record.get("EVORAO:keywords")))
    labels = {item.get("dcterms:title") for item in nodes if isinstance(item, dict)
              and isinstance(item.get("dcterms:title"), str)}
    for label in as_list(record.get("dcat:keyword")):
        if not isinstance(label, str) or not label.strip() or label in labels:
            continue
        # Repeated plain keyword labels share one anonymous node in this graph.
        digest = hashlib.sha256(label.encode("utf-8")).hexdigest()
        nodes.append({"@id": "_:publicationKeyword" + digest, "@type": "EVORAO:Keyword", "dcterms:title": label})
        labels.add(label)
    if nodes:
        record["EVORAO:keywords"] = nodes


def publication_document(portal):
    context = dict(portal["@context"])
    for key in URI_LITERALS:
        context[key] = {"@type": "xsd:anyURI"}
    graph = []
    for source in portal["@graph"]:
        record = convert(source)
        if source["@type"] != "dcat:Catalog":
            add_keyword_nodes(record)
            # Legacy records sometimes carry the publisher name only in the projection.
            publisher = source.get("search:publisherName")
            if publisher and "dcterms:publisher" not in record:
                record["dcterms:publisher"] = {"@type": "foaf:Organization", "foaf:name": publisher}
        graph.append(record)
    return {"@context": context, "@graph": graph}


if __name__ == "__main__":
    source_path, destination = map(Path, sys.argv[1:])
    result = publication_document(json.loads(source_path.read_bytes()))
    destination.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
