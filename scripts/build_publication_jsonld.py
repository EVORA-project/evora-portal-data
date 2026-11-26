"""Produce EVORAO 1.2.0 publication JSON-LD from the merged portal export."""
from collections import Counter, defaultdict
import csv
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


def collection_key(record, collection):
    title = collection.get("dcterms:title", collection.get("dct:title"))
    if not isinstance(title, str) or not title.strip():
        return None
    publisher = record.get("search:publisherName") or ""
    return collection.get("@id", ""), title.strip(), publisher


def collection_members(graph):
    members = defaultdict(set)
    for record in graph:
        for collection in as_list(record.get("EVORAO:collection")):
            if isinstance(collection, dict):
                key = collection_key(record, collection)
                if key:
                    members[key].add(record["@id"])
    return members


def describe_collections(record, source, members, generated):
    for collection in as_list(record.get("EVORAO:collection")):
        if not isinstance(collection, dict):
            continue
        if any(str(collection.get(key) or "").strip() for key in ("dcterms:description", "dct:description")):
            continue
        key = collection_key(source, collection)
        if not key:
            continue
        identifier, title, publisher = key
        count = len(members[key])
        noun = "catalogue record" if count == 1 else "catalogue records"
        verb = "carries" if count == 1 else "carry"
        origin = f" from {publisher}" if publisher else ""
        text = f"In this EVORA snapshot, {count} {noun}{origin} {verb} the collection label “{title}”."
        collection["dcterms:description"] = text
        generated[(identifier, title, publisher, count, text)] += 1


def publication_document(portal, generated=None):
    if generated is None:
        generated = Counter()
    members = collection_members(portal["@graph"])
    context = dict(portal["@context"])
    for key in URI_LITERALS:
        context[key] = {"@type": "xsd:anyURI"}
    graph = []
    for source in portal["@graph"]:
        record = convert(source)
        if source["@type"] != "dcat:Catalog":
            describe_collections(record, source, members, generated)
            add_keyword_nodes(record)
            # Legacy records sometimes carry the publisher name only in the projection.
            publisher = source.get("search:publisherName")
            if publisher and "dcterms:publisher" not in record:
                record["dcterms:publisher"] = {"@type": "foaf:Organization", "foaf:name": publisher}
        graph.append(record)
    return {"@context": context, "@graph": graph}


if __name__ == "__main__":
    source_path, destination, evidence = map(Path, sys.argv[1:])
    generated = Counter()
    result = publication_document(json.loads(source_path.read_bytes()), generated)
    destination.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    evidence.parent.mkdir(parents=True, exist_ok=True)
    with evidence.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["collection_id", "collection_title", "source_publisher", "snapshot_records",
                         "generated_description", "embeddings_filled"])
        for key in sorted(generated):
            writer.writerow([*key, generated[key]])
