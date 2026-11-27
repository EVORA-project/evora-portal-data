"""Produce EVORAO 1.2.0 publication JSON-LD from the merged portal export."""
import argparse
from collections import Counter, defaultdict
from decimal import Decimal
import csv
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit


if __package__:
    from .ictv_parent_annotations import identified_taxa, parent_ids_from_cache
else:
    from ictv_parent_annotations import identified_taxa, parent_ids_from_cache


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


def normalize_eva_image(image):
    origin = "https://cdn.european-virus-archive.com"
    url = image.get("EVORAO:contentUrl")
    if not isinstance(url, str):
        return
    if url.startswith(origin + origin + "/"):
        url = url[len(origin):]
        image["EVORAO:contentUrl"] = url
    if not url.startswith(origin + "/"):
        return
    filename = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
    media_type = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png"}.get(
        Path(filename).suffix.lower())
    if media_type:
        image.setdefault("foaf:name", filename)
        image.setdefault("EVORAO:format", media_type)


def describe_ictv_taxonomy(taxonomy):
    title = taxonomy.get("dcterms:title", taxonomy.get("dct:title"))
    version = taxonomy.get("dcat:version")
    if title != "ICTV" or not isinstance(version, str) or not re.fullmatch(r"MSL[0-9]+(?:[.]v[0-9]+)?", version):
        return
    keys = ("dcterms:description", "dct:description")
    if any(str(taxonomy.get(key) or "").strip() for key in keys):
        return
    taxonomy.pop("dct:description", None)
    taxonomy["dcterms:description"] = (
        "Virus taxonomy published by the International Committee on Taxonomy of Viruses "
        f"in its Master Species List ({version})."
    )


def describe_metadata_provider(provider):
    # EVORAO 1.2.0 documents zero as the default ordering weight.
    provider.setdefault("EVORAO:weight", 0)
    title = provider.get("dcterms:title", provider.get("dct:title"))
    if title not in ("ERINHA", "European Virus Archive", "FAIRsharing", "ICTV via OLS4"):
        return
    provider.setdefault("EVORAO:queryMethod", "GET")
    # FAIRsharing and OLS request JSON. ERINHA reads a Sheets API ValueRange.
    # The EVA adapter parses JSON-LD without recording the HTTP media type.
    if title != "European Virus Archive":
        provider.setdefault("EVORAO:contentType", "application/json")
    if title == "ERINHA":
        provider.setdefault("EVORAO:providedEntityType", "https://w3id.org/evorao/Service")
        provider.setdefault("dcat:endpointURL", "https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}/values/{range}")
    elif title == "ICTV via OLS4":
        # The taxon entity response carries its MSL annotation. This template
        # describes the entity lookup used by the OLS client.
        provider.setdefault("dcat:endpointURL", "https://www.ebi.ac.uk/ols4/api/v2/ontologies/ictv/entities/{encodedIri}")
        provider.setdefault("EVORAO:providedEntityType", "https://w3id.org/evorao/Taxon")


def describe_fairsharing_collection_endpoint(collection):
    identifier = collection.get("@id")
    if not isinstance(identifier, str):
        return
    match = re.fullmatch(r"https://fairsharing[.]org/([0-9]+)/?", identifier)
    if not match:
        return
    for provider in as_list(collection.get("EVORAO:collectionDataProvider")):
        if (isinstance(provider, dict) and "EVORAO:DataProvider" in as_list(provider.get("@type"))
                and provider.get("dcterms:title") == "FAIRsharing"):
            provider.setdefault("dcat:endpointURL", "https://api.fairsharing.org/fairsharing_records/" + match[1])
            provider.setdefault("EVORAO:providedEntityType", "https://w3id.org/evorao/Collection")


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

    if result.get("@type") == "EVORAO:Image":
        normalize_eva_image(result)
    elif "EVORAO:Taxonomy" in as_list(result.get("@type")):
        describe_ictv_taxonomy(result)
    elif "EVORAO:DataProvider" in as_list(result.get("@type")):
        describe_metadata_provider(result)
    elif "EVORAO:Collection" in as_list(result.get("@type")):
        describe_fairsharing_collection_endpoint(result)

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


def supplied_classes(graph):
    classes = defaultdict(set)
    for record in graph:
        for identification, _, key in identified_taxa(record):
            value = identification.get("EVORAO:pathogenType")
            if isinstance(value, str) and value.strip():
                classes[key].add(value)
    allowed = {"Virus", "Bacterium", "Fungus", "Protozoan", "Viroid", "Prion"}
    return {key: next(iter(values)) for key, values in classes.items() if len(values) == 1 and values <= allowed}


def reconcile_record(record, parent_ids, classes, changes):
    for identification, taxon, key in identified_taxa(record):
        if "EVORAO:pathogenType" not in identification and key in classes:
            identification["EVORAO:pathogenType"] = classes[key]
            changes["classFromSameIdentifiedTaxon"] += 1
        parent = taxon.get("EVORAO:parentTaxon")
        if (isinstance(parent, dict) and parent.get("@type") == "EVORAO:Taxon"
                and set(parent) <= {"@type", "dcterms:title"}):
            identifier = parent_ids.get((*key, parent.get("dcterms:title")))
            if identifier:
                parent["EVORAO:taxonomicId"] = identifier
                changes["parentIdFromCache"] += 1

    # These classes permit the properties on NaturalPartOrigin only. Keep
    # competing or ambiguous assertions for curation instead of choosing one.
    if record.get("@type") not in ("EVORAO:Virus", "EVORAO:NucleicAcid"):
        return
    parts = [part for origin in as_list(record.get("EVORAO:biologicalMaterialOrigin"))
             if isinstance(origin, dict) for part in as_list(origin.get("EVORAO:biologicalPartOrigin"))]
    if not all(isinstance(part, dict) and part.get("@type") in ("EVORAO:NaturalPartOrigin", "EVORAO:SyntheticPartOrigin")
               for part in parts):
        return
    natural = [part for part in parts if part["@type"] == "EVORAO:NaturalPartOrigin"]
    if len(natural) != 1:
        return
    for field in ("EVORAO:collectionDate", "EVORAO:countryOfCollection"):
        outer = record.get(field)
        inner = natural[0].get(field)
        if field == "EVORAO:countryOfCollection" and isinstance(inner, dict):
            inner = inner.get("dcterms:title")
        if isinstance(outer, str) and outer.strip() and outer == inner:
            del record[field]
            changes["duplicateOriginFieldRemoved:" + field] += 1


def normalize_source_fields(record, source, changes):
    if source == "FAIRsharing":
        identification = record.get("EVORAO:pathogenIdentification")
        if isinstance(identification, dict) and "EVORAO:pathogenType" not in identification:
            taxon = identification.get("EVORAO:taxon")
            if (isinstance(taxon, dict) and taxon.get("EVORAO:taxonomicId") == "NCBITaxon:10239"
                    and taxon.get("dcterms:title") == "Viruses"):
                identification["EVORAO:pathogenType"] = "Virus"
                changes["classFromDeclaredVirusesTaxon"] += 1
    if source == "European Virus Archive":
        amount = record.get("EVORAO:unitCost")
        if type(amount) in (int, float) and Decimal(str(amount)).is_finite():
            record["EVORAO:unitCost"] = {"@type": "xsd:decimal", "@value": format(Decimal(str(amount)), "f")}
            changes["costTypedAsDecimal"] += 1
        doi = record.get("EVORAO:doi")
        if isinstance(doi, str):
            # A comma or semicolon can occur inside a DOI. Split only when the
            # separator is followed by whitespace and another DOI.
            prefix = r"(?:(?:doi:\s*|doi\s+))?(?:(?:https?://)?(?:dx[.])?doi[.]org/)?"
            pieces = re.split(r"[,;]\s+(?=" + prefix + r"10[.][0-9]{4,9}/)", doi.strip(), flags=re.I)
            values = [re.sub("^" + prefix, "", part, flags=re.I) for part in pieces]
            if all(re.fullmatch(r"10[.][0-9]{4,9}/[^\s]+", v)
                   and len(re.findall(r"10[.][0-9]{4,9}/", v)) == 1 for v in values):
                nodes = [{"@type": "EVORAO:Doi", "dcterms:title": v} for v in values]
                record["EVORAO:doi"] = nodes[0] if len(nodes) == 1 else nodes
                changes["doiTypedAsTerm"] += 1


def publication_document(portal, generated=None, cache=None, changes=None):
    parent_ids = parent_ids_from_cache(cache or {})
    classes = supplied_classes(portal["@graph"])
    if changes is None:
        changes = Counter()
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
            normalize_source_fields(record, source.get("search:publisherName"), changes)
            reconcile_record(record, parent_ids, classes, changes)
            describe_collections(record, source, members, generated)
            add_keyword_nodes(record)
            # Legacy records sometimes carry the publisher name only in the projection.
            publisher = source.get("search:publisherName")
            if publisher and "dcterms:publisher" not in record:
                record["dcterms:publisher"] = {"@type": "foaf:Organization", "foaf:name": publisher}
        graph.append(record)
    return {"@context": context, "@graph": graph}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ("source", "destination", "evidence"):
        parser.add_argument(arg, type=Path)
    parser.add_argument("--ictv-cache", type=Path)
    args = parser.parse_args()
    source_path, destination, evidence = args.source, args.destination, args.evidence
    cache_bytes = args.ictv_cache.read_bytes() if args.ictv_cache else None
    changes = Counter()
    generated = Counter()
    source_bytes = source_path.read_bytes()
    result = publication_document(json.loads(source_bytes), generated,
                                  json.loads(cache_bytes) if cache_bytes is not None else None, changes)
    destination.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    evidence.parent.mkdir(parents=True, exist_ok=True)
    audit = {"sourceSha256": hashlib.sha256(source_bytes).hexdigest(),
             "cacheSha256": hashlib.sha256(cache_bytes).hexdigest() if cache_bytes is not None else None,
             "changes": dict(changes)}
    evidence.with_name("source-reconciliation.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    with evidence.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["collection_id", "collection_title", "source_publisher", "snapshot_records",
                         "generated_description", "embeddings_filled"])
        for key in sorted(generated):
            writer.writerow([*key, generated[key]])
