"""Strict release SHACL gate and per-record missing-metadata report."""
import csv
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import sys

from pyshacl import validate
from rdflib import Graph, Literal, RDF, RDFS, SH, URIRef
from rdflib.namespace import DCAT, DCTERMS, FOAF


RELEASE = {
    "evora_schema.shacl.ttl": "1382e3786f8936331dba8f725e46ca1773766fc3e933519075d552c1d545d82d",
    "evora_ontology.owl.ttl": "c6a7aec464784ad3a8e07e03c230687fa125d7bda9cd9bab9542aaddf7704c9f",
}


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def required_fields(graph, shapes, hierarchy):
    """Index the strict minimum cardinalities applicable to each explicit type."""
    by_type = {}
    for cls in set(graph.objects(None, RDF.type)):
        fields = Counter()
        for parent in hierarchy.transitive_objects(cls, RDFS.subClassOf):
            for shape in shapes.subjects(SH.targetClass, parent):
                for prop in shapes.objects(shape, SH.property):
                    minimum = shapes.value(prop, SH.minCount)
                    if minimum:
                        path = shapes.value(prop, SH.path)
                        if not isinstance(path, URIRef):
                            raise ValueError("Expected a simple path in the pinned release")
                        fields[path] = max(fields[path], int(minimum))
        by_type[cls] = fields
    return by_type


def missing_report(graph, sources, shapes, hierarchy, output):
    roots = {URIRef(record["@id"]) for record in sources}
    required = required_fields(graph, shapes, hierarchy)
    totals = Counter()
    groups = Counter()
    affected = Counter()
    with output.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["record_id", "source_publisher", "path_from_record", "focus_type", "focus_label", "missing_predicate"])
        for record in sources:
            root = URIRef(record["@id"])
            publisher = record.get("search:publisherName", "")
            record_groups = set()
            pending = [(root, [])]
            visited = set()
            while pending:
                node, path = pending.pop()
                if node in visited:
                    continue
                visited.add(node)
                fields = {}
                for cls in sorted(graph.objects(node, RDF.type)):
                    for pred, minimum in required[cls].items():
                        if pred not in fields or fields[pred][1] < minimum:
                            fields[pred] = (cls, minimum)
                for pred, (cls, minimum) in sorted(fields.items()):
                    if sum(1 for _ in graph.objects(node, pred)) < minimum:
                        label = graph.value(node, DCTERMS.title) or graph.value(node, FOAF.name) or ""
                        writer.writerow([root, publisher, " / ".join(path), cls, label, pred])
                        group = (publisher, str(cls), str(pred))
                        groups[group] += 1
                        record_groups.add(group)
                        totals[str(pred)] += 1
                for predicate, obj in graph.predicate_objects(node):
                    if (predicate != RDF.type and not isinstance(obj, Literal) and obj not in roots
                            and (obj, RDF.type, None) in graph):
                        pending.append((obj, path + [str(predicate)]))
            affected.update(record_groups)
    with output.with_name("curation-summary.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["source_publisher", "focus_type", "missing_predicate", "occurrences", "affected_records"])
        for group in sorted(groups):
            writer.writerow([*group, groups[group], affected[group]])
    return dict(totals.most_common())


def check(source, candidate, model_dir, report_dir):
    report_dir.mkdir(parents=True, exist_ok=True)
    for filename, checksum in RELEASE.items():
        if digest(model_dir / filename) != checksum:
            raise ValueError(f"Unexpected EVORAO 1.2.0 bytes for {filename}")
    shapes = Graph().parse(model_dir / "evora_schema.shacl.ttl")
    ontology = Graph().parse(model_dir / "evora_ontology.owl.ttl")
    hierarchy = Graph()
    for s, p, o in ontology.triples((None, RDFS.subClassOf, None)):
        if isinstance(s, URIRef) and isinstance(o, URIRef):
            hierarchy.add((s, p, o))

    sources = json.loads(source.read_bytes())["@graph"]
    with candidate.open("rb") as stream:
        graph = Graph().parse(file=stream, format="json-ld")
    ids = [URIRef(record["@id"]) for record in sources]
    catalogues = list(graph.subjects(RDF.type, DCAT.Catalog))
    if len(catalogues) != 1 or len(ids) <= 1 or len(set(ids)) != len(ids):
        raise ValueError("Expected one catalogue and unique source record IDs")
    if set(graph.objects(catalogues[0], DCAT.dataset)) != set(ids) - {catalogues[0]}:
        raise ValueError("Publication catalogue membership differs from source IDs")
    for record, subject in zip(sources, ids):
        types = record["@type"] if isinstance(record["@type"], list) else [record["@type"]]
        for cls in types:
            expected = graph.namespace_manager.expand_curie(cls)
            if (subject, RDF.type, expected) not in graph or (None, SH.targetClass, expected) not in shapes:
                raise ValueError(f"Resource type is missing or has no release shape: {subject}, {cls}")
    missing = missing_report(graph, sources, shapes, hierarchy, report_dir / "missing-metadata.csv")
    triples = len(graph)
    graph += hierarchy
    # A failing run stops at the first failing SHACL evaluation. The cardinality
    # inventory above covers all records independently of this fast failure path.
    conforms, results, _ = validate(graph, shacl_graph=shapes, inference="none",
                                    abort_on_first=True, inplace=True)
    if not isinstance(results, Graph):
        raise RuntimeError(str(results))
    with gzip.open(report_dir / "shacl-report.ttl.gz", "wb") as stream:
        results.serialize(destination=stream, format="turtle", encoding="utf-8")
    report = {"ontology": "EVORAO 1.2.0", "modelSha256": RELEASE, "sourceSha256": digest(source),
              "candidateSha256": digest(candidate), "resources": len(ids) - 1, "rdfTriples": triples,
              "conforms": bool(conforms), "abortOnFirst": True, "missingRowsByPredicate": missing}
    (report_dir / "validation.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"EVORAO 1.2.0 conforms={conforms}; {len(ids)-1} resources; {sum(missing.values())} missing-field rows")
    return bool(conforms)


if __name__ == "__main__":
    sys.exit(0 if check(*map(Path, sys.argv[1:])) else 1)
