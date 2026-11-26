"""Release-model integration tests used by the publication workflow."""
import copy
import csv
import json
import os
from pathlib import Path
import tempfile
import unittest

from scripts.build_publication_jsonld import publication_document
from scripts.validate_publication_jsonld import check


class PublicationTests(unittest.TestCase):
    def test_source_backed_mapping(self):
        record = {
            "@id": "urn:fixture:service", "@type": "EVORAO:Service",
            "EVORAO:category": {"@type": "EVORAO:ProductCategory", "dcterms:title": "Service"},
            "search:publisherName": "Example publisher", "dcat:keyword": ["One", "Two"],
            "EVORAO:keywords": [{"@type": "EVORAO:Keyword", "dcterms:title": "One", "skos:note": "Original"}],
            "EVORAO:collection": {"EVORAO:license": {"@id": "https://example.org/license",
                "@type": "EVORAO:License", "EVORAO:resourceUrl": "https://example.org/license",
                "EVORAO:licensingOrAttribution": "Source-specific credit"}},
        }
        portal = {"@context": {}, "@graph": [record]}
        original = copy.deepcopy(portal)
        candidate = publication_document(portal)
        doc = candidate["@graph"][0]
        self.assertEqual(original, portal)
        self.assertEqual(doc["dcat:theme"], record["EVORAO:category"])
        self.assertEqual(doc["dcterms:publisher"]["foaf:name"], "Example publisher")
        self.assertNotIn("search:publisherName", doc)
        self.assertEqual([n["dcterms:title"] for n in doc["EVORAO:keywords"]], ["One", "Two"])
        self.assertEqual(doc["EVORAO:keywords"][0]["skos:note"], "Original")
        statement = doc["EVORAO:collection"]["dcterms:license"]
        self.assertNotIn("@id", statement)
        self.assertEqual(statement["EVORAO:resourceUrl"], "https://example.org/license")
        self.assertEqual(statement["EVORAO:licensingOrAttribution"], "Source-specific credit")
        self.assertEqual(candidate["@context"]["EVORAO:accessPointUrl"], {"@type": "xsd:anyURI"})

    def test_strict_pass_and_curation_failure(self):
        models = Path(os.environ["EVORAO_MODEL_DIR"])
        catalogue = {"@id": "urn:test:catalogue", "@type": "dcat:Catalog", "dcterms:title": "Fixture",
                     "dcterms:description": "Test catalogue", "dcat:dataset": {"@id": "urn:test:keyword"}}
        resource = {"@id": "urn:test:keyword", "@type": "EVORAO:Keyword", "dcterms:title": "Example"}
        portal = {"@context": {"EVORAO": "https://w3id.org/evorao/", "dcat": "http://www.w3.org/ns/dcat#",
                               "dcterms": "http://purl.org/dc/terms/", "xsd": "http://www.w3.org/2001/XMLSchema#",
                               "foaf": "http://xmlns.com/foaf/0.1/"},
                  "@graph": [catalogue, resource]}
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            source, candidate = directory / "source.jsonld", directory / "candidate.jsonld"
            for valid in (True, False):
                if not valid:
                    del resource["dcterms:title"]
                source.write_text(json.dumps(portal))
                candidate.write_text(json.dumps(publication_document(portal)))
                self.assertEqual(check(source, candidate, models, directory), valid)
                with (directory / "missing-metadata.csv").open(newline="") as stream:
                    rows = list(csv.DictReader(stream))
                if valid:
                    self.assertEqual(rows, [])
                else:
                    self.assertEqual([(r["record_id"], r["missing_predicate"]) for r in rows],
                                     [("urn:test:keyword", "http://purl.org/dc/terms/title")])
