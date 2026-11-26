"""Release-model integration tests used by the publication workflow."""
from collections import Counter
import copy
import csv
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from scripts.build_publication_jsonld import publication_document
from scripts.validate_publication_jsonld import check
from scripts.stage_publication_site import stage_site


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

    def test_eva_image_metadata_from_source_filename(self):
        origin = "https://cdn.european-virus-archive.com"
        key = "EVORAO:contentUrl"
        images = [
            {"@type": "EVORAO:Image", key: origin + origin + "/files/Example%20one.JPG"},
            {"@type": "EVORAO:Image", key: origin + "/two.png", "foaf:name": "Credit",
             "EVORAO:format": "image/custom"},
            {"@type": "EVORAO:Image", key: origin + "/image?format=png"},
            {"@type": "EVORAO:Image", key: "https://example.org/third.jpg"},
        ]
        source = {"@context": {}, "@graph": [{"@id": "urn:fixture", "@type": "EVORAO:Service",
                  "EVORAO:productPicture": images}]}
        result = publication_document(source)["@graph"][0]["EVORAO:productPicture"]
        self.assertEqual(result[0], {"@type": "EVORAO:Image", key: origin + "/files/Example%20one.JPG",
                                    "foaf:name": "Example one.JPG", "EVORAO:format": "image/jpeg"})
        self.assertEqual(images[0][key], origin + origin + "/files/Example%20one.JPG")
        self.assertEqual(result[1:], images[1:])

    def test_site_requires_matching_verified_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            full = b'{"@graph":[]}'
            digest = hashlib.sha256(full).hexdigest()
            snapshot = root / "snapshots" / digest
            snapshot.mkdir(parents=True)
            (snapshot / "catalogue.jsonld").write_bytes(full)
            (snapshot / "index.json").write_bytes(b'{}')
            legacy = b'{ "@graph": [] }'
            source = root / "source.jsonld"
            source.write_bytes(legacy)
            source_digest = hashlib.sha256(legacy).hexdigest()
            validation = {"conforms": False, "candidateSha256": digest, "sourceSha256": source_digest}
            parity = {"snapshot": {"id": "sha256:" + digest, "resourceCount": 0},
                      "projectionSourceSha256": source_digest, "indexSha256": hashlib.sha256(b'{}').hexdigest()}
            (root / "compact-parity.json").write_text(json.dumps(parity))
            def set_validation():
                (root / "validation.json").write_text(json.dumps(validation))
            set_validation()
            with self.assertRaisesRegex(ValueError, "conformance"):
                stage_site(root, root, source)
            self.assertFalse((root / "data/portal/latest.json").exists())
            validation["conforms"] = True
            set_validation()
            source.write_bytes(b'{}')
            with self.assertRaisesRegex(ValueError, "companion bytes"):
                stage_site(root, root, source)
            self.assertFalse((root / "data/portal/latest.json").exists())
            source.write_bytes(legacy)
            stage_site(root, root, source)
            self.assertEqual((root / "data/portal/evora_portal_all.jsonld").read_bytes(), full)
            manifest = json.loads((root / "data/portal/latest.json").read_bytes())
            self.assertEqual(manifest["snapshotId"], "sha256:" + digest)
            self.assertEqual(manifest["legacySha256"], source_digest)
            legacy_relative = f"snapshots/{source_digest}/catalogue.jsonld"
            self.assertEqual(manifest["legacyExportUrl"], "https://data.evora-project.eu/" + legacy_relative)
            self.assertEqual((root / legacy_relative).read_bytes(), legacy)
            (snapshot / "index.json").write_bytes(b'{"changed":true}')
            with self.assertRaisesRegex(ValueError, "bytes"):
                stage_site(root, root, source)

    def test_generated_collection_descriptions(self):
        def record(identifier, title="Example", publisher="Partner", **extra):
            return {"@id": identifier, "@type": "EVORAO:Service", "search:publisherName": publisher,
                    "EVORAO:collection": {"dcterms:title": title, **extra}}
        sources = [record("urn:1"), record("urn:2", **{"dcterms:description": "Partner-supplied text"}),
                   record("urn:3", publisher="Second partner"), record("urn:4", title="")]
        generated = Counter()
        docs = publication_document({"@context": {}, "@graph": sources}, generated)["@graph"]
        descriptions = [n["EVORAO:collection"].get("dcterms:description") for n in docs]
        self.assertEqual(descriptions, [
            "In this EVORA snapshot, 2 catalogue records from Partner carry the collection label “Example”.",
            "Partner-supplied text",
            "In this EVORA snapshot, 1 catalogue record from Second partner carries the collection label “Example”.",
            None,
        ])
        self.assertEqual(sum(generated.values()), 2)

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
