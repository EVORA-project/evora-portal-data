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
from scripts.ictv_parent_annotations import index_terms, missing_parents, parent_ids_from_cache, update_parent_terms


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

    def test_data_provider_metadata_uses_adapter_evidence_and_preserves_overrides(self):
        def provider(title, **fields):
            return {"@type": "EVORAO:DataProvider", "dcterms:title": title, **fields}
        def collection(identifier, data):
            return {"@type": "EVORAO:Collection", "@id": identifier, "EVORAO:collectionDataProvider": data}
        nodes = [collection("https://fairsharing.org/7589/", provider("FAIRsharing")),
                 collection("https://example.org/7589", provider("FAIRsharing")),
                 provider("ERINHA"), provider("European Virus Archive"), provider("Other"),
                 provider("ICTV via OLS4", **{"EVORAO:weight": 1, "EVORAO:queryMethod": "POST",
                                             "EVORAO:contentType": "application/example"})]
        source = {"@context": {}, "@graph": [{"@id": "urn:example", "@type": "EVORAO:Service",
                  "example:nodes": nodes}]}
        mapped = publication_document(source)["@graph"][0]["example:nodes"]
        fair = mapped[0]["EVORAO:collectionDataProvider"]
        self.assertEqual(fair["dcat:endpointURL"], "https://api.fairsharing.org/fairsharing_records/7589")
        self.assertEqual(fair["EVORAO:providedEntityType"], "https://w3id.org/evorao/Collection")
        self.assertNotIn("dcat:endpointURL", mapped[1]["EVORAO:collectionDataProvider"])
        self.assertEqual(mapped[2]["EVORAO:providedEntityType"], "https://w3id.org/evorao/Service")
        self.assertEqual(mapped[2]["EVORAO:contentType"], "application/json")
        self.assertEqual(mapped[3]["EVORAO:queryMethod"], "GET")
        self.assertNotIn("EVORAO:contentType", mapped[3])
        self.assertEqual(mapped[4], {**nodes[4], "EVORAO:weight": 0})
        self.assertEqual(mapped[5], nodes[5])

    def test_reconcile_existing_taxon_evidence(self):
        child = {"msl": "MSL41", "iri": "urn:child", "ictv_id": "C", "label": "Child",
                 "direct_parent_iri": "urn:parent:123", "direct_parent_label": "Parent"}
        parent = {"msl": "MSL41", "iri": "urn:parent:123", "ictv_id": "P", "label": "Parent"}
        cache = {"one": {"current": child}, "two": {"current": parent}}
        def record(i, version="MSL41", **supplied):
            taxon = {"@type": "EVORAO:Taxon", "dcterms:title": "Child", "EVORAO:taxonomicId": "C",
                     "EVORAO:taxonomy": {"dcterms:title": "ICTV", "dcat:version": version},
                     "EVORAO:parentTaxon": {"@type": "EVORAO:Taxon", "dcterms:title": "Parent"}}
            return {"@id": str(i), "@type": "EVORAO:Service",
                    "EVORAO:pathogenIdentification": {"EVORAO:taxon": taxon, **supplied}}
        graph = [record(1, **{"EVORAO:pathogenType": "Virus"}), record(2), record(3, "MSL36")]
        source = {"@context": {}, "@graph": graph}
        def inspect(i):
            rec = publication_document(source, cache=cache)["@graph"][i]["EVORAO:pathogenIdentification"]
            return rec.get("EVORAO:pathogenType"), rec["EVORAO:taxon"]["EVORAO:parentTaxon"].get("EVORAO:taxonomicId")
        self.assertEqual(inspect(1), ("Virus", "P"))
        self.assertEqual(inspect(2), (None, None))
        cache["conflict"] = {"obsolete": {**parent, "ictv_id": "different"}}
        graph.append(record(4, **{"EVORAO:pathogenType": "Viroid"}))
        self.assertEqual(inspect(1), (None, None))
        self.assertEqual(inspect(0), ("Virus", None))

    def test_live_parent_cache_requires_explicit_matching_annotation(self):
        child = {"msl": "MSL41", "iri": "urn:child", "ictv_id": "C", "label": "Child",
                 "direct_parent_iri": "urn:parent:123", "direct_parent_label": "Parent"}
        cache = {"one": {"current": child}}
        parent = {"@type": "EVORAO:Taxon", "dcterms:title": "Parent"}
        taxon = {"dcterms:title": "Child", "EVORAO:taxonomicId": "C", "EVORAO:parentTaxon": parent,
                 "EVORAO:taxonomy": {"dcterms:title": "ICTV", "dcat:version": "MSL41"}}
        record = {"EVORAO:pathogenIdentification": {"EVORAO:taxon": taxon}}
        needed = missing_parents([record, record], *index_terms(cache))
        self.assertEqual(needed, {("MSL41", "urn:parent:123", "Parent")})
        ver, ident = "http://www.w3.org/2002/07/owl#versionInfo", "http://purl.org/dc/terms/identifier"
        answer = {"iri": "urn:parent:123", ver: "MSL41", "label": ["Parent"], ident: "P"}
        def outage(iri, timeout):
            raise TimeoutError()
        self.assertEqual(update_parent_terms(cache, needed, outage), 0)
        for bad in ({ver: "MSL36"}, {"iri": "urn:other"}, {"label": "Other"}, {ident: None}):
            self.assertEqual(update_parent_terms(cache, needed, lambda iri, timeout: {**answer, **bad}), 0)
        self.assertEqual(update_parent_terms(cache, needed, lambda iri, timeout: answer), 1)
        self.assertFalse(missing_parents([record], *index_terms(cache)))
        self.assertEqual(parent_ids_from_cache(cache)[("MSL41", "C", "Child", "Parent")], "P")

    def test_reconcile_duplicate_origin_fields(self):
        field = "EVORAO:collectionDate"
        country = "EVORAO:countryOfCollection"
        part = {"@type": "EVORAO:NaturalPartOrigin", field: "2001-01-01", country: {"dcterms:title": "Example"}}
        def record(parts, date):
            return {"@type": "EVORAO:NucleicAcid", "@id": "urn:test", field: date, country: "Example",
                    "EVORAO:biologicalMaterialOrigin": {"EVORAO:biologicalPartOrigin": parts}}
        sources = [record([part, {"@type": "EVORAO:SyntheticPartOrigin"}], "2001-01-01"),
                   record(part, "2002-02-02"), record([part, part], "2001-01-01")]
        output = publication_document({"@context": {}, "@graph": sources})["@graph"]
        self.assertNotIn(field, output[0])
        self.assertNotIn(country, output[0])
        self.assertEqual(output[0]["EVORAO:biologicalMaterialOrigin"], sources[0]["EVORAO:biologicalMaterialOrigin"])
        self.assertEqual(output[1][field], "2002-02-02")
        self.assertEqual(output[2], sources[2])

    def test_ictv_taxonomy_descriptions_use_the_taxonomy_release(self):
        taxonomy = {"@type": "EVORAO:Taxonomy", "dcterms:title": "ICTV", "dcat:version": "MSL41"}
        variants = [taxonomy, {**taxonomy, "dcat:version": "MSL36", "dct:description": " "},
                    {**taxonomy, "dct:description": {"@value": "Existing text", "@language": "en"}},
                    {**taxonomy, "dcterms:title": "Other"}, {**taxonomy, "dcat:version": None},
                    {**taxonomy, "dcat:version": "unknown"}]
        parent = {"@type": "EVORAO:Taxon", "EVORAO:taxonomicId": "example",
                  "EVORAO:taxonomy": variants[0], "EVORAO:rank": {"EVORAO:taxonomy": variants[1]}}
        source = {"@context": {}, "@graph": [{"@id": "urn:test", "@type": "EVORAO:Service",
                  "example:nodes": [parent, *variants[2:]]}]}
        original = copy.deepcopy(source)
        result = publication_document(source)["@graph"][0]["example:nodes"]
        text = "Virus taxonomy published by the International Committee on Taxonomy of Viruses in its Master Species List"
        self.assertEqual(result[0]["EVORAO:taxonomy"]["dcterms:description"], text + " (MSL41).")
        rank_taxonomy = result[0]["EVORAO:rank"]["EVORAO:taxonomy"]
        self.assertEqual(rank_taxonomy["dcterms:description"], text + " (MSL36).")
        self.assertNotIn("dct:description", rank_taxonomy)
        self.assertNotIn("dcterms:description", result[0])
        self.assertEqual(result[1:], variants[2:])
        self.assertEqual(source, original)

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
