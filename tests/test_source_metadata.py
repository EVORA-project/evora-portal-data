import copy
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


def load_module(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


eva = load_module("eva_merge", "scripts/eva-data/merge_eva_jsonld.py")
fairsharing = load_module("fairsharing_converter", "scripts/fairsharing-data/fairsharing_to_evorao.py")


def sample_collection():
    return {
        "collection": {"data": {"attributes": {"linked_records": [
            {"linked_record_id": 42, "linked_record_name": "Example resource", "linked_record_type": "knowledgebase"},
            {"linked_record_name": "Invalid missing ID"},
        ]}}},
        "records": {"42": {"data": {"id": "42", "attributes": {"metadata": {
            "name": "Example virology resource", "description": "Resource description", "homepage": "https://example.org/resource",
        }}}}},
    }


class EvaMetadataLicenseTests(unittest.TestCase):
    def test_adds_metadata_licence_without_changing_collection_or_material_provider(self):
        entity = {"@id": "urn:evorao:EVA:IP:example", "EVORAO:provider": {"foaf:name": "IP"}, "EVORAO:collection": {"dcterms:title": "IP"}}
        eva.add_eva_metadata_license(entity)
        self.assertEqual(entity["EVORAO:provider"], {"foaf:name": "IP"})
        self.assertEqual(entity["EVORAO:collection"]["dcterms:title"], "IP")
        provider = entity["EVORAO:collection"]["EVORAO:collectionDataProvider"]
        self.assertEqual(provider["dcterms:title"], "European Virus Archive")
        self.assertEqual(provider["EVORAO:license"]["@id"], "https://creativecommons.org/licenses/by/4.0/")
        self.assertIn("Public catalogue data and metadata", provider["EVORAO:license"]["EVORAO:licensingOrAttribution"])
        self.assertNotIn("EVORAO:license", entity)
        self.assertEqual(provider["EVORAO:license"]["EVORAO:logo"]["@type"], "EVORAO:Image")
        self.assertIn("LOGO_EVA.svg", provider["EVORAO:license"]["EVORAO:logo"]["EVORAO:contentUrl"])

    def test_preserves_specific_source_licence(self):
        specific = {"@id": "https://example.org/specific-licence"}
        entity = {"EVORAO:collection": {"dcterms:title": "IP", "EVORAO:collectionDataProvider": {"dcterms:title": "Original source", "EVORAO:license": specific}}}
        eva.add_eva_metadata_license(entity)
        provider = entity["EVORAO:collection"]["EVORAO:collectionDataProvider"]
        self.assertEqual(provider["dcterms:title"], "Original source")
        self.assertIs(provider["EVORAO:license"], specific)

    def test_specific_collection_or_record_notice_takes_precedence_over_default(self):
        for notice_at in ("collection", "record"):
            with self.subTest(notice_at=notice_at):
                entity = {"EVORAO:collection": {"dcterms:title": "IP"}}
                target = entity["EVORAO:collection"] if notice_at == "collection" else entity
                target["dcterms:license"] = {"@id": "https://example.org/exception"}
                eva.add_eva_metadata_license(entity)
                licence = entity["EVORAO:collection"]["EVORAO:collectionDataProvider"]["EVORAO:license"]
                self.assertEqual(licence["@id"], "https://example.org/exception")

    def test_multiple_collections_and_repeated_processing_are_stable(self):
        entity = {"EVORAO:collection": [{"dcterms:title": "IP"}, {"dcterms:title": "MUG"}]}
        eva.add_eva_metadata_license(entity)
        expected = copy.deepcopy(entity)
        eva.add_eva_metadata_license(entity)
        self.assertEqual(entity, expected)
        first, second = entity["EVORAO:collection"]
        first["EVORAO:collectionDataProvider"]["EVORAO:license"]["dcterms:title"] = "Changed"
        self.assertEqual(second["EVORAO:collectionDataProvider"]["EVORAO:license"]["dcterms:title"], "CC-BY 4.0")


class FairsharingCollectionTests(unittest.TestCase):
    def test_existing_elixir_identifiers_and_provider_are_preserved(self):
        graph = fairsharing.convert_records(sample_collection(), "5449")["@graph"]
        self.assertEqual(len(graph), 1)
        entity = graph[0]
        self.assertEqual(entity["@id"], "urn:evorao:FAIRSHARING:Service:42")
        self.assertEqual(entity["search:providerName"], "ELIXIR")
        self.assertEqual(entity["search:publisherName"], "FAIRsharing")
        self.assertEqual(entity["EVORAO:provider"]["foaf:name"], "ELIXIR Europe")

    def test_evora_provider_and_publisher_keep_fairsharing_metadata_licence_and_credit(self):
        entity = fairsharing.convert_records(sample_collection(), "7589")["@graph"][0]
        self.assertEqual(entity["search:providerName"], "EVORA")
        self.assertEqual(entity["search:publisherName"], "EVORA")
        self.assertEqual(entity["EVORAO:provider"]["foaf:name"], "European Viral Outbreak Response Alliance (EVORA)")
        self.assertEqual(entity["EVORAO:publisher"]["EVORAO:alternateName"], "EVORA")
        self.assertEqual(entity["EVORAO:publisher"]["EVORAO:logo"]["@type"], "EVORAO:Image")
        self.assertIn("images/logo/logo.svg", entity["EVORAO:publisher"]["EVORAO:logo"]["EVORAO:contentUrl"])
        collection = entity["EVORAO:collection"]
        self.assertEqual(collection["@id"], "https://fairsharing.org/7589/")
        source = collection["EVORAO:collectionDataProvider"]
        self.assertEqual(source["dcterms:title"], "FAIRsharing")
        self.assertEqual(source["EVORAO:license"]["@id"], "https://creativecommons.org/licenses/by-sa/4.0/")
        self.assertIn("fairsharing-attribution.svg", source["EVORAO:license"]["EVORAO:licensingOrAttribution"])
        self.assertEqual(entity["dcterms:source"]["@id"], "https://fairsharing.org/42/")

    def test_logo_images_have_inherited_file_fields(self):
        entity = fairsharing.convert_records(sample_collection(), "7589")["@graph"][0]
        logos = [
            entity["EVORAO:provider"]["EVORAO:logo"],
            entity["EVORAO:publisher"]["EVORAO:logo"],
            entity["EVORAO:collection"]["EVORAO:collectionDataProvider"]["EVORAO:license"]["EVORAO:logo"],
            eva.EVA_METADATA_LICENSE["EVORAO:logo"],
        ]
        for logo in logos:
            with self.subTest(url=logo.get("@id")):
                self.assertEqual(logo["@type"], "EVORAO:Image")
                self.assertTrue(logo["foaf:name"])
                self.assertTrue(logo["EVORAO:contentUrl"].startswith("https://"))
                self.assertEqual(logo["EVORAO:format"], "image/svg+xml")
                self.assertNotIn("EVORAO:resourceUrl", logo)

    def test_same_fairsharing_record_in_two_collections_cannot_overwrite_the_other(self):
        raw = sample_collection()
        original = copy.deepcopy(raw)
        elixir = fairsharing.convert_records(raw, "5449")["@graph"]
        evora = fairsharing.convert_records(raw, "7589")["@graph"]
        merged = {entity["@id"]: entity for entity in elixir + evora}
        self.assertEqual(len(merged), 2)
        self.assertEqual(raw, original)
        self.assertEqual(evora[0]["EVORAO:accessPointUrl"], "https://example.org/resource")


if __name__ == "__main__":
    unittest.main()
