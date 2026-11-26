import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

path = Path(__file__).resolve().parents[1] / "scripts/ictv-enrich/enrich_with_ictv.py"
spec = importlib.util.spec_from_file_location("ictv_enrichment", path)
enrichment = importlib.util.module_from_spec(spec)
spec.loader.exec_module(enrichment)


class ICTVCacheTests(unittest.TestCase):
    def test_offline_coverage_does_not_resolve_or_rewrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            source = directory / "input.jsonld"
            source.write_text(json.dumps({"@graph": [{"EVORAO:pathogenIdentification": {
                "EVORAO:taxon": {"dcterms:title": "Fixture label"}}}]}))
            cache = directory / "cache.json"
            cache.write_text('{"_ols_updated_at":"known","Fixture label":{}}')
            original = cache.read_bytes()
            with patch.object(enrichment, "resolve_all_labels", side_effect=AssertionError("unexpected lookup")):
                enrichment.enrich_file(source, directory / "out", cache, None, cache_only=True)
                self.assertEqual(cache.read_bytes(), original)
                cache.write_text('{"_ols_updated_at":"known"}')
                with self.assertRaisesRegex(ValueError, "Fixture label"):
                    enrichment.enrich_file(source, directory / "missing", cache, None, cache_only=True)
                self.assertFalse((directory / "missing").exists())

    def test_live_build_reuses_hits_and_retries_missing_results(self):
        cached = {"hit": {"ok": True}, "failed": None}
        with patch.object(enrichment, "resolve_label_once", return_value={"ok": True}) as resolve:
            enrichment.resolve_all_labels({"hit", "new", "failed"}, cached, max_workers=1)
        self.assertEqual({call.args[0] for call in resolve.call_args_list}, {"new", "failed"})

    def test_unavailable_release_stamp_preserves_known_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cache.json"
            enrichment.save_cache({"_ols_updated_at": "known"}, path)
            self.assertEqual(json.loads(path.read_text())["_ols_updated_at"], "known")
