"""Exercise production cache decisions with synthetic inputs and a local OLS stub."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts/ictv-enrich"))
import run_catalogue_enrichment as workflow
sys.path.pop(0)


STAMP = "2026-01-01T00:00:00Z"
HELPER = b"""
from pathlib import Path
class ICTVOLSClient:
    def resolveToLatest(self, label):
        with Path(__file__).with_name("calls").open("a") as stream:
            print(label, file=stream)
        return {"status": "not_found"}
"""


class WorkflowCacheTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.scratch = self.root / "scratch"
        self.scratch.mkdir()
        self.cache = self.root / "cache.json"
        self.cache.write_text(json.dumps({"_ols_updated_at": STAMP, "known": {"status": "not_found"}}))
        self.original = self.cache.read_bytes()
        self.partners = [(self.root / f"input-{i}", self.root / f"output-{i}") for i in range(2)]
        self.set_labels(["known"], ["known"])
        self.enterContext(patch.object(workflow, "PARTNERS", self.partners))
        self.enterContext(patch.dict(os.environ, ICTV_CACHE_ONLY="false", PYTHONIOENCODING="utf-8"))

    def set_labels(self, *inputs):
        for (source, _), labels in zip(self.partners, inputs):
            graph = [{"EVORAO:pathogenIdentification": {"EVORAO:taxon": {"dcterms:title": label}}}
                     for label in labels]
            source.write_text(json.dumps({"@graph": graph}))

    def run_with(self, *replies, offline=False, cache_first=False):
        with patch.object(workflow, "download", side_effect=list(replies)) as fetch:
            workflow.build(self.cache, self.scratch, offline, cache_first)
            return [call.args[0] for call in fetch.call_args_list]

    def test_metadata_timeout_complete_cache_skips_helper(self):
        calls = self.run_with(subprocess.TimeoutExpired("metadata", 40))
        self.assertEqual(calls, [workflow.METADATA])
        self.assertEqual(self.cache.read_bytes(), self.original)
        self.assertTrue(all(out.exists() for _, out in self.partners))

    def test_metadata_timeout_resolves_only_missing_label_without_advancing_release(self):
        self.set_labels(["known", "new"], ["new"])
        self.run_with(subprocess.TimeoutExpired("metadata", 40), HELPER)
        self.assertEqual((self.scratch / "calls").read_text().splitlines(), ["new"])
        cached = json.loads(self.cache.read_bytes())
        self.assertEqual(cached["_ols_updated_at"], STAMP)
        self.assertEqual(cached["known"], {"status": "not_found"})
        self.assertEqual(cached["new"]["status"], "not_found")

    def test_unanswered_miss_preserves_original_cache_and_both_outputs(self):
        self.set_labels(["known"], ["new"])
        for _, output in self.partners:
            output.write_bytes(b"prior")
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_with(subprocess.TimeoutExpired("metadata", 40),
                          HELPER.replace(b'return {"status": "not_found"}', b"return None"))
        self.assertEqual(self.cache.read_bytes(), self.original)
        self.assertTrue(all(out.read_bytes() == b"prior" for _, out in self.partners))

    def test_failed_new_release_replays_both_sources_from_old_cache(self):
        self.run_with(b'{"updated":"2026-02-01T00:00:00Z"}',
                      HELPER.replace(b'return {"status": "not_found"}', b"return None"))
        self.assertEqual(self.cache.read_bytes(), self.original)
        self.assertTrue(all(out.exists() for _, out in self.partners))

    def test_healthy_new_release_replaces_cache_after_both_inputs_succeed(self):
        self.run_with(b'{"updated":"2026-02-01T00:00:00Z"}', HELPER)
        self.assertEqual(json.loads(self.cache.read_bytes())["_ols_updated_at"], "2026-02-01T00:00:00Z")
        self.assertEqual((self.scratch / "calls").read_text().splitlines(), ["known"])

    def test_branch_rebuild_with_miss_skips_metadata_and_tries_only_new_label(self):
        self.set_labels(["known"], ["new"])
        self.assertEqual(self.run_with(HELPER, cache_first=True), [workflow.HELPER])
        self.assertEqual((self.scratch / "calls").read_text().splitlines(), ["new"])
        self.assertEqual(json.loads(self.cache.read_bytes())["_ols_updated_at"], STAMP)

    def test_offline_replay_skips_all_downloads(self):
        self.assertEqual(self.run_with(offline=True), [])
        self.assertEqual(self.cache.read_bytes(), self.original)
