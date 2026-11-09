import contextlib
import importlib.util
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("publish_eu_store", ROOT / "scripts/publish_eu_store.py")
store = importlib.util.module_from_spec(spec)
spec.loader.exec_module(store)

LEGACY_ID = "698f9d4506e66732d18fc093"
CURRENT_ID = "01KYA77Z9ANKXYGN0M7RE0QAT7"
TOKEN = "private-test-token"


class StorePublisherTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.export = Path(self.temp.name) / "evora_portal_all.jsonld"
        self.export.write_text('{"@graph": []}', encoding="utf-8")
        self.session = Mock()
        self.session.put.return_value.status_code = 204
        self.session.get.return_value.status_code = 200
        self.session.get.return_value.json.return_value = {"results": [
            {"id": "another-file", "legacy_id": "698f8c4b06e66732d18fc092"},
            {"id": CURRENT_ID, "legacy_id": LEGACY_ID},
        ]}

    def upload(self, file_id=CURRENT_ID, **kwargs):
        return store.upload_export(self.session, TOKEN, "evora", file_id, self.export, **kwargs)

    def test_replaces_current_id_with_multipart_data_and_store_token(self):
        file_id, url = self.upload()
        self.assertEqual(file_id, CURRENT_ID)
        self.assertEqual(url, f"{store.STORE_API}/evora/{CURRENT_ID}")
        self.session.get.assert_not_called()
        args, kwargs = self.session.put.call_args
        self.assertEqual(args, (url,))
        self.assertEqual(kwargs["headers"], {"Authorization": f"Bearer {TOKEN}"})
        self.assertEqual(set(kwargs["files"]), {"data"})
        self.assertEqual(kwargs["files"]["data"][0], self.export.name)
        self.assertEqual(kwargs["files"]["data"][2], "application/ld+json")
        self.assertFalse(kwargs["allow_redirects"])
        self.assertEqual(kwargs["timeout"], (30, 600))

    def test_legacy_id_maps_exactly_instead_of_choosing_first_file(self):
        file_id, url = self.upload(LEGACY_ID)
        self.assertEqual(file_id, CURRENT_ID)
        self.assertTrue(url.endswith(CURRENT_ID))
        self.session.get.assert_called_once_with(
            f"{store.STORE_API}/evora/files", params={"q": self.export.name},
            timeout=(30, 60), allow_redirects=False,
        )

    def test_missing_or_ambiguous_legacy_id_never_uploads(self):
        for results in ([], [{"id": "one", "legacy_id": LEGACY_ID}, {"id": "two", "legacy_id": LEGACY_ID}]):
            with self.subTest(results=results):
                self.session.get.return_value.json.return_value = {"results": results}
                with self.assertRaisesRegex(store.StoreError, "mapped uniquely"):
                    self.upload(LEGACY_ID)
                self.session.put.assert_not_called()

    def test_malformed_lookup_response_never_uploads(self):
        for payload in ({}, {"results": None}, {"results": [{}]}):
            with self.subTest(payload=payload):
                self.session.get.return_value.json.return_value = payload
                with self.assertRaises(store.StoreError):
                    self.upload(LEGACY_ID)
                self.session.put.assert_not_called()

    def test_auth_permission_and_other_http_failures_are_not_success(self):
        for status, hint in ((401, "EU_REFRESH_TOKEN"), (403, "access"), (404, "EU_FILE_ID"), (500, "retry"), (302, "retry")):
            with self.subTest(status=status):
                self.session.put.return_value.status_code = status
                self.session.put.return_value.text = TOKEN
                with self.assertRaisesRegex(store.StoreError, hint) as error:
                    self.upload()
                self.assertNotIn(TOKEN, str(error.exception))

    def test_missing_or_empty_export_never_calls_store(self):
        self.export.write_bytes(b"")
        with self.assertRaisesRegex(store.StoreError, "missing or empty"):
            self.upload()
        self.export.unlink()
        with self.assertRaisesRegex(store.StoreError, "missing or empty"):
            self.upload()
        self.session.put.assert_not_called()
        self.session.get.assert_not_called()

    def test_unsafe_id_or_bearer_prefix_is_rejected(self):
        for file_id in ("../another", "file\nOTHER=value", "file?catalog=another"):
            with self.subTest(file_id=file_id):
                with self.assertRaises(store.StoreError):
                    self.upload(file_id)
        with self.assertRaisesRegex(store.StoreError, "token alone"):
            store.upload_export(self.session, "Bearer " + TOKEN, "evora", CURRENT_ID, self.export)
        self.session.put.assert_not_called()

    def test_main_exports_resolved_url_without_token(self):
        env_file = Path(self.temp.name) / "github_env"
        url = f"{store.STORE_API}/evora/{CURRENT_ID}"
        with patch.dict(os.environ, {"EU_STORE_TOKEN": TOKEN, "EU_FILE_ID": LEGACY_ID, "EU_CATALOG_ID": "evora", "GITHUB_ENV": str(env_file)}, clear=True), patch.object(store, "upload_export", return_value=(CURRENT_ID, url)), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(store.main(), 0)
        self.assertEqual(env_file.read_text(), f"EU_RESOLVED_FILE_ID={CURRENT_ID}\nEU_DOWNLOAD_URL={url}\n")
        self.assertNotIn(TOKEN, env_file.read_text())

    def test_main_fails_without_secrets_or_outputs(self):
        env_file = Path(self.temp.name) / "github_env"
        errors = io.StringIO()
        with patch.dict(os.environ, {"GITHUB_ENV": str(env_file)}, clear=True), patch.object(store, "upload_export") as upload, contextlib.redirect_stderr(errors):
            self.assertEqual(store.main(), 1)
        upload.assert_not_called()
        self.assertFalse(env_file.exists())
        self.assertIn("EU_STORE_TOKEN", errors.getvalue())

    def test_main_keeps_network_exception_details_private(self):
        errors = io.StringIO()
        with patch.dict(os.environ, {"EU_STORE_TOKEN": TOKEN, "EU_FILE_ID": CURRENT_ID, "GITHUB_ENV": str(Path(self.temp.name) / "github_env")}, clear=True), patch.object(store, "upload_export", side_effect=store.requests.RequestException(TOKEN)), contextlib.redirect_stderr(errors):
            self.assertEqual(store.main(), 1)
        self.assertNotIn(TOKEN, errors.getvalue())


if __name__ == "__main__":
    unittest.main()
