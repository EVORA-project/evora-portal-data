import base64
import contextlib
import importlib.util
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from nacl.public import PrivateKey, SealedBox
import requests

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("refresh_eu_store_token", ROOT / "scripts/refresh_eu_store_token.py")
refresh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(refresh)
OLD = "old-refresh-token"
NEW = "replacement-refresh-token"
ACCESS = "replacement-access-token"
WRITER = "github-secret-writer-token"
REPO = "EVORA-project/evora-portal-data"


class TokenRefreshTests(unittest.TestCase):
    def setUp(self):
        self.key = PrivateKey.generate()
        self.session = Mock()
        self.session.get.return_value.status_code = 200
        self.session.get.return_value.json.return_value = {
            "key_id": "test-key", "key": base64.b64encode(bytes(self.key.public_key)).decode(),
        }
        self.session.post.return_value.status_code = 200
        self.session.post.return_value.json.return_value = {"access": ACCESS, "refresh": NEW}
        self.session.put.return_value.status_code = 204
        self.output = io.StringIO()
        self.capture = contextlib.redirect_stdout(self.output)
        self.capture.__enter__()
        self.addCleanup(self.capture.__exit__, None, None, None)

    def renew(self):
        return refresh.renew(self.session, OLD, WRITER, REPO)

    def test_refresh_persists_encrypted_replacement_before_returning_access(self):
        self.assertEqual(self.renew(), ACCESS)
        self.assertEqual([call[0] for call in self.session.method_calls], ["get", "put", "post", "put"])
        args, post = self.session.post.call_args
        self.assertEqual(args, (refresh.REFRESH_URL,))
        self.assertEqual(post["json"], {"refresh": OLD})
        self.assertNotIn("headers", post)  # Never send the GitHub credential to EU.
        self.assertFalse(post["allow_redirects"])
        args, put = self.session.put.call_args
        self.assertTrue(args[0].endswith("/environments/PORTAL_DATA_WORKFLOW/secrets/EU_REFRESH_TOKEN"))
        self.assertEqual(put["headers"]["Authorization"], f"Bearer {WRITER}")
        payload = put["json"]
        self.assertEqual(payload["key_id"], "test-key")
        decrypted = SealedBox(self.key).decrypt(base64.b64decode(payload["encrypted_value"]))
        self.assertEqual(decrypted.decode(), NEW)
        self.assertNotIn(NEW, repr(payload))
        preflight = self.session.put.call_args_list[0].kwargs["json"]
        self.assertEqual(SealedBox(self.key).decrypt(base64.b64decode(preflight["encrypted_value"])).decode(), OLD)
        self.assertIn(f"::add-mask::{ACCESS}", self.output.getvalue())
        self.assertIn(f"::add-mask::{NEW}", self.output.getvalue())

    def test_github_key_failure_does_not_consume_refresh_token(self):
        self.session.get.return_value.status_code = 403
        with self.assertRaisesRegex(refresh.RefreshError, "No EU refresh was attempted"):
            self.renew()
        self.session.post.assert_not_called()
        self.session.put.assert_not_called()

    def test_invalid_key_does_not_consume_refresh_token(self):
        self.session.get.return_value.json.return_value["key"] = "invalid-key"
        with self.assertRaisesRegex(refresh.RefreshError, "encryption key"):
            self.renew()
        self.session.post.assert_not_called()

    def test_rejects_missing_or_unchanged_replacement_token(self):
        for payload in ({"access": ACCESS}, {"access": ACCESS, "refresh": ""}, {"access": ACCESS, "refresh": OLD}, {"access": "bad\nvalue", "refresh": NEW}):
            with self.subTest(payload=payload):
                self.session.reset_mock()
                self.session.post.return_value.json.return_value = payload
                with self.assertRaises(refresh.RefreshError):
                    self.renew()
                self.session.put.assert_called_once()

    def test_expired_token_is_not_saved_or_retried(self):
        self.session.post.return_value.status_code = 401
        with self.assertRaisesRegex(refresh.RefreshError, "HTTP 401"):
            self.renew()
        self.session.post.assert_called_once()
        self.session.put.assert_called_once()

    def test_refresh_timeout_is_not_retried(self):
        self.session.post.side_effect = requests.Timeout(OLD)
        with self.assertRaisesRegex(refresh.RefreshError, "outcome is unknown") as error:
            self.renew()
        self.assertNotIn(OLD, str(error.exception))
        self.session.post.assert_called_once()
        self.session.put.assert_called_once()

    def test_retries_only_idempotent_secret_write(self):
        self.session.put.side_effect = [Mock(status_code=204), requests.Timeout(NEW), Mock(status_code=503), Mock(status_code=204)]
        with patch.object(refresh.time, "sleep"):
            self.assertEqual(self.renew(), ACCESS)
        self.assertEqual(self.session.put.call_count, 4)
        self.session.post.assert_called_once()
        payloads = [call.kwargs["json"] for call in self.session.put.call_args_list[1:]]
        self.assertTrue(all(payload == payloads[0] for payload in payloads))

    def test_missing_github_write_permission_does_not_consume_eu_token(self):
        self.session.put.return_value.status_code = 403
        with self.assertRaisesRegex(refresh.RefreshError, "No EU refresh was attempted"):
            self.renew()
        self.session.post.assert_not_called()

    def test_permanent_save_failure_requires_recovery(self):
        self.session.put.side_effect = [Mock(status_code=204), Mock(status_code=403)]
        with self.assertRaisesRegex(refresh.RefreshError, "generate a new EU token pair"):
            self.renew()
        self.assertEqual(self.session.put.call_count, 2)

    def test_save_failure_never_exports_access(self):
        with tempfile.TemporaryDirectory() as temporary:
            env_file = Path(temporary) / "github_env"
            env = {"EU_REFRESH_TOKEN": OLD, "EU_SECRET_WRITER_TOKEN": WRITER, "GITHUB_REPOSITORY": REPO, "GITHUB_ENV": str(env_file)}
            self.session.put.side_effect = [Mock(status_code=204), Mock(status_code=403)]
            manager = Mock()
            manager.__enter__ = Mock(return_value=self.session)
            manager.__exit__ = Mock(return_value=False)
            errors = io.StringIO()
            with patch.dict(os.environ, env, clear=True), patch.object(refresh.requests, "Session", return_value=manager), contextlib.redirect_stderr(errors):
                self.assertEqual(refresh.main(), 1)
            self.assertEqual(env_file.read_text(), "")
            for token in (OLD, NEW, ACCESS, WRITER):
                self.assertNotIn(token, errors.getvalue())

    def test_success_exports_access_without_refresh_or_writer(self):
        with tempfile.TemporaryDirectory() as temporary:
            env_file = Path(temporary) / "github_env"
            env = {"EU_REFRESH_TOKEN": OLD, "EU_SECRET_WRITER_TOKEN": WRITER, "GITHUB_REPOSITORY": REPO, "GITHUB_ENV": str(env_file)}
            manager = Mock()
            manager.__enter__ = Mock(return_value=self.session)
            manager.__exit__ = Mock(return_value=False)
            with patch.dict(os.environ, env, clear=True), patch.object(refresh.requests, "Session", return_value=manager):
                self.assertEqual(refresh.main(), 0)
            self.assertEqual(env_file.read_text(), f"EU_STORE_TOKEN={ACCESS}\n")
            for token in (OLD, NEW, WRITER):
                self.assertNotIn(token, env_file.read_text())

    def test_bad_output_path_does_not_consume_refresh(self):
        with tempfile.TemporaryDirectory() as temporary:
            env = {"EU_REFRESH_TOKEN": OLD, "EU_SECRET_WRITER_TOKEN": WRITER, "GITHUB_REPOSITORY": REPO, "GITHUB_ENV": str(Path(temporary) / "missing" / "env")}
            with patch.dict(os.environ, env, clear=True), patch.object(refresh, "renew") as renew, contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(refresh.main(), 1)
            renew.assert_not_called()


if __name__ == "__main__":
    unittest.main()
