"""Refresh EU store tokens and persist rotation before exposing the access token."""

import base64
import binascii
import os
from pathlib import Path
import re
import sys
import time
from urllib.parse import quote

from nacl.public import PublicKey, SealedBox
import requests

REFRESH_URL = "https://data.europa.eu/data-management/users/api/token/refresh/"
SECRET_NAME = "EU_REFRESH_TOKEN"
ENVIRONMENT = "PORTAL_DATA_WORKFLOW"


class RefreshError(RuntimeError):
    pass


def setting(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise RefreshError(f"Set {name} before refreshing the EU store token. See README.md.")
    return value


def valid_token(value):
    return isinstance(value, str) and bool(value) and not any(c.isspace() for c in value)


def mask(token):
    print(f"::add-mask::{token}", flush=True)


def save_refresh(session, url, headers, payload, rotated=True):
    # Saving the same encrypted value is idempotent; refreshing a single-use
    # EU token is not. Retry only this GitHub write, never the refresh POST.
    for attempt in range(3):
        try:
            response = session.put(
                url, headers=headers, json=payload,
                timeout=(15, 60), allow_redirects=False,
            )
        except requests.RequestException:
            retryable = True
        else:
            if response.status_code in (201, 204):
                return
            retryable = response.status_code == 429 or response.status_code >= 500
        if not retryable or attempt == 2:
            if not rotated:
                raise RefreshError(
                    "Cannot write the EU refresh secret in GitHub. Check EU_SECRET_WRITER_TOKEN "
                    "(Environments: write) and any organisation approval requirement. "
                    "No EU refresh was attempted."
                ) from None
            raise RefreshError(
                "The EU token rotated, but its replacement could not be saved to GitHub. "
                "Check EU_SECRET_WRITER_TOKEN (Environments: write), generate a new EU token pair "
                "and replace EU_REFRESH_TOKEN before rerunning."
            ) from None
        time.sleep(2 ** attempt)


def renew(session, refresh_token, writer_token, repository):
    if not valid_token(refresh_token) or not valid_token(writer_token):
        raise RefreshError("Tokens must contain the token alone, without whitespace or a Bearer prefix.")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise RefreshError("GITHUB_REPOSITORY must identify the owner and repository.")
    secret_url = (
        f"https://api.github.com/repos/{repository}/environments/"
        f"{quote(ENVIRONMENT, safe='')}/secrets/{SECRET_NAME}"
    )
    github_headers = {
        "Authorization": f"Bearer {writer_token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    # Fetch and validate the encryption key before consuming the single-use token.
    response = session.get(
        secret_url.rsplit("/", 1)[0] + "/public-key",
        headers=github_headers, timeout=(15, 60), allow_redirects=False,
    )
    if response.status_code != 200:
        raise RefreshError("Cannot read the GitHub environment encryption key. Check EU_SECRET_WRITER_TOKEN and PORTAL_DATA_WORKFLOW. No EU refresh was attempted.")
    try:
        key_data = response.json()
        key_id = key_data["key_id"]
        if not isinstance(key_id, str) or not key_id:
            raise ValueError
        encryptor = SealedBox(PublicKey(base64.b64decode(key_data["key"], validate=True)))
    except (ValueError, KeyError, TypeError, binascii.Error) as exc:
        raise RefreshError("GitHub returned an invalid environment encryption key. No EU refresh was attempted.") from exc

    # Confirm write permission by saving the current value unchanged before
    # consuming it. Reading a public key alone does not prove write permission.
    current_payload = {
        "key_id": key_id,
        "encrypted_value": base64.b64encode(encryptor.encrypt(refresh_token.encode("utf-8"))).decode("ascii"),
    }
    save_refresh(session, secret_url, github_headers, current_payload, rotated=False)

    # No automatic retries: a timeout could mean the server already rotated it.
    try:
        response = session.post(
            REFRESH_URL, json={"refresh": refresh_token},
            timeout=(15, 60), allow_redirects=False,
        )
    except requests.RequestException:
        raise RefreshError("The EU refresh outcome is unknown after a network error. Do not retry the old token automatically; check or regenerate EU_REFRESH_TOKEN in the self-service.") from None
    if response.status_code != 200:
        raise RefreshError(f"EU token refresh failed (HTTP {response.status_code}). Check or regenerate EU_REFRESH_TOKEN in the self-service.")
    try:
        data = response.json()
        access, replacement = data["access"], data["refresh"]
        if not valid_token(access) or not valid_token(replacement):
            raise ValueError
    except (ValueError, KeyError, TypeError) as exc:
        raise RefreshError("The refresh response must contain both access and refresh tokens. No access token was exported; check the platform's rotation response and regenerate the pair if necessary.") from exc
    mask(access)
    mask(replacement)
    if replacement == refresh_token:
        raise RefreshError("The platform returned the old refresh token despite its rotation policy. No access token was exported; check the refresh contract with the platform team.")
    payload = {
        "key_id": key_id,
        "encrypted_value": base64.b64encode(encryptor.encrypt(replacement.encode("utf-8"))).decode("ascii"),
    }
    save_refresh(session, secret_url, github_headers, payload)
    return access


def main():
    try:
        refresh_token = setting("EU_REFRESH_TOKEN")
        writer_token = setting("EU_SECRET_WRITER_TOKEN")
        repository = setting("GITHUB_REPOSITORY")
        github_env = Path(setting("GITHUB_ENV"))
        # Fail on a bad output path before rotating a single-use credential.
        with github_env.open("a", encoding="utf-8"):
            pass
        mask(refresh_token)
        mask(writer_token)
        with requests.Session() as session:
            access = renew(session, refresh_token, writer_token, repository)
        with github_env.open("a", encoding="utf-8") as env_file:
            env_file.write(f"EU_STORE_TOKEN={access}\n")
        print("Refreshed EU access token and saved the replacement refresh token.")
    except (RefreshError, requests.RequestException, OSError) as exc:
        message = str(exc) if isinstance(exc, RefreshError) else "Token renewal failed before completion due to a network or local file error."
        print(f"::error::{message}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
