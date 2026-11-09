"""Replace the EVORA export in the migrated data.europa.eu store."""

import os
from pathlib import Path
import re
import sys
from urllib.parse import quote

import requests

STORE_API = "https://data.europa.eu/data-management/store/api/data"
EXPORT = Path("data/portal/evora_portal_all.jsonld")


class StoreError(RuntimeError):
    pass


def required_setting(environ, name):
    value = environ.get(name, "").strip()
    if not value:
        raise StoreError(f"Set {name} in the PORTAL_DATA_WORKFLOW environment.")
    return value


def path_segment(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise StoreError("Catalogue and file IDs must contain only letters, digits, _ or -.")
    return quote(value, safe="")


def require_status(response, expected, operation):
    if response.status_code == expected:
        return
    hint = {
        401: "Check the refreshed EU store access token; regenerate EU_REFRESH_TOKEN in the self-service if necessary.",
        403: "Check that the self-service account has access to the EVORA catalogue.",
        404: "Check EU_CATALOG_ID and EU_FILE_ID in the migrated store.",
    }.get(response.status_code, "Check the store service and retry the workflow.")
    # Never print response bodies, which may contain credentials or diagnostics.
    raise StoreError(f"{operation} failed (HTTP {response.status_code}). {hint}")


def upload_export(session, token, catalogue, file_id, export=EXPORT):
    catalogue_url = f"{STORE_API}/{path_segment(catalogue)}"
    path_segment(file_id)
    if not token or any(character.isspace() for character in token):
        raise StoreError("EU_STORE_TOKEN must contain the token alone, without a Bearer prefix or whitespace.")
    if not export.is_file() or export.stat().st_size == 0:
        raise StoreError(f"Export is missing or empty: {export}")

    # The migration replaced 24-character legacy IDs with new store IDs. Resolve
    # the exact old ID, never the newest file or just a matching filename.
    if re.fullmatch(r"[0-9a-fA-F]{24}", file_id):
        response = session.get(
            f"{catalogue_url}/files",
            params={"q": export.name},
            timeout=(30, 60),
            allow_redirects=False,
        )
        require_status(response, 200, "Migrated file lookup")
        try:
            matches = [
                item["id"] for item in response.json()["results"]
                if item.get("legacy_id") == file_id
            ]
        except (ValueError, KeyError, TypeError) as exc:
            raise StoreError("Unexpected response from the migrated file listing.") from exc
        if len(matches) != 1:
            raise StoreError("EU_FILE_ID could not be mapped uniquely. Set it to the current file ID from the store self-service.")
        file_id = matches[0]

    download_url = f"{catalogue_url}/{path_segment(file_id)}"
    with export.open("rb") as stream:
        response = session.put(
            download_url,
            headers={"Authorization": f"Bearer {token}"},
            files={"data": (export.name, stream, "application/ld+json")},
            timeout=(30, 600),
            allow_redirects=False,
        )
    require_status(response, 204, "Store file replacement")
    return file_id, download_url


def main():
    try:
        token = required_setting(os.environ, "EU_STORE_TOKEN")
        file_id = required_setting(os.environ, "EU_FILE_ID")
        catalogue = os.environ.get("EU_CATALOG_ID", "evora")
        github_env = Path(required_setting(os.environ, "GITHUB_ENV"))
        with requests.Session() as session:
            file_id, download_url = upload_export(session, token, catalogue, file_id)
        # Export only public identifiers. The token stays scoped to this step.
        with github_env.open("a", encoding="utf-8") as env_file:
            env_file.write(f"EU_RESOLVED_FILE_ID={file_id}\nEU_DOWNLOAD_URL={download_url}\n")
        print(f"Replaced store file {file_id} (HTTP 204).")
    except (StoreError, requests.RequestException, OSError) as exc:
        # requests exceptions can include request information; keep them private.
        message = str(exc) if isinstance(exc, StoreError) else "Store upload failed due to a network or local file error."
        print(f"::error::{message}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
