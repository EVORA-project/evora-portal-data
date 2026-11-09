"""Replace the EVORA export in the migrated data.europa.eu store."""

import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import quote

from nacl.public import PublicKey, SealedBox
import requests

STORE_API = "https://data.europa.eu/data-management/store/api/data"
EXPORT = Path("data/portal/evora_portal_all.jsonld")
CHUNK_SIZE = 5 * 1024 * 1024
DIRECT_UPLOAD_LIMIT = CHUNK_SIZE


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
    # Include only a short validation message, redacting credentials even if an
    # upstream error accidentally reflects one. Never print complete responses.
    if response.status_code in (400, 409, 413):
        try:
            payload = response.json()
            detail = json.dumps(payload, ensure_ascii=True) if isinstance(payload, (dict, list)) else None
            if isinstance(detail, str):
                for name, value in os.environ.items():
                    if value and ("TOKEN" in name or "BEARER" in name or "SECRET" in name):
                        detail = detail.replace(value, "[redacted]")
                detail = re.sub(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", "[redacted JWT]", detail)
                detail = " ".join(detail.split())[:1500]
                hint += f" Server validation: {detail}"
        except ValueError:
            pass
    raise StoreError(f"{operation} failed (HTTP {response.status_code}). {hint}")



def chunked_replace(session, token, catalogue_url, file_id, export):
    upload_url = f"{catalogue_url}/chunked/{path_segment(file_id)}"
    headers = {"Authorization": f"Bearer {token}"}
    size = export.stat().st_size
    existing = session.get(
        f"{catalogue_url}/{path_segment(file_id)}", headers=headers, stream=True,
        timeout=(30, 60), allow_redirects=False,
    )
    status = existing.status_code
    existing.close()
    print(f"Existing file access check: HTTP {status}.")
    require_status(existing, 200, "Existing store file access")
    response = session.put(
        f"{upload_url}/update", headers=headers,
        json={"filename": export.name, "mimetype": "application/ld+json", "size": size, "chunk_size": CHUNK_SIZE},
        timeout=(30, 120), allow_redirects=False,
    )
    require_status(response, 200, "Chunked file replacement initialization")
    finish_chunked(session, token, catalogue_url, file_id, export, response.json())


def finish_chunked(session, token, catalogue_url, file_id, export, info):
    upload_url = f"{catalogue_url}/chunked/{path_segment(file_id)}"
    headers = {"Authorization": f"Bearer {token}"}
    size = export.stat().st_size
    try:
        if not info.get("resume_existing") and (not isinstance(info.get("upload_id"), str) or not info["upload_id"]):
            raise ValueError
        chunk_size = info.get("chunk_size", CHUNK_SIZE)
        if type(chunk_size) is not int or not 0 < chunk_size <= 64 * 1024 * 1024:
            raise ValueError
        if info.get("file_id", file_id) != file_id:
            raise ValueError
        parts = []
        transferred = 0
        with export.open("rb") as stream:
            while chunk := stream.read(chunk_size):
                number = len(parts) + 1
                checksum = base64.b64encode(hashlib.sha256(chunk).digest()).decode("ascii")
                response = session.put(
                    f"{upload_url}/part", params={"part_number": number},
                    headers={**headers, "X-Checksum-SHA256": checksum},
                    files={"data": ("chunk", chunk, "application/octet-stream")},
                    timeout=(30, 300), allow_redirects=False,
                )
                require_status(response, 200, f"Chunk {number} upload")
                etag = response.json()["etag"]
                if not isinstance(etag, str) or not etag:
                    raise ValueError
                parts.append({"part": number, "etag": etag})
                transferred += len(chunk)
                print(f"Uploaded chunk {number}: {transferred}/{size} bytes.")
        if transferred != size:
            raise StoreError("Export changed during the chunked upload; completion was stopped.")
        response = session.post(
            f"{upload_url}/complete", headers=headers, json={"parts": parts},
            timeout=(30, 300), allow_redirects=False,
        )
        require_status(response, 201, "Chunked upload completion")
    except (StoreError, requests.RequestException, OSError, ValueError, KeyError, TypeError) as exc:
        # Do not call the abort endpoint for updates to existing files: after a
        # failed migrated-file update it removed the file from the public list.
        if isinstance(exc, (ValueError, KeyError, TypeError)):
            raise StoreError("Unexpected response from the chunked upload API.") from exc
        raise


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
    if export.stat().st_size > DIRECT_UPLOAD_LIMIT:
        chunked_replace(session, token, catalogue_url, file_id, export)
    else:
        with export.open("rb") as stream:
            response = session.put(
                download_url,
                headers={"Authorization": f"Bearer {token}"},
                files={"data": (export.name, stream, "application/ld+json")},
                timeout=(30, 600),
                allow_redirects=False,
            )
        if response.status_code == 413:
            chunked_replace(session, token, catalogue_url, file_id, export)
        else:
            require_status(response, 204, "Store file replacement")
    return file_id, download_url



def file_id_persister(session):
    repository = required_setting(os.environ, "GITHUB_REPOSITORY")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise StoreError("Invalid GITHUB_REPOSITORY for file recovery.")
    writer = required_setting(os.environ, "EU_SECRET_WRITER_TOKEN")
    github_headers = {"Authorization": f"Bearer {writer}", "Accept": "application/vnd.github+json"}
    secrets_url = f"https://api.github.com/repos/{repository}/environments/PORTAL_DATA_WORKFLOW/secrets"
    key_response = session.get(f"{secrets_url}/public-key", headers=github_headers, timeout=(15, 60), allow_redirects=False)
    require_status(key_response, 200, "Recovery secret encryption key lookup")
    key = key_response.json()
    encryptor = SealedBox(PublicKey(base64.b64decode(key["key"])))

    def persist(file_id):
        response = session.put(
            f"{secrets_url}/EU_FILE_ID", headers=github_headers,
            json={"key_id": key["key_id"], "encrypted_value": base64.b64encode(encryptor.encrypt(file_id.encode())).decode()},
            timeout=(15, 60), allow_redirects=False,
        )
        require_status(response, 204, "Recovery file ID persistence")

    return persist


def resume_pending_export(session, token, catalogue, current_id, pending_id, export=EXPORT):
    """Finish an existing upload; do not initialise or create a file."""
    catalogue_url = f"{STORE_API}/{path_segment(catalogue)}"
    path_segment(pending_id)
    if not export.is_file() or export.stat().st_size == 0:
        raise StoreError("Resume export is missing or empty.")
    persist = file_id_persister(session)
    persist(current_id)
    print(f"Resuming existing store upload: {pending_id}")
    # The store keeps its upload session under the file ID. Part PUTs replace
    # those same numbered parts; no new file or multipart session is created.
    finish_chunked(session, token, catalogue_url, pending_id, export, {
        "file_id": pending_id, "chunk_size": CHUNK_SIZE, "resume_existing": True,
    })
    persist(pending_id)
    return pending_id, f"{catalogue_url}/{pending_id}"


def main():
    try:
        token = required_setting(os.environ, "EU_STORE_TOKEN")
        file_id = required_setting(os.environ, "EU_FILE_ID")
        catalogue = os.environ.get("EU_CATALOG_ID", "evora")
        github_env = Path(required_setting(os.environ, "GITHUB_ENV"))
        with requests.Session() as session:
            pending_id = os.environ.get("EU_RESUME_FILE_ID", "").strip()
            if pending_id:
                file_id, download_url = resume_pending_export(session, token, catalogue, file_id, pending_id)
            else:
                file_id, download_url = upload_export(session, token, catalogue, file_id)
        # Export only public identifiers. The token stays scoped to this step.
        with github_env.open("a", encoding="utf-8") as env_file:
            env_file.write(f"EU_RESOLVED_FILE_ID={file_id}\nEU_DOWNLOAD_URL={download_url}\n")
        print(f"Replaced store file {file_id}.")
    except (StoreError, requests.RequestException, OSError) as exc:
        # requests exceptions can include request information; keep them private.
        message = str(exc) if isinstance(exc, StoreError) else "Store upload failed due to a network or local file error."
        print(f"::error::{message}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
