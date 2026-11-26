"""Assemble the stable Pages paths after verification of an immutable snapshot."""
import hashlib
import json
from pathlib import Path
import shutil
import sys


def checksum(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def stage_site(site, reports, source):
    validation = json.loads((reports / "validation.json").read_bytes())
    parity = json.loads((reports / "compact-parity.json").read_bytes())
    if validation["conforms"] is not True:
        raise ValueError("Publication requires strict EVORAO conformance")
    digest = validation["candidateSha256"]
    if (len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest)
            or parity["snapshot"]["id"] != "sha256:" + digest
            or parity["projectionSourceSha256"] != validation["sourceSha256"]):
        raise ValueError("Validation and compact parity reports refer to different inputs")
    snapshot = site / "snapshots" / digest
    export = snapshot / "catalogue.jsonld"
    if checksum(export) != digest or checksum(snapshot / "index.json") != parity["indexSha256"]:
        raise ValueError("Snapshot bytes differ from the verified files")
    legacy_digest = checksum(source)
    if legacy_digest != validation["sourceSha256"]:
        raise ValueError("Legacy API companion bytes differ from the verified projection source")
    legacy_path = site / "snapshots" / legacy_digest / "catalogue.jsonld"
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    if legacy_digest != digest:
        shutil.copyfile(source, legacy_path)
    origin = "https://data.evora-project.eu"
    base = f"{origin}/snapshots/{digest}/"
    manifest = {"schemaVersion": "evora.portal.release/1", "snapshotId": parity["snapshot"]["id"],
                "exportUrl": base + "catalogue.jsonld", "indexUrl": base + "index.json",
                "legacyExportUrl": f"{origin}/snapshots/{legacy_digest}/catalogue.jsonld",
                "legacySha256": legacy_digest,
                "indexSha256": parity["indexSha256"], "resourceCount": parity["snapshot"]["resourceCount"]}
    public = site / "data" / "portal"
    public.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(export, public / "evora_portal_all.jsonld")
    (public / "latest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    stage_site(*map(Path, sys.argv[1:]))
