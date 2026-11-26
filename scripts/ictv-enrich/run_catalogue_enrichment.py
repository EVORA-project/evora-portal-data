"""Build both partner outputs from one live cache generation or one committed cache."""
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

from enrich_with_ictv import collect_labels_for_resolution


SCRIPT = Path(__file__).with_name("enrich_with_ictv.py").resolve()
METADATA = "https://www.ebi.ac.uk/ols4/api/ontologies/ictv"
HELPER = "https://cdn.jsdelivr.net/gh/EVORA-project/ictv-ontology/helpers/python/ictv-api.py?latest=true"
PARTNERS = [
    ("data/eva/eva_merged.jsonld", "data/eva/eva_merged_enriched.jsonld"),
    ("data/erinha/erinha_services.jsonld", "data/erinha/erinha_services_enriched.jsonld"),
]


def download(url):
    return subprocess.check_output(
        ["curl", "--fail", "--silent", "--show-error", "--location", "--connect-timeout", "5",
         "--max-time", "15", "--retry", "1", "--retry-max-time", "30", url], timeout=40)


def instant(stamp):
    value = datetime.fromisoformat(stamp)
    if value.tzinfo is None:
        raise ValueError("Expected an OLS timestamp with timezone")
    return value


def run_partners(cache, directory, stamp=None):
    args = ["--ols_updated_at", stamp] if stamp is not None else ["--cache-only"]
    env = dict(os.environ, PYTHONPATH=str(directory) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    deadline = time.monotonic() + (5 * 60 if stamp == "" else 90 * 60)
    for index, (source, _) in enumerate(PARTNERS):
        subprocess.run([sys.executable, str(SCRIPT), "--input", source, "--output",
                        str(directory / f"partner-{index}.jsonld"), "--cache", str(cache), *args],
                       env=env, check=True, timeout=max(1, deadline - time.monotonic()))


def build(cache, directory, offline=False, cache_first=False):
    candidate = directory / "candidate-cache.json"
    if not offline:
        current = json.loads(cache.read_bytes()) if cache.exists() else {}
        known = current.get("_ols_updated_at")
        stamp = ""
        release = None
        if not cache_first:
            try:
                stamp = json.loads(download(METADATA))["updated"]
                release = instant(stamp)
            except (KeyError, TypeError, ValueError, subprocess.SubprocessError) as exc:
                print(f"::warning::OLS release metadata unavailable ({type(exc).__name__}). "
                      "Keeping cached entries and trying only missing labels.")
                stamp = ""
        if known and release and instant(known) > release:
            print("::warning::OLS reports an older release. Keeping the last verified timestamp.")
            stamp = ""
            release = None
        if release is None or (known and instant(known) == release):
            if cache.exists():
                shutil.copyfile(cache, candidate)
            labels = set()
            for source, _ in PARTNERS:
                graph = json.loads(Path(source).read_bytes())["@graph"]
                labels.update(collect_labels_for_resolution(graph))
            offline = all(isinstance(current.get(label), dict) for label in labels)
            if offline:
                print("All required labels are cached. Skipping live lookups.")
    if not offline:
        try:
            (directory / "ictv_api.py").write_bytes(download(HELPER))
        except subprocess.SubprocessError:
            print("::warning::ICTV client download unavailable. Using committed cache.")
            offline = True
    if not offline:
        try:
            run_partners(candidate, directory, stamp)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            if isinstance(exc, subprocess.CalledProcessError) and exc.returncode != 75:
                raise
            print("::warning::Live OLS lookup failed or timed out. Replaying both inputs from committed cache.")
            offline = True
    if offline:
        run_partners(cache, directory)
    else:
        os.replace(candidate, cache)
    for index, (_, output) in enumerate(PARTNERS):
        shutil.copyfile(directory / f"partner-{index}.jsonld", output)


if __name__ == "__main__":
    cache = Path("data/ictv_cache.json")
    with tempfile.TemporaryDirectory(dir=cache.parent) as scratch:
        build(cache, Path(scratch).resolve(), offline=os.environ.get("ICTV_CACHE_ONLY") == "true",
              cache_first=os.environ.get("ICTV_CACHE_FIRST") == "true")
