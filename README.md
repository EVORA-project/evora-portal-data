# evora-portal-data

## Licensing

### Code
All source code in this repository is licensed under the MIT License.
See the `LICENSE` file.

### Data
Catalogue data, JSON-LD files, and dataset exports are licensed under
Creative Commons Attribution-ShareAlike 4.0 International (CC-BY-SA 4.0).

See the `DATA_LICENSE` file.

## Publishing to data.europa.eu

The `build-data.yml` workflow publishes the full JSON-LD export and its DCAT
metadata after a successful build on `main`. Publication checks out the exact
commit produced by that build. Manual runs on other branches build data without
publishing it to the EU catalogue.

### Credentials after the store migration

The migrated store uses a separate Bearer JWT. The old service-account login
remains in use for the catalogue metadata request to hub-repo; its token must not
be reused for the new store.

1. Sign in through EU Login at the [EVORA catalogue management page](https://data.europa.eu/data-management/data/repo/catalogue/evora/manage/).
2. Once the portal team has granted EVORA catalogue access, generate a token pair
   at the [API token self-service](https://data.europa.eu/data-management/users/api-token/).
   Use the refresh token to initialise the workflow; access tokens are obtained
   automatically for each publishing run.
3. In GitHub, open **Settings > Environments > PORTAL_DATA_WORKFLOW** and configure:

| Type | Name | Value |
| --- | --- | --- |
| Secret | `EU_REFRESH_TOKEN` | Initial self-service refresh token, without a `Bearer ` prefix. The workflow replaces this environment secret after every renewal. |
| Secret | `EU_SECRET_WRITER_TOKEN` | GitHub fine-grained PAT restricted to this repository with **Environments: read and write**, or an appropriately authorised GitHub App token. This is separate from the EU credentials. |
| Secret | `EU_FILE_ID` | Existing store file ID. The old 24-character ID is also accepted when it can be mapped to a migrated file. |
| Secret | `EU_ID` | Existing hub-repo service-account client ID. |
| Secret | `EU_TOKEN` | Existing hub-repo service-account client secret. |
| Variable | `EU_CATALOG_ID` | Catalogue identifier for both requests; defaults to `evora`. |

### Automatic token renewal

Before each upload, `refresh_eu_store_token.py` sends a JSON POST to
`https://data.europa.eu/data-management/users/api/token/refresh/` with
`{"refresh": "<current refresh token>"}`. Opening that URL in a browser sends GET
and returns HTTP 405; POST is the supported method.

Before consuming the EU refresh token, the script verifies GitHub write
permission by encrypting and saving the current refresh token unchanged. This
catches missing permission or organisation approval without invalidating it.

The workflow requires the response to contain both `access` and a replacement
`refresh`, following the platform's self-rotation policy. It encrypts the
replacement with the GitHub environment public key and saves it back as
`EU_REFRESH_TOKEN` **before making the access token available to the upload**.
`EU_STORE_TOKEN` is now a temporary runtime value, not a secret to set manually.

Set `EU_REFRESH_TOKEN` specifically in **PORTAL_DATA_WORKFLOW**, not at repository
or organisation scope: the updater writes to that environment. The publishing
job uses one concurrency group with cancellation disabled. Other workflows using
the same refresh token must use that group too, and the token must not be reused
manually or by another application. See GitHub's documentation on
[environment secret updates](https://docs.github.com/en/rest/actions/secrets#create-or-update-an-environment-secret)
and [when secrets are read](https://docs.github.com/en/actions/reference/security/secrets).

The default workflow `GITHUB_TOKEN` cannot update environment secrets. Configure
`EU_SECRET_WRITER_TOKEN` with the permission above and maintain its validity. If
using a GitHub App, supply a valid installation token at runtime; a static
installation token expires and is not a permanent bootstrap credential.

The self-service describes access tokens as lasting seven days and refresh
tokens as lasting thirty days. Use the actual expiry supplied by the platform;
the quoted access expiry did not match the seven-day label. Whether rotated
refresh tokens receive a new thirty-day window or have an absolute lifetime
still needs confirmation from the platform team. If the refresh token expires
or is revoked, generate a new pair and replace `EU_REFRESH_TOKEN` manually.

The refresh POST is never retried automatically: a lost response may mean the
single-use token was already consumed. The subsequent GitHub write is idempotent
and is retried on transient failures. A cancellation, runner failure, or failure
to save after rotation can still require a fresh pair from the self-service;
there is no transaction spanning the two services. Token bodies are not logged,
and returned tokens are masked in GitHub Actions.

The [store API documentation](https://data.europa.eu/data-management/store/api/schema/docs/)
specifies a multipart `data` field, Bearer authentication, and HTTP 204 for a
successful direct file replacement. The full EVORA export exceeds the migrated
store's direct-upload limit (HTTP 413). Files larger than 5 MiB use the chunked
`update`, `part`, and `complete` endpoints, preserving the existing file ID. Parts
are uploaded sequentially with SHA-256 checksums, using the chunk size confirmed
by the server; completion submits their ordered ETags. An incomplete upload is
left for inspection rather than aborted: the new API's abort operation affected
the migrated file's public visibility during validation. Small files also fall back to chunking if
the direct request returns HTTP 413.

The uploader replaces an existing file and fails if
`EU_FILE_ID` is missing, so repeated runs cannot create duplicate files. For an
old ID, it searches the store's file listing and matches `legacy_id` exactly;
if the ID cannot be mapped uniquely, set `EU_FILE_ID` to the current ID from the
self-service. It never chooses a file solely by its name or modification date.

After a successful upload, the metadata wrapper uses the resolved download URL
under `/data-management/store/api/data/evora/<new-file-id>` (or the configured
catalogue) for its distribution and download link. Metadata publication still
uses the existing hub-repo endpoint.

A 401 from the store means the refreshed access token needs checking; a 403 means
catalogue permissions need checking. If the store succeeds but hub-repo fails,
check the separate `EU_ID` / `EU_TOKEN` credentials or the repository service.
The July migration email offers two independent paths for metadata: direct
hub-repo updates (the current path) or indirect data-management updates with
history and manual editing. Switching to the indirect path requires the platform
team to arrange the EVORA datasets and its authenticated API contract to be
verified. It does not remove the rotating-token requirement for store uploads.

Install `requests` and `PyNaCl`, then run the publishing tests without credentials
or network access:

```sh
python -m unittest discover -s tests -v
```

After configuring credentials, run **Build EVORA portal data** manually on `main`
and check both the file replacement and metadata publication steps. Local tests
do not verify live credentials or catalogue permissions.

For a retry using the already committed export, select `publish_existing` in
**Run workflow**. The scheduled run still rebuilds all partner data normally.

For deliberate recovery of a store file that returns HTTP 404, also select
`recover_missing_file`. This creates the export via the chunked creation API,
saves its new ID in `EU_FILE_ID` only after completion, and updates the existing
catalogue dataset to use the new download URL. It refuses recovery when the old
file is still accessible. The normal scheduled path never creates a new file.
The store abort API must not be called on updates to existing files: it removed
the migrated record during a failed upload, rather than only its temporary parts.
