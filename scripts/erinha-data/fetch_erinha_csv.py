#!/usr/bin/env python3
import json
import csv
import os
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

# Constants

OUTPUT_DIR = Path("data/erinha")
OUTPUT_CSV = OUTPUT_DIR / "erinha_catalogue.csv"

def main() -> int:
    sa_json_raw = os.environ.get("ERINHA_GSHEET_SA_JSON")
    if not sa_json_raw:
        raise SystemExit("❌ Missing ERINHA_GSHEET_SA_JSON env variable.")

    sheet_id = os.environ.get("ERINHA_SHEET_ID")
    if not sheet_id:
        raise SystemExit("❌ Missing ERINHA_SHEET_ID env variable.")

    sheet_gid = os.environ.get("ERINHA_SHEET_GID")
    if not sheet_gid:
        raise SystemExit("❌ Missing ERINHA_SHEET_GID env variable.")

    sa_data = json.loads(sa_json_raw)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    creds = Credentials.from_service_account_info(
        sa_data,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    client = gspread.authorize(creds)




    spreadsheet = client.open_by_key(sheet_id)
    # Debug: list worksheets
    for ws in spreadsheet.worksheets():
        print(f"✅ Worksheet: {ws.title} (GID={ws.id})")

    worksheet = next(ws for ws in spreadsheet.worksheets() if str(ws.id) == sheet_gid)
    rows = worksheet.get_all_values()

    with OUTPUT_CSV.open("w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    print(f"✅ Saved {len(rows)} rows to {OUTPUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
