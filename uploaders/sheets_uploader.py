"""
uploaders/sheets_uploader.py — Uploadează CSV-urile procesate în Google Sheets.

Tab-uri:
  "games"                ← data/processed/games_normalized.csv
  "player_stats"         ← data/processed/player_stats_normalized.csv
  "player_stats_per_game"← data/processed/player_stats_per_game_normalized.csv

Dacă tab-ul există deja, îl suprascrie complet (clear + re-write).
Dacă nu există, îl creează.

Usage:
    python uploaders/sheets_uploader.py
    python uploaders/sheets_uploader.py --tab games
    python uploaders/sheets_uploader.py --dry-run
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import GOOGLE_SHEETS_ID, GOOGLE_SERVICE_ACCOUNT_FILE

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Scopes necesare pentru Sheets ─────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# ── Definiție tab-uri ──────────────────────────────────────────────────────
TABS = [
    {
        "tab":  "games",
        "csv":  ROOT / "data" / "processed" / "games_normalized.csv",
    },
    {
        "tab":  "player_stats",
        "csv":  ROOT / "data" / "processed" / "player_stats_normalized.csv",
    },
    {
        "tab":  "player_stats_per_game",
        "csv":  ROOT / "data" / "processed" / "player_stats_per_game_normalized.csv",
    },
]

# Google Sheets API permite max 60 cereri/minut; scriem în batch-uri
BATCH_ROWS = 1000


# ── Auth ───────────────────────────────────────────────────────────────────

def _auth() -> gspread.Client:
    sa_file = Path(GOOGLE_SERVICE_ACCOUNT_FILE)
    if not sa_file.exists():
        log.error(f"Service account lipsește: {sa_file}")
        sys.exit(1)

    creds = Credentials.from_service_account_file(str(sa_file), scopes=SCOPES)
    return gspread.authorize(creds)


# ── Upload unui tab ────────────────────────────────────────────────────────

def _upload_tab(
    spreadsheet: gspread.Spreadsheet,
    tab_name: str,
    df: pd.DataFrame,
    dry_run: bool,
) -> int:
    # Pregătesc datele: NaN → "" pentru o scriere curată
    df_clean = df.fillna("").astype(str)
    header   = df_clean.columns.tolist()
    rows     = df_clean.values.tolist()
    all_data = [header] + rows

    if dry_run:
        log.info(f"  [DRY-RUN] {tab_name}: {len(rows)} rânduri, {len(header)} coloane — nimic scris")
        return len(rows)

    # Găsesc sau creez tab-ul
    try:
        ws = spreadsheet.worksheet(tab_name)
        log.info(f"  Tab '{tab_name}' există — șterg conținutul vechi...")
        ws.clear()
    except gspread.WorksheetNotFound:
        log.info(f"  Tab '{tab_name}' nu există — îl creez...")
        ws = spreadsheet.add_worksheet(title=tab_name, rows=len(all_data) + 10, cols=len(header))

    # Scriere în batch-uri pentru a evita timeout-urile pe seturi mari
    log.info(f"  Scriu {len(rows)} rânduri în tab '{tab_name}' (batch_size={BATCH_ROWS})...")
    for i in range(0, len(all_data), BATCH_ROWS):
        chunk     = all_data[i : i + BATCH_ROWS]
        start_row = i + 1
        end_row   = start_row + len(chunk) - 1
        ws.update(
            range_name=f"A{start_row}",
            values=chunk,
            value_input_option="RAW",
        )
        log.info(f"    rânduri {start_row}–{end_row} ✓")
        if i + BATCH_ROWS < len(all_data):
            time.sleep(1.2)   # respectă rate limit Sheets API

    # Înghețe primul rând (header) pentru ușurință navigare
    ws.freeze(rows=1)

    return len(rows)


# ── Entry point ────────────────────────────────────────────────────────────

def run(only_tab: str | None = None, dry_run: bool = False) -> None:
    log.info("=" * 60)
    log.info("sheets_uploader.py — start")
    if dry_run:
        log.info("  [DRY-RUN] — nicio scriere reală")
    log.info("=" * 60)

    if not GOOGLE_SHEETS_ID:
        log.error("GOOGLE_SHEETS_ID lipsește din .env")
        sys.exit(1)

    client      = _auth()
    spreadsheet = client.open_by_key(GOOGLE_SHEETS_ID)
    log.info(f"Spreadsheet: '{spreadsheet.title}'  ({GOOGLE_SHEETS_ID})")

    tabs_to_run = [t for t in TABS if only_tab is None or t["tab"] == only_tab]
    if not tabs_to_run:
        log.error(f"Tab necunoscut: '{only_tab}'. Valori valide: {[t['tab'] for t in TABS]}")
        sys.exit(1)

    summary: list[tuple[str, int]] = []

    for cfg in tabs_to_run:
        tab_name = cfg["tab"]
        csv_path = cfg["csv"]

        log.info(f"\n[{tab_name}]  ← {csv_path.name}")

        if not csv_path.exists():
            log.warning(f"  CSV lipsește ({csv_path}) — skip.")
            summary.append((tab_name, 0))
            continue

        df = pd.read_csv(csv_path)
        log.info(f"  Citit: {len(df)} rânduri × {len(df.columns)} coloane")

        n = _upload_tab(spreadsheet, tab_name, df, dry_run)
        summary.append((tab_name, n))

    # ── Summary ───────────────────────────────────────────────────────────
    log.info("\n" + "─" * 60)
    log.info("SUMAR")
    for tab_name, n_rows in summary:
        status = "DRY-RUN" if dry_run else "✓"
        log.info(f"  {tab_name:<30} {n_rows:>6} rânduri  [{status}]")
    log.info("─" * 60)
    log.info("sheets_uploader.py — done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload CSV-uri procesate în Google Sheets")
    parser.add_argument(
        "--tab", metavar="TAB",
        help="Uploadează doar un tab: games / player_stats / player_stats_per_game",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Citește CSV-urile dar nu scrie nimic în Sheets",
    )
    args = parser.parse_args()
    run(only_tab=args.tab, dry_run=args.dry_run)
