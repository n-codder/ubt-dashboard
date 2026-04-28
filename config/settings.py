"""
Central configuration loader.
Reads from .env and team_ids.json.
"""

import json
import os
from pathlib import Path

from dotenv import load_dotenv

# Project root = parent of this file's directory
ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")

# ── Paths ──────────────────────────────────────────────────────────────────
DATA_RAW       = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"
ASSETS_PLAYERS = ROOT / "player-profiles" / "assets" / "players"
ASSETS_FLAGS   = ROOT / "player-profiles" / "assets" / "flags"

# ── API ────────────────────────────────────────────────────────────────────
API_BASKETBALL_KEY    = os.getenv("API_BASKETBALL_KEY", "")
API_BASKETBALL_BASE   = "https://v1.basketball.api-sports.io"
API_BASKETBALL_LIMIT  = 100  # requests per day

RAPIDAPI_KEY              = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_HOST_BASKETBALL  = "basketapi1.p.rapidapi.com"
RAPIDAPI_BASE_BASKETBALL  = "https://basketapi1.p.rapidapi.com"

# ── Google Sheets ──────────────────────────────────────────────────────────
GOOGLE_SHEETS_ID           = os.getenv("GOOGLE_SHEETS_ID", "")
GOOGLE_SERVICE_ACCOUNT_FILE = ROOT / os.getenv(
    "GOOGLE_SERVICE_ACCOUNT_FILE", "config/service_account.json"
)

# ── Season ─────────────────────────────────────────────────────────────────
SEASON = os.getenv("SEASON", "2025-2026")

# ── Team IDs ───────────────────────────────────────────────────────────────
with open(ROOT / "config" / "team_ids.json") as f:
    TEAM_IDS = json.load(f)

# ── Sheet tabs (must match Looker Studio data sources) ─────────────────────
SHEET_TABS = {
    "games":        "games",
    "player_stats": "player_stats",
    "standings":    "standings",
}

# ── Competition labels (display) ───────────────────────────────────────────
COMPETITION_LABELS = {
    "eurocup":       "EuroCup",
    "aba_league":    "ABA League",
    "liga_nationala": "Liga Națională",
    "cupa_romaniei": "Cupa României",
}
