"""
fetchers/schedule.py — Programul meciurilor U-BT → data/processed/schedule.json

O singură cerere RapidAPI (SofaScore /team/{id}/matches/next/0), care întoarce
meciurile viitoare din toate competițiile (EuroCup, ABA, LN).

Fetch-ul automat (fetchers/auto_fetch.py) citește fișierul ca să știe când
s-a terminat un meci și abia atunci face fetch — fără apeluri API la fiecare
verificare (planul RapidAPI are doar 50 de cereri pe zi).

Rulează la finalul run_all, după fiecare fetch.

Usage:
    python fetchers/schedule.py
"""

import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import RAPIDAPI_KEY, RAPIDAPI_HOST_BASKETBALL, RAPIDAPI_BASE_BASKETBALL, TEAM_IDS

log = logging.getLogger(__name__)

SCHEDULE_PATH = ROOT / "data" / "processed" / "schedule.json"
TEAM_ID = TEAM_IDS["competitions"]["aba_league"]["team_id"]

# SofaScore uniqueTournament.id → competition_key
TOURNAMENTS = {
    TEAM_IDS["competitions"]["aba_league"]["tournament_id"]: "aba_league",
    TEAM_IDS["competitions"]["liga_nationala"]["tournament_id"]: "liga_nationala",
    TEAM_IDS["competitions"]["eurocup"].get("sofascore_tournament_id", 141): "eurocup",
}

# meciurile trecute rămân în fișier o săptămână, ca fetch-ul automat să le vadă
KEEP_PAST = timedelta(days=7)


def load() -> list[dict]:
    if not SCHEDULE_PATH.exists():
        return []
    return json.loads(SCHEDULE_PATH.read_text()).get("games", [])


def _fetch_next() -> list[dict]:
    r = requests.get(
        f"{RAPIDAPI_BASE_BASKETBALL}/api/basketball/team/{TEAM_ID}/matches/next/0",
        headers={"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": RAPIDAPI_HOST_BASKETBALL},
        timeout=20,
    )
    r.raise_for_status()
    games = []
    for e in (r.json() or {}).get("events", []):
        comp = TOURNAMENTS.get(e.get("tournament", {}).get("uniqueTournament", {}).get("id"))
        if not comp or not e.get("startTimestamp"):
            continue  # amicale, alte competiții
        games.append({
            "id": e["id"],
            "competition_key": comp,
            "start_utc": datetime.fromtimestamp(e["startTimestamp"], timezone.utc).isoformat(),
            "home": e.get("homeTeam", {}).get("name", ""),
            "away": e.get("awayTeam", {}).get("name", ""),
        })
    return games


def refresh() -> int:
    """Actualizează schedule.json; întoarce câte meciuri viitoare are."""
    upcoming = _fetch_next()
    now = datetime.now(timezone.utc)
    by_id = {g["id"]: g for g in load() if datetime.fromisoformat(g["start_utc"]) > now - KEEP_PAST}
    for g in upcoming:
        by_id[g["id"]] = g  # ora se poate schimba (meci reprogramat)
    # meci care era viitor, dar nu mai apare în listă și n-a început → amânat
    upcoming_ids = {g["id"] for g in upcoming}
    games = sorted(
        (g for g in by_id.values()
         if g["id"] in upcoming_ids or datetime.fromisoformat(g["start_utc"]) <= now),
        key=lambda g: g["start_utc"],
    )
    text = json.dumps({"games": games}, indent=2, ensure_ascii=False) + "\n"
    if not SCHEDULE_PATH.exists() or SCHEDULE_PATH.read_text() != text:
        SCHEDULE_PATH.write_text(text)
    log.info("Program meciuri: %d viitoare → %s", len(upcoming), SCHEDULE_PATH.name)
    return len(upcoming)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    refresh()
