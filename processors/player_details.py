"""
processors/player_details.py — Generează player_details.csv

Extrage naționalitate + an naștere pentru jucătorii U-BT din:
  - EuroCup: euroleague_api GameStats (person.country + person.birthDate)
  - ABA League: basketapi1 /api/basketball/player/{id}

Output: data/processed/player_details.csv
  player_id, nationality_code, nationality_name, birth_year

Usage:
    python processors/player_details.py
    python processors/player_details.py --full
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import (
    DATA_RAW, DATA_PROCESSED,
    RAPIDAPI_KEY, RAPIDAPI_HOST_BASKETBALL, RAPIDAPI_BASE_BASKETBALL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUT_PATH = DATA_PROCESSED / "player_details.csv"
UBT_CLUB = "CLU"

HEADERS = {
    "x-rapidapi-key":  RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST_BASKETBALL,
}


# ── EuroCup ────────────────────────────────────────────────────────────────

def fetch_eurocup_details(season: int) -> dict[int, dict]:
    """Extrage din statisticile per meci EuroCup deja salvate."""
    from euroleague_api.game_stats import GameStats

    per_game = DATA_PROCESSED / "player_stats_per_game_normalized.csv"
    if not per_game.exists():
        log.warning("player_stats_per_game_normalized.csv lipsește — skip EuroCup.")
        return {}

    df = pd.read_csv(per_game)
    ec = df[df["competition"] == "EuroCup"]
    if ec.empty:
        return {}

    # Ia primul game_code disponibil — conține toți jucătorii U-BT din acel meci
    # Iterăm câteva meciuri ca să acoperim toți jucătorii din lot
    game_codes = ec["game_code"].unique().tolist()
    gs = GameStats("U")
    details: dict[int, dict] = {}

    for gc in game_codes[:10]:  # primele 10 meciuri acoperă de obicei tot lotul
        try:
            df_game = gs.get_game_stats(season=season, game_code=int(gc))
            for side in ("local.players", "road.players"):
                if side not in df_game.columns:
                    continue
                player_list = df_game[side].iloc[0]
                if not isinstance(player_list, list):
                    continue
                for entry in player_list:
                    if entry["player"]["club"]["code"] != UBT_CLUB:
                        continue
                    person = entry["player"]["person"]
                    pid = int(person["code"])
                    if pid in details:
                        continue
                    country = person.get("country") or {}
                    birth = person.get("birthDate", "")[:4]
                    details[pid] = {
                        "player_id":        pid,
                        "nationality_code":  country.get("code", ""),
                        "nationality_name":  country.get("name", ""),
                        "birth_year":        int(birth) if birth.isdigit() else None,
                    }
        except Exception as exc:
            log.warning("gc=%s: %s — skip", gc, exc)
        time.sleep(0.2)

        if len(details) >= 20:  # lot complet acoperit
            break

    log.info("EuroCup: %d jucători cu detalii.", len(details))
    return details


# ── ABA League ─────────────────────────────────────────────────────────────

def fetch_aba_details(season: int) -> dict[int, dict]:
    raw_path = DATA_RAW / "aba" / f"player_stats_per_game_{season}.csv"
    if not raw_path.exists():
        log.warning("ABA per-game raw lipsește — skip ABA.")
        return {}

    df = pd.read_csv(raw_path, usecols=["player_id"])
    player_ids = df["player_id"].dropna().unique().tolist()
    log.info("ABA: %d jucători unici de fetch-uit.", len(player_ids))

    details: dict[int, dict] = {}
    for pid in player_ids:
        pid = int(pid)
        try:
            r = requests.get(
                f"{RAPIDAPI_BASE_BASKETBALL}/api/basketball/player/{pid}",
                headers=HEADERS, timeout=20,
            )
            if r.status_code == 429:
                remaining = r.headers.get("X-RateLimit-Requests-Remaining", "?")
                reset_in  = r.headers.get("X-RateLimit-Requests-Reset", "?")
                if remaining == "0":
                    raise RuntimeError(f"Quota epuizată. Reset în {reset_in}s.")
                log.warning("Rate-limited — retry în 30s")
                time.sleep(30)
                r = requests.get(
                    f"{RAPIDAPI_BASE_BASKETBALL}/api/basketball/player/{pid}",
                    headers=HEADERS, timeout=20,
                )
            r.raise_for_status()
            player = r.json().get("player", {})
            country = player.get("country") or {}
            dob = player.get("dateOfBirth", "")[:4]
            details[pid] = {
                "player_id":        pid,
                "nationality_code":  country.get("alpha3") or country.get("alpha2", ""),
                "nationality_name":  country.get("name", ""),
                "birth_year":        int(dob) if dob.isdigit() else None,
            }
            log.debug("  %d: %s, %s", pid, country.get("name", ""), dob)
        except Exception as exc:
            log.warning("  player_id=%d: %s — skip", pid, exc)
        time.sleep(1)

    log.info("ABA: %d jucători cu detalii.", len(details))
    return details


# ── Save ───────────────────────────────────────────────────────────────────

def save(details: dict[int, dict], full: bool) -> None:
    if not details:
        log.info("Nicio dată nouă.")
        return

    df_new = pd.DataFrame(list(details.values()))

    if not full and OUT_PATH.exists():
        df_existing = pd.read_csv(OUT_PATH)
        known = set(df_existing["player_id"].tolist())
        novel = df_new[~df_new["player_id"].isin(known)]
        df_out = pd.concat([df_existing, novel], ignore_index=True)
    else:
        df_out = df_new

    df_out.drop_duplicates(subset=["player_id"], keep="last", inplace=True)
    df_out.sort_values("player_id", inplace=True)
    df_out.reset_index(drop=True, inplace=True)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUT_PATH, index=False)
    log.info("Salvat → %s  (%d jucători total)", OUT_PATH, len(df_out))


# ── Entry point ────────────────────────────────────────────────────────────

def run(season: int, full: bool) -> None:
    log.info("=== Player Details — season %d %s ===", season, "(full)" if full else "(incremental)")

    all_details: dict[int, dict] = {}

    ec = fetch_eurocup_details(season)
    all_details.update(ec)

    aba = fetch_aba_details(season)
    # ABA poate suprascrie dacă același jucător e în ambele (mai puțin probabil)
    for pid, d in aba.items():
        if pid not in all_details:
            all_details[pid] = d

    save(all_details, full)
    log.info("=== Done ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrage detalii jucători (vârstă, naționalitate)")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--full", action="store_true", help="Re-fetch complet")
    args = parser.parse_args()
    run(args.season, args.full)
