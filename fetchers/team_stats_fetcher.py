"""
fetchers/team_stats_fetcher.py — Fetch U-BT team-level box-score stats per game.

Surse:
  - EuroCup : euroleague_api GameStats('U').get_game_stats(season, game_code)
  - ABA     : Sofascore /api/basketball/match/{match_id}/statistics
  - LN      : date indisponibile (skip)

Rulare:
    python fetchers/team_stats_fetcher.py
    python fetchers/team_stats_fetcher.py --season 2025
    python fetchers/team_stats_fetcher.py --full
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
    DATA_PROCESSED,
    RAPIDAPI_KEY, RAPIDAPI_HOST_BASKETBALL, RAPIDAPI_BASE_BASKETBALL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT_PATH = DATA_PROCESSED / "game_stats.csv"

STAT_COLS = [
    "pts", "pir",
    "fg2m", "fg2a", "fg3m", "fg3a", "ftm", "fta",
    "oreb", "dreb", "reb",
    "ast", "stl", "blk", "to", "fouls",
]


def _ubt_opp_prefix(ubt_is_home: bool):
    return ("ubt_", "opp_") if ubt_is_home else ("opp_", "ubt_")


# ── EuroCup ────────────────────────────────────────────────────────────────

def _fetch_eurocup_game(season: int, game_code: int, ubt_is_home: bool) -> dict | None:
    try:
        from euroleague_api.game_stats import GameStats
        gs = GameStats("U")
        df = gs.get_game_stats(season, game_code)
    except Exception as exc:
        log.warning(f"  EuroCup game_stats({season}, {game_code}): {exc}")
        return None

    if df.empty:
        return None

    row = df.iloc[0]

    def _get(side: str, col: str, default=None):
        # .total. = accumulated game totals; .team. = single quarter only
        key = f"{side}.total.{col}"
        return row.get(key, default)

    local = {
        "pts":   _get("local", "points"),
        "pir":   _get("local", "valuation"),
        "fg2m":  _get("local", "fieldGoalsMade2"),
        "fg2a":  _get("local", "fieldGoalsAttempted2"),
        "fg3m":  _get("local", "fieldGoalsMade3"),
        "fg3a":  _get("local", "fieldGoalsAttempted3"),
        "ftm":   _get("local", "freeThrowsMade"),
        "fta":   _get("local", "freeThrowsAttempted"),
        "oreb":  _get("local", "offensiveRebounds"),
        "dreb":  _get("local", "defensiveRebounds"),
        "reb":   _get("local", "totalRebounds"),
        "ast":   _get("local", "assistances"),
        "stl":   _get("local", "steals"),
        "blk":   _get("local", "blocksFavour"),
        "to":    _get("local", "turnovers"),
        "fouls": _get("local", "foulsCommited"),
    }
    road = {
        "pts":   _get("road", "points"),
        "pir":   _get("road", "valuation"),
        "fg2m":  _get("road", "fieldGoalsMade2"),
        "fg2a":  _get("road", "fieldGoalsAttempted2"),
        "fg3m":  _get("road", "fieldGoalsMade3"),
        "fg3a":  _get("road", "fieldGoalsAttempted3"),
        "ftm":   _get("road", "freeThrowsMade"),
        "fta":   _get("road", "freeThrowsAttempted"),
        "oreb":  _get("road", "offensiveRebounds"),
        "dreb":  _get("road", "defensiveRebounds"),
        "reb":   _get("road", "totalRebounds"),
        "ast":   _get("road", "assistances"),
        "stl":   _get("road", "steals"),
        "blk":   _get("road", "blocksFavour"),
        "to":    _get("road", "turnovers"),
        "fouls": _get("road", "foulsCommited"),
    }

    ubt_stats  = local if ubt_is_home else road
    opp_stats  = road  if ubt_is_home else local

    result = {}
    for col in STAT_COLS:
        result[f"ubt_{col}"] = ubt_stats[col]
        result[f"opp_{col}"] = opp_stats[col]
    return result


# ── ABA ─────────────────────────────────────────────────────────────────────

def _aba_get(path: str) -> dict:
    url = f"{RAPIDAPI_BASE_BASKETBALL}{path}"
    r = requests.get(url, headers={
        "x-rapidapi-key":  RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST_BASKETBALL,
    }, timeout=15)
    r.raise_for_status()
    return r.json()


def _aggregate_lineups(players: list) -> dict:
    """Sum player stats from lineups endpoint. Note: team rebounds not assigned to players."""
    t: dict = {}
    for p in players:
        for k, v in p.get("statistics", {}).items():
            if isinstance(v, (int, float)):
                t[k] = t.get(k, 0) + v
    return dict(
        pts=t.get("points"),
        pir=None,  # not available from lineups
        fg2m=t.get("twoPointsMade"),
        fg2a=t.get("twoPointAttempts"),
        fg3m=t.get("threePointsMade"),
        fg3a=t.get("threePointAttempts"),
        ftm=t.get("freeThrowsMade"),
        fta=t.get("freeThrowAttempts"),
        oreb=t.get("offensiveRebounds"),
        dreb=t.get("defensiveRebounds"),
        reb=t.get("rebounds"),
        ast=t.get("assists"),
        stl=t.get("steals"),
        blk=t.get("blocks"),
        to=t.get("turnovers"),
        fouls=t.get("personalFouls"),
    )


def _fetch_aba_game(match_id: int, ubt_is_home: bool) -> dict | None:
    try:
        data = _aba_get(f"/api/basketball/match/{match_id}/lineups")
    except Exception as exc:
        log.warning(f"  ABA lineups({match_id}): {exc}")
        return None

    ubt_side = "home" if ubt_is_home else "away"
    opp_side = "away" if ubt_is_home else "home"

    ubt_stats = _aggregate_lineups(data.get(ubt_side, {}).get("players", []))
    opp_stats = _aggregate_lineups(data.get(opp_side, {}).get("players", []))

    result = {}
    for col in STAT_COLS:
        result[f"ubt_{col}"] = ubt_stats[col]
        result[f"opp_{col}"] = opp_stats[col]
    return result


# ── Main ─────────────────────────────────────────────────────────────────────

def fetch_team_stats(season: int | None = None, full: bool = False) -> pd.DataFrame:
    games_path = DATA_PROCESSED / "games.csv"
    if not games_path.exists():
        log.error(f"games.csv lipsă: {games_path}")
        return pd.DataFrame()

    df_games = pd.read_csv(games_path)
    df_games = df_games[df_games["result"].isin(["W", "L"])].copy()

    if season is not None:
        df_games = df_games[df_games["season"].astype(int) == season]

    if df_games.empty:
        log.info("Niciun meci terminat de procesat.")
        return pd.DataFrame()

    # Determine which game_codes already exist
    existing_keys: set = set()
    if OUTPUT_PATH.exists() and not full:
        df_existing = pd.read_csv(OUTPUT_PATH)
        existing_keys = set(zip(df_existing["game_code"].astype(str), df_existing["season"].astype(str), df_existing["competition"]))
        log.info(f"game_stats.csv existent: {len(df_existing)} rânduri, {len(existing_keys)} chei.")

    new_rows = []
    for _, game in df_games.iterrows():
        comp       = game["competition"]
        gc         = str(game["game_code"])
        key        = (gc, str(game["season"]), comp)
        ubt_home   = bool(game["ubt_is_home"]) if isinstance(game["ubt_is_home"], bool) else str(game["ubt_is_home"]).lower() == "true"

        if key in existing_keys:
            continue

        log.info(f"  {comp} gc={gc}  {game.get('home_team','')} vs {game.get('away_team','')} ({game.get('date','')})")

        stats = None
        if comp == "EuroCup":
            stats = _fetch_eurocup_game(int(game["season"]), int(gc), ubt_home)
            time.sleep(0.5)
        elif comp == "ABA League":
            stats = _fetch_aba_game(int(gc), ubt_home)
            time.sleep(2)
        else:
            log.debug(f"  Skip {comp} — date echipă indisponibile")
            continue

        if stats is None:
            log.warning(f"  Nu s-au obținut date pentru {comp} gc={gc}")
            continue

        row = {
            "competition": comp,
            "season":      int(game["season"]),
            "game_code":   gc,
            "date":        game.get("date", ""),
            "opponent":    game.get("opponent", ""),
            "ubt_is_home": ubt_home,
            "result":      game.get("result", ""),
        }
        row.update(stats)
        new_rows.append(row)

    if not new_rows:
        log.info("Niciun rând nou de adăugat.")
        if OUTPUT_PATH.exists():
            return pd.read_csv(OUTPUT_PATH)
        return pd.DataFrame()

    df_new = pd.DataFrame(new_rows)
    cols = ["competition", "season", "game_code", "date", "opponent", "ubt_is_home", "result"]
    for col in STAT_COLS:
        cols += [f"ubt_{col}", f"opp_{col}"]
    df_new = df_new[cols]

    if OUTPUT_PATH.exists() and not full:
        df_existing = pd.read_csv(OUTPUT_PATH)
        df_out = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_out = df_new

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUTPUT_PATH, index=False)
    log.info(f"Exportat → {OUTPUT_PATH}  ({len(df_new)} rânduri noi, {len(df_out)} total)")
    return df_out


run = fetch_team_stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch team box-score stats per game")
    parser.add_argument("--season", type=int, default=None,
                        help="Sezonul de procesat (default: toate)")
    parser.add_argument("--full", action="store_true",
                        help="Re-fetch complet, suprascrie game_stats.csv")
    args = parser.parse_args()
    fetch_team_stats(season=args.season, full=args.full)
