"""
fetchers/eurocup_fetcher.py — Fetch U-BT EuroCup data.

Usage:
    python fetchers/eurocup_fetcher.py
    python fetchers/eurocup_fetcher.py --season 2024
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ── Project root ───────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import DATA_RAW, DATA_PROCESSED, TEAM_IDS

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────
COMPETITION       = "U"
TEAM_CODE         = TEAM_IDS["competitions"]["eurocup"]["team_code"]  # "CLU"
COMPETITION_LABEL = "EuroCup"
OUTPUT_RAW        = DATA_RAW / "eurocup"


# ── Games ──────────────────────────────────────────────────────────────────

def fetch_games(season: int) -> pd.DataFrame:
    from euroleague_api.game_metadata import GameMetadata

    log.info(f"Fetching EuroCup {season} game metadata...")
    gm = GameMetadata(COMPETITION)
    df_raw = gm.get_game_metadata_single_season(season=season)
    log.info(f"Total meciuri în sezon: {len(df_raw)}")

    # Save raw
    OUTPUT_RAW.mkdir(parents=True, exist_ok=True)
    df_raw.to_csv(OUTPUT_RAW / f"games_raw_{season}.csv", index=False)

    # Filter U-BT
    mask = (df_raw["CodeTeamA"] == TEAM_CODE) | (df_raw["CodeTeamB"] == TEAM_CODE)
    df_ubt = df_raw[mask].copy()
    log.info(f"Meciuri U-BT: {len(df_ubt)}")

    if df_ubt.empty:
        log.error(f"Nu s-au găsit meciuri pentru TEAM_CODE='{TEAM_CODE}'.")
        log.error(f"Coduri disponibile: {sorted(df_raw['CodeTeamA'].unique().tolist())}")
        return pd.DataFrame()

    return _normalize_games(df_ubt, season)


def _normalize_games(df: pd.DataFrame, season: int) -> pd.DataFrame:
    norm = pd.DataFrame({
        "competition": COMPETITION_LABEL,
        "season":      season,
        "game_code":   df["Gamecode"],
        "round":       df["Round"],
        "date":        pd.to_datetime(df["Date"], format="%d/%m/%Y"),
        "home_team":   df["TeamA"],
        "home_code":   df["CodeTeamA"],
        "away_team":   df["TeamB"],
        "away_code":   df["CodeTeamB"],
        "score_home":  pd.to_numeric(df["ScoreA"], errors="coerce"),
        "score_away":  pd.to_numeric(df["ScoreB"], errors="coerce"),
        "venue":       df["Stadium"],
        "attendance":  pd.to_numeric(df["Capacity"], errors="coerce"),
        "phase":       df["Phase"],
    })

    norm["ubt_is_home"] = norm["home_code"] == TEAM_CODE
    norm["ubt_score"]   = norm.apply(lambda r: r["score_home"] if r["ubt_is_home"] else r["score_away"], axis=1)
    norm["opp_score"]   = norm.apply(lambda r: r["score_away"] if r["ubt_is_home"] else r["score_home"], axis=1)
    norm["opponent"]    = norm.apply(lambda r: r["away_team"] if r["ubt_is_home"] else r["home_team"], axis=1)
    norm["result"]      = norm.apply(lambda r: "W" if r["ubt_score"] > r["opp_score"] else "L", axis=1)
    norm["score_diff"]  = norm["ubt_score"] - norm["opp_score"]
    norm.reset_index(drop=True, inplace=True)

    return norm


# ── Player Stats ───────────────────────────────────────────────────────────

def fetch_player_stats(season: int) -> pd.DataFrame:
    from euroleague_api.player_stats import PlayerStats

    log.info(f"Fetching EuroCup {season} player stats...")
    ps = PlayerStats(COMPETITION)

    # Accumulated — include jucători cu minute puține (confirmat în explorare)
    df_raw = ps.get_player_stats_single_season(
        endpoint="traditional",
        season=season,
        statistic_mode="Accumulated",
    )
    log.info(f"Total jucători în competiție: {len(df_raw)}")

    # Save raw
    df_raw.to_csv(OUTPUT_RAW / f"player_stats_raw_{season}.csv", index=False)

    # Filter U-BT
    df_ubt = df_raw[df_raw["player.team.code"] == TEAM_CODE].copy()
    log.info(f"Jucători U-BT: {len(df_ubt)}")

    if df_ubt.empty:
        log.error(f"Nu s-au găsit jucători pentru TEAM_CODE='{TEAM_CODE}'.")
        return pd.DataFrame()

    return _normalize_players(df_ubt, season)


def _normalize_players(df: pd.DataFrame, season: int) -> pd.DataFrame:
    norm = pd.DataFrame({
        "competition":  COMPETITION_LABEL,
        "season":       season,
        "player_id":    df["player.code"],
        "player_name":  df["player.name"],
        "team_code":    TEAM_CODE,
        "games_played": df["gamesPlayed"],
        "minutes":      df["minutesPlayed"],
        "points":       df["pointsScored"],
        "rebounds":     df["totalRebounds"],
        "off_rebounds": df["offensiveRebounds"],
        "def_rebounds": df["defensiveRebounds"],
        "assists":      df["assists"],
        "steals":       df["steals"],
        "blocks":       df["blocks"],
        "turnovers":    df["turnovers"],
        "fouls":        df["foulsCommited"],
        "fg2_made":     df["twoPointersMade"],
        "fg2_att":      df["twoPointersAttempted"],
        "fg3_made":     df["threePointersMade"],
        "fg3_att":      df["threePointersAttempted"],
        "ft_made":      df["freeThrowsMade"],
        "ft_att":       df["freeThrowsAttempted"],
        "pir":          df["pir"],
        "image_url":    df["player.imageUrl"],
    })

    # Per game averages
    for col in ["points", "rebounds", "assists", "steals", "blocks", "turnovers", "minutes"]:
        norm[f"{col}_pg"] = (norm[col] / norm["games_played"]).round(2)

    # Shooting percentages
    norm["fg2_pct"] = (norm["fg2_made"] / norm["fg2_att"]).round(3)
    norm["fg3_pct"] = (norm["fg3_made"] / norm["fg3_att"]).round(3)
    norm["ft_pct"]  = (norm["ft_made"]  / norm["ft_att"]).round(3)

    norm.reset_index(drop=True, inplace=True)
    return norm


# ── Export ─────────────────────────────────────────────────────────────────

def export(df_games: pd.DataFrame, df_players: pd.DataFrame) -> None:
    """
    Append to existing CSVs or create new ones.
    Removes previous entries for same competition+season before writing.
    """
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    for path, df_new in [
        (DATA_PROCESSED / "games.csv",       df_games),
        (DATA_PROCESSED / "player_stats.csv", df_players),
    ]:
        if df_new.empty:
            log.warning(f"DataFrame gol — skip export {path.name}")
            continue

        if path.exists():
            df_existing = pd.read_csv(path)
            # Remove old data for this competition+season
            mask = ~(
                (df_existing["competition"] == COMPETITION_LABEL) &
                (df_existing["season"] == df_new["season"].iloc[0])
            )
            df_existing = df_existing[mask]
            df_out = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_out = df_new

        df_out.to_csv(path, index=False)
        log.info(f"Exportat → {path}  ({len(df_new)} rânduri noi, {len(df_out)} total)")


def update_last_updated(season: int) -> None:
    path = DATA_PROCESSED / "last_updated.json"
    data = json.loads(path.read_text()) if path.exists() else {}
    data["last_updated"] = datetime.now(timezone.utc).isoformat()
    data.setdefault("competitions", {})
    data["competitions"]["eurocup"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(data, indent=2))


# ── Entry point ────────────────────────────────────────────────────────────

def run(season: int) -> None:
    log.info(f"=== EuroCup Fetcher — season {season} ===")

    df_games   = fetch_games(season)
    df_players = fetch_player_stats(season)

    if not df_games.empty and not df_players.empty:
        export(df_games, df_players)
        update_last_updated(season)
        log.info("=== Done ===")
    else:
        log.error("Fetch incomplet — nu s-a exportat nimic.")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch U-BT EuroCup data")
    parser.add_argument("--season", type=int, default=2025,
                        help="Season year (default: 2025)")
    args = parser.parse_args()
    run(args.season)
