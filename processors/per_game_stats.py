"""
processors/per_game_stats.py — Generează player_stats_per_game_normalized.csv

Citește games.csv, pentru fiecare meci EuroCup fetch-uiește statisticile
per jucător din euroleague_api și salvează incremental.
ABA per-game stats vin din data/raw/aba/player_stats_per_game_{season}.csv
(produs de aba_fetcher.py).

Usage:
    python processors/per_game_stats.py
    python processors/per_game_stats.py --season 2025
    python processors/per_game_stats.py --full
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import DATA_RAW, DATA_PROCESSED, TEAM_IDS

# ABA → EuroCup player_id mapping
_ID_MAP_PATH = ROOT / "config" / "player_id_map.json"
_ABA_TO_EC: dict[int, int] = {}
if _ID_MAP_PATH.exists():
    _ABA_TO_EC = {
        int(k): int(v)
        for k, v in json.loads(_ID_MAP_PATH.read_text()).get("aba_to_eurocup", {}).items()
    }

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUT_PATH     = DATA_PROCESSED / "player_stats_per_game_normalized.csv"
UBT_CLUB     = "CLU"   # codul U-BT în euroleague_api
TEAM_CODE    = "UBT"


# ── EuroCup ────────────────────────────────────────────────────────────────

def process_eurocup(season: int, full: bool) -> pd.DataFrame:
    from euroleague_api.game_stats import GameStats

    games_path = DATA_PROCESSED / "games.csv"
    if not games_path.exists():
        log.warning("games.csv lipsește — skip EuroCup.")
        return pd.DataFrame()

    games = pd.read_csv(games_path)
    ec = games[
        (games["competition"] == "EuroCup") &
        (games["season"] == season) &
        (games["result"].isin(["W", "L"]))
    ].copy()

    if ec.empty:
        log.info("Niciun meci EuroCup terminat în games.csv pentru season=%d", season)
        return pd.DataFrame()

    known_codes: set[int] = set()
    if not full and OUT_PATH.exists():
        existing = pd.read_csv(OUT_PATH, usecols=["competition", "season", "game_code"])
        known = existing[
            (existing["competition"] == "EuroCup") &
            (existing["season"] == season)
        ]
        known_codes = set(known["game_code"].astype(int).tolist())

    todo = ec[~ec["game_code"].astype(int).isin(known_codes)]
    log.info("EuroCup %d — %d meciuri, %d noi de fetch-uit.", season, len(ec), len(todo))

    gs    = GameStats("U")
    rows  = []

    for _, game in todo.iterrows():
        gc       = int(game["game_code"])
        side     = "local" if game["ubt_is_home"] else "road"
        opponent = game["opponent"]
        date_    = game["date"]
        round_   = game.get("round", "")
        result   = game.get("result", "")

        try:
            df_game = gs.get_game_stats(season=season, game_code=gc)
            players_col = f"{side}.players"
            if players_col not in df_game.columns:
                log.warning("  gc=%d: coloana '%s' lipsește — skip", gc, players_col)
                continue

            player_list = df_game[players_col].iloc[0]
            if not isinstance(player_list, list):
                continue

            ubt_players = [p for p in player_list if p["player"]["club"]["code"] == UBT_CLUB]
            log.info("  gc=%d vs %s — %d jucători U-BT", gc, opponent, len(ubt_players))

            for entry in ubt_players:
                person  = entry["player"]["person"]
                stats   = entry["stats"]
                pid     = int(person["code"])
                seconds = stats.get("timePlayed") or 0

                rows.append({
                    "competition":     "EuroCup",
                    "competition_key": "eurocup",
                    "season":          season,
                    "game_code":       gc,
                    "round":           round_,
                    "date":            date_,
                    "opponent":        opponent,
                    "result":          result,
                    "team_code":       TEAM_CODE,
                    "player_id":       pid,
                    "player_name":     _fmt_name(person.get("name", "")),
                    "jersey":          entry["player"].get("dorsal", ""),
                    "is_starter":      stats.get("startFive", False),
                    "seconds_played":  int(seconds),
                    "minutes_played":  round(seconds / 60, 2),
                    "points":          int(stats.get("points") or 0),
                    "fg2_made":        int(stats.get("fieldGoalsMade2") or 0),
                    "fg2_att":         int(stats.get("fieldGoalsAttempted2") or 0),
                    "fg3_made":        int(stats.get("fieldGoalsMade3") or 0),
                    "fg3_att":         int(stats.get("fieldGoalsAttempted3") or 0),
                    "ft_made":         int(stats.get("freeThrowsMade") or 0),
                    "ft_att":          int(stats.get("freeThrowsAttempted") or 0),
                    "rebounds":        int(stats.get("totalRebounds") or 0),
                    "off_rebounds":    int(stats.get("offensiveRebounds") or 0),
                    "def_rebounds":    int(stats.get("defensiveRebounds") or 0),
                    "assists":         int(stats.get("assistances") or 0),
                    "steals":          int(stats.get("steals") or 0),
                    "blocks":          int(stats.get("blocksFavour") or 0),
                    "turnovers":       int(stats.get("turnovers") or 0),
                    "fouls":           int(stats.get("foulsCommited") or 0),
                    "plus_minus":      stats.get("plusMinus"),
                    "pir":             stats.get("valuation"),
                })
        except Exception as exc:
            log.warning("  gc=%d: eroare %s — skip", gc, exc)
            continue

        time.sleep(0.2)

    return pd.DataFrame(rows)


# ── ABA League ─────────────────────────────────────────────────────────────

def process_aba(season: int, full: bool) -> pd.DataFrame:
    raw_path = DATA_RAW / "aba" / f"player_stats_per_game_{season}.csv"
    if not raw_path.exists():
        log.info("ABA per-game raw lipsește (%s) — skip.", raw_path.name)
        return pd.DataFrame()

    df = pd.read_csv(raw_path)
    if df.empty:
        return pd.DataFrame()

    known_codes: set[int] = set()
    if not full and OUT_PATH.exists():
        existing = pd.read_csv(OUT_PATH, usecols=["competition", "season", "game_code"])
        known = existing[
            (existing["competition"] == "ABA League") &
            (existing["season"] == season)
        ]
        known_codes = set(known["game_code"].astype(int).tolist())

    df_new = df[~df["match_id"].astype(int).isin(known_codes)].copy()
    if df_new.empty:
        log.info("ABA %d — toate meciurile deja procesate.", season)
        return pd.DataFrame()

    # Aliniez coloanele cu formatul EuroCup
    df_new = df_new.rename(columns={"match_id": "game_code"})
    df_new["competition"]     = "ABA League"
    df_new["competition_key"] = "aba_league"
    df_new["season"]          = season
    df_new["team_code"]       = TEAM_CODE
    df_new["jersey"]          = df_new.get("jersey_number", "")

    # Calculez VAL (echivalentul PIR în ABA League)
    # VAL = pts + reb + ast + stl + blk − (fga−fgm) − (fta−ftm) − to − pf
    fgm = df_new["fg2_made"].fillna(0) + df_new["fg3_made"].fillna(0)
    fga = df_new["fg2_att"].fillna(0)  + df_new["fg3_att"].fillna(0)
    df_new["pir"] = (
        df_new["points"].fillna(0)
        + df_new["rebounds"].fillna(0)
        + df_new["assists"].fillna(0)
        + df_new["steals"].fillna(0)
        + df_new["blocks"].fillna(0)
        - (fga - fgm)
        - (df_new["ft_att"].fillna(0) - df_new["ft_made"].fillna(0))
        - df_new["turnovers"].fillna(0)
        - df_new["fouls"].fillna(0)
    )

    # Adaug info meci din games.csv dacă există
    games_path = DATA_PROCESSED / "games.csv"
    if games_path.exists():
        games = pd.read_csv(games_path)
        aba_games = games[games["competition"] == "ABA League"][["game_code", "opponent", "result", "round"]].copy()
        aba_games["game_code"] = aba_games["game_code"].astype(int)
        df_new["game_code"] = df_new["game_code"].astype(int)
        df_new = df_new.merge(aba_games, on="game_code", how="left")
    else:
        df_new["opponent"] = ""
        df_new["result"]   = ""
        df_new["round"]    = ""

    # Normalizez numele
    df_new["player_name"] = df_new["player_name"].apply(_fmt_name)

    # Unificare IDs ABA → EuroCup
    if _ABA_TO_EC:
        df_new["player_id"] = df_new["player_id"].apply(
            lambda pid: _ABA_TO_EC.get(int(pid), int(pid)) if pd.notna(pid) else pid
        )

    cols = [
        "competition", "competition_key", "season", "game_code",
        "round", "date", "opponent", "result", "team_code",
        "player_id", "player_name", "jersey", "is_starter",
        "seconds_played", "minutes_played",
        "points", "fg2_made", "fg2_att", "fg3_made", "fg3_att",
        "ft_made", "ft_att",
        "rebounds", "off_rebounds", "def_rebounds",
        "assists", "steals", "blocks", "turnovers", "fouls",
        "plus_minus", "pir",
    ]
    existing_cols = [c for c in cols if c in df_new.columns]
    log.info("ABA %d — %d rânduri noi.", season, len(df_new))
    return df_new[existing_cols]


# ── Liga Națională ─────────────────────────────────────────────────────────

def process_ln(season: int, full: bool) -> pd.DataFrame:
    raw_path = DATA_RAW / "ln" / f"player_stats_per_game_{season}.csv"
    if not raw_path.exists():
        log.info("LN per-game raw lipsește (%s) — skip.", raw_path.name)
        return pd.DataFrame()

    df = pd.read_csv(raw_path)
    if df.empty:
        return pd.DataFrame()

    known_codes: set[int] = set()
    if not full and OUT_PATH.exists():
        existing = pd.read_csv(OUT_PATH, usecols=["competition", "season", "game_code"])
        known = existing[
            (existing["competition"] == "Liga Națională") &
            (existing["season"] == season)
        ]
        known_codes = set(known["game_code"].astype(int).tolist())

    df_new = df[~df["match_id"].astype(int).isin(known_codes)].copy()
    if df_new.empty:
        log.info("LN %d — toate meciurile deja procesate.", season)
        return pd.DataFrame()

    df_new = df_new.rename(columns={"match_id": "game_code"})
    df_new["competition"]     = "Liga Națională"
    df_new["competition_key"] = "liga_nationala"
    df_new["season"]          = season
    df_new["team_code"]       = TEAM_CODE
    df_new["jersey"]          = df_new.get("jersey_number", "")

    # PIR per meci
    fgm = df_new["fg2_made"].fillna(0) + df_new["fg3_made"].fillna(0)
    fga = df_new["fg2_att"].fillna(0)  + df_new["fg3_att"].fillna(0)
    df_new["pir"] = (
        df_new["points"].fillna(0)
        + df_new["rebounds"].fillna(0)
        + df_new["assists"].fillna(0)
        + df_new["steals"].fillna(0)
        + df_new["blocks"].fillna(0)
        - (fga - fgm)
        - (df_new["ft_att"].fillna(0) - df_new["ft_made"].fillna(0))
        - df_new["turnovers"].fillna(0)
        - df_new["fouls"].fillna(0)
    )

    # Info meci din games.csv
    games_path = DATA_PROCESSED / "games.csv"
    if games_path.exists():
        games = pd.read_csv(games_path)
        ln_games = games[games["competition"] == "Liga Națională"][["game_code", "opponent", "result", "round"]].copy()
        ln_games["game_code"] = ln_games["game_code"].astype(int)
        df_new["game_code"] = df_new["game_code"].astype(int)
        df_new = df_new.merge(ln_games, on="game_code", how="left")
    else:
        df_new["opponent"] = ""
        df_new["result"]   = ""
        df_new["round"]    = ""

    df_new["player_name"] = df_new["player_name"].apply(_fmt_name)

    # LN folosește aceleași ID-uri SofaScore ca ABA → aplicăm același map
    if _ABA_TO_EC:
        df_new["player_id"] = df_new["player_id"].apply(
            lambda pid: _ABA_TO_EC.get(int(pid), int(pid)) if pd.notna(pid) else pid
        )

    cols = [
        "competition", "competition_key", "season", "game_code",
        "round", "date", "opponent", "result", "team_code",
        "player_id", "player_name", "jersey", "is_starter",
        "seconds_played", "minutes_played",
        "points", "fg2_made", "fg2_att", "fg3_made", "fg3_att",
        "ft_made", "ft_att",
        "rebounds", "off_rebounds", "def_rebounds",
        "assists", "steals", "blocks", "turnovers", "fouls",
        "plus_minus", "pir",
    ]
    existing_cols = [c for c in cols if c in df_new.columns]
    log.info("LN %d — %d rânduri noi.", season, len(df_new))
    return df_new[existing_cols]


# ── Helpers ────────────────────────────────────────────────────────────────

def _fmt_name(name: str) -> str:
    if not name:
        return ""
    if "," in name and name == name.upper():
        surname, _, first = name.partition(",")
        return f"{first.strip().title()} {surname.strip().title()}"
    return name


# ── Merge & save ───────────────────────────────────────────────────────────

def save(df_new: pd.DataFrame, full: bool) -> None:
    if df_new.empty:
        log.info("Niciun rând nou de adăugat.")
        return

    df_new["date"] = pd.to_datetime(df_new["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    if full or not OUT_PATH.exists():
        df_out = df_new
    else:
        df_existing = pd.read_csv(OUT_PATH)
        # Identificatori unici: competition + season + game_code + player_id
        new_keys = set(zip(df_new["competition"], df_new["season"], df_new["game_code"], df_new["player_id"]))
        mask = df_existing.apply(
            lambda r: (r["competition"], r["season"], r["game_code"], r["player_id"]) not in new_keys,
            axis=1,
        )
        df_out = pd.concat([df_existing[mask], df_new], ignore_index=True)

    df_out.sort_values(["competition_key", "season", "date", "player_id"], inplace=True)
    df_out.reset_index(drop=True, inplace=True)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUT_PATH, index=False)
    log.info("Salvat → %s  (%d rânduri noi, %d total)", OUT_PATH, len(df_new), len(df_out))


# ── Entry point ────────────────────────────────────────────────────────────

def run(season: int, full: bool) -> None:
    log.info("=== Per-Game Stats — season %d %s ===", season, "(full)" if full else "(incremental)")

    ln_seasons = TEAM_IDS["competitions"].get("liga_nationala", {}).get("seasons", {})
    ln_season  = max(int(k) for k in ln_seasons) if ln_seasons else season

    frames = []

    df_ec = process_eurocup(season, full)
    if not df_ec.empty:
        frames.append(df_ec)

    df_aba = process_aba(season, full)
    if not df_aba.empty:
        frames.append(df_aba)

    df_ln = process_ln(ln_season, full)
    if not df_ln.empty:
        frames.append(df_ln)

    if frames:
        save(pd.concat(frames, ignore_index=True), full)
    else:
        log.info("Nicio dată nouă de procesat.")

    log.info("=== Done ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generează statistici per meci normalizate")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--full",   action="store_true", help="Re-procesează tot istoricul")
    args = parser.parse_args()
    run(args.season, args.full)
