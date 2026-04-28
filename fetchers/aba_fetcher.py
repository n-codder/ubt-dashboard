"""
fetchers/aba_fetcher.py — Fetch U-BT ABA League data via basketapi1 (RapidAPI).

Incremental: prima rulare aduce tot istoricul sezonului; rulările ulterioare
aduc numai meciurile noi față de ultimul match_id deja salvat.

Usage:
    python fetchers/aba_fetcher.py
    python fetchers/aba_fetcher.py --season 2025
    python fetchers/aba_fetcher.py --season 2025 --full
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

# ── Project root ───────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import (
    DATA_RAW, DATA_PROCESSED, TEAM_IDS,
    RAPIDAPI_KEY, RAPIDAPI_HOST_BASKETBALL, RAPIDAPI_BASE_BASKETBALL,
)

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────
COMPETITION_LABEL = "ABA League"
ABA_CFG           = TEAM_IDS["competitions"]["aba_league"]
TEAM_ID           = ABA_CFG["team_id"]       # 59003
TOURNAMENT_ID     = ABA_CFG["tournament_id"] # 235
OUTPUT_RAW        = DATA_RAW / "aba"

HEADERS = {
    "x-rapidapi-key":  RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST_BASKETBALL,
}

_SOFASCORE_IMAGE = "https://api.sofascore.app/api/v1/player/{id}/image"


# ── HTTP helper ────────────────────────────────────────────────────────────

def _get(path: str, retries: int = 3) -> dict:
    url = f"{RAPIDAPI_BASE_BASKETBALL}{path}"
    rate_wait = 30
    attempt   = 0
    while True:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 429:
                remaining = r.headers.get("X-RateLimit-Requests-Remaining", "?")
                reset_in  = r.headers.get("X-RateLimit-Requests-Reset", "?")
                if remaining == "0":
                    raise RuntimeError(
                        f"Quota RapidAPI epuizată. Reset în {reset_in}s "
                        f"(~{int(reset_in)//60} min). Rulează din nou după reset."
                    )
                log.warning(f"Rate limited — retry în {rate_wait}s")
                time.sleep(rate_wait)
                rate_wait = min(rate_wait * 2, 120)
                continue
            r.raise_for_status()
            if not r.content:
                return {}
            return r.json()
        except requests.RequestException as exc:
            attempt += 1
            if attempt >= retries:
                raise
            log.warning(f"Eroare {exc} — retry {attempt}/{retries}")
            time.sleep(2)


# ── Season helpers ─────────────────────────────────────────────────────────

def _season_id(season: int) -> int:
    seasons = ABA_CFG.get("seasons", {})
    sid = seasons.get(str(season))
    if sid is None:
        raise ValueError(
            f"season_id necunoscut pentru season={season}. "
            f"Adaugă-l în config/team_ids.json → competitions.aba_league.seasons"
        )
    return sid


# ── Incremental state ──────────────────────────────────────────────────────

def _known_match_ids(season: int) -> set[int]:
    """Match-uri deja salvate în raw games JSON pentru sezonul dat."""
    raw_path = OUTPUT_RAW / f"games_raw_{season}.json"
    if not raw_path.exists():
        return set()
    events = json.loads(raw_path.read_text())
    return {e["id"] for e in events}


def _known_stats_match_ids(season: int) -> set[int]:
    """Match-uri pentru care avem deja statistici per jucător."""
    raw_path = OUTPUT_RAW / f"player_stats_per_game_{season}.csv"
    if not raw_path.exists():
        return set()
    df = pd.read_csv(raw_path, usecols=["match_id"])
    return set(df["match_id"].tolist())


# ── Games ──────────────────────────────────────────────────────────────────

def fetch_games(season: int, full: bool = False) -> pd.DataFrame:
    sid = _season_id(season)
    known = set() if full else _known_match_ids(season)
    log.info(
        f"Fetching ABA {season} meciuri — "
        f"{'full re-fetch' if full else f'{len(known)} meciuri deja salvate'}"
    )

    new_events = _fetch_new_matches(sid, known)

    if not new_events:
        log.info("Niciun meci nou — încarc din raw JSON.")
        all_events = _load_raw_games(season)
        return _normalize_games(all_events, season)

    log.info(f"{len(new_events)} meciuri noi găsite.")
    _save_raw_games(season, new_events, overwrite=full)

    all_events = _load_raw_games(season)
    return _normalize_games(all_events, season)


def _fetch_new_matches(season_id: int, known_ids: set[int]) -> list[dict]:
    """
    Paginează /matches/previous/N (cele mai recente primul).
    Se oprește când o pagină întreagă e deja cunoscută sau epuizată.
    """
    new_events = []
    page = 0
    while True:
        data   = _get(f"/api/basketball/team/{TEAM_ID}/matches/previous/{page}")
        events = data.get("events", [])
        if not events:
            break

        aba = [
            e for e in events
            if e.get("tournament", {}).get("uniqueTournament", {}).get("id") == TOURNAMENT_ID
            and e.get("season", {}).get("id") == season_id
            and _is_finished(e)
        ]
        novel = [e for e in aba if e["id"] not in known_ids]
        new_events.extend(novel)

        log.debug(f"  page {page}: {len(events)} total, {len(aba)} ABA, {len(novel)} noi")

        # Dacă toată pagina ABA e deja cunoscută, nu mai avem ce căuta
        if aba and not novel:
            break
        # Dacă pagina nu conținea deloc ABA și avem deja date, nu continuăm
        if not aba and known_ids:
            break

        page += 1
        time.sleep(1)

    return new_events


def _is_finished(event: dict) -> bool:
    status = event.get("status", {})
    return status.get("type") == "finished" or status.get("description") == "Ended"


def _save_raw_games(season: int, new_events: list[dict], overwrite: bool) -> None:
    OUTPUT_RAW.mkdir(parents=True, exist_ok=True)
    raw_path = OUTPUT_RAW / f"games_raw_{season}.json"

    if overwrite or not raw_path.exists():
        existing = []
    else:
        existing = json.loads(raw_path.read_text())

    existing_ids = {e["id"] for e in existing}
    merged = existing + [e for e in new_events if e["id"] not in existing_ids]
    raw_path.write_text(json.dumps(merged, indent=2))
    log.info(f"Raw games salvat → {raw_path}  ({len(merged)} total)")


def _load_raw_games(season: int) -> list[dict]:
    return json.loads((OUTPUT_RAW / f"games_raw_{season}.json").read_text())


def _load_existing_games(season: int) -> pd.DataFrame:
    path = DATA_PROCESSED / "games.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    mask = (df["competition"] == COMPETITION_LABEL) & (df["season"] == season)
    return df[mask].copy()


def _normalize_games(events: list[dict], season: int) -> pd.DataFrame:
    rows = []
    for e in events:
        home_id    = e["homeTeam"]["id"]
        ubt_home   = home_id == TEAM_ID
        home_score = e.get("homeScore", {}).get("current")
        away_score = e.get("awayScore", {}).get("current")
        ubt_score  = home_score if ubt_home else away_score
        opp_score  = away_score if ubt_home else home_score
        opponent   = e["awayTeam"]["name"] if ubt_home else e["homeTeam"]["name"]

        ts   = e.get("startTimestamp")
        date = datetime.fromtimestamp(ts, tz=timezone.utc).date() if ts else None

        has_score = ubt_score is not None and opp_score is not None
        rows.append({
            "competition": COMPETITION_LABEL,
            "season":      season,
            "game_code":   str(e["id"]),
            "round":       e.get("roundInfo", {}).get("round"),
            "date":        date,
            "home_team":   e["homeTeam"]["name"],
            "home_code":   e["homeTeam"].get("slug", ""),
            "away_team":   e["awayTeam"]["name"],
            "away_code":   e["awayTeam"].get("slug", ""),
            "score_home":  home_score,
            "score_away":  away_score,
            "venue":       e.get("venue", {}).get("name"),
            "attendance":  e.get("venue", {}).get("stadium", {}).get("capacity"),
            "phase":       "Regular Season",
            "ubt_is_home": ubt_home,
            "ubt_score":   ubt_score,
            "opp_score":   opp_score,
            "opponent":    opponent,
            "result":      ("W" if ubt_score > opp_score else "L") if has_score else None,
            "score_diff":  (ubt_score - opp_score) if has_score else None,
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ── Player Stats ───────────────────────────────────────────────────────────

def fetch_player_stats(season: int, df_games: pd.DataFrame, full: bool = False) -> pd.DataFrame:
    finished = df_games[df_games["result"].isin(["W", "L"])].copy()

    known_stats = set() if full else _known_stats_match_ids(season)
    new_games   = finished[~finished["game_code"].astype(int).isin(known_stats)]

    log.info(
        f"Player stats — {len(finished)} meciuri terminate, "
        f"{len(known_stats)} deja în cache, {len(new_games)} de fetch-uit."
    )

    new_rows = []
    for _, game in new_games.iterrows():
        match_id = int(game["game_code"])
        side     = "home" if game["ubt_is_home"] else "away"
        log.info(f"  Lineups {match_id} — {game['home_team']} vs {game['away_team']} ({game['date'].date()})")

        try:
            data = _get(f"/api/basketball/match/{match_id}/lineups")
        except Exception as exc:
            log.warning(f"  Skip {match_id}: {exc}")
            continue

        for p in data.get(side, {}).get("players", []):
            player = p.get("player", {})
            stats  = p.get("statistics", {})
            if not stats:
                continue
            seconds = stats.get("secondsPlayed", 0)
            new_rows.append({
                "competition":   COMPETITION_LABEL,
                "season":        season,
                "match_id":      match_id,
                "date":          game["date"],
                "player_id":     player.get("id"),
                "player_name":   player.get("name"),
                "player_slug":   player.get("slug"),
                "position":      p.get("position"),
                "jersey_number": p.get("jerseyNumber"),
                "is_starter":    not p.get("substitute", True),
                "seconds_played": seconds,
                "minutes_played": round(seconds / 60, 2),
                "points":         stats.get("points", 0),
                "fg2_made":       stats.get("twoPointsMade", 0),
                "fg2_att":        stats.get("twoPointAttempts", 0),
                "fg3_made":       stats.get("threePointsMade", 0),
                "fg3_att":        stats.get("threePointAttempts", 0),
                "ft_made":        stats.get("freeThrowsMade", 0),
                "ft_att":         stats.get("freeThrowAttempts", 0),
                "rebounds":       stats.get("rebounds", 0),
                "off_rebounds":   stats.get("offensiveRebounds", 0),
                "def_rebounds":   stats.get("defensiveRebounds", 0),
                "assists":        stats.get("assists", 0),
                "steals":         stats.get("steals", 0),
                "blocks":         stats.get("blocks", 0),
                "turnovers":      stats.get("turnovers", 0),
                "fouls":          stats.get("personalFouls", 0),
                "plus_minus":     stats.get("plusMinus"),
            })
        time.sleep(1)

    _append_raw_stats(season, new_rows, overwrite=full)

    df_per_game = _load_raw_stats(season)
    if df_per_game.empty:
        log.error("Nu există statistici per jucător salvate.")
        return pd.DataFrame()

    return _aggregate_player_stats(df_per_game, season)


def _append_raw_stats(season: int, new_rows: list[dict], overwrite: bool) -> None:
    OUTPUT_RAW.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_RAW / f"player_stats_per_game_{season}.csv"

    if new_rows:
        df_new = pd.DataFrame(new_rows)
        if overwrite or not path.exists():
            df_new.to_csv(path, index=False)
        else:
            df_existing = pd.read_csv(path)
            known = set(df_existing["match_id"].tolist())
            df_novel = df_new[~df_new["match_id"].isin(known)]
            pd.concat([df_existing, df_novel], ignore_index=True).to_csv(path, index=False)
        log.info(f"Raw stats salvat → {path}  ({len(new_rows)} rânduri noi)")
    elif not path.exists():
        log.warning("Niciun rând nou și nu există fișier raw stats.")


def _load_raw_stats(season: int) -> pd.DataFrame:
    path = OUTPUT_RAW / f"player_stats_per_game_{season}.csv"
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _aggregate_player_stats(df: pd.DataFrame, season: int) -> pd.DataFrame:
    grp = df.groupby(
        ["competition", "season", "player_id", "player_name", "player_slug", "position"],
        dropna=False,
    )
    agg = grp.agg(
        games_played  = ("match_id", "count"),
        minutes       = ("minutes_played", "sum"),
        points        = ("points", "sum"),
        fg2_made      = ("fg2_made", "sum"),
        fg2_att       = ("fg2_att", "sum"),
        fg3_made      = ("fg3_made", "sum"),
        fg3_att       = ("fg3_att", "sum"),
        ft_made       = ("ft_made", "sum"),
        ft_att        = ("ft_att", "sum"),
        rebounds      = ("rebounds", "sum"),
        off_rebounds  = ("off_rebounds", "sum"),
        def_rebounds  = ("def_rebounds", "sum"),
        assists       = ("assists", "sum"),
        steals        = ("steals", "sum"),
        blocks        = ("blocks", "sum"),
        turnovers     = ("turnovers", "sum"),
        fouls         = ("fouls", "sum"),
    ).reset_index()

    agg["team_code"] = "UBT"

    # VAL (echivalentul PIR în ABA League)
    # VAL = pts + reb + ast + stl + blk − (fga−fgm) − (fta−ftm) − to − pf
    fgm = agg["fg2_made"] + agg["fg3_made"]
    fga = agg["fg2_att"]  + agg["fg3_att"]
    val_total = (
        agg["points"] + agg["rebounds"] + agg["assists"]
        + agg["steals"] + agg["blocks"]
        - (fga - fgm)
        - (agg["ft_att"] - agg["ft_made"])
        - agg["turnovers"] - agg["fouls"]
    )
    agg["pir"] = val_total.round(1)   # total sezon, ca EuroCup
    agg["image_url"] = agg["player_id"].apply(
        lambda pid: _SOFASCORE_IMAGE.format(id=pid) if pd.notna(pid) else None
    )

    for col in ["points", "rebounds", "assists", "steals", "blocks", "turnovers", "minutes"]:
        agg[f"{col}_pg"] = (agg[col] / agg["games_played"]).round(2)

    agg["fg2_pct"] = (agg["fg2_made"] / agg["fg2_att"].replace(0, pd.NA)).round(3)
    agg["fg3_pct"] = (agg["fg3_made"] / agg["fg3_att"].replace(0, pd.NA)).round(3)
    agg["ft_pct"]  = (agg["ft_made"]  / agg["ft_att"].replace(0, pd.NA)).round(3)

    agg.sort_values("points", ascending=False, inplace=True)
    agg.reset_index(drop=True, inplace=True)
    return agg


# ── Export ─────────────────────────────────────────────────────────────────

def export(df_games: pd.DataFrame, df_players: pd.DataFrame, season: int) -> None:
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    for path, df_new in [
        (DATA_PROCESSED / "games.csv",        df_games),
        (DATA_PROCESSED / "player_stats.csv", df_players),
    ]:
        if df_new.empty:
            log.warning(f"DataFrame gol — skip export {path.name}")
            continue

        if path.exists():
            df_existing = pd.read_csv(path)
            mask = ~(
                (df_existing["competition"] == COMPETITION_LABEL) &
                (df_existing["season"]      == season)
            )
            df_out = pd.concat([df_existing[mask], df_new], ignore_index=True)
        else:
            df_out = df_new

        df_out.to_csv(path, index=False)
        log.info(f"Exportat → {path}  ({len(df_new)} rânduri noi, {len(df_out)} total)")


def _update_last_updated(season: int) -> None:
    path = DATA_PROCESSED / "last_updated.json"
    data = json.loads(path.read_text()) if path.exists() else {}
    now  = datetime.now(timezone.utc).isoformat()
    data["last_updated"] = now
    data.setdefault("competitions", {})
    data["competitions"]["aba_league"] = now
    path.write_text(json.dumps(data, indent=2))


# ── Entry point ────────────────────────────────────────────────────────────

def run(season: int, full: bool = False) -> None:
    log.info(f"=== ABA League Fetcher — season {season} {'(full)' if full else '(incremental)'} ===")

    if not RAPIDAPI_KEY:
        log.error("RAPIDAPI_KEY lipsește din .env")
        sys.exit(1)

    df_games = fetch_games(season, full=full)
    if df_games.empty:
        log.error("Niciun meci obținut — abort.")
        sys.exit(1)
    log.info(f"Meciuri: {len(df_games)} total, {df_games['result'].notna().sum()} terminate.")

    df_players = fetch_player_stats(season, df_games, full=full)
    if df_players.empty:
        log.error("Statistici incomplete — nu s-a exportat nimic.")
        sys.exit(1)

    export(df_games, df_players, season)
    _update_last_updated(season)
    log.info("=== Done ===")


if __name__ == "__main__":
    default_season = max(int(k) for k in ABA_CFG["seasons"])
    parser = argparse.ArgumentParser(description="Fetch U-BT ABA League data")
    parser.add_argument(
        "--season", type=int, default=default_season,
        help=f"Anul de start al sezonului (default: {default_season})",
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Forțează re-fetch complet ignorând cache-ul local",
    )
    args = parser.parse_args()
    run(args.season, full=args.full)
