"""
processors/normalizer.py — Standardizează games.csv și player_stats.csv.

Operații aplicate:
  - Adaugă competition_key (eurocup / aba_league / liga_nationala / cupa_romaniei)
  - Normalizează numele echipelor (variante multiple → formă canonică)
  - Normalizează numele jucătorilor ("ALTIT, BEN" → "Ben Altit")
  - Standardizează tipurile coloanelor (date, numeric, string)
  - Salvează data/processed/games_normalized.csv și player_stats_normalized.csv

Usage:
    python processors/normalizer.py
    python processors/normalizer.py --games-only
    python processors/normalizer.py --players-only
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

# ── Project root ───────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import DATA_PROCESSED

# ── Player ID unification map (ABA basketapi1 → EuroCup euroleague_api) ───
_ID_MAP_PATH = ROOT / "config" / "player_id_map.json"
_ABA_TO_EC: dict[int, int] = {}
if _ID_MAP_PATH.exists():
    _ABA_TO_EC = {
        int(k): int(v)
        for k, v in json.loads(_ID_MAP_PATH.read_text()).get("aba_to_eurocup", {}).items()
    }

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Competition key map ────────────────────────────────────────────────────
COMPETITION_KEY: dict[str, str] = {
    "EuroCup":          "eurocup",
    "Eurocup":          "eurocup",
    "EUROCUP":          "eurocup",
    "ABA League":       "aba_league",
    "ABA LEAGUE":       "aba_league",
    "Liga Națională":   "liga_nationala",
    "Liga Nationala":   "liga_nationala",
    "LIGA NATIONALA":   "liga_nationala",
    "Cupa României":    "cupa_romaniei",
    "Cupa Romaniei":    "cupa_romaniei",
    "CUPA ROMANIEI":    "cupa_romaniei",
}

# ── Canonical team names ───────────────────────────────────────────────────
# Keys: all known variants (exact match, case-sensitive stored here as lowered at lookup)
TEAM_CANONICAL: dict[str, str] = {
    # U-BT — toate variantele
    "u-bt cluj-napoca":                              "U-BT Cluj-Napoca",
    "u-banca transilvania cluj-napoca":              "U-BT Cluj-Napoca",
    "u-banca transilvania":                          "U-BT Cluj-Napoca",
    "ubt cluj-napoca":                               "U-BT Cluj-Napoca",

    # EuroCup — versiuni UPPERCASE din euroleague_api
    "bahcesehir college istanbul":                   "Bahçeşehir Koleji",
    "neptunas klaipeda":                             "Klaipėdos Neptūnas",
    "klaipedos neptunas":                            "Klaipėdos Neptūnas",
    "slask wroclaw":                                 "Śląsk Wrocław",
    "umana reyer venice":                            "Umana Reyer Venezia",
    "umana reyer venezia":                           "Umana Reyer Venezia",
    "veolia towers hamburg":                         "Hamburg Towers",
    "hamburg towers":                                "Hamburg Towers",
    "cedevita olimpija ljubljana":                   "KK Cedevita Olimpija",
    "kk cedevita olimpija":                          "KK Cedevita Olimpija",
    "cedevita olimpija":                             "KK Cedevita Olimpija",
    "buducnost voli podgorica":                      "KK Budućnost VOLI",
    "kk buducnost voli":                             "KK Budućnost VOLI",
    "buducnost voli":                                "KK Budućnost VOLI",
    "kk budućnost voli":                             "KK Budućnost VOLI",
    "hapoel midtown jerusalem":                      "Hapoel Jerusalem BC",
    "hapoel jerusalem bc":                           "Hapoel Jerusalem BC",
    "baxi manresa":                                  "BAXI Manresa",
    "aris thessaloniki betsson":                     "Aris BC",
    "aris bc":                                       "Aris BC",

    # ABA League
    "kk krka novo mesto":                            "KK Krka",
    "kk krka":                                       "KK Krka",
    "kk split":                                      "KK Split",
    "kk sc derby":                                   "KK SC Derby",
    "fmp beograd":                                   "FMP Beograd",
    "kk partizan mozzart bet":                       "KK Partizan",
    "kk partizan":                                   "KK Partizan",
    "igokea m:tel":                                  "Igokea m:tel",
    "igokea":                                        "Igokea m:tel",
    "bc dubai":                                      "BC Dubai",
    "kk borac mozzart":                              "KK Borac",
    "kk borac":                                      "KK Borac",
    "kk crvena zvezda":                              "KK Crvena zvezda",
    "kk bosna":                                      "KK Bosna",
}


def _normalize_team(name: str) -> str:
    """Returnează forma canonică; fallback: Title Case."""
    if not isinstance(name, str) or not name.strip():
        return name
    canonical = TEAM_CANONICAL.get(name.strip().lower())
    if canonical:
        return canonical
    # Fallback: title case păstrând acronimele (2-3 litere mari)
    words = []
    for w in name.strip().split():
        if w.isupper() and len(w) <= 3:
            words.append(w)          # KK, BC, FK — rămân uppercase
        else:
            words.append(w.capitalize())
    return " ".join(words)


def _normalize_player_name(name: str) -> str:
    """
    Convertește "ALTIT, BEN" → "Ben Altit".
    Numele deja în format normal sunt returnate neschimbate.
    """
    if not isinstance(name, str) or not name.strip():
        return name
    if "," in name and name == name.upper():
        surname, _, first = name.partition(",")
        return f"{first.strip().title()} {surname.strip().title()}"
    return name


# ── Player ID unification ──────────────────────────────────────────────────

def _apply_id_map(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remapează player_id-urile ABA și LN la ID-urile canonice EuroCup.
    Actualizează și player_name + image_url pentru consistență.
    """
    if not _ABA_TO_EC or "competition" not in df.columns:
        return df

    # Construiește lookup name+image din rândurile EuroCup
    ec_rows = df[df["competition"] == "EuroCup"].set_index("player_id")
    ec_name  = ec_rows["player_name"].to_dict()  if "player_name" in ec_rows.columns else {}
    ec_image = ec_rows["image_url"].to_dict()    if "image_url"   in ec_rows.columns else {}

    sofascore_mask = df["competition"].isin(["ABA League", "Liga Națională"])
    for aba_id, ec_id in _ABA_TO_EC.items():
        mask = sofascore_mask & (df["player_id"].astype("Int64") == aba_id)
        if not mask.any():
            continue
        df.loc[mask, "player_id"] = ec_id
        if ec_id in ec_name:
            df.loc[mask, "player_name"] = ec_name[ec_id]
        if ec_id in ec_image:
            df.loc[mask, "image_url"] = ec_image[ec_id]

    log.info("  ID unification: %d jucători ABA+LN remapați la IDs EuroCup.", len(_ABA_TO_EC))
    return df


# ── Games normalization ────────────────────────────────────────────────────

GAMES_COLUMN_ORDER = [
    "competition", "competition_key", "season",
    "game_code", "round", "phase", "date",
    "home_team", "home_code", "away_team", "away_code",
    "score_home", "score_away",
    "venue", "attendance",
    "ubt_is_home", "ubt_score", "opp_score", "opponent",
    "result", "score_diff",
]


def normalize_games(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Normalizez games: {len(df)} rânduri, {df['competition'].nunique()} competiții.")
    df = df.copy()

    # competition_key
    df["competition_key"] = df["competition"].map(COMPETITION_KEY).fillna(
        df["competition"].str.lower().str.replace(" ", "_")
    )

    # Echipe
    for col in ("home_team", "away_team", "opponent"):
        if col in df.columns:
            before = df[col].nunique()
            df[col] = df[col].map(_normalize_team)
            after = df[col].nunique()
            if before != after:
                log.info(f"  {col}: {before} variante → {after} forme canonice")

    # Date — format="mixed" handles both "YYYY-MM-DD" (EuroCup) and "YYYY-MM-DD HH:MM:SS" (ABA/LN)
    df["date"] = pd.to_datetime(df["date"], errors="coerce", format="mixed").dt.date.astype(str)
    df.loc[df["date"] == "NaT", "date"] = pd.NA

    # game_code — string
    df["game_code"] = df["game_code"].astype(str)

    # Numerice nullable
    for col in ("score_home", "score_away", "ubt_score", "opp_score",
                "score_diff", "round", "attendance"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # season — int normal
    df["season"] = df["season"].astype(int)

    # result — numai W / L / None
    df["result"] = df["result"].where(df["result"].isin(["W", "L"]), other=pd.NA)

    # ubt_is_home — bool
    df["ubt_is_home"] = df["ubt_is_home"].astype(bool)

    # Ordonare coloane (păstrăm ce nu e în lista canonică la final)
    extra = [c for c in df.columns if c not in GAMES_COLUMN_ORDER]
    final_cols = [c for c in GAMES_COLUMN_ORDER if c in df.columns] + extra
    df = df[final_cols]

    df.sort_values(["competition_key", "season", "date"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ── Player stats normalization ─────────────────────────────────────────────

PLAYERS_COLUMN_ORDER = [
    "competition", "competition_key", "season",
    "player_id", "player_name", "team_code",
    "games_played", "minutes",
    "points", "rebounds", "off_rebounds", "def_rebounds",
    "assists", "steals", "blocks", "turnovers", "fouls",
    "fg2_made", "fg2_att", "fg3_made", "fg3_att",
    "ft_made", "ft_att",
    "points_pg", "rebounds_pg", "assists_pg", "steals_pg",
    "blocks_pg", "turnovers_pg", "minutes_pg",
    "fg2_pct", "fg3_pct", "ft_pct",
    "pir", "image_url",
]


def normalize_players(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Normalizez player_stats: {len(df)} rânduri, {df['competition'].nunique()} competiții.")
    df = df.copy()

    # competition_key
    df["competition_key"] = df["competition"].map(COMPETITION_KEY).fillna(
        df["competition"].str.lower().str.replace(" ", "_")
    )

    # Nume jucători
    df["player_name"] = df["player_name"].map(_normalize_player_name)

    # season — int
    df["season"] = df["season"].astype(int)

    # Coloane întregi
    for col in ("games_played", "fg2_made", "fg2_att", "fg3_made", "fg3_att",
                "ft_made", "ft_att", "points", "rebounds", "off_rebounds",
                "def_rebounds", "assists", "steals", "blocks", "turnovers", "fouls"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Coloane float
    for col in ("minutes", "pir", "points_pg", "rebounds_pg", "assists_pg",
                "steals_pg", "blocks_pg", "turnovers_pg", "minutes_pg",
                "fg2_pct", "fg3_pct", "ft_pct"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    # player_id — Int64 nullable (unele API-uri returnează None)
    if "player_id" in df.columns:
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")

    # team_code — uppercase
    if "team_code" in df.columns:
        df["team_code"] = df["team_code"].str.upper()

    # Unificare IDs între competiții
    df = _apply_id_map(df)

    # Ordonare coloane
    extra = [c for c in df.columns if c not in PLAYERS_COLUMN_ORDER]
    final_cols = [c for c in PLAYERS_COLUMN_ORDER if c in df.columns] + extra
    df = df[final_cols]

    df.sort_values(["competition_key", "season", "points"], ascending=[True, True, False], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ── Export ─────────────────────────────────────────────────────────────────

def _save(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)
    log.info(f"Salvat → {path}  ({len(df)} rânduri, {df['competition'].nunique()} competiții)")


def run(games_only: bool = False, players_only: bool = False) -> None:
    log.info("=== Normalizer ===")
    any_done = False

    if not players_only:
        src = DATA_PROCESSED / "games.csv"
        if not src.exists():
            log.warning(f"Lipsește {src} — skip games.")
        else:
            df_games = pd.read_csv(src)
            df_norm  = normalize_games(df_games)
            _save(df_norm, DATA_PROCESSED / "games_normalized.csv")
            any_done = True

    if not games_only:
        src = DATA_PROCESSED / "player_stats.csv"
        if not src.exists():
            log.warning(f"Lipsește {src} — skip player_stats.")
        else:
            df_players = pd.read_csv(src)
            df_norm    = normalize_players(df_players)
            _save(df_norm, DATA_PROCESSED / "player_stats_normalized.csv")
            any_done = True

    if any_done:
        log.info("=== Done ===")
    else:
        log.error("Niciun fișier procesat.")
        sys.exit(1)


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalizează games.csv și player_stats.csv")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--games-only",   action="store_true", help="Procesează numai games.csv")
    group.add_argument("--players-only", action="store_true", help="Procesează numai player_stats.csv")
    args = parser.parse_args()
    run(games_only=args.games_only, players_only=args.players_only)
