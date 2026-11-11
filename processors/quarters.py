"""
processors/quarters.py — Scorul pe sferturi pentru meciurile U-BT → data/processed/quarters.csv

Sursele sunt fișierele brute deja descărcate (fără apeluri API):
  - EuroCup        : data/raw/eurocup/games_raw_{an}.csv   (ScoreQuarter1A … ScoreExtraTimeB, cumulat)
  - ABA League, LN : data/raw/{aba,ln}/games_raw_{an}.json (homeScore.period1…4, overtime)

data/raw/ nu se commitează, de aceea pagina de meci citește fișierul procesat.
Scorurile sunt pe sfert (nu cumulate); prelungirile sunt adunate într-o singură coloană.

Usage:
    python processors/quarters.py
"""

import json
import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import DATA_RAW, DATA_PROCESSED, TEAM_IDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

OUT_PATH = DATA_PROCESSED / "quarters.csv"
EC_CODE = TEAM_IDS["competitions"]["eurocup"]["team_code"]           # "CLU"
SOFA_TEAM_ID = TEAM_IDS["competitions"]["liga_nationala"]["team_id"]  # 59003 (același și în ABA)
COLS = ["competition_key", "season", "game_code",
        "ubt_q1", "ubt_q2", "ubt_q3", "ubt_q4", "ubt_ot", "opp_q1", "opp_q2", "opp_q3", "opp_q4", "opp_ot"]


def _num(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def eurocup_rows() -> list[dict]:
    rows = []
    for path in sorted((DATA_RAW / "eurocup").glob("games_raw_*.csv")):
        df = pd.read_csv(path)
        for _, o in df.iterrows():
            a = str(o.get("CodeTeamA", "")).strip() == EC_CODE
            b = str(o.get("CodeTeamB", "")).strip() == EC_CODE
            if not (a or b):
                continue
            us, them = ("A", "B") if a else ("B", "A")
            cu = [_num(o.get(f"ScoreQuarter{n}{us}")) for n in range(1, 5)]
            co = [_num(o.get(f"ScoreQuarter{n}{them}")) for n in range(1, 5)]
            if None in cu or None in co or not cu[3]:
                continue   # meci nejucat sau fără sferturi
            per = lambda c: [c[0]] + [c[i] - c[i - 1] for i in range(1, 4)]
            fu, fo = _num(o.get(f"Score{us}")), _num(o.get(f"Score{them}"))
            u, p = per(cu), per(co)
            rows.append({"competition_key": "eurocup", "season": int(o["Season"]), "game_code": str(int(o["Gamecode"])),
                         **{f"ubt_q{i + 1}": u[i] for i in range(4)}, "ubt_ot": (fu or 0) - cu[3],
                         **{f"opp_q{i + 1}": p[i] for i in range(4)}, "opp_ot": (fo or 0) - co[3]})
    return rows


def sofascore_rows(folder: str, comp_key: str) -> list[dict]:
    """ABA / LN — sezonul se ia din games_normalized (convenția anului de început)."""
    rows = []
    for path in sorted((DATA_RAW / folder).glob("games_raw_*.json")):
        for e in json.loads(path.read_text()):
            home = e.get("homeTeam", {}).get("id") == SOFA_TEAM_ID
            away = e.get("awayTeam", {}).get("id") == SOFA_TEAM_ID
            if not (home or away):
                continue
            su, so = (e.get("homeScore", {}), e.get("awayScore", {})) if home else (e.get("awayScore", {}), e.get("homeScore", {}))
            u = [_num(su.get(f"period{n}")) for n in range(1, 5)]
            p = [_num(so.get(f"period{n}")) for n in range(1, 5)]
            if None in u or None in p:
                continue
            rows.append({"competition_key": comp_key, "season": None, "game_code": str(e["id"]),
                         **{f"ubt_q{i + 1}": u[i] for i in range(4)}, "ubt_ot": _num(su.get("overtime")) or 0,
                         **{f"opp_q{i + 1}": p[i] for i in range(4)}, "opp_ot": _num(so.get("overtime")) or 0})
    return rows


def run() -> None:
    log.info("=== Quarters ===")
    games = pd.read_csv(DATA_PROCESSED / "games_normalized.csv", dtype={"game_code": str})
    season_of = {(r.competition_key, r.game_code): int(r.season) for r in games.itertuples()}

    rows = eurocup_rows() + sofascore_rows("aba", "aba_league") + sofascore_rows("ln", "liga_nationala")
    df = pd.DataFrame(rows, columns=COLS)
    # ABA/LN: sezonul din games_normalized; păstrăm doar meciurile care există acolo
    miss = df["season"].isna()
    df.loc[miss, "season"] = [season_of.get((k, c)) for k, c in zip(df.loc[miss, "competition_key"], df.loc[miss, "game_code"])]
    df = df[df["season"].notna()].copy()
    df["season"] = df["season"].astype(int)
    known = set(zip(games["competition_key"], games["season"].astype(int), games["game_code"]))
    df = df[[k in known for k in zip(df["competition_key"], df["season"], df["game_code"])]]
    df = df.drop_duplicates(["competition_key", "season", "game_code"], keep="last")
    df.sort_values(["competition_key", "season", "game_code"], inplace=True)
    df.to_csv(OUT_PATH, index=False)
    log.info("Salvat → %s  (%d meciuri: %s)", OUT_PATH, len(df), df.groupby("competition_key").size().to_dict())


if __name__ == "__main__":
    run()
