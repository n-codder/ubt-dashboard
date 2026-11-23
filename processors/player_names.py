"""
processors/player_names.py — Un singur nume afișat per jucător.

Sursele dau același jucător cu nume diferite („SMART, JAVONTE” în EuroCup,
„Ja'Vonte Smart” în ABA, „Bobe Nicolescu” / „Bogdan Nicolescu”), așa că
paginile site-ului ajungeau să afișeze variante diferite.

Ordinea de prioritate pentru numele canonic al unui player_id:
  1. config/player_bio.json → players[id].name  (oficial, de pe u-bt.ro)
  2. numele din EuroCup (cea mai recentă apariție)
  3. numele cel mai recent din orice competiție
Apoi se aplică în player_stats_normalized.csv și
player_stats_per_game_normalized.csv. Rulează în run_all după dedup.

Usage:
    python processors/player_names.py
"""

import json
import logging
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"
BIO_PATH = ROOT / "config" / "player_bio.json"
FILES = [
    PROCESSED / "player_stats_normalized.csv",
    PROCESSED / "player_stats_per_game_normalized.csv",
]

log = logging.getLogger(__name__)

_ROMAN = re.compile(r"\b(Ii|Iii|Iv)\b")
_MC = re.compile(r"\bMc ([A-Z])")


def fmt_name(name):
    """
    Formatare uniformă:
      "LIVINGSTON II, OTIS" → "Otis Livingston II"
      "MC CREARY, JALYN"    → "Jalyn McCreary"
    Numele deja scrise normal rămân neschimbate (în afară de II/Mc).
    """
    if not isinstance(name, str) or not name.strip():
        return name
    name = " ".join(name.split())
    if "," in name and name == name.upper():
        surname, _, first = name.partition(",")
        name = f"{first.strip().title()} {surname.strip().title()}"
    name = _ROMAN.sub(lambda m: m.group(0).upper(), name)
    return _MC.sub(r"Mc\1", name)


def _official_names() -> dict[str, str]:
    if not BIO_PATH.exists():
        return {}
    players = json.loads(BIO_PATH.read_text()).get("players", {})
    return {str(pid): p["name"] for pid, p in players.items() if p.get("name")}


def canonical_names(frames: list[pd.DataFrame]) -> dict[str, str]:
    rows = pd.concat(
        [f[["player_id", "player_name", "competition_key", "season"]
           + (["date"] if "date" in f.columns else [])] for f in frames],
        ignore_index=True,
    )
    rows = rows[rows["player_name"].astype(str).str.strip() != ""].copy()
    rows["player_id"] = rows["player_id"].astype(str)
    rows["is_ec"] = rows["competition_key"] == "eurocup"
    if "date" not in rows.columns:
        rows["date"] = ""
    rows["date"] = rows["date"].fillna("").astype(str)
    # cel mai bun rând per jucător: EuroCup întâi, apoi cel mai recent
    rows = rows.sort_values(["is_ec", "season", "date"])
    names = {pid: fmt_name(n) for pid, n in rows.groupby("player_id")["player_name"].last().items()}
    names.update(_official_names())
    return names


def run() -> None:
    # totul ca text: fișierele se rescriu exact, doar numele se schimbă
    frames = [pd.read_csv(p, dtype=str, keep_default_na=False) for p in FILES]
    names = canonical_names(frames)
    for path, df in zip(FILES, frames):
        new = df["player_id"].map(names).fillna(df["player_name"])
        changed = int((new != df["player_name"]).sum())
        if changed:
            df["player_name"] = new
            tmp = path.with_suffix(".csv.tmp")
            df.to_csv(tmp, index=False)
            tmp.replace(path)
        log.info("Nume jucători: %s — %d rânduri actualizate.", path.name, changed)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    sys.path.insert(0, str(ROOT))
    run()
