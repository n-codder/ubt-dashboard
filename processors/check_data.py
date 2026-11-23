"""
processors/check_data.py — Verifică datele procesate înainte de publicare.

Rulează la finalul run_all (deci și în fetch-ul automat din GitHub Actions)
și înainte de „Publică” din admin. Dacă găsește o problemă, iese cu cod 1,
iar datele NU se publică.

Verificări:
  1. Fiecare CSV din data/processed se citește corect (același număr de
     coloane pe fiecare rând).
  2. Niciun CSV nu a pierdut rânduri față de ultima versiune publicată
     (git HEAD) — peste o mică toleranță. Așa a arătat bug-ul din 19.11.2026:
     player_stats_per_game_normalized.csv a scăzut de la 1430 la 309 rânduri.
  3. Fiecare meci terminat din games.csv are statistici pe jucători,
     scor pe sferturi și statistici de echipă.

Usage:
    python processors/check_data.py
"""

import io
import logging
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"

log = logging.getLogger(__name__)

# Câte rânduri are voie să piardă un fișier (meciuri reprogramate, duplicate
# eliminate). Peste asta, e aproape sigur o eroare.
MAX_DROP_ROWS = 3
MAX_DROP_PCT = 0.02


def _published_csv(rel_path: str) -> pd.DataFrame | None:
    """Versiunea fișierului din ultimul commit; None dacă nu există acolo."""
    r = subprocess.run(["git", "show", f"HEAD:{rel_path}"], cwd=ROOT, capture_output=True, text=True)
    if r.returncode != 0:
        return None
    return pd.read_csv(io.StringIO(r.stdout))


def _game_keys(df: pd.DataFrame, comp_col: str) -> set[tuple[str, str]]:
    return set(zip(df[comp_col].astype(str), df["game_code"].astype("int64").astype(str)))


def check() -> list[str]:
    problems: list[str] = []
    frames: dict[str, pd.DataFrame] = {}

    # 1 + 2: citire și număr de rânduri
    for path in sorted(PROCESSED.glob("*.csv")):
        rel = path.relative_to(ROOT).as_posix()
        try:
            df = pd.read_csv(path, on_bad_lines="error")
        except Exception as exc:
            problems.append(f"{path.name}: nu se poate citi ({exc})")
            continue
        frames[path.name] = df

        old = _published_csv(rel)
        if old is None:
            continue
        lost = len(old) - len(df)
        if lost > max(MAX_DROP_ROWS, MAX_DROP_PCT * len(old)):
            problems.append(f"{path.name}: a pierdut {lost} rânduri ({len(old)} → {len(df)})")

    # 3: fiecare meci terminat are toate datele
    games = frames.get("games_normalized.csv")
    if games is not None:
        finished = games[games["result"].isin(["W", "L"])]
        checks = [
            ("player_stats_per_game_normalized.csv", "statistici pe jucători", "competition_key", None),
            ("quarters.csv", "scor pe sferturi", "competition_key", None),
            # LN nu are statistici de echipă la sursă
            ("game_stats.csv", "statistici de echipă", "competition", "Liga Națională"),
        ]
        for fname, label, col, skip_comp in checks:
            df = frames.get(fname)
            if df is None:
                problems.append(f"{fname}: lipsește")
                continue
            need = finished
            if skip_comp:
                need = need[need["competition"] != skip_comp]
            missing = _game_keys(need, col) - _game_keys(df, col)
            if missing:
                rows = need[[k in missing for k in zip(need[col].astype(str), need["game_code"].astype("int64").astype(str))]]
                rows = rows.sort_values("date", ascending=False)
                desc = ", ".join(f"{r.competition} {str(r.date)[:10]} vs {r.opponent}" for r in rows.head(5).itertuples())
                if len(rows) > 5:
                    desc += f" și încă {len(rows) - 5}"
                problems.append(f"{label} lipsesc pentru {len(missing)} meci(uri): {desc}")

    return problems


def run() -> bool:
    """True dacă totul e în regulă."""
    problems = check()
    if problems:
        log.error("VERIFICARE DATE EȘUATĂ — datele nu trebuie publicate:")
        for p in problems:
            log.error("  ✗ %s", p)
        return False
    log.info("Verificare date: OK.")
    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    sys.exit(0 if run() else 1)
