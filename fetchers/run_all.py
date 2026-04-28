"""
fetchers/run_all.py — Rulează toți fetcherii + procesoarele în ordine.

Skip logic per competiție:
  - status "finished"    → skip permanent (sezon încheiat)
  - status "not_started" → skip până la primul meci
  - fetch-uit în ultimele 24h → skip (deja actualizat)

Usage:
    python fetchers/run_all.py
    python fetchers/run_all.py --force          # ignoră cache-ul de 24h
    python fetchers/run_all.py --only eurocup   # rulează doar o competiție
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import DATA_PROCESSED, TEAM_IDS

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

FRESH_WINDOW = timedelta(hours=24)

# ── Competiții definite ────────────────────────────────────────────────────
# Fiecare intrare: (competition_key, season, fetcher_module, run_kwargs)
COMPETITIONS = [
    ("eurocup",        2025, "fetchers.eurocup_fetcher", {"season": 2025}),
    ("aba_league",     2025, "fetchers.aba_fetcher",     {"season": 2025, "full": False}),
    ("liga_nationala", 2026, "fetchers.ln_fetcher",      {"season": 2026, "full": False}),
]


# ── Helpers ────────────────────────────────────────────────────────────────

def _load_last_updated() -> dict:
    path = DATA_PROCESSED / "last_updated.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text()).get("competitions", {})


def _last_fetch(comp_key: str, last_updated: dict) -> datetime | None:
    ts = last_updated.get(comp_key)
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def _should_skip(comp_key: str, last_updated: dict, force: bool) -> tuple[bool, str]:
    cfg    = TEAM_IDS["competitions"].get(comp_key, {})
    status = cfg.get("status", "active")

    if status == "finished":
        return True, "sezon încheiat (status=finished)"

    if status == "not_started":
        return True, "sezon neînceput (status=not_started)"

    if not force:
        last = _last_fetch(comp_key, last_updated)
        if last:
            age = datetime.now(timezone.utc) - last
            if age < FRESH_WINDOW:
                remaining = FRESH_WINDOW - age
                h, m = divmod(int(remaining.total_seconds()), 3600)
                m //= 60
                return True, f"fetch-uit acum {int(age.total_seconds()//3600)}h — următor în {h}h{m:02d}m"

    return False, ""


def _run_fetcher(module_path: str, kwargs: dict) -> bool:
    import importlib
    try:
        mod = importlib.import_module(module_path)
        mod.run(**kwargs)
        return True
    except SystemExit as e:
        if e.code != 0:
            log.error(f"Fetcher {module_path} s-a oprit cu eroare.")
            return False
        return True
    except Exception as exc:
        log.error(f"Eroare în {module_path}: {exc}")
        return False


def _run_processors(season: int, full: bool) -> None:
    import importlib
    log.info("─" * 60)
    log.info("Rulez processors/normalizer.py ...")
    try:
        norm = importlib.import_module("processors.normalizer")
        norm.run()
        log.info("Normalizer — done.")
    except Exception as exc:
        log.error(f"Normalizer eroare: {exc}")

    log.info("Rulez processors/per_game_stats.py ...")
    try:
        pgs = importlib.import_module("processors.per_game_stats")
        for _, s, _, _ in COMPETITIONS:
            pgs.run(season=s, full=full)
    except Exception as exc:
        log.error(f"Per-game stats eroare: {exc}")


def _summary() -> None:
    log.info("─" * 60)
    log.info("SUMAR FINAL")

    games_path   = DATA_PROCESSED / "games.csv"
    players_path = DATA_PROCESSED / "player_stats_normalized.csv"

    if games_path.exists():
        import pandas as pd
        df_g = pd.read_csv(games_path)
        for comp, grp in df_g.groupby("competition"):
            finished = grp["result"].isin(["W", "L"]).sum()
            log.info(f"  {comp:<25} {len(grp):>3} meciuri  ({finished} terminate)")
    else:
        log.info("  games.csv — nu există încă")

    if players_path.exists():
        import pandas as pd
        df_p = pd.read_csv(players_path)
        for comp, grp in df_p.groupby("competition"):
            n_players = grp["player_id"].nunique()
            log.info(f"  {comp:<25} {n_players:>3} jucători unici")
    else:
        log.info("  player_stats_normalized.csv — nu există încă")


# ── Entry point ────────────────────────────────────────────────────────────

def run(force: bool = False, only: str | None = None) -> None:
    log.info("=" * 60)
    log.info("run_all.py — start")
    log.info("=" * 60)

    last_updated  = _load_last_updated()
    ran_any       = False

    for comp_key, season, module, kwargs in COMPETITIONS:
        if only and comp_key != only:
            continue

        log.info(f"\n{'─'*60}")
        log.info(f"[{comp_key.upper()}] season={season}")

        skip, reason = _should_skip(comp_key, last_updated, force)
        if skip:
            log.info(f"  → SKIP: {reason}")
            continue

        log.info(f"  → Rulez {module} ...")
        ok = _run_fetcher(module, kwargs)
        if ok:
            ran_any = True
            last_updated = _load_last_updated()   # reîncarcă după scriere

    if ran_any:
        _run_processors(season=2025, full=False)
    else:
        log.info("\nToate competițiile au fost skip-uite — procesoarele nu rulează.")

    _summary()
    log.info("\n" + "=" * 60)
    log.info("run_all.py — done")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rulează toți fetcherii U-BT")
    parser.add_argument(
        "--force", action="store_true",
        help="Ignoră fereastra de 24h și forțează fetch",
    )
    parser.add_argument(
        "--only", metavar="COMP",
        help="Rulează doar o competiție (eurocup / aba_league / liga_nationala)",
    )
    args = parser.parse_args()
    run(force=args.force, only=args.only)
