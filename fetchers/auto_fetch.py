"""
fetchers/auto_fetch.py — Fetch automat, rulat din oră în oră de GitHub Actions
(.github/workflows/auto-fetch.yml).

Decide fără niciun apel API (citește doar fișierele din repo):
  1. Meci terminat, lipsă din date → fetch doar pentru competiția lui.
     Un meci e considerat terminat la READY_AFTER după ora de start
     (din data/processed/schedule.json). Dacă datele nu sunt încă gata,
     reîncearcă la cel mult RETRY_EVERY, până la GIVE_UP după start.
  2. Altfel, fetch de siguranță pentru toate competițiile o dată la
     `every_days` zile (config/auto_fetch.json, setat din admin).

Partea de decizie folosește doar biblioteca standard, ca rularea din oră în oră
să nu instaleze pandas & co. când nu e nimic de făcut.

Usage:
    python fetchers/auto_fetch.py            # decide și rulează
    python fetchers/auto_fetch.py --decide   # doar decide (scrie comps=... pentru GitHub Actions)
    python fetchers/auto_fetch.py --run eurocup,aba_league   # rulează pentru competițiile date ("all" = toate)
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

READY_AFTER = timedelta(hours=2, minutes=30)   # meci ~2h + timp ca sursele să publice statisticile
RETRY_EVERY = timedelta(hours=3)
GIVE_UP     = timedelta(hours=48)

RAW_FOLDER = {"aba_league": "aba", "liga_nationala": "ln"}


def _say(msg: str) -> None:
    print(msg, file=sys.stderr)


def _read_json(path: Path, default):
    return json.loads(path.read_text()) if path.exists() else default


def _have_game(g: dict) -> bool:
    """E meciul deja în datele publicate?"""
    comp = g["competition_key"]
    if comp in RAW_FOLDER:
        for path in (ROOT / "data" / "raw" / RAW_FOLDER[comp]).glob("games_raw_*.json"):
            if any(e.get("id") == g["id"] for e in json.loads(path.read_text())):
                return True
        return False
    # EuroCup: altă sursă (alte ID-uri) → potrivire după dată
    day = g["start_utc"][:10]
    with (ROOT / "data" / "processed" / "games.csv").open(newline="") as f:
        return any(r["competition"] == "EuroCup" and r["date"][:10] == day for r in csv.DictReader(f))


def decide(now: datetime | None = None) -> tuple[str, str]:
    """Întoarce (comps, motiv). comps: "" = nimic, "all" = toate, altfel listă separată prin virgulă."""
    now = now or datetime.now(timezone.utc)
    cfg = _read_json(ROOT / "config" / "auto_fetch.json", {})
    if not cfg.get("enabled"):
        return "", "fetch automat oprit (config/auto_fetch.json)"

    last = _read_json(ROOT / "data" / "processed" / "last_updated.json", {})
    last_comp = {k: datetime.fromisoformat(v) for k, v in last.get("competitions", {}).items() if v}

    schedule = _read_json(ROOT / "data" / "processed" / "schedule.json", {}).get("games", [])
    due, reasons = [], []
    for g in schedule:
        start = datetime.fromisoformat(g["start_utc"])
        age = now - start
        if not (READY_AFTER <= age <= GIVE_UP) or _have_game(g):
            continue
        comp = g["competition_key"]
        prev = last_comp.get(comp)
        if prev and prev > start + READY_AFTER - timedelta(minutes=30) and now - prev < RETRY_EVERY:
            reasons.append(f"{g['home']} – {g['away']}: încercat acum {(now - prev).seconds // 60} min, aștept")
            continue
        if comp not in due:
            due.append(comp)
        reasons.append(f"meci terminat fără date: {g['home']} – {g['away']} ({g['start_utc'][:16]} UTC)")
    if due:
        return ",".join(due), "; ".join(reasons)

    every = timedelta(days=int(cfg.get("every_days", 2)))
    last_all = datetime.fromisoformat(last["last_updated"]) if last.get("last_updated") else None
    # o oră toleranță, ca rularea de la aceeași oră să nu sară o zi
    if not last_all or now - last_all >= every - timedelta(hours=1):
        return "all", f"fetch de siguranță (o dată la {every.days} zile)"
    return "", "; ".join(reasons) or "niciun meci nou terminat"


def run(comps: str) -> int:
    from fetchers import run_all
    errors = []
    if comps == "all":
        errors += run_all.run()
    else:
        for comp in comps.split(","):
            # force: ignoră fereastra de 24h din run_all — știm că e un meci nou
            errors += run_all.run(force=True, only=comp)
    # cod ≠ 0 → GitHub Actions nu publică și trimite email de eșec
    return 1 if errors else 0


def main() -> int:
    args = sys.argv[1:]
    if args[:1] == ["--run"]:
        return run(args[1]) if len(args) > 1 and args[1] else 0
    comps, reason = decide()
    _say(f"Decizie: {comps or 'nimic de făcut'} — {reason}")
    if args[:1] == ["--decide"]:
        print(f"comps={comps}")
        return 0
    return run(comps) if comps else 0


if __name__ == "__main__":
    sys.exit(main())
