"""
fetchers/aba_attendance.py — Spectatori (și sala) pentru meciurile U-BT din ABA League,
de pe site-ul oficial aba-liga.com. SofaScore (sursa principală ABA) nu îi are.

Cum găsește meciul:
  1. calendarul sezonului  /calendar/{yy}/1/  → numărul ABA al fiecărui meci U-BT + data
  2. pagina meciului       /match/{nr}/{yy}/1/Overview/  → „<i class="fas fa-users"></i> 4612”
Potrivirea cu meciurile noastre (ID SofaScore) se face după dată (ora Europei Centrale).

Rezultatul se păstrează în data/raw/aba/attendance_{season}.json, așa că fiecare meci
se citește o singură dată. Un meci fără număr pe pagină (încă nepublicat) se reîncearcă
la rularea următoare. Pauză de 2 s între cereri (Crawl-delay din robots.txt).

Usage:
    python fetchers/aba_attendance.py --season 2026
"""

import argparse
import html
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import DATA_RAW, TEAM_IDS

log = logging.getLogger(__name__)

BASE = "https://www.aba-liga.com"
CET = ZoneInfo("Europe/Belgrade")
DELAY = 2.0
TEAM_SLUG = "cluj"  # apare în adresa tuturor meciurilor U-BT (u-bt-cluj-napoca-...)
TEAM_ID = TEAM_IDS["competitions"]["aba_league"]["team_id"]
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ubt-dashboard/1.0)"}


def cache_path(season: int) -> Path:
    return DATA_RAW / "aba" / f"attendance_{season}.json"


def load(season: int) -> dict:
    p = cache_path(season)
    return json.loads(p.read_text()) if p.exists() else {}


def _get(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    time.sleep(DELAY)
    return r.text


def _calendar(season: int) -> dict[str, int]:
    """data (YYYY-MM-DD, CET) → numărul ABA al meciului U-BT din acea zi."""
    yy = f"{season % 100:02d}"
    page = _get(f"{BASE}/calendar/{yy}/1/")
    out = {}
    for row in page.split("<tr"):
        m = re.search(rf'/match/(\d+)/{yy}/1/[^"]*{TEAM_SLUG}', row, re.I)
        d = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", row)
        if m and d:
            out[f"{d.group(3)}-{d.group(2)}-{d.group(1)}"] = int(m.group(1))
    return out


def _match_page(season: int, nr: int) -> dict:
    yy = f"{season % 100:02d}"
    page = _get(f"{BASE}/match/{nr}/{yy}/1/Overview/")
    att = re.search(r'fa-users"\s*>\s*</i>\s*([\d.,\s]+?)\s*<', page)
    venue = re.search(r"Venue:\s*([^<\n]+)", page)
    n = int(re.sub(r"\D", "", att.group(1))) if att and re.sub(r"\D", "", att.group(1)) else None
    return {"aba_match": nr, "attendance": n, "venue": html.unescape(venue.group(1)).strip() if venue else None}


def _is_finished(e: dict) -> bool:
    st = e.get("status", {})
    return st.get("type") == "finished" or st.get("description") == "Ended"


def update(season: int, events: list[dict]) -> dict:
    """Completează cache-ul pentru meciurile terminate care nu au încă spectatori."""
    cache = load(season)
    todo = {}
    for e in events:
        if not _is_finished(e) or not e.get("startTimestamp"):
            continue
        if (cache.get(str(e["id"])) or {}).get("attendance"):
            continue
        day = datetime.fromtimestamp(e["startTimestamp"], timezone.utc).astimezone(CET).date().isoformat()
        todo[day] = e["id"]
    if not todo:
        return cache

    log.info("Spectatori ABA %d: caut %d meci(uri) pe aba-liga.com ...", season, len(todo))
    cal = _calendar(season)
    for day, sofa_id in sorted(todo.items()):
        nr = cal.get(day)
        if nr is None:
            log.warning("  %s: meciul nu apare în calendarul aba-liga.com — sărit", day)
            continue
        info = _match_page(season, nr)
        if info["attendance"]:
            cache[str(sofa_id)] = {"date": day, **info}
            log.info("  %s: %s spectatori (%s)", day, f"{info['attendance']:,}".replace(",", "."), info["venue"])
        else:
            log.info("  %s: numărul de spectatori nu e încă publicat — reîncerc data viitoare", day)

    p = cache_path(season)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(dict(sorted(cache.items(), key=lambda kv: kv[1]["date"])), indent=2, ensure_ascii=False) + "\n")
    tmp.replace(p)
    return cache


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    parser = argparse.ArgumentParser(description="Spectatori ABA League de pe aba-liga.com")
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    events = json.loads((DATA_RAW / "aba" / f"games_raw_{args.season}.json").read_text())
    update(args.season, events)
