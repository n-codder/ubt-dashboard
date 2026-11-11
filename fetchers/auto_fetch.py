"""
fetchers/auto_fetch.py — Rulat zilnic de GitHub Actions (.github/workflows/auto-fetch.yml).

Citește config/auto_fetch.json (setat din admin):
    {"enabled": true, "every_days": 2}
și rulează run_all doar dacă e pornit și a trecut intervalul de la ultimul fetch.
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main() -> int:
    cfg = json.loads((ROOT / "config" / "auto_fetch.json").read_text())
    if not cfg.get("enabled"):
        print("Fetch automat oprit (config/auto_fetch.json).")
        return 0

    every = timedelta(days=int(cfg.get("every_days", 2)))
    last_path = ROOT / "data" / "processed" / "last_updated.json"
    last = json.loads(last_path.read_text()).get("last_updated") if last_path.exists() else None
    if last:
        age = datetime.now(timezone.utc) - datetime.fromisoformat(last)
        # o oră toleranță, ca rularea zilnică de la aceeași oră să nu sară o zi
        if age < every - timedelta(hours=1):
            print(f"Ultimul fetch acum {age.days} zile {age.seconds // 3600} h; intervalul e {every.days} zile — nimic de făcut.")
            return 0

    from fetchers import run_all
    run_all.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
