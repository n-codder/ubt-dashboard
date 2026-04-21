#!/usr/bin/env python3
"""
setup.py — Initial project setup and environment validation.

Run once after cloning the repo:
    python setup.py

Checks:
    - Python version >= 3.10
    - .env file exists (copies from .env.example if not)
    - Required directories exist
    - Dependencies installed
    - API connectivity (optional, skipped if keys missing)
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent

# ── ANSI colors ────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):    print(f"  {GREEN}✓{RESET}  {msg}")
def warn(msg):  print(f"  {YELLOW}⚠{RESET}  {msg}")
def fail(msg):  print(f"  {RED}✗{RESET}  {msg}")
def info(msg):  print(f"  {CYAN}→{RESET}  {msg}")
def header(msg): print(f"\n{BOLD}{msg}{RESET}")


def check_python_version():
    header("1. Python version")
    major, minor = sys.version_info[:2]
    if (major, minor) >= (3, 10):
        ok(f"Python {major}.{minor} — OK")
        return True
    else:
        fail(f"Python {major}.{minor} — requires >= 3.10")
        return False


def check_env_file():
    header("2. Environment file (.env)")
    env_path     = ROOT / ".env"
    example_path = ROOT / ".env.example"

    if env_path.exists():
        ok(".env found")
    else:
        if example_path.exists():
            shutil.copy(example_path, env_path)
            warn(".env not found — copied from .env.example")
            warn("Edit .env and fill in your API keys before running fetchers")
        else:
            fail(".env.example missing — cannot create .env")
            return False
    return True


def check_directories():
    header("3. Directory structure")
    required = [
        "data/raw/eurocup",
        "data/raw/aba_league",
        "data/raw/liga_nationala",
        "data/raw/cupa_romaniei",
        "data/processed",
        "fetchers",
        "processors",
        "uploaders",
        "player-profiles/assets/players",
        "player-profiles/assets/flags",
        "config",
        "research/fetchers",
        "research/analysis",
        "research/exports",
    ]
    all_ok = True
    for d in required:
        path = ROOT / d
        if path.exists():
            ok(d)
        else:
            path.mkdir(parents=True, exist_ok=True)
            warn(f"{d} — created")
    return all_ok


def install_dependencies():
    header("4. Python dependencies")
    req_file = ROOT / "requirements.txt"
    if not req_file.exists():
        fail("requirements.txt not found")
        return False

    info("Installing from requirements.txt...")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(req_file), "-q"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        ok("All dependencies installed")
        return True
    else:
        fail("pip install failed")
        print(result.stderr[:500])
        return False


def check_api_keys():
    header("5. API keys validation")
    try:
        from dotenv import load_dotenv
        import os
        load_dotenv(ROOT / ".env")

        api_key    = os.getenv("API_BASKETBALL_KEY", "")
        sheets_id  = os.getenv("GOOGLE_SHEETS_ID", "")

        if api_key and api_key != "your_key_here":
            ok("API_BASKETBALL_KEY — set")
        else:
            warn("API_BASKETBALL_KEY — not set (needed for ABA / LN / Cupă)")

        if sheets_id and sheets_id != "your_sheet_id_here":
            ok("GOOGLE_SHEETS_ID — set")
        else:
            warn("GOOGLE_SHEETS_ID — not set (needed for Looker Studio sync)")

        service_acc = ROOT / "config" / "service_account.json"
        if service_acc.exists():
            ok("Google service account JSON — found")
        else:
            warn("config/service_account.json — missing (needed for Sheets upload)")

    except ImportError:
        warn("python-dotenv not installed yet — skipping key check")


def check_euroleague_package():
    header("6. euroleague-api package")
    try:
        import euroleague_api  # noqa: F401
        ok("euroleague-api imported successfully")
    except ImportError:
        warn("euroleague-api not available — run: pip install euroleague-api")


def update_last_updated():
    """Stamp setup time in last_updated.json."""
    path = ROOT / "data" / "processed" / "last_updated.json"
    if path.exists():
        with open(path) as f:
            data = json.load(f)
        data["setup_at"] = datetime.utcnow().isoformat() + "Z"
        with open(path, "w") as f:
            json.dump(data, f, indent=2)


def main():
    print(f"\n{BOLD}{'═'*50}{RESET}")
    print(f"{BOLD}  U-BT Dashboard — Setup{RESET}")
    print(f"{'═'*50}")

    results = [
        check_python_version(),
        check_env_file(),
        check_directories(),
        install_dependencies(),
    ]
    check_api_keys()
    check_euroleague_package()
    update_last_updated()

    header("Summary")
    if all(results):
        ok("Setup complete — you can now run: python fetchers/run_all.py")
    else:
        warn("Setup finished with warnings — check items marked ✗ above")

    print()


if __name__ == "__main__":
    main()
