"""
admin/app.py — Admin local pentru U-BT în cifre.

Pornire:
    python admin/app.py            # http://127.0.0.1:8770

Ce face:
  - pornește fetch-ul (toate competițiile sau una), cu log live și istoric
  - setează fetch-ul automat din GitHub Actions (config/auto_fetch.json)
  - încarcă logo-uri pentru adversari (config/site.json → team_logos)
  - ascunde / afișează sezoane pe site (config/site.json → hidden_seasons)
  - publică: git pull --rebase, commit, push → Vercel face deploy

Ascultă doar pe 127.0.0.1 — nu are login, nu-l expune în rețea.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import threading
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent.parent
ADMIN = Path(__file__).resolve().parent
PROCESSED = ROOT / "data" / "processed"
SITE_JSON = ROOT / "config" / "site.json"
AUTO_JSON = ROOT / "config" / "auto_fetch.json"
TEAM_IDS = ROOT / "config" / "team_ids.json"
LOGO_DIR = ROOT / "player-profiles" / "assets" / "logos"
LOG_DIR = ROOT / "logs" / "admin"
PY = sys.executable

COMPETITIONS = {"eurocup": "EuroCup", "aba_league": "ABA League", "liga_nationala": "Liga Națională"}
LOGO_TYPES = {"image/svg+xml": ".svg", "image/png": ".png", "image/webp": ".webp", "image/jpeg": ".jpg"}
MAX_LOGO_BYTES = 2 * 1024 * 1024

app = FastAPI(title="U-BT admin", docs_url=None, redoc_url=None)


# ── Helpers ────────────────────────────────────────────────────────────────

def _read_json(path: Path, default: dict) -> dict:
    try:
        return {**default, **json.loads(path.read_text())}
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(default)


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def _site() -> dict:
    return _read_json(SITE_JSON, {"hidden_seasons": [], "team_logos": {}})


def _git(*args: str, timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout)


def _slug(name: str) -> str:
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-") or "echipa"


def _season_label(s: int) -> str:
    return f"{s}–{s + 1}"


# ── Fetch (un singur proces o dată) ────────────────────────────────────────

class FetchRunner:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.proc: subprocess.Popen | None = None
        self.current: dict | None = None

    def start(self, only: str | None, force: bool) -> dict:
        with self.lock:
            if self.proc and self.proc.poll() is None:
                raise HTTPException(409, "Un fetch rulează deja.")
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
            cmd = [PY, "fetchers/run_all.py"]
            if only:
                cmd += ["--only", only]
            if force:
                cmd.append("--force")
            log_path = LOG_DIR / f"{run_id}.log"
            meta = {"id": run_id, "only": only, "force": force, "started": datetime.now(timezone.utc).isoformat(),
                    "finished": None, "exit_code": None}
            (LOG_DIR / f"{run_id}.json").write_text(json.dumps(meta))
            fh = log_path.open("w")
            self.proc = subprocess.Popen(cmd, cwd=ROOT, stdout=fh, stderr=subprocess.STDOUT, text=True)
            self.current = meta
            threading.Thread(target=self._wait, args=(self.proc, fh, meta), daemon=True).start()
            return meta

    def _wait(self, proc: subprocess.Popen, fh, meta: dict) -> None:
        code = proc.wait()
        fh.close()
        meta.update(finished=datetime.now(timezone.utc).isoformat(), exit_code=code)
        (LOG_DIR / f"{meta['id']}.json").write_text(json.dumps(meta))

    def running(self) -> bool:
        return bool(self.proc and self.proc.poll() is None)


runner = FetchRunner()


def _run_summary(log_text: str) -> str:
    """Scoate din log ce s-a întâmplat, în câteva cuvinte."""
    new_games = re.findall(r"(EuroCup|ABA League|Liga Națională|ABA|LN) \d{4}.*?(\d+) noi de fetch-uit", log_text)
    rows = re.findall(r"\((\d+) rânduri noi", log_text)
    errors = len(re.findall(r"\bERROR\b", log_text))
    parts = []
    added = sum(int(n) for _, n in new_games)
    if added:
        parts.append(f"{added} meci{'uri' if added != 1 else ''} nou{'i' if added != 1 else ''}")
    elif rows and any(int(r) for r in rows):
        parts.append("date noi")
    else:
        parts.append("nimic nou")
    if "SKIP" in log_text and not added:
        parts.append("unele competiții sărite (fetch recent)")
    if errors:
        parts.append(f"{errors} erori")
    return " · ".join(parts)


# ── API ────────────────────────────────────────────────────────────────────

@app.get("/api/status")
def status():
    last = _read_json(PROCESSED / "last_updated.json", {"competitions": {}})
    team = _read_json(TEAM_IDS, {"competitions": {}})
    comps = []
    for key, label in COMPETITIONS.items():
        comps.append({"key": key, "label": label, "last_fetch": last.get("competitions", {}).get(key),
                      "status": team["competitions"].get(key, {}).get("status", "active")})
    g = pd.read_csv(PROCESSED / "games_normalized.csv")
    last_games = (g.sort_values("date").groupby("competition_key").tail(1)
                  [["competition_key", "date", "opponent", "ubt_score", "opp_score", "result"]].to_dict("records"))
    for c in comps:
        lg = next((x for x in last_games if x["competition_key"] == c["key"]), None)
        c["last_game"] = lg and {**lg, "date": str(lg["date"])[:10]}

    _git("fetch", "-q", "origin", timeout=20)
    ahead_behind = _git("rev-list", "--left-right", "--count", "HEAD...@{u}").stdout.split()
    changed = [l for l in _git("status", "--porcelain").stdout.splitlines() if l.strip()]
    return {
        "competitions": comps,
        "git": {
            "branch": _git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip(),
            "ahead": int(ahead_behind[0]) if ahead_behind else 0,
            "behind": int(ahead_behind[1]) if len(ahead_behind) > 1 else 0,
            "changed": changed,
            "last_commit": _git("log", "-1", "--format=%h · %s · %cr").stdout.strip(),
        },
        "fetch_running": runner.running(),
    }


@app.post("/api/fetch")
def fetch_start(only: str = Form(""), force: bool = Form(False)):
    if only and only not in COMPETITIONS:
        raise HTTPException(400, "Competiție necunoscută.")
    return runner.start(only or None, force)


@app.get("/api/fetch/runs")
def fetch_runs():
    out = []
    for meta_path in sorted(LOG_DIR.glob("*.json"), reverse=True)[:20]:
        meta = json.loads(meta_path.read_text())
        log = (LOG_DIR / f"{meta['id']}.log")
        text = log.read_text(errors="replace") if log.exists() else ""
        meta["summary"] = _run_summary(text) if meta.get("finished") else "rulează…"
        out.append(meta)
    return out


@app.get("/api/fetch/runs/{run_id}")
def fetch_log(run_id: str):
    if not re.fullmatch(r"\d{8}-\d{6}", run_id):
        raise HTTPException(404)
    log = LOG_DIR / f"{run_id}.log"
    meta = LOG_DIR / f"{run_id}.json"
    if not meta.exists():
        raise HTTPException(404)
    return {**json.loads(meta.read_text()), "log": log.read_text(errors="replace") if log.exists() else ""}


@app.get("/api/auto")
def auto_get():
    cfg = _read_json(AUTO_JSON, {"enabled": False, "every_days": 2})
    bot = _git("log", "-5", "--author=github-actions", "--format=%h · %s · %cr", "origin/main").stdout.strip()
    return {**cfg, "recent_runs": [l for l in bot.splitlines() if l]}


@app.post("/api/auto")
def auto_set(enabled: bool = Form(...), every_days: int = Form(...)):
    if every_days not in (1, 2, 3, 4, 5, 7):
        raise HTTPException(400, "Interval invalid.")
    _write_json(AUTO_JSON, {"enabled": enabled, "every_days": every_days})
    return auto_get()


@app.get("/api/teams")
def teams():
    g = pd.read_csv(PROCESSED / "games_normalized.csv")
    logos = _site()["team_logos"]
    agg = g.groupby("opponent").agg(games=("game_code", "size"), last=("date", "max"),
                                    comps=("competition", lambda s: sorted(set(s)))).reset_index()
    agg = agg.sort_values("last", ascending=False)
    return [{"name": r.opponent, "games": int(r.games), "last": str(r.last)[:10], "competitions": r.comps,
             "logo": logos.get(r.opponent)} for r in agg.itertuples()]


@app.post("/api/teams/logo")
async def logo_upload(name: str = Form(...), file: UploadFile = File(...)):
    ext = LOGO_TYPES.get(file.content_type or "")
    if not ext:
        raise HTTPException(400, "Format nesuportat: folosește SVG, PNG, WebP sau JPG.")
    data = await file.read()
    if len(data) > MAX_LOGO_BYTES:
        raise HTTPException(400, "Fișierul e mai mare de 2 MB.")
    if ext == ".svg" and re.search(rb"<script|on\w+\s*=", data, re.I):
        raise HTTPException(400, "SVG-ul conține scripturi; exportă-l din nou fără ele.")
    site = _site()
    old = site["team_logos"].get(name)
    # numele fișierului include un hash scurt → la înlocuire browserele nu păstrează logo-ul vechi
    fname = f"{_slug(name)}-{hashlib.sha1(data).hexdigest()[:8]}{ext}"
    LOGO_DIR.mkdir(parents=True, exist_ok=True)
    (LOGO_DIR / fname).write_bytes(data)
    site["team_logos"][name] = f"assets/logos/{fname}"
    _write_json(SITE_JSON, site)
    _remove_unused_logo(old, site)
    return {"name": name, "logo": site["team_logos"][name]}


@app.delete("/api/teams/logo")
def logo_delete(name: str):
    site = _site()
    old = site["team_logos"].pop(name, None)
    if old is None:
        raise HTTPException(404, "Echipa nu are logo.")
    _write_json(SITE_JSON, site)
    _remove_unused_logo(old, site)
    return {"name": name, "logo": None}


def _remove_unused_logo(path: str | None, site: dict) -> None:
    """Șterge fișierul vechi doar dacă nu-l mai folosește nicio echipă."""
    if not path or path in site["team_logos"].values():
        return
    f = (ROOT / "player-profiles" / path).resolve()
    if f.parent == LOGO_DIR.resolve() and f.exists():
        f.unlink()


@app.get("/api/seasons")
def seasons():
    g = pd.read_csv(PROCESSED / "games_normalized.csv")
    hidden = {str(s) for s in _site()["hidden_seasons"]}
    out = []
    for s, grp in sorted(g.groupby("season"), reverse=True):
        out.append({"season": int(s), "label": _season_label(int(s)), "games": len(grp),
                    "competitions": sorted(set(grp["competition"])), "hidden": str(s) in hidden})
    return out


@app.post("/api/seasons")
def seasons_set(hidden: str = Form("")):
    hid = sorted({int(x) for x in hidden.split(",") if x.strip()})
    all_seasons = {x["season"] for x in seasons()}
    if not all_seasons - set(hid):
        raise HTTPException(400, "Trebuie să rămână vizibil cel puțin un sezon.")
    site = _site()
    site["hidden_seasons"] = hid
    _write_json(SITE_JSON, site)
    return seasons()


@app.get("/api/publish")
def publish_preview():
    files = []
    for line in _git("status", "--porcelain", "--untracked-files=all").stdout.splitlines():
        files.append({"status": line[:2].strip(), "path": line[3:]})
    return {"files": files, "default_message": f"data: update {datetime.now():%Y-%m-%d}"}


@app.post("/api/publish")
def publish(message: str = Form(...)):
    if runner.running():
        raise HTTPException(409, "Așteaptă să se termine fetch-ul.")
    message = message.strip() or f"data: update {datetime.now():%Y-%m-%d}"
    steps = []

    def step(label: str, *args: str, timeout: int = 120) -> bool:
        r = _git(*args, timeout=timeout)
        steps.append({"step": label, "ok": r.returncode == 0, "output": (r.stdout + r.stderr).strip()})
        return r.returncode == 0

    has_changes = bool(_git("status", "--porcelain").stdout.strip())
    if has_changes:
        if not (step("Adaug fișierele", "add", "-A") and step("Commit", "commit", "-m", message)):
            return {"ok": False, "steps": steps}
    # GitHub Actions poate fi făcut commit între timp (fetch automat)
    if not step("Iau modificările de pe GitHub", "pull", "--rebase", "--autostash", "origin", "main"):
        step("Anulez rebase-ul", "rebase", "--abort")
        return {"ok": False, "steps": steps}
    ok = step("Push", "push", "origin", "HEAD:main")
    return {"ok": ok, "steps": steps}


# ── Pagina ─────────────────────────────────────────────────────────────────

app.mount("/logos", StaticFiles(directory=LOGO_DIR), name="logos")
app.mount("/static", StaticFiles(directory=ADMIN / "static"), name="static")


@app.get("/", response_class=HTMLResponse)
def index():
    return FileResponse(ADMIN / "static" / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8770, log_level="warning")
