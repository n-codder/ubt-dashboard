# Manual de utilizare — U-BT Dashboard

---

## Cuprins

1. [Fluxul complet](#1-fluxul-complet)
2. [Cum actualizezi după un meci nou](#2-cum-actualizezi-după-un-meci-nou)
3. [Fetcheri — detalii per competiție](#3-fetcheri--detalii-per-competiție)
4. [Procesoare](#4-procesoare)
5. [Upload Google Sheets](#5-upload-google-sheets)
6. [Pagina cu jucători (GitHub Pages)](#6-pagina-cu-jucători-github-pages)
7. [Referință rapidă — comenzi](#7-referință-rapidă--comenzi)

---

## 1. Fluxul complet

```
Fetcher (API)
    ↓
data/raw/          ← date brute, nu se commitează
    ↓
processors/normalizer.py
processors/per_game_stats.py
    ↓
data/processed/    ← CSV-uri finale, se commitează
    ↓  ↓
    │  └─ player-profiles/  ← citite direct de pagina de jucători
    ↓
uploaders/sheets_uploader.py
    ↓
Google Sheets → Looker Studio
```

---

## 2. Cum actualizezi după un meci nou

### Varianta rapidă (toate competițiile)

```bash
python fetchers/run_all.py
python uploaders/sheets_uploader.py
```

`run_all.py` rulează automat și procesoarele dacă găsește date noi.  
Dacă vrei să forțezi fetch chiar dacă ultimul a fost în ultimele 24h:

```bash
python fetchers/run_all.py --force
```

---

### Varianta manuală (o singură competiție)

**Pas 1 — Fetch**

```bash
# ABA League
python fetchers/aba_fetcher.py --season 2025

# Liga Națională
python fetchers/ln_fetcher.py --season 2026

# EuroCup (sezon încheiat — nu mai e nevoie)
python fetchers/eurocup_fetcher.py --season 2025
```

**Pas 2 — Procesare**

```bash
python processors/normalizer.py
python processors/per_game_stats.py
```

**Pas 3 — Upload Sheets**

```bash
python uploaders/sheets_uploader.py
```

**Pas 4 — Commit date procesate**

```bash
git add data/processed/
git commit -m "data: update $(date +%Y-%m-%d)"
git push
```

---

## 3. Fetcheri — detalii per competiție

| Competiție | Script | Sezon | Status |
|------------|--------|-------|--------|
| EuroCup | `fetchers/eurocup_fetcher.py --season 2025` | 2025 | Încheiat |
| ABA League | `fetchers/aba_fetcher.py --season 2025` | 2025 | Activ |
| Liga Națională | `fetchers/ln_fetcher.py --season 2026` | 2026 | Activ |
| Cupa României | — | — | Neînceput |

**Skip automat în `run_all.py`:**
- Competițiile cu `status: finished` sunt ignorate permanent.
- Competițiile cu `status: not_started` sunt ignorate până la primul meci.
- Dacă s-a fetch-uit în ultimele 24h, e ignorată (bypass cu `--force`).

**Re-fetch complet** (rescrie tot istoricul):

```bash
python fetchers/aba_fetcher.py --season 2025 --full
python fetchers/ln_fetcher.py  --season 2026 --full
```

---

## 4. Procesoare

### normalizer.py

Citește `data/processed/games.csv` și `player_stats.csv`, normalizează și salvează versiunile `*_normalized.csv`.

```bash
python processors/normalizer.py           # ambele
python processors/normalizer.py --games-only
python processors/normalizer.py --players-only
```

### per_game_stats.py

Generează `player_stats_per_game_normalized.csv` — statistici individuale per meci.

```bash
python processors/per_game_stats.py           # incremental (doar meciuri noi)
python processors/per_game_stats.py --full    # re-procesează tot
```

> **Notă:** Sezonul pentru EuroCup/ABA e `--season 2025` (default).  
> Liga Națională folosește automat sezonul 2026 din `config/team_ids.json`.

---

## 5. Upload Google Sheets

```bash
python uploaders/sheets_uploader.py                    # toate taburile
python uploaders/sheets_uploader.py --tab games        # doar games
python uploaders/sheets_uploader.py --tab player_stats
python uploaders/sheets_uploader.py --tab player_stats_per_game
python uploaders/sheets_uploader.py --dry-run          # test fără scriere
```

**Taburi actualizate:**

| Tab Sheets | Sursă CSV |
|------------|-----------|
| `games` | `data/processed/games_normalized.csv` |
| `player_stats` | `data/processed/player_stats_normalized.csv` |
| `player_stats_per_game` | `data/processed/player_stats_per_game_normalized.csv` |

După upload, în **Looker Studio** dai **Refresh data** (colț dreapta sus).

---

## 6. Pagina cu jucători (GitHub Pages)

Pagina citește direct din `data/processed/` — nu are nevoie de Sheets.

**Actualizare:**

1. Rulează procesoarele (Pasul 2 de mai sus)
2. Commit + push CSV-urile procesate:

```bash
git add data/processed/
git commit -m "data: update $(date +%Y-%m-%d)"
git push
```

GitHub Pages actualizează automat în câteva secunde după push.

**Local (preview înainte de push):**

```bash
# Din root-ul proiectului
python3 -m http.server 8000
# Deschide: http://localhost:8000/player-profiles/
```

---

## 7. Referință rapidă — comenzi

### Update complet după meciuri noi

```bash
python fetchers/run_all.py
python uploaders/sheets_uploader.py
git add data/processed/ && git commit -m "data: update $(date +%Y-%m-%d)" && git push
```

### Update forțat (ignoră cache 24h)

```bash
python fetchers/run_all.py --force
python uploaders/sheets_uploader.py
git add data/processed/ && git commit -m "data: update $(date +%Y-%m-%d)" && git push
```

### Update doar o competiție

```bash
python fetchers/run_all.py --only aba_league
python uploaders/sheets_uploader.py
git add data/processed/ && git commit -m "data: update aba $(date +%Y-%m-%d)" && git push
```

### Re-fetch complet (după erori de date)

```bash
python fetchers/aba_fetcher.py --season 2025 --full
python fetchers/ln_fetcher.py  --season 2026 --full
python processors/normalizer.py
python processors/per_game_stats.py --full
python uploaders/sheets_uploader.py
git add data/processed/ && git commit -m "data: full rebuild $(date +%Y-%m-%d)" && git push
```
