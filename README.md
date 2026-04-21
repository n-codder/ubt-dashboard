# U-BT Cluj-Napoca — Performance Dashboard 2025–2026

Dashboard de performanță pentru sezonul 2025–2026 al echipei **U-BT Cluj-Napoca**.  
Competiții urmărite: **EuroCup · ABA League · Liga Națională · Cupa României**

---

## Arhitectură

```
Python scripts → CSV-uri → Google Sheets → Looker Studio
                                        ↘ GitHub Pages (profiluri jucători)
```

## Quick Start

```bash
# 1. Clonează repo
git clone https://github.com/tu/ubt-dashboard.git
cd ubt-dashboard

# 2. Setup inițial (creează .env, instalează deps)
python setup.py

# 3. Completează .env cu cheile API

# 4. Fetch date
python fetchers/run_all.py

# 5. Upload în Google Sheets
python uploaders/sheets_uploader.py
```

## Structură

| Folder | Rol |
|--------|-----|
| `fetchers/` | Fetch date din API-uri |
| `processors/` | Normalizare, agregare, export CSV |
| `uploaders/` | Sync cu Google Sheets |
| `player-profiles/` | Site GitHub Pages (profiluri jucători) |
| `data/processed/` | CSV-uri commituite în git |
| `data/raw/` | Date brute — **nu se commitează** |
| `config/` | Setări, ID-uri echipă |
| `research/` | Faza 2 — analiză la nivel de ligă |

## Surse date

| Competiție | Sursă | Note |
|------------|-------|-------|
| EuroCup | [euroleague-api](https://pypi.org/project/euroleague-api/) | Gratuit, fără limită |
| ABA League | [api-basketball.com](https://api-basketball.com) | 100 req/zi |
| Liga Națională | api-basketball.com | 100 req/zi |
| Cupa României | api-basketball.com | 100 req/zi |

## Update date

```bash
python fetchers/run_all.py          # fetch toate competițiile
python uploaders/sheets_uploader.py  # sync Google Sheets
git add data/processed/ && git commit -m "data: update $(date +%Y-%m-%d)"
git push                             # GitHub Pages actualizat automat
```

## Status

- [ ] Etapa 1 — Setup & structură
- [ ] Etapa 2 — Fetcheri (EuroCup, ABA, LN, Cupă)
- [ ] Etapa 3 — Procesare & export CSV
- [ ] Etapa 4 — Looker Studio dashboard
- [ ] Etapa 5 — GitHub Pages profiluri jucători
- [ ] Faza 2 — Cercetare "Cât de european e baschetul european?"
