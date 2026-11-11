# U-BT în cifre

Dashboard cu statisticile echipei de baschet **U-BT Cluj-Napoca**: meciuri, jucători și echipă, pe sezoane.
Competiții urmărite: **EuroCup · ABA League · Liga Națională**.

Proiect de fan, necomercial. Nu este afiliat clubului U-BT Cluj-Napoca sau organizatorilor competițiilor.

## Ce conține site-ul

Site-ul este static (HTML + JS, fără build) și citește datele direct din CSV-urile din `data/processed/`.

- **Start / Echipă** — rezultate, formă, statistici de echipă
- **Meci** — scor pe sferturi, comparația pe echipe, statisticile fiecărui jucător U-BT
- **Jucători** — clasamente pe medii și totaluri
- **Profil jucător** — meciurile sezonului, medii, comparație cu restul echipei, istoric pe sezoane

Pornire locală:

```bash
python3 -m http.server 8765
# apoi deschide http://localhost:8765/player-profiles/
```

## Cum se actualizează datele

```bash
pip install -r requirements.txt
cp .env.example .env              # completează RAPIDAPI_KEY (pentru ABA și LN)
python fetchers/run_all.py        # fetch + procesare; --force ignoră cache-ul de 24h, --only eurocup
```

După o schimbare în `player-profiles/assets/`, crește versiunea `?v=` din paginile HTML ca browserele să nu păstreze fișierele vechi.

## Structură

| Folder | Rol |
|--------|-----|
| `fetchers/` | Aduc datele din API-uri (`run_all.py` rulează tot, cu procesoarele) |
| `processors/` | Normalizare nume și ID-uri, statistici pe meci, scor pe sferturi |
| `data/processed/` | CSV-urile folosite de site |
| `config/` | ID-uri echipă și competiții, bio jucători, mapare ID-uri |
| `player-profiles/` | Site-ul |

## Surse date

| Competiție | Sursă |
|------------|-------|
| EuroCup | [euroleague-api](https://pypi.org/project/euroleague-api/) |
| ABA League, Liga Națională | [API-Basketball](https://api-basketball.com) (RapidAPI) |

Logo-urile echipelor aparțin cluburilor respective și sunt folosite doar pentru identificarea echipelor.
