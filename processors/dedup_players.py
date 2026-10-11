"""
Deduplicare jucători cu ID-uri diferite în competiții diferite.

Cum funcționează:
  1. Normalizează numele (lowercase, fără diacritice/apostrofe/punctuație).
  2. Grupează rândurile per (player_id, competition_key) — un jucător poate apărea
     în mai multe competiții cu ID-uri diferite.
  3. Compară fiecare pereche (player A, player B) din competiții diferite.
     Dacă similaritatea numelor e ≥ THRESHOLD → sunt același jucător.
  4. Canonicalizare: preferă ID-ul din EuroCup; dacă nu există, cel mai mic ID numeric.
  5. Actualizează player_stats_normalized.csv și player_stats_per_game_normalized.csv.
     Poza folosită: prima disponibilă din surse în ordinea PHOTO_PRIORITY.

Rulare:
  python3 processors/dedup_players.py [--dry-run] [--threshold 0.88]
"""

import argparse
import csv
import pathlib
import re
import unicodedata
from difflib import SequenceMatcher
from collections import defaultdict

ROOT = pathlib.Path(__file__).resolve().parent.parent
PROCESSED = ROOT / 'data' / 'processed'

STATS_FILES = [
    PROCESSED / 'player_stats_normalized.csv',
    PROCESSED / 'player_stats_per_game_normalized.csv',
]

# Ordinea preferată pentru ID canonic și poze
PHOTO_PRIORITY = ['eurocup', 'aba_league', 'liga_nationala', 'cupa_romaniei']

THRESHOLD = 0.88  # similaritate minimă pentru match automat


def normalize_name(name: str) -> str:
    """Normalizează un nume pentru comparare: fără diacritice, punctuație, spații extra."""
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    name = name.lower()
    name = re.sub(r"[''`\-\.]", '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def find_duplicates(rows: list, threshold: float) -> list:
    """
    Returnează lista de tuple (old_id, canonical_id, canonical_name, sim)
    pentru jucătorii detectați ca duplicați între competiții diferite.
    """
    players = {}
    for r in rows:
        pid = r['player_id']
        if pid not in players:
            players[pid] = r

    pid_to_comps = defaultdict(set)
    for r in rows:
        pid_to_comps[r['player_id']].add(r['competition_key'])

    pid_list = list(players.keys())
    merged = []
    seen = set()

    for i, pid_a in enumerate(pid_list):
        if pid_a in seen:
            continue
        ra = players[pid_a]
        na = normalize_name(ra['player_name'])
        comps_a = pid_to_comps[pid_a]

        for pid_b in pid_list[i + 1:]:
            if pid_b in seen:
                continue
            rb = players[pid_b]
            comps_b = pid_to_comps[pid_b]

            # Compară doar jucători din competiții diferite
            if comps_a & comps_b:
                continue

            nb = normalize_name(rb['player_name'])
            sim = similarity(na, nb)
            if sim >= threshold:
                # Preferă EuroCup ca ID canonic
                if 'eurocup' in comps_a:
                    canonical_id, canonical_name, other_id = pid_a, ra['player_name'], pid_b
                elif 'eurocup' in comps_b:
                    canonical_id, canonical_name, other_id = pid_b, rb['player_name'], pid_a
                else:
                    canonical_id = min(pid_a, pid_b, key=lambda x: int(x) if x.isdigit() else 0)
                    canonical_name = players[canonical_id]['player_name']
                    other_id = pid_b if canonical_id == pid_a else pid_a

                merged.append((other_id, canonical_id, canonical_name, sim))
                seen.add(other_id)

    return merged


def best_photo(rows_for_player: list) -> str:
    """Alege poza din sursa cu prioritatea cea mai mare."""
    by_comp = {}
    for r in rows_for_player:
        comp = r.get('competition_key', '')
        url  = r.get('image_url', '').strip()
        if url and comp not in by_comp:
            by_comp[comp] = url
    for comp in PHOTO_PRIORITY:
        if comp in by_comp:
            return by_comp[comp]
    return ''


def apply_merge(path: pathlib.Path, merge_map: dict, dry_run: bool) -> int:
    rows = list(csv.DictReader(path.open()))
    changed = 0
    for r in rows:
        if r['player_id'] in merge_map:
            cid, cname, cphoto = merge_map[r['player_id']]
            r['player_id']   = cid
            r['player_name'] = cname
            if cphoto:
                r['image_url'] = cphoto
            changed += 1
    if not dry_run:
        with path.open('w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)
    return changed


def main():
    parser = argparse.ArgumentParser(description='Deduplică jucători cu ID-uri diferite')
    parser.add_argument('--dry-run', action='store_true',
                        help='Doar afișează ce s-ar schimba, fără să modifice fișierele')
    parser.add_argument('--threshold', type=float, default=THRESHOLD,
                        help=f'Similaritate minimă pentru match automat (default {THRESHOLD})')
    args = parser.parse_args()

    stats_path = STATS_FILES[0]
    if not stats_path.exists():
        print(f'Fișier lipsă: {stats_path}')
        return

    rows = list(csv.DictReader(stats_path.open()))
    duplicates = find_duplicates(rows, args.threshold)

    if not duplicates:
        print('Niciun duplicat detectat.')
        return

    prefix = 'DRY RUN — ' if args.dry_run else ''
    print(f'{prefix}Duplicați detectați ({len(duplicates)}):')
    pid_to_name = {r['player_id']: r['player_name'] for r in rows}
    for old_id, can_id, can_name, sim in duplicates:
        old_name = pid_to_name.get(old_id, '?')
        print(f'  [{sim:.2f}] "{old_name}" (id={old_id})  →  "{can_name}" (id={can_id})')

    # Construiește merge_map cu cea mai bună poză
    pid_to_rows = defaultdict(list)
    for r in rows:
        pid_to_rows[r['player_id']].append(r)

    merge_map = {}
    for old_id, can_id, can_name, _ in duplicates:
        all_rows = pid_to_rows[old_id] + pid_to_rows[can_id]
        photo = best_photo(all_rows)
        merge_map[old_id] = (can_id, can_name, photo)

    print()
    for path in STATS_FILES:
        if not path.exists():
            print(f'  LIPSĂ: {path.name}')
            continue
        changed = apply_merge(path, merge_map, args.dry_run)
        status = '(dry-run, neschimbat)' if args.dry_run else 'rânduri actualizate'
        print(f'  {path.name}: {changed} {status}')


if __name__ == '__main__':
    main()
