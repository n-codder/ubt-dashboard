/* ─── Config ─────────────────────────────────────────────────────────────── */
// Relative path works on GitHub Pages and locally via http-server.
// For GitHub raw: 'https://raw.githubusercontent.com/USER/REPO/main/data/processed/...'
const _CB = Date.now();
const DATA_URL          = `../data/processed/player_stats_normalized.csv?v=${_CB}`;
const PER_GAME_DATA_URL = `../data/processed/player_stats_per_game_normalized.csv?v=${_CB}`;
const DETAILS_URL       = `../data/processed/player_details.csv?v=${_CB}`;
const BIO_URL           = `../config/player_bio.json?v=${_CB}`;

const COMP_LABEL = {
  eurocup:        'EuroCup',
  aba_league:     'ABA League',
  liga_nationala: 'Liga Națională',
  cupa_romaniei:  'Cupa României',
};

const COMP_ORDER = ['eurocup', 'aba_league', 'liga_nationala', 'cupa_romaniei'];

const EVOLUTION_STATS = [
  { key: 'points',        label: 'Puncte' },
  { key: 'pir',           label: 'PIR' },
  { key: 'assists',       label: 'Pase decisive' },
  { key: 'rebounds',      label: 'Recuperări' },
  { key: 'steals',        label: 'Intercepții' },
  { key: 'blocks',        label: 'Blocaje' },
  { key: 'minutes_played',label: 'Minute jucate' },
  { key: 'turnovers',     label: 'Pierderi' },
];

/* Maps sort key → per-game and total stat metadata for the player card */
const SORT_META = {
  points_pg:   { pgKey: 'points_pg',  pgLabel: 'Puncte/meci',   pgFmt: v => fmt(v),
                 totalKey: 'points',   totalLabel: 'Total puncte',   totalFmt: v => fmt(v, 0) },
  pir:         { pgKey: null,          pgLabel: 'PIR/meci',       pgFmt: v => fmt(v),
                 totalKey: 'pir',      totalLabel: 'PIR total',       totalFmt: v => fmt(v, 0) },
  assists_pg:  { pgKey: 'assists_pg', pgLabel: 'Pase/meci',      pgFmt: v => fmt(v),
                 totalKey: 'assists',  totalLabel: 'Total pase',       totalFmt: v => fmt(v, 0) },
  rebounds_pg: { pgKey: 'rebounds_pg',pgLabel: 'Reb/meci',       pgFmt: v => fmt(v),
                 totalKey: 'rebounds', totalLabel: 'Total recuperări', totalFmt: v => fmt(v, 0) },
  blocks_pg:   { pgKey: 'blocks_pg',  pgLabel: 'Blocaje/meci',   pgFmt: v => fmt(v),
                 totalKey: 'blocks',   totalLabel: 'Total blocaje',    totalFmt: v => fmt(v, 0) },
  minutes_pg:  { pgKey: 'minutes_pg', pgLabel: 'Min/meci',       pgFmt: v => fmtMinutes(v),
                 totalKey: 'minutes',  totalLabel: 'Min total',        totalFmt: v => fmt(v, 0) },
  fg2_pct:     { pgKey: 'fg2_pct',   pgLabel: 'FG2%',           pgFmt: v => v != null ? fmtPct(v)+'%' : '—',
                 totalKey: null,       totalLabel: '',                 totalFmt: () => '' },
  fg3_pct:     { pgKey: 'fg3_pct',   pgLabel: 'FG3%',           pgFmt: v => v != null ? fmtPct(v)+'%' : '—',
                 totalKey: null,       totalLabel: '',                 totalFmt: () => '' },
  ft_pct:      { pgKey: 'ft_pct',    pgLabel: 'FT%',            pgFmt: v => v != null ? fmtPct(v)+'%' : '—',
                 totalKey: null,       totalLabel: '',                 totalFmt: () => '' },
  player_name: { pgKey: 'points_pg',  pgLabel: 'Puncte/meci',   pgFmt: v => fmt(v),
                 totalKey: 'points',   totalLabel: 'Total puncte',   totalFmt: v => fmt(v, 0) },
};

const COMP_COLOR = {
  eurocup:        '#f6a800',
  aba_league:     '#00b4d8',
  liga_nationala: '#e63946',
  cupa_romaniei:  '#7c3aed',
};

/* ─── Data loading ───────────────────────────────────────────────────────── */
async function loadData() {
  return new Promise((resolve, reject) => {
    Papa.parse(DATA_URL, {
      download:      true,
      header:        true,
      dynamicTyping: true,
      skipEmptyLines: true,
      complete: ({ data }) => resolve(data.filter(r => r.player_id)),
      error:    (err)      => reject(err),
    });
  });
}

/* ─── Helpers ────────────────────────────────────────────────────────────── */
function formatName(name) {
  if (!name) return '';
  // "RUSSELL, DARON" → "Daron Russell"
  if (name.includes(',') && name === name.toUpperCase()) {
    const [last, first] = name.split(',').map(s => s.trim().toLowerCase()
      .replace(/\b(\w)/g, c => c.toUpperCase()));
    return `${first} ${last}`;
  }
  return name;
}

function getInitials(name) {
  return name.split(' ').slice(0, 2)
    .map(w => w[0]?.toUpperCase() || '')
    .join('');
}

function fmt(val, dec = 1) {
  if (val === null || val === undefined || isNaN(val)) return '—';
  return Number(val).toFixed(dec);
}

function fmtMinutes(val) {
  if (val === null || val === undefined || isNaN(val)) return '—';
  const totalSec = Math.round(Number(val) * 60);
  const m = Math.floor(totalSec / 60);
  const s = totalSec % 60;
  return `${m}:${String(s).padStart(2, '0')}`;
}

function fmtPct(val) {
  if (val === null || val === undefined || isNaN(val)) return null;
  return (Number(val) * 100).toFixed(1);
}

function seasonLabel(s) {
  if (s == null) return 'Toate sezoanele';
  const n = +s;
  return `${n}–${n + 1}`;
}

function shootClass(pct, fg3 = false, ft = false) {
  if (pct === null) return { bar: 'bar-none', val: 'val-none' };
  if (ft)  return pct >= 78 ? { bar: 'bar-good', val: 'val-good' }
                : pct >= 65 ? { bar: 'bar-avg',  val: 'val-avg'  }
                            : { bar: 'bar-poor', val: 'val-poor' };
  if (fg3) return pct >= 38 ? { bar: 'bar-good', val: 'val-good' }
                : pct >= 30 ? { bar: 'bar-avg',  val: 'val-avg'  }
                            : { bar: 'bar-poor', val: 'val-poor' };
  /* fg2 */    return pct >= 55 ? { bar: 'bar-good', val: 'val-good' }
                : pct >= 45 ? { bar: 'bar-avg',  val: 'val-avg'  }
                            : { bar: 'bar-poor', val: 'val-poor' };
}

/* Group rows by player_id → Map<id, {id, name, rows}> */
function groupPlayers(rows) {
  const map = new Map();
  for (const row of rows) {
    const name = formatName(row.player_name);
    if (!map.has(row.player_id)) map.set(row.player_id, { id: row.player_id, name, rows: [] });
    map.get(row.player_id).rows.push({ ...row, player_name: name });
  }
  return [...map.values()];
}

/* Pick the "primary" row (highest-priority competition with most games) */
function primaryRow(player) {
  for (const key of COMP_ORDER) {
    const r = player.rows.find(r => r.competition_key === key);
    if (r) return r;
  }
  return player.rows[0];
}

/* ─── Leaderboard — Index page ───────────────────────────────────────────── */
let _activeSortKey = 'points_pg';

const SORT_LABELS = {
  points_pg:   'Puncte pe meci',
  pir:         'Eficiență (PIR)',
  assists_pg:  'Pase decisive pe meci',
  rebounds_pg: 'Recuperări pe meci',
  blocks_pg:   'Blocaje pe meci',
  minutes_pg:  'Minute jucate pe meci',
  fg2_pct:     'Procent din 2',
  fg3_pct:     'Procent din 3',
  ft_pct:      'Procent la libere',
  player_name: 'Nume A–Z',
};

function initIndex(rows) {
  // Season setup
  const allSeasons = [...new Set(rows.map(r => r.season).filter(Boolean))].map(Number).sort((a, b) => b - a);
  let activeSeason  = allSeasons[0] ?? null;

  // Populate #season-select
  const seasonSel = document.getElementById('season-select');
  if (seasonSel && allSeasons.length > 0) {
    seasonSel.innerHTML = allSeasons.map(s =>
      `<option value="${s}">${seasonLabel(s)}</option>`
    ).join('');
    seasonSel.value = String(activeSeason);
  }

  const seasonEl = document.getElementById('info-season');
  function updateBadge() {
    if (seasonEl) seasonEl.textContent = `Sezon: ${seasonLabel(activeSeason)}`;
  }
  updateBadge();

  function seasonRows() {
    return activeSeason == null ? rows : rows.filter(r => +r.season === activeSeason);
  }

  let filteredComp = 'all';
  let sortKey      = 'points_pg';
  _activeSortKey   = sortKey;

  const top3El    = document.getElementById('top3-grid');
  const rosterEl  = document.getElementById('roster-grid');
  const countEl   = document.getElementById('results-count');
  const sortLblEl = document.getElementById('sort-label');
  const titleEl   = document.getElementById('top3-title');

  function getStatRow(p) {
    return filteredComp !== 'all'
      ? (p.rows.find(r => r.competition_key === filteredComp) || primaryRow(p))
      : (p.rows.length > 1 ? _aggregateRows(p.rows) : primaryRow(p));
  }

  function sorted() {
    const sRows   = seasonRows();
    const players = groupPlayers(sRows);
    return players
      .filter(p => filteredComp === 'all' || p.rows.some(r => r.competition_key === filteredComp))
      .sort((a, b) => {
        if (sortKey === 'player_name') return a.name.localeCompare(b.name);
        const getVal = p => {
          const r = getStatRow(p);
          return sortKey === 'pir' ? (r.pir ?? -Infinity) : (r[sortKey] ?? -Infinity);
        };
        return getVal(b) - getVal(a);
      });
  }

  // Assigns proper ranks — tied values get the same rank number.
  // Players with null/missing stat for the active sort key are pushed to rest.
  function rankedList(list) {
    if (sortKey === 'player_name') {
      return list.map((p, i) => ({ player: p, rank: i + 1, val: p.name }));
    }
    const getVal = p => {
      const r = getStatRow(p);
      const v = sortKey === 'pir' ? r.pir : r[sortKey];
      return (v != null && !isNaN(v)) ? +v : null;
    };
    const withVals = list.map(p => ({ player: p, val: getVal(p) }));
    let rank = 1;
    return withVals.map(({ player, val }, i) => {
      if (i > 0 && val !== withVals[i - 1].val) rank = i + 1;
      return { player, rank, val };
    });
  }

  function render() {
    _activeSortKey = sortKey;
    const list  = sorted();
    const label = SORT_LABELS[sortKey] || sortKey;

    if (countEl)   countEl.textContent   = `${list.length} jucători în lot`;
    if (sortLblEl) sortLblEl.textContent = sortKey === 'player_name'
      ? 'Sortare curentă: Nume A–Z'
      : `Sortare curentă: ${label} (descrescător)`;

    const ranked = rankedList(list);
    // Top section: rank ≤ 3 with actual data (null-stat players go to rest)
    const topN = ranked.filter(({ rank, val }) =>
      rank <= 3 && (sortKey === 'player_name' || val !== null)
    );
    const rest = ranked.filter(({ rank, val }) =>
      rank > 3 || (sortKey !== 'player_name' && val === null)
    );

    if (titleEl) titleEl.textContent = `⭐ TOP ${topN.length} – după ${label}`;

    if (top3El) {
      top3El.innerHTML = topN.map(({ player, rank }) =>
        buildTop3Card(player, rank, getStatRow(player))
      ).join('');
      top3El.querySelector('.top3-card')?.classList.add('is-active');
    }
    updateCarouselDots(topN.length);
    if (rosterEl) rosterEl.innerHTML = rest.map(({ player, rank }) =>
      buildRosterCard(player, rank, getStatRow(player))
    ).join('');
  }

  // Filter buttons
  document.querySelectorAll('.filter-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      filteredComp = btn.dataset.comp;
      render();
    });
  });

  // Sort
  document.getElementById('sort-select').addEventListener('change', e => {
    sortKey = e.target.value;
    render();
  });

  // Season
  if (seasonSel) {
    seasonSel.addEventListener('change', e => {
      activeSeason = +e.target.value;
      updateBadge();
      render();
    });
  }

  render();

  // Carousel scroll → update dots + active card (added once)
  const grid = document.getElementById('top3-grid');
  if (grid) {
    grid.addEventListener('scroll', () => {
      const cards = grid.querySelectorAll('.top3-card');
      if (!cards.length) return;
      const cardWidth = cards[0].offsetWidth + 12; // gap 12px
      const idx = Math.min(Math.round(grid.scrollLeft / cardWidth), cards.length - 1);
      document.querySelectorAll('.carousel-dot').forEach((d, i) => d.classList.toggle('active', i === idx));
      cards.forEach((c, i) => c.classList.toggle('is-active', i === idx));
    }, { passive: true });
  }
}

function buildTop3Card(player, rank, row) {
  const isFirst  = rank === 1;
  const meta     = SORT_META[_activeSortKey] || SORT_META['points_pg'];
  const pgVal    = meta.pgKey ? row[meta.pgKey] : (row.pir != null ? row.pir / (row.games_played || 1) : null);
  const totVal   = row[meta.totalKey];
  const photoRow = primaryRow(player);
  const name     = player.name;

  return `
    <article class="top3-card${isFirst ? ' rank-1' : ''}"
             onclick="location.href='player.html?id=${player.id}'"
             role="button" tabindex="0" aria-label="${name}">
      <div class="top3-card-photo">
        <img src="${photoRow.image_url || ''}" alt="${name}"
             referrerpolicy="no-referrer"
             onload="this.style.opacity=1"
             onerror="this.closest('.top3-card-photo').classList.add('no-photo')"
             style="opacity:0;transition:opacity .3s">
        <div class="top3-card-photo-placeholder">${getInitials(name)}</div>
        <div class="top3-card-photo-gradient"></div>
        <span class="top3-rank-badge${isFirst ? ' rank-1' : ''}">${rank}</span>
        ${isFirst ? '<span class="top3-performer-badge">⭐ Top performer</span>' : ''}
      </div>
      <div class="top3-card-content">
        <h3 class="top3-name" title="${name}">${name}</h3>
        <div class="top3-stats-row">
          <div class="top3-stat-block">
            <span class="top3-stat-val">${meta.pgFmt(pgVal)}</span>
            <span class="top3-stat-lbl">${meta.pgLabel}</span>
          </div>
          ${totVal != null ? `
          <div class="top3-stats-sep"></div>
          <div class="top3-stat-block top3-stat-block--total">
            <span class="top3-total-val">${meta.totalFmt(totVal)}</span>
            <span class="top3-stat-lbl">${meta.totalLabel}</span>
          </div>` : ''}
        </div>
        ${totVal != null ? `<div class="top3-mobile-total">${meta.totalFmt(totVal)} total</div>` : ''}
        <div class="top3-see-profile">
          <span>Vezi profil complet</span>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M5 12h14M12 5l7 7-7 7"/></svg>
        </div>
      </div>
    </article>`;
}

function buildRosterCard(player, rank, row) {
  const meta     = SORT_META[_activeSortKey] || SORT_META['points_pg'];
  const pgVal    = meta.pgKey ? row[meta.pgKey] : (row.pir != null ? row.pir / (row.games_played || 1) : null);
  const totVal   = row[meta.totalKey];
  const photoRow = primaryRow(player);
  const name     = player.name;

  return `
    <article class="roster-card"
             onclick="location.href='player.html?id=${player.id}'"
             role="button" tabindex="0" aria-label="${name}">
      <span class="roster-rank">${rank}</span>
      <div class="roster-photo-wrap" id="rphoto-${player.id}">
        <img src="${photoRow.image_url || ''}" alt="${name}"
             referrerpolicy="no-referrer"
             onload="this.style.opacity=1"
             onerror="document.getElementById('rphoto-${player.id}').classList.add('no-photo')"
             style="opacity:0;transition:opacity .3s">
        <div class="roster-photo-placeholder">${getInitials(name)}</div>
      </div>
      <div class="roster-info">
        <div class="roster-name" title="${name}">${name}</div>
        <div class="roster-stat">
          <span class="roster-stat-val">${meta.pgFmt(pgVal)}</span>
          <span class="roster-stat-lbl">${meta.pgLabel}</span>
          ${totVal != null ? `<span class="roster-total">${meta.totalFmt(totVal)} total</span>` : ''}
        </div>
      </div>
      <div class="roster-arrow">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M5 12h14M12 5l7 7-7 7"/></svg>
      </div>
    </article>`;
}

function updateCarouselDots(count) {
  const el = document.getElementById('carousel-dots');
  if (!el) return;
  el.innerHTML = Array.from({ length: count }, (_, i) =>
    `<button class="carousel-dot${i === 0 ? ' active' : ''}" onclick="scrollTop3To(${i})" aria-label="Card ${i + 1}"></button>`
  ).join('');
}

function scrollTop3To(index) {
  const grid = document.getElementById('top3-grid');
  if (!grid) return;
  const cards = grid.querySelectorAll('.top3-card');
  if (!cards[index]) return;
  const cardWidth = cards[0].offsetWidth + 16;
  grid.scrollTo({ left: index * cardWidth, behavior: 'smooth' });
  document.querySelectorAll('.carousel-dot').forEach((d, i) => d.classList.toggle('active', i === index));
}

/* ─── Player page ────────────────────────────────────────────────────────── */
async function initPlayer(rows) {
  const params   = new URLSearchParams(location.search);
  const playerId = Number(params.get('id'));
  const players  = groupPlayers(rows);
  const player   = players.find(p => p.id === playerId);

  if (!player) {
    document.getElementById('player-content').innerHTML = `
      <div class="error-state">
        <div class="error-state-title">Jucătorul nu a fost găsit</div>
        <div class="error-state-sub">ID: ${playerId}</div>
      </div>`;
    return;
  }

  const row = primaryRow(player);

  // Set active season to newest the player appears in
  const playerSeasons = [...new Set(player.rows.map(r => r.season).filter(Boolean))]
    .map(Number).sort((a, b) => b - a);
  _activePlayerSeason = playerSeasons[0] ?? null;

  // Load per-game data, player details and bio in parallel
  let perGameRows = [], detailRows = [], bioData = { players: {} };
  try {
    [perGameRows, detailRows, bioData] = await Promise.all([loadPerGame(), loadDetails(), loadBio()]);
  } catch (_) { /* optional */ }

  const playerGames = perGameRows
    .filter(r => Number(r.player_id) === playerId)
    .sort((a, b) => new Date(a.date) - new Date(b.date));

  const detail = detailRows.find(d => Number(d.player_id) === playerId) || null;
  const bio    = bioData.players?.[String(playerId)] || null;

  document.title = `${player.name} · U-BT Stats`;
  document.getElementById('player-content').innerHTML = buildProfile(player, row, rows, playerGames, detail, bio);

  requestAnimationFrame(() => {
    document.querySelectorAll('.shooting-bar-fill[data-pct]').forEach(el => {
      el.style.width = el.dataset.pct + '%';
    });
    const { sRows, sGames } = _playerCtx;
    const multiComp  = sRows.length > 1;
    const activeRow  = multiComp ? _aggregateRows(sRows) : (sRows[0] || row);
    const initGames  = multiComp ? sGames : sGames.filter(g => g.competition_key === (sRows[0]?.competition_key || row.competition_key));
    buildRadarChart(document.getElementById('radar-chart'), groupPlayers(rows), { rows: [activeRow] });
    if (initGames.length >= 2) buildEvolutionChart(initGames, 'points');
  });
}

function loadPerGame() {
  return new Promise((resolve, reject) => {
    Papa.parse(PER_GAME_DATA_URL, {
      download: true, header: true, dynamicTyping: true, skipEmptyLines: true,
      complete: ({ data }) => resolve(data.filter(r => r.player_id)),
      error: reject,
    });
  });
}

function loadDetails() {
  return new Promise((resolve) => {
    Papa.parse(DETAILS_URL, {
      download: true, header: true, dynamicTyping: true, skipEmptyLines: true,
      complete: ({ data }) => resolve(data),
      error: () => resolve([]),
    });
  });
}

function loadBio() {
  return fetch(BIO_URL)
    .then(r => r.ok ? r.json() : { players: {} })
    .catch(() => ({ players: {} }));
}

function buildProfile(player, row, allRows, playerGames = [], detail = null, bio = null) {
  // Season filtering
  const s      = _activePlayerSeason;
  const sRows  = s === null ? player.rows : player.rows.filter(r => +r.season === s);
  const sGames = s === null ? playerGames : playerGames.filter(g => +g.season === s);

  const multiComp = sRows.length > 1;

  _playerCtx     = { player, allRows, playerGames, detail, bio, sRows, sGames };
  _activeCompKey = multiComp ? 'all' : (sRows[0]?.competition_key || row.competition_key);

  const activeRow = multiComp ? _aggregateRows(sRows) : (sRows[0] || row);

  const name = player.name;
  const compKeys = [...new Set(sRows.map(r => r.competition_key))]
    .sort((a, b) => COMP_ORDER.indexOf(a) - COMP_ORDER.indexOf(b));

  // Butoane filtre: "Toate" + fiecare competiție
  const allBtn = multiComp
    ? `<button class="comp-badge comp-filter-btn comp-all${_activeCompKey === 'all' ? ' active' : ''}"
               data-key="all" onclick="filterByComp('all')">Toate</button>`
    : '';
  const compBtns = compKeys.map(key => {
    const isActive = key === _activeCompKey;
    return `<button class="comp-badge comp-filter-btn comp-${key}${isActive ? ' active' : ''}"
                    data-key="${key}" onclick="filterByComp('${key}')">${COMP_LABEL[key] ?? key}</button>`;
  }).join('');
  const badges = allBtn + compBtns;

  // Total meciuri + breakdown per competiție (din sezonul activ)
  const totalGP = sRows.reduce((sum, r) => sum + (Number(r.games_played) || 0), 0);
  const gpBreakdown = sRows
    .filter(r => Number(r.games_played) > 0)
    .sort((a, b) => COMP_ORDER.indexOf(a.competition_key) - COMP_ORDER.indexOf(b.competition_key))
    .map(r => `${Number(r.games_played)} în ${COMP_LABEL[r.competition_key] ?? r.competition_key}`)
    .join(' · ');

  // Vârstă + bio
  const currentYear = new Date().getFullYear();
  let age = null;
  if (bio?.birth_date) {
    const born = new Date(bio.birth_date);
    const thisYearBday = new Date(currentYear, born.getMonth(), born.getDate());
    age = currentYear - born.getFullYear() - (new Date() < thisYearBday ? 1 : 0);
  } else if (detail?.birth_year) {
    age = currentYear - detail.birth_year;
  }

  const bioLine1 = bio
    ? [bio.jersey != null ? `#${bio.jersey}` : null, bio.position || null].filter(Boolean)
    : null;
  const bioLine2 = bio
    ? [bio.height_cm ? `${bio.height_cm} cm` : null, age ? `${age} ani` : null, bio.birthplace || null].filter(Boolean)
    : [detail?.nationality_name || null, age ? `${age} ani` : (detail?.birth_year ? `n. ${detail.birth_year}` : null)].filter(Boolean);
  const natLine = !bio && bioLine2.length ? bioLine2.join(' · ') : null;

  const gamesForComp = multiComp ? sGames : sGames.filter(g => g.competition_key === _activeCompKey);

  // Season selector (only if player has multiple seasons at U-BT)
  const playerSeasons = [...new Set(player.rows.map(r => r.season).filter(Boolean))]
    .map(Number).sort((a, b) => b - a);
  const hasMultiSeason = playerSeasons.length > 1;

  let seasonSelectorHTML = '';
  if (hasMultiSeason) {
    const opts = playerSeasons.map(ps =>
      `<option value="${ps}"${ps === s ? ' selected' : ''}>${seasonLabel(ps)}</option>`
    ).join('');
    const allOpt = `<option value="null"${s === null ? ' selected' : ''}>Toate sezoanele la U-BT</option>`;
    seasonSelectorHTML = `
      <div style="margin-top:10px;display:flex;align-items:center;gap:8px">
        <span style="font-size:12px;color:var(--text-secondary)">Sezon:</span>
        <select class="sort-select" style="font-size:12px;padding:4px 8px"
                onchange="filterBySeason(this.value === 'null' ? null : +this.value)">
          ${opts}
          ${allOpt}
        </select>
      </div>`;
  }

  return `
    <!-- Hero (static) -->
    <div class="player-hero">
      <div class="hero-photo-wrap" id="hero-photo">
        <img src="${activeRow.image_url || row.image_url || ''}" alt="${name}"
             onload="this.style.opacity=1"
             onerror="document.getElementById('hero-photo').classList.add('no-photo')"
             style="opacity:0;transition:opacity .4s">
        <div class="hero-photo-placeholder">${getInitials(name)}</div>
      </div>
      <div class="hero-content">
        <h1 class="hero-name">${name}</h1>
        ${bioLine1?.length ? `<p class="hero-meta hero-bio-pos"><span class="hero-jersey">${bioLine1[0]}</span>${bioLine1[1] ? `<span class="hero-position">${bioLine1[1]}</span>` : ''}</p>` : ''}
        ${bio && bioLine2?.length ? `<p class="hero-meta hero-bio-details">${bioLine2.join(' · ')}</p>` : (natLine ? `<p class="hero-meta hero-meta-nat">${natLine}</p>` : '')}
        <p class="hero-meta"><span>${totalGP}</span> meciuri${gpBreakdown ? ` <span class="hero-meta-gp-detail">(${gpBreakdown})</span>` : ''}</p>
        ${seasonSelectorHTML}
        ${multiComp ? `
        <div class="hero-comp-filter">
          <span class="hero-comp-filter-label">Performanțe pe competiție:</span>
          <div class="hero-comps">${badges}</div>
        </div>` : `<div class="hero-comps" style="margin-top:12px">${badges}</div>`}
      </div>
    </div>

    <!-- Stats body (re-rendered on filter change) -->
    <div id="stats-body">${_renderStatsBody(activeRow, player, gamesForComp, sRows)}</div>`;
}

function buildEvolutionSection() {
  const opts = EVOLUTION_STATS.map(s =>
    `<option value="${s.key}">${s.label}</option>`
  ).join('');
  return `
    <div class="chart-section" id="evolution-section">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:10px">
        <p class="section-title" style="margin:0">Evoluție per meci</p>
        <select id="evolution-stat-select" class="sort-select" style="font-size:12px;padding:6px 10px">
          ${opts}
        </select>
      </div>
      <div class="evo-legend">
        <div class="evo-legend-item"><div class="evo-dot" style="background:rgba(34,197,94,0.8)"></div> Victorie</div>
        <div class="evo-legend-item"><div class="evo-dot" style="background:rgba(230,57,70,0.8)"></div> Înfrângere</div>
        <div class="evo-legend-item" style="margin-left:auto;color:rgba(246,168,0,0.7)">--- Medie sezon</div>
      </div>
      <div class="chart-wrap" style="height:260px">
        <canvas id="evolution-chart"></canvas>
      </div>
    </div>`;
}

let _compTableMode = 'medii';

function toggleCompTableMode() {
  _compTableMode = _compTableMode === 'medii' ? 'total' : 'medii';
  if (_playerCtx) {
    const section = document.querySelector('.comp-table-section');
    if (section) section.outerHTML = buildCompTable(_playerCtx.sRows);
  }
}

function buildCompTable(rows) {
  const sorted = [...rows].sort((a, b) => COMP_ORDER.indexOf(a.competition_key) - COMP_ORDER.indexOf(b.competition_key));
  const mode = _compTableMode;

  const COLS_MEDII = [
    { label: 'Meciuri jucate', fmt: r => r.games_played ?? '—' },
    { label: 'Puncte',         fmt: r => fmt(r.points_pg) },
    { label: 'Recuperări',     fmt: r => fmt(r.rebounds_pg) },
    { label: 'Pase decisive',  fmt: r => fmt(r.assists_pg) },
    { label: 'Intercepții',    fmt: r => fmt(r.steals_pg) },
    { label: 'Blocaje',        fmt: r => fmt(r.blocks_pg) },
    { label: 'Eficiență',      fmt: r => fmt(r.pir != null ? r.pir / (r.games_played || 1) : null) },
    { label: 'Minute/meci',    fmt: r => fmtMinutes(r.minutes_pg) },
    { label: 'FG2%',           fmt: r => { const p = fmtPct(r.fg2_pct); return p !== null ? p + '%' : '—'; } },
    { label: 'FG3%',           fmt: r => { const p = fmtPct(r.fg3_pct); return p !== null ? p + '%' : '—'; } },
    { label: 'FT%',            fmt: r => { const p = fmtPct(r.ft_pct);  return p !== null ? p + '%' : '—'; } },
  ];

  const COLS_TOTAL = [
    { label: 'Meciuri jucate', fmt: r => r.games_played ?? '—' },
    { label: 'Puncte',         fmt: r => fmt(r.points, 0) },
    { label: 'Recuperări',     fmt: r => fmt(r.rebounds, 0) },
    { label: 'Pase decisive',  fmt: r => fmt(r.assists, 0) },
    { label: 'Intercepții',    fmt: r => fmt(r.steals, 0) },
    { label: 'Blocaje',        fmt: r => fmt(r.blocks, 0) },
    { label: 'Eficiență',      fmt: r => fmt(r.pir, 0) },
    { label: 'Minute jucate',  fmt: r => fmtMinutes(r.minutes != null ? r.minutes : (r.minutes_pg != null ? r.minutes_pg * (r.games_played || 1) : null)) },
    { label: 'FG2%',           fmt: r => { const p = fmtPct(r.fg2_pct); return p !== null ? p + '%' : '—'; } },
    { label: 'FG3%',           fmt: r => { const p = fmtPct(r.fg3_pct); return p !== null ? p + '%' : '—'; } },
    { label: 'FT%',            fmt: r => { const p = fmtPct(r.ft_pct);  return p !== null ? p + '%' : '—'; } },
  ];

  const cols = mode === 'medii' ? COLS_MEDII : COLS_TOTAL;
  const headers = cols.map(c => `<th>${c.label}</th>`).join('');

  // Find best numeric value per column for highlighting
  const colMaxes = cols.map(col =>
    Math.max(...sorted.map(r => parseFloat(col.fmt(r)) || 0))
  );

  const trows = sorted.map(r => {
    const cells = cols.map((c, ci) => {
      const val = c.fmt(r);
      const num = parseFloat(val) || 0;
      const isBest = sorted.length > 1 && num > 0 && num === colMaxes[ci];
      return `<td${isBest ? ' class="best-cell"' : ''}>${val}</td>`;
    }).join('');
    return `<tr>
      <td><span class="comp-badge comp-${r.competition_key}">${COMP_LABEL[r.competition_key] ?? r.competition_key}</span></td>
      ${cells}
    </tr>`;
  }).join('');

  // Mobile cards (one per competition)
  const mobileCards = sorted.map(r => {
    const gp = r.games_played ?? '—';
    const mStats = mode === 'medii' ? [
      { val: fmt(r.points_pg,   1), lbl: 'Puncte/meci' },
      { val: fmt(r.rebounds_pg, 1), lbl: 'Rec./meci'   },
      { val: fmt(r.assists_pg,  1), lbl: 'Pase/meci'   },
      { val: fmt(r.steals_pg,   1), lbl: 'Intercepții' },
      { val: fmt(r.blocks_pg,   1), lbl: 'Blocaje'     },
      { val: fmtMinutes(r.minutes_pg), lbl: 'Minute/meci' },
    ] : [
      { val: fmt(r.points,   0), lbl: 'Puncte'      },
      { val: fmt(r.rebounds, 0), lbl: 'Recuperări'  },
      { val: fmt(r.assists,  0), lbl: 'Pase'        },
      { val: fmt(r.steals,   0), lbl: 'Intercepții' },
      { val: fmt(r.blocks,   0), lbl: 'Blocaje'     },
      { val: fmtMinutes(r.minutes != null ? r.minutes : (r.minutes_pg != null ? r.minutes_pg * (r.games_played || 1) : null)), lbl: 'Minute' },
    ];
    return `
      <div class="comp-mobile-card">
        <div class="comp-mobile-header">
          <span class="comp-badge comp-${r.competition_key}">${COMP_LABEL[r.competition_key] ?? r.competition_key}</span>
          <span class="comp-mobile-gp">${gp} meciuri</span>
        </div>
        <div class="comp-mobile-stats">
          ${mStats.map(s => `
            <div class="comp-mobile-stat">
              <span class="comp-mobile-stat-val">${s.val}</span>
              <span class="comp-mobile-stat-lbl">${s.lbl}</span>
            </div>`).join('')}
        </div>
      </div>`;
  }).join('');

  return `
    <div class="comp-table-section">
      <div class="comp-table-header">
        <p class="section-title" style="margin:0">Pe competiție</p>
        <div class="comp-table-toggle">
          <button class="${mode === 'medii' ? 'active' : ''}" onclick="toggleCompTableMode()">Medii</button>
          <button class="${mode === 'total' ? 'active' : ''}" onclick="toggleCompTableMode()">Total</button>
        </div>
      </div>
      <div class="comp-table-scroll">
        <table class="comp-table">
          <thead><tr><th>Competiție</th>${headers}</tr></thead>
          <tbody>${trows}</tbody>
        </table>
      </div>
      <div class="comp-mobile-cards">${mobileCards}</div>
    </div>`;
}

/* ─── Player page state ──────────────────────────────────────────────────── */
let _playerCtx          = null;   // { player, allRows, playerGames, detail, sRows, sGames }
let _activeCompKey      = null;
let _activePlayerSeason = null;   // null = toate sezoanele, number = sezon specific

/* Agregă toate rândurile unui jucător într-un singur rând combinat */
function _aggregateRows(rows) {
  const gp = rows.reduce((s, r) => s + (Number(r.games_played) || 0), 0);
  if (!gp) return rows[0];
  const sum = k => rows.reduce((s, r) => s + (Number(r[k]) || 0), 0);
  const fg2m = sum('fg2_made'), fg2a = sum('fg2_att');
  const fg3m = sum('fg3_made'), fg3a = sum('fg3_att');
  const ftm  = sum('ft_made'),  fta  = sum('ft_att');
  const pts  = sum('points'),   reb  = sum('rebounds');
  const ast  = sum('assists'),  stl  = sum('steals');
  const blk  = sum('blocks'),   to   = sum('turnovers');
  const min  = sum('minutes');
  return {
    ...rows[0],
    competition_key: 'all',
    competition:     'Toate',
    games_played:    gp,
    points: pts, rebounds: reb, assists: ast, steals: stl,
    blocks: blk, turnovers: to, minutes: min,
    fg2_made: fg2m, fg2_att: fg2a,
    fg3_made: fg3m, fg3_att: fg3a,
    ft_made:  ftm,  ft_att:  fta,
    points_pg:    +(pts / gp).toFixed(2),
    rebounds_pg:  +(reb / gp).toFixed(2),
    assists_pg:   +(ast / gp).toFixed(2),
    steals_pg:    +(stl / gp).toFixed(2),
    blocks_pg:    +(blk / gp).toFixed(2),
    turnovers_pg: +(to  / gp).toFixed(2),
    minutes_pg:   min / gp,
    fg2_pct: fg2a ? +(fg2m / fg2a).toFixed(3) : null,
    fg3_pct: fg3a ? +(fg3m / fg3a).toFixed(3) : null,
    ft_pct:  fta  ? +(ftm  / fta ).toFixed(3) : null,
    pir: rows.reduce((s, r) => s + (Number(r.pir) || 0), 0),
  };
}

function filterByComp(key) {
  if (!_playerCtx || _activeCompKey === key) return;
  _activeCompKey = key;

  // Update badge active state
  document.querySelectorAll('.comp-filter-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.key === key);
  });

  const { sRows, sGames } = _playerCtx;

  const row = key === 'all'
    ? _aggregateRows(sRows)
    : (sRows.find(r => r.competition_key === key) || sRows[0] || primaryRow(_playerCtx.player));

  // Re-render stats body
  const gamesForComp = key === 'all'
    ? sGames
    : sGames.filter(g => g.competition_key === key);
  document.getElementById('stats-body').innerHTML =
    _renderStatsBody(row, _playerCtx.player, gamesForComp, sRows);

  // Re-animate bars + rebuild charts
  requestAnimationFrame(() => {
    document.querySelectorAll('.shooting-bar-fill[data-pct]').forEach(el => {
      el.style.width = '0';
      requestAnimationFrame(() => { el.style.width = el.dataset.pct + '%'; });
    });
    const compRows = key === 'all'
      ? _playerCtx.allRows
      : _playerCtx.allRows.filter(r => r.competition_key === key);
    buildRadarChart(document.getElementById('radar-chart'), groupPlayers(compRows), { rows: [row] });
    if (gamesForComp.length >= 2) buildEvolutionChart(gamesForComp, 'points');
  });
}

function filterBySeason(season) {
  if (!_playerCtx) return;
  _activePlayerSeason = season;
  const { player, allRows, playerGames, detail, bio } = _playerCtx;
  const row = primaryRow(player);
  document.getElementById('player-content').innerHTML = buildProfile(player, row, allRows, playerGames, detail, bio);
  requestAnimationFrame(() => {
    document.querySelectorAll('.shooting-bar-fill[data-pct]').forEach(el => {
      el.style.width = el.dataset.pct + '%';
    });
    const { sRows, sGames } = _playerCtx;
    const multiComp = sRows.length > 1;
    const activeRow = multiComp ? _aggregateRows(sRows) : (sRows[0] || row);
    const initGames = multiComp ? sGames : sGames.filter(g => g.competition_key === (sRows[0]?.competition_key || row.competition_key));
    buildRadarChart(document.getElementById('radar-chart'), groupPlayers(allRows), { rows: [activeRow] });
    if (initGames.length >= 2) buildEvolutionChart(initGames, 'points');
  });
}


function shootLabel(cls) {
  if (cls.bar === 'bar-good') return 'Excelent';
  if (cls.bar === 'bar-avg')  return 'Bun';
  if (cls.bar === 'bar-poor') return 'Slab';
  return '';
}

function buildBestGame(gamesForComp) {
  if (!gamesForComp || !gamesForComp.length) return '';
  const best = gamesForComp.reduce((b, g) => {
    const pts = Number(g.points) || 0, bPts = Number(b.points) || 0;
    if (pts > bPts) return g;
    if (pts === bPts) return (Number(g.pir) || 0) > (Number(b.pir) || 0) ? g : b;
    return b;
  }, gamesForComp[0]);

  const pts  = Number(best.points)   || 0;
  const reb  = Number(best.rebounds) || 0;
  const ast  = Number(best.assists)  || 0;
  const pir  = Number(best.pir)      || 0;
  const opp  = best.opponent         || '—';
  const comp = best.competition_key  || '';
  const compLabel = COMP_LABEL[comp] || comp || '—';
  const date = best.date
    ? new Date(best.date).toLocaleDateString('ro-RO', { day: 'numeric', month: 'short', year: 'numeric' })
    : '';
  const res = best.result || '';
  const resBadge = res
    ? `<span class="bestgame-result ${res === 'W' ? 'win' : 'loss'}">${res === 'W' ? 'Victorie' : 'Înfrângere'}</span>`
    : '';

  const gameLink = best.game_code
    ? `onclick="location.href='game.html?id=${best.game_code}'" style="cursor:pointer"`
    : '';

  return `
    <div class="bestgame-section">
      <p class="section-title" style="margin-bottom:14px">⭐ Cel mai bun meci</p>
      <div class="bestgame-card" ${gameLink}>
        <div class="bestgame-primary">
          <div class="bestgame-pts">${pts}</div>
          <div class="bestgame-pts-lbl">puncte</div>
        </div>
        <div class="bestgame-info">
          <div class="bestgame-opp">vs <strong>${opp}</strong> ${resBadge}</div>
          <div class="bestgame-meta-row">
            <span class="comp-badge comp-${comp}">${compLabel}</span>
            ${date ? `<span class="bestgame-date">${date}</span>` : ''}
          </div>
        </div>
        <div class="bestgame-secondary">
          <div class="bestgame-sec-item">
            <span class="bestgame-sec-val">${reb}</span>
            <span class="bestgame-sec-lbl">Rec.</span>
          </div>
          <div class="bestgame-sec-item">
            <span class="bestgame-sec-val">${ast}</span>
            <span class="bestgame-sec-lbl">Pase</span>
          </div>
          <div class="bestgame-sec-item">
            <span class="bestgame-sec-val">${pir}</span>
            <span class="bestgame-sec-lbl">PIR</span>
          </div>
        </div>
      </div>
    </div>`;
}

function _renderStatsBody(row, player, gamesForComp, sRows = null) {
  const gp = Number(row.games_played) || 0;

  // Stat boxes — Puncte/meci is primary (larger, accented)
  const statDefs = [
    { v: fmt(row.points_pg,   1), l: 'Puncte / meci',         primary: true  },
    { v: fmt(row.rebounds_pg, 1), l: 'Recuperări / meci',     primary: false },
    { v: fmt(row.assists_pg,  1), l: 'Pase decisive / meci',  primary: false },
    { v: fmt(row.steals_pg,   1), l: 'Intercepţii / meci',    primary: false },
    { v: fmt(row.blocks_pg,   1), l: 'Blocaje / meci',        primary: false },
    { v: fmt(row.turnovers_pg,1), l: 'Mingi pierdute / meci', primary: false },
    { v: fmtMinutes(row.minutes_pg), l: 'Minute / meci',      primary: false },
    { v: gp,                         l: 'Meciuri jucate',     primary: false },
    { v: row.pir !== null && row.pir !== undefined && !isNaN(row.pir)
        ? fmt(row.pir / (gp || 1), 1) : '—',                  l: 'PIR / meci', primary: false },
  ];
  const statBoxes = statDefs.map(s => `
    <div class="stat-box${s.primary ? ' stat-box-primary' : ''}">
      <div class="stat-box-value">${s.v}</div>
      <div class="stat-box-label">${s.l}</div>
    </div>`).join('');

  // Shooting rows — with context label
  const shootingRows = [
    { label: 'FG2%', rawPct: fmtPct(row.fg2_pct), maxPct: 70,  cls: shootClass(fmtPct(row.fg2_pct) !== null ? +fmtPct(row.fg2_pct) : null) },
    { label: 'FG3%', rawPct: fmtPct(row.fg3_pct), maxPct: 60,  cls: shootClass(fmtPct(row.fg3_pct) !== null ? +fmtPct(row.fg3_pct) : null, true) },
    { label: 'FT%',  rawPct: fmtPct(row.ft_pct),  maxPct: 100, cls: shootClass(fmtPct(row.ft_pct)  !== null ? +fmtPct(row.ft_pct)  : null, false, true) },
  ].map(({ label, rawPct, maxPct, cls }) => {
    const pct    = rawPct !== null ? +rawPct : null;
    const barPct = pct !== null ? Math.min((pct / maxPct) * 100, 100) : 0;
    const ctxLbl = shootLabel(cls);
    return `
      <div class="shooting-row">
        <span class="shooting-label">${label}</span>
        <div class="shooting-bar-track">
          <div class="shooting-bar-fill ${cls.bar}" data-pct="${barPct}"></div>
        </div>
        <div class="shooting-value-wrap">
          <span class="shooting-value ${cls.val}">${pct !== null ? pct.toFixed(1) + '%' : '—'}</span>
          ${ctxLbl ? `<span class="shooting-context ${cls.val}">${ctxLbl}</span>` : ''}
        </div>
      </div>`;
  }).join('');

  const activeRows = sRows ?? player.rows;
  const compTable = activeRows.length > 1 ? buildCompTable(activeRows) : '';
  const bestGame  = buildBestGame(gamesForComp);

  return `
    <p class="section-title">Statistici per meci</p>
    <div class="stat-grid">${statBoxes}</div>

    ${bestGame}

    <div class="shooting-section">
      <p class="section-title" style="margin-bottom:6px">Eficiență la aruncare</p>
      <p class="chart-subtitle">Bare proporționale față de benchmark per tip de aruncare</p>
      ${shootingRows}
    </div>

    <div class="chart-section radar-section">
      <p class="section-title" style="margin-bottom:4px">Profil statistic față de echipă</p>
      <p class="chart-subtitle">Procentaje față de maximul din lot, pe fiecare categorie</p>
      <div class="chart-wrap" style="margin-top:12px"><canvas id="radar-chart"></canvas></div>
      <div class="radar-mobile-bars" id="radar-mobile-bars"></div>
    </div>

    ${gamesForComp.length >= 2 ? buildEvolutionSection() : ''}
    ${compTable}`;
}

/* ─── Evolution chart ────────────────────────────────────────────────────── */
let _evolutionChart = null;

function buildEvolutionChart(games, statKey) {
  const canvas = document.getElementById('evolution-chart');
  if (!canvas || !window.Chart) return;

  const statLabel = EVOLUTION_STATS.find(s => s.key === statKey)?.label ?? statKey;
  const isMin     = statKey === 'minutes_played';

  const labels = games.map((g, i) => g.round ? `R${g.round}` : `M${i + 1}`);

  // Raw (absolute) values — used for average and tooltip display
  const rawValues = games.map(g => {
    const v = parseFloat(g[statKey]);
    return isNaN(v) ? null : +v.toFixed(2);
  });

  // Display values: negate for losses so bars drop below X axis
  const displayValues = rawValues.map((v, i) =>
    v !== null && games[i].result === 'L' ? -v : v
  );

  const nonNull  = rawValues.filter(v => v !== null);
  const avgVal   = nonNull.length ? nonNull.reduce((s, v) => s + v, 0) / nonNull.length : 0;

  const barColors       = games.map(g => g.result === 'W' ? 'rgba(34,197,94,0.75)'  : g.result === 'L' ? 'rgba(230,57,70,0.75)'  : 'rgba(148,163,184,0.6)');
  const barColorsBorder = games.map(g => g.result === 'W' ? 'rgba(34,197,94,1)'     : g.result === 'L' ? 'rgba(230,57,70,1)'     : 'rgba(148,163,184,1)');

  if (_evolutionChart) {
    _evolutionChart.destroy();
    _evolutionChart = null;
  }

  _evolutionChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label:           statLabel,
          data:             displayValues,
          backgroundColor:  barColors,
          borderColor:      barColorsBorder,
          borderWidth:      1.5,
          borderRadius:     3,
          borderSkipped:    false,
        },
        {
          type:        'line',
          label:       'Medie',
          data:        games.map(() => +avgVal.toFixed(2)),
          borderColor: 'rgba(246,168,0,0.55)',
          borderWidth: 1.5,
          borderDash:  [6, 4],
          pointRadius: 0,
          fill:        false,
          tension:     0,
        },
      ],
    },
    options: {
      responsive:          true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      scales: {
        x: {
          grid:  { color: 'rgba(255,255,255,0.04)' },
          ticks: { color: '#6B7280', font: { size: 11 }, maxRotation: 0, maxTicksLimit: 9 },
        },
        y: {
          grid:  { color: 'rgba(255,255,255,0.04)' },
          ticks: {
            color: '#6B7280', font: { size: 11 },
            callback: v => Math.abs(v),
          },
        },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#171A21',
          borderColor:     'rgba(194,106,46,0.25)',
          borderWidth:     1,
          titleColor:      '#EDEFF2',
          bodyColor:       '#9AA3AF',
          padding:         12,
          callbacks: {
            title: (items) => {
              const g = games[items[0].dataIndex];
              const res = g.result === 'W' ? 'Victorie' : g.result === 'L' ? 'Înfrângere' : '';
              return [g.opponent || '?', [g.date, res].filter(Boolean).join(' · ')].filter(Boolean).join('\n');
            },
            label: (item) => {
              if (item.datasetIndex === 1) return `  Medie: ${isMin ? fmtMinutes(item.raw) : item.raw}`;
              const v = rawValues[item.dataIndex];
              return v !== null ? `  ${statLabel}: ${isMin ? fmtMinutes(v) : v}` : '  —';
            },
          },
        },
      },
    },
  });

  // Stat selector
  const sel = document.getElementById('evolution-stat-select');
  if (sel) {
    sel.value = statKey;
    sel.onchange = (e) => buildEvolutionChart(games, e.target.value);
  }
}

/* ─── Radar chart ────────────────────────────────────────────────────────── */
function buildRadarChart(canvas, allPlayers, target) {
  if (!canvas || !window.Chart) return;

  const get = (p, key) => primaryRow(p)[key] ?? 0;
  const maxOf = key => Math.max(...allPlayers.map(p => get(p, key))) || 1;

  const maxes = {
    points_pg:   maxOf('points_pg'),
    rebounds_pg: maxOf('rebounds_pg'),
    assists_pg:  maxOf('assists_pg'),
    steals_pg:   maxOf('steals_pg'),
    blocks_pg:   maxOf('blocks_pg'),
    minutes_pg:  maxOf('minutes_pg') || 40,
  };

  const pct = key => Math.min((get(target, key) / maxes[key]) * 100, 100);

  const data = [
    pct('points_pg'),
    pct('rebounds_pg'),
    pct('assists_pg'),
    pct('steals_pg'),
    pct('blocks_pg'),
    pct('minutes_pg'),
  ];

  // Populate mobile bar list (shown instead of radar on small screens)
  const mobileEl = document.getElementById('radar-mobile-bars');
  if (mobileEl) {
    const radarLabels = ['Puncte înscrise', 'Recuperări', 'Pase decisive', 'Mingi furate', 'Blocaje', 'Minute/meci'];
    mobileEl.innerHTML = radarLabels.map((lbl, i) => `
      <div class="radar-bar-row">
        <span class="radar-bar-lbl">${lbl}</span>
        <div class="radar-bar-track">
          <div class="radar-bar-fill" style="width:${data[i].toFixed(0)}%"></div>
        </div>
        <span class="radar-bar-pct">${data[i].toFixed(0)}%</span>
      </div>`).join('');
  }

  new Chart(canvas, {
    type: 'radar',
    data: {
      labels: ['Puncte înscrise', 'Recuperări', 'Pase decisive', 'Mingi furate', 'Blocaje', 'Minute/meci'],
      datasets: [{
        data,
        backgroundColor: 'rgba(194,106,46,0.12)',
        borderColor:     '#C26A2E',
        borderWidth:     2,
        pointBackgroundColor: '#C26A2E',
        pointBorderColor:     '#0F1115',
        pointRadius:     5,
        pointHoverRadius: 7,
      }],
    },
    options: {
      responsive:          true,
      maintainAspectRatio: false,
      scales: {
        r: {
          min: 0, max: 100,
          ticks:      { display: false, stepSize: 25 },
          grid:       { color: 'rgba(255,255,255,0.07)' },
          angleLines: { color: 'rgba(255,255,255,0.07)' },
          pointLabels: {
            color:    '#9AA3AF',
            font:     { size: 12, family: 'Inter, system-ui', weight: '600' },
          },
        },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => ` ${ctx.raw.toFixed(0)}% din maxim`,
          },
          backgroundColor: '#171A21',
          borderColor:     'rgba(194,106,46,0.25)',
          borderWidth:     1,
          titleColor:      '#EDEFF2',
          bodyColor:       '#9AA3AF',
          padding:         10,
        },
      },
    },
  });
}

/* ─── Bootstrap ──────────────────────────────────────────────────────────── */
(async function init() {
  const isPlayer = document.body.dataset.page === 'player';

  // Loading spinner
  if (isPlayer) {
    const mount = document.getElementById('player-content');
    if (mount) mount.innerHTML = `
      <div class="loading-spinner">
        <div class="spinner"></div>
        <span>Se încarcă datele…</span>
      </div>`;
  } else {
    const top3 = document.getElementById('top3-grid');
    if (top3) top3.innerHTML = `
      <div class="loading-spinner" style="grid-column:1/-1">
        <div class="spinner"></div>
        <span>Se încarcă datele…</span>
      </div>`;
  }

  try {
    const rows = await loadData();
    if (isPlayer) initPlayer(rows);
    else          initIndex(rows);
  } catch (err) {
    console.error(err);
    const errHtml = `
      <div class="error-state">
        <div class="error-state-title">Eroare la încărcarea datelor</div>
        <div class="error-state-sub">${err.message ?? String(err)}</div>
      </div>`;
    if (isPlayer) {
      const m = document.getElementById('player-content');
      if (m) m.innerHTML = errHtml;
    } else {
      const m = document.getElementById('top3-grid');
      if (m) m.innerHTML = errHtml;
    }
  }
})();
