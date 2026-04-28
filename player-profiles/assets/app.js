/* ─── Config ─────────────────────────────────────────────────────────────── */
// Relative path works on GitHub Pages and locally via http-server.
// For GitHub raw: 'https://raw.githubusercontent.com/USER/REPO/main/data/processed/...'
const _CB = Date.now();
const DATA_URL          = `../data/processed/player_stats_normalized.csv?v=${_CB}`;
const PER_GAME_DATA_URL = `../data/processed/player_stats_per_game_normalized.csv?v=${_CB}`;
const DETAILS_URL       = `../data/processed/player_details.csv?v=${_CB}`;

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

/* ─── Index page ─────────────────────────────────────────────────────────── */
function initIndex(rows) {
  const players = groupPlayers(rows);

  let filteredComp = 'all';
  let sortKey      = 'points_pg';
  let searchQ      = '';

  const grid       = document.getElementById('player-grid');
  const countEl    = document.getElementById('results-count');

  function filtered() {
    return players
      .filter(p => {
        if (filteredComp !== 'all' && !p.rows.some(r => r.competition_key === filteredComp)) return false;
        if (searchQ && !p.name.toLowerCase().includes(searchQ)) return false;
        return true;
      })
      .sort((a, b) => {
        if (sortKey === 'player_name') return a.name.localeCompare(b.name);
        const getVal = p => {
          const r = filteredComp !== 'all'
            ? (p.rows.find(r => r.competition_key === filteredComp) || primaryRow(p))
            : (p.rows.length > 1 ? _aggregateRows(p.rows) : primaryRow(p));
          return sortKey === 'pir' ? (r.pir ?? -Infinity) : (r[sortKey] ?? -Infinity);
        };
        return getVal(b) - getVal(a);
      });
  }

  function render() {
    const list = filtered();
    countEl.textContent = `${list.length} jucători`;

    if (!list.length) {
      grid.innerHTML = `
        <div class="state-empty">
          <div class="state-empty-icon">🏀</div>
          <div class="state-empty-title">Niciun jucător găsit</div>
          <div class="state-empty-sub">Încearcă alt filtru sau termen de căutare.</div>
        </div>`;
      return;
    }

    grid.innerHTML = list.map(p => buildCard(p, sortKey, filteredComp)).join('');
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

  // Search
  document.getElementById('search-input').addEventListener('input', e => {
    searchQ = e.target.value.toLowerCase().trim();
    render();
  });

  render();
}

function buildCard(player, sortKey = 'points_pg', filteredComp = 'all') {
  const photoRow  = primaryRow(player);   // always EuroCup photo
  const statsRow  = filteredComp !== 'all'
    ? (player.rows.find(r => r.competition_key === filteredComp) || photoRow)
    : (player.rows.length > 1 ? _aggregateRows(player.rows) : photoRow);
  const row = statsRow;
  const name = player.name;

  const meta   = SORT_META[sortKey] || SORT_META['points_pg'];
  const pgVal  = meta.pgKey ? row[meta.pgKey] : (row.pir != null ? row.pir / (row.games_played || 1) : null);
  const totVal = row[meta.totalKey];

  return `
    <article class="player-card" onclick="location.href='player.html?id=${player.id}'" role="button" tabindex="0" aria-label="${name}">
      <div class="card-photo-wrap" id="photo-${player.id}">
        <img src="${photoRow.image_url || ''}" alt="${name}"
             onload="this.style.opacity=1"
             onerror="document.getElementById('photo-${player.id}').classList.add('no-photo')"
             style="opacity:0;transition:opacity .3s">
        <div class="card-photo-placeholder">${getInitials(name)}</div>
        <div class="card-gradient"></div>
      </div>
      <div class="card-body">
        <h3 class="card-name" title="${name}">${name}</h3>
        <div class="card-stats">
          <div class="card-stat card-stat-main">
            <span class="card-stat-value">${meta.pgFmt(pgVal)}</span>
            <span class="card-stat-label">${meta.pgLabel}</span>
          </div>
          <div class="card-stat-sep"></div>
          <div class="card-stat card-stat-total">
            <span class="card-stat-value card-stat-value-total">${meta.totalFmt(totVal)}</span>
            <span class="card-stat-label">${meta.totalLabel}</span>
          </div>
        </div>
      </div>
    </article>`;
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

  // Load per-game data and player details in parallel
  let perGameRows = [], detailRows = [];
  try {
    [perGameRows, detailRows] = await Promise.all([loadPerGame(), loadDetails()]);
  } catch (_) { /* optional */ }

  const playerGames = perGameRows
    .filter(r => Number(r.player_id) === playerId)
    .sort((a, b) => new Date(a.date) - new Date(b.date));

  const detail = detailRows.find(d => Number(d.player_id) === playerId) || null;

  document.title = `${player.name} · U-BT Stats`;
  document.getElementById('player-content').innerHTML = buildProfile(player, row, rows, playerGames, detail);

  requestAnimationFrame(() => {
    document.querySelectorAll('.shooting-bar-fill[data-pct]').forEach(el => {
      el.style.width = el.dataset.pct + '%';
    });
    const multiComp  = player.rows.length > 1;
    const activeRow  = multiComp ? _aggregateRows(player.rows) : row;
    const initGames  = multiComp ? playerGames : playerGames.filter(g => g.competition_key === row.competition_key);
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

function buildProfile(player, row, allRows, playerGames = [], detail = null) {
  const multiComp = player.rows.length > 1;

  // Default: 'all' când jucătorul are mai multe competiții
  _playerCtx     = { player, allRows, playerGames, detail };
  _activeCompKey = multiComp ? 'all' : row.competition_key;

  // Rândul inițial (agregat sau competiție unică)
  const activeRow = multiComp ? _aggregateRows(player.rows) : row;

  const name = player.name;
  const compKeys = [...new Set(player.rows.map(r => r.competition_key))]
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

  // Total meciuri + breakdown per competiție
  const totalGP = player.rows.reduce((s, r) => s + (Number(r.games_played) || 0), 0);
  const gpBreakdown = player.rows
    .filter(r => Number(r.games_played) > 0)
    .sort((a, b) => COMP_ORDER.indexOf(a.competition_key) - COMP_ORDER.indexOf(b.competition_key))
    .map(r => `${Number(r.games_played)} în ${COMP_LABEL[r.competition_key] ?? r.competition_key}`)
    .join(' · ');

  // Vârstă + naționalitate
  const currentYear = new Date().getFullYear();
  const age = detail?.birth_year ? currentYear - detail.birth_year : null;
  const natLine = [
    detail?.nationality_name || null,
    age ? `${age} ani` : (detail?.birth_year ? `n. ${detail.birth_year}` : null),
  ].filter(Boolean).join(' · ');

  const gamesForComp = multiComp ? playerGames : playerGames.filter(g => g.competition_key === _activeCompKey);

  return `
    <!-- Hero (static) -->
    <div class="player-hero">
      <div class="hero-photo-wrap" id="hero-photo">
        <img src="${row.image_url || ''}" alt="${name}"
             onload="this.style.opacity=1"
             onerror="document.getElementById('hero-photo').classList.add('no-photo')"
             style="opacity:0;transition:opacity .4s">
        <div class="hero-photo-placeholder">${getInitials(name)}</div>
      </div>
      <div class="hero-content">
        <h1 class="hero-name">${name}</h1>
        ${natLine ? `<p class="hero-meta hero-meta-nat">${natLine}</p>` : ''}
        <p class="hero-meta"><span>${totalGP}</span> meciuri${gpBreakdown ? ` <span class="hero-meta-gp-detail">(${gpBreakdown})</span>` : ''}</p>
        <div class="hero-kpis" id="hero-kpis-wrap">${_buildHeroKpis(activeRow)}</div>
        ${multiComp ? `
        <div class="hero-comp-filter">
          <span class="hero-comp-filter-label">Performanțe pe competiție:</span>
          <div class="hero-comps">${badges}</div>
        </div>` : `<div class="hero-comps" style="margin-top:12px">${badges}</div>`}
      </div>
    </div>

    <!-- Stats body (re-rendered on filter change) -->
    <div id="stats-body">${_renderStatsBody(activeRow, player, gamesForComp)}</div>`;
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
        <div class="evo-legend-item"><div class="evo-dot evo-dot-win"></div> Victorie</div>
        <div class="evo-legend-item"><div class="evo-dot evo-dot-loss"></div> Înfrângere</div>
        <div class="evo-legend-item" style="margin-left:auto;color:rgba(246,168,0,0.7)">--- Medie sezon</div>
      </div>
      <div class="chart-wrap" style="height:260px">
        <canvas id="evolution-chart"></canvas>
      </div>
    </div>`;
}

function buildCompTable(rows) {
  const sorted = [...rows].sort((a, b) => COMP_ORDER.indexOf(a.competition_key) - COMP_ORDER.indexOf(b.competition_key));
  const cols = ['GP','PPG','RPG','APG','SPG','BPG','FG2%','FG3%','FT%'];
  const headers = cols.map(c => `<th>${c}</th>`).join('');
  const trows = sorted.map(r => {
    const f2 = fmtPct(r.fg2_pct); const f3 = fmtPct(r.fg3_pct); const ft = fmtPct(r.ft_pct);
    return `<tr>
      <td><span class="comp-badge comp-${r.competition_key}">${COMP_LABEL[r.competition_key] ?? r.competition_key}</span></td>
      <td>${r.games_played ?? '—'}</td>
      <td>${fmt(r.points_pg)}</td>
      <td>${fmt(r.rebounds_pg)}</td>
      <td>${fmt(r.assists_pg)}</td>
      <td>${fmt(r.steals_pg)}</td>
      <td>${fmt(r.blocks_pg)}</td>
      <td>${f2 !== null ? f2+'%' : '—'}</td>
      <td>${f3 !== null ? f3+'%' : '—'}</td>
      <td>${ft  !== null ? ft +'%' : '—'}</td>
    </tr>`;
  }).join('');

  return `
    <div class="comp-table-section">
      <p class="section-title" style="padding:20px 20px 0">Pe competiție</p>
      <table class="comp-table">
        <thead><tr><th>Competiție</th>${headers}</tr></thead>
        <tbody>${trows}</tbody>
      </table>
    </div>`;
}

/* ─── Player page state ──────────────────────────────────────────────────── */
let _playerCtx     = null;   // { player, allRows, playerGames, detail }
let _activeCompKey = null;

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
  console.trace('[filterByComp]', key, 'prev:', _activeCompKey);
  _activeCompKey = key;

  // Update badge active state
  document.querySelectorAll('.comp-filter-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.key === key);
  });

  const row = key === 'all'
    ? _aggregateRows(_playerCtx.player.rows)
    : (_playerCtx.player.rows.find(r => r.competition_key === key) || primaryRow(_playerCtx.player));

  // Update hero KPIs
  document.getElementById('hero-kpis-wrap').innerHTML = _buildHeroKpis(row);

  // Re-render stats body
  const gamesForComp = key === 'all'
    ? _playerCtx.playerGames
    : _playerCtx.playerGames.filter(g => g.competition_key === key);
  document.getElementById('stats-body').innerHTML =
    _renderStatsBody(row, _playerCtx.player, gamesForComp);

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

function _buildHeroKpis(row) {
  return [
    { v: fmt(row.points_pg),   l: 'PPG' },
    { v: fmt(row.rebounds_pg), l: 'RPG' },
    { v: fmt(row.assists_pg),  l: 'APG' },
    { v: fmt(row.steals_pg),   l: 'SPG' },
    { v: fmt(row.blocks_pg),   l: 'BPG' },
  ].map((k, i) => `
    ${i > 0 ? '<div class="kpi-divider"></div>' : ''}
    <div class="hero-kpi">
      <span class="hero-kpi-value">${k.v}</span>
      <span class="hero-kpi-label">${k.l}</span>
    </div>`).join('');
}

function _renderStatsBody(row, player, gamesForComp) {
  const gp = Number(row.games_played) || 0;

  const statBoxes = [
    { v: fmt(row.points_pg,   1), l: 'Puncte / meci',         h: true  },
    { v: fmt(row.rebounds_pg, 1), l: 'Recuperări / meci',     h: false },
    { v: fmt(row.assists_pg,  1), l: 'Pase decisive / meci',  h: false },
    { v: fmt(row.steals_pg,   1), l: 'Intercepţii / meci',    h: false },
    { v: fmt(row.blocks_pg,   1), l: 'Blocaje / meci',        h: false },
    { v: fmt(row.turnovers_pg,1), l: 'Mingi pierdute / meci', h: false },
    { v: fmtMinutes(row.minutes_pg), l: 'Minute / meci',      h: false },
    { v: gp,                      l: 'Meciuri jucate',        h: false },
    { v: row.pir !== null && row.pir !== undefined && !isNaN(row.pir)
        ? fmt(row.pir / (gp || 1), 1) : '—',                  l: 'PIR / meci', h: false },
  ].map(s => `
    <div class="stat-box">
      <div class="stat-box-value${s.h ? ' highlight' : ''}">${s.v}</div>
      <div class="stat-box-label">${s.l}</div>
    </div>`).join('');

  const shootingRows = [
    { label: 'FG2%', rawPct: fmtPct(row.fg2_pct), maxPct: 70,  cls: shootClass(fmtPct(row.fg2_pct) !== null ? +fmtPct(row.fg2_pct) : null) },
    { label: 'FG3%', rawPct: fmtPct(row.fg3_pct), maxPct: 60,  cls: shootClass(fmtPct(row.fg3_pct) !== null ? +fmtPct(row.fg3_pct) : null, true) },
    { label: 'FT%',  rawPct: fmtPct(row.ft_pct),  maxPct: 100, cls: shootClass(fmtPct(row.ft_pct)  !== null ? +fmtPct(row.ft_pct)  : null, false, true) },
  ].map(({ label, rawPct, maxPct, cls }) => {
    const pct    = rawPct !== null ? +rawPct : null;
    const barPct = pct !== null ? Math.min((pct / maxPct) * 100, 100) : 0;
    return `
      <div class="shooting-row">
        <span class="shooting-label">${label}</span>
        <div class="shooting-bar-track">
          <div class="shooting-bar-fill ${cls.bar}" data-pct="${barPct}"></div>
        </div>
        <span class="shooting-value ${cls.val}">${pct !== null ? pct.toFixed(1) + '%' : '—'}</span>
      </div>`;
  }).join('');

  const compTable = player.rows.length > 1 ? buildCompTable(player.rows) : '';

  return `
    <p class="section-title">Statistici per meci</p>
    <div class="stat-grid">${statBoxes}</div>

    <div class="shooting-section">
      <p class="section-title" style="margin-bottom:18px">Eficiență la aruncare</p>
      ${shootingRows}
    </div>

    <div class="chart-section">
      <p class="section-title" style="margin-bottom:16px">Profil statistic față de echipă</p>
      <div class="chart-wrap"><canvas id="radar-chart"></canvas></div>
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

  const labels = games.map((g, i) => {
    const opp   = (g.opponent || '?').split(' ').slice(0, 2).join(' ');
    const round = g.round ? `R${g.round}` : `M${i + 1}`;
    return `${round}`;
  });

  const values = games.map(g => {
    const v = parseFloat(g[statKey]);
    return isNaN(v) ? null : +v.toFixed(2);
  });

  const avg = values.filter(v => v !== null).reduce((s, v) => s + v, 0) /
              (values.filter(v => v !== null).length || 1);

  // Result colors per point
  const pointColors = games.map(g =>
    g.result === 'W' ? '#22c55e' : g.result === 'L' ? '#e63946' : '#94a3b8'
  );

  if (_evolutionChart) {
    _evolutionChart.destroy();
    _evolutionChart = null;
  }

  _evolutionChart = new Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: statLabel,
          data:  values,
          borderColor:           '#C26A2E',
          backgroundColor:       'rgba(194,106,46,0.08)',
          borderWidth:           2.5,
          tension:               0.35,
          fill:                  true,
          pointRadius:           5,
          pointHoverRadius:      8,
          pointBackgroundColor:  pointColors,
          pointBorderColor:      '#0F1115',
          pointBorderWidth:      2,
          spanGaps:              true,
        },
        {
          label:       'Medie',
          data:        games.map(() => +avg.toFixed(2)),
          borderColor: 'rgba(154,163,175,0.35)',
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
          grid:  { color: 'rgba(255,255,255,0.05)' },
          ticks: { color: '#6B7280', font: { size: 11 }, maxRotation: 0 },
        },
        y: {
          beginAtZero: true,
          grid:  { color: 'rgba(255,255,255,0.05)' },
          ticks: { color: '#6B7280', font: { size: 11 } },
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
              const g   = games[items[0].dataIndex];
              const opp = g.opponent || '?';
              const date = g.date ? ` · ${g.date}` : '';
              const res  = g.result ? ` (${g.result})` : '';
              return `${opp}${date}${res}`;
            },
            label: (item) => {
              const isMin = statKey === 'minutes_played';
              if (item.datasetIndex === 1) return `  Medie: ${isMin ? fmtMinutes(item.raw) : item.raw}`;
              const v = item.raw;
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
    fg3_pct:     0.5,   // cap at 50% for display
  };

  const row = target.rows?.length === 1 ? target.rows[0] : primaryRow(target);
  const pct = key => Math.min((get(target, key) / maxes[key]) * 100, 100);

  const data = [
    pct('points_pg'),
    pct('rebounds_pg'),
    pct('assists_pg'),
    pct('steals_pg'),
    pct('blocks_pg'),
    Math.min(((row.fg3_pct ?? 0) / maxes.fg3_pct) * 100, 100),
  ];

  new Chart(canvas, {
    type: 'radar',
    data: {
      labels: ['Scoring', 'Rebounds', 'Assists', 'Steals', 'Blocks', 'FG3%'],
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
  const mountId  = isPlayer ? 'player-content' : 'player-grid';
  const mount    = document.getElementById(mountId);

  mount.innerHTML = `
    <div class="loading-spinner" ${!isPlayer ? 'style="grid-column:1/-1"' : ''}>
      <div class="spinner"></div>
      <span>Se încarcă datele…</span>
    </div>`;

  try {
    const rows = await loadData();
    if (isPlayer) initPlayer(rows);
    else          initIndex(rows);
  } catch (err) {
    console.error(err);
    mount.innerHTML = `
      <div class="error-state" ${!isPlayer ? 'style="grid-column:1/-1"' : ''}>
        <div class="error-state-title">Eroare la încărcarea datelor</div>
        <div class="error-state-sub">${err.message ?? String(err)}</div>
      </div>`;
  }
})();
