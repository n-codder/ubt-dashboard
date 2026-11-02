/* ─── Home page · home.js v1 ──────────────────────────────────────────────── */

const GAMES_URL = `../data/processed/games_normalized.csv?v=${Date.now()}`;

const COMP_LABEL = {
  eurocup:       'EuroCup',
  aba_league:    'ABA League',
  liga_nationala:'Liga Națională',
};

const COMP_KEY = {
  'EuroCup':        'eurocup',
  'ABA League':     'aba_league',
  'Liga Națională': 'liga_nationala',
};

function parseCsv(url) {
  return new Promise((res, rej) =>
    Papa.parse(url, {
      download: true, header: true, skipEmptyLines: true,
      complete: d => res(d.data.filter(r => r.competition != null)),
      error: rej,
    })
  );
}

function round1(v) { return Math.round(v * 10) / 10; }
function pct(v)    { return Math.round(v * 100) + '%'; }

function seasonLabel(s) {
  if (!s) return '—';
  const n = +s;
  return `${n - 1}/${String(n).slice(2)}`;
}

/* ── Build per-competition per-season stats ────────────────────────────── */
function buildStats(games) {
  const map = {};
  for (const g of games) {
    const comp = COMP_KEY[g.competition] || g.competition_key;
    const season = +g.season;
    const key = `${comp}|${season}`;
    if (!map[key]) map[key] = { comp, season, games: [] };
    map[key].games.push(g);
  }

  return Object.values(map).map(({ comp, season, games }) => {
    const n = games.length;
    const wins = games.filter(g => g.result === 'W').length;
    const scored    = games.reduce((s, g) => s + (+g.ubt_score  || 0), 0) / n;
    const conceded  = games.reduce((s, g) => s + (+g.opp_score  || 0), 0) / n;
    return { comp, season, n, wins, losses: n - wins, winPct: wins / n, scored, conceded, diff: scored - conceded };
  });
}

/* ── Bar helper ────────────────────────────────────────────────────────── */
function bar(year, value, maxValue, colorClass) {
  const w = maxValue > 0 ? Math.round((value / maxValue) * 100) : 0;
  return `
    <div class="season-bar-row">
      <span class="season-bar-year">${year}</span>
      <div class="season-bar-track">
        <div class="season-bar-fill ${colorClass}" style="width:${w}%"></div>
      </div>
      <span class="season-bar-val">${value}</span>
    </div>`;
}

/* ── Season comparison card ────────────────────────────────────────────── */
function buildSeasonCard(rows) {
  if (!rows.length) return '';
  rows = rows.slice().sort((a, b) => a.season - b.season);
  const latest = rows[rows.length - 1];

  // badge: how does current season compare to others on winPct?
  const maxWinPct = Math.max(...rows.map(r => r.winPct));
  const minWinPct = Math.min(...rows.map(r => r.winPct));
  let badge = '', badgeClass = 'mid';
  if (rows.length > 1) {
    if (latest.winPct === maxWinPct) { badge = '↑ Cel mai bun'; badgeClass = 'best'; }
    else if (latest.winPct === minWinPct) { badge = '↓ Sub așteptări'; badgeClass = 'worst'; }
    else { badge = 'În curs'; badgeClass = 'mid'; }
  }

  const maxScored   = Math.max(...rows.map(r => r.scored));
  const maxConceded = Math.max(...rows.map(r => r.conceded));
  const maxWins     = Math.max(...rows.map(r => r.wins));

  const scoredBars   = rows.map(r => bar(seasonLabel(r.season), round1(r.scored),   maxScored,   '')).join('');
  const concededBars = rows.map(r => bar(seasonLabel(r.season), round1(r.conceded), maxConceded, 'red')).join('');
  const winsBars     = rows.map(r => bar(seasonLabel(r.season), r.wins,             maxWins,     'green')).join('');
  const wlRows       = rows.map(r => `
    <div class="season-bar-row">
      <span class="season-bar-year">${seasonLabel(r.season)}</span>
      <div class="season-bar-track" style="overflow:visible;background:none;height:auto">
        <span style="font-family:var(--mono);font-size:12px;font-weight:700;color:var(--text-primary)">
          ${r.wins}V – ${r.losses}Î
        </span>
        <span style="font-size:11px;color:var(--text-muted);margin-left:6px">(${pct(r.winPct)})</span>
      </div>
      <span class="season-bar-val" style="color:var(--text-muted);font-size:11px">${r.n} j.</span>
    </div>`).join('');

  return `
  <div class="season-card">
    <div class="season-card-header">
      <span class="season-card-label">${COMP_LABEL[rows[0].comp] || rows[0].comp}</span>
      ${badge ? `<span class="season-card-badge ${badgeClass}">${badge}</span>` : ''}
    </div>
    <div class="season-metrics">
      <div class="season-metric">
        <span class="season-metric-label">Victorie / Înfrângere</span>
        <div class="season-metric-bars">${wlRows}</div>
      </div>
      <div class="season-metric">
        <span class="season-metric-label">Puncte marcate / meci</span>
        <div class="season-metric-bars">${scoredBars}</div>
      </div>
      <div class="season-metric">
        <span class="season-metric-label">Puncte primite / meci</span>
        <div class="season-metric-bars">${concededBars}</div>
      </div>
    </div>
  </div>`;
}

/* ── KPI summary for all games in selected comp ────────────────────────── */
function buildKpis(stats, comp, allGames) {
  const rows = comp === 'all' ? stats : stats.filter(r => r.comp === comp);
  if (!rows.length) return '';

  const seasons = [...new Set(rows.map(r => r.season))].sort();
  const latest  = seasons[seasons.length - 1];
  const prev    = seasons[seasons.length - 2];

  const cur  = rows.filter(r => r.season === latest);
  const old  = prev != null ? rows.filter(r => r.season === prev) : [];

  function agg(arr) {
    if (!arr.length) return null;
    const totalGames = arr.reduce((s, r) => s + r.n, 0);
    const totalWins  = arr.reduce((s, r) => s + r.wins, 0);
    const scored     = arr.reduce((s, r) => s + r.scored * r.n, 0) / totalGames;
    const conceded   = arr.reduce((s, r) => s + r.conceded * r.n, 0) / totalGames;
    return { wins: totalWins, n: totalGames, winPct: totalWins / totalGames, scored, conceded };
  }

  const c = agg(cur);
  const p = agg(old);
  if (!c) return '';

  function delta(curVal, prevVal, higherIsBetter = true) {
    if (p == null || prevVal == null) return '';
    const d = curVal - prevVal;
    if (Math.abs(d) < 0.05) return `<div class="home-kpi-delta neu">≈</div>`;
    const pos = higherIsBetter ? d > 0 : d < 0;
    const sign = d > 0 ? '+' : '';
    return `<div class="home-kpi-delta ${pos ? 'pos' : 'neg'}">${sign}${round1(d)}</div>`;
  }

  function deltaPct(curVal, prevVal, higherIsBetter = true) {
    if (p == null || prevVal == null) return '';
    const d = curVal - prevVal;
    if (Math.abs(d) < 0.005) return `<div class="home-kpi-delta neu">≈</div>`;
    const pos = higherIsBetter ? d > 0 : d < 0;
    const sign = d > 0 ? '+' : '';
    return `<div class="home-kpi-delta ${pos ? 'pos' : 'neg'}">${sign}${Math.round(d * 100)}%</div>`;
  }

  const seasonStr = comp === 'all'
    ? `Sezon ${seasonLabel(latest)} · Toate competițiile`
    : `${COMP_LABEL[comp] || comp} · ${seasonLabel(latest)}`;

  return `
  <p class="comp-section-title" style="margin-top:0">${seasonStr}</p>
  <div class="home-kpi-row">
    <div class="home-kpi">
      <div class="home-kpi-val">${c.wins}/${c.n}</div>
      <div class="home-kpi-lbl">Victorii / Meciuri</div>
      ${deltaPct(c.winPct, p?.winPct)}
    </div>
    <div class="home-kpi">
      <div class="home-kpi-val">${pct(c.winPct)}</div>
      <div class="home-kpi-lbl">Procent victorii</div>
      ${deltaPct(c.winPct, p?.winPct)}
    </div>
    <div class="home-kpi">
      <div class="home-kpi-val">${round1(c.scored)}</div>
      <div class="home-kpi-lbl">Puncte marcate/meci</div>
      ${delta(c.scored, p?.scored)}
    </div>
    <div class="home-kpi">
      <div class="home-kpi-val">${round1(c.conceded)}</div>
      <div class="home-kpi-lbl">Puncte primite/meci</div>
      ${delta(c.conceded, p?.conceded, false)}
    </div>
    <div class="home-kpi">
      <div class="home-kpi-val">${round1(c.scored - c.conceded) > 0 ? '+' : ''}${round1(c.scored - c.conceded)}</div>
      <div class="home-kpi-lbl">Diferență/meci</div>
      ${delta(c.scored - c.conceded, p ? p.scored - p.conceded : null)}
    </div>
  </div>`;
}

/* ── Render ────────────────────────────────────────────────────────────── */
let _stats = [], _games = [], _activeComp = 'all';

function render() {
  const comps = ['eurocup', 'aba_league', 'liga_nationala'];
  const filtered = _activeComp === 'all' ? _stats : _stats.filter(r => r.comp === _activeComp);

  const byComp = {};
  for (const s of filtered) {
    if (!byComp[s.comp]) byComp[s.comp] = [];
    byComp[s.comp].push(s);
  }

  const cards = Object.values(byComp).map(buildSeasonCard).join('');
  const kpis  = buildKpis(_stats, _activeComp, _games);

  document.getElementById('home-page').innerHTML = `
    <div class="home-hero">
      <div class="home-hero-title">U-BT Cluj-Napoca · Statistici</div>
      <div class="home-hero-sub">Analiză și evoluție pe sezoane — EuroCup, ABA League, Liga Națională</div>
    </div>

    <!-- Quick nav -->
    <div class="home-nav-row">
      <a class="home-nav-card" href="players.html">
        <div class="home-nav-icon">
          <svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <circle cx="12" cy="8" r="4"/><path d="M4 20c0-4 3.6-7 8-7s8 3 8 7"/>
          </svg>
        </div>
        <div>
          <div class="home-nav-label">Jucători</div>
          <div class="home-nav-desc">Statistici individuale</div>
        </div>
      </a>
      <a class="home-nav-card" href="team.html">
        <div class="home-nav-icon">
          <svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/>
            <rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>
          </svg>
        </div>
        <div>
          <div class="home-nav-label">Echipă</div>
          <div class="home-nav-desc">Formă, rezultate, tendințe</div>
        </div>
      </a>
    </div>

    <!-- KPI summary -->
    ${kpis}

    <!-- Filter tabs -->
    <div class="comp-filter-row" id="comp-tabs">
      <button class="comp-tab ${_activeComp === 'all' ? 'active' : ''}" data-comp="all">Toate</button>
      ${comps.map(c => `<button class="comp-tab ${_activeComp === c ? 'active' : ''}" data-comp="${c}">${COMP_LABEL[c]}</button>`).join('')}
    </div>

    <!-- Season cards -->
    <p class="comp-section-title">Comparație sezoane</p>
    <div class="season-grid">${cards || '<p style="color:var(--text-muted);font-size:13px">Fără date pentru selecția curentă.</p>'}</div>
  `;

  document.getElementById('comp-tabs').addEventListener('click', e => {
    const btn = e.target.closest('.comp-tab');
    if (!btn) return;
    _activeComp = btn.dataset.comp;
    render();
  });
}

/* ── Init ──────────────────────────────────────────────────────────────── */
parseCsv(GAMES_URL).then(rows => {
  _games = rows;
  _stats = buildStats(rows);
  render();
}).catch(() => {
  document.getElementById('home-page').innerHTML =
    '<p style="color:var(--red-text);padding:40px;text-align:center">Eroare la încărcarea datelor.</p>';
});
