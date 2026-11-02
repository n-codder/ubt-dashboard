/* ─── Team page · team.js v6 ──────────────────────────────────────────────── */

const GAMES_URL = `../data/processed/games_normalized.csv?v=${Date.now()}`;

const COMP_LABEL = {
  eurocup:        'EuroCup',
  aba_league:     'ABA League',
  liga_nationala: 'Liga Națională',
};
const COMP_ORDER = ['eurocup', 'aba_league', 'liga_nationala'];

/* ─── State ────────────────────────────────────────────────────────────────── */
let _allGames    = [];
let _activeComp  = 'all';
let _activeSeason = null; // null = toate sezoanele, number = sezon specific
let _scoreChart  = null;

function seasonLabel(s) {
  if (s == null) return 'Toate sezoanele';
  const n = +s;
  return `${n}–${n + 1}`;
}

/* ─── Helpers ──────────────────────────────────────────────────────────────── */
function compKey(raw) {
  if (raw === 'EuroCup')    return 'eurocup';
  if (raw === 'ABA League') return 'aba_league';
  return 'liga_nationala';
}

function toTitleCase(str) {
  return str.toLowerCase().replace(/(^|[\s'-])(\p{L})/gu, (m, sep, c) => sep + c.toUpperCase());
}

function oppShort(name) {
  const PREFIXES = new Set(['KK', 'BC', 'SC', 'FK', 'BK', 'SK', 'RK', 'NK', 'SD']);
  const parts = name.split(' ');
  const first = parts[0].toUpperCase();
  return (PREFIXES.has(first) && parts.length > 1) ? parts[1] : parts[0];
}

function avg(arr) {
  if (!arr.length) return 0;
  return arr.reduce((s, v) => s + v, 0) / arr.length;
}

function stdDev(arr) {
  if (arr.length < 2) return 0;
  const m = avg(arr);
  return Math.sqrt(avg(arr.map(v => (v - m) ** 2)));
}

function fmtDiff(v) {
  const n = +v.toFixed(1);
  return (n >= 0 ? '+' : '') + n;
}

function fmtDate(d) {
  return new Date(d).toLocaleDateString('ro-RO', { day: 'numeric', month: 'short' });
}

function getConsistencyLabel(sd) {
  if (sd < 7)  return 'Foarte constantă';
  if (sd < 12) return 'Destul de constantă';
  return 'Imprevizibilă';
}

function getConsistencyColor(sd) {
  if (sd < 7)  return 'var(--green)';
  if (sd < 12) return 'var(--accent-text)';
  return 'var(--red-text)';
}

/* Calculate current streak from sorted games array */
function calcStreak(games) {
  if (!games.length) return null;
  const lastResult = games[games.length - 1].result;
  let count = 0;
  for (let i = games.length - 1; i >= 0; i--) {
    if (games[i].result === lastResult) count++;
    else break;
  }
  return { type: lastResult, count };
}

/* ─── Data loading ─────────────────────────────────────────────────────────── */
Papa.parse(GAMES_URL, {
  download:      true,
  header:        true,
  dynamicTyping: true,
  complete({ data }) {
    _allGames = data
      .filter(r => r.competition && r.ubt_score != null)
      .map(r => ({
        ...r,
        competition_key: compKey(r.competition),
        ubt_score:  +r.ubt_score,
        opp_score:  +r.opp_score,
        score_diff: +r.score_diff,
        _date:      new Date(r.date),
        opponent:   toTitleCase(String(r.opponent)),
        is_home:    String(r.ubt_is_home) === 'True',
        season:     r.season != null ? +r.season : null,
      }))
      .sort((a, b) => a._date - b._date);

    // Default: newest season
    const allSeasons = [...new Set(_allGames.map(g => g.season).filter(s => s != null))].sort((a, b) => b - a);
    _activeSeason = allSeasons[0] ?? null;

    document.getElementById('team-page').innerHTML = buildPageHTML(allSeasons);
    attachFilterListeners();
    renderDynamic(filtered());
  },
  error(err) {
    document.getElementById('team-page').innerHTML =
      `<div class="error-state"><div class="error-state-title">Eroare la încărcare</div><div class="error-state-sub">${err}</div></div>`;
  },
});

/* ─── Filter ───────────────────────────────────────────────────────────────── */
function filtered() {
  let games = _activeSeason == null
    ? _allGames
    : _allGames.filter(g => g.season === _activeSeason);
  if (_activeComp !== 'all') games = games.filter(g => g.competition_key === _activeComp);
  return games;
}

function setFilter(key) {
  _activeComp = key;
  document.querySelectorAll('.team-filter-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.comp === key);
  });
  renderDynamic(filtered());
}

function setSeason(season) {
  _activeSeason = season;
  const sel = document.getElementById('team-season-select');
  if (sel) sel.value = season == null ? 'null' : String(season);
  renderDynamic(filtered());
}

function attachFilterListeners() {
  document.querySelectorAll('.team-filter-btn').forEach(btn => {
    btn.addEventListener('click', () => setFilter(btn.dataset.comp));
  });
  const sel = document.getElementById('team-season-select');
  if (sel) {
    sel.addEventListener('change', e => {
      setSeason(e.target.value === 'null' ? null : +e.target.value);
    });
  }
}

/* ─── Static HTML scaffold ─────────────────────────────────────────────────── */
function buildPageHTML(allSeasons = []) {
  const seasonOpts = allSeasons.map(s =>
    `<option value="${s}"${s === _activeSeason ? ' selected' : ''}>${seasonLabel(s)}</option>`
  ).join('');

  return `
    <!-- 1. Hero -->
    <div class="team-hero">
      <div class="team-hero-title-wrap">
        <h1 class="team-hero-title">U-BT Cluj-Napoca</h1>
        <div class="team-hero-season">${seasonLabel(_activeSeason)}</div>
      </div>
      <div class="team-filter-wrap">
        <div class="filter-group">
          <button class="filter-btn team-filter-btn active" data-comp="all">Toate</button>
          <button class="filter-btn team-filter-btn" data-comp="eurocup">EuroCup</button>
          <button class="filter-btn team-filter-btn" data-comp="aba_league">ABA</button>
          <button class="filter-btn team-filter-btn" data-comp="liga_nationala">LN</button>
        </div>
        ${allSeasons.length > 1 ? `
        <div class="sort-wrap">
          <select id="team-season-select" class="sort-select" aria-label="Sezon">
            ${seasonOpts}
          </select>
        </div>` : ''}
        <div id="team-record" class="team-record"></div>
      </div>
    </div>

    <!-- 2. Snapshot -->
    <p class="section-title">Statistici echipă</p>
    <div class="team-snapshot-grid" id="team-snapshot"></div>
    <p class="team-snapshot-insight" id="team-snapshot-insight"></p>

    <!-- 3. Formă recentă -->
    <div class="team-card" id="team-momentum"></div>

    <!-- 4. Evoluție scoruri -->
    <div class="chart-section team-card" style="padding:24px;">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:8px;">
        <p class="section-title" style="margin:0">Evoluție scoruri</p>
        <div class="team-chart-legend">
          <span class="team-legend-dot" style="background:rgba(34,197,94,0.8)"></span><span>Victorie</span>
          <span class="team-legend-dot" style="background:rgba(230,57,70,0.8)"></span><span>Înfrângere</span>
        </div>
      </div>
      <div class="chart-wrap" style="height:260px"><canvas id="score-chart"></canvas></div>
    </div>

    <!-- 5 + 6. Win condition + Consistență (side by side on desktop) -->
    <div class="team-insight-row">
      <div class="team-card team-wincond-card" id="team-wincond"></div>
      <div class="team-card team-consistency-card" id="team-consistency"></div>
    </div>

    <!-- 7. Pe competiții -->
    <div id="team-comp-section"></div>
  `;
}

/* ─── Dynamic render (re-runs on filter change) ────────────────────────────── */
function renderDynamic(games) {
  renderRecord(games);
  renderSnapshot(games);
  renderMomentum(games);
  renderScoreChart(games);
  renderWinCondition(games);
  renderConsistency(games);
  renderCompTable(games);
}

function qs(id) { return document.getElementById(id); }

/* ─── 1. Record ─────────────────────────────────────────────────────────────── */
function renderRecord(games) {
  const el = qs('team-record'); if (!el) return;
  const w = games.filter(g => g.result === 'W').length;
  const l = games.filter(g => g.result === 'L').length;
  el.innerHTML = `<span class="team-record-w">${w} victorii</span> · <span class="team-record-l">${l} înfrângeri</span>`;
}

/* ─── 2. Snapshot ───────────────────────────────────────────────────────────── */
function renderSnapshot(games) {
  const el = qs('team-snapshot');
  const insightEl = qs('team-snapshot-insight');
  if (!el) return;

  if (!games.length) {
    el.innerHTML = '<p class="stat-box-label" style="padding:12px">Fără date pentru selecția curentă.</p>';
    if (insightEl) insightEl.textContent = '';
    return;
  }

  const avgScored  = avg(games.map(g => g.ubt_score));
  const avgAllowed = avg(games.map(g => g.opp_score));
  const avgDiff    = avg(games.map(g => g.score_diff));
  const gp         = games.length;
  const w          = games.filter(g => g.result === 'W').length;
  const l          = gp - w;
  const wr         = (w / gp * 100);
  const diffColor  = avgDiff >= 0 ? 'var(--green)' : 'var(--red)';

  el.innerHTML = `
    <div class="stat-box stat-box-primary">
      <div class="stat-box-value">${avgScored.toFixed(1)}</div>
      <div class="stat-box-label">Puncte marcate / meci</div>
    </div>
    <div class="stat-box">
      <div class="stat-box-value">${avgAllowed.toFixed(1)}</div>
      <div class="stat-box-label">Puncte primite / meci</div>
    </div>
    <div class="stat-box">
      <div class="stat-box-value" style="color:${diffColor}">${fmtDiff(avgDiff)}</div>
      <div class="stat-box-label">Diferență medie</div>
    </div>
    <div class="stat-box">
      <div class="stat-box-value">${wr.toFixed(0)}<span style="font-size:18px;font-weight:600;color:var(--text-secondary)">%</span></div>
      <div class="stat-box-label">victorii · ${w}–${l} record</div>
    </div>
  `;

  // Snapshot insight sentence
  if (insightEl) {
    let insight;
    if (avgDiff > 5)       insight = 'Echipă pozitivă, cu avantaj clar de scor.';
    else if (avgDiff >= 0) insight = 'Marchează mai mult decât primește, dar la limită.';
    else                   insight = 'Primește mai multe puncte decât marchează.';
    insightEl.textContent = insight;
  }
}

/* ─── 3. Formă recentă ──────────────────────────────────────────────────────── */
function renderMomentum(games) {
  const el = qs('team-momentum'); if (!el) return;
  if (!games.length) { el.innerHTML = ''; return; }

  const last5  = [...games].slice(-5);
  const wins   = last5.filter(g => g.result === 'W').length;
  const streak = calcStreak(games);

  let streakText = 'Fără serie activă';
  if (streak && streak.count >= 2) {
    streakText = streak.count + (streak.type === 'W'
      ? ` victorii consecutive`
      : ` înfrângeri consecutive`);
  } else if (streak && streak.count === 1) {
    streakText = streak.type === 'W' ? 'Ultima victorie' : 'Ultima înfrângere';
  }

  const badges = last5.map(g => `
    <div class="momentum-badge-wrap" ${g.game_code ? `onclick="location.href='game.html?id=${g.game_code}&season=${g.season}'" style="cursor:pointer"` : ''}>
      <div class="momentum-badge ${g.result === 'W' ? 'win' : 'loss'}" title="${g.opponent} (${fmtDate(g._date)})">
        ${g.result === 'W' ? 'V' : 'Î'}
      </div>
      <div class="momentum-opp" aria-hidden="true">${oppShort(g.opponent)}</div>
    </${g.game_code ? 'a' : 'div'}>
  `).join('');

  el.innerHTML = `
    <div class="team-card-header">
      <div class="momentum-title-wrap">
        <p class="section-title" style="margin:0">Formă recentă</p>
        <div class="momentum-legend">
          <span class="momentum-legend-dot win"></span><span>Victorie</span>
          <span class="momentum-legend-dot loss"></span><span>Înfrângere</span>
        </div>
      </div>
      <div class="momentum-right">
        <span class="team-momentum-summary">${wins} victorii din ultimele ${last5.length}</span>
        <span class="momentum-streak">Serie: ${streakText}</span>
      </div>
    </div>
    <div class="momentum-row">${badges}</div>
  `;
}

/* ─── 4. Evoluție scoruri ───────────────────────────────────────────────────── */
function renderScoreChart(games) {
  const canvas = qs('score-chart'); if (!canvas) return;
  if (_scoreChart) { _scoreChart.destroy(); _scoreChart = null; }
  if (!games.length) return;

  const sorted = [...games].sort((a, b) => a._date - b._date);
  const labels = sorted.map(g => fmtDate(g._date));
  const diffs  = sorted.map(g => g.score_diff);

  const barColors       = sorted.map(g => g.result === 'W' ? 'rgba(34,197,94,0.75)'  : 'rgba(230,57,70,0.75)');
  const barColorsBorder = sorted.map(g => g.result === 'W' ? 'rgba(34,197,94,1)'     : 'rgba(230,57,70,1)');

  _scoreChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label:           'Diferență de scor',
        data:             diffs,
        backgroundColor:  barColors,
        borderColor:      barColorsBorder,
        borderWidth:      1.5,
        borderRadius:     3,
        borderSkipped:    false,
      }],
    },
    options: {
      responsive:          true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      onClick(evt, elements) {
        if (!elements.length) return;
        const g = sorted[elements[0].index];
        if (g && g.game_code) window.location.href = `game.html?id=${g.game_code}&season=${g.season}`;
      },
      scales: {
        x: {
          grid:  { color: 'rgba(255,255,255,0.04)' },
          ticks: { color: '#848C99', font: { size: 11 }, maxRotation: 0, maxTicksLimit: 10 },
        },
        y: {
          grid:  { color: 'rgba(255,255,255,0.04)' },
          ticks: {
            color: '#848C99', font: { size: 11 },
            callback: v => (v > 0 ? '+' : '') + v,
          },
        },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#1C2028',
          borderColor:     '#262B36',
          borderWidth:     1,
          titleColor:      '#EDEFF2',
          bodyColor:       '#9AA3AF',
          padding:         12,
          callbacks: {
            title(items) {
              const g = sorted[items[0].dataIndex];
              return g.opponent;
            },
            beforeBody(items) {
              const g = sorted[items[0].dataIndex];
              return `${COMP_LABEL[g.competition_key] ?? g.competition_key} · ${fmtDate(g._date)}`;
            },
            label(item) {
              const g = sorted[item.dataIndex];
              const d = g.score_diff;
              return [
                ` ${g.result === 'W' ? 'Victorie' : 'Înfrângere'}: ${g.ubt_score}–${g.opp_score}`,
                ` Diferență: ${d > 0 ? '+' : ''}${d}`,
              ];
            },
          },
        },
      },
    },
  });
}

/* ─── 5. Win condition ──────────────────────────────────────────────────────── */
function renderWinCondition(games) {
  const el = qs('team-wincond'); if (!el) return;
  if (games.length < 3) {
    el.innerHTML = '<p class="stat-box-label" style="padding:12px">Date insuficiente.</p>'; return;
  }

  const avgScored = avg(games.map(g => g.ubt_score));
  const threshold = Math.round(avgScored);

  const above = games.filter(g => g.ubt_score >= threshold);
  const below = games.filter(g => g.ubt_score < threshold);

  const wrAbove = above.length ? (above.filter(g => g.result === 'W').length / above.length * 100) : 0;
  const wrBelow = below.length ? (below.filter(g => g.result === 'W').length / below.length * 100) : 0;
  const ppDiff  = Math.round(wrAbove - wrBelow);

  const aboveColor = wrAbove >= 60 ? 'var(--green)' : wrAbove >= 40 ? 'var(--accent)' : 'var(--red)';
  const belowColor = wrBelow >= 60 ? 'var(--green)' : wrBelow >= 40 ? 'var(--accent)' : 'var(--red)';
  const ppColor    = ppDiff >= 0 ? 'var(--green)' : 'var(--red)';

  el.innerHTML = `
    <div class="team-card-header">
      <p class="section-title" style="margin:0">Condiția de victorie</p>
    </div>
    <div class="wincond-row">
      <div class="wincond-scenario">
        <div class="wincond-label">Când marchează ${threshold}+ puncte</div>
        <div class="wincond-rate" style="color:${aboveColor}">${wrAbove.toFixed(0)}%</div>
        <div class="wincond-sub">din ${above.length} meciuri</div>
      </div>
      <div class="wincond-divider"></div>
      <div class="wincond-scenario">
        <div class="wincond-label">Sub ${threshold} puncte</div>
        <div class="wincond-rate" style="color:${belowColor}">${wrBelow.toFixed(0)}%</div>
        <div class="wincond-sub">din ${below.length} meciuri</div>
      </div>
    </div>
    <div class="wincond-insight">
      <span style="color:${ppColor};font-weight:600">${ppDiff >= 0 ? '+' : ''}${Math.abs(ppDiff)}% mai multe șanse de victorie</span> când depășește pragul de ${threshold} puncte.
    </div>
  `;
}

/* ─── 6. Consistență – sezon vs recent ─────────────────────────────────────── */
function renderConsistency(games) {
  const el = qs('team-consistency'); if (!el) return;
  if (games.length < 2) { el.innerHTML = ''; return; }

  const scores       = games.map(g => g.ubt_score);
  const seasonSd     = stdDev(scores);
  const recentGames  = [...games].slice(-5);
  const recentN      = recentGames.length;
  const recentSd     = stdDev(recentGames.map(g => g.ubt_score));
  const minS         = Math.min(...scores);
  const maxS         = Math.max(...scores);

  const seasonLabel  = getConsistencyLabel(seasonSd);
  const recentLabel  = getConsistencyLabel(recentSd);
  const seasonColor  = getConsistencyColor(seasonSd);
  const recentColor  = getConsistencyColor(recentSd);

  // Trend insight
  let trendIcon, trendText;
  if (recentSd <= seasonSd - 2) {
    trendIcon = '↘'; trendText = 'Echipa a devenit mai stabilă în ultimele meciuri.';
  } else if (recentSd >= seasonSd + 2) {
    trendIcon = '↗'; trendText = 'Echipa e mai imprevizibilă recent.';
  } else {
    trendIcon = '→'; trendText = 'Nivel de consistență similar cu media sezonului.';
  }

  el.innerHTML = `
    <div class="team-card-header" style="margin-bottom:12px">
      <p class="section-title" style="margin:0">Consistența echipei</p>
    </div>
    <p class="consistency-subtitle">Cât de mult variază scorul de la meci la meci.</p>
    <div class="consistency-dual">
      <div class="consistency-period">
        <div class="consistency-period-lbl">Sezon</div>
        <div class="consistency-period-val" style="color:${seasonColor}">±${Math.round(seasonSd)}<span class="consistency-unit"> pct</span></div>
        <div class="consistency-period-sub" style="color:${seasonColor}">${seasonLabel}</div>
      </div>
      <div class="consistency-sep"></div>
      <div class="consistency-period">
        <div class="consistency-period-lbl">Ultimele ${recentN} meciuri</div>
        <div class="consistency-period-val" style="color:${recentColor}">±${Math.round(recentSd)}<span class="consistency-unit"> pct</span></div>
        <div class="consistency-period-sub" style="color:${recentColor}">${recentLabel}</div>
      </div>
    </div>
    <div class="consistency-minmax">Între ${minS} și ${maxS} puncte marcate</div>
    <div class="consistency-trend">${trendIcon} ${trendText}</div>
  `;
}

/* ─── 7. Pe competiții ──────────────────────────────────────────────────────── */
function renderCompTable(games) {
  const el = qs('team-comp-section'); if (!el) return;
  if (!games.length) { el.innerHTML = ''; return; }

  const groups = {};
  COMP_ORDER.forEach(k => { groups[k] = []; });
  games.forEach(g => { if (groups[g.competition_key]) groups[g.competition_key].push(g); });

  const rows = COMP_ORDER
    .filter(k => groups[k].length > 0)
    .map(k => {
      const gs = groups[k];
      const w  = gs.filter(g => g.result === 'W').length;
      return {
        key: k, gp: gs.length, w, l: gs.length - w,
        avgScored:  avg(gs.map(g => g.ubt_score)),
        avgAllowed: avg(gs.map(g => g.opp_score)),
        avgDiff:    avg(gs.map(g => g.score_diff)),
      };
    });

  const tableRows = rows.map(r => {
    const dc = r.avgDiff >= 0 ? 'var(--green)' : 'var(--red-text)';
    return `<tr>
      <td><span class="comp-badge comp-${r.key}">${COMP_LABEL[r.key]}</span></td>
      <td>${r.gp}</td>
      <td><span style="color:var(--green);font-weight:700">${r.w}</span> / <span style="color:var(--red-text)">${r.l}</span></td>
      <td>${r.avgScored.toFixed(1)}</td>
      <td>${r.avgAllowed.toFixed(1)}</td>
      <td style="color:${dc};font-weight:700">${fmtDiff(r.avgDiff)}</td>
    </tr>`;
  }).join('');

  const mobileCards = rows.map(r => {
    const dc = r.avgDiff >= 0 ? 'var(--green)' : 'var(--red-text)';
    return `<div class="comp-mobile-card">
      <div class="comp-mobile-header">
        <span class="comp-badge comp-${r.key}">${COMP_LABEL[r.key]}</span>
        <span class="comp-mobile-gp">${r.gp} meciuri · <span style="color:var(--green)">${r.w}V</span> <span style="color:var(--red-text)">${r.l}Î</span></span>
      </div>
      <div class="comp-mobile-stats">
        <div class="comp-mobile-stat">
          <span class="comp-mobile-stat-val">${r.avgScored.toFixed(1)}</span>
          <span class="comp-mobile-stat-lbl">Marcate/meci</span>
        </div>
        <div class="comp-mobile-stat">
          <span class="comp-mobile-stat-val">${r.avgAllowed.toFixed(1)}</span>
          <span class="comp-mobile-stat-lbl">Primite/meci</span>
        </div>
        <div class="comp-mobile-stat">
          <span class="comp-mobile-stat-val" style="color:${dc}">${fmtDiff(r.avgDiff)}</span>
          <span class="comp-mobile-stat-lbl">Diferență</span>
        </div>
      </div>
    </div>`;
  }).join('');

  el.innerHTML = `
    <div class="comp-table-section">
      <div class="comp-table-header">
        <p class="section-title" style="margin:0">Pe competiții</p>
      </div>
      <div class="comp-table-scroll">
        <table class="comp-table">
          <thead>
            <tr>
              <th>Competiție</th><th>Meciuri</th><th>Victorii / Înfrângeri</th>
              <th>Marcate/meci</th><th>Primite/meci</th><th>Diferență</th>
            </tr>
          </thead>
          <tbody>${tableRows}</tbody>
        </table>
      </div>
      <div class="comp-mobile-cards">${mobileCards}</div>
    </div>
  `;
}
