/* ─── Team page · team.js ─────────────────────────────────────────────────── */

const GAMES_URL = `../data/processed/games.csv?v=${Date.now()}`;

const COMP_LABEL = {
  eurocup:        'EuroCup',
  aba_league:     'ABA League',
  liga_nationala: 'Liga Națională',
};
const COMP_COLOR = {
  eurocup:        '#f6a800',
  aba_league:     '#00b4d8',
  liga_nationala: '#e63946',
};
const COMP_ORDER = ['eurocup', 'aba_league', 'liga_nationala'];

/* ─── State ────────────────────────────────────────────────────────────────── */
let _allGames  = [];
let _activeComp = 'all';
let _scoreChart = null;

/* ─── Helpers ──────────────────────────────────────────────────────────────── */
function compKey(raw) {
  if (raw === 'EuroCup')        return 'eurocup';
  if (raw === 'ABA League')     return 'aba_league';
  return 'liga_nationala';
}

function toTitleCase(str) {
  return str.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

function avg(arr) {
  if (!arr.length) return 0;
  return arr.reduce((s, v) => s + v, 0) / arr.length;
}

function fmtDiff(v) {
  const n = +v.toFixed(1);
  return (n >= 0 ? '+' : '') + n;
}

function fmtDate(d) {
  return new Date(d).toLocaleDateString('ro-RO', { day: 'numeric', month: 'short' });
}

function stdDev(arr) {
  if (arr.length < 2) return 0;
  const m = avg(arr);
  return Math.sqrt(avg(arr.map(v => (v - m) ** 2)));
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
      }))
      .sort((a, b) => a._date - b._date);

    renderAll(filtered());
    document.getElementById('team-page').innerHTML = buildPageHTML();
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
  if (_activeComp === 'all') return _allGames;
  return _allGames.filter(g => g.competition_key === _activeComp);
}

function setFilter(key) {
  _activeComp = key;
  // Update button states
  document.querySelectorAll('.team-filter-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.comp === key);
  });
  renderDynamic(filtered());
}

function attachFilterListeners() {
  document.querySelectorAll('.team-filter-btn').forEach(btn => {
    btn.addEventListener('click', () => setFilter(btn.dataset.comp));
  });
}

/* ─── Static HTML scaffold ─────────────────────────────────────────────────── */
function buildPageHTML() {
  return `
    <!-- 1. Hero -->
    <div class="team-hero">
      <div class="team-hero-title-wrap">
        <h1 class="team-hero-title">U-BT Cluj-Napoca</h1>
        <div class="team-hero-season">Sezon 2025–2026</div>
      </div>
      <div class="team-filter-wrap">
        <div class="filter-group">
          <button class="filter-btn team-filter-btn active" data-comp="all">Toate</button>
          <button class="filter-btn team-filter-btn" data-comp="eurocup">EuroCup</button>
          <button class="filter-btn team-filter-btn" data-comp="aba_league">ABA</button>
          <button class="filter-btn team-filter-btn" data-comp="liga_nationala">LN</button>
        </div>
        <div id="team-record" class="team-record"></div>
      </div>
    </div>

    <!-- 2. Snapshot -->
    <p class="section-title">Statistici echipă</p>
    <div class="team-snapshot-grid" id="team-snapshot"></div>

    <!-- 3. Formă recentă -->
    <div class="team-card" id="team-momentum"></div>

    <!-- 4. Evoluție scoruri -->
    <div class="chart-section team-card" style="padding:24px;">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:8px;">
        <p class="section-title" style="margin:0">Evoluție scoruri</p>
        <div class="team-chart-legend">
          <span class="team-legend-dot" style="background:var(--accent)"></span><span>U-BT</span>
          <span class="team-legend-dot" style="background:rgba(154,163,175,0.5)"></span><span>Adversar</span>
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

/* helper — safe querySelector (elements may not exist yet) */
function qs(id) { return document.getElementById(id); }

/* ─── Render helpers ───────────────────────────────────────────────────────── */

function renderRecord(games) {
  const el = qs('team-record'); if (!el) return;
  const w = games.filter(g => g.result === 'W').length;
  const l = games.filter(g => g.result === 'L').length;
  el.innerHTML = `<span class="team-record-w">${w} victorii</span> · <span class="team-record-l">${l} înfrângeri</span>`;
}

function renderSnapshot(games) {
  const el = qs('team-snapshot'); if (!el) return;
  if (!games.length) { el.innerHTML = '<p class="stat-box-label" style="padding:12px">Fără date pentru selecția curentă.</p>'; return; }

  const avgScored  = avg(games.map(g => g.ubt_score));
  const avgAllowed = avg(games.map(g => g.opp_score));
  const avgDiff    = avg(games.map(g => g.score_diff));
  const gp         = games.length;
  const wr         = (games.filter(g => g.result === 'W').length / gp * 100);

  const diffColor  = avgDiff >= 0 ? 'var(--accent)' : 'var(--red)';

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
      <div class="stat-box-label">Win rate · ${gp} meciuri</div>
    </div>
  `;
}

function renderMomentum(games) {
  const el = qs('team-momentum'); if (!el) return;
  if (!games.length) { el.innerHTML = ''; return; }

  const last5 = [...games].slice(-5);
  const wins  = last5.filter(g => g.result === 'W').length;

  const badges = last5.map(g => `
    <div class="momentum-badge-wrap">
      <div class="momentum-badge ${g.result === 'W' ? 'win' : 'loss'}" title="${g.opponent} (${fmtDate(g._date)})">
        ${g.result === 'W' ? 'V' : 'Î'}
      </div>
      <div class="momentum-opp">${g.opponent.split(' ')[0]}</div>
    </div>
  `).join('');

  el.innerHTML = `
    <div class="team-card-header">
      <p class="section-title" style="margin:0">Formă recentă</p>
      <span class="team-momentum-summary">${wins} victorii din ultimele ${last5.length}</span>
    </div>
    <div class="momentum-row">${badges}</div>
  `;
}

function renderScoreChart(games) {
  const canvas = document.getElementById('score-chart');
  if (!canvas) return;

  if (_scoreChart) { _scoreChart.destroy(); _scoreChart = null; }
  if (!games.length) return;

  const sorted = [...games].sort((a, b) => a._date - b._date);
  const labels = sorted.map(g => fmtDate(g._date));
  const ubtScores = sorted.map(g => g.ubt_score);
  const oppScores = sorted.map(g => g.opp_score);
  const avgUbt    = avg(ubtScores);

  const ptColors  = sorted.map(g => g.result === 'W' ? '#22c55e' : '#e63946');

  _scoreChart = new Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label:                'U-BT',
          data:                 ubtScores,
          borderColor:          '#C26A2E',
          backgroundColor:      'rgba(194,106,46,0.06)',
          borderWidth:          2.5,
          tension:              0.3,
          fill:                 true,
          pointRadius:          5,
          pointHoverRadius:     8,
          pointBackgroundColor: ptColors,
          pointBorderColor:     '#0F1115',
          pointBorderWidth:     2,
          spanGaps:             true,
        },
        {
          label:       'Adversar',
          data:        oppScores,
          borderColor: 'rgba(154,163,175,0.45)',
          borderWidth: 2,
          tension:     0.3,
          fill:        false,
          pointRadius: 3,
          pointHoverRadius: 6,
          pointBackgroundColor: 'rgba(154,163,175,0.45)',
          pointBorderColor:     '#0F1115',
          pointBorderWidth:     1,
          spanGaps:    true,
        },
        {
          label:       'Medie U-BT',
          data:        sorted.map(() => +avgUbt.toFixed(1)),
          borderColor: 'rgba(194,106,46,0.35)',
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
          ticks: { color: '#6B7280', font: { size: 11 }, maxRotation: 0, maxTicksLimit: 10 },
        },
        y: {
          beginAtZero: false,
          grid:  { color: 'rgba(255,255,255,0.04)' },
          ticks: { color: '#6B7280', font: { size: 11 } },
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
              return `${g.opponent}`;
            },
            beforeBody(items) {
              const g = sorted[items[0].dataIndex];
              return `${COMP_LABEL[g.competition_key] ?? g.competition_key} · ${fmtDate(g._date)}`;
            },
            label(item) {
              const g = sorted[item.dataIndex];
              if (item.datasetIndex === 0) return ` U-BT ${g.ubt_score} – ${g.opp_score} Adv  (${g.result === 'W' ? 'Victorie' : 'Înfrângere'})`;
              return null;
            },
            afterLabel() { return null; },
          },
          filter(item) { return item.datasetIndex <= 1; },
        },
      },
    },
  });
}

function renderWinCondition(games) {
  const el = qs('team-wincond'); if (!el) return;
  if (games.length < 3) { el.innerHTML = '<p class="stat-box-label" style="padding:12px">Date insuficiente.</p>'; return; }

  const avgScored = avg(games.map(g => g.ubt_score));
  const threshold = Math.round(avgScored);

  const above = games.filter(g => g.ubt_score >= threshold);
  const below = games.filter(g => g.ubt_score < threshold);

  const wrAbove = above.length ? (above.filter(g => g.result === 'W').length / above.length * 100) : 0;
  const wrBelow = below.length ? (below.filter(g => g.result === 'W').length / below.length * 100) : 0;

  const aboveColor = wrAbove >= 60 ? 'var(--green)' : wrAbove >= 40 ? 'var(--accent)' : 'var(--red)';
  const belowColor = wrBelow >= 60 ? 'var(--green)' : wrBelow >= 40 ? 'var(--accent)' : 'var(--red)';

  el.innerHTML = `
    <div class="team-card-header">
      <p class="section-title" style="margin:0">🎯 Win condition</p>
    </div>
    <div class="wincond-row">
      <div class="wincond-scenario">
        <div class="wincond-label">Marchează ${threshold}+ puncte</div>
        <div class="wincond-rate" style="color:${aboveColor}">${wrAbove.toFixed(0)}%</div>
        <div class="wincond-sub">win rate · ${above.length} meciuri</div>
      </div>
      <div class="wincond-divider"></div>
      <div class="wincond-scenario">
        <div class="wincond-label">Marchează sub ${threshold}</div>
        <div class="wincond-rate" style="color:${belowColor}">${wrBelow.toFixed(0)}%</div>
        <div class="wincond-sub">win rate · ${below.length} meciuri</div>
      </div>
    </div>
    <div class="wincond-insight">
      Pragul de ${threshold} puncte marchează diferența dintre victorie și înfrângere.
    </div>
  `;
}

function renderConsistency(games) {
  const el = qs('team-consistency'); if (!el) return;
  if (games.length < 3) { el.innerHTML = ''; return; }

  const scores = games.map(g => g.ubt_score);
  const sd     = stdDev(scores);
  const minS   = Math.min(...scores);
  const maxS   = Math.max(...scores);

  let label, desc, color;
  if (sd < 8) {
    label = 'Foarte constantă'; desc = 'Performanță previzibilă de la meci la meci.'; color = 'var(--green)';
  } else if (sd < 14) {
    label = 'Constantă'; desc = 'Variație moderată — echipa e relativ predictibilă.'; color = 'var(--accent)';
  } else {
    label = 'Variabilă'; desc = 'Diferențe mari între meciuri — greu de anticipat.'; color = 'var(--red)';
  }

  el.innerHTML = `
    <div class="team-card-header">
      <p class="section-title" style="margin:0">📊 Consistență</p>
    </div>
    <div class="consistency-main">
      <div class="consistency-label" style="color:${color}">${label}</div>
      <div class="consistency-sd">±${sd.toFixed(1)} puncte variație standard</div>
    </div>
    <div class="consistency-range">
      <span class="consistency-range-item"><span class="cr-dot" style="background:var(--red)"></span>${minS} min</span>
      <div class="consistency-bar-wrap">
        <div class="consistency-bar-track">
          <div class="consistency-bar-fill" style="width:${Math.min(100, (sd / 20) * 100).toFixed(0)}%;background:${color}"></div>
        </div>
      </div>
      <span class="consistency-range-item">${maxS} max<span class="cr-dot" style="background:var(--green);margin-left:6px"></span></span>
    </div>
    <div class="consistency-desc">${desc}</div>
  `;
}

function renderCompTable(games) {
  const el = qs('team-comp-section'); if (!el) return;
  if (!games.length) { el.innerHTML = ''; return; }

  // Group by competition_key
  const groups = {};
  COMP_ORDER.forEach(k => { groups[k] = []; });
  games.forEach(g => {
    if (groups[g.competition_key]) groups[g.competition_key].push(g);
  });

  const rows = COMP_ORDER
    .filter(k => groups[k].length > 0)
    .map(k => {
      const gs = groups[k];
      const w  = gs.filter(g => g.result === 'W').length;
      const l  = gs.length - w;
      return {
        key:     k,
        gp:      gs.length,
        w, l,
        avgScored:  avg(gs.map(g => g.ubt_score)),
        avgAllowed: avg(gs.map(g => g.opp_score)),
        avgDiff:    avg(gs.map(g => g.score_diff)),
      };
    });

  // Desktop table
  const tableRows = rows.map(r => {
    const diffColor = r.avgDiff >= 0 ? 'var(--accent)' : 'var(--red)';
    return `<tr>
      <td><span class="comp-badge comp-${r.key}">${COMP_LABEL[r.key]}</span></td>
      <td>${r.gp}</td>
      <td><span style="color:var(--green);font-weight:700">${r.w}</span> / <span style="color:var(--red)">${r.l}</span></td>
      <td>${r.avgScored.toFixed(1)}</td>
      <td>${r.avgAllowed.toFixed(1)}</td>
      <td style="color:${diffColor};font-weight:700">${fmtDiff(r.avgDiff)}</td>
    </tr>`;
  }).join('');

  // Mobile cards
  const mobileCards = rows.map(r => {
    const diffColor = r.avgDiff >= 0 ? 'var(--accent)' : 'var(--red)';
    return `<div class="comp-mobile-card">
      <div class="comp-mobile-header">
        <span class="comp-badge comp-${r.key}">${COMP_LABEL[r.key]}</span>
        <span class="comp-mobile-gp">${r.gp} meciuri · <span style="color:var(--green)">${r.w}V</span> <span style="color:var(--red)">${r.l}Î</span></span>
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
          <span class="comp-mobile-stat-val" style="color:${diffColor}">${fmtDiff(r.avgDiff)}</span>
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
              <th>Competiție</th>
              <th>Meciuri</th>
              <th>V / Î</th>
              <th>Marcate/meci</th>
              <th>Primite/meci</th>
              <th>Diferență</th>
            </tr>
          </thead>
          <tbody>${tableRows}</tbody>
        </table>
      </div>
      <div class="comp-mobile-cards">${mobileCards}</div>
    </div>
  `;
}

/* ─── Initial render (before DOM is ready — runs after buildPageHTML) ───────── */
function renderAll(games) {
  // Called before HTML scaffold exists; actual rendering happens in renderDynamic
  // after attachFilterListeners runs. This is a no-op placeholder.
}
