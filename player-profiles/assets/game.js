/* ─── Game detail page · game.js ─────────────────────────────────────────── */

const _cb = Date.now();
const GAMES_URL      = `../data/processed/games.csv?v=${_cb}`;
const PG_STATS_URL   = `../data/processed/player_stats_per_game_normalized.csv?v=${_cb}`;
const GAME_STATS_URL = `../data/processed/game_stats.csv?v=${_cb}`;

const COMP_LABEL = {
  eurocup:        'EuroCup',
  aba_league:     'ABA League',
  liga_nationala: 'Liga Națională',
};

/* ─── Helpers ──────────────────────────────────────────────────────────────── */
function toTitleCase(str) {
  return str.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}
function avg(arr) {
  if (!arr.length) return 0;
  return arr.reduce((s, v) => s + v, 0) / arr.length;
}
function pct(made, att) {
  return att > 0 ? (made / att * 100).toFixed(1) + '%' : '—';
}
function fmtDiff(v) {
  return (v >= 0 ? '+' : '') + v;
}
function fmtDate(d) {
  return new Date(d).toLocaleDateString('ro-RO', { day: 'numeric', month: 'long', year: 'numeric' });
}
function sum(arr, key) {
  return arr.reduce((s, r) => s + (+r[key] || 0), 0);
}

/* ─── URL param ────────────────────────────────────────────────────────────── */
const gameCode = new URLSearchParams(location.search).get('id');

/* ─── Load ─────────────────────────────────────────────────────────────────── */
function parseCsv(url) {
  return new Promise((res, rej) => Papa.parse(url, {
    download: true, header: true, dynamicTyping: true,
    complete: d => res(d.data.filter(r => r.game_code != null)),
    error: rej,
  }));
}

Promise.all([
  parseCsv(GAMES_URL),
  parseCsv(PG_STATS_URL),
  parseCsv(GAME_STATS_URL).catch(() => []),
]).then(([allGames, allPlayerGames, allGameStats]) => {
  const game = allGames.find(g => String(g.game_code) === String(gameCode));

  if (!game) {
    document.getElementById('game-page').innerHTML = `
      <div class="error-state">
        <div class="error-state-title">Meciul nu a fost găsit</div>
        <div class="error-state-sub">ID: ${gameCode}</div>
      </div>`;
    return;
  }

  const gamePlayers = allPlayerGames.filter(r => String(r.game_code) === String(gameCode));

  // Exact team box-score stats (may be null if not in game_stats.csv)
  const gameStats = allGameStats.find(r =>
    String(r.game_code) === String(gameCode) &&
    r.competition === game.competition
  ) || null;

  const compKey = game.competition_key ||
    (game.competition === 'EuroCup' ? 'eurocup' :
     game.competition === 'ABA League' ? 'aba_league' : 'liga_nationala');

  const sameComp = allGames.filter(g =>
    (g.competition_key || g.competition) === (game.competition_key || game.competition)
  );

  const seasonAvgScored   = avg(sameComp.map(g => +g.ubt_score));
  const seasonAvgConceded = avg(sameComp.map(g => +g.opp_score));

  const sortedGames = allGames
    .filter(g => g.date)
    .sort((a, b) => new Date(a.date) - new Date(b.date));

  render(game, gamePlayers, gameStats, seasonAvgScored, seasonAvgConceded, sortedGames, compKey);
}).catch(err => {
  document.getElementById('game-page').innerHTML =
    `<div class="error-state"><div class="error-state-title">Eroare la încărcare</div><div class="error-state-sub">${err}</div></div>`;
});

/* ─── Render ────────────────────────────────────────────────────────────────── */
function render(game, players, gameStats, seasonAvgScored, seasonAvgConceded, sortedGames, compKey) {
  const ubt     = +game.ubt_score;
  const opp     = +game.opp_score;
  const diff    = +game.score_diff;
  const isWin   = game.result === 'W';
  const opp_name = toTitleCase(String(game.opponent));
  const comp_label = COMP_LABEL[compKey] || game.competition;
  const dateFmt  = fmtDate(game.date);
  const phase    = game.phase && game.phase !== 'REGULAR SEASON'
    ? ' · ' + toTitleCase(String(game.phase)) : '';
  const venue    = game.venue && String(game.venue).trim() !== ''
    ? String(game.venue) : null;

  // ── 1. Insights ───────────────────────────────────────────────────────────
  const takeaway = buildTakeaway(ubt, opp, diff, isWin, seasonAvgScored, seasonAvgConceded);
  const moment   = buildMoment(diff, isWin);
  const context  = buildContext(game, sortedGames, seasonAvgScored, ubt);

  // ── 2. Top performers ─────────────────────────────────────────────────────
  const topPerformers = [...players]
    .sort((a, b) => (+b.points || 0) - (+a.points || 0))
    .slice(0, 3)
    .filter(p => (+p.points || 0) > 0);

  // ── 3. Team stats — prefer exact data from game_stats.csv ─────────────────
  const gs = gameStats;
  const hasExact = gs != null;

  // Back link: prefer referrer, fall back to team page
  const backHref = document.referrer.includes('game.html') || !document.referrer
    ? 'team.html' : document.referrer.split('/').pop() || 'team.html';

  document.getElementById('game-page').innerHTML = `

    <!-- Back -->
    <a class="back-btn" href="${backHref}">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
        <path d="M19 12H5M12 19l-7-7 7-7"/>
      </svg>
      Înapoi
    </a>

    <!-- 1. HEADER MECI -->
    <div class="game-header">
      <div class="game-meta-top">
        <span class="comp-badge comp-${compKey}">${comp_label}</span>
        <span class="game-date-str">${dateFmt}${phase}</span>
        ${venue ? `<span class="game-venue">${venue}</span>` : ''}
      </div>

      <div class="game-score-block">
        <div class="game-team-col">
          <div class="game-team-name">U-BT Cluj</div>
          <div class="game-score ${isWin ? 'score-win' : 'score-loss'}">${ubt}</div>
        </div>
        <div class="game-score-sep">
          <div class="game-vs-dash">–</div>
          <div class="game-result-pill ${isWin ? 'result-win' : 'result-loss'}">
            ${isWin ? 'Victorie' : 'Înfrângere'}
          </div>
          <div class="game-diff ${diff > 0 ? 'diff-pos' : 'diff-neg'}">${fmtDiff(diff)} pct</div>
        </div>
        <div class="game-team-col">
          <div class="game-team-name opp">${opp_name}</div>
          <div class="game-score opp-score">${opp}</div>
        </div>
      </div>
    </div>

    <!-- 2. KEY TAKEAWAY -->
    <div class="game-takeaway ${isWin ? 'takeaway-win' : 'takeaway-loss'}">
      <span class="takeaway-icon">${isWin ? '✓' : '✗'}</span>
      ${takeaway}
    </div>

    <!-- 3. STATS RAPIDE -->
    <div class="game-quick-stats">
      <div class="game-stat-box">
        <div class="game-stat-val accent">${ubt}</div>
        <div class="game-stat-lbl">Puncte marcate</div>
      </div>
      <div class="game-stat-box">
        <div class="game-stat-val">${opp}</div>
        <div class="game-stat-lbl">Puncte primite</div>
      </div>
      <div class="game-stat-box">
        <div class="game-stat-val ${diff > 0 ? 'pos' : 'neg'}">${fmtDiff(diff)}</div>
        <div class="game-stat-lbl">Diferență</div>
      </div>
      <div class="game-stat-box">
        <div class="game-stat-val ${isWin ? 'win-col' : 'loss-col'}">${isWin ? 'V' : 'Î'}</div>
        <div class="game-stat-lbl">Rezultat</div>
      </div>
    </div>

    <!-- 5. MOMENT CHEIE + 8. CONTEXT -->
    <div class="game-insight-row">
      <div class="game-insight-card">
        <div class="game-insight-icon">⚡</div>
        <div class="game-insight-text">${moment}</div>
      </div>
      ${context ? `<div class="game-insight-card">
        <div class="game-insight-icon">📊</div>
        <div class="game-insight-text">${context}</div>
      </div>` : ''}
    </div>

    <!-- 6. TOP PERFORMERS -->
    ${topPerformers.length ? `
    <div class="game-section">
      <p class="section-title">Top performeri</p>
      <div class="game-performers-list">
        ${topPerformers.map((p, i) => buildPerformerCard(p, i)).join('')}
      </div>
    </div>` : ''}

    <!-- 7. COMPARAȚIE ECHIPĂ -->
    ${buildTeamComparison(gs, players, opp_name)}
  `;
}

/* ─── Performer card ───────────────────────────────────────────────────────── */
function buildPerformerCard(p, rank) {
  const name  = toTitleCase(String(p.player_name || ''));
  const pts   = +p.points || 0;
  const pir   = p.pir != null ? (+p.pir).toFixed(0) : '—';
  const reb   = +p.rebounds || 0;
  const ast   = +p.assists  || 0;
  const medal = rank === 0 ? '🥇' : rank === 1 ? '🥈' : '🥉';

  return `
    <div class="game-performer">
      <span class="performer-medal">${medal}</span>
      <div class="performer-info">
        <div class="performer-name">${name}</div>
        <div class="performer-sub">${reb > 0 ? reb + ' rec' : ''} ${ast > 0 ? ast + ' pase' : ''} · PIR ${pir}</div>
      </div>
      <div class="performer-pts">${pts}<span class="performer-pts-lbl">pct</span></div>
    </div>
  `;
}

/* ─── Team comparison ───────────────────────────────────────────────────────── */
function buildTeamComparison(gs, players, oppName) {
  let ubt, opp;

  if (gs) {
    ubt = {
      pts:  gs.ubt_pts,  pir:  gs.ubt_pir,
      fg2m: gs.ubt_fg2m, fg2a: gs.ubt_fg2a,
      fg3m: gs.ubt_fg3m, fg3a: gs.ubt_fg3a,
      ftm:  gs.ubt_ftm,  fta:  gs.ubt_fta,
      oreb: gs.ubt_oreb, dreb: gs.ubt_dreb, reb: gs.ubt_reb,
      ast:  gs.ubt_ast,  stl:  gs.ubt_stl,  blk: gs.ubt_blk,
      to:   gs.ubt_to,   fouls: gs.ubt_fouls,
    };
    opp = {
      pts:  gs.opp_pts,  pir:  gs.opp_pir,
      fg2m: gs.opp_fg2m, fg2a: gs.opp_fg2a,
      fg3m: gs.opp_fg3m, fg3a: gs.opp_fg3a,
      ftm:  gs.opp_ftm,  fta:  gs.opp_fta,
      oreb: gs.opp_oreb, dreb: gs.opp_dreb, reb: gs.opp_reb,
      ast:  gs.opp_ast,  stl:  gs.opp_stl,  blk: gs.opp_blk,
      to:   gs.opp_to,   fouls: gs.opp_fouls,
    };
  } else if (players.length) {
    // Fall back to aggregated player stats (U-BT only, no opponent data)
    const s = {
      fg2m: sum(players, 'fg2_made'), fg2a: sum(players, 'fg2_att'),
      fg3m: sum(players, 'fg3_made'), fg3a: sum(players, 'fg3_att'),
      ftm:  sum(players, 'ft_made'),  fta:  sum(players, 'ft_att'),
      reb:  sum(players, 'rebounds'), ast:  sum(players, 'assists'),
      to:   sum(players, 'turnovers'),stl:  sum(players, 'steals'),
      blk:  sum(players, 'blocks'),
    };
    return `
    <div class="game-section">
      <p class="section-title">Statistici U-BT</p>
      <div class="game-team-stats-grid">
        ${[
          [pct(s.fg2m+s.fg3m, s.fg2a+s.fg3a), 'FG%'],
          [pct(s.fg3m, s.fg3a), '3PT%'],
          [pct(s.ftm, s.fta), 'FT%'],
          [s.reb, 'Recuperări'],
          [s.ast, 'Pase decisive'],
          [s.to,  'Pierderi minge'],
          [s.stl, 'Intercepții'],
          [s.blk, 'Blocaje'],
        ].map(([v, l]) => `<div class="game-team-stat">
          <div class="game-team-stat-val">${v}</div>
          <div class="game-team-stat-lbl">${l}</div>
        </div>`).join('')}
      </div>
    </div>`;
  } else {
    return '';
  }

  // Build comparison rows
  const fmtN = v => (v == null || isNaN(+v)) ? '—' : (+v % 1 === 0 ? +v : (+v).toFixed(1));
  const fmtP = (m, a) => (a > 0) ? (+m / +a * 100).toFixed(1) + '%' : '—';

  // higher = better for most, except turnovers/fouls
  function cmpClass(ubtVal, oppVal, higherIsBetter = true) {
    const u = +ubtVal, o = +oppVal;
    if (isNaN(u) || isNaN(o) || u === o) return '';
    return (higherIsBetter ? u > o : u < o) ? 'cmp-better' : 'cmp-worse';
  }

  const hasPir = ubt.pir != null;

  const rows = [
    ...(hasPir ? [[
      fmtN(ubt.pir), fmtN(opp.pir), 'PIR',
      cmpClass(ubt.pir, opp.pir)
    ]] : []),
    [
      `${fmtN(ubt.fg2m)}/${fmtN(ubt.fg2a)} (${fmtP(ubt.fg2m, ubt.fg2a)})`,
      `${fmtN(opp.fg2m)}/${fmtN(opp.fg2a)} (${fmtP(opp.fg2m, opp.fg2a)})`,
      '2 puncte',
      cmpClass(ubt.fg2m / (ubt.fg2a||1), opp.fg2m / (opp.fg2a||1))
    ],
    [
      `${fmtN(ubt.fg3m)}/${fmtN(ubt.fg3a)} (${fmtP(ubt.fg3m, ubt.fg3a)})`,
      `${fmtN(opp.fg3m)}/${fmtN(opp.fg3a)} (${fmtP(opp.fg3m, opp.fg3a)})`,
      '3 puncte',
      cmpClass(ubt.fg3m / (ubt.fg3a||1), opp.fg3m / (opp.fg3a||1))
    ],
    [
      `${fmtN(ubt.ftm)}/${fmtN(ubt.fta)} (${fmtP(ubt.ftm, ubt.fta)})`,
      `${fmtN(opp.ftm)}/${fmtN(opp.fta)} (${fmtP(opp.ftm, opp.fta)})`,
      'Libere',
      cmpClass(ubt.ftm / (ubt.fta||1), opp.ftm / (opp.fta||1))
    ],
    [fmtN(ubt.reb),   fmtN(opp.reb),   'Recuperări',      cmpClass(ubt.reb, opp.reb)],
    [fmtN(ubt.oreb),  fmtN(opp.oreb),  '  Ofensive',      cmpClass(ubt.oreb, opp.oreb)],
    [fmtN(ubt.dreb),  fmtN(opp.dreb),  '  Defensive',     cmpClass(ubt.dreb, opp.dreb)],
    [fmtN(ubt.ast),   fmtN(opp.ast),   'Pase decisive',   cmpClass(ubt.ast, opp.ast)],
    [fmtN(ubt.stl),   fmtN(opp.stl),   'Intercepții',     cmpClass(ubt.stl, opp.stl)],
    [fmtN(ubt.blk),   fmtN(opp.blk),   'Blocaje',         cmpClass(ubt.blk, opp.blk)],
    [fmtN(ubt.to),    fmtN(opp.to),    'Pierderi minge',  cmpClass(ubt.to, opp.to, false)],
    [fmtN(ubt.fouls), fmtN(opp.fouls), 'Faulturi',        cmpClass(ubt.fouls, opp.fouls, false)],
  ];

  const shortOpp = oppName.split(' ').slice(0, 2).join(' ');

  return `
    <div class="game-section">
      <p class="section-title">Comparație echipe</p>
      <div class="game-cmp-table">
        <div class="game-cmp-header">
          <span class="cmp-team-label cmp-ubt">U-BT Cluj</span>
          <span class="cmp-stat-label"></span>
          <span class="cmp-team-label cmp-opp">${shortOpp}</span>
        </div>
        ${rows.map(([uv, ov, label, cls]) => `
        <div class="game-cmp-row">
          <span class="cmp-val cmp-val-ubt ${cls}">${uv}</span>
          <span class="cmp-stat-name">${label}</span>
          <span class="cmp-val cmp-val-opp ${cls ? (cls === 'cmp-better' ? 'cmp-worse' : 'cmp-better') : ''}">${ov}</span>
        </div>`).join('')}
      </div>
    </div>`;
}

/* ─── Insight generators ────────────────────────────────────────────────────── */
function buildTakeaway(ubt, opp, diff, isWin, seasonAvg, seasonAvgConceded) {
  if (diff > 10)  return 'Victorie clară — U-BT a dominat clar meciul.';
  if (diff < -10) return 'Înfrângere categorică — diferență prea mare.';
  if (Math.abs(diff) <= 5) return 'Meci echilibrat, decis la mici diferențe.';
  if (ubt > seasonAvg + 5) return `Atac excelent — ${ubt} puncte, peste media sezonului.`;
  if (ubt < seasonAvg - 5) return `Ofensiv sub nivelul obișnuit — ${ubt} puncte.`;
  if (opp > seasonAvgConceded + 5) return 'Probleme defensive — multe puncte primite.';
  return isWin ? 'Victorie muncită, rezultat strâns.' : 'Înfrângere la mică diferență.';
}

function buildMoment(diff, isWin) {
  if (Math.abs(diff) > 10 && isWin)  return 'U-BT a controlat meciul de la un capăt la altul.';
  if (Math.abs(diff) > 10 && !isWin) return 'Adversarul a preluat controlul și nu l-a mai cedat.';
  if (Math.abs(diff) <= 3)           return 'Meci decis în ultimele minute.';
  return isWin ? 'U-BT a gestionat bine presiunea.' : 'Nu s-a reușit revenirea.';
}

function buildContext(game, sortedGames, seasonAvg, ubt) {
  const idx = sortedGames.findIndex(g => String(g.game_code) === String(game.game_code));
  if (idx < 0) return null;

  const before = sortedGames.slice(0, idx);
  const allUbt = sortedGames.map(g => +g.ubt_score);
  const maxUbt = Math.max(...allUbt);

  // Check streak
  let streak = 0;
  const streakType = game.result;
  for (let i = idx - 1; i >= 0; i--) {
    if (sortedGames[i].result === streakType) streak++;
    else break;
  }

  if (ubt === maxUbt) return `Cel mai bun scor ofensiv din sezon — ${ubt} puncte.`;
  if (streak >= 2) {
    return streakType === 'W'
      ? `A ${streak + 1}-a victorie consecutivă.`
      : `A ${streak + 1}-a înfrângere consecutivă.`;
  }
  if (before.length >= 3) {
    const recent3 = before.slice(-3);
    const recentAvg = avg(recent3.map(g => +g.ubt_score));
    if (ubt > recentAvg + 5) return `Ofensiv mai bun decât în ultimele 3 meciuri.`;
    if (ubt < recentAvg - 5) return `Sub forma ultimelor meciuri la atac.`;
  }
  return null;
}
