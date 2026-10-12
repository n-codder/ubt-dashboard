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

    <!-- 6b. ALL PLAYERS TABLE -->
    ${players.length ? buildPlayersTable(players) : ''}

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
  const href  = p.player_id ? `player.html?id=${p.player_id}` : null;

  const inner = `
    <div class="game-performer${href ? ' game-performer-link' : ''}">
      <span class="performer-medal">${medal}</span>
      <div class="performer-info">
        <div class="performer-name">${name}</div>
        <div class="performer-sub">${reb > 0 ? reb + ' rec' : ''} ${ast > 0 ? ast + ' pase' : ''} · PIR ${pir}</div>
      </div>
      <div class="performer-pts">${pts}<span class="performer-pts-lbl">pct</span></div>
    </div>
  `;
  return href ? `<a class="performer-anchor" href="${href}">${inner}</a>` : inner;
}

/* ─── All players table ────────────────────────────────────────────────────── */
function buildPlayersTable(players) {
  const sorted = [...players]
    .filter(p => (+p.minutes_played || 0) > 0)
    .sort((a, b) => (+b.pir || 0) - (+a.pir || 0));

  if (!sorted.length) return '';

  const rows = sorted.map(p => {
    const name = toTitleCase(String(p.player_name || ''));
    const href = p.player_id ? `player.html?id=${p.player_id}` : null;
    const nameCell = href
      ? `<a class="ptbl-name-link" href="${href}">${name}</a>`
      : name;
    const minRaw = +p.minutes_played || 0;
    const min    = minRaw > 0 ? Math.floor(minRaw) + "'" : '—';
    const pts    = +p.points    || 0;
    const reb    = +p.rebounds  || 0;
    const ast    = +p.assists   || 0;
    const stl    = +p.steals    || 0;
    const blk    = +p.blocks    || 0;
    const to     = +p.turnovers || 0;
    const pm     = p.plus_minus != null && p.plus_minus !== ''
      ? ((+p.plus_minus >= 0 ? '+' : '') + p.plus_minus) : '—';
    const pirVal = p.pir != null && p.pir !== '' ? +p.pir : null;
    const pir    = pirVal != null ? pirVal.toFixed(0) : '—';
    const pirCls = pirVal == null ? '' : pirVal >= 15 ? 'pir-high' : pirVal >= 5 ? 'pir-mid' : pirVal < 0 ? 'pir-neg' : '';

    return `<tr>
      <td class="ptbl-name">${nameCell}</td>
      <td>${min}</td>
      <td class="ptbl-pts">${pts}</td>
      <td>${reb}</td>
      <td>${ast}</td>
      <td>${stl}</td>
      <td>${blk}</td>
      <td>${to}</td>
      <td>${pm}</td>
      <td class="ptbl-pir ${pirCls}">${pir}</td>
    </tr>`;
  }).join('');

  return `
  <div class="game-section">
    <details class="game-players-details">
      <summary class="game-players-summary">
        <span class="game-players-summary-label">Toți jucătorii din meci</span>
        <svg class="game-players-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M6 9l6 6 6-6"/></svg>
      </summary>
      <div class="players-table-scroll">
        <table class="players-table">
          <thead>
            <tr>
              <th class="ptbl-name">Jucător</th>
              <th>Min</th>
              <th>Pct</th>
              <th>Rec</th>
              <th>Pase</th>
              <th>Furt</th>
              <th>Blk</th>
              <th>Pierd</th>
              <th>+/-</th>
              <th>PIR</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </details>
  </div>`;
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

  const fmtN = v => (v == null || isNaN(+v)) ? '—' : (+v % 1 === 0 ? String(+v) : (+v).toFixed(1));
  const fmtP = (m, a) => (+a > 0) ? (+m / +a * 100).toFixed(1) + '%' : '—';

  // Bar width as % of combined total (0–100); for pct stats uses the ratio of pcts
  function barW(uv, ov) {
    const u = +uv, o = +ov;
    if (!isFinite(u) || !isFinite(o) || u + o === 0) return 50;
    return Math.round(u / (u + o) * 100);
  }
  // For shooting, compare percentages
  function barWpct(um, ua, om, oa) {
    const u = +ua > 0 ? +um / +ua : 0;
    const o = +oa > 0 ? +om / +oa : 0;
    return barW(u, o);
  }

  // true = higher is better for U-BT
  function isBetter(uv, ov, higherGood = true) {
    const u = +uv, o = +ov;
    if (!isFinite(u) || !isFinite(o) || u === o) return null;
    return higherGood ? u > o : u < o;
  }
  function isBetterPct(um, ua, om, oa) {
    const u = +ua > 0 ? +um / +ua : 0;
    const o = +oa > 0 ? +om / +oa : 0;
    return isBetter(u, o);
  }

  // Build a bar row: [label, ubt_display, opp_display, ubt_bar_pct, ubt_wins (bool|null)]
  const hasPir = ubt.pir != null && !isNaN(+ubt.pir);
  const rows = [
    ...(hasPir ? [['PIR',
      fmtN(ubt.pir), fmtN(opp.pir),
      barW(ubt.pir, opp.pir), isBetter(ubt.pir, opp.pir)
    ]] : []),
    ['2 puncte',
      `${fmtN(ubt.fg2m)}/${fmtN(ubt.fg2a)} (${fmtP(ubt.fg2m, ubt.fg2a)})`,
      `${fmtN(opp.fg2m)}/${fmtN(opp.fg2a)} (${fmtP(opp.fg2m, opp.fg2a)})`,
      barWpct(ubt.fg2m, ubt.fg2a, opp.fg2m, opp.fg2a),
      isBetterPct(ubt.fg2m, ubt.fg2a, opp.fg2m, opp.fg2a),
    ],
    ['3 puncte',
      `${fmtN(ubt.fg3m)}/${fmtN(ubt.fg3a)} (${fmtP(ubt.fg3m, ubt.fg3a)})`,
      `${fmtN(opp.fg3m)}/${fmtN(opp.fg3a)} (${fmtP(opp.fg3m, opp.fg3a)})`,
      barWpct(ubt.fg3m, ubt.fg3a, opp.fg3m, opp.fg3a),
      isBetterPct(ubt.fg3m, ubt.fg3a, opp.fg3m, opp.fg3a),
    ],
    ['Libere',
      `${fmtN(ubt.ftm)}/${fmtN(ubt.fta)} (${fmtP(ubt.ftm, ubt.fta)})`,
      `${fmtN(opp.ftm)}/${fmtN(opp.fta)} (${fmtP(opp.ftm, opp.fta)})`,
      barWpct(ubt.ftm, ubt.fta, opp.ftm, opp.fta),
      isBetterPct(ubt.ftm, ubt.fta, opp.ftm, opp.fta),
    ],
    ['Recuperări',   fmtN(ubt.reb),   fmtN(opp.reb),   barW(ubt.reb, opp.reb),   isBetter(ubt.reb, opp.reb)],
    ['— Ofensive',   fmtN(ubt.oreb),  fmtN(opp.oreb),  barW(ubt.oreb, opp.oreb), isBetter(ubt.oreb, opp.oreb)],
    ['— Defensive',  fmtN(ubt.dreb),  fmtN(opp.dreb),  barW(ubt.dreb, opp.dreb), isBetter(ubt.dreb, opp.dreb)],
    ['Pase decisive', fmtN(ubt.ast),  fmtN(opp.ast),   barW(ubt.ast, opp.ast),   isBetter(ubt.ast, opp.ast)],
    ['Intercepții',  fmtN(ubt.stl),   fmtN(opp.stl),   barW(ubt.stl, opp.stl),   isBetter(ubt.stl, opp.stl)],
    ['Blocaje',      fmtN(ubt.blk),   fmtN(opp.blk),   barW(ubt.blk, opp.blk),   isBetter(ubt.blk, opp.blk)],
    ['Pierderi',     fmtN(ubt.to),    fmtN(opp.to),    barW(opp.to, ubt.to),      isBetter(ubt.to, opp.to, false)],
    ['Faulturi',     fmtN(ubt.fouls), fmtN(opp.fouls), barW(opp.fouls, ubt.fouls),isBetter(ubt.fouls, opp.fouls, false)],
  ];

  const shortOpp = oppName.split(' ').slice(0, 3).join(' ');

  const rowHtml = rows.map(([label, uv, ov, uBarPct, uWins]) => {
    const oBarPct = 100 - uBarPct;
    const uCls = uWins === true ? 'bar-win' : uWins === false ? 'bar-loss' : 'bar-neutral';
    const oCls = uWins === false ? 'bar-win' : uWins === true ? 'bar-loss' : 'bar-neutral';
    const isSubrow = label.startsWith('—');
    return `
    <div class="cmp-row${isSubrow ? ' cmp-subrow' : ''}">
      <div class="cmp-side cmp-side-ubt">
        <span class="cmp-num cmp-num-ubt ${uCls}">${uv}</span>
        <div class="cmp-bar-wrap">
          <div class="cmp-bar cmp-bar-ubt ${uCls}" style="width:${uBarPct}%"></div>
        </div>
      </div>
      <div class="cmp-label">${label}</div>
      <div class="cmp-side cmp-side-opp">
        <div class="cmp-bar-wrap">
          <div class="cmp-bar cmp-bar-opp ${oCls}" style="width:${oBarPct}%"></div>
        </div>
        <span class="cmp-num cmp-num-opp ${oCls}">${ov}</span>
      </div>
    </div>`;
  }).join('');

  return `
    <div class="game-section">
      <p class="section-title">Comparație echipe</p>
      <div class="game-cmp-table">
        <div class="cmp-header">
          <span class="cmp-hdr-ubt">U-BT Cluj</span>
          <span class="cmp-hdr-center"></span>
          <span class="cmp-hdr-opp">${shortOpp}</span>
        </div>
        ${rowHtml}
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
