/* ─── Game detail page · game.js ─────────────────────────────────────────── */

const _cb = Date.now();
const GAMES_URL    = `../data/processed/games.csv?v=${_cb}`;
const PG_STATS_URL = `../data/processed/player_stats_per_game_normalized.csv?v=${_cb}`;

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
Promise.all([
  new Promise((res, rej) => Papa.parse(GAMES_URL, {
    download: true, header: true, dynamicTyping: true,
    complete: d => res(d.data.filter(r => r.game_code != null)),
    error: rej,
  })),
  new Promise((res, rej) => Papa.parse(PG_STATS_URL, {
    download: true, header: true, dynamicTyping: true,
    complete: d => res(d.data.filter(r => r.game_code != null)),
    error: rej,
  })),
]).then(([allGames, allPlayerGames]) => {
  // Find the game
  const game = allGames.find(g => String(g.game_code) === String(gameCode));

  if (!game) {
    document.getElementById('game-page').innerHTML = `
      <div class="error-state">
        <div class="error-state-title">Meciul nu a fost găsit</div>
        <div class="error-state-sub">ID: ${gameCode}</div>
      </div>`;
    return;
  }

  // Per-game player stats for this game
  const gamePlayers = allPlayerGames.filter(r => String(r.game_code) === String(gameCode));

  // All games for context (same competition_key)
  const compKey = game.competition_key ||
    (game.competition === 'EuroCup' ? 'eurocup' :
     game.competition === 'ABA League' ? 'aba_league' : 'liga_nationala');

  const sameComp = allGames.filter(g =>
    (g.competition_key || g.competition) === (game.competition_key || game.competition)
  );

  // Season averages for context
  const seasonAvgScored   = avg(sameComp.map(g => +g.ubt_score));
  const seasonAvgConceded = avg(sameComp.map(g => +g.opp_score));

  // Sorted games for streak/context
  const sortedGames = allGames
    .filter(g => g.date)
    .sort((a, b) => new Date(a.date) - new Date(b.date));

  render(game, gamePlayers, seasonAvgScored, seasonAvgConceded, sortedGames, compKey);
}).catch(err => {
  document.getElementById('game-page').innerHTML =
    `<div class="error-state"><div class="error-state-title">Eroare la încărcare</div><div class="error-state-sub">${err}</div></div>`;
});

/* ─── Render ────────────────────────────────────────────────────────────────── */
function render(game, players, seasonAvgScored, seasonAvgConceded, sortedGames, compKey) {
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

  // ── 3. Team stats (aggregate) ─────────────────────────────────────────────
  const fg2Made = sum(players, 'fg2_made');
  const fg2Att  = sum(players, 'fg2_att');
  const fg3Made = sum(players, 'fg3_made');
  const fg3Att  = sum(players, 'fg3_att');
  const ftMade  = sum(players, 'ft_made');
  const ftAtt   = sum(players, 'ft_att');
  const reb     = sum(players, 'rebounds');
  const ast     = sum(players, 'assists');
  const tov     = sum(players, 'turnovers');
  const stl     = sum(players, 'steals');
  const blk     = sum(players, 'blocks');
  const fgAll   = pct(fg2Made + fg3Made, fg2Att + fg3Att);
  const fg3Pct  = pct(fg3Made, fg3Att);
  const ftPct   = pct(ftMade, ftAtt);

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

    <!-- 7. STATISTICI ECHIPĂ -->
    ${players.length ? `
    <div class="game-section">
      <p class="section-title">Statistici U-BT</p>
      <div class="game-team-stats-grid">
        <div class="game-team-stat">
          <div class="game-team-stat-val">${fgAll}</div>
          <div class="game-team-stat-lbl">FG%</div>
        </div>
        <div class="game-team-stat">
          <div class="game-team-stat-val">${fg3Pct}</div>
          <div class="game-team-stat-lbl">3PT%</div>
        </div>
        <div class="game-team-stat">
          <div class="game-team-stat-val">${ftPct}</div>
          <div class="game-team-stat-lbl">FT%</div>
        </div>
        <div class="game-team-stat">
          <div class="game-team-stat-val">${reb}</div>
          <div class="game-team-stat-lbl">Recuperări</div>
        </div>
        <div class="game-team-stat">
          <div class="game-team-stat-val">${ast}</div>
          <div class="game-team-stat-lbl">Pase decisive</div>
        </div>
        <div class="game-team-stat">
          <div class="game-team-stat-val">${tov}</div>
          <div class="game-team-stat-lbl">Pierderi minge</div>
        </div>
        <div class="game-team-stat">
          <div class="game-team-stat-val">${stl}</div>
          <div class="game-team-stat-lbl">Intercepții</div>
        </div>
        <div class="game-team-stat">
          <div class="game-team-stat-val">${blk}</div>
          <div class="game-team-stat-lbl">Blocaje</div>
        </div>
      </div>
    </div>` : ''}
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
