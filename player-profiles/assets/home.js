/* Start — forma echipei, bilanț, condiția de victorie, toate meciurile, marcatori */
(function () {
  'use strict';
  const U = UBT;
  const page = U.mount('start');
  const S = { games: [], players: [], bio: {}, comp: 'all', season: null, ha: 'all' };
  const COMP_ALL = [{ key: 'all', label: 'Toate' }].concat(U.ORDER.map(k => ({ key: k, label: U.COMP[k] })));
  let G = [];   // meciurile afișate (după filtre), cronologic

  Promise.all([U.csv(U.URLS.games), U.csv(U.URLS.perGame), U.csv(U.URLS.players)]).then(([g, pg, ps]) => {
    S.games = U.normGames(g);
    S.bio = U.bioFromStats(ps);
    S.players = pg.map(o => ({ key: `${o.competition_key}|${o.season}|${o.game_code}`, id: o.player_id, name: U.cleanName(o.player_name), pts: +o.points, min: +o.minutes_played }));
    S.season = [...new Set(S.games.map(x => x.season))].sort().reverse()[0] || null;
    render();
  });

  U.on('comp', t => { S.comp = t.dataset.v; render(); });
  U.onChange('ha', v => { S.ha = v; render(); });
  U.onChange('season', v => { S.season = v; S.comp = 'all'; render(); });

  function card(g) {
    return {
      res: g.win ? 'V' : 'Î', resLong: g.win ? 'Victorie' : 'Înfrângere', cls: g.win ? 'c-win' : 'c-loss',
      date: U.dLong(g.date), dateShort: U.dShort(g.date), comp: U.SHORT[g.comp] || g.comp,
      where: g.home ? 'Acasă' : 'Deplasare', diffTxt: (g.diff > 0 ? '+' : U.MINUS) + Math.abs(g.diff),
      href: U.gameHref(g.code, g.season, 'start'),
    };
  }

  function render() {
    const seasons0 = [...new Set(S.games.map(g => g.season))].sort().reverse();
    const sg = S.games.filter(g => g.season === S.season);
    const comps = COMP_ALL.filter(c => c.key === 'all' || sg.some(g => g.comp === c.key)).map(c => ({ ...c, on: S.comp === c.key }));
    if (!comps.some(c => c.on)) { S.comp = 'all'; comps[0].on = true; }
    G = sg.filter(g => (S.comp === 'all' || g.comp === S.comp) && (S.ha === 'all' || (S.ha === 'home') === g.home));
    const n = G.length;
    const compLabel = (COMP_ALL.find(c => c.key === S.comp) || {}).label;
    const haLabel = S.ha === 'all' ? 'acasă și deplasare' : S.ha === 'home' ? 'doar acasă' : 'doar în deplasare';
    const context = `Sezonul ${U.SL(S.season)} · ${S.comp === 'all' ? 'toate competițiile' : compLabel}` + (S.ha === 'all' ? '' : ' · ' + (S.ha === 'home' ? 'acasă' : 'în deplasare'));
    const summary = [S.comp === 'all' ? 'Toate competițiile' : compLabel, S.ha === 'all' ? '' : haLabel[0].toUpperCase() + haLabel.slice(1), U.SL(S.season)].filter(Boolean).join(' · ');

    let html = U.filters({ comps, ha: S.ha, season: S.season, seasons: seasons0, summary, open: U.filtersOpen });
    html += '<main class="main gap-48">';
    html += formSection(n, sg.length === 0, S.season === seasons0[0] && n > 0 && n < 5, context);
    if (n > 0) html += bilant() + winCondition() + chartSection() + scorers();
    html += U.endCards(
      { href: 'team.html', title: 'Echipa', sub: 'Cum arată sezonul acesta față de cele trecute, pe fiecare competiție.', dir: 'next' },
      { href: 'players.html', title: 'Jucătorii', sub: 'Lotul complet, cu statistici și profil pentru fiecare jucător.', dir: 'next' });
    html += '</main>';
    U.paint(page, html);
  }

  // ── Forma echipei ─────────────────────────────────────────────────────────
  function formSection(n, seasonEmpty, early, context) {
    let h = `<section class="sec"><div class="sec-head"><h1 class="h1">Forma echipei</h1>
      <p class="lead"><span class="only-desk">Ultimele 5 meciuri, în ordine cronologică · ${U.esc(context)}</span><span class="only-mob">Ultimele 5 meciuri · ${U.esc(context)}</span></p></div>`;
    if (n === 0) {
      h += `<div class="empty empty-c"><div class="empty-t">${seasonEmpty ? 'Sezonul nu a început încă' : 'Niciun meci'}</div>
        <div>${seasonEmpty ? `Primul meci din sezonul ${U.SL(S.season)} va apărea aici imediat ce se joacă.` : 'Nu există meciuri pentru combinația de filtre aleasă. Încearcă „Toate” la competiție sau „Acasă și deplasare”.'}</div></div>`;
      return h + '</section>';
    }
    if (early) h += `<div class="early"><span class="badge-early">Început de sezon</span><span>U-BT a jucat ${n === 1 ? 'un singur meci' : n + ' meciuri'} în sezonul ${U.SL(S.season)}. Mediile și clasamentele devin mai relevante pe măsură ce se adună meciurile.</span></div>`;
    const last5 = G.slice(-5).map((g, i, a) => ({ g, c: card(g), last: i === a.length - 1 }));
    const ph = early ? 5 - last5.length : 0;
    // desktop: 5 carduri (3a)
    h += '<div class="form5 only-desk">' + last5.map(({ g, c, last }) => `
      <div class="form5-col"><div class="label-acc form5-lbl">${last ? 'Cel mai recent meci' : ''}</div>
        <a href="${c.href}" class="gcard${last ? ' last' : ''}">
          <span class="gcard-res">${U.resBadge(g.win, 'lg')}<span class="${c.cls}">${c.resLong}</span></span>
          <span class="gcard-score"><span class="gcard-us"><span>U-BT</span><b>${g.us}</b></span><span class="gcard-them"><span>${U.esc(g.opp)}</span><b>${g.them}</b></span></span>
          <span class="gcard-meta">${c.date} · ${c.comp} · ${c.where}</span>
        </a></div>`).join('') +
      Array.from({ length: ph }, () => '<div class="form5-col"><div class="form5-lbl"></div><div class="gcard-ph">Meci viitor</div></div>').join('') + '</div>';
    // mobil: listă, cel mai recent sus
    h += '<div class="only-mob"><div class="formlist">' + last5.slice().reverse().map(({ g, c, last }) => `
      <a href="${c.href}" class="formrow${last ? ' last' : ''}">${U.resBadge(g.win, 'lg')}
        <span class="formrow-mid">${last ? '<span class="formrow-lbl">Cel mai recent meci</span>' : ''}<span class="formrow-opp">${U.esc(g.opp)}</span><span class="formrow-meta">${c.dateShort} · ${c.comp} · ${c.where}</span></span>
        <span class="formrow-sc"><b>${g.us}</b><b class="c-mute">${g.them}</b></span></a>`).join('') +
      Array.from({ length: ph }, () => '<div class="formrow-ph">Meci viitor</div>').join('') +
      '</div><p class="note formlist-note">Scorul din dreapta: U-BT sus, adversarul jos</p></div>';
    return h + '</section>';
  }

  // ── Bilanț ────────────────────────────────────────────────────────────────
  function bilant() {
    const n = G.length, w = G.filter(g => g.win).length;
    const pf = G.reduce((s, g) => s + g.us, 0) / n, pa = G.reduce((s, g) => s + g.them, 0) / n, d = pf - pa;
    const pct = Math.round(w / n * 100) + '%';
    const diff = (d >= 0 ? '+' : U.MINUS) + U.num(Math.abs(d));
    const dc = d >= 0 ? 'c-win' : 'c-loss';
    const wl = `<span class="wl"><span class="w">${w}</span><span class="d">–</span><span class="l">${n - w}</span></span>`;
    return `<section class="sec">${U.secHead('Bilanțul sezonului')}
      <div class="kpi4 only-desk">
        <div class="tile"><div class="label">Victorii – Înfrângeri</div>${wl}<div class="tile-s">${pct} din ${n} meciuri câștigate</div></div>
        <div class="tile"><div class="label">Puncte marcate / meci</div><div class="tile-v">${U.num(pf)}</div><div class="tile-s">Media punctelor U-BT</div></div>
        <div class="tile"><div class="label">Puncte primite / meci</div><div class="tile-v">${U.num(pa)}</div><div class="tile-s">Media punctelor adversarilor</div></div>
        <div class="tile"><div class="tile-l"><span class="label">Diferență / meci</span>${U.infoK('diff', 'r')}</div><div class="tile-v ${dc}">${diff}</div><div class="tile-s">Marcate minus primite</div></div>
      </div>
      <div class="kpim only-mob">
        <div class="tile kpim-wl"><div class="kpim-row"><span class="label">Victorii – Înfrângeri</span><span class="tile-s">${pct} câștigate</span></div>${wl}
          <div class="kpim-bar"><div style="width:${pct}"></div></div></div>
        <div class="tile kpim-pts"><div class="label">Puncte pe meci, în medie</div>
          <div class="kpim-two"><div><div class="tile-v">${U.num(pf)}</div><div class="kpim-l">marcate de U-BT</div></div>
            <div class="kpim-sep"><div class="tile-v c-2">${U.num(pa)}</div><div class="kpim-l">primite</div></div></div>
          <div class="kpim-diff"><span>Diferență / meci</span>${U.infoK('diff', 'l')}<b class="${dc}">${diff}</b></div></div>
      </div></section>`;
  }

  // ── Condiția de victorie ──────────────────────────────────────────────────
  function winCondition() {
    const n = G.length;
    if (n < 3) return '';
    const pf = G.reduce((s, g) => s + g.us, 0) / n, t = Math.round(pf);
    const A = G.filter(g => g.us >= t), B = G.filter(g => g.us < t);
    const aW = A.filter(g => g.win).length, bW = B.filter(g => g.win).length;
    const ar = A.length ? aW / A.length * 100 : 0, br = B.length ? bW / B.length * 100 : 0;
    const col = r => (r >= 60 ? 'c-win' : r >= 40 ? '' : 'c-loss');
    let insight;
    if (!B.length || !A.length) insight = 'Prea puține meciuri de o parte a pragului pentru o concluzie.';
    else if (bW === 0) insight = `Când a marcat sub ${t} puncte, U-BT nu a câștigat niciun meci. Când a marcat cel puțin ${t}, a câștigat ${aW} din ${A.length}.`;
    else if (ar >= br * 1.2) insight = `Când marchează cel puțin ${t} puncte, U-BT câștigă de ${U.num(ar / br)} ori mai des decât sub prag.`;
    else insight = `Numărul de puncte marcate nu schimbă mult rezultatul: U-BT câștigă cam la fel de des cu cel puțin ${t} puncte și sub.`;
    const side = (lbl, pct, c, w, m) => `<div class="wc-side"><p class="wc-txt">Când U-BT marchează <b>${lbl}</b> puncte, câștigă <b class="${c}">${pct}</b> din meciuri.</p><p class="tile-s">${w} victorii din ${m} meciuri</p></div>`;
    return `<section class="sec"><div class="tile-l"><h2 class="h2">Condiția de victorie</h2>${U.info(`De unde vine pragul de ${t}?`, 'Este media punctelor marcate de U-BT în meciurile selectate.', 'Comparăm cât de des câștigă echipa când marchează cel puțin atât, față de când marchează mai puțin.', 'l')}</div>
      <div class="box wc"><div class="wc-grid">${side('cel puțin ' + t, Math.round(ar) + '%', col(ar), aW, A.length)}<div class="wc-div"></div>${side('sub ' + t, Math.round(br) + '%', col(br), bW, B.length)}</div>
        <p class="wc-ins">${U.esc(insight)}</p></div></section>`;
  }

  // ── Toate meciurile (grafic) ──────────────────────────────────────────────
  function chartSection() {
    const max = Math.ceil(Math.max(10, ...G.map(g => Math.abs(g.diff))) / 10) * 10;
    const y = v => `${(1 - v / max) * 50}%`;
    const ticks = []; for (let v = 10; v <= max; v += 10) ticks.push(v);
    const axis = [...ticks.slice().reverse(), 0, ...ticks.map(v => -v)].map(v => `<span style="top:${y(v)}">${v > 0 ? '+' + v : v < 0 ? U.MINUS + Math.abs(v) : '0'}</span>`).join('');
    const grid = [...ticks, ...ticks.map(v => -v)].map(v => `<div class="chart-grid" style="top:${y(v)}"></div>`).join('');
    const bars = G.map((g, i) => {
      const c = card(g), p = `max(4px, ${Math.abs(g.diff) / max * 100}%)`, close = Math.abs(g.diff) <= 3;
      const left = i < G.length / 2;
      const tip = `<div class="tip ${g.win ? 'tip-below' : 'tip-above'} ${left ? 'tip-l' : 'tip-r'}">
        <div class="tip-h">${U.resBadge(g.win)}<span class="tip-opp">${U.esc(g.opp)}</span></div>
        <div class="tip-score"><b>${g.us}<i>–</i>${g.them}</b><span class="tip-d ${g.win ? 'w' : 'l'}">${c.diffTxt}</span></div>
        <div class="tip-meta">${c.where} · ${c.date} · ${c.comp}</div></div>`;
      const dn = U.dNum(g.date);
      return `<a href="${c.href}" class="gbar" data-i="${i}" aria-label="${U.esc(`${c.date} · ${g.opp} · ${g.us}–${g.them}`)}">
        <div class="gbar-up">${!g.win ? tip : ''}${close && g.win ? `<div class="gbar-close c-win">${c.diffTxt}</div>` : ''}<div class="gbar-w" style="height:${g.win ? p : '0'}"></div></div>
        <div class="gbar-dn">${g.win ? tip : ''}<div class="gbar-l" style="height:${g.win ? '0' : p}"></div>${close && !g.win ? `<div class="gbar-close c-loss">${c.diffTxt}</div>` : ''}</div>
        <div class="xlab">${dn.d}<small>${dn.y}</small></div></a>`;
    }).join('');
    return `<section class="sec"><div class="sec-row"><div class="sec-head"><h2 class="h2">Toate meciurile</h2>
        <p class="lead only-desk">O bară pe meci. Sus: victorie, jos: înfrângere; înălțimea arată diferența de puncte. La meciurile strânse (cel mult 3 puncte) diferența e scrisă lângă bară. Derulează spre stânga pentru meciurile mai vechi.</p>
        <p class="lead only-mob">Sus: <b class="c-win">victorie</b>, jos: <b class="c-loss">înfrângere</b>. Înălțimea arată diferența de puncte.</p></div>
        <div class="legend only-desk"><span><i class="lg-w"></i>Victorie</span><span><i class="lg-l"></i>Înfrângere</span></div></div>
      <div class="chart gchart"><div class="chart-axis">${axis}</div>
        <div class="chart-scroll noscroll-m" data-scroll-end><div class="chart-track" data-gtrack>${grid}<div class="chart-zero" style="top:50%"></div>${bars}</div></div>
        <div class="chart-foot"><p class="hint only-desk">Treci cu mouse-ul peste o bară ca să vezi detaliile meciului · click pe bară deschide pagina meciului</p>
          <div class="only-mob" data-gsel><p class="hint">Atinge o bară ca să vezi detaliile meciului. Glisează spre dreapta pentru meciurile mai vechi.</p></div></div>
      </div></section>`;
  }

  // hover (desktop) / tap (mobil) — fără re-randare, ca să nu se piardă scroll-ul
  function setOn(track, i) {
    track.querySelectorAll('.gbar.on').forEach(b => b.classList.remove('on'));
    track.classList.toggle('has-on', i != null);
    if (i != null) track.querySelector(`.gbar[data-i="${i}"]`).classList.add('on');
  }
  page.addEventListener('mouseover', e => {
    if (U.isMobile()) return;
    const b = e.target.closest('.gbar'); if (!b) return;
    setOn(b.parentElement, b.dataset.i);
  });
  page.addEventListener('mouseout', e => {
    if (U.isMobile()) return;
    const b = e.target.closest('.gbar'); if (!b || b.contains(e.relatedTarget)) return;
    setOn(b.parentElement, null);
  });
  page.addEventListener('click', e => {
    const b = e.target.closest('.gbar');
    if (!b || !U.isMobile()) return;
    e.preventDefault();
    const track = b.parentElement, i = +b.dataset.i, was = b.classList.contains('on');
    setOn(track, was ? null : i);
    const box = page.querySelector('[data-gsel]');
    if (was) { box.innerHTML = '<p class="hint">Atinge o bară ca să vezi detaliile meciului. Glisează spre dreapta pentru meciurile mai vechi.</p>'; return; }
    const g = G[i], c = card(g);
    box.innerHTML = `<a href="${c.href}" class="selcard"><span class="tip-h">${U.resBadge(g.win)}<span class="tip-opp">${U.esc(g.opp)}</span></span>
      <span class="tip-score"><b>${g.us}<i>–</i>${g.them}</b><span class="tip-d ${g.win ? 'w' : 'l'}">${c.diffTxt}</span></span>
      <span class="tip-meta selcard-row"><span>${c.where} · ${c.date} · ${c.comp}</span><span class="selcard-go">Detalii →</span></span></a>`;
  });

  // ── Cei mai buni marcatori ────────────────────────────────────────────────
  function scorers() {
    const keys = new Set(G.map(g => g.key)), agg = {};
    S.players.filter(p => keys.has(p.key) && p.min > 0).forEach(p => {
      const a = agg[p.id] || (agg[p.id] = { id: p.id, name: p.name, gp: 0, pts: 0 }); a.gp++; a.pts += p.pts;
    });
    const minGp = Math.max(1, Math.ceil(G.length * 0.3));
    const top = Object.values(agg).filter(a => a.gp >= minGp).sort((a, b) => b.pts / b.gp - a.pts / a.gp).slice(0, 3);
    if (!top.length) return '';
    return `<section class="sec">${U.secHead('Cei mai buni marcatori', 'Ordonați după media punctelor pe meci')}
      <div class="pcards3">${top.map((a, i) => {
        const b = S.bio[a.id] || {};
        return `<div class="pcard-col"><div class="label-acc">Locul ${i + 1}</div><a href="${U.playerHref(a.id, 'start')}" class="pcard">
          ${U.photo(b.img, a.name, 'pcard-ph', '<span class="ph-none">fără fotografie</span>')}
          <span class="pcard-body"><span class="pcard-nm"><b>${U.esc(a.name)}</b><span>${U.esc(U.POS(b.pos))}</span></span>
            <span class="pcard-v"><b>${U.num(a.pts / a.gp)}</b><span>Puncte / meci</span></span>
            <span class="pcard-line">${U.meci(a.gp)} jucate</span></span></a></div>`;
      }).join('')}</div></section>`;
  }
})();
