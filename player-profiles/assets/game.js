/* Meci — scor, sferturi, comparația pe echipe, jucători, față de un meci obișnuit, context */
(function () {
  'use strict';
  const U = UBT;
  const page = U.mount(null);
  const Q0 = new URLSearchParams(location.search);
  const FROM = Q0.get('from');
  const back = U.backTarget('start');

  let LOGOS = {};   // din config/site.json → team_logos (se editează din admin)
  const ORD = { 2: 'doua', 3: 'treia', 4: 'patra', 5: 'cincea', 6: 'șasea', 7: 'șaptea', 8: 'opta', 9: 'noua', 10: 'zecea' };
  const ST = { games: [], stats: {}, players: [], bio: {}, quarters: {}, sort: 'pts', msort: 'pts' };

  Promise.all([U.csv(U.URLS.games), U.csv(U.URLS.gameStats), U.csv(U.URLS.perGame), U.csv(U.URLS.players), U.site()]).then(async ([g, ts, pg, ps, cfg]) => {
    LOGOS = cfg.team_logos || {};
    ST.games = U.normGames(g);
    ST.stats = U.normTeamStats(ts);
    ST.bio = U.bioFromStats(ps);
    ST.players = pg.map(o => ({
      key: `${o.competition_key}|${o.season}|${o.game_code}`, id: o.player_id, name: U.cleanName(o.player_name), jersey: o.jersey,
      starter: U.isTrue(o.is_starter), min: +o.minutes_played, sec: +o.seconds_played, pts: +o.points, reb: +o.rebounds, ast: +o.assists,
      stl: +o.steals, blk: +o.blocks, to: +o.turnovers, pm: +o.plus_minus, pir: +o.pir,
    }));
    // Scorul pe sferturi: data/processed/quarters.csv (EuroCup, ABA, LN; generat de processors/quarters.py)
    (await U.csv(U.URLS.quarters)).forEach(o => {
      const u = [1, 2, 3, 4].map(n => +o['ubt_q' + n]), p = [1, 2, 3, 4].map(n => +o['opp_q' + n]);
      const cum = a => a.map((v, i) => a.slice(0, i + 1).reduce((t, x) => t + x, 0));
      ST.quarters[`${o.competition_key}|${o.season}|${o.game_code}`] = { u, o: p, uc: cum(u), oc: cum(p), uot: +o.ubt_ot || 0, oot: +o.opp_ot || 0 };
    });
    render();
  });

  U.on('psort', t => { ST.sort = t.dataset.v; render(true); });
  U.onChange('msort', v => { ST.msort = v; render(true); });
  U.on('prow', t => t.closest('.acc-row').classList.toggle('open'));

  const shortD = x => `${U.dShort(x.date)} ${x.date.slice(0, 4)}`;
  const tc = x => String(x || '').toLowerCase().replace(/(^|[\s'-])(\p{L})/gu, (m, a, c) => a + c.toUpperCase());
  const mz = n => `${n} ${n === 1 ? 'meci' : n >= 20 ? 'de meciuri' : 'meciuri'}`;
  const val = (p, k) => (k === 'min' ? U.mmss(p.sec) : k === 'pir' ? Math.round(p.pir) : p[k]);

  function render(keepScroll) {
    const y = window.scrollY;
    const G = ST.games;
    if (!G.length) { U.paint(page, U.backBar(back) + '<main class="main"><div class="empty">Nu am găsit meciuri.</div></main>'); return; }
    let g = G.find(x => x.code === Q0.get('id') && (!Q0.get('season') || x.season === Q0.get('season'))) || G[G.length - 1];
    const S = G.filter(x => x.season === g.season), i = S.indexOf(g);
    const comp = U.COMP[g.comp] || g.comp, seasonL = U.SL(g.season);
    const oppShort = g.opp.split(' ').slice(0, 3).join(' ');
    const s = ST.stats[g.skey];
    const PS = S.filter(x => x.comp === g.comp), pi = PS.indexOf(g);
    const P = ST.players.filter(p => p.key === g.key), on = P.filter(p => p.min > 0);
    document.title = `U-BT ${g.us}–${g.them} ${g.opp} · U-BT în cifre`;

    let h = U.backBar(back);
    h += scoreHead(g, comp);
    h += '<main class="main">';
    h += quarters(g, oppShort);
    h += teamCompare(g, s);
    if (on.length) h += topPlayers(on, g) + allPlayers(on, g);
    else h += '<div class="empty">Statisticile jucătorilor pentru acest meci nu sunt încă disponibile.</div>';
    h += vsTypical(g, s, PS, pi, comp, seasonL, oppShort);
    h += nav(S, i);
    h += '</main>';
    U.paint(page, h);
    if (keepScroll) window.scrollTo(0, y);
  }

  // ── 1. Scor ───────────────────────────────────────────────────────────────
  function scoreHead(g, comp) {
    const ph = (g.phase || '').toLowerCase().trim();
    const stage = ph === '' || ph === 'regular season' ? `Etapa ${g.round}` : ({ playoffs: 'Play-off', 'play-offs': 'Play-off', 'play-off': 'Play-off', 'final four': 'Final Four', final: 'Finala', finals: 'Finala', 'top 16': 'Top 16', top16: 'Top 16', quarterfinals: 'Sferturi de finală', semifinals: 'Semifinale', eighthfinals: 'Optimi de finală' })[ph] || tc(g.phase);
    const venue = g.venue ? tc(g.venue) : '';
    const att = g.att ? `${g.att.toLocaleString('ro-RO')} spectatori` : '';
    const ha = g.home ? 'Acasă' : 'În deplasare';
    const ad = Math.abs(g.diff), diffTxt = (g.diff > 0 ? '+' : U.MINUS) + ad;
    const oppInit = g.opp.split(/\s+/).filter(w => /^\p{Lu}/u.test(w)).map(w => w[0]).slice(0, 2).join('') || g.opp.slice(0, 2).toUpperCase();
    const oppLogo = LOGOS[g.opp] ? `<img src="${LOGOS[g.opp]}" alt="${U.esc(g.opp)}" class="sh-logo">` : `<span class="sh-ini">${U.esc(oppInit)}</span>`;
    const rc = g.win ? 'win' : 'loss';
    return `<div class="sh">
      <div class="sh-comp"><span>${U.esc(comp)}</span><i></i><span class="c-2">${U.esc(stage)}</span></div>
      <h1 class="sh-score">
        <span class="sh-team sh-us"><img src="assets/logos/ubt.svg" alt="U-BT Cluj-Napoca" class="sh-logo"><span class="sh-name">U-BT Cluj-Napoca</span><span class="sh-pts">${g.us}</span></span>
        <span class="sh-mid"><span class="sh-res sh-${rc}">${g.win ? 'Victorie' : 'Înfrângere'}</span><span class="sh-diff c-${rc}">${diffTxt}<span class="only-desk"> puncte</span></span></span>
        <span class="sh-team sh-them">${oppLogo}<span class="sh-name">${U.esc(g.opp)}</span><span class="sh-pts">${g.them}</span></span>
      </h1>
      <div class="sh-meta only-desk"><b>${U.dWeek(g.date)}</b><i>|</i><span>${ha}</span>${venue ? `<i>|</i><span>${U.esc(venue)}</span>` : ''}${att ? `<i>–</i><span>${att}</span>` : ''}</div>
      <div class="sh-meta-m only-mob"><b>${U.dWeek(g.date)}</b><span>${[ha, venue, att].filter(Boolean).map(U.esc).join(' · ')}</span></div>
    </div>`;
  }

  // ── 2. Scorul pe sferturi ─────────────────────────────────────────────────
  function quarters(g, oppShort) {
    const Q = ST.quarters[`${g.comp}|${g.season}|${g.code}`];
    if (!Q) return '';   // secțiunea dispare dacă lipsesc datele
    const ot = Q.uot > 0 || Q.oot > 0;
    const Us = ot ? [...Q.u, Q.uot] : Q.u, Os = ot ? [...Q.o, Q.oot] : Q.o;
    const UC = ot ? [...Q.uc, g.us] : Q.uc, OC = ot ? [...Q.oc, g.them] : Q.oc;
    const n = Us.length;
    const won = Us.filter((v, k) => v > Os[k]).length, lost = Us.filter((v, k) => v < Os[k]).length;
    const dif = Us.map((v, k) => v - Os[k]); let bi = 0; dif.forEach((d, k) => { if (Math.abs(d) > Math.abs(dif[bi])) bi = k; });
    const qn = k => (k === 4 ? 'prelungiri' : `sfertul ${k + 1}`);
    const cap = `U-BT a câștigat ${won} din ${n} ${ot ? 'perioade' : 'sferturi'}${lost + won < n ? ` (${n - won - lost} la egalitate)` : ''}. Cea mai mare diferență: ${qn(bi)}, ${Math.abs(dif[bi])} puncte pentru ${dif[bi] > 0 ? 'U-BT' : oppShort}.`;
    const cell = (a, b) => `<span class="q-c${a > b ? ' b' : a < b ? ' m' : ''}">${a}</span>`;
    const head = Us.map((_, k) => `<span>${k === 4 ? 'Prel.' : 'Sf. ' + (k + 1)}</span>`).join('');
    const cols = `style="--qn:${n}"`;
    const row = (name, nameShort, cls, A, B, fin, finB) => `<div class="q-row" ${cols}><span class="q-name ${cls}"><span class="only-desk">${U.esc(name)}</span><span class="only-mob">${U.esc(nameShort)}</span></span>${A.map((v, k) => cell(v, B[k])).join('')}<span class="q-fin${finB ? '' : ' m'}">${fin}</span></div>`;
    const run = UC.map((v, k) => v - OC[k]); const M = Math.max(10, ...run.map(Math.abs));
    const bars = run.map(d => {
      const hh = Math.max(4, Math.abs(d) / M * 85);   // % din jumătate
      const lab = d > 0 ? '+' + d : d < 0 ? U.MINUS + Math.abs(d) : '0';
      return `<div class="qb"><div class="qb-bar ${d > 0 ? 'p' : d < 0 ? 'n' : 'z'}" style="${d >= 0 ? `bottom:50%` : `top:50%`};height:${hh / 2}%"></div>
        <div class="qb-lab${d < 0 ? ' n' : ''}" style="${d >= 0 ? `bottom:calc(50% + ${hh / 2}% + 4px)` : `top:calc(50% + ${hh / 2}% + 4px)`}">${lab}</div></div>`;
    }).join('');
    return `<section class="sec">${U.secHead('Scorul pe sferturi', U.esc(cap))}
      <div class="q-grid">
        <div class="box q-tbl"><div class="q-row q-head" ${cols}><span></span>${head}<span class="t-r">Final</span></div>
          ${row('U-BT Cluj-Napoca', 'U-BT', 'us', Us, Os, g.us, g.win)}${row(g.opp, oppShort, 'them', Os, Us, g.them, !g.win)}
          <p class="q-note">Cu negru: echipa care a câștigat sfertul.</p></div>
        <div class="q-chart"><div class="label">Diferența de scor după fiecare sfert</div>
          <div class="q-bars" ${cols}><div class="q-zero"></div>${bars}</div>
          <div class="q-after" ${cols}>${Us.map((_, k) => `<span>${k === 4 ? 'Final' : 'După ' + (k + 1)}</span>`).join('')}</div>
          <p class="note">Peste linie: U-BT conducea. Sub linie: conducea ${U.esc(oppShort)}.</p></div>
      </div></section>`;
  }

  // ── 3. Comparația pe echipe (din câmpurile de echipă, nu suma jucătorilor) ─
  function teamCompare(g, s) {
    const head = U.secHead('Comparația pe echipe', 'Cum s-au descurcat cele două echipe la fiecare capitol.');
    if (!s) return `<section class="sec">${head}<div class="empty">Pentru acest meci nu avem încă statisticile pe echipe. Le adăugăm imediat ce apar în sursa oficială.</div></section>`;
    const DEF = [{ k: 'fg2', l: 'Aruncări de 2 puncte (%)', sh: 1 }, { k: 'fg3', l: 'Aruncări de 3 puncte (%)', sh: 1 }, { k: 'ft', l: 'Aruncări libere (%)', sh: 1, info: 'ft' },
      { k: 'reb', l: 'Recuperări', info: 'reb' }, { k: 'ast', l: 'Pase decisive', info: 'ast' }, { k: 'to', l: 'Mingi pierdute', low: 1 }, { k: 'pir', l: 'Eficiență', info: 'pir' }];
    const rows = DEF.map(r => {
      let u, o, uM, oM, uS = '', oS = '', uW, oW;
      if (r.sh) {
        const um = s[`ubt_${r.k}m`], ua = s[`ubt_${r.k}a`], om = s[`opp_${r.k}m`], oa = s[`opp_${r.k}a`];
        if (ua == null || oa == null) return '';
        u = ua ? um / ua : 0; o = oa ? om / oa : 0;
        uM = Math.round(u * 100) + '%'; oM = Math.round(o * 100) + '%'; uS = `${um} din ${ua}`; oS = `${om} din ${oa}`; uW = u * 100; oW = o * 100;
      } else {
        u = s['ubt_' + r.k]; o = s['opp_' + r.k];
        if (u == null || o == null) return '';
        uM = U.neg(Math.round(u)); oM = U.neg(Math.round(o)); const m = Math.max(u, o, 1); uW = Math.max(0, u) / m * 100; oW = Math.max(0, o) / m * 100;
      }
      const ub = u === o ? null : r.low ? u < o : u > o;
      const cls = b => (b === null ? 'eq' : b ? 'win' : 'lose');
      const uc = cls(ub), oc = cls(ub === null ? null : !ub);
      const lbl = `<span class="cmp-lbl"><span>${r.l}</span>${r.info ? U.infoK(r.info, 'c') : ''}</span>${r.low ? '<span class="cmp-note">mai puține e mai bine</span>' : ''}`;
      return `<div class="cmp-row">
        <div class="cmp-v cmp-u ${uc}"><b>${uM}</b><span>${uS}</span></div>
        <div class="cmp-bar cmp-bu"><div class="${uc}" style="width:${uW}%"></div></div>
        <div class="cmp-mid">${lbl}</div>
        <div class="cmp-bar cmp-bo"><div class="${oc}" style="width:${oW}%"></div></div>
        <div class="cmp-v cmp-o ${oc}"><b>${oM}</b><span>${oS}</span></div></div>`;
    }).join('');
    return `<section class="sec">${head}<div class="box cmp">
      <div class="cmp-head"><span class="cmp-hu">U-BT Cluj-Napoca</span><span class="cmp-vs">vs</span><span class="cmp-ho">${U.esc(g.opp)}</span></div>${rows}</div></section>`;
  }

  // ── 4. Top jucători ───────────────────────────────────────────────────────
  function topPlayers(on, g) {
    const from = `game:${g.code}:${g.season}`;
    const ln = p => `${U.pl(p.reb, 'recuperare', 'recuperări')} · ${U.pl(p.ast, 'pasă decisivă', 'pase decisive')} · eficiență ${U.neg(Math.round(p.pir))}`;
    const top = [...on].sort((a, b) => b.pts - a.pts || b.pir - a.pir).slice(0, 3);
    return `<section class="sec">${U.secHead('Top jucători', 'Jucătorii U-BT cu cele mai multe puncte în acest meci')}
      <div class="pcards3">${top.map((p, k) => {
        const b = ST.bio[p.id] || {};
        return `<div class="pcard-col"><div class="label-acc">Locul ${k + 1}</div><a href="${U.playerHref(p.id, from)}" class="pcard">
          ${U.photo(b.img, p.name, 'pcard-ph', '<span class="ph-none">fără fotografie</span>')}
          <span class="pcard-body"><span class="pcard-nm"><b>${U.esc(p.name)}</b><span>${U.esc(U.POS(b.pos))}</span></span>
            <span class="pcard-v"><b>${p.pts}</b><span>Puncte</span></span><span class="pcard-line">${ln(p)}</span></span></a></div>`;
      }).join('')}</div></section>`;
  }

  // ── 5. Toți jucătorii ─────────────────────────────────────────────────────
  function allPlayers(on, g) {
    const from = `game:${g.code}:${g.season}`;
    const COLS = [['min', 'Minute'], ['pts', 'Puncte'], ['reb', 'Recuperări', 'reb'], ['ast', 'Pase decisive', 'ast'], ['stl', 'Mingi furate'], ['blk', 'Capace', 'blk'], ['to', 'Mingi pierdute'], ['pir', 'Eficiență', 'pir']];
    const max = {}; COLS.forEach(([k]) => { max[k] = Math.max(...on.map(p => p[k])); });
    const best = (p, k) => k !== 'to' && k !== 'min' && p[k] === max[k] && max[k] > 0;
    const cellCls = (p, k) => (best(p, k) ? 'b' : (k === 'pir' && p.pir < 0) ? 'm' : '');
    // desktop: tabel cu ordonare pe coloane
    const head = `<div class="pt-row pt-head"><div class="pt-h pt-hname"><span>Jucător</span></div>${COLS.map(([k, l, inf]) => `
      <div class="pt-h"><button type="button" class="pt-sort${ST.sort === k ? ' on' : ''}" data-act="psort" data-v="${k}"><span>${l}${ST.sort === k ? ' ↓' : ''}</span></button>
        <div class="pt-i">${inf ? U.infoK(inf, 'r') : ''}</div></div>`).join('')}</div>`;
    const rows = [...on].sort((a, b) => b[ST.sort] - a[ST.sort] || b.pts - a.pts).map(p => `
      <div class="pt-row hov"><div class="pt-name"><span class="pt-j">${U.esc(p.jersey)}</span><a href="${U.playerHref(p.id, from)}">${U.esc(p.name)}</a>${p.starter ? '<span class="pt-st">Titular</span>' : ''}</div>
        ${COLS.map(([k]) => `<div class="t-n pt-n ${cellCls(p, k)}">${U.neg(val(p, k))}</div>`).join('')}</div>`).join('');
    // mobil: rânduri care se deschid
    const STL = [['pts', 'Puncte', 'puncte', 'punct'], ['min', 'Minute', 'min', 'min'], ['reb', 'Recuperări', 'recuperări', 'recuperare'], ['ast', 'Pase decisive', 'pase decisive', 'pasă decisivă'],
      ['stl', 'Mingi furate', 'mingi furate', 'minge furată'], ['blk', 'Capace', 'capace', 'capac'], ['to', 'Mingi pierdute', 'mingi pierdute', 'minge pierdută'], ['pir', 'Eficiență', 'eficiență', 'eficiență']];
    const LB = Object.fromEntries(STL.map(x => [x[0], x]));
    const sk = ST.msort;
    const list = [...on].sort((a, b) => b[sk] - a[sk] || b.pts - a.pts).map(p => {
      const line = ['min', 'pts', 'reb', 'ast'].filter(x => x !== sk).slice(0, 3).map(k => { const v = val(p, k); return `${U.neg(v)} ${v === 1 ? LB[k][3] : LB[k][2]}`; }).join(' · ');
      return `<div class="acc-row"><button type="button" class="acc-btn mp-btn" data-act="prow">
          <span class="mp-j">${U.esc(p.jersey)}</span>
          <span class="mp-mid"><span class="mp-nm">${U.esc(p.name)}${p.starter ? ' · titular' : ''}</span><span class="mp-line">${line}</span></span>
          <span class="bigstat"><span class="bigstat-v">${U.neg(val(p, sk))}</span><span class="bigstat-l">${LB[sk][1]}</span></span>
          <span class="acc-chev"></span></button>
        <div class="acc-body mp-body"><div class="cells4">${STL.map(([k, l]) => `<div class="cell"><span class="cell-v${best(p, k) ? ' best' : ''}">${U.neg(val(p, k))}</span><span class="cell-l">${l}</span></div>`).join('')}</div>
          <a href="${U.playerHref(p.id, from)}" class="mp-link">Profilul jucătorului →</a></div></div>`;
    }).join('');
    const opts = ['pts', 'pir', 'reb', 'ast', 'min'].map(k => `<option value="${k}"${k === sk ? ' selected' : ''}>${LB[k][1]}</option>`).join('');
    return `<section class="sec"><div class="sec-head"><h2 class="h2">Toți jucătorii U-BT</h2>
        <p class="lead only-desk">Click pe numele unei coloane ca să ordonezi după ea. Cu îngroșat: cea mai bună valoare din echipă.</p>
        <p class="lead only-mob">Atinge un jucător ca să vezi toate cifrele lui.</p></div>
      <div class="tbl pt only-desk">${head}${rows}</div>
      <div class="only-mob"><label class="msort">Ordonează după<select data-change="msort">${opts}</select></label>
        <div class="mp-list">${list}</div></div></section>`;
  }

  // ── 6. Față de un meci obișnuit + 7. În context ───────────────────────────
  function vsTypical(g, s, PS, pi, comp, seasonL, oppShort) {
    const SS = PS.filter(x => x !== g).map(x => ST.stats[x.skey]).filter(Boolean);
    const avgOf = f => { const v = SS.map(f).filter(x => x != null); return v.length ? v.reduce((t, x) => t + x, 0) / v.length : null; };
    let main = '';
    if (!s) main = '<div class="empty">Pentru acest meci nu avem încă statisticile pe echipe, așa că nu îl putem compara cu media.</div>';
    else if (SS.length < 3) {
      main = `<div class="tile vs-early"><span class="badge-early dark">Început de sezon</span><p>E prea devreme pentru o comparație: înainte de acest meci, U-BT a jucat ${SS.length === 0 ? 'niciun alt meci' : SS.length === 1 ? 'un singur alt meci' : SS.length + ' alte meciuri'} în ${U.esc(comp)} în sezonul ${seasonL}. Comparația apare de la al patrulea meci.</p></div>`;
    } else {
      const R = [['Puncte marcate', x => x.ubt_pts], ['Puncte primite', x => x.opp_pts, 1], ['Recuperări', x => x.ubt_reb], ['Pase decisive', x => x.ubt_ast], ['Mingi pierdute', x => x.ubt_to, 1], ['Aruncări de 3 reușite', x => x.ubt_fg3m]]
        .map(([l, f, low]) => {
          const v = f(s), a = avgOf(f);
          if (v == null || a == null) return '';
          const dv = v - a, good = low ? dv < 0 : dv > 0, same = Math.abs(dv) < 1;
          const sub = l === 'Aruncări de 3 reușite' && s.ubt_fg3a != null ? `din ${s.ubt_fg3a} încercări` : '';
          const pill = same ? '<span class="vs-same">Cât media</span>' : `<span class="vs-pill${good ? ' good' : ''}">${good ? 'Mai bine' : 'Mai slab'}</span>`;
          const vc = same ? 'c-2' : good ? '' : 'c-mute';
          return `<div class="vs-row"><span class="vs-l">${l}<span class="only-mob">${pill}</span></span>
            <span class="vs-v"><b class="${vc}">${Math.round(v)}</b>${sub ? `<span class="vs-sub">${sub}</span>` : ''}<span class="vs-am only-mob">media <b>${U.num(a)}</b></span></span>
            <span class="vs-a only-desk">${U.num(a)}</span><span class="vs-p only-desk">${pill}</span></div>`;
        }).join('');
      const lv = f => { const a = avgOf(f), v = f(s); if (a == null || v == null) return 0; const d = v - a; return d >= 3 ? 1 : d <= -3 ? -1 : 0; };
      const at = lv(x => x.ubt_pts), df = -lv(x => x.opp_pts);
      const T = { 1: 'peste nivelul obișnuit', 0: 'la nivelul obișnuit', '-1': 'sub nivelul obișnuit' };
      const headTxt = at === df ? `Un meci ${T[at]}, și în atac, și în apărare.` : `${T[at][0].toUpperCase() + T[at].slice(1)} în atac, ${T[df]} în apărare.`;
      main = `<div class="box vs"><p class="vs-head">${headTxt}</p><div class="vs-tbl">
        <div class="vs-row vs-th only-desk"><span></span><span class="t-r">În acest meci</span><span class="t-r">Media</span><span></span></div>${R}</div></div>`;
    }
    // În context
    let stk = 1; for (let j = pi - 1; j >= 0 && PS[j].win === g.win; j--) stk++;
    const last3 = PS.slice(Math.max(0, pi - 3), pi), w3 = last3.filter(x => x.win).length;
    const w4 = w3 + (g.win ? 1 : 0), n4 = last3.length + 1;
    const formTxt = !last3.length ? `Primul meci din ${comp} în acest sezon.` : stk >= 3 ? `A ${ORD[stk] || stk + '-a'} ${g.win ? 'victorie' : 'înfrângere'} la rând.` : `${w4 === 0 ? 'Nicio victorie' : w4 === 1 ? 'O victorie' : w4 + ' victorii'} în ultimele ${n4} meciuri.`;
    const dots = last3.concat([g]).map(x => `<span class="res md${x === g ? ' cur' : ''} ${x.win ? 'res-w' : 'res-l'}" title="${U.esc(`${shortD(x)} · ${x.opp} · ${x.us}–${x.them}`)}">${x.win ? 'V' : 'Î'}</span>`).join('');
    const PB = PS.slice(0, pi + 1), w = PB.filter(x => x.win).length, l = PB.length - w;
    const prevM = PS.slice(0, pi).filter(x => x.opp === g.opp).pop();
    const ctx = [
      ['Forma recentă', formTxt, dots],
      [`Bilanțul în ${comp}`, `După acest meci: ${w} ${w === 1 ? 'victorie' : 'victorii'} și ${l} ${l === 1 ? 'înfrângere' : 'înfrângeri'}.`],
      [`Cu ${oppShort}`, prevM ? `Meciul anterior: ${prevM.win ? 'victorie' : 'înfrângere'} ${prevM.us}–${prevM.them}, ${shortD(prevM)}.` : 'Prima întâlnire din acest sezon.'],
    ].map(([lb, tx, d]) => `<div class="ctx-it"><span class="ctx-l">${U.esc(lb)}</span>${d ? `<span class="ctx-dots">${d}</span>` : ''}<span class="ctx-t">${U.esc(tx)}</span></div>`).join('');
    return `<section class="sec">${U.secHead('Față de un meci obișnuit', `Comparat cu media U-BT în celelalte ${mz(SS.length)} din ${U.esc(comp)}, sezonul ${seasonL}.`)}
      <div class="vs-grid">${main}<div class="tile ctx"><div class="label">În context</div>${ctx}</div></div></section>`;
  }

  // ── 8. Carduri de final ───────────────────────────────────────────────────
  function nav(S, i) {
    const keep = FROM ? '&from=' + encodeURIComponent(FROM) : '';
    const card = x => ({ sub: `${x.win ? 'Victorie' : 'Înfrângere'} ${x.us}–${x.them} cu ${x.opp} · ${shortD(x)}`, href: `game.html?id=${encodeURIComponent(x.code)}&season=${x.season}${keep}` });
    const a = S[i - 1] ? { title: 'Meciul anterior', ...card(S[i - 1]), dir: 'prev' } : { title: 'Toate meciurile', sub: 'Forma echipei și graficul sezonului, pe pagina de start', href: 'index.html', dir: 'prev' };
    const b = S[i + 1] ? { title: 'Meciul următor', ...card(S[i + 1]), dir: 'next' } : { title: 'Jucătorii', sub: 'Ultimul meci din sezon până acum. Vezi lotul complet, cu statistici.', href: 'players.html', dir: 'next' };
    return U.endCards(a, b).replace('endcard endcard-prev', 'endcard endcard-prev endcard-right');
  }
})();
