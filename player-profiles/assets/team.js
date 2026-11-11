/* Echipă — cap pe competiții, evoluția performanței, ce s-a schimbat, statisticile echipei,
   cele mai bune / slabe meciuri. Un singur selector de comparație pentru cele trei secțiuni din mijloc. */
(function () {
  'use strict';
  const U = UBT;
  const page = U.mount('team');
  const MINUS = U.MINUS;

  const num = (v, d = 1) => (Math.round(v * 10 ** d) / 10 ** d).toFixed(d).replace('.', ',');
  const F = ['pts', 'fg2m', 'fg2a', 'fg3m', 'fg3a', 'ftm', 'fta', 'oreb', 'dreb', 'reb', 'ast', 'stl', 'blk', 'to', 'pir'];

  // ── Indicatori (formulele din spec, din câmpurile de echipă) ─────────────
  const IND = [
    { k: 'winp', l: 'Procentul victoriilor', ab: 'Win %', hi: 1, pct: 1, dec: 0, thr: 1, span: .15, info: 'Câte meciuri din 100 câștigă echipa.' },
    { k: 'ortg', l: 'Eficiența atacului', ab: 'ORtg', hi: 1, thr: 1, span: 6, info: 'Câte puncte marchează echipa la 100 de posesii. O posesie = o fază de atac, până ajunge mingea la adversar. Mai mare înseamnă mai bine.' },
    { k: 'drtg', l: 'Eficiența apărării', ab: 'DRtg', hi: 0, thr: 1, span: 6, info: 'Câte puncte primește echipa la 100 de posesii ale adversarilor. Cu cât valoarea e mai mică, cu atât apărarea e mai bună.' },
    { k: 'net', l: 'Bilanțul atac–apărare', ab: 'Net Rating', hi: 1, thr: 1, span: 6, sign: 1, info: 'Eficiența atacului minus eficiența apărării. Peste zero: echipa marchează mai mult decât primește la același număr de posesii.' },
    { k: 'efg', l: 'Eficiența aruncărilor', ab: 'eFG%', hi: 1, pct: 1, dec: 1, thr: .5, span: .06, info: 'Procentul de aruncări reușite, ajustat pentru că un coș de 3 puncte valorează mai mult decât unul de 2.' },
    { k: 'tov', l: 'Procentul posesiilor pierdute', ab: 'TOV%', hi: 0, pct: 1, dec: 1, thr: .5, span: .06, info: 'Din 100 de posesii, câte se termină cu mingea pierdută. Mai mic înseamnă mai bine.' },
  ];
  const TRAD = [
    { cat: 'Puncte', k: 'pf', l: 'Puncte marcate', hi: 1, thr: 1 }, { cat: 'Puncte', k: 'pa', l: 'Puncte primite', hi: 0, thr: 1 },
    { cat: 'Aruncări', k: 'fg2', l: 'Aruncări de 2 reușite', hi: 1, pct: 1, thr: 1 }, { cat: 'Aruncări', k: 'fg3', l: 'Aruncări de 3 reușite', hi: 1, pct: 1, thr: 1 }, { cat: 'Aruncări', k: 'ft', l: 'Aruncări libere reușite', hi: 1, pct: 1, thr: 1 },
    { cat: 'Recuperări', k: 'oreb', l: 'Recuperări ofensive', hi: 1, thr: .3 }, { cat: 'Recuperări', k: 'dreb', l: 'Recuperări defensive', hi: 1, thr: .3 },
    { cat: 'Pase, intercepții, mingi pierdute', k: 'ast', l: 'Pase decisive', hi: 1, thr: .3 }, { cat: 'Pase, intercepții, mingi pierdute', k: 'stl', l: 'Intercepții', hi: 1, thr: .3 },
    { cat: 'Pase, intercepții, mingi pierdute', k: 'to', l: 'Mingi pierdute', hi: 0, thr: .3 }, { cat: 'Pase, intercepții, mingi pierdute', k: 'blk', l: 'Capace', hi: 1, thr: .3 },
    { cat: 'Indicele de performanță', k: 'pir', l: 'Indicele de performanță (PIR)', hi: 1, thr: 1, info: 'O notă pentru tot jocul echipei: adună ce a mers bine (puncte, recuperări, pase, intercepții, capace) și scade ce a mers prost (aruncări ratate, mingi pierdute, faulturi).' },
  ];
  const CATS = ['Puncte', 'Aruncări', 'Recuperări', 'Pase, intercepții, mingi pierdute', 'Indicele de performanță'];
  const SENT = {
    winp: (b, n, r) => `Echipa câștigă ${b ? 'mai des' : 'mai rar'}: ${n} din meciuri, față de ${r}.`,
    ortg: (b, n, r, a) => `Atacul produce cu ${a} puncte ${b ? 'mai mult' : 'mai puțin'} la 100 de posesii.`,
    drtg: (b, n, r, a) => `Echipa primește cu ${a} puncte ${b ? 'mai puțin' : 'mai mult'} la 100 de posesii.`,
    net: (b, n, r, a) => `Bilanțul atac–apărare s-a ${b ? 'îmbunătățit' : 'înrăutățit'} cu ${a} puncte la 100 de posesii.`,
    efg: (b, n, r, a) => `Eficiența aruncărilor a ${b ? 'crescut' : 'scăzut'} cu ${a} puncte procentuale.`,
    tov: (b, n, r) => `Echipa pierde mingea ${b ? 'mai rar' : 'mai des'}: în ${n} din posesii, față de ${r}.`,
    fg2: (b, n, r) => `Aruncările de 2 intră ${b ? 'mai des' : 'mai rar'}: ${n}, față de ${r}.`,
    fg3: (b, n, r) => `Aruncările de 3 intră ${b ? 'mai des' : 'mai rar'}: ${n}, față de ${r}.`,
    ft: (b, n, r) => `Aruncările libere intră ${b ? 'mai des' : 'mai rar'}: ${n}, față de ${r}.`,
    oreb: (b, n, r) => `Echipa ia ${b ? 'mai multe' : 'mai puține'} recuperări ofensive: ${n} pe meci, față de ${r}.`,
    dreb: (b, n, r) => `Echipa ia ${b ? 'mai multe' : 'mai puține'} recuperări defensive: ${n} pe meci, față de ${r}.`,
    ast: (b, n, r) => `Echipa dă ${b ? 'mai multe' : 'mai puține'} pase decisive: ${n} pe meci, față de ${r}.`,
    stl: (b, n, r) => `Echipa face ${b ? 'mai multe' : 'mai puține'} intercepții: ${n} pe meci, față de ${r}.`,
    blk: (b, n, r) => `Echipa dă ${b ? 'mai multe' : 'mai puține'} capace: ${n} pe meci, față de ${r}.`,
  };
  const TITLE = {
    winp: b => b ? 'Mai multe victorii' : 'Mai puține victorii', ortg: b => b ? 'Atac mai eficient' : 'Atac mai puțin eficient',
    drtg: b => b ? 'Apărare mai bună' : 'Apărare mai slabă', net: b => b ? 'Bilanț atac–apărare mai bun' : 'Bilanț atac–apărare mai slab',
    efg: b => b ? 'Aruncări mai eficiente' : 'Aruncări mai puțin eficiente', tov: b => b ? 'Mai puține mingi pierdute' : 'Mai multe mingi pierdute',
    fg2: b => b ? 'Aruncări de 2 mai precise' : 'Aruncări de 2 mai puțin precise', fg3: b => b ? 'Aruncări de 3 mai precise' : 'Aruncări de 3 mai puțin precise',
    ft: b => b ? 'Aruncări libere mai precise' : 'Aruncări libere mai puțin precise',
    oreb: b => b ? 'Mai multe recuperări ofensive' : 'Mai puține recuperări ofensive', dreb: b => b ? 'Mai multe recuperări defensive' : 'Mai puține recuperări defensive',
    ast: b => b ? 'Mai multe pase decisive' : 'Mai puține pase decisive', stl: b => b ? 'Mai multe intercepții' : 'Mai puține intercepții', blk: b => b ? 'Mai multe capace' : 'Mai puține capace',
  };
  const UNIT = m => (m.pct ? 'puncte procentuale' : ['ortg', 'drtg', 'net'].includes(m.k) ? 'puncte la 100 de posesii' : 'pe meci');
  const WITHV = ['ortg', 'drtg', 'net', 'efg'];
  const fmtV = (m, v) => (v == null ? '–' : m.pct ? num(v * 100, m.dec ?? 0) + '%' : m.sign ? (v > 0 ? '+' : v < 0 ? MINUS : '') + num(Math.abs(v)) : num(v));
  const diffOf = (m, a, b) => {
    if (a == null || b == null) return { has: false, d: 0, same: true, better: false };
    const d = (a - b) * (m.pct ? 100 : 1), same = Math.abs(d) < m.thr;
    return { has: true, d, same, better: m.hi ? d > 0 : d < 0 };
  };
  const sgnD = (d, dec) => (d >= 0 ? '+' : MINUS) + num(Math.abs(d), dec);
  const obsItem = (m, x, n, r) => {
    let s = SENT[m.k](x.better, n, r, num(Math.abs(x.d), m.dec ?? 1));
    if (WITHV.includes(m.k)) s = s.replace(/\.$/, ` (${n} față de ${r}).`);
    return { t: TITLE[m.k](x.better), s, d: sgnD(x.d, m.pct && m.dec === 0 ? 0 : 1), u: UNIT(m) };
  };

  // Agregare pe o listă de meciuri cu cifre de echipă (sume pe perioadă, apoi raporturi)
  function agg(rows) {
    const n = rows.length;
    if (!n) return null;
    const S = (p, k) => rows.reduce((t, r) => t + (r[p][k] || 0), 0);
    const Uu = k => S('u', k), O = k => S('o', k);
    const fga = Uu('fg2a') + Uu('fg3a'), fgm = Uu('fg2m') + Uu('fg3m'), ofga = O('fg2a') + O('fg3a');
    const pu = fga + .44 * Uu('fta') - Uu('oreb') + Uu('to'), po = ofga + .44 * O('fta') - O('oreb') + O('to'), poss = (pu + po) / 2;
    const w = rows.filter(r => r.w).length, pr = rows.filter(r => r.u.pir != null);
    return {
      n, w, winp: w / n,
      ortg: poss ? 100 * Uu('pts') / poss : null, drtg: poss ? 100 * O('pts') / poss : null, net: poss ? 100 * (Uu('pts') - O('pts')) / poss : null,
      efg: fga ? (fgm + .5 * Uu('fg3m')) / fga : null, tov: (fga + .44 * Uu('fta') + Uu('to')) ? Uu('to') / (fga + .44 * Uu('fta') + Uu('to')) : null,
      pf: Uu('pts') / n, pa: O('pts') / n,
      fg2: Uu('fg2a') ? Uu('fg2m') / Uu('fg2a') : null, fg3: Uu('fg3a') ? Uu('fg3m') / Uu('fg3a') : null, ft: Uu('fta') ? Uu('ftm') / Uu('fta') : null,
      oreb: Uu('oreb') / n, dreb: Uu('dreb') / n, ast: Uu('ast') / n, stl: Uu('stl') / n, blk: Uu('blk') / n, to: Uu('to') / n,
      pir: pr.length ? pr.reduce((t, r) => t + r.u.pir, 0) / pr.length : null,
    };
  }

  const ST = { games: [], team: [], season: null, comp: 'all', mode: 'same', ref: null, exAll: false, open: { Puncte: true } };

  Promise.all([U.csv(U.URLS.games), U.csv(U.URLS.gameStats)]).then(([g, ts]) => {
    ST.games = U.normGames(g);
    const T = U.normTeamStats(ts);
    // Sincronizare: doar meciurile prezente în ambele fișiere (competiție + sezon + game_code)
    ST.team = ST.games.filter(x => T[x.skey]).map(x => {
      const s = T[x.skey], P = p => Object.fromEntries(F.map(k => [k, s[p + '_' + k] ?? null]));
      return { ck: x.comp, season: x.season, date: x.date, w: x.win, u: P('ubt'), o: P('opp') };
    });
    render();
  });

  U.on('comp', t => { ST.comp = t.dataset.v; render(true); });
  U.onChange('season', v => { ST.season = v; ST.comp = 'all'; ST.ref = null; render(true); });
  U.on('mode', t => { ST.mode = t.dataset.v; render(true); });
  U.onChange('ref', v => { ST.ref = v; render(true); });
  U.on('cat', t => { const c = t.dataset.v; ST.open[c] = !ST.open[c]; render(true); });
  U.on('allcats', () => { const all = CATS.every(c => ST.open[c]); ST.open = Object.fromEntries(CATS.map(c => [c, !all])); render(true); });
  U.on('ex', t => { ST.exAll = t.dataset.v === '1'; render(true); });

  // Grafic linie dintr-un card de indicator
  function spark(m, pts, season) {
    const vv = pts.map(p => p.v).filter(v => v != null);
    if (!vv.length) return '';
    const vmin = Math.min(...vv), vmax = Math.max(...vv), half = Math.max(m.span, (vmax - vmin) / 2), mid = (vmin + vmax) / 2;
    const lo = mid - half, hi = mid + half;
    const Y = v => Math.round((10 + (1 - (v - lo) / (hi - lo)) * 80) * 10) / 10;
    const X = i => Math.round(10 + i * (80 / Math.max(1, pts.length - 1)));
    const P = pts.map((p, i) => ({ ...p, x: X(i), y: p.v == null ? null : Y(p.v) })).filter(p => p.y != null);
    const base = m.k === 'net' ? 0 : m.k === 'winp' ? .5 : null;
    const multi = pts.length > 2;
    const dots = P.map((p, i) => `<span class="sp-dot${p.cur ? ' cur' : ''}${multi && !p.cur ? ' past' : ''}" style="left:${p.x}%;top:${p.y}%"></span>` +
      `<span class="sp-val only-desk${p.cur ? ' cur' : ''}" style="left:${p.x}%;top:${p.y}%">${fmtV(m, p.v)}</span>` +
      `<span class="sp-lab${p.cur ? ' cur' : ''}${i === 0 || p.cur ? '' : ' only-desk'}" style="left:${p.x}%">${p.lab}</span>`).join('');
    return `<div class="sp">${base != null && base > lo && base < hi ? `<div class="sp-base" style="top:${Y(base)}%"></div>` : ''}
      <svg viewBox="0 0 100 100" preserveAspectRatio="none"><polyline points="${multi ? P.map(p => p.x + ',' + p.y).join(' ') : ''}" class="sp-all"></polyline>
      <polyline points="${P.slice(-2).map(p => p.x + ',' + p.y).join(' ')}" class="sp-last" style="stroke:var(--vc)"></polyline></svg>${dots}</div>`;
  }

  const vcls = x => (!x.has || x.same ? 'v-eq' : x.better ? 'v-up' : 'v-dn');

  function render(keepScroll) {
    const y = window.scrollY;
    if (!ST.games.length) { U.paint(page, '<main class="main"><div class="empty">Nu am găsit meciuri.</div></main>'); return; }
    const seasons = [...new Set(ST.games.map(g => g.season))].sort((a, b) => b - a);
    const season = seasons.includes(ST.season) ? ST.season : seasons[0];
    const compKeys = U.ORDER.filter(k => ST.games.some(g => g.season === season && g.comp === k));
    const comp = compKeys.includes(ST.comp) ? ST.comp : 'all';
    const comps = [{ key: 'all', label: 'Toate' }].concat(compKeys.map(k => ({ key: k, label: U.COMP[k] }))).map(c => ({ ...c, on: c.key === comp }));
    const keys = comp === 'all' ? compKeys : [comp];
    const gs = ST.games.filter(g => g.season === season && keys.includes(g.comp));
    const rec = a => { const n = a.length, w = a.filter(g => g.win).length; return { n, w, l: n - w }; };
    const pct = o => (o.n ? Math.round(o.w / o.n * 100) + '%' : '–');
    const wl = (o, cls) => `<span class="wl ${cls || ''}"><span class="w">${o.w}</span><span class="d">–</span><span class="l">${o.l}</span></span>`;
    const prevS = seasons[seasons.indexOf(season) + 1];
    const B = rec(prevS ? ST.games.filter(g => g.season === prevS && keys.includes(g.comp)) : []);
    const summary = (comp === 'all' ? 'Toate competițiile' : U.COMP[comp]) + ' · ' + U.SL(season);

    let h = U.filters({ comps, season, seasons, summary, open: U.filtersOpen });
    h += '<main class="main">';

    // ── 1. Cap (varianta D „Pe competiții”) ────────────────────────────────
    const tot = rec(gs), hm = rec(gs.filter(g => g.home)), aw = rec(gs.filter(g => !g.home));
    const showPC = comp === 'all' && compKeys.length > 1;
    const perComp = showPC ? compKeys.map(k => {
      const a = gs.filter(g => g.comp === k), r = rec(a), ah = rec(a.filter(g => g.home)), aa = rec(a.filter(g => !g.home));
      return `<button type="button" class="tm-pc" data-act="comp" data-v="${k}" title="Arată doar ${U.COMP[k]}"><span class="label">${U.COMP[k]}</span>${wl(r, 'wl-pc')}
        <span class="tm-pc-s">${U.meci(r.n)} · acasă ${ah.w}–${ah.l} · depl. ${aa.w}–${aa.l}</span></button>`;
    }).join('') : '';
    h += `<section class="sec"><div class="sec-head"><h1 class="h1">Echipa U-BT</h1><p class="lead">${U.meci(tot.n)} · ${comp === 'all' ? 'toate competițiile' : U.COMP[comp]}, sezonul ${U.SL(season)}</p></div>
      <div class="tm-cap${showPC ? '' : ' solo'}" style="--pc:${compKeys.length};--capc:minmax(0,1.3fr) minmax(0,${(compKeys.length * .9).toFixed(1)}fr)">
        <div class="tm-dark"><span class="label">Victorii – înfrângeri</span>
          <div class="tm-main">${wl(tot, 'wl-cap')}<span class="tm-pct">${pct(tot)} câștigate</span></div>
          <div class="tm-ha"><span>Acasă <b>${hm.w}–${hm.l}</b></span><span>În deplasare <b>${aw.w}–${aw.l}</b></span></div></div>
        ${showPC ? `<div class="tm-pcs">${perComp}</div>` : ''}
      </div>${B.n ? `<p class="tm-prev">Sezonul trecut, în aceleași competiții: ${B.w}–${B.l} (${pct(B)} din meciuri câștigate).</p>` : ''}</section>`;

    // ── Comparația (un singur selector) ────────────────────────────────────
    const TR = ST.team;
    const byS = (s, ks) => TR.filter(r => r.season === s && ks.includes(r.ck)).sort((a, b) => a.date.localeCompare(b.date));
    const teamKeys = keys.filter(k => TR.some(r => r.season === season && r.ck === k));
    const refOpts = [...new Set(TR.filter(r => r.season < season && teamKeys.includes(r.ck)).map(r => r.season))].sort((a, b) => b - a);
    const ecPrev = [...new Set(TR.filter(r => r.ck === 'eurocup' && r.season < season).map(r => r.season))];
    const multiOk = ecPrev.length >= 2 && TR.some(r => r.ck === 'eurocup' && r.season === season);
    const multiSel = ST.ref === 'multi' && multiOk;
    const mode = multiSel ? 'multi' : ST.mode;
    const ref = refOpts.includes(ST.ref) ? ST.ref : refOpts[0];
    let cur = null, refA = null, series = null, cmpTxt = '', dSub = '', eSub = '';
    const notes = [];
    if (mode === 'multi') {
      const ss = [...new Set(TR.filter(r => r.ck === 'eurocup' && r.season <= season).map(r => r.season))].sort().slice(-5);
      const ag = ss.map(s => ({ s, a: agg(byS(s, ['eurocup'])) }));
      cur = ag[ag.length - 1].a;
      const prev = ag.slice(0, -1);
      refA = {};
      Object.keys(cur).forEach(k => { const v = prev.map(p => p.a[k]).filter(x => x != null); refA[k] = v.length ? v.reduce((t, x) => t + x, 0) / v.length : null; });
      series = ag;
      const p0 = prev[0].s, p1 = prev[prev.length - 1].s;
      cmpTxt = `${U.SL(season)} (${U.meci(cur.n)} în EuroCup), față de media sezoanelor ${U.SL(p0)} – ${U.SL(p1)}.`;
      dSub = `Față de media sezoanelor ${U.SL(p0)} – ${U.SL(p1)}, doar în EuroCup.`;
      eSub = 'Pe sezoane, doar EuroCup; diferența e față de media sezoanelor anterioare.';
      notes.push('Doar EuroCup: singura competiție cu date în toate sezoanele. Sezoanele anterioare sunt luate întregi.');
    } else if (ref) {
      const cmpKeys = teamKeys.filter(k => TR.some(r => r.season === ref && r.ck === k));
      const curR = byS(season, cmpKeys);
      let refR = [];
      if (mode === 'same') cmpKeys.forEach(k => { refR = refR.concat(byS(ref, [k]).slice(0, curR.filter(r => r.ck === k).length)); });
      else refR = byS(ref, cmpKeys);
      cur = agg(curR); refA = agg(refR);
      if (cur && refA) {
        const parts = rs => cmpKeys.map(k => { const n = rs.filter(r => r.ck === k).length; return n ? `${n} ${U.COMP[k]}` : ''; }).filter(Boolean).join(' + ');
        const mc = cmpKeys.length > 1;
        if (mode === 'same') {
          cmpTxt = `Primele ${U.meci(curR.length)} din ${U.SL(season)}${mc ? ` (${parts(curR)})` : ''}, față de primele ${refR.length} din ${U.SL(ref)}${mc && refR.length !== curR.length ? ` (${parts(refR)})` : ''}.`;
          dSub = `Față de primele ${U.meci(refR.length)} din ${U.SL(ref)}.`;
          eSub = `Comparația e cu aceeași perioadă din sezonul ${U.SL(ref)}: primele ${U.meci(refA.n)}.`;
        } else {
          cmpTxt = `Cele ${U.meci(curR.length)} din ${U.SL(season)}, față de tot sezonul ${U.SL(ref)} (${U.meci(refR.length)}).`;
          dSub = `Față de tot sezonul ${U.SL(ref)}.`;
          eSub = `Comparația e cu tot sezonul ${U.SL(ref)} (${U.meci(refA.n)}).`;
          notes.push(`Atenție, perioadele diferă: ${U.meci(curR.length)} de la începutul sezonului, comparate cu un sezon întreg de ${U.meci(refR.length)}. Calendarul și adversarii nu sunt aceiași, așa că diferențele trebuie citite cu grijă.`);
        }
        teamKeys.filter(k => !cmpKeys.includes(k)).forEach(k => notes.push(ST.games.some(g => g.season === ref && g.comp === k)
          ? `Fără ${U.COMP[k]}: în ${U.SL(ref)} lipsesc cifrele de joc (avem doar scorurile).`
          : `Fără ${U.COMP[k]}: nu s-a jucat în ${U.SL(ref)}.`));
        cmpKeys.forEach(k => {
          const all = gs.filter(g => g.comp === k).length, withTs = curR.filter(r => r.ck === k).length;
          if (withTs < all) notes.push(`${U.COMP[k]}: avem cifrele de joc doar pentru ${withTs} din ${U.meci(all)}; restul nu intră în comparație.`);
        });
      }
    }
    if (mode !== 'multi') keys.filter(k => !teamKeys.includes(k)).forEach(k => notes.push(`Fără ${U.COMP[k]}: lipsesc cifrele de joc (avem doar scorurile).`));
    const ok = !!(cur && refA);
    const shRef = mode === 'multi' ? 'media' : ref ? U.SLs(ref) : '';

    // ── 2. Evoluția performanței ───────────────────────────────────────────
    const modes = [['same', 'Același număr de meciuri'], ['full', 'Tot sezonul']];
    const refSel = refOpts.length || multiOk ? `<label class="cmp-ref"><span class="label">față de</span><select class="fsel" data-change="ref" aria-label="Sezonul de comparație">
        ${refOpts.map(s => `<option value="${s}"${!multiSel && s === ref ? ' selected' : ''}>${U.SL(s)}</option>`).join('')}
        ${multiOk ? `<option value="multi"${multiSel ? ' selected' : ''}>Toate sezoanele anterioare (doar EuroCup)</option>` : ''}</select></label>` : '';
    h += `<section class="sec">${U.secHead('Evoluția performanței', 'Șase cifre care arată cât de bine joacă echipa, dincolo de scor. Apasă pe „i” pentru explicații.')}
      <div class="cmp-box"><div class="cmp-ctrls"><span class="label only-desk">Compară</span>
        ${multiSel ? '' : `<div class="seg seg-acc" role="group" aria-label="Tipul comparației">${modes.map(([k, l]) => `<button type="button" class="${mode === k ? 'on' : ''}" aria-pressed="${mode === k}" data-act="mode" data-v="${k}">${l}</button>`).join('')}</div>`}
        ${refSel}</div>
        ${ok ? `<p class="cmp-txt">${cmpTxt}</p>` : ''}${notes.map(n => `<p class="note">${U.esc(n)}</p>`).join('')}</div>`;
    if (!ok) {
      h += `<div class="cmp-none">Nu avem un sezon anterior cu cifre de joc pentru această selecție.</div></section>`;
    } else {
      h += `<div class="ind-grid">${IND.map(m => {
        const a = cur[m.k], b = refA[m.k], x = diffOf(m, a, b), dec = m.dec ?? 1;
        const pts = series ? series.map(s => ({ v: s.a[m.k], lab: U.SLs(s.s), cur: s.s === season })) : [{ v: b, lab: shRef, cur: false }, { v: a, lab: U.SLs(season), cur: true }];
        return `<div class="ind ${vcls(x)}"><div class="ind-top"><span class="ind-t"><b>${m.l}</b><span>${m.ab}${m.hi ? '' : ' · mai mic = mai bine'}</span></span>${U.info(m.l, m.info, '', 'r')}</div>
          <div class="ind-now"><span class="ind-v">${fmtV(m, a)}</span><span class="ind-ref">față de ${fmtV(m, b)}<span class="only-desk"> ${series ? '(media sezoanelor anterioare)' : 'în ' + shRef}</span></span></div>
          <div class="ind-vd"><span class="dot"></span><span>${!x.has ? '–' : x.same ? 'Stagnare' : x.better ? 'Progres' : 'Regres'}</span><span>${x.has ? sgnD(x.d, dec) + (m.pct ? ' puncte' : '') : ''}</span></div>
          ${spark(m, pts, season)}</div>`;
      }).join('')}</div></section>`;

      // ── 3. Ce s-a schimbat ────────────────────────────────────────────────
      const up = [], dn = [], stag = [];
      IND.forEach(m => {
        const x = diffOf(m, cur[m.k], refA[m.k]);
        if (!x.has) return;
        if (x.same) { stag.push(m.l.toLowerCase()); return; }
        (x.better ? up : dn).push(obsItem(m, x, fmtV(m, cur[m.k]), fmtV(m, refA[m.k])));
      });
      TRAD.filter(m => SENT[m.k]).map(m => { const a = cur[m.k], b = refA[m.k], x = diffOf(m, a, b); return { m, x, a, b, rel: x.has ? Math.abs(m.pct ? x.d / (b * 100) : x.d / (b || 1)) : 0 }; })
        .filter(o => o.x.has && !o.x.same).sort((p, q) => q.rel - p.rel)
        .forEach(o => { const L = o.x.better ? up : dn; if (L.length >= 5) return; L.push(obsItem({ ...o.m, dec: 1 }, o.x, fmtV(o.m, o.a), fmtV(o.m, o.b))); });
      const obsCol = (t, cls, items, empty) => `<div class="obs-col"><div class="obs-h"><span class="dot ${cls}"></span><h3 class="h3">${t}</h3></div>
        ${items.length ? items.map(it => `<div class="obs-it"><span class="obs-d"><b class="${cls === 'up' ? 'c-win' : 'c-loss'}">${it.d}</b><span>${it.u}</span></span><span class="obs-tx"><b>${it.t}</b><span>${it.s}</span></span></div>`).join('') : `<div class="obs-empty">${empty}</div>`}</div>`;
      h += `<section class="sec">${U.secHead('Ce s-a schimbat', dSub)}
        ${stag.length ? `<p class="obs-stag"><b>Fără schimbări notabile:</b> ${stag.join(', ')}.</p>` : ''}
        <div class="obs">${obsCol('În progres', 'up', up, 'Nicio cifră nu arată un progres clar.')}${obsCol('În regres', 'dn', dn, 'Nicio cifră nu arată un regres clar.')}</div>
        ${cur.n < 10 ? `<p class="note">Doar ${U.meci(cur.n)} în sezonul ales: un singur rezultat poate schimba vizibil aceste cifre.</p>` : ''}</section>`;

      // ── 4. Statisticile echipei ───────────────────────────────────────────
      const allOpen = CATS.every(c => ST.open[c]);
      const hdr = series
        ? series.map(s => { const c = s.s === season; return { top: c ? 'Sezonul ales' : '', big: U.SLs(s.s), bigM: U.SLs(s.s), sub: U.meci(s.a.n), cur: c }; })
        : [{ top: 'Sezonul ales', big: U.SL(season), bigM: U.SLs(season), sub: U.meci(cur.n), cur: true },
          { top: 'Comparat cu', big: U.SL(ref), bigM: U.SLs(ref), sub: mode === 'same' ? `primele ${refA.n} meciuri` : 'tot sezonul', cur: false }];
      const cols = series ? `minmax(0,1fr) repeat(${hdr.length},104px) 130px` : '300px 160px 160px 130px minmax(0,1fr)';
      const colsM = series ? `120px repeat(${hdr.length},52px) 60px` : 'minmax(0,1fr) 72px 72px 60px';
      const minW = series ? (120 + hdr.length * 58 + 66) + 'px' : '0';
      const head = `<div class="st-row st-head"><span></span>${hdr.map(x => `<span class="st-hc${x.cur ? ' cur' : ''}"><span class="st-top only-desk">${x.top}</span><b class="only-desk">${x.big}</b><b class="only-mob">${x.bigM}</b><span>${x.sub}</span></span>`).join('')}
        <span class="st-dh"><span class="only-desk">Diferență</span><span class="only-mob">Dif.</span>${series ? '<span class="only-desk st-dsub">față de medie</span>' : ''}</span>${series ? '' : '<span class="only-desk"></span>'}</div>`;
      const cats = CATS.map(c => {
        const rows = TRAD.filter(m => m.cat === c).map(m => {
          const a = cur[m.k], b = refA[m.k], x = diffOf(m, a, b), dec = m.pct ? 0 : 1, mm = { ...m, dec };
          const vals = series ? series.map(s => ({ v: fmtV(mm, s.a[m.k]), cur: s.s === season })) : [{ v: fmtV(mm, a), cur: true }, { v: fmtV(mm, b), cur: false }];
          const mx = Math.max(a || 0, b || 0) || 1;
          return { x, html: `<div class="st-row ${vcls(x)}"><span class="st-l">${m.l}${m.info ? U.info(m.l, m.info, '', 'l') : ''}</span>
            ${vals.map(v => `<span class="st-v${v.cur ? ' cur' : ''}">${v.v}</span>`).join('')}
            <span class="st-d"><b>${x.has ? sgnD(x.d, dec) : '–'}</b><span>${!x.has ? '' : x.same ? 'la fel' : x.better ? 'mai bine' : 'mai slab'}</span></span>
            ${series ? '' : `<span class="st-bars only-desk"><span style="width:${(a || 0) / mx * 100}%"></span><span style="width:${(b || 0) / mx * 100}%"></span></span>`}</div>` };
        });
        const nb = rows.filter(r => r.x.has && !r.x.same && r.x.better).length, nw = rows.filter(r => r.x.has && !r.x.same && !r.x.better).length, ns = rows.length - nb - nw;
        const cf = n => `${n} ${n === 1 ? 'cifră' : 'cifre'}`;
        const chips = [nb ? `<span><i class="dot up"></i>${cf(nb)} în progres</span>` : '', nw ? `<span><i class="dot dn"></i>${cf(nw)} în regres</span>` : '', ns ? `<span><i class="dot"></i>${cf(ns)} la fel</span>` : ''].join('');
        const open = !!ST.open[c];
        return `<div class="st-cat"><button type="button" class="st-cat-b" data-act="cat" data-v="${c}" aria-expanded="${open}"><span class="st-cat-t"><span class="st-cat-n">${c}</span><span class="st-chips">${chips}</span></span><span class="st-chev">${open ? '−' : '+'}</span></button>
          ${open ? `<div class="st-wrap"><div class="st-tbl" style="--cols:${cols};--colsm:${colsM};--minw:${minW}">${head}${rows.map(r => r.html).join('')}</div></div>` : ''}</div>`;
      }).join('');
      h += `<section class="sec"><div class="sec-row">${U.secHead('Statisticile echipei', 'Media pe meci. ' + eSub)}
          <button type="button" class="btn-out only-desk" data-act="allcats">${allOpen ? 'Închide toate' : 'Deschide toate'}</button></div>
        <div class="st-cats">${cats}</div></section>`;
    }

    // ── 5. Cele mai bune și cele mai slabe meciuri ─────────────────────────
    const exG = ST.exAll ? ST.games.filter(g => comp === 'all' || g.comp === comp) : gs;
    const firstS = [...new Set(ST.games.map(g => g.season))].sort()[0];
    const card = g => `<a href="${U.gameHref(g.code, g.season, 'team')}" class="ex-row"><span class="ex-mid"><span class="ex-sc">U-BT ${g.us} – ${g.them} ${U.esc(g.opp)}</span>
      <span class="ex-meta">${U.dLong(g.date)} · ${U.COMP[g.comp]} · ${g.home ? 'acasă' : 'deplasare'}${ST.exAll ? ' · ' + U.SL(g.season) : ''}</span></span>
      <span class="ex-d ${g.win ? 'c-win' : 'c-loss'}">${(g.us - g.them > 0 ? '+' : MINUS) + Math.abs(g.us - g.them)}</span></a>`;
    const wins = exG.filter(g => g.win).sort((a, b) => (b.us - b.them) - (a.us - a.them)).slice(0, 3);
    const losses = exG.filter(g => !g.win).sort((a, b) => (a.us - a.them) - (b.us - b.them)).slice(0, 3);
    const xcol = (t, list, empty) => `<div class="ex-col"><div class="label">${t}</div><div class="ex-list">${list.length ? list.map(card).join('') : `<div class="ex-empty">${empty}</div>`}</div></div>`;
    h += `<section class="sec"><div class="ex-head"><h2 class="h2">Cele mai bune și cele mai slabe meciuri</h2>
        <div class="seg" role="group" aria-label="Perioada"><button type="button" class="${ST.exAll ? '' : 'on'}" aria-pressed="${!ST.exAll}" data-act="ex" data-v="0">Sezonul ales</button><button type="button" class="${ST.exAll ? 'on' : ''}" aria-pressed="${ST.exAll}" data-act="ex" data-v="1">Toate sezoanele (din ${firstS})</button></div></div>
      <div class="ex-grid">${xcol('Cele mai mari victorii', wins, 'Încă nicio victorie.')}${xcol('Cele mai grele înfrângeri', losses, 'Nicio înfrângere.')}</div></section>`;

    h += U.endCards({ href: 'index.html', title: 'Start', sub: 'Ultimul meci și toate rezultatele sezonului', dir: 'prev' }, { href: 'players.html', title: 'Jucătorii', sub: 'Clasamentul lotului pe cifre', dir: 'next' });
    h += '</main>';
    U.paint(page, h);
    if (keepScroll) window.scrollTo(0, y);
  }
})();
