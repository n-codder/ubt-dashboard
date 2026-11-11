/* Profil jucător — cap, cifre pe meci, aruncări, loc în echipă, evoluție, meciuri, competiții, istoric */
(function () {
  'use strict';
  const U = UBT;
  const page = U.mount(null);
  const back = U.backTarget('players');
  const NAT = { Lithuania: 'Lituania', Romania: 'România', 'United States': 'SUA', USA: 'SUA', Serbia: 'Serbia', Croatia: 'Croația', Montenegro: 'Muntenegru', Estonia: 'Estonia', 'Dominican Republic': 'Rep. Dominicană', Ghana: 'Ghana', Slovenia: 'Slovenia', 'Bosnia and Herzegovina': 'Bosnia', Greece: 'Grecia', France: 'Franța', Spain: 'Spania', Italy: 'Italia', Germany: 'Germania', Canada: 'Canada', Israel: 'Israel', Latvia: 'Letonia', Ukraine: 'Ucraina' };
  const F = ['games_played', 'minutes', 'points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'fg2_made', 'fg2_att', 'fg3_made', 'fg3_att', 'ft_made', 'ft_att', 'pir'];
  const agg = rows => { const o = {}; F.forEach(k => { o[k] = rows.reduce((t, r) => t + (+r[k] || 0), 0); }); return o; };
  const ST = { rows: [], games: {}, pg: [], bio: {}, det: {}, pid: new URLSearchParams(location.search).get('id'), season: null, comp: 'all' };
  let BARS = [];

  Promise.all([U.csv(U.URLS.players), U.csv(U.URLS.games), U.csv(U.URLS.perGame), U.csv(U.URLS.details), U.json(U.URLS.bio)]).then(([st, gt, pt, dt, bj]) => {
    ST.rows = st.map(r => ({ ...r, season: String(r.season), player_id: String(r.player_id) }));
    U.normGames(gt).forEach(g => { ST.games[g.key] = g; });
    ST.pg = pt.map(r => ({ ...r, season: String(r.season), player_id: String(r.player_id) }));
    dt.forEach(o => { ST.det[o.player_id] = o; });
    ST.bio = (bj && bj.players) || {};
    render();
  });

  U.on('comp', t => { ST.comp = t.dataset.v; render(true); });
  U.onChange('season', v => { ST.season = v; ST.comp = 'all'; render(true); });
  U.on('crow', t => t.closest('.acc-row').classList.toggle('open'));

  function render(keepScroll) {
    const y = window.scrollY;
    if (!ST.rows.length) { U.paint(page, U.backBar(back) + '<main class="main"><div class="empty">Nu am găsit date.</div></main>'); return; }
    let P = ST.rows.filter(r => r.player_id === ST.pid);
    if (!P.length) {   // pagină deschisă fără id valid → cel mai bun marcator din ultimul sezon
      const ls = ST.rows.reduce((m, r) => (r.season > m ? r.season : m), '0'), tot = {};
      ST.rows.filter(r => r.season === ls).forEach(r => { const t = tot[r.player_id] || (tot[r.player_id] = { p: 0, g: 0 }); t.p += +r.points; t.g += +r.games_played; });
      const best = Object.entries(tot).filter(([, t]) => t.g >= 3).sort((a, b) => b[1].p / b[1].g - a[1].p / a[1].g)[0];
      ST.pid = best ? best[0] : ST.rows[0].player_id;
      P = ST.rows.filter(r => r.player_id === ST.pid);
    }
    const pid = ST.pid;
    const seasons = [...new Set(P.map(r => r.season))].sort((a, b) => b - a);
    const season = seasons.includes(ST.season) ? ST.season : seasons[0];
    const sRows = P.filter(r => r.season === season && +r.games_played > 0).sort((a, b) => U.ORDER.indexOf(a.competition_key) - U.ORDER.indexOf(b.competition_key));
    const compKeys = sRows.map(r => r.competition_key);
    const comp = compKeys.includes(ST.comp) ? ST.comp : 'all';
    const inC = k => comp === 'all' || k === comp;
    const A = agg(sRows.filter(r => inC(r.competition_key))), gp = A.games_played || 0;
    const from = `player:${pid}`;

    // ── Cap (1a) ────────────────────────────────────────────────────────────
    const name = U.cleanName(P[0].player_name);
    document.title = `${name} · U-BT în cifre`;
    const bio = ST.bio[pid] || {}, det = ST.det[pid] || {};
    const img = P.map(r => r.image_url).find(Boolean);
    const now = new Date(); let age = null;
    if (bio.birth_date) { const b = new Date(bio.birth_date); age = now.getFullYear() - b.getFullYear() - ((now.getMonth() < b.getMonth() || (now.getMonth() === b.getMonth() && now.getDate() < b.getDate())) ? 1 : 0); }
    else if (det.birth_year) age = now.getFullYear() - +det.birth_year;
    const posLong = U.POS(bio.position || P.map(r => r.position).find(Boolean));
    const nat = bio.nationality || (det.nationality_name ? NAT[det.nationality_name] || det.nationality_name : '');
    const jersey = bio.jersey != null ? bio.jersey : (ST.pg.find(r => r.player_id === pid && r.jersey) || {}).jersey;
    const facts = [['Număr', jersey != null && jersey !== '' ? '#' + jersey : '–'], ['Înălțime', bio.height_cm ? bio.height_cm + ' cm' : '–'], ['Vârstă', age ? age + ' ani' : '–'], [bio.nationality ? 'Naționalitate' : 'Țara', nat || '–']]
      .concat(seasons.length > 1 ? [['La U-BT', seasons.length + ' sezoane']] : []);
    const allGp = sRows.reduce((t, r) => t + +r.games_played, 0);
    const gpTxt = `${U.meci(allGp)} pentru U-BT în sezonul ${U.SL(season)}` + (sRows.length > 1 ? ' · ' + sRows.map(r => `${r.games_played} în ${U.COMP[r.competition_key]}`).join(' · ') : '');

    let h = U.backBar(back);
    h += `<div class="ph-hero">${U.photo(img, name, 'ph-hero-img')}
      <div class="ph-hero-info"><div class="ph-hero-nm"><div class="ph-hero-pos">${U.esc(posLong)}</div><h1 class="ph-hero-h1">${U.esc(name)}</h1></div>
        <div class="ph-facts" style="--nf:${facts.length}">${facts.map(([l, v]) => `<div class="ph-fact"><span class="label">${l}</span><b>${U.esc(v)}</b></div>`).join('')}</div>
        <p class="ph-gp">${U.esc(gpTxt)}</p></div></div>`;

    // ── Filtre (în conținut) ────────────────────────────────────────────────
    const comps = (compKeys.length > 1 ? [{ key: 'all', label: 'Toate', on: comp === 'all' }] : [])
      .concat(compKeys.map(k => ({ key: k, label: U.COMP[k] || k, on: k === comp || compKeys.length === 1 })));
    const ctxComp = comp === 'all' ? (compKeys.length > 1 ? 'toate competițiile' : U.COMP[compKeys[0]] || '') : U.COMP[comp];
    const summary = (comp === 'all' ? (compKeys.length > 1 ? 'Toate competițiile' : U.COMP[compKeys[0]] || '') : U.COMP[comp]) + ' · ' + U.SL(season);
    h += '<main class="main">';
    h += U.filters({ comps, season, seasons, summary, inline: true, open: U.filtersOpen });

    // meciurile jucate (detaliate)
    const G = ST.pg.filter(r => r.player_id === pid && r.season === season && inC(r.competition_key) && +r.seconds_played > 0).map(r => {
      const g = ST.games[`${r.competition_key}|${r.season}|${r.game_code}`] || {};
      return { date: g.date || String(r.date || '').slice(0, 10), comp: r.competition_key, opp: g.opp || r.opponent, win: (g.win != null ? g.win : r.result === 'W'), us: g.us, them: g.them, code: r.game_code, season: r.season,
        v: { pts: +r.points, reb: +r.rebounds, ast: +r.assists, pir: Math.round(+r.pir), min: +r.seconds_played } };
    }).sort((a, b) => a.date.localeCompare(b.date));
    const n = G.length;

    if (!gp) {
      h += '<div class="empty t-c">Jucătorul nu are meciuri pentru combinația de filtre aleasă.</div>';
    } else {
      h += avgSection(A, gp, ctxComp, season);
      h += shotsAndRanks(A, gp, season, inC);
      if (n) h += evolution(G, from) + gamesList(G, gp, from);
    }
    if (sRows.length > 1) h += compSection(sRows, season);
    if (seasons.length > 1) h += history(P, seasons, pid);
    h += U.endCards({ href: 'players.html', title: 'Toți jucătorii', sub: 'Lotul complet, cu statistici', dir: 'prev' }, { href: 'team.html', title: 'Echipa', sub: 'Cifrele U-BT pe sezon', dir: 'next' });
    h += '</main>';
    U.paint(page, h);
    if (keepScroll) window.scrollTo(0, y);
  }

  // ── 1. Cifrele pe meci ────────────────────────────────────────────────────
  function avgSection(A, gp, ctxComp, season) {
    const d = x => x / gp;
    const cards = [['Puncte', A.points, 'puncte'], ['Pase decisive', A.assists, 'pase decisive'], ['Recuperări', A.rebounds, 'recuperări'], ['Eficiență', A.pir, null], ['Minute', A.minutes, 'minute']];
    return `<section class="sec">${U.secHead('Cifrele pe meci', `Media pe meci · ${U.esc(ctxComp)}, sezonul ${U.SL(season)} · ${U.meci(gp)}`)}
      <div class="avg5">${cards.map(([l, t, u]) => `<div class="tile"><div class="tile-l"><span class="label">${l}</span>${l === 'Eficiență' ? U.infoK('pir', 'l') : ''}</div>
        <div class="avg-v">${l === 'Minute' ? U.mmss(d(t) * 60) : U.num(d(t)).replace('-', U.MINUS)}</div><div class="tile-s">${U.neg(Math.round(t))} ${u ? u + ' ' : ''}în total</div></div>`).join('')}</div></section>`;
  }

  // ── 2. Aruncări | Față de restul echipei ──────────────────────────────────
  function shotsAndRanks(A, gp, season, inC) {
    const shots = [['Aruncări de 2 puncte', A.fg2_made, A.fg2_att], ['Aruncări de 3 puncte', A.fg3_made, A.fg3_att], ['Aruncări libere', A.ft_made, A.ft_att, 'ft']].map(([l, m, a, inf]) => `
      <div class="sh-row"><div class="tile-l"><span class="sh-l">${l}</span>${inf ? U.infoK(inf, 'l') : ''}</div><span class="sh-p">${U.pctR(m, a)}</span>
        <div class="sh-bar"><div class="bar-track"><div class="bar-fill" style="width:${a ? m / a * 100 : 0}%"></div></div><span>${m} din ${a}</span></div></div>`).join('');
    const tm = {};
    ST.rows.filter(r => r.season === season && inC(r.competition_key) && +r.games_played > 0).forEach(r => { (tm[r.player_id] = tm[r.player_id] || []).push(r); });
    const minG = gp >= 3 ? 3 : 1;
    const TA = Object.entries(tm).map(([id, rs]) => { const a = agg(rs); return { id, gp: a.games_played, a }; }).filter(x => x.gp >= minG);
    const RK = [['Puncte', 'points'], ['Pase decisive', 'assists'], ['Recuperări', 'rebounds'], ['Eficiență', 'pir'], ['Minute', 'minutes']];
    const ranks = RK.map(([l, k]) => {
      const vals = TA.map(x => x.a[k] / x.gp).sort((a, b) => b - a);
      const me = A[k] / gp, rank = 1 + vals.filter(v => v > me + 1e-9).length, mx = Math.max(...vals, 1e-9);
      return `<div class="rk-row"><span class="rk-l">${l} / meci</span><span class="rk-v">${k === 'minutes' ? U.mmss(me * 60) : U.num(me).replace('-', U.MINUS)}</span>
        <span class="rk-r"><b>Locul ${rank}</b><span>din ${vals.length} jucători</span></span>
        <div class="bar-track rk-bar"><div class="bar-fill" style="width:${Math.max(0, me) / mx * 100}%"></div></div></div>`;
    }).join('');
    return `<div class="sr-grid">
      <section class="sec sr-col">${U.secHead('Aruncări', 'Procentaj reușite')}<div class="box sh-box">${shots}</div></section>
      <section class="sec sr-col">${U.secHead('Față de restul echipei', 'Locul în lotul U-BT, după media pe meci')}<div class="box rk-box">${ranks}</div></section></div>`;
  }

  // ── 3. Evoluția pe meciuri (doar puncte) ──────────────────────────────────
  function evolution(G, from) {
    const vs = G.map(x => x.v.pts), top = Math.max(10, ...vs), bot = Math.min(0, ...vs), H = 120, zero = top / (top - bot) * H, n = G.length;
    BARS = G.map(x => ({ ...x, href: U.gameHref(x.code, x.season, from) }));
    const bars = G.map((x, i) => {
      const v = x.v.pts, hh = Math.max(v === 0 ? 2 : 4, Math.abs(v) / (top - bot) * H);
      const dn = U.dNum(x.date), slots = Math.max(n, 20), nearEnd = slots - 1 - i < 7;   // tooltip-ul (300px) încape la dreapta?
      return `<a href="${U.gameHref(x.code, x.season, from)}" class="pbar" data-i="${i}"${i === n - 1 ? ' data-last' : ''} aria-label="${U.esc(`${x.opp}, ${U.dShort(x.date)}: ${v}`)}">
        <div class="pbar-area">${tipHtml(x, nearEnd)}<div class="pbar-b" style="top:${v >= 0 ? zero - hh : zero}px;height:${hh}px"></div>
          <div class="pbar-lab" style="top:${v >= 0 ? zero - hh - 18 : zero + hh + 2}px">${v < 0 ? U.MINUS + Math.abs(v) : v === 0 ? '' : v}</div></div>
        <div class="xlab">${dn.d}<small>${dn.y}</small></div></a>`;
    }).join('');
    const fut = Array.from({ length: Math.max(0, 20 - n) }, () => '<div class="pbar-fut" aria-hidden="true"><div></div></div>').join('');
    return `<section class="sec">${U.secHead('Evoluția pe meciuri', '<span class="only-desk">Punctele din fiecare meci · locurile punctate sunt meciurile care urmează</span><span class="only-mob">Punctele din fiecare meci · atinge o bară pentru detalii</span>')}
      <div class="box pchart"><div class="chart-scroll pchart-scroll noscroll-m" data-scroll-end><div class="chart-track" data-ptrack><div class="chart-zero" style="top:${zero}px"></div>${bars}${fut}</div></div>
        <p class="hint only-desk pchart-foot">Treci cu mouse-ul peste o bară ca să vezi meciul · click deschide pagina meciului</p>
        <div class="only-mob" data-psel></div></div></section>`;
  }
  function allStats(x) {
    return [['pts', 'puncte'], ['ast', 'pase'], ['reb', 'recup.'], ['pir', 'efic.'], ['min', 'minute']].map(([k, l]) => `<div><b>${k === 'min' ? U.mmss(x.v.min) : U.neg(x.v[k])}</b><span>${l}</span></div>`).join('');
  }
  function tipHtml(x, nearEnd) {
    return `<div class="tip ptip ${nearEnd ? 'ptip-r' : 'ptip-l'}"><div class="tip-h">${U.resBadge(x.win)}<span class="tip-opp">${U.esc(x.opp)}</span><b class="ptip-sc">${x.us}–${x.them}</b></div>
      <div class="ptip-all">${allStats(x)}</div><div class="tip-meta">${U.dLong(x.date)} · ${U.COMP[x.comp]}</div></div>`;
  }
  function setOn(track, i) {
    track.querySelectorAll('.pbar.on').forEach(b => b.classList.remove('on'));
    if (i != null) track.querySelector(`.pbar[data-i="${i}"]`).classList.add('on');
  }
  page.addEventListener('mouseover', e => { if (U.isMobile()) return; const b = e.target.closest('.pbar'); if (b) setOn(b.parentElement, b.dataset.i); });
  page.addEventListener('mouseout', e => { if (U.isMobile()) return; const b = e.target.closest('.pbar'); if (b && !b.contains(e.relatedTarget)) setOn(b.parentElement, null); });
  page.addEventListener('click', e => {
    const b = e.target.closest('.pbar'); if (!b || !U.isMobile()) return;
    e.preventDefault();
    const was = b.classList.contains('on'), i = +b.dataset.i, box = page.querySelector('[data-psel]');
    setOn(b.parentElement, was ? null : i);
    if (was) { box.innerHTML = ''; return; }
    const x = BARS[i];
    box.innerHTML = `<a href="${x.href}" class="selcard psel"><span class="tip-h">${U.resBadge(x.win)}<span class="tip-opp">${U.esc(x.opp)}</span><b class="ptip-sc">${x.us}–${x.them}</b></span>
      <span class="ptip-all">${allStats(x)}</span><span class="tip-meta">${U.dLong(x.date)} · ${U.COMP[x.comp]}</span><span class="psel-go">Pagina meciului →</span></a>`;
  });

  // ── 4. Meciurile sezonului ────────────────────────────────────────────────
  function gamesList(G, gp, from) {
    const n = G.length;
    const best = {}; ['pts', 'ast', 'reb', 'pir', 'min'].forEach(k => { best[k] = Math.max(...G.map(x => x.v[k])); });
    const bestG = n > 1 ? [...G].sort((a, b) => b.v.pts - a.v.pts || b.v.pir - a.v.pir)[0] : null;
    const R = [...G].reverse();
    const rows = R.map(x => `<a href="${U.gameHref(x.code, x.season, from)}" class="tbl-row gl-row">
        <span class="gl-d">${U.dLong(x.date)}</span><span class="gl-c">${U.COMP[x.comp]}</span>
        <span class="gl-o"><span>${U.esc(x.opp)}</span>${x === bestG ? '<span class="tag-best">Cel mai bun meci</span>' : ''}</span>
        <span class="gl-r">${U.resBadge(x.win)}<b>${x.us}–${x.them}</b></span>
        ${['pts', 'ast', 'reb', 'pir', 'min'].map(k => { const b = k !== 'min' && x.v[k] === best[k] && best[k] > 0; return `<span class="t-n${b ? ' b' : x.v[k] < 0 ? ' m' : ''}">${k === 'min' ? U.mmss(x.v.min) : U.neg(x.v[k])}</span>`; }).join('')}</a>`).join('');
    const mrows = R.map(x => `<a href="${U.gameHref(x.code, x.season, from)}" class="gm-row"><span class="gm-mid">
        <span class="gm-top">${U.resBadge(x.win, 'sm')}<span class="gm-opp">${U.esc(x.opp)}</span><span class="gm-sc">${x.us}–${x.them}</span></span>
        <span class="gm-meta">${U.dLong(x.date)} · ${U.COMP[x.comp]} · ${x.v.ast} ${x.v.ast === 1 ? 'pasă' : 'pase'} · ${x.v.reb} recup. · ${U.mmss(x.v.min)} min</span>
        ${x === bestG ? '<span class="tag-best gm-best">Cel mai bun meci</span>' : ''}</span>
        <span class="bigstat"><span class="bigstat-v">${U.neg(x.v.pts)}</span><span class="bigstat-l">puncte</span></span></a>`).join('');
    const cnt = (n < gp ? `${n} din ${gp} meciuri au statistici detaliate` : U.meci(n)) + ', cel mai recent primul';
    return `<section class="sec">${U.secHead('Meciurile sezonului', `<span class="only-desk">${cnt} · click pe un rând deschide pagina meciului</span><span class="only-mob">${cnt}</span>`)}
      <div class="tbl only-desk"><div class="tbl-row tbl-head gl-row"><span>Data</span><span>Competiție</span><span>Adversar</span><span>Rezultat</span><span class="t-r">Puncte</span><span class="t-r">Pase</span><span class="t-r">Recup.</span><span class="t-r">Efic.</span><span class="t-r">Minute</span></div>${rows}</div>
      <p class="note only-desk">Cu îngroșat: cel mai bun meci al lui în sezon la acea cifră.</p>
      <div class="gm-list only-mob">${mrows}</div></section>`;
  }

  // ── 5. Pe competiții ──────────────────────────────────────────────────────
  function compSection(sRows, season) {
    const row = (l, rs, tot) => { const a = agg(rs), g = a.games_played || 1; return { l, tot, cells: [a.games_played, U.num(a.points / g), U.num(a.assists / g), U.num(a.rebounds / g), U.num(a.pir / g), U.mmss(a.minutes / g * 60), U.pctR(a.fg2_made, a.fg2_att), U.pctR(a.fg3_made, a.fg3_att), U.pctR(a.ft_made, a.ft_att)] }; };
    const R = sRows.map(r => row(U.COMP[r.competition_key], [r])).concat([row('Tot sezonul', sRows, true)]);
    const cL = ['Puncte', 'Pase', 'Recup.', 'Efic.', 'Minute', '2 puncte', '3 puncte', 'Libere'];
    const desk = R.map(r => `<div class="tbl-row cp-row${r.tot ? ' total' : ''}"><span class="cp-l${r.tot ? ' b' : ''}">${r.l}</span>${r.cells.map(v => `<span class="t-n${r.tot ? ' b' : ''}">${v}</span>`).join('')}</div>`).join('');
    const mob = R.map(r => `<div class="acc-row"><button type="button" class="acc-btn cm-btn" data-act="crow">
        <span class="cm-mid"><span class="cm-l${r.tot ? ' b' : ''}">${r.l}</span><span class="cm-gp">${U.meci(+r.cells[0])}</span></span>
        <span class="bigstat"><span class="bigstat-v">${r.cells[1]}</span><span class="bigstat-l">puncte / meci</span></span><span class="acc-chev"></span></button>
        <div class="acc-body cm-body"><div class="cells4">${r.cells.slice(2).map((v, i) => `<div class="cell"><span class="cell-v best">${v}</span><span class="cell-l">${cL[i + 1]}</span></div>`).join('')}</div></div></div>`).join('');
    return `<section class="sec">${U.secHead('Pe competiții', `<span class="only-desk">Media pe meci în fiecare competiție, sezonul ${U.SL(season)}</span><span class="only-mob">Sezonul ${U.SL(season)} · atinge o competiție pentru toate cifrele</span>`)}
      <div class="tbl only-desk"><div class="tbl-row tbl-head cp-row"><span>Competiție</span><span class="t-r">Meciuri</span><span class="t-r">Puncte</span><span class="t-r">Pase</span><span class="t-r">Recup.</span><span class="t-r">Efic.</span><span class="t-r">Minute</span><span class="t-r">2 puncte</span><span class="t-r">3 puncte</span><span class="t-r">Libere</span></div>${desk}</div>
      <div class="mp-list only-mob">${mob}</div></section>`;
  }

  // ── 6. Istoricul la U-BT ──────────────────────────────────────────────────
  function history(P, seasons, pid) {
    const allR = P.filter(r => +r.games_played > 0), AT = agg(allR), atg = AT.games_played || 1;
    const hs = [...seasons].sort();
    const sv = hs.map(s => { const a = agg(allR.filter(r => r.season === s)), g = a.games_played || 1; return { s, a, pg: { pts: a.points / g, ast: a.assists / g, reb: a.rebounds / g, pir: a.pir / g, min: a.minutes / g } }; });
    const K = ['pts', 'ast', 'reb', 'pir', 'min'], bs = {}; K.forEach(k => { bs[k] = Math.max(...sv.map(x => x.pg[k])); });
    const fmtI = v => Math.round(v).toLocaleString('ro-RO');
    const tot = [['Meciuri jucate', AT.games_played, 'în ' + hs.length + ' sezoane'], ['Puncte', AT.points, U.num(AT.points / atg) + ' pe meci'], ['Pase decisive', AT.assists, U.num(AT.assists / atg) + ' pe meci'], ['Recuperări', AT.rebounds, U.num(AT.rebounds / atg) + ' pe meci']]
      .map(([l, v, sub]) => `<div class="tile"><div class="label">${l}</div><div class="avg-v">${fmtI(v)}</div><div class="tile-s">${sub}</div></div>`).join('');
    const PGall = ST.pg.filter(r => r.player_id === pid && +r.seconds_played > 0).sort((a, b) => +b.points - +a.points), top1 = PGall[0];
    let rec = '';
    if (top1) {
      const tg = ST.games[`${top1.competition_key}|${top1.season}|${top1.game_code}`] || {};
      rec = `<a href="${U.gameHref(top1.game_code, top1.season, 'player:' + pid)}" class="rec"><span class="rec-txt"><span class="label">Cel mai bun meci<span class="only-mob"> la U-BT</span></span>
        <span class="rec-v"><b>${top1.points}</b><span>puncte</span></span><span class="rec-m">vs ${U.esc(tg.opp || top1.opponent)}, ${tg.date ? U.dLong(tg.date) : ''}<span class="only-desk"> →</span></span></span><span class="rec-arr only-mob">→</span></a>`;
    }
    const rows = sv.slice().reverse().map(x => ({ l: U.SL(x.s), gp: x.a.games_played, cells: K.map(k => ({ v: k === 'min' ? U.mmss(x.pg[k] * 60) : U.num(x.pg[k]), b: sv.length > 1 && x.pg[k] === bs[k] })) }))
      .concat([{ l: 'Toate sezoanele', tot: true, gp: AT.games_played, cells: [AT.points, AT.assists, AT.rebounds, AT.pir, AT.minutes].map((v, i) => ({ v: i === 4 ? U.mmss(v / atg * 60) : U.num(v / atg), b: true })) }]);
    const desk = rows.map(r => `<div class="tbl-row hs-row${r.tot ? ' total' : ''}"><span class="cp-l${r.tot ? ' b' : ''}">${r.l}</span><span class="t-n${r.tot ? ' b' : ''}">${r.gp}</span>${r.cells.map(c => `<span class="t-n${c.b ? ' b' : ''}">${c.v}</span>`).join('')}</div>`).join('');
    const hL = ['Puncte', 'Pase', 'Recup.', 'Efic.', 'Minute'];
    const mob = rows.map(r => `<div class="hm-card${r.tot ? ' total' : ''}"><div class="hm-h"><span class="cp-l${r.tot ? ' b' : ''}">${r.l}</span><span class="note">${U.meci(r.gp)}</span></div>
      <div class="hm-cells">${r.cells.map((c, i) => `<div><b class="${c.b ? 'b' : ''}">${c.v}</b><span>${hL[i]}</span></div>`).join('')}</div></div>`).join('');
    return `<section class="sec">${U.secHead('Istoricul la U-BT', `Tot ce a făcut pentru U-BT, din sezonul ${U.SL(hs[0])} până acum, în toate competițiile`)}
      <div class="hist-tiles">${tot}${rec}</div>
      <div class="tbl only-desk"><div class="tbl-row tbl-head hs-row"><span>Sezon</span><span class="t-r">Meciuri</span><span class="t-r">Puncte</span><span class="t-r">Pase</span><span class="t-r">Recup.</span><span class="t-r">Efic.</span><span class="t-r">Minute</span></div>${desk}</div>
      <div class="hm-list only-mob">${mob}</div>
      <p class="note">Media pe meci, toate competițiile · cu îngroșat: cel mai bun sezon<span class="only-desk"> la acea cifră</span></p></section>`;
  }
})();
