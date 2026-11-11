/* Jucători — clasament după criteriu, Top 3, restul lotului */
(function () {
  'use strict';
  const U = UBT;
  const page = U.mount('players');
  const F = ['games_played', 'minutes', 'points', 'rebounds', 'assists', 'pir', 'fg2_made', 'fg2_att', 'fg3_made', 'fg3_att', 'ft_made', 'ft_att'];
  const SORTS = [
    { k: 'pts', l: 'Puncte', u: 'puncte / meci', s: 'pct.' }, { k: 'ast', l: 'Pase decisive', u: 'pase / meci', s: 'pase' },
    { k: 'reb', l: 'Recuperări', u: 'recuperări / meci', s: 'recup.' }, { k: 'pir', l: 'Eficiență', u: 'eficiență / meci', s: 'efic.' },
    { k: 'min', l: 'Minute', u: 'minute / meci', s: 'min' },
    { k: 'fg2', l: 'Aruncări de 2 (%)', u: 'reușite la 2 puncte', pct: 1 }, { k: 'fg3', l: 'Aruncări de 3 (%)', u: 'reușite la 3 puncte', pct: 1 },
    { k: 'ft', l: 'Aruncări libere (%)', u: 'reușite la libere', pct: 1 }, { k: 'name', l: 'Nume (A–Z)' },
  ];
  const MINATT = 10;
  const ST = { rows: [], bio: {}, jersey: {}, season: null, comp: 'all', sort: 'pts' };

  Promise.all([U.csv(U.URLS.players), U.csv(U.URLS.perGame), U.json(U.URLS.bio)]).then(([st, pt, bj]) => {
    ST.rows = st.map(r => ({ ...r, season: String(r.season), player_id: String(r.player_id) }));
    pt.sort((a, b) => String(a.date || '').localeCompare(String(b.date || ''))).forEach(r => { if (r.jersey) ST.jersey[r.player_id] = r.jersey; });
    ST.bio = (bj && bj.players) || {};
    render();
  });

  U.on('comp', t => { ST.comp = t.dataset.v; render(); });
  U.onChange('season', v => { ST.season = v; ST.comp = 'all'; render(); });
  U.on('sort', t => { ST.sort = t.dataset.v; render(); });
  U.onChange('sortmore', v => { if (v !== '_') { ST.sort = v; render(); } });

  function render() {
    if (!ST.rows.length) { U.paint(page, '<main class="main"><div class="empty">Nu am găsit date.</div></main>'); return; }
    const seasons = [...new Set(ST.rows.map(r => r.season))].sort((a, b) => b - a);
    const season = seasons.includes(ST.season) ? ST.season : seasons[0];
    const sR = ST.rows.filter(r => r.season === season && +r.games_played > 0);
    const compKeys = U.ORDER.filter(k => sR.some(r => r.competition_key === k));
    const comp = compKeys.includes(ST.comp) ? ST.comp : 'all';
    const comps = [{ key: 'all', label: 'Toate' }].concat(compKeys.map(k => ({ key: k, label: U.COMP[k] }))).map(c => ({ ...c, on: c.key === comp }));

    const by = {};
    sR.filter(r => comp === 'all' || r.competition_key === comp).forEach(r => { (by[r.player_id] = by[r.player_id] || []).push(r); });
    const P = Object.entries(by).map(([id, rs]) => {
      const a = {}; F.forEach(k => { a[k] = rs.reduce((t, r) => t + (+r[k] || 0), 0); });
      const g = a.games_played || 1, bio = ST.bio[id] || {};
      return {
        id, name: U.cleanName(rs[0].player_name), img: ST.rows.filter(r => r.player_id === id).map(r => r.image_url).find(Boolean),
        gp: a.games_played, pos: U.POS(bio.position || rs.map(r => r.position).find(Boolean)), jersey: bio.jersey != null ? bio.jersey : ST.jersey[id],
        v: { pts: a.points / g, ast: a.assists / g, reb: a.rebounds / g, pir: a.pir / g, min: a.minutes / g, fg2: a.fg2_att ? a.fg2_made / a.fg2_att : 0, fg3: a.fg3_att ? a.fg3_made / a.fg3_att : 0, ft: a.ft_att ? a.ft_made / a.ft_att : 0 },
        att: { fg2: a.fg2_att, fg3: a.fg3_att, ft: a.ft_att }, mk: { fg2: a.fg2_made, fg3: a.fg3_made, ft: a.ft_made },
        tot: { pts: a.points, ast: a.assists, reb: a.rebounds, pir: a.pir, min: a.minutes },
      };
    });

    const S = SORTS.find(s => s.k === ST.sort) || SORTS[0], sk = S.k;
    // Eligibilitate: ≥3 meciuri (≥1 dacă maximul din lot e sub 5); la procente și ≥10 aruncări
    const maxGp = Math.max(0, ...P.map(p => p.gp)), minG = maxGp >= 5 ? 3 : 1;
    const ok = p => p.gp >= minG && (!S.pct || p.att[sk] >= MINATT);
    let list;
    if (sk === 'name') list = [...P].sort((a, b) => a.name.localeCompare(b.name, 'ro')).map(p => ({ p, rank: null }));
    else {
      const el = P.filter(ok).sort((a, b) => b.v[sk] - a.v[sk]); let r = 0;
      list = el.map((p, i) => { if (i === 0 || p.v[sk] !== el[i - 1].v[sk]) r = i + 1; return { p, rank: r }; })   // egalitate → același loc
        .concat(P.filter(p => !ok(p)).sort((a, b) => b.v[sk] - a.v[sk]).map(p => ({ p, rank: null })));
    }
    const fmt = (k, v) => (SORTS.find(s => s.k === k).pct ? Math.round(v * 100) + '%' : k === 'min' ? U.mmss(v * 60) : U.num(v).replace('-', U.MINUS));

    const card = ({ p, rank }, lowGroup) => {
      const shown = sk === 'name' ? 'pts' : sk, Sx = SORTS.find(s => s.k === shown), low = sk !== 'name' && !ok(p);
      const c0 = { v: low && Sx.pct ? '–' : fmt(shown, p.v[shown]), l: Sx.pct ? Sx.l.replace(' (%)', '') : Sx.l + '/meci' };
      const c1 = Sx.pct ? { v: `${p.mk[shown]}/${p.att[shown]}`, l: 'Reușite' } : { v: U.neg(Math.round(p.tot[shown])), l: 'Total ' + Sx.l.toLowerCase() };
      const meta = [p.jersey != null && p.jersey !== '' ? '#' + p.jersey : '', p.pos].filter(Boolean).join(' · ');
      return {
        href: U.playerHref(p.id, 'players'), name: p.name, img: p.img, rank, metaBase: meta,
        meta: lowGroup ? [meta, U.meci(p.gp)].filter(Boolean).join(' · ') : meta,
        cells: [{ ...c0, cls: 'dk' }, c1, { v: String(p.gp), l: 'Meciuri' }],
        c0v: c0.v, c0l: Sx.pct ? c0.l : Sx.l.replace('Pase decisive', 'Pase').replace('Recuperări', 'Recup.') + '/meci',
        sub2: lowGroup ? U.meci(p.gp) : `${c1.l} ${c1.v} · ${U.meci(p.gp)}`,
      };
    };
    const top = sk === 'name' ? [] : list.filter(x => x.rank != null && x.rank <= 3).slice(0, 3);
    const restL = list.filter(x => !top.includes(x));
    const groups = sk === 'name' ? [{ items: restL.map(x => card(x)) }]
      : [{ items: restL.filter(x => x.rank != null).map(x => card(x)) },
         { items: restL.filter(x => x.rank == null).map(x => card(x, true)), title: S.pct ? 'Prea puține aruncări' : 'Puține meciuri jucate', sub: S.pct ? `Nu intră în clasament: mai puțin de ${MINATT} aruncări.` : `Nu intră în clasament: mai puțin de ${minG} meciuri.` }].filter(g => g.items.length);

    const ctx = `${P.length} jucători · ${comp === 'all' ? 'toate competițiile' : U.COMP[comp]}, sezonul ${U.SL(season)}`;
    const summary = (comp === 'all' ? 'Toate competițiile' : U.COMP[comp]) + ' · ' + U.SL(season);
    const sortBtn = (s, label) => `<button type="button" class="pill${s.k === sk ? ' on' : ''}" aria-pressed="${s.k === sk}" data-act="sort" data-v="${s.k}">${label || s.l}</button>`;
    const moreSel = (from, first) => { const more = SORTS.slice(from).some(s => s.k === sk);
      return `<select class="fsel round${more ? ' on' : ''}" data-change="sortmore" aria-label="${first}">${[{ k: '_', l: first }].concat(SORTS.slice(from)).map(s => `<option value="${s.k}"${s.k === (more ? sk : '_') ? ' selected' : ''}>${s.l}</option>`).join('')}</select>`; };
    const cells = cs => cs.map(c => `<div class="rc${c.cls ? ' ' + c.cls : ''}"><b>${U.esc(c.v)}</b><span>${U.esc(c.l).replace('/', '/\u200b')}</span></div>`).join('');

    let h = U.filters({ comps, season, seasons, summary, open: U.filtersOpen });
    h += `<main class="main pl-main"><section class="sec pl-pad"><div class="sec-head"><h1 class="h1">Jucătorii U-BT</h1><p class="lead">${U.esc(ctx)}</p></div>
      <div class="pl-sort only-desk"><span class="label">Clasament după</span><div class="pills" role="group">${SORTS.slice(0, 5).map(s => sortBtn(s)).join('')}</div>${moreSel(5, 'Mai multe…')}</div>
      <div class="pl-sort-m only-mob"><span class="label">Clasament după</span><div class="pills" role="group">${SORTS.slice(0, 3).map(s => sortBtn(s, s.k === 'ast' ? 'Pase' : '')).join('')}${moreSel(3, 'Vezi toate')}</div></div></section>`;

    if (top.length) {
      const T = top.map(x => card(x));
      h += `<section class="sec pl-pad"><h2 class="h2">Top 3 · ${U.esc(S.l.replace(' (%)', ' %'))}</h2>
        <div class="top3 only-desk">${T.map(c => `<a href="${c.href}" class="t3">${U.photo(c.img, c.name, 't3-ph', `<span class="t3-rank">Locul ${c.rank}</span>`)}
          <span class="t3-body"><span class="t3-nm"><b>${U.esc(c.name)}</b><span>${U.esc(c.meta)}</span></span><span class="rcells">${cells(c.cells)}</span></span></a>`).join('')}</div>
        <div class="top3m only-mob">${T.map(c => `<a href="${c.href}" class="t3m">${U.photo(c.img, c.name, 't3m-ph', `<span class="t3m-rank">Locul ${c.rank}</span>`)}
          <span class="t3m-body"><span class="t3-nm"><b>${U.esc(c.name)}</b><span>${U.esc(c.meta)}</span></span><span class="rcells rcells-sm">${cells(c.cells)}</span></span></a>`).join('')}</div></section>`;
    }
    h += `<section class="sec pl-pad"><h2 class="h2">${sk === 'name' ? 'Tot lotul' : 'Restul lotului'}</h2>`;
    groups.forEach(gr => {
      if (gr.title) h += `<div class="pl-grp"><div class="h3 pl-grp-t">${gr.title}</div><p class="note">${gr.sub}</p></div>`;
      h += `<div class="pl-grid only-desk">${gr.items.map(c => `<div class="pl-col"><div class="label-acc">${c.rank != null ? 'Locul ' + c.rank : ''}</div>
          <a href="${c.href}" class="pl-card">${U.photo(c.img, c.name, 'pl-ph')}<span class="pl-body"><span class="t3-nm pl-nm"><b>${U.esc(c.name)}</b><span>${U.esc(c.meta)}</span></span>
          <span class="rcells rcells-sm">${cells(c.cells)}</span></span></a></div>`).join('')}</div>
        <div class="pl-list only-mob">${gr.items.map(c => `<a href="${c.href}" class="pl-row"><span class="pl-rk">${c.rank != null ? c.rank : '–'}</span>${U.photo(c.img, c.name, 'pl-rph')}
          <span class="pl-mid"><b>${U.esc(c.name)}</b><span>${U.esc(c.metaBase)}</span><span>${U.esc(c.sub2)}</span></span>
          <span class="bigstat"><span class="bigstat-v">${U.esc(c.c0v)}</span><span class="pl-c0l">${U.esc(c.c0l)}</span></span></a>`).join('')}</div>`;
    });
    h += '</section>';
    h += `<div class="pl-pad">${U.endCards({ href: 'index.html', title: 'Start', sub: 'Forma echipei și toate meciurile', dir: 'prev' }, { href: 'team.html', title: 'Echipa', sub: 'Cifrele U-BT pe sezon', dir: 'next' })}</div>`;
    h += '</main>';
    U.paint(page, h);
  }
})();
