/* ═══════════════════════════════════════════════════════════════════════════
   U-BT în cifre — elemente comune (header, filtre, footer, formate, popover)
   Folosit de toate paginile: index, game, player, players, team.
   ═══════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  const CB = Date.now();
  const DATA = '../data/processed/';
  const URLS = {
    games:      DATA + 'games_normalized.csv?v=' + CB,
    gameStats:  DATA + 'game_stats.csv?v=' + CB,
    players:    DATA + 'player_stats_normalized.csv?v=' + CB,
    perGame:    DATA + 'player_stats_per_game_normalized.csv?v=' + CB,
    details:    DATA + 'player_details.csv?v=' + CB,
    updated:    DATA + 'last_updated.json?v=' + CB,
    bio:        '../config/player_bio.json?v=' + CB,
    quarters:   DATA + 'quarters.csv?v=' + CB,
  };

  // ── Competiții ────────────────────────────────────────────────────────────
  const ORDER = ['eurocup', 'aba_league', 'liga_nationala', 'cupa_romaniei'];
  const COMP  = { eurocup: 'EuroCup', aba_league: 'ABA League', liga_nationala: 'Liga Națională', cupa_romaniei: 'Cupa României' };
  const SHORT = { eurocup: 'EuroCup', aba_league: 'ABA', liga_nationala: 'LN', cupa_romaniei: 'Cupă' };

  // ── Formate ───────────────────────────────────────────────────────────────
  const MS = ['ian.', 'feb.', 'mar.', 'apr.', 'mai', 'iun.', 'iul.', 'aug.', 'sept.', 'oct.', 'nov.', 'dec.'];
  const ML = ['ianuarie', 'februarie', 'martie', 'aprilie', 'mai', 'iunie', 'iulie', 'august', 'septembrie', 'octombrie', 'noiembrie', 'decembrie'];
  const WD = ['Duminică', 'Luni', 'Marți', 'Miercuri', 'Joi', 'Vineri', 'Sâmbătă'];
  const MINUS = '−';

  const num  = v => (Math.round(v * 10) / 10).toFixed(1).replace('.', ',');
  const mmss = sec => { const t = Math.round(+sec || 0); return `${Math.floor(t / 60)}:${String(t % 60).padStart(2, '0')}`; }; // secunde → m:ss
  const neg  = v => (v < 0 ? MINUS + Math.abs(v) : String(v));          // întreg cu minus tipografic
  const sgn  = v => (v > 0 ? '+' + v : v < 0 ? MINUS + Math.abs(v) : '0');
  const sgnN = v => (v > 0 ? '+' : v < 0 ? MINUS : '') + num(Math.abs(v)); // zecimal cu semn
  const pctR = (m, a) => (a ? Math.round(m / a * 100) + '%' : '–');
  const SL   = s => `${s}–${+s + 1}`;
  const SLs  = s => `${String(s).slice(2)}–${String(+s + 1).slice(2)}`;
  const dLong  = d => { if (!d) return ''; const [y, m, dd] = d.split('-'); return `${+dd} ${MS[+m - 1]} ${y}`; };
  const dShort = d => { if (!d) return ''; const [, m, dd] = d.split('-'); return `${+dd} ${MS[+m - 1]}`; };
  const dWeek  = d => { const x = new Date(d + 'T12:00:00'); return `${WD[x.getDay()]}, ${x.getDate()} ${ML[x.getMonth()]} ${x.getFullYear()}`; };
  const dNum   = d => ({ d: d.slice(8, 10) + '.' + d.slice(5, 7), y: d.slice(0, 4) });
  // „1 meci”, „2–19 meciuri”, „20 de meciuri”
  const plural = (n, one, many) => (n === 1 ? `1 ${one}` : `${n}${n >= 20 ? ' de' : ''} ${many}`);
  const meci   = n => plural(n, 'meci', 'meciuri');
  const pl     = (n, one, many) => (n === 1 ? `o ${one}` : `${n}${n >= 20 ? ' de' : ''} ${many}`);

  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const initials = name => String(name || '').split(/\s+/).filter(Boolean).map(w => w[0]).slice(0, 2).join('').toUpperCase();
  const cleanName = n => String(n || '').replace(/\b(Ii|Iii|Iv)\b/g, m => m.toUpperCase()).replace(/\bMc ([A-Z])/g, 'Mc$1');
  const POS = p => {
    const s = (p || '').toLowerCase().trim();
    if (!s) return '';
    if (s === 'c' || s.includes('center')) return 'Pivot';
    if (s === 'fc' || s === 'cf') return 'Extremă / pivot';
    if (s === 'fg' || s === 'gf') return 'Fundaș / extremă';
    if (s === 'f' || s.includes('forward')) return 'Extremă';
    if (s === 'g' || s.includes('guard')) return 'Fundaș';
    return p;
  };

  // ── Date ──────────────────────────────────────────────────────────────────
  const json = url => fetch(url).then(r => (r.ok ? r.json() : null)).catch(() => null);
  // config/site.json (scris din admin): sezoane ascunse + logo-uri echipe
  let siteP = null;
  const site = () => siteP || (siteP = json('../config/site.json?v=' + CB).then(c => ({ hidden_seasons: [], team_logos: {}, ...(c || {}) })));
  // CSV-urile se citesc fără rândurile din sezoanele ascunse
  const csv = url => Promise.all([site(), fetch(url).then(r => (r.ok ? r.text() : '')).catch(() => '')]).then(([cfg, t]) => {
    if (!t) return [];
    const hid = new Set((cfg.hidden_seasons || []).map(String));
    const rows = Papa.parse(t.trim(), { header: true, skipEmptyLines: true }).data;
    return hid.size ? rows.filter(o => !hid.has(String(o.season))) : rows;
  }).catch(() => []);

  const isTrue = v => v === true || v === 'True' || v === 'true' || v === '1';

  function normGames(rows) {
    return rows.filter(o => o.date && o.game_code).map(o => ({
      key: `${o.competition_key}|${o.season}|${o.game_code}`,
      skey: `${o.competition}|${o.season}|${o.game_code}`,
      comp: o.competition_key, compName: o.competition, season: String(o.season), code: String(o.game_code),
      round: o.round, phase: o.phase, date: String(o.date).slice(0, 10), opp: o.opponent,
      home: isTrue(o.ubt_is_home), venue: o.venue || '', att: +o.attendance || 0,
      us: +o.ubt_score, them: +o.opp_score, win: o.result === 'W', diff: +o.score_diff,
    })).sort((a, b) => a.date.localeCompare(b.date));
  }

  // game_stats.csv → { "EuroCup|2025|2": {ubt_pts: 90, ...} }  (gol = null, nu 0)
  function normTeamStats(rows) {
    const out = {};
    rows.forEach(o => {
      const s = {};
      Object.keys(o).forEach(k => {
        if (/^(ubt|opp)_/.test(k)) s[k] = o[k] === '' || o[k] == null || isNaN(+o[k]) ? null : +o[k];
      });
      out[`${o.competition}|${o.season}|${o.game_code}`] = s;
    });
    return out;
  }

  // Imagini + post din player_stats_normalized
  function bioFromStats(rows) {
    const bio = {};
    rows.forEach(o => {
      const b = bio[o.player_id] || (bio[o.player_id] = {});
      if (o.image_url && !b.img) b.img = o.image_url;
      if (o.position && !b.pos) b.pos = o.position;
    });
    return bio;
  }

  // ── Navigare „Înapoi” ─────────────────────────────────────────────────────
  // ?from=start|team|players|game:{id}:{season}|player:{id}
  function fromParam() {
    const q = new URLSearchParams(location.search);
    let f = q.get('from');
    if (f) return f;
    try {
      const r = document.referrer ? new URL(document.referrer) : null;
      if (r && r.origin === location.origin) {
        const p = r.pathname.split('/').pop();
        const rq = r.searchParams;
        if (p === '' || p === 'index.html') return 'start';
        if (p === 'team.html') return 'team';
        if (p === 'players.html') return 'players';
        if (p === 'game.html' && rq.get('id')) return `game:${rq.get('id')}:${rq.get('season') || ''}`;
        if (p === 'player.html' && rq.get('id')) return `player:${rq.get('id')}`;
      }
    } catch (e) { /* ignore */ }
    return null;
  }

  function backTarget(fallback) {
    const f = fromParam() || fallback;
    const [kind, a, b] = String(f).split(':');
    switch (kind) {
      case 'team':    return { href: 'team.html', label: 'Înapoi la Echipă' };
      case 'players': return { href: 'players.html', label: 'Înapoi la Toți jucătorii' };
      case 'game':    return { href: `game.html?id=${encodeURIComponent(a)}${b ? '&season=' + encodeURIComponent(b) : ''}`, label: 'Înapoi la meci' };
      case 'player':  return { href: `player.html?id=${encodeURIComponent(a)}`, label: 'Înapoi la profilul jucătorului' };
      default:        return { href: 'index.html', label: 'Înapoi la Start' };
    }
  }

  const gameHref   = (code, season, from) => `game.html?id=${encodeURIComponent(code)}&season=${encodeURIComponent(season)}${from ? '&from=' + encodeURIComponent(from) : ''}`;
  const playerHref = (id, from) => `player.html?id=${encodeURIComponent(id)}${from ? '&from=' + encodeURIComponent(from) : ''}`;

  // ── Header / footer ───────────────────────────────────────────────────────
  let updatedTxt = '';
  function header(active) {
    const nav = [['team', 'team.html', 'Echipă'], ['players', 'players.html', 'Jucători']]
      .map(([k, h, l]) => `<a href="${h}" class="hdr-link${active === k ? ' on' : ''}"${active === k ? ' aria-current="page"' : ''}>${l}</a>`).join('');
    const mnav = [['start', 'index.html', 'Start'], ['team', 'team.html', 'Echipă'], ['players', 'players.html', 'Jucători']]
      .map(([k, h, l]) => `<a href="${h}" class="mnav-link">${l}${active === k ? '<span class="mnav-cur">Pagina curentă</span>' : '<span class="mnav-arr">→</span>'}</a>`).join('');
    return `
      <header class="hdr">
        <a href="index.html" class="hdr-logo" title="Prima pagină"><span><img src="assets/logo-ubt.svg" alt="U-BT"></span></a>
        <a href="index.html" class="hdr-brand"><span class="hdr-name">U-BT Cluj-Napoca</span><span class="hdr-sub">U-BT în cifre</span></a>
        <nav class="hdr-nav" aria-label="Pagini">${nav}</nav>
        <button type="button" class="hdr-burger" aria-label="Meniu" aria-expanded="false" data-act="menu"><span></span><span></span><span></span><b>×</b></button>
        <nav class="mnav" aria-label="Meniu">${mnav}<div class="mnav-upd" data-updated-short></div></nav>
      </header>`;
  }

  function footer() {
    return `<footer class="ftr"><span data-updated>Date actualizate</span> · Surse: EuroCup, ABA League, Liga Națională</footer>`;
  }

  function loadUpdated() {
    return json(URLS.updated).then(j => {
      const iso = j && j.last_updated;
      if (!iso) return;
      // last_updated e în UTC: afișăm data locală a cititorului
      const d = new Date(iso);
      const local = isNaN(d) ? String(iso).slice(0, 10) : `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
      updatedTxt = 'Date actualizate la ' + dLong(local);
      paintUpdated();
    });
  }
  function paintUpdated() {
    if (!updatedTxt) return;
    document.querySelectorAll('[data-updated],[data-updated-short]').forEach(el => { el.textContent = updatedTxt; });
  }

  // ── Bara „Înapoi” (Meci, Profil) ──────────────────────────────────────────
  function backBar(t) {
    return `<div class="backbar"><a href="${esc(t.href)}" class="back-link"><span aria-hidden="true">←</span>${esc(t.label)}</a></div>`;
  }

  // ── Filtre ────────────────────────────────────────────────────────────────
  // opts: { comps:[{key,label,on}], season, seasons:[s], ha?:'all'|'home'|'away', summary, inline? }
  // inline=true → în conținut (Profil), nu în bara sticky
  function pills(comps, act) {
    return comps.map(c => `<button type="button" class="pill${c.on ? ' on' : ''}" aria-pressed="${c.on}" data-act="${act}" data-v="${esc(c.key)}">${esc(c.label)}</button>`).join('');
  }
  function seasonSelect(season, seasons, act, cls) {
    return `<select class="fsel ${cls || ''}" data-change="${act}" aria-label="Sezon">${seasons.map(s => `<option value="${esc(s)}"${String(s) === String(season) ? ' selected' : ''}>${SL(s)}</option>`).join('')}</select>`;
  }
  const HA_OPTS = [['all', 'Acasă și deplasare'], ['home', 'Doar acasă'], ['away', 'Doar în deplasare']];
  function filters(o) {
    const ha = o.ha != null ? `
      <label class="fgroup"><span class="flabel">Meciuri</span>
        <select class="fsel" data-change="ha" aria-label="Meciuri">${HA_OPTS.map(([k, l]) => `<option value="${k}"${o.ha === k ? ' selected' : ''}>${l}</option>`).join('')}</select>
      </label>` : '';
    return `
      <div class="fbar${o.inline ? ' fbar-inline' : ''}${o.open ? ' open' : ''}" data-fbar>
        <div class="fbar-sum">
          <div class="fbar-sumtxt"><span class="flabel">Afișezi</span><span class="fbar-sumval">${esc(o.summary)}</span></div>
          <button type="button" class="fbar-toggle" aria-expanded="${!!o.open}" data-act="filters">Filtre</button>
        </div>
        <div class="fbar-ctrls">
          <div class="fgroup"><span class="flabel">Competiție</span><div class="pills">${pills(o.comps, 'comp')}</div></div>
          ${ha}
          <label class="fgroup fgroup-season"><span class="flabel">Sezon</span>${seasonSelect(o.season, o.seasons, 'season')}</label>
        </div>
      </div>`;
  }

  // ── Popover „i” ───────────────────────────────────────────────────────────
  // pos: 'r' (aliniat la dreapta), 'l' (stânga), 'c' (centrat)
  function info(title, text, ex, pos) {
    return `<span class="info" data-info><button type="button" class="info-btn" aria-label="Ce înseamnă?" aria-expanded="false" data-act="info">i</button>` +
      `<span class="info-pop info-${pos || 'l'}" role="dialog"><strong>${esc(title)}</strong><span>${esc(text)}</span>${ex ? `<span class="info-ex">${esc(ex)}</span>` : ''}</span></span>`;
  }
  const INFO = {
    reb: ['Recuperări', 'Mingea prinsă după o aruncare ratată, fie la coșul propriu (în atac), fie la coșul adversarului (în apărare).', 'Mai mare înseamnă mai bine.'],
    ast: ['Pase decisive', 'Pasa după care un coechipier înscrie imediat.', 'Mai mare înseamnă mai bine.'],
    blk: ['Capace', 'O aruncare a adversarului oprită cu mâna înainte să ajungă la coș.', 'Mai mare înseamnă mai bine.'],
    ft:  ['Aruncări libere', 'Aruncări fără apărare, de la linia de 4,6 m, primite după un fault. Fiecare reușită valorează 1 punct.', 'Mai mare înseamnă mai bine.'],
    pir: ['Eficiență', 'Un singur număr pentru tot jocul: adună punctele, recuperările, pasele decisive, mingile furate și capacele, apoi scade aruncările ratate, mingile pierdute și faulturile.', 'Mai mare înseamnă mai bine.'],
    diff: ['Diferență / meci', 'Cu câte puncte a câștigat (+) sau a pierdut (−) U-BT, în medie, un meci.', 'Mai mare înseamnă mai bine.'],
  };
  const infoK = (k, pos) => info(INFO[k][0], INFO[k][1], INFO[k][2], pos);

  // ── Elemente mici ─────────────────────────────────────────────────────────
  function photo(img, name, cls, extra) {
    const st = img ? ` style="background-image:url('${esc(img)}')"` : '';
    return `<div class="ph ${cls || ''}"><span class="ph-ini">${esc(initials(name))}</span>${extra || ''}${img ? `<div class="ph-img" role="img" aria-label="${esc(name)}"${st}></div>` : ''}</div>`;
  }
  const resBadge = (win, cls) => `<span class="res ${win ? 'res-w' : 'res-l'} ${cls || ''}">${win ? 'V' : 'Î'}</span>`;
  function endCards(a, b) {
    const c = (x, dir) => `<a href="${esc(x.href)}" class="endcard endcard-${dir}">` +
      (dir === 'prev' ? '<span class="endcard-arr">←</span>' : '') +
      `<span class="endcard-txt"><span class="endcard-t">${esc(x.title)}</span><span class="endcard-s">${esc(x.sub)}</span></span>` +
      (dir === 'next' ? '<span class="endcard-arr">→</span>' : '') + '</a>';
    return `<div class="endcards">${c(a, a.dir || 'prev')}${c(b, b.dir || 'next')}</div>`;
  }
  const secHead = (h2, lead, extra) => `<div class="sec-head"><h2 class="h2">${h2}</h2>${lead ? `<p class="lead">${lead}</p>` : ''}${extra || ''}</div>`;

  // ── Evenimente (delegare) ─────────────────────────────────────────────────
  const handlers = {};
  const changeHandlers = {};
  function on(act, fn) { handlers[act] = fn; }
  function onChange(act, fn) { changeHandlers[act] = fn; }

  document.addEventListener('click', e => {
    // închide popover-ele „i” la click în afara lor
    const inInfo = e.target.closest('[data-info]');
    document.querySelectorAll('[data-info].open').forEach(el => {
      if (el !== inInfo) { el.classList.remove('open'); el.querySelector('.info-btn').setAttribute('aria-expanded', 'false'); }
    });
    const t = e.target.closest('[data-act]');
    if (!t) return;
    const act = t.dataset.act;
    if (act === 'info') {
      const box = t.closest('[data-info]');
      const open = !box.classList.contains('open');
      box.classList.toggle('open', open);
      t.setAttribute('aria-expanded', String(open));
      return;
    }
    if (act === 'menu') {
      const h = t.closest('.hdr');
      const open = !h.classList.contains('menu-open');
      h.classList.toggle('menu-open', open);
      t.setAttribute('aria-expanded', String(open));
      return;
    }
    if (act === 'filters') {
      const fb = t.closest('[data-fbar]');
      const open = !fb.classList.contains('open');
      fb.classList.toggle('open', open);
      t.setAttribute('aria-expanded', String(open));
      UBT.filtersOpen = open;
      return;
    }
    if (handlers[act]) handlers[act](t, e);
  });
  document.addEventListener('change', e => {
    const t = e.target.closest('[data-change]');
    if (t && changeHandlers[t.dataset.change]) changeHandlers[t.dataset.change](t.value, t, e);
  });

  // Graficele cu scroll orizontal pornesc de la cele mai recente meciuri (dreapta)
  function scrollChartsToEnd(root) {
    (root || document).querySelectorAll('[data-scroll-end]').forEach(el => {
      const last = el.querySelector('[data-last]');   // ultimul meci jucat (după el pot urma locuri goale)
      el.scrollLeft = last ? Math.max(0, last.offsetLeft + last.offsetWidth + 52 - el.clientWidth) : el.scrollWidth;
    });
  }

  const isMobile = () => window.matchMedia('(max-width: 768px)').matches;

  // Shell-ul paginii: header + (bară) + conținut + footer
  function mount(active) {
    const shell = document.getElementById('app');
    shell.innerHTML = header(active) + '<div id="page"><div class="loading">Se încarcă datele…</div></div>' + footer();
    loadUpdated();
    return document.getElementById('page');
  }
  function paint(el, html) {
    el.innerHTML = html;
    paintUpdated();
    scrollChartsToEnd(el);
  }

  window.UBT = {
    URLS, ORDER, COMP, SHORT, MS, MINUS,
    num, neg, mmss, sgn, sgnN, pctR, SL, SLs, dLong, dShort, dWeek, dNum, meci, pl, esc, initials, cleanName, POS,
    csv, json, site, isTrue, normGames, normTeamStats, bioFromStats,
    backTarget, gameHref, playerHref,
    header, footer, backBar, filters, pills, seasonSelect, info, infoK, INFO, photo, resBadge, endCards, secHead,
    on, onChange, mount, paint, scrollChartsToEnd, isMobile, filtersOpen: false,
  };
})();
