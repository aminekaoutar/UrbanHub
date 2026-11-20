/* UrbanHub web app · every chart is a query to the FastAPI + DuckDB backend (/api/...). */
(() => {
  const $ = (s) => document.querySelector(s);
  const F = { i: d3.format(',.0f'), d: d3.format(',.1f'), d2: d3.format(',.2f'), s: (v) => (v > 0 ? '+' : '') + d3.format('.1f')(v) };
  const pdate = d3.utcParse('%Y-%m-%d'), pmonth = d3.utcParse('%Y-%m');
  const fdate = d3.utcFormat('%-d %b %Y'), fmonth = d3.utcFormat('%b %Y');
  const CITIES = ['paris', 'lyon', 'lille'];
  const NAME = { paris: 'Paris', lyon: 'Lyon', lille: 'Lille', france: 'France' };
  const CC = (c) => `var(--${c})`;
  const POLN = { pm25: 'PM2.5', pm10: 'PM10', no2: 'NO₂', o3: 'O₃', co: 'CO' };
  const POLC = { pm25: 'var(--pollution)', pm10: 'var(--warn)', no2: 'var(--paris)', o3: 'var(--lille)', co: 'var(--france)' };
  const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
  const DOW = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
  const store = { get(k) { try { return localStorage.getItem(k); } catch { return null; } }, set(k, v) { try { localStorage.setItem(k, v); } catch { /* private mode */ } } };
  const state = { city: CITIES.includes(store.get('uh-city')) ? store.get('uh-city') : 'paris', page: 'overview', start: '2024-01-01', end: '', hidden: {}, meta: null, token: 0, table: 'station_hourly' };

  // ---------------- API client ----------------
  const cache = new Map();
  let pending = 0;
  function live(kind, text) { const l = $('#live'); l.className = `live ${kind}`; $('#liveTxt').textContent = text; }
  async function api(path, params = {}) {
    const q = new URLSearchParams(Object.entries(params).filter(([, v]) => v !== '' && v != null));
    const url = `/api/${path}${q.toString() ? `?${q}` : ''}`;
    if (cache.has(url)) return cache.get(url);
    pending += 1; live('busy', 'Querying the database…');
    const p = fetch(url).then(async (r) => {
      const body = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(body.detail || `HTTP ${r.status}`);
      return body;
    }).finally(() => { pending -= 1; if (!pending) live('', `Live · ${state.meta ? state.meta.backend === 'postgres' ? 'PostgreSQL' : 'Data Lake (DuckDB)' : 'API'}`); });
    cache.set(url, p);
    p.catch(() => cache.delete(url));
    return p;
  }
  const P = (extra = {}) => ({ start: state.start, end: state.end, ...extra });
  const PC = (extra = {}) => P({ city: state.city, ...extra });

  // SQL footer under a chart
  function sql(id, ...results) {
    const el = document.getElementById(id);
    if (!el) return;
    const rs = results.filter(Boolean);
    el.replaceChildren();
    const d = document.createElement('details');
    const s = document.createElement('summary');
    const rows = d3.sum(rs, (r) => (r.rows ? r.rows.length : 0)), ms = d3.sum(rs, (r) => r.ms || 0);
    const b = document.createElement('b'); b.textContent = 'SQL';
    s.append(b, document.createTextNode(`${F.i(rows)} rows · ${F.d(ms)} ms · show the query`));
    const pre = document.createElement('pre'); pre.textContent = rs.map((r) => r.sql).join('\n\n');
    d.append(s, pre); el.append(d);
  }

  // ---------------- tooltip ----------------
  const tip = $('#tip');
  function showTip(ev, lines) {
    tip.replaceChildren(...lines.map((t, i) => { const d = document.createElement('div'); if (i === 0) d.className = 'tip-h'; d.textContent = t; return d; }));
    tip.hidden = false;
    const r = tip.getBoundingClientRect();
    let x = ev.clientX + 14, y = ev.clientY + 14;
    if (x + r.width > innerWidth - 8) x = ev.clientX - r.width - 14;
    if (y + r.height > innerHeight - 8) y = ev.clientY - r.height - 14;
    tip.style.left = `${Math.max(8, x)}px`; tip.style.top = `${Math.max(8, y)}px`;
  }
  const hideTip = () => { tip.hidden = true; };

  // ---------------- chart toolkit ----------------
  function frame(el, h, label) {
    el.replaceChildren();
    const w = Math.max(260, Math.floor(el.clientWidth));
    const svg = d3.select(el).append('svg').attr('viewBox', `0 0 ${w} ${h}`).attr('width', '100%').attr('height', h).attr('role', 'img').attr('aria-label', label);
    return { svg, w, h };
  }
  function msg(el, text, cls = 'note') { el.replaceChildren(); const p = document.createElement('p'); p.className = cls; p.style.padding = '24px 0'; p.textContent = text; el.append(p); }
  function yAxisLeft(svg, y, w, m, fmt, ticks = 5) {
    const g = svg.append('g').attr('class', 'ax').call(d3.axisLeft(y).ticks(ticks).tickFormat(fmt).tickSize(0).tickPadding(6));
    g.select('.domain').remove();
    m.left = Math.ceil(d3.max(g.selectAll('text').nodes(), (n) => n.getComputedTextLength()) || 18) + 12;
    g.attr('transform', `translate(${m.left},0)`);
    svg.insert('g', ':first-child').attr('class', 'grid').selectAll('line').data(y.ticks(ticks)).join('line')
      .attr('x1', m.left).attr('x2', w - m.right).attr('y1', (d) => y(d)).attr('y2', (d) => y(d));
  }
  function withGaps(vals, maxGap) { const out = []; vals.forEach((d, i) => { if (i && d.x - vals[i - 1].x > maxGap) out.push({ x: d.x, y: null }); out.push(d); }); return out; }
  function lineChart(el, { series, h = 260, label, yFmt = F.i, xType = 'time', xFmt, zero = true, maxGap = 1.5 * 864e5, bands = [], dots = [], unit = '', xTicks }) {
    const vis = series.filter((s) => s.values.length);
    const all = vis.flatMap((s) => s.values).filter((d) => d.y != null);
    if (!all.length) return msg(el, 'No data for this period.');
    const { svg, w } = frame(el, h, label);
    const m = { top: 12, right: 14, bottom: 26, left: 40 };
    let [lo, hi] = d3.extent(all.concat(dots), (d) => d.y);
    if (zero) lo = Math.min(0, lo);
    const y = d3.scaleLinear().domain([lo, hi]).nice().range([h - m.bottom, m.top]);
    yAxisLeft(svg, y, w, m, yFmt);
    const x = (xType === 'time' ? d3.scaleUtc() : d3.scaleLinear()).domain(d3.extent(all, (d) => d.x)).range([m.left, w - m.right]);
    const ax = d3.axisBottom(x).ticks(xTicks || Math.max(2, Math.floor((w - m.left) / 90))).tickSizeOuter(0);
    if (xFmt) ax.tickFormat(xFmt);
    svg.append('g').attr('class', 'ax').attr('transform', `translate(0,${h - m.bottom})`).call(ax);
    bands.forEach((b) => svg.append('rect').attr('x', x(b.x0)).attr('width', Math.max(2, x(b.x1) - x(b.x0))).attr('y', m.top).attr('height', h - m.top - m.bottom)
      .style('fill', b.color).style('opacity', 0.22).on('pointermove', (ev) => showTip(ev, b.lines)).on('pointerleave', hideTip));
    vis.forEach((s) => svg.append('path').attr('fill', 'none').style('stroke', s.color).attr('stroke-width', s.width || 1.6).attr('stroke-dasharray', s.dash || null).attr('stroke-linejoin', 'round')
      .attr('d', d3.line().defined((d) => d.y != null).x((d) => x(d.x)).y((d) => y(d.y))(xType === 'time' && !s.noGap ? withGaps(s.values, maxGap) : s.values)));
    const xs = Array.from(new Set(all.map((d) => +d.x))).sort((a, b) => a - b);
    const maps = vis.map((s) => new Map(s.values.map((d) => [+d.x, d.y])));
    const cur = svg.append('line').attr('y1', m.top).attr('y2', h - m.bottom).style('stroke', 'var(--axis)').attr('stroke-dasharray', '3 3').attr('visibility', 'hidden');
    svg.append('rect').attr('x', m.left).attr('y', m.top).attr('width', w - m.left - m.right).attr('height', h - m.top - m.bottom).style('fill', 'transparent')
      .on('pointermove', (ev) => {
        const v = xs[d3.bisectCenter(xs, +x.invert(d3.pointer(ev)[0]))];
        cur.attr('x1', x(v)).attr('x2', x(v)).attr('visibility', 'visible');
        const head = xType === 'time' ? (xFmt ? xFmt(new Date(v)) : fdate(new Date(v))) : (xFmt ? xFmt(v) : v);
        const lines = [head];
        vis.forEach((s, i) => { const val = maps[i].get(v); if (val != null) lines.push(`${s.name}: ${yFmt(val)}${unit}`); });
        showTip(ev, lines);
      }).on('pointerleave', () => { cur.attr('visibility', 'hidden'); hideTip(); });
    dots.forEach((d) => svg.append('circle').attr('cx', x(d.x)).attr('cy', y(d.y)).attr('r', 4.5).style('fill', d.color).style('stroke', 'var(--surface)').attr('stroke-width', 1.5)
      .on('pointermove', (ev) => showTip(ev, d.lines)).on('pointerleave', hideTip));
  }
  function barChart(el, { groups, keys, get, color, h = 250, label, fmt = F.i, groupFmt = (g) => g, keyFmt = (k) => k, stack = false, unit = '', highlight, ref, tickEvery = 1, labels = true }) {
    const cells = groups.flatMap((g) => keys.map((k) => ({ g, k, v: get(g, k) }))).filter((d) => d.v != null && !Number.isNaN(d.v));
    if (!cells.length) return msg(el, 'No data for this period.');
    const { svg, w } = frame(el, h, label);
    const m = { top: 18, right: 10, bottom: 26, left: 40 };
    const tot = stack ? d3.max(groups, (g) => d3.sum(keys, (k) => get(g, k) || 0)) : d3.max(cells, (d) => d.v);
    const y = d3.scaleLinear().domain([Math.min(0, d3.min(cells, (d) => d.v)), Math.max(tot, ref ? ref.v : 0)]).nice().range([h - m.bottom, m.top]);
    yAxisLeft(svg, y, w, m, fmt);
    const x0 = d3.scaleBand().domain(groups).range([m.left, w - m.right]).paddingInner(0.22).paddingOuter(0.06);
    const x1 = d3.scaleBand().domain(stack ? ['_'] : keys).range([0, x0.bandwidth()]).padding(0.06);
    svg.append('g').attr('class', 'ax').attr('transform', `translate(0,${h - m.bottom})`).call(d3.axisBottom(x0).tickFormat((g, i) => (i % tickEvery === 0 ? groupFmt(g) : '')).tickSizeOuter(0).tickSize(0).tickPadding(8));
    let rects;
    if (stack) { const base = new Map(); rects = cells.map((d) => { const b = base.get(d.g) || 0; base.set(d.g, b + d.v); return { ...d, y0: b, y1: b + d.v }; }); }
    else rects = cells.map((d) => ({ ...d, y0: Math.min(0, d.v), y1: Math.max(0, d.v) }));
    svg.append('g').selectAll('rect').data(rects).join('rect').attr('x', (d) => x0(d.g) + x1(stack ? '_' : d.k)).attr('width', x1.bandwidth())
      .attr('y', (d) => y(d.y1)).attr('height', (d) => Math.max(0.5, y(d.y0) - y(d.y1))).attr('rx', 2)
      .style('fill', (d) => color(d.k, d.g)).style('opacity', (d) => (highlight && !highlight(d.k, d.g) ? 0.3 : 1))
      .on('pointermove', (ev, d) => showTip(ev, [`${groupFmt(d.g)}${keys.length > 1 ? ` · ${keyFmt(d.k)}` : ''}`, `${fmt(d.v)}${unit}`])).on('pointerleave', hideTip);
    if (labels && !stack && x1.bandwidth() >= 24) svg.append('g').selectAll('text').data(rects).join('text').attr('class', 'lbl').attr('text-anchor', 'middle')
      .attr('x', (d) => x0(d.g) + x1(d.k) + x1.bandwidth() / 2).attr('y', (d) => y(d.y1) - 4).text((d) => fmt(d.v));
    if (ref) {
      svg.append('line').attr('x1', m.left).attr('x2', w - m.right).attr('y1', y(ref.v)).attr('y2', y(ref.v)).style('stroke', 'var(--fg)').attr('stroke-dasharray', '4 3');
      svg.append('text').attr('class', 'lbl-muted').attr('x', w - m.right).attr('y', y(ref.v) - 4).attr('text-anchor', 'end').text(ref.label);
    }
  }
  function rankChart(el, { rows, label, fmt = F.i, unit = '', ref, onClick }) {
    if (!rows.length) return msg(el, 'No data for this period.');
    const bh = 22, h = rows.length * bh + 24;
    const { svg, w } = frame(el, h, label);
    const texts = svg.append('g').attr('class', 'ax').selectAll('text').data(rows).join('text').attr('y', (d, i) => i * bh + 15).text((d) => d.label);
    const lw = d3.max(texts.nodes(), (n) => n.getComputedTextLength()) || 60;
    const maxLw = Math.min(lw, w * 0.42);
    texts.each(function (d) { let t = d.label; while (this.getComputedTextLength() > maxLw && t.length > 4) { t = t.slice(0, -2); this.textContent = `${t}…`; } });
    texts.attr('x', maxLw).attr('text-anchor', 'end');
    const m = { left: maxLw + 10, right: 52 };
    const x = d3.scaleLinear().domain([0, Math.max(d3.max(rows, (d) => d.v), ref ? ref.v : 0)]).nice().range([m.left, w - m.right]);
    svg.append('g').selectAll('rect').data(rows).join('rect').attr('x', m.left).attr('y', (d, i) => i * bh + 4).attr('height', bh - 8)
      .attr('width', (d) => Math.max(1, x(d.v) - m.left)).attr('rx', 3).style('fill', (d) => d.color).style('cursor', onClick ? 'pointer' : null)
      .on('pointermove', (ev, d) => showTip(ev, d.lines || [d.label, `${fmt(d.v)}${unit}`])).on('pointerleave', hideTip).on('click', (ev, d) => onClick && onClick(d));
    svg.append('g').selectAll('text').data(rows).join('text').attr('class', 'lbl').attr('x', (d) => x(d.v) + 5).attr('y', (d, i) => i * bh + 15).text((d) => fmt(d.v));
    if (ref) {
      svg.append('line').attr('x1', x(ref.v)).attr('x2', x(ref.v)).attr('y1', 0).attr('y2', rows.length * bh).style('stroke', 'var(--fg)').attr('stroke-dasharray', '4 3');
      svg.append('text').attr('class', 'lbl-muted').attr('x', x(ref.v) + 4).attr('y', rows.length * bh + 16).text(ref.label);
    }
  }
  // map of points (+ optional lines between points)
  function mapChart(el, { points, lines = [], h, label, onClick }) {
    const all = points.concat(lines.flatMap((l) => [{ lat: l.a[0], lon: l.a[1] }, { lat: l.b[0], lon: l.b[1] }]));
    if (!all.length) return msg(el, 'No stations for this period.');
    const w0 = Math.max(260, Math.floor(el.clientWidth));
    const lat0 = d3.mean(all, (p) => p.lat) * Math.PI / 180;
    const px = (p) => p.lon * Math.cos(lat0), py = (p) => -p.lat;
    const [x0, x1] = d3.extent(all, px), [y0, y1] = d3.extent(all, py);
    h = h || Math.max(260, Math.min(560, Math.round(w0 * ((y1 - y0) / Math.max(1e-6, x1 - x0)))));
    const { svg, w } = frame(el, h, label);
    const pad = 14, k = Math.min((w - 2 * pad) / Math.max(1e-6, x1 - x0), (h - 2 * pad - 14) / Math.max(1e-6, y1 - y0));
    const ox = (w - k * (x1 - x0)) / 2, oy = (h - 14 - k * (y1 - y0)) / 2;
    const X = (p) => ox + (px(p) - x0) * k, Y = (p) => oy + (py(p) - y0) * k;
    svg.append('g').selectAll('line').data(lines).join('line')
      .attr('x1', (l) => X({ lat: l.a[0], lon: l.a[1] })).attr('y1', (l) => Y({ lat: l.a[0], lon: l.a[1] })).attr('x2', (l) => X({ lat: l.b[0], lon: l.b[1] })).attr('y2', (l) => Y({ lat: l.b[0], lon: l.b[1] }))
      .style('stroke', (l) => l.color).attr('stroke-width', (l) => l.width).attr('stroke-linecap', 'round').style('opacity', 0.65)
      .on('pointermove', (ev, l) => showTip(ev, l.lines)).on('pointerleave', hideTip);
    const sorted = points.slice().sort((a, b) => (a.top ? 1 : 0) - (b.top ? 1 : 0) || a.r - b.r);
    svg.append('g').selectAll('circle').data(sorted).join('circle').attr('cx', X).attr('cy', Y).attr('r', (p) => p.r)
      .style('fill', (p) => p.color).style('opacity', (p) => (p.dim ? 0.25 : 0.88)).style('stroke', (p) => (p.top ? 'var(--fg)' : 'var(--surface)')).attr('stroke-width', (p) => (p.top ? 1.4 : 0.6))
      .style('cursor', onClick ? 'pointer' : null)
      .on('pointermove', (ev, p) => showTip(ev, p.lines)).on('pointerleave', hideTip).on('click', (ev, p) => onClick && onClick(p));
    const km = k / 111.32;
    const L = [0.5, 1, 2, 5, 10].find((v) => v * km > 50) || 10;
    const g = svg.append('g').attr('transform', `translate(${pad},${h - 8})`);
    g.append('line').attr('x2', L * km).style('stroke', 'var(--muted)').attr('stroke-width', 2);
    g.append('text').attr('class', 'lbl-muted').attr('x', L * km + 6).attr('y', 4).text(`${L} km`);
  }
  function scatter(el, { rows, x: xf, y: yf, color, label, xFmt = F.d, yFmt = F.i, tipf, trend = false, h = 280 }) {
    const r0 = rows.filter((d) => xf(d) != null && yf(d) != null);
    if (!r0.length) return msg(el, 'No data for this period.');
    const { svg, w } = frame(el, h, label);
    const m = { top: 10, right: 12, bottom: 28, left: 40 };
    const y = d3.scaleLinear().domain(d3.extent(r0, yf)).nice().range([h - m.bottom, m.top]);
    yAxisLeft(svg, y, w, m, yFmt);
    const x = d3.scaleLinear().domain(d3.extent(r0, xf)).nice().range([m.left, w - m.right]);
    svg.append('g').attr('class', 'ax').attr('transform', `translate(0,${h - m.bottom})`).call(d3.axisBottom(x).ticks(Math.max(3, Math.floor(w / 80))).tickFormat(xFmt).tickSizeOuter(0));
    svg.append('g').selectAll('circle').data(r0).join('circle').attr('cx', (d) => x(xf(d))).attr('cy', (d) => y(yf(d))).attr('r', 3)
      .style('fill', color).style('opacity', 0.7).on('pointermove', (ev, d) => showTip(ev, tipf(d))).on('pointerleave', hideTip);
    if (trend) {
      const r = pearson(r0, xf, yf);
      if (r != null) {
        const mx = d3.mean(r0, xf), my = d3.mean(r0, yf);
        const sl = d3.sum(r0, (d) => (xf(d) - mx) * (yf(d) - my)) / d3.sum(r0, (d) => (xf(d) - mx) ** 2);
        const [a, b] = x.domain();
        svg.append('line').attr('x1', x(a)).attr('x2', x(b)).attr('y1', y(my + sl * (a - mx))).attr('y2', y(my + sl * (b - mx))).style('stroke', 'var(--fg)').attr('stroke-dasharray', '5 4').attr('stroke-width', 1.2);
        svg.append('text').attr('class', 'lbl').attr('x', w - m.right).attr('y', m.top + 10).attr('text-anchor', 'end').text(`r = ${r.toFixed(2)}`);
      }
    }
  }
  function heatmap(el, { rows, cols, get, label, fmt = F.i, rowFmt = (r) => r, colFmt = (c) => c, interp = d3.interpolateGreens }) {
    const vals = rows.flatMap((r) => cols.map((c) => get(r, c))).filter((v) => v != null);
    if (!vals.length) return msg(el, 'No data for this period.');
    const w0 = Math.max(260, el.clientWidth), lab = 38, cw = (w0 - lab) / cols.length, ch = 24, h = rows.length * ch + 22;
    const { svg } = frame(el, h, label);
    const sc = d3.scaleSequential((t) => interp(0.08 + 0.92 * t)).domain([0, d3.max(vals)]);
    rows.forEach((r, i) => {
      svg.append('text').attr('class', 'lbl-muted').attr('x', 0).attr('y', i * ch + 16).text(rowFmt(r));
      svg.append('g').selectAll('rect').data(cols).join('rect').attr('x', (c, j) => lab + j * cw).attr('y', i * ch).attr('width', Math.max(1, cw - 1.5)).attr('height', ch - 2).attr('rx', 2)
        .style('fill', (c) => (get(r, c) == null ? 'var(--soft)' : sc(get(r, c))))
        .on('pointermove', (ev, c) => showTip(ev, [`${rowFmt(r)} · ${colFmt(c)}`, get(r, c) == null ? 'No data' : fmt(get(r, c))])).on('pointerleave', hideTip);
    });
    svg.append('g').selectAll('text').data(cols.filter((c, j) => j % 3 === 0)).join('text').attr('class', 'lbl-muted')
      .attr('x', (c) => lab + cols.indexOf(c) * cw).attr('y', rows.length * ch + 14).text(colFmt);
  }
  function pearson(rows, fa, fb) {
    const a = typeof fa === 'string' ? (d) => d[fa] : fa, b = typeof fb === 'string' ? (d) => d[fb] : fb;
    const r = rows.filter((d) => a(d) != null && b(d) != null);
    if (r.length < 10) return null;
    const ma = d3.mean(r, a), mb = d3.mean(r, b);
    let num = 0, da = 0, db = 0;
    r.forEach((d) => { const x = a(d) - ma, y = b(d) - mb; num += x * y; da += x * x; db += y * y; });
    return da && db ? num / Math.sqrt(da * db) : null;
  }
  function legendButtons(el, items, key, onChange) {
    const hid = state.hidden[key] || (state.hidden[key] = new Set());
    el.replaceChildren(...items.map(([id, name, color]) => {
      const b = document.createElement('button'); b.type = 'button'; b.setAttribute('aria-pressed', String(!hid.has(id)));
      const i = document.createElement('i'); i.style.background = color; b.append(i, document.createTextNode(name));
      b.addEventListener('click', () => { hid.has(id) ? hid.delete(id) : hid.add(id); onChange(); });
      return b;
    }));
    return hid;
  }
  function legendStatic(el, items) {
    el.replaceChildren(...items.map(([name, color]) => { const s = document.createElement('span'); s.style.display = 'inline-flex'; s.style.alignItems = 'center'; s.style.gap = '6px';
      const i = document.createElement('i'); i.style.background = color; s.append(i, document.createTextNode(name)); return s; }));
  }
  function legendGrad(el, interp, lo, hi, fmt) {
    const c = d3.range(0, 1.01, 0.1).map((t) => interp(t)).join(',');
    const a = document.createElement('span'); a.textContent = fmt(lo);
    const g = document.createElement('span'); g.className = 'grad'; g.style.background = `linear-gradient(90deg, ${c})`;
    const b = document.createElement('span'); b.textContent = fmt(hi);
    el.replaceChildren(a, g, b);
  }
  function setHTML(el, parts) { el.replaceChildren(...parts.map((p) => { if (Array.isArray(p)) { const b = document.createElement('b'); b.textContent = p[0]; return b; } return document.createTextNode(p); })); }
  function rowsInto(tbody, rows, cols, onClick) {
    tbody.replaceChildren(...rows.map((r) => { const tr = document.createElement('tr');
      cols.forEach(([get, cls]) => { const td = document.createElement('td'); const v = get(r); if (v instanceof Node) td.append(v); else td.textContent = v; if (cls) td.className = cls; tr.append(td); });
      if (onClick) { tr.style.cursor = 'pointer'; tr.addEventListener('click', () => onClick(r)); }
      return tr; }));
  }
  function pill(text, cls) { const s = document.createElement('span'); s.className = `pill ${cls}`; s.textContent = text; return s; }
  function kpis(el, items) {
    el.replaceChildren(...items.map(([v, l]) => { const d = document.createElement('div'); d.className = 'kpi';
      const a = document.createElement('div'); a.className = 'v'; a.textContent = v; const b = document.createElement('div'); b.className = 'l'; b.textContent = l; d.append(a, b); return d; }));
  }
  function metaInto(el, pairs) { el.replaceChildren(); pairs.forEach(([a, b]) => { const s = document.createElement('span'); s.textContent = a; const v = document.createElement('b'); v.textContent = b; el.append(s, v); }); }
  const periodText = () => `${state.start ? fdate(pdate(state.start)) : 'start'} – ${state.end ? fdate(pdate(state.end)) : 'today'}`;
  const tableInfo = (n) => (state.meta.tables.find((t) => t.name === n) || {});

  // ---------------- OVERVIEW ----------------
  async function renderOverview(tk) {
    const m = state.meta;
    metaInto($('#ov-meta'), [['Cities', 'Paris · Lyon · Lille'], ['Database', m.backend === 'postgres' ? 'PostgreSQL' : 'Data Lake (DuckDB on Parquet)'], ['Tables', String(m.tables.length)], ['Rows', F.i(d3.sum(m.tables, (t) => t.rows))]]);
    const parts = [
      ['weather', 'Part 1 · Weather', 'Urban weather', F.i(tableInfo('weather_city_hourly').rows), 'city-hours of weather'],
      ['mobility', 'Part 2 · Mobility', 'Bike-share', F.i(tableInfo('station_hourly').rows), `station-hours${m.has_trips ? ` and ${F.i(tableInfo('vlille_trips').rows)} V'Lille trips` : ''}`],
      ['pollution', 'Part 3 · Pollution', 'Air quality', F.i(tableInfo('measurements').rows), 'sensor measurements'],
      ['cross', 'Part 4 · Cross analysis', 'Weather × bikes × air', 'city + day', 'the join key of every cross query'],
    ];
    $('#ov-parts').replaceChildren(...parts.map(([id, tag, title, big, text]) => {
      const a = document.createElement('a'); a.className = 'part'; a.href = `#${id}`; a.style.setProperty('--c', `var(--${id})`);
      const t = document.createElement('div'); t.className = 'tag'; t.textContent = tag;
      const h = document.createElement('h3'); h.textContent = title;
      const b = document.createElement('div'); b.className = 'big'; b.textContent = big;
      const p = document.createElement('p'); p.textContent = text;
      const g = document.createElement('div'); g.className = 'go'; g.textContent = 'Open →';
      a.append(t, h, b, p, g); return a; }));
    const cov = await api('coverage');
    if (tk !== state.token) return;
    const el = $('#ov-coverage');
    const months = d3.utcMonths(pmonth('2024-01'), d3.utcMonth.offset(d3.utcMonth.floor(new Date()), 1)).map(d3.utcFormat('%Y-%m'));
    const rows = ['Weather', 'Bikes', 'Pollution'].flatMap((s) => CITIES.map((c2) => `${s}|${c2}`));
    const v = new Map(cov.rows.map((r) => [`${r.source}|${r.city}|${r.month}`, r.days]));
    const w0 = Math.max(300, el.clientWidth), mm = { left: 118, top: 18, right: 4 }, cw = (w0 - mm.left - mm.right) / months.length, ch = 18, hh = rows.length * (ch + 3) + mm.top + 6;
    const { svg } = frame(el, hh, 'Days of data per month for each source and city');
    const srcColor = { Weather: 'var(--weather)', Bikes: 'var(--mobility)', Pollution: 'var(--pollution)' };
    rows.forEach((r, i) => {
      const [s, c2] = r.split('|');
      svg.append('text').attr('x', 0).attr('y', mm.top + i * (ch + 3) + 13).style('fill', 'var(--muted)').style('font-size', '11.5px').text(`${s} · ${NAME[c2]}`);
      svg.append('g').selectAll('rect').data(months).join('rect').attr('x', (mo, j) => mm.left + j * cw).attr('y', mm.top + i * (ch + 3)).attr('width', Math.max(1, cw - 1.5)).attr('height', ch).attr('rx', 2)
        .style('fill', (mo) => (v.get(`${r}|${mo}`) ? srcColor[s] : 'var(--soft)')).style('opacity', (mo) => (v.get(`${r}|${mo}`) ? 0.25 + 0.75 * Math.min(1, v.get(`${r}|${mo}`) / 28) : 1))
        .on('pointermove', (ev, mo) => showTip(ev, [`${s} · ${NAME[c2]} · ${fmonth(pmonth(mo))}`, `${v.get(`${r}|${mo}`) || 0} days with data`])).on('pointerleave', hideTip);
    });
    svg.append('g').selectAll('text').data(months.filter((mo) => mo.endsWith('-01'))).join('text').attr('class', 'lbl-muted').attr('x', (mo) => mm.left + months.indexOf(mo) * cw).attr('y', 11).text((mo) => mo.slice(0, 4));
    legendStatic($('#ov-coverage-legend'), [['Weather', 'var(--weather)'], ['Bikes', 'var(--mobility)'], ['Pollution', 'var(--pollution)'], ['No data', 'var(--soft)']]);
    sql('ov-coverage-sql', cov);
  }

  // ---------------- PART 1 · WEATHER ----------------
  async function renderWeather(tk) {
    const c = state.city;
    const z = $('#w-z').value, mind = $('#w-min').value;
    const ext = { heat: $('#w-heat').value, cold: $('#w-cold').value, rain: $('#w-rain').value, wind: $('#w-wind').value };
    const [daily, ano, monthly, extremes, vis] = await Promise.all([api('weather/daily', PC()), api('weather/anomalies', P({ z, min_days: mind })), api('weather/monthly', P()), api('weather/extremes', P(ext)), api('weather/visibility', P())]);
    if (tk !== state.token) return;
    metaInto($('#w-meta'), [['Table', 'weather_city_hourly'], ['Source', state.meta.weather_source.includes('noaa') ? 'NOAA + Open-Meteo (Dev 1)' : 'Open-Meteo (local copy)'], ['Period', periodText()]]);
    const wd = daily.rows.map((d) => ({ ...d, x: pdate(d.day) }));
    if (!wd.length) { kpis($('#w-kpis'), [['—', 'no weather for this period']]); return; }
    const hottest = d3.greatest(wd, (d) => d.tmax), coldest = d3.least(wd, (d) => d.tmin);
    kpis($('#w-kpis'), [[`${F.d(d3.mean(wd, (d) => d.tmean))} °C`, `average in ${NAME[c]}`], [`${F.d(hottest.tmax)} °C`, `hottest, ${fdate(hottest.x)}`], [`${F.d(coldest.tmin)} °C`, `coldest, ${fdate(coldest.x)}`], [`${F.i(d3.sum(wd, (d) => d.rain))} mm`, 'total rain']]);
    // Q1
    const A = ano.rows, ca = A.filter((a) => a.city === c);
    const strongest = d3.greatest(A, (a) => Math.abs(a.gap));
    setHTML($('#w-a1'), strongest ? ['With a z-score of ', [z], ` and at least ${mind} days, the database finds `, [`${A.length} abnormal periods`], ` (${A.filter((a) => a.type === 'Hot').length} hot, ${A.filter((a) => a.type === 'Cold').length} cold), `, [`${ca.length} in ${NAME[c]}`], '. The strongest: ', [NAME[strongest.city]], ` from ${fdate(pdate(strongest.start))}, `, [`${F.s(strongest.gap)} °C`], ' from the normal.'] : ['No abnormal period with these settings. Lower the sensitivity.']);
    $('#w-temp-sub').textContent = `°C · ${NAME[c]} · daily mean, abnormal periods shaded`;
    lineChart($('#w-temp'), { label: `Daily mean temperature in ${NAME[c]} with abnormal periods`, zero: false, yFmt: F.d, unit: ' °C',
      series: [{ name: 'Mean', color: 'var(--weather)', values: wd.map((d) => ({ x: d.x, y: d.tmean })), width: 1.2 }],
      bands: ca.map((a) => ({ x0: pdate(a.start), x1: d3.utcDay.offset(pdate(a.end), 1), color: a.type === 'Hot' ? 'var(--hot)' : 'var(--cold)', lines: [`${a.type} period · ${a.days} days`, `${fdate(pdate(a.start))} → ${fdate(pdate(a.end))}`, `Mean ${F.d(a.tmean)} °C (${F.s(a.gap)} °C vs normal)`] })) });
    sql('w-temp-sql', daily);
    timeline($('#w-timeline'), A, c, wd);
    sql('w-timeline-sql', ano);
    // Q2
    const vr = vis.rows.filter((r) => r.by === 'rain'), vw = vis.rows.filter((r) => r.by === 'wind');
    const hours = d3.sum(vr, (r) => r.hours);
    if (hours) {
      const dry = vr.find((r) => r.band.startsWith('0 ')), wet = d3.least(vr, (r) => r.visibility_km);
      const calm = vw.find((r) => r.band === '0–1'), windy = d3.greatest(vw, (r) => r.band);
      setHTML($('#w-a2'), ['Yes. Over ', [`${F.i(hours)} hours`], ' with a visibility reading, it averages ', [`${F.d(dry ? dry.visibility_km : NaN)} km`], ' when dry and drops to ', [`${F.d(wet.visibility_km)} km`], ` with ${wet.band.replace(' +', ' mm or more').replace(/^(\d)–(\d)$/, '$1 to $2 mm')} of rain in the hour.`, calm ? ` Calm air (under 1 m/s) has fog ${F.d(calm.fog_pct)} % of the time, against ${F.d(windy.fog_pct)} % above ${windy.band.replace(' +', '')} m/s.` : '']);
    } else setHTML($('#w-a2'), ['No visibility readings in this period. Visibility only exists in part of the weather table: widen the period.']);
    const rb = ['0 (dry)', '0–1', '1–2', '2–5', '5 +'], wb = ['0–1', '1–2', '2–4', '4–6', '6 +'];
    barChart($('#w-vrain'), { groups: rb, keys: ['v'], get: (g) => (vr.find((r) => r.band === g) || {}).visibility_km, color: () => 'var(--weather)', fmt: F.d, unit: ' km', label: 'Visibility by rain' });
    barChart($('#w-vwind'), { groups: wb, keys: ['v', 'f'], get: (g, k) => { const r = vw.find((x) => x.band === g); return r ? (k === 'v' ? r.visibility_km : r.fog_pct) : null; }, color: (k) => (k === 'v' ? 'var(--weather)' : 'var(--france)'), keyFmt: (k) => (k === 'v' ? 'Visibility (km)' : 'Fog hours (%)'), fmt: F.d, label: 'Visibility and fog by wind' });
    sql('w-vis-sql', vis);
    // Q3
    const mon = monthly.rows.map((d) => ({ ...d, x: pmonth(d.month) }));
    const draw3 = () => {
      const hid = legendButtons($('#w-month-legend'), CITIES.map((c2) => [c2, NAME[c2], CC(c2)]), 'wmonth', draw3);
      lineChart($('#w-month'), { label: 'Monthly mean temperature', zero: false, yFmt: F.d, unit: ' °C', xFmt: fmonth, maxGap: 40 * 864e5,
        series: CITIES.filter((c2) => !hid.has(c2)).map((c2) => ({ name: NAME[c2], color: CC(c2), width: c2 === c ? 2.6 : 1.4, values: mon.filter((d) => d.city === c2).map((d) => ({ x: d.x, y: d.tmean })) })) });
    };
    draw3();
    sql('w-month-sql', monthly);
    const moy = (cc) => [...d3.rollup(mon.filter((d) => d.city === cc), (v) => d3.mean(v, (d) => d.tmean), (d) => +d.month.slice(5))];
    const ranges = CITIES.map((cc) => { const r = moy(cc); return { c: cc, v: r.length ? d3.max(r, (d) => d[1]) - d3.min(r, (d) => d[1]) : null }; }).filter((r) => r.v != null);
    const mc = moy(c);
    if (mc.length && ranges.length) {
      const coldM = d3.least(mc, (d) => d[1]), warmM = d3.greatest(mc, (d) => d[1]);
      const most = d3.greatest(ranges, (r) => r.v), least = d3.least(ranges, (r) => r.v);
      setHTML($('#w-a3'), [`In ${NAME[c]} the coldest month is `, [MONTHS[coldM[0] - 1]], ` (${F.d(coldM[1])} °C) and the warmest is `, [MONTHS[warmM[0] - 1]], ` (${F.d(warmM[1])} °C). `, [NAME[most.c]], ' has the most contrasted climate (', [`${F.d(most.v)} °C`], ' between its warmest and coldest month), ', [NAME[least.c]], ' the mildest (', [`${F.d(least.v)} °C`], ').']);
    }
    const season = (mo) => ['Winter', 'Winter', 'Spring', 'Spring', 'Spring', 'Summer', 'Summer', 'Summer', 'Autumn', 'Autumn', 'Autumn', 'Winter'][+mo.slice(5) - 1];
    const sv = d3.rollup(mon, (v) => d3.mean(v, (d) => d.tmean), (d) => season(d.month), (d) => d.city);
    legendStatic($('#w-season-legend'), CITIES.map((k) => [NAME[k], CC(k)]));
    barChart($('#w-season'), { groups: ['Winter', 'Spring', 'Summer', 'Autumn'], keys: CITIES, get: (g, k) => sv.get(g)?.get(k), color: (k) => CC(k), keyFmt: (k) => NAME[k], fmt: F.d, unit: ' °C', highlight: (k) => k === c, label: 'Average temperature by season' });
    rankChart($('#w-range'), { rows: ranges.map((r) => ({ label: NAME[r.c], v: r.v, color: CC(r.c) })), fmt: F.d, unit: ' °C', label: 'Summer–winter gap' });
    // Q4
    const E = extremes.rows, types = ['Heat', 'Heavy rain', 'Strong wind', 'Severe cold'];
    const tcol = { Heat: 'var(--hot)', 'Heavy rain': 'var(--rain)', 'Strong wind': 'var(--france)', 'Severe cold': 'var(--cold)' };
    const ce = E.filter((e) => e.city === c);
    const byCity = [...d3.rollup(E, (v) => v.length, (e) => e.city)];
    const top = d3.greatest(byCity, (d) => d[1]);
    setHTML($('#w-a4'), top ? [[`${E.length} extreme days`], ' with these thresholds across the three cities. ', [NAME[top[0]]], ` has the most (${top[1]}). ${NAME[c]}: `, [`${ce.length}`], ` (${types.map((t) => `${ce.filter((e) => e.type === t).length} ${t.toLowerCase()}`).join(', ')}).`] : ['No day passes these thresholds in this period.']);
    const draw4 = () => {
      const hidE = legendButtons($('#w-ext-legend'), types.map((t) => [t, t, tcol[t]]), 'wext', draw4);
      const el = $('#w-ext');
      const { svg, w } = frame(el, 150, 'Extreme days over time');
      const mm = { left: 50, right: 10, top: 6 }, rh = 36;
      const x = d3.scaleUtc().domain(d3.extent(wd, (d) => d.x)).range([mm.left, w - mm.right]);
      svg.append('g').attr('class', 'ax').attr('transform', `translate(0,${mm.top + 3 * rh + 4})`).call(d3.axisBottom(x).ticks(Math.max(3, Math.floor(w / 110))).tickSizeOuter(0));
      CITIES.forEach((c2, i) => {
        svg.append('text').attr('x', 0).attr('y', mm.top + i * rh + 22).style('fill', c2 === c ? 'var(--fg)' : 'var(--muted)').style('font-size', '12px').style('font-weight', c2 === c ? 600 : 400).text(NAME[c2]);
        svg.append('g').selectAll('circle').data(E.filter((e) => e.city === c2 && !hidE.has(e.type))).join('circle').attr('cx', (e) => x(pdate(e.day))).attr('cy', (e) => mm.top + i * rh + 8 + types.indexOf(e.type) * 6).attr('r', 3.6)
          .style('fill', (e) => tcol[e.type]).style('opacity', c2 === c ? 0.95 : 0.4)
          .on('pointermove', (ev, e) => showTip(ev, [`${NAME[c2]} · ${e.type}`, fdate(pdate(e.day)), `High ${F.d(e.tmax)} °C · low ${F.d(e.tmin)} °C`, `Rain ${F.d(e.rain)} mm · wind up to ${F.d(e.wind_max)} m/s`])).on('pointerleave', hideTip);
      });
    };
    draw4();
    sql('w-ext-sql', extremes);
    barChart($('#w-ext-bars'), { groups: types, keys: CITIES, get: (g, k) => E.filter((e) => e.city === k && e.type === g).length, color: (k) => CC(k), keyFmt: (k) => NAME[k], highlight: (k) => k === c, label: 'Extreme days by type' });
    const rec = [['Highest temperature', d3.greatest(wd, (d) => d.tmax), (d) => `${F.d(d.tmax)} °C`], ['Lowest temperature', d3.least(wd, (d) => d.tmin), (d) => `${F.d(d.tmin)} °C`], ['Most rain in a day', d3.greatest(wd, (d) => d.rain), (d) => `${F.d(d.rain)} mm`], ['Windiest day (mean)', d3.greatest(wd, (d) => d.wind), (d) => `${F.d(d.wind)} m/s`]];
    $('#w-rec-sub').textContent = `${NAME[c]} · ${periodText()}`;
    rowsInto($('#w-records'), rec, [[(r) => r[0]], [(r) => r[2](r[1]), 'num'], [(r) => fdate(r[1].x)]]);
  }
  function timeline(el, A, c, wd) {
    const { svg, w } = frame(el, 120, 'Abnormal weather periods for the three cities');
    const m = { left: 50, right: 10, top: 6 }, rh = 30;
    const x = d3.scaleUtc().domain(d3.extent(wd, (d) => d.x)).range([m.left, w - m.right]);
    svg.append('g').attr('class', 'ax').attr('transform', `translate(0,${m.top + 3 * rh})`).call(d3.axisBottom(x).ticks(Math.max(3, Math.floor(w / 110))).tickSizeOuter(0));
    CITIES.forEach((c2, i) => {
      svg.append('text').attr('x', 0).attr('y', m.top + i * rh + 19).style('fill', c2 === c ? 'var(--fg)' : 'var(--muted)').style('font-size', '12px').style('font-weight', c2 === c ? 600 : 400).text(NAME[c2]);
      svg.append('line').attr('x1', m.left).attr('x2', w - m.right).attr('y1', m.top + i * rh + 15).attr('y2', m.top + i * rh + 15).style('stroke', 'var(--grid)');
      svg.append('g').selectAll('rect').data(A.filter((a) => a.city === c2)).join('rect').attr('x', (a) => x(pdate(a.start))).attr('width', (a) => Math.max(3, x(d3.utcDay.offset(pdate(a.end), 1)) - x(pdate(a.start)))).attr('y', m.top + i * rh + 6).attr('height', 18).attr('rx', 3)
        .style('fill', (a) => (a.type === 'Hot' ? 'var(--hot)' : 'var(--cold)')).style('opacity', c2 === c ? 0.95 : 0.45)
        .on('pointermove', (ev, a) => showTip(ev, [`${NAME[c2]} · ${a.type} · ${a.days} days`, `${fdate(pdate(a.start))} → ${fdate(pdate(a.end))}`, `${F.s(a.gap)} °C vs normal`])).on('pointerleave', hideTip);
    });
  }

  // ---------------- PART 2 · MOBILITY ----------------
  const crit = { k: 'crit', dir: -1, rows: [] };
  async function renderMobility(tk) {
    const c = state.city;
    const [stR, dailyR, profR, dowR, heatR, monR, occR] = await Promise.all([api('mobility/stations', PC()), api('mobility/daily', PC()), api('mobility/profile', PC()), api('mobility/dow', PC()), api('mobility/heatmap', PC()), api('mobility/monthly', P()), api('mobility/occupancy', PC())]);
    if (tk !== state.token) return;
    const clean = (n, id) => (n || '').replace(/^\d+\s*-\s*/, '').trim() || `Station ${id.slice(0, 6)}`;   // '3019 - PART-DIEU' → 'PART-DIEU'
    const st = stR.rows.map((s) => ({ ...s, name: clean(s.name, s.station_id) })), md = dailyR.rows.map((d) => ({ ...d, x: pdate(d.day) })), prof = profR.rows;
    metaInto($('#m-meta'), [['Tables', 'station_hourly, mobility_city_hourly'], ['Sources', 'CityBikes, Grand Lyon, ilévia'], ['Period', periodText()]]);
    if (!st.length || !md.length) { kpis($('#m-kpis'), [['—', `no bike data for ${NAME[c]} in this period`]]); ['m-a1', 'm-a2', 'm-a3', 'm-a4', 'm-a5'].forEach((id) => setHTML($(`#${id}`), ['No bike data for this city and period. Bike data starts in Jan 2024 for Lyon and Nov 2024 for Paris and Lille.'])); }
    else {
      const wk = prof.filter((p) => p.daytype === 'Weekday');
      const wkPeak = d3.greatest(wk, (p) => p.moves);
      kpis($('#m-kpis'), [[F.i(d3.mean(md, (d) => d.moves)), 'bike movements a day'], [F.i(st.length), 'active stations'], [`${F.d(d3.mean(md, (d) => d.empty_pct))} %`, 'of stations empty, on average'], [wkPeak ? `${wkPeak.hour}:00` : '—', 'busiest weekday hour']]);
      sql('m-stations-sql', stR);
      // Q1
      const sorted = st.slice().sort((a, b) => b.per_day - a.per_day), top = sorted.slice(0, 15);
      const share = d3.sum(sorted.slice(0, Math.ceil(st.length * 0.1)), (s) => s.per_day) / d3.sum(st, (s) => s.per_day) * 100;
      setHTML($('#m-a1'), ['The busiest station in ', [NAME[c]], ' is ', [top[0].name], ' with about ', [`${F.i(top[0].per_day)} movements a day`], '. The busiest 10 % of stations handle ', [`${F.i(share)} %`], ' of all movements.']);
      rankChart($('#m-top'), { rows: top.map((s) => ({ ...s, label: s.name, v: s.per_day, color: 'var(--mobility)', lines: [s.name, `${F.i(s.per_day)} movements / day`, `Empty ${F.d(s.empty_pct)} % · full ${F.d(s.full_pct)} % of the time`, 'Click for detail'] })), label: `Top 15 stations in ${NAME[c]}`, onClick: openStation });
      const topSet = new Set(top.map((s) => s.station_id)), rMax = d3.max(st, (s) => s.per_day);
      mapChart($('#m-topmap'), { label: `Stations in ${NAME[c]} sized by use`, onClick: openStation, points: st.map((s) => ({ ...s, r: 1.5 + 9 * Math.sqrt(s.per_day / rMax), color: topSet.has(s.station_id) ? 'var(--mobility)' : 'var(--france)', dim: !topSet.has(s.station_id), top: topSet.has(s.station_id), lines: [s.name, `${F.i(s.per_day)} movements / day`] })) });
      // Q2
      const metric = $('#m-map-metric').value;
      const q95 = (k) => d3.quantile(st.map((s) => s[k]).sort(d3.ascending), 0.95);
      const mdef = { empty_pct: [d3.interpolateYlOrRd, (v) => `${F.i(v)} %`, 60], full_pct: [d3.interpolatePuBu, (v) => `${F.i(v)} %`, 40], per_day: [d3.interpolateGreens, F.i, q95('per_day')], cap: [d3.interpolateBlues, F.i, q95('cap')] }[metric];
      const col = (v) => mdef[0](0.12 + 0.88 * Math.min(1, (v || 0) / mdef[2]));
      legendGrad($('#m-map-legend'), (t) => mdef[0](0.12 + 0.88 * t), 0, mdef[2], (v) => `${mdef[1](v)}${v === mdef[2] ? '+' : ''}`);
      mapChart($('#m-map'), { label: `Map of ${NAME[c]} stations`, onClick: openStation, points: st.map((s) => ({ ...s, r: 3.4, color: col(s[metric]), lines: [s.name, `Empty ${F.d(s.empty_pct)} % · full ${F.d(s.full_pct)} % of the time`, `${F.i(s.per_day)} movements / day · ${F.i(s.cap)} docks`, 'Click for detail'] })) });
      const zones = d3.rollups(st, (v) => ({ n: v.length, empty: d3.mean(v, (s) => s.empty_pct), per_day: d3.sum(v, (s) => s.per_day), near: d3.greatest(v, (s) => s.per_day).name }), (s) => `${Math.round(s.lon / 0.0137)}|${Math.round(s.lat / 0.009)}`)
        .map(([, z]) => z).filter((z) => z.n >= 2 && z.per_day >= 20).sort((a, b) => b.empty - a.empty).slice(0, 10);
      rowsInto($('#m-zones'), zones, [[(z) => z.near], [(z) => F.i(z.n), 'num'], [(z) => pill(`${F.i(z.empty)} %`, z.empty > 40 ? 'bad' : 'warn')], [(z) => F.i(z.per_day), 'num']]);
      const CENTER = { paris: [48.8566, 2.3522], lyon: [45.764, 4.8357], lille: [50.6292, 3.0573] }[c];
      const km = (s) => Math.hypot((s.lat - CENTER[0]) * 111.32, (s.lon - CENTER[1]) * 111.32 * Math.cos(CENTER[0] * Math.PI / 180));
      const eIn = d3.mean(st.filter((s) => km(s) <= 4), (s) => s.empty_pct), eOut = d3.mean(st.filter((s) => km(s) > 4), (s) => s.empty_pct);
      const nE = st.filter((s) => s.empty_pct > 30).length;
      setHTML($('#m-a2'), [[`${nE} stations`], ` in ${NAME[c]} (${F.i(nE / st.length * 100)} %) are empty more than 30 % of the time.`, zones[0] ? ` The worst area is around ${zones[0].near} (${F.i(zones[0].empty)} % of the time). ` : ' ', 'Stations more than 4 km from the centre are empty ', [`${F.d(eOut)} %`], ' of the time, against ', [`${F.d(eIn)} %`], ' within 4 km.']);
      lineChart($('#m-emptyhour'), { label: 'Share of empty stations by hour', xType: 'linear', xFmt: (h) => `${h}:00`, xTicks: 8, yFmt: F.d, unit: ' %',
        series: ['Weekday', 'Weekend'].map((k) => ({ name: k, color: k === 'Weekday' ? 'var(--mobility)' : 'var(--dry)', width: 2, values: prof.filter((p) => p.daytype === k).map((p) => ({ x: p.hour, y: p.empty_pct })) })) });
      barChart($('#m-occ'), { groups: occR.rows.map((r) => r.band), keys: ['p'], get: (g) => (occR.rows.find((r) => r.band === g) || {}).pct, color: (k, g) => (g === 0 ? 'var(--bad)' : g === 90 ? 'var(--paris)' : 'var(--mobility)'), groupFmt: (g) => `${g}–${g + 10}`, fmt: F.d, unit: ' %', label: 'Share of station-hours by fill rate' });
      sql('m-occ-sql', occR);
      // Q3
      const am = d3.greatest(wk.filter((p) => p.hour >= 6 && p.hour <= 10), (p) => p.moves), pm = d3.greatest(wk.filter((p) => p.hour >= 15 && p.hour <= 21), (p) => p.moves), we = d3.greatest(prof.filter((p) => p.daytype === 'Weekend'), (p) => p.moves);
      const bestDow = d3.greatest(dowR.rows, (d) => d.moves);
      setHTML($('#m-a3'), ['On weekdays there are two peaks: the morning commute at ', [`${am.hour}:00`], ` (${F.i(am.moves)} movements an hour) and the evening peak at `, [`${pm.hour}:00`], ` (${F.i(pm.moves)}); the ${am.moves > pm.moves ? 'morning' : 'evening'} one is bigger. At weekends the busiest hour is `, [`${we ? we.hour : '—'}:00`], '. The busiest day is ', [DOW[bestDow.dow - 1]], '.']);
      const draw3 = () => {
        const hid = legendButtons($('#m-hour-legend'), [['Weekday', 'Weekday', 'var(--mobility)'], ['Weekend', 'Weekend', 'var(--dry)']], 'mhour', draw3);
        lineChart($('#m-hours'), { label: 'Movements by hour', xType: 'linear', xFmt: (h) => `${h}:00`, xTicks: 8, series: ['Weekday', 'Weekend'].filter((k) => !hid.has(k)).map((k) => ({ name: k, color: k === 'Weekday' ? 'var(--mobility)' : 'var(--dry)', width: 2.2, values: prof.filter((p) => p.daytype === k).map((p) => ({ x: p.hour, y: p.moves })) })) });
      };
      draw3();
      sql('m-hours-sql', profR);
      const hv = new Map(heatR.rows.map((r) => [`${r.dow}|${r.hour}`, r.moves]));
      heatmap($('#m-heat'), { rows: [1, 2, 3, 4, 5, 6, 7], cols: d3.range(24), get: (r, h) => hv.get(`${r}|${h}`), rowFmt: (r) => DOW[r - 1].slice(0, 3), colFmt: (h) => `${h}h`, label: 'Movements by weekday and hour', fmt: (v) => `${F.i(v)} movements/h` });
      sql('m-heat-sql', heatR);
      barChart($('#m-dow'), { groups: [1, 2, 3, 4, 5, 6, 7], keys: ['m'], get: (g) => (dowR.rows.find((d) => d.dow === g) || {}).moves, color: (k, g) => (g >= 6 ? 'var(--dry)' : 'var(--mobility)'), groupFmt: (g) => DOW[g - 1].slice(0, 3), label: 'Movements by day of the week', labels: false });
      lineChart($('#m-daily'), { label: 'Movements per day', series: [{ name: 'Movements', color: 'var(--mobility)', values: md.map((d) => ({ x: d.x, y: d.moves })), width: 1.1 }] });
      sql('m-daily-sql', dailyR);
      // Q4
      const sw = st.filter((s) => s.occ_night != null && s.occ_day != null).map((s) => ({ ...s, swing: s.occ_day - s.occ_night }));
      if (sw.length) {
        const ext = Math.max(20, d3.quantile(sw.map((s) => Math.abs(s.swing)).sort(d3.ascending), 0.95));
        const div = (v) => d3.interpolatePuOr(0.5 + 0.5 * Math.max(-1, Math.min(1, v / ext)));
        legendGrad($('#m-swing-legend'), (t) => d3.interpolatePuOr(t), -ext, ext, (v) => `${v > 0 ? '+' : ''}${F.i(v)} pts`);
        mapChart($('#m-swing'), { label: 'Night vs midday fill rate', onClick: openStation, points: sw.map((s) => ({ ...s, r: 3.6, color: div(s.swing), lines: [s.name, `Night ${F.i(s.occ_night)} % full → midday ${F.i(s.occ_day)} %`, 'Click for detail'] })) });
        const fill = sw.slice().sort((a, b) => b.swing - a.swing).slice(0, 8), drain = sw.slice().sort((a, b) => a.swing - b.swing).slice(0, 8);
        rowsInto($('#m-fill'), fill, [[(s) => s.name], [(s) => `${F.i(s.occ_night)} %`, 'num'], [(s) => `${F.i(s.occ_day)} %`, 'num']], openStation);
        rowsInto($('#m-drain'), drain, [[(s) => s.name], [(s) => `${F.i(s.occ_night)} %`, 'num'], [(s) => `${F.i(s.occ_day)} %`, 'num']], openStation);
        setHTML($('#m-a4'), ['Yes. ', [fill[0].name], ` goes from ${F.i(fill[0].occ_night)} % full at night to ${F.i(fill[0].occ_day)} % at midday: bikes arrive there for work or study. `, [drain[0].name], ` goes from ${F.i(drain[0].occ_night)} % to ${F.i(drain[0].occ_day)} %: people leave home by bike. `, [`${sw.filter((s) => s.swing > 20).length}`], ' stations fill up by more than 20 points during the day, ', [`${sw.filter((s) => s.swing < -20).length}`], ' empty by more than 20.']);
      }
      // Q5
      crit.rows = st.filter((s) => s.per_day >= 5).map((s) => ({ ...s, crit: s.empty_pct + s.full_pct }));
      renderCrit();
      const worst = d3.greatest(crit.rows, (s) => s.crit);
      if (worst) setHTML($('#m-a5'), [[`${crit.rows.filter((s) => s.crit > 30).length} active stations`], ` in ${NAME[c]} are empty or full more than 30 % of the time. The most critical is `, [worst.name], ` (${F.i(worst.empty_pct)} % empty, ${F.i(worst.full_pct)} % full). Click a row to see when it happens.`]);
      // size vs use
      scatter($('#m-sizeuse'), { rows: st, x: (s) => s.cap, y: (s) => s.per_day, color: 'var(--mobility)', xFmt: F.i, label: 'Docks vs movements per day', trend: true, tipf: (s) => [s.name, `${F.i(s.cap)} docks`, `${F.i(s.per_day)} movements / day`] });
    }
    // networks compared (all cities)
    const mon = monR.rows.map((d) => ({ ...d, x: pmonth(d.month) }));
    const drawCmp = () => {
      const hid = legendButtons($('#m-cmp-legend'), CITIES.map((cc) => [cc, NAME[cc], CC(cc)]), 'mcmp', drawCmp);
      lineChart($('#m-cmp'), { label: 'Movements per station per day by month', xFmt: fmonth, maxGap: 40 * 864e5, yFmt: F.i, series: CITIES.filter((cc) => !hid.has(cc)).map((cc) => ({ name: NAME[cc], color: CC(cc), width: cc === c ? 2.6 : 1.4, values: mon.filter((d) => d.city === cc).map((d) => ({ x: d.x, y: d.moves_per_station })) })) });
    };
    drawCmp();
    sql('m-cmp-sql', monR);
    lineChart($('#m-size'), { label: 'Stations reporting by month', xFmt: fmonth, maxGap: 40 * 864e5, zero: false, series: CITIES.map((cc) => ({ name: NAME[cc], color: CC(cc), width: 2, values: mon.filter((d) => d.city === cc).map((d) => ({ x: d.x, y: d.stations })) })) });
    lineChart($('#m-emptym'), { label: 'Empty stations by month', xFmt: fmonth, maxGap: 40 * 864e5, yFmt: F.d, unit: ' %', series: CITIES.map((cc) => ({ name: NAME[cc], color: CC(cc), width: 2, values: mon.filter((d) => d.city === cc).map((d) => ({ x: d.x, y: d.empty_pct })) })) });
    const lastM = (cc) => d3.greatest(mon.filter((d) => d.city === cc), (d) => d.month);
    const per = CITIES.map((cc) => [cc, d3.mean(mon.filter((d) => d.city === cc), (d) => d.moves_per_station)]).filter((d) => d[1] != null);
    if (per.length) { const best = d3.greatest(per, (d) => d[1]); setHTML($('#m-a6'), ['Per station, ', [NAME[best[0]]], ' is the busiest network with ', [`${F.i(best[1])} movements per station per day`], ` on average (${per.map(([cc, v]) => `${NAME[cc]} ${F.i(v)}`).join(', ')}). Latest network sizes: ${CITIES.map((cc) => (lastM(cc) ? `${NAME[cc]} ${F.i(lastM(cc).stations)} stations` : null)).filter(Boolean).join(', ')}.`]); }
    await renderTrips(tk);
  }
  function renderCrit() {
    const rows = crit.rows.slice().sort((a, b) => crit.dir * (a[crit.k] > b[crit.k] ? 1 : a[crit.k] < b[crit.k] ? -1 : 0)).slice(0, 20);
    rowsInto($('#m-crit'), rows, [[(s) => s.name], [(s) => pill(`${F.i(s.crit)} %`, s.crit > 50 ? 'bad' : 'warn')], [(s) => `${F.i(s.empty_pct)} %`, 'num'], [(s) => `${F.i(s.full_pct)} %`, 'num'], [(s) => F.i(s.per_day), 'num'], [(s) => F.i(s.cap), 'num'], [(s) => (s.empty_pct >= s.full_pct ? 'Bring bikes' : 'Take bikes away')]], openStation);
    document.querySelectorAll('#m-crit-table th[data-k]').forEach((th) => th.setAttribute('aria-sort', th.dataset.k === crit.k ? (crit.dir > 0 ? 'ascending' : 'descending') : 'none'));
  }
  async function renderTrips(tk) {
    const sec = $('#mq7');
    if (!state.meta.has_trips) { sec.hidden = true; return; }
    sec.hidden = false;
    const top = $('#t-top').value;
    const [sum, hourly, dur, daily, flows, sts] = await Promise.all([api('trips/summary', P()), api('trips/hourly', P()), api('trips/durations', P()), api('trips/daily', P()), api('trips/flows', P({ top })), api('trips/stations', P())]);
    if (tk !== state.token) return;
    const s = sum.rows[0];
    if (!s || !s.trips) { setHTML($('#m-a7'), ['No V\'Lille trips in this period. Trips start on 1 April 2025.']); ['t-flows', 't-hours', 't-dur', 't-daily', 't-net'].forEach((id) => msg(document.getElementById(id), 'No trips in this period.')); kpis($('#t-kpis'), []); return; }
    kpis($('#t-kpis'), [[F.i(s.trips), 'trips'], [F.i(s.trips / s.days), 'trips a day'], [`${F.d(s.median_min)} min`, 'median trip'], [`${F.i(s.under30_pct)} %`, 'under 30 minutes'], [`${F.d(s.round_trip_pct)} %`, 'return to the same station']]);
    const fr = flows.rows;
    setHTML($('#m-a7'), [[`${F.i(s.trips)} trips`], ` over ${F.i(s.days)} days, about `, [`${F.i(s.trips / s.days)} a day`], '. Half the trips last under ', [`${F.d(s.median_min)} minutes`], fr[0] ? `. The busiest route is ${fr[0].start_station} → ${fr[0].end_station} (${F.i(fr[0].trips)} trips).` : '.']);
    const wmax = d3.max(fr, (f) => f.trips) || 1;
    const stp = sts.rows;
    mapChart($('#t-flows'), { label: 'Busiest V\'Lille routes', h: 420,
      lines: fr.map((f) => ({ a: [f.start_lat, f.start_lon], b: [f.end_lat, f.end_lon], width: 1 + 7 * (f.trips / wmax), color: 'var(--mobility)', lines: [`${f.start_station} → ${f.end_station}`, `${F.i(f.trips)} trips · median ${F.d(f.median_min)} min`] })),
      points: stp.map((p) => ({ lat: p.lat, lon: p.lon, r: 1.6, color: 'var(--france)', dim: true, lines: [p.station, `${F.i(p.departures)} departures · ${F.i(p.arrivals)} arrivals`] })) });
    sql('t-flows-sql', flows, sts);
    const drawH = () => {
      const hid = legendButtons($('#t-hour-legend'), [['Weekday', 'Weekday', 'var(--mobility)'], ['Weekend', 'Weekend', 'var(--dry)']], 'thour', drawH);
      lineChart($('#t-hours'), { label: 'Trips by hour', xType: 'linear', xFmt: (h) => `${h}:00`, xTicks: 8, series: ['Weekday', 'Weekend'].filter((k) => !hid.has(k)).map((k) => ({ name: k, color: k === 'Weekday' ? 'var(--mobility)' : 'var(--dry)', width: 2.2, values: hourly.rows.filter((r) => r.daytype === k).map((r) => ({ x: r.hour, y: r.trips })) })) });
    };
    drawH();
    const buckets = Array.from(new Set(dur.rows.map((r) => r.bucket))).sort((a, b) => a - b);
    const dv = new Map(dur.rows.map((r) => [`${r.bucket}|${r.daytype}`, r.trips]));
    barChart($('#t-dur'), { groups: buckets, keys: ['Weekday', 'Weekend'], stack: true, get: (g, k) => dv.get(`${g}|${k}`) || null, color: (k) => (k === 'Weekday' ? 'var(--mobility)' : 'var(--dry)'), groupFmt: (g) => (g === 60 ? '60+' : String(g)), tickEvery: 2, label: 'Trips by duration' });
    lineChart($('#t-daily'), { label: 'Trips per day', series: [{ name: 'Trips', color: 'var(--mobility)', width: 1.1, values: daily.rows.map((r) => ({ x: pdate(r.day), y: r.trips })) }] });
    const nmax = Math.max(50, d3.quantile(stp.map((p) => Math.abs(p.net)).sort(d3.ascending), 0.95) || 50);
    legendGrad($('#t-net-legend'), (t) => d3.interpolatePuOr(t), -nmax, nmax, (v) => `${v > 0 ? '+' : ''}${F.i(v)}`);
    mapChart($('#t-net'), { label: 'Arrivals minus departures per station', points: stp.map((p) => ({ lat: p.lat, lon: p.lon, r: 2 + 6 * Math.sqrt(p.departures / d3.max(stp, (q) => q.departures)), color: d3.interpolatePuOr(0.5 + 0.5 * Math.max(-1, Math.min(1, p.net / nmax))), lines: [p.station, `${F.i(p.departures)} departures · ${F.i(p.arrivals)} arrivals`, `${p.net > 0 ? '+' : ''}${F.i(p.net)} bikes over the period`] })) });
  }
  async function openStation(s) {
    const dr = $('#drawer');
    dr.hidden = false;
    $('#dr-name').textContent = s.name;
    $('#dr-sub').textContent = `${NAME[state.city]} · ${F.i(s.cap)} docks · ${periodText()}`;
    kpis($('#dr-kpis'), [[F.i(s.per_day), 'movements a day'], [`${F.i(s.empty_pct)} %`, 'of time empty'], [`${F.i(s.full_pct)} %`, 'of time full']]);
    ['dr-prof', 'dr-daily'].forEach((id) => msg(document.getElementById(id), 'Querying…'));
    try {
      const r = await api(`mobility/station/${s.station_id}`, P());
      legendStatic($('#dr-legend'), [['Weekday', 'var(--mobility)'], ['Weekend', 'var(--dry)']]);
      lineChart($('#dr-prof'), { label: `Fill rate by hour at ${s.name}`, xType: 'linear', xFmt: (h) => `${h}:00`, xTicks: 6, yFmt: F.d, unit: ' %', h: 200,
        series: ['Weekday', 'Weekend'].map((k) => ({ name: k, color: k === 'Weekday' ? 'var(--mobility)' : 'var(--dry)', width: 2, values: r.profile.rows.filter((p) => p.daytype === k).map((p) => ({ x: p.hour, y: p.occ })) })) });
      lineChart($('#dr-daily'), { label: `Movements per day at ${s.name}`, h: 180, series: [{ name: 'Movements', color: 'var(--mobility)', width: 1.1, values: r.daily.rows.map((d) => ({ x: pdate(d.day), y: d.moves })) }] });
      sql('dr-sql', r.profile, r.daily);
    } catch (e) { msg($('#dr-prof'), e.message, 'err-msg'); }
  }

  // ---------------- PART 3 · POLLUTION ----------------
  async function renderPollution(tk) {
    const c = state.city, M = state.meta;
    const hp = $('#p-hour-pol').value, ep = $('#p-ep-pol').value, pz = $('#p-z').value, pw = $('#p-win').value;
    const [dailyR, stR, profR, domR, monR, epR] = await Promise.all([api('pollution/daily', PC()), api('pollution/stations', P()), api('pollution/profile', PC()), api('pollution/dominant', P()), api('pollution/monthly', PC()), api('pollution/episodes', PC({ pollutant: ep, z: pz, window: pw }))]);
    if (tk !== state.token) return;
    metaInto($('#p-meta'), [['Tables', 'pollution_city_hourly, measurements'], ['Source', 'OpenAQ API v3 (EEA)'], ['Period', periodText()]]);
    const pd = dailyR.rows.map((d) => ({ ...d, x: pdate(d.day) }));
    const above = (cc, k) => (domR.rows.find((r) => r.kind === 'above' && r.city === cc && r.pollutant === k) || {}).pct;
    kpis($('#p-kpis'), [[F.d(d3.mean(pd.filter((d) => d.pm25 != null), (d) => d.pm25)), 'PM2.5 µg/m³ (WHO year: 5)'], [F.d(d3.mean(pd.filter((d) => d.no2 != null), (d) => d.no2)), 'NO₂ µg/m³ (WHO year: 10)'], [`${F.i(above(c, 'pm25') || 0)} %`, 'of days above WHO PM2.5 daily'], [F.i(stR.rows.filter((s) => s.city === c).length), 'stations']]);
    // Q1
    const pol = $('#p-map-pol').value, who = M.who_year[pol];
    const all = stR.rows.filter((s) => s[pol] != null), sts = all.filter((s) => s.city === c);
    const mx = d3.max(all, (s) => s[pol]) || 1;
    const sc = (v) => d3.interpolateYlOrRd(0.15 + 0.85 * Math.min(1, v / mx));
    legendGrad($('#p-map-legend'), (t) => d3.interpolateYlOrRd(0.15 + 0.85 * t), 0, mx, (v) => `${F.i(v)} µg/m³`);
    mapChart($('#p-map'), { label: `${POLN[pol]} stations in ${NAME[c]}`, h: 300, points: sts.map((s) => ({ lat: s.lat, lon: s.lon, r: 7, color: sc(s[pol]), lines: [s.name, `${POLN[pol]} ${F.d(s[pol])} µg/m³`, `${F.d(s[pol] / who)}× the WHO yearly guideline`] })) });
    sql('p-st-sql', stR);
    $('#p-rank-sub').textContent = `${POLN[pol]}, µg/m³ · dashed line = WHO yearly guideline (${who})`;
    rankChart($('#p-rank'), { rows: sts.slice().sort((a, b) => b[pol] - a[pol]).slice(0, 12).map((s) => ({ label: s.name, v: s[pol], color: sc(s[pol]) })), fmt: F.d, ref: { v: who, label: 'WHO' }, label: `Stations ranked by ${POLN[pol]}` });
    const ratio = (cc, k) => { const v = stR.rows.filter((s) => s.city === cc && s[k] != null); return v.length ? d3.mean(v, (s) => s[k]) / M.who_year[k] : null; };
    barChart($('#p-who'), { groups: ['pm25', 'pm10', 'no2'], keys: CITIES, get: (g, k) => ratio(k, g), color: (k) => CC(k), keyFmt: (k) => NAME[k], groupFmt: (g) => POLN[g], fmt: F.d2, unit: '×', highlight: (k) => k === c, ref: { v: 1, label: 'WHO guideline' }, label: 'City averages divided by the WHO yearly guideline' });
    const worst = d3.greatest(stR.rows.filter((s) => s.no2 != null), (s) => s.no2);
    if (worst) setHTML($('#p-a1'), ['The highest NO₂ is at ', [worst.name], ` (${NAME[worst.city]}): `, [`${F.d(worst.no2)} µg/m³`], `, ${F.d(worst.no2 / 10)}× the WHO yearly guideline. ${NAME[c]}'s stations average `, [`${F.d(ratio(c, 'pm25'))}×`], ' the guideline for PM2.5 and ', [`${F.d(ratio(c, 'no2'))}×`], ' for NO₂.']);
    // Q2
    const prof = profR.rows, wk = prof.filter((p) => p.daytype === 'Weekday');
    const drawH = () => {
      const hid = legendButtons($('#p-hour-legend'), [['Weekday', 'Weekday', POLC[hp]], ['Weekend', 'Weekend', 'var(--france)']], 'phour', drawH);
      lineChart($('#p-hours'), { label: `${POLN[hp]} by hour`, xType: 'linear', xFmt: (h) => `${h}:00`, xTicks: 8, yFmt: F.d, unit: ' µg/m³', series: ['Weekday', 'Weekend'].filter((k) => !hid.has(k)).map((k) => ({ name: k, color: k === 'Weekday' ? POLC[hp] : 'var(--france)', width: 2.2, dash: k === 'Weekend' ? '5 3' : null, values: prof.filter((p) => p.daytype === k).map((p) => ({ x: p.hour, y: p[hp] })) })) });
    };
    drawH();
    sql('p-hours-sql', profR);
    const n2 = wk.filter((p) => p.no2 != null);
    if (n2.length) {
      const mxh = d3.greatest(n2, (p) => p.no2), mnh = d3.least(n2, (p) => p.no2), o3h = d3.greatest(wk.filter((p) => p.o3 != null), (p) => p.o3);
      const amNo2 = (k) => d3.mean(prof.filter((p) => p.daytype === k && p.hour >= 7 && p.hour <= 10), (p) => p.no2);
      setHTML($('#p-a2'), [`NO₂ follows traffic: on weekdays in ${NAME[c]} it peaks around `, [`${mxh.hour}:00`], ` (${F.d(mxh.no2)} µg/m³) and is lowest around `, [`${mnh.hour}:00`], ` (${F.d(mnh.no2)}).${o3h ? ` Ozone, made by sunlight, peaks around ${o3h.hour}:00.` : ''} Between 7 and 10 am, NO₂ averages `, [`${F.d(amNo2('Weekday'))} µg/m³`], ' on weekdays and ', [F.d(amNo2('Weekend'))], ' at weekends.']);
    }
    // Q3
    const polK = ['pm25', 'pm10', 'no2', 'o3', 'co'];
    const domPct = (cc, k) => (domR.rows.find((r) => r.kind === 'dominant' && r.city === cc && r.pollutant === k) || {}).pct;
    legendStatic($('#p-dom-legend'), polK.filter((k) => CITIES.some((cc) => domPct(cc, k))).map((k) => [POLN[k], POLC[k]]));
    barChart($('#p-dom'), { groups: CITIES, keys: polK, stack: true, get: (g, k) => domPct(g, k) || null, color: (k) => POLC[k], keyFmt: (k) => POLN[k], groupFmt: (g) => NAME[g], fmt: F.i, unit: ' %', label: 'Main pollutant of the day' });
    sql('p-dom-sql', domR);
    legendStatic($('#p-above-legend'), CITIES.map((cc) => [NAME[cc], CC(cc)]));
    barChart($('#p-above'), { groups: ['pm25', 'pm10', 'no2', 'o3'], keys: CITIES, get: (g, k) => above(k, g), color: (k) => CC(k), keyFmt: (k) => NAME[k], groupFmt: (g) => POLN[g], fmt: F.i, unit: ' %', highlight: (k) => k === c, label: 'Days above the WHO daily guideline' });
    const mon = monR.rows.map((d) => ({ ...d, x: pmonth(d.month) }));
    const drawM = () => {
      const hid = legendButtons($('#p-month-legend'), ['pm25', 'pm10', 'no2', 'o3'].map((k) => [k, POLN[k], POLC[k]]), 'pmonth', drawM);
      lineChart($('#p-month'), { label: 'Monthly pollutant levels', xFmt: fmonth, maxGap: 40 * 864e5, yFmt: F.d, unit: ' µg/m³', series: ['pm25', 'pm10', 'no2', 'o3'].filter((k) => !hid.has(k)).map((k) => ({ name: POLN[k], color: POLC[k], width: 2, values: mon.map((d) => ({ x: d.x, y: d[k] })) })) });
    };
    drawM();
    sql('p-month-sql', monR);
    const domC = polK.map((k) => [k, domPct(c, k) || 0]).sort((a, b) => b[1] - a[1]);
    const peak = (k) => { const r = [...d3.rollup(mon.filter((d) => d[k] != null), (v) => d3.mean(v, (d) => d[k]), (d) => d.x.getUTCMonth())]; return r.length ? MONTHS[d3.greatest(r, (d) => d[1])[0]] : '—'; };
    setHTML($('#p-a3'), [`In ${NAME[c]}, `, [POLN[domC[0][0]]], ' is most often the main pollutant (', [`${F.i(domC[0][1])} % of days`], '), then ', [POLN[domC[1][0]]], ` (${F.i(domC[1][1])} %). PM2.5 is above the WHO daily guideline on `, [`${F.i(above(c, 'pm25') || 0)} % of days`], ', NO₂ on ', [`${F.i(above(c, 'no2') || 0)} %`], '. PM2.5 is highest in ', [peak('pm25')], ', ozone in ', [peak('o3')], '.']);
    // Q4
    const er = epR.rows, eps = er.filter((e) => e.episode);
    $('#p-ep-sub').textContent = `${POLN[ep]}, µg/m³ · dots = days more than ${pz} standard deviations above the previous ${pw} days`;
    lineChart($('#p-ep'), { label: `Daily ${POLN[ep]} with detected episodes`, yFmt: F.d, unit: ' µg/m³',
      series: [{ name: POLN[ep], color: POLC[ep], width: 1.1, values: er.map((d) => ({ x: pdate(d.day), y: d.value })) }, ...(er.length ? [{ name: `WHO daily (${M.who_day[ep]})`, color: 'var(--fg)', dash: '4 3', width: 1, noGap: true, values: [{ x: pdate(er[0].day), y: M.who_day[ep] }, { x: pdate(er[er.length - 1].day), y: M.who_day[ep] }] }] : [])],
      dots: eps.map((e) => ({ x: pdate(e.day), y: e.value, color: 'var(--bad)', lines: [`Episode · ${fdate(pdate(e.day))}`, `${POLN[ep]} ${F.d(e.value)} µg/m³`, `z-score ${F.d(e.z)}`] })) });
    sql('p-ep-sql', epR);
    const topE = eps.slice().sort((a, b) => b.z - a.z).slice(0, 10);
    rowsInto($('#p-ep-table'), topE, [[(e) => fdate(pdate(e.day))], [(e) => `${F.d(e.value)} µg/m³`, 'num'], [(e) => { const r = e.value / M.who_day[ep]; return pill(`${F.d(r)}×`, r > 1 ? 'bad' : 'warn'); }], [(e) => F.d(e.z), 'num']]);
    const season = (d) => ['winter', 'winter', 'spring', 'spring', 'spring', 'summer', 'summer', 'summer', 'autumn', 'autumn', 'autumn', 'winter'][pdate(d).getUTCMonth()];
    const bs = d3.greatest([...d3.rollup(eps, (v) => v.length, (e) => season(e.day))], (d) => d[1]);
    setHTML($('#p-a4'), eps.length ? ['With these settings the database flags ', [`${eps.length} ${POLN[ep]} episode days`], ` in ${NAME[c]}. The strongest: `, [fdate(pdate(topE[0].day))], ` at ${F.d(topE[0].value)} µg/m³ (z = ${F.d(topE[0].z)}). Most fall in `, [bs[0]], ` (${bs[1]} of ${eps.length}).`] : ['No episode with these settings. Lower the z-score or change the pollutant.']);
  }

  // ---------------- PART 4 · CROSS ----------------
  const VARS = { moves: ['Bike movements', F.i, ''], tmean: ['Temperature', F.d, ' °C'], rain: ['Rain', F.d, ' mm'], wind: ['Wind', F.d, ' m/s'], pm25: ['PM2.5', F.d, ' µg/m³'], no2: ['NO₂', F.d, ' µg/m³'] };
  const xsel = { x: 'tmean', y: 'moves' };
  async function renderCross(tk) {
    const c = state.city;
    const R = await api('cross/daily', P());
    if (tk !== state.token) return;
    const X = R.rows;
    sql('x-sql', R);
    if (!X.length) { setHTML($('#x-a1'), ['No day has all three sources in this period.']); return; }
    const days = X.map((d) => d.day).sort();
    $('#x-period').textContent = `${fdate(pdate(days[0]))} – ${fdate(pdate(days[days.length - 1]))}`;
    $('#x-days').textContent = F.i(X.length);
    const bands = ['Dry (< 1 mm)', 'Light (1–5 mm)', 'Rainy (5 mm +)'];
    const band = (r) => (r < 1 ? bands[0] : r < 5 ? bands[1] : bands[2]);
    const bcol = { [bands[0]]: 'var(--dry)', [bands[1]]: 'color-mix(in srgb, var(--rain) 55%, var(--surface))', [bands[2]]: 'var(--rain)' };
    const wkd = X.filter((d) => !d.weekend);
    const rb = d3.rollup(wkd, (v) => d3.mean(v, (d) => d.moves), (d) => d.city, (d) => band(d.rain));
    legendStatic($('#x-rain-legend'), bands.map((b) => [b, bcol[b]]));
    barChart($('#x-rain'), { groups: CITIES, keys: bands, get: (g, k) => rb.get(g)?.get(k), color: (k) => bcol[k], groupFmt: (g) => NAME[g], highlight: (k, g) => g === c, label: 'Weekday movements by rain band' });
    const dry = rb.get(c)?.get(bands[0]), wet = rb.get(c)?.get(bands[2]);
    const drops = CITIES.map((cc) => [cc, rb.get(cc)?.get(bands[2]) / rb.get(cc)?.get(bands[0])]).filter((d) => d[1]);
    setHTML($('#x-a1'), dry && wet ? ['Yes. On weekdays with 5 mm of rain or more, ', [NAME[c]], ' has ', [`${F.i(Math.abs((wet / dry - 1) * 100))} % ${wet < dry ? 'fewer' : 'more'}`], ` bike movements than on dry days (${F.i(wet)} vs ${F.i(dry)}). Change by city: ${drops.map(([cc, v]) => `${NAME[cc]} ${F.s((v - 1) * 100)} %`).join(', ')}.`] : ['Not enough rainy days in this period to compare.']);
    const tb = (t) => Math.floor(t / 3) * 3;
    const tr = CITIES.map((cc) => { const rows = wkd.filter((d) => d.city === cc); const avg = d3.mean(rows, (d) => d.moves);
      return { cc, values: d3.rollups(rows, (v) => (v.length >= 4 ? d3.mean(v, (d) => d.moves) / avg * 100 : null), (d) => tb(d.tmean)).map(([x, y]) => ({ x, y })).filter((d) => d.y != null).sort((a, b) => a.x - b.x) }; });
    const drawT = () => {
      const hid = legendButtons($('#x-temp-legend'), CITIES.map((cc) => [cc, NAME[cc], CC(cc)]), 'xtemp', drawT);
      lineChart($('#x-temp'), { label: 'Movements by temperature band', xType: 'linear', zero: false, xFmt: (t) => `${t}°`, yFmt: F.i, unit: ' %', series: tr.filter((s) => !hid.has(s.cc)).map((s) => ({ name: NAME[s.cc], color: CC(s.cc), width: s.cc === c ? 2.6 : 1.4, values: s.values })) });
    };
    drawT();
    const wb = (w) => (w < 2 ? '0–2' : w < 3 ? '2–3' : w < 4 ? '3–4' : w < 5 ? '4–5' : w < 6 ? '5–6' : '6 +');
    const wbands = ['0–2', '2–3', '3–4', '4–5', '5–6', '6 +'];
    const wn = d3.rollup(X.filter((d) => d.no2 != null), (v) => (v.length >= 5 ? d3.mean(v, (d) => d.no2) : null), (d) => d.city, (d) => wb(d.wind));
    barChart($('#x-wind'), { groups: wbands, keys: CITIES, get: (g, k) => wn.get(k)?.get(g), color: (k) => CC(k), keyFmt: (k) => NAME[k], groupFmt: (g) => `${g} m/s`, fmt: F.d, unit: ' µg/m³', highlight: (k) => k === c, label: 'NO₂ by wind band', labels: false });
    const wkn = d3.rollup(X.filter((d) => d.no2 != null), (v) => d3.mean(v, (d) => d.no2), (d) => d.city, (d) => (d.weekend ? 'Weekend' : 'Weekday'));
    barChart($('#x-weekend'), { groups: CITIES, keys: ['Weekday', 'Weekend'], get: (g, k) => wkn.get(g)?.get(k), color: (k) => (k === 'Weekday' ? 'var(--paris)' : 'var(--france)'), groupFmt: (g) => NAME[g], fmt: F.d, unit: ' µg/m³', highlight: (k, g) => g === c, label: 'NO₂ on weekdays and weekends' });
    const calm = wn.get(c)?.get('0–2'), windy = wn.get(c)?.get('5–6') ?? wn.get(c)?.get('6 +');
    const ww = wkn.get(c);
    const cr = X.filter((d) => d.city === c);
    const pmRain = d3.mean(cr.filter((d) => d.pm25 != null && d.rain >= 1), (d) => d.pm25), pmDry = d3.mean(cr.filter((d) => d.pm25 != null && d.rain < 1), (d) => d.pm25);
    if (calm && windy && ww) setHTML($('#x-a2'), ['Yes. In ', [NAME[c]], ', NO₂ averages ', [`${F.d(calm)} µg/m³`], ' on calm days and ', [`${F.d(windy)} µg/m³`], ' on windy days. Weekends have ', [`${F.i((1 - ww.get('Weekend') / ww.get('Weekday')) * 100)} % less NO₂`], ' than weekdays. PM2.5 averages ', [`${F.d(pmRain)} µg/m³`], ' on rainy days against ', [F.d(pmDry)], ' on dry days.']);
    const keys = Object.keys(VARS);
    ['x', 'y'].forEach((a) => { const s = $(`#x-${a}sel`); if (!s.options.length) { keys.forEach((k) => s.append(new Option(VARS[k][0], k))); s.addEventListener('change', () => { xsel[a] = s.value; drawExplorer(); }); } s.value = xsel[a]; });
    function drawExplorer() {
      scatter($('#x-scatter'), { rows: cr, x: (d) => d[xsel.x], y: (d) => d[xsel.y], color: (d) => (d.rain >= 1 ? 'var(--rain)' : 'var(--dry)'), xFmt: VARS[xsel.x][1], yFmt: VARS[xsel.y][1], trend: true, label: `${VARS[xsel.y][0]} vs ${VARS[xsel.x][0]}`,
        tipf: (d) => [fdate(pdate(d.day)) + (d.weekend ? ' · weekend' : ''), `${VARS[xsel.x][0]}: ${VARS[xsel.x][1](d[xsel.x])}${VARS[xsel.x][2]}`, `${VARS[xsel.y][0]}: ${VARS[xsel.y][1](d[xsel.y])}${VARS[xsel.y][2]}`] });
      const el = $('#x-corr');
      const w0 = Math.max(260, el.clientWidth), lab = 104, n = keys.length, cs = Math.min(56, (w0 - lab) / n), h = 22 + n * cs + 10;
      const { svg } = frame(el, h, 'Correlation table');
      keys.forEach((k, i) => {
        svg.append('text').attr('class', 'lbl-muted').attr('x', lab - 6).attr('y', 22 + i * cs + cs / 2 + 4).attr('text-anchor', 'end').text(VARS[k][0]);
        svg.append('text').attr('class', 'lbl-muted').attr('x', lab + i * cs + cs / 2).attr('y', 12).attr('text-anchor', 'middle').text(VARS[k][0].replace('Bike movements', 'Bikes').replace('Temperature', 'Temp.'));
      });
      keys.forEach((a, i) => keys.forEach((b, j) => {
        const r = a === b ? 1 : pearson(cr, a, b);
        const g = svg.append('g').attr('transform', `translate(${lab + j * cs},${22 + i * cs})`).style('cursor', a === b ? 'default' : 'pointer');
        g.append('rect').attr('width', cs - 2).attr('height', cs - 2).attr('rx', 4).style('fill', r == null ? 'var(--soft)' : d3.interpolateRdBu(0.5 - r / 2)).style('stroke', xsel.x === b && xsel.y === a ? 'var(--fg)' : 'none').attr('stroke-width', 2);
        g.append('text').attr('x', cs / 2 - 1).attr('y', cs / 2 + 3).attr('text-anchor', 'middle').style('font-size', '11px').style('font-family', 'var(--font-num)').style('fill', r != null && Math.abs(r) > 0.55 ? '#fff' : '#1b1f24').text(r == null ? '–' : r.toFixed(2));
        g.on('pointermove', (ev) => showTip(ev, [`${VARS[a][0]} × ${VARS[b][0]}`, r == null ? 'Not enough days' : `r = ${r.toFixed(2)}`])).on('pointerleave', hideTip)
          .on('click', () => { if (a === b) return; xsel.x = b; xsel.y = a; $('#x-xsel').value = b; $('#x-ysel').value = a; drawExplorer(); });
      }));
    }
    drawExplorer();
    const rT = pearson(cr, 'tmean', 'moves'), rR = pearson(cr, 'rain', 'moves'), rW = pearson(cr, 'wind', 'no2');
    setHTML($('#x-a3'), [`In ${NAME[c]}, bike use goes up with temperature (r = `, [rT == null ? '–' : rT.toFixed(2)], ') and down with rain (r = ', [rR == null ? '–' : rR.toFixed(2)], '). NO₂ falls when the wind picks up (r = ', [rW == null ? '–' : rW.toFixed(2)], '). Pick any two measures, or click a cell.']);
  }

  // ---------------- DATA ----------------
  async function renderData(tk) {
    const M = state.meta;
    metaInto($('#d-meta'), [['Database', M.backend === 'postgres' ? 'PostgreSQL (+ lake for the clean tables)' : 'Data Lake · DuckDB on Parquet'], ['Weather table', M.weather_source], ['Rows in total', F.i(d3.sum(M.tables, (t) => t.rows))]]);
    $('#d-tables').replaceChildren(...M.tables.map((t) => {
      const b = document.createElement('button'); b.type = 'button'; b.className = 'tbl'; b.setAttribute('aria-pressed', String(t.name === state.table));
      const n = document.createElement('b'); n.textContent = t.name;
      const r = document.createElement('div'); r.className = 'n'; r.textContent = F.i(t.rows);
      const d = document.createElement('div'); d.className = 'note'; d.textContent = `${t.description} · ${t.first} → ${t.last}`;
      const cols = document.createElement('div'); cols.className = 'cols'; cols.textContent = t.columns.map((c2) => `${c2.name} ${c2.type.toLowerCase()}`).join(' · ');
      b.append(n, r, d, cols);
      b.addEventListener('click', () => { state.table = t.name; renderData(++state.token); });
      return b; }));
    const pv = await api(`table/${state.table}`, { limit: 20 });
    if (tk !== state.token) return;
    $('#d-prev-title').textContent = `Latest rows · ${state.table}`;
    const cols = pv.rows.length ? Object.keys(pv.rows[0]) : [];
    const tr = document.createElement('tr'); cols.forEach((c2) => { const th = document.createElement('th'); th.textContent = c2; tr.append(th); });
    $('#d-prev-head').replaceChildren(tr);
    rowsInto($('#d-prev-body'), pv.rows, cols.map((c2) => [(r) => (r[c2] == null ? '' : typeof r[c2] === 'number' ? (Number.isInteger(r[c2]) ? String(r[c2]) : r[c2].toFixed(3)) : String(r[c2]))]));
    sql('d-prev-sql', pv);
  }

  // ---------------- router + filters ----------------
  const PAGES = ['overview', 'weather', 'mobility', 'pollution', 'cross', 'data'];
  const RENDER = { overview: renderOverview, weather: renderWeather, mobility: renderMobility, pollution: renderPollution, cross: renderCross, data: renderData };
  const QS = { weather: ['Abnormal periods', 'Visibility', 'Seasons', 'Extreme days'], mobility: ['Most used', 'Low supply', 'Daily peaks', 'Imbalance', 'Rebalancing', 'Networks', 'Trips'], pollution: ['Polluted areas', 'Daily rhythm', 'Main pollutants', 'Episodes'], cross: ['Rain & bikes', 'Weather & air', 'Explorer'] };
  const QP = { weather: 'wq', mobility: 'mq', pollution: 'pq', cross: 'xq' };
  document.querySelectorAll('.qnav').forEach((nav) => {
    const p = nav.dataset.for;
    QS[p].forEach((t, i) => { const b = document.createElement('button'); b.type = 'button'; const n = document.createElement('b'); n.textContent = i < 5 || p !== 'mobility' ? `Q${i + 1}` : '+'; b.append(n, document.createTextNode(t));
      b.addEventListener('click', () => document.getElementById(`${QP[p]}${i + 1}`).scrollIntoView({ behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block: 'start' })); nav.append(b); });
  });
  async function render() {
    if (!state.meta) return;
    const tk = ++state.token;
    const page = document.getElementById(`page-${state.page}`);
    document.querySelectorAll('.chart.loading').forEach((c) => c.classList.remove('loading'));   // from a render that was replaced
    page.querySelectorAll('.chart').forEach((c) => c.classList.add('loading'));
    try { await RENDER[state.page](tk); }
    catch (e) { console.error(e); if (tk === state.token) { live('err', `Query failed: ${e.message}`); } }
    finally { if (tk === state.token) page.querySelectorAll('.chart').forEach((c) => c.classList.remove('loading')); }
  }
  function route() {
    const h = (location.hash || '').slice(1);
    const p = PAGES.includes(h) ? h : 'overview';
    const changed = p !== state.page;
    state.page = p;
    PAGES.forEach((q) => { $(`#page-${q}`).hidden = !state.meta || q !== p; });
    document.querySelectorAll('.nav a').forEach((a) => (a.getAttribute('href') === `#${p}` ? a.setAttribute('aria-current', 'page') : a.removeAttribute('aria-current')));
    document.documentElement.style.setProperty('--accent', `var(--${['overview', 'data'].includes(p) ? 'cross' : p})`);
    render();
    if (changed) window.scrollTo(0, 0);
  }
  window.addEventListener('hashchange', route);
  document.querySelectorAll('#citySeg button').forEach((b) => b.addEventListener('click', () => {
    state.city = b.dataset.city; store.set('uh-city', state.city);
    document.querySelectorAll('#citySeg button').forEach((x) => x.setAttribute('aria-pressed', String(x === b)));
    $('#drawer').hidden = true; render();
  }));
  document.querySelectorAll('#citySeg button').forEach((x) => x.setAttribute('aria-pressed', String(x.dataset.city === state.city)));
  function setPeriod(start, end, preset) {
    state.start = start; state.end = end;
    $('#dStart').value = start; $('#dEnd').value = end || $('#dEnd').max || '';
    document.querySelectorAll('#presets button').forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.p === preset)));
    render();
  }
  document.querySelectorAll('#presets button').forEach((b) => b.addEventListener('click', () => {
    const p = b.dataset.p, today = new Date().toISOString().slice(0, 10);
    if (p === 'all') setPeriod('2024-01-01', '', p);
    else if (p === '90') setPeriod(new Date(Date.now() - 90 * 864e5).toISOString().slice(0, 10), '', p);
    else setPeriod(`${p}-01-01`, `${p}-12-31` < today ? `${p}-12-31` : '', p);
  }));
  ['#dStart', '#dEnd'].forEach((s) => $(s).addEventListener('change', (ev) => {
    const el = ev.target;
    if (!(el.value && el.validity.valid)) return;
    setPeriod($('#dStart').value, $('#dEnd').value === $('#dEnd').max ? '' : $('#dEnd').value, null);
  }));
  $('#refresh').addEventListener('click', async () => { cache.clear(); state.meta = await api('meta'); render(); });
  // live controls
  const bindRange = (id, out) => { const el = $(id); el.addEventListener('input', () => { $(out).textContent = el.value; }); el.addEventListener('change', render); };
  bindRange('#w-z', '#w-z-o'); bindRange('#w-min', '#w-min-o'); bindRange('#p-z', '#p-z-o'); bindRange('#p-win', '#p-win-o');
  ['#w-heat', '#w-cold', '#w-rain', '#w-wind', '#m-map-metric', '#p-map-pol', '#p-hour-pol', '#p-ep-pol', '#t-top'].forEach((s) => $(s).addEventListener('change', render));
  document.querySelectorAll('#m-crit-table th[data-k]').forEach((th) => { th.style.cursor = 'pointer'; th.addEventListener('click', () => { crit.dir = crit.k === th.dataset.k ? -crit.dir : -1; crit.k = th.dataset.k; renderCrit(); }); });
  $('#drawerClose').addEventListener('click', () => { $('#drawer').hidden = true; });
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') $('#drawer').hidden = true; });
  let rt; window.addEventListener('resize', () => { clearTimeout(rt); rt = setTimeout(render, 200); });

  api('meta').then((m) => {
    state.meta = m;
    const last = d3.max(m.tables, (t) => t.last).slice(0, 10);
    ['#dStart', '#dEnd'].forEach((s) => { $(s).max = last; });
    $('#dStart').value = state.start; $('#dEnd').value = last;
    document.querySelector('#presets [data-p="all"]').setAttribute('aria-pressed', 'true');
    $('#loading').hidden = true;
    route();
  }).catch((e) => { $('#loading').textContent = `Could not reach the UrbanHub API (${e.message}). Start it with: .venv/bin/python -m dashboard.webapp`; live('err', 'API not reachable'); });
})();
