// History for a recent SPIN-OFF, read from the carve-out financial
// statements in its spin-off information statement.
//
// A spin-off lists with almost nothing in Finnhub or SEC XBRL -- verified
// live: Honeywell Aerospace (HONA, CIK 2089271) began trading in June 2026
// and SEC holds exactly one XBRL report for it (the Q2 2026 10-Q), yet its
// Form 10 information statement (final version filed as a 424B3) carries
// audited combined statements of operations, balance sheets and cash flows
// for FY2023-2025 plus Q1 2026/Q1 2025, in plain HTML tables. This turns
// those into report entries shaped exactly like buildSecSyntheticReports'
// output, so every existing builder consumes them unchanged and
// mergeSyntheticReports only ever fills periods the real feed lacks.
//
// It also reads the 10-Q's own prior-year comparatives (e.g. H1 2025 in
// the Q2 2026 10-Q) by their actual dates. SEC stamps every fact in a
// filing with that filing's fy/fp, so the generic SEC path files a 2025
// comparative under "FY2026 Q2" and drops it.
//
// SCOPE -- applies to nothing but recent spin-offs (see isRecentSpinoff):
// the caller only asks for a ticker whose own reports are sparse, and this
// then requires a Form 10-12B registration in its SEC history (the form a
// spin-off registers its shares with) and statements titled "Combined"
// (carve-out) in the information statement. An established company never
// reaches any of it.
//
// Strict by design: a statement is used only with an explicit units
// caption, an exact period header, and the column count matching the
// periods; anything ambiguous is skipped, never guessed.

const SEC_SUBMISSIONS_BASE = 'https://data.sec.gov/submissions';
const SEC_ARCHIVES_BASE = 'https://www.sec.gov/Archives/edgar/data';
const MAX_SPINOFF_AGE_DAYS = 3 * 365; // a spin-off's own history fills in after ~3 years of filings
const MAX_DOCS_TO_SCAN = 3;

const MONTHS = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'];

function decodeEntities(s) {
  return s
    .replace(/&nbsp;|&#160;|&#xa0;/gi, ' ')
    .replace(/&amp;|&#38;/gi, '&')
    .replace(/&#8212;|&mdash;|&#x2014;/gi, '—')
    .replace(/&#8211;|&ndash;|&#x2013;/gi, '–')
    .replace(/&#8217;|&rsquo;|&#x2019;/gi, "'")
    .replace(/&#8220;|&#8221;|&ldquo;|&rdquo;/gi, '"')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)));
}

function textOf(html) {
  return decodeEntities(html.replace(/<br\s*\/?>/gi, ' ').replace(/<[^>]+>/g, ' '))
    .replace(/\s+/g, ' ')
    .trim();
}

// Every <table> with its rows (cells as text) and the text just before it.
function parseTables(html) {
  const tables = [];
  const re = /<table[\s\S]*?<\/table>/gi;
  let m;
  while ((m = re.exec(html))) {
    const rows = [];
    for (const tr of m[0].match(/<tr[\s\S]*?<\/tr>/gi) || []) {
      const cells = (tr.match(/<t[dh][\s\S]*?<\/t[dh]>/gi) || []).map(textOf).filter((c) => c !== '');
      if (cells.length) rows.push(cells);
    }
    const before = textOf(html.slice(Math.max(0, m.index - 2500), m.index)).slice(-400);
    tables.push({ rows, before });
  }
  return tables;
}

function statementKind(before) {
  const tail = before.toUpperCase();
  if (/PRO FORMA/.test(tail)) return null;
  if (!/COMBINED/.test(tail)) return null; // carve-out statements only
  // The heading closest to the table wins.
  const candidates = [
    ['ic', tail.lastIndexOf('STATEMENTS OF OPERATIONS')],
    ['ic', tail.lastIndexOf('STATEMENT OF OPERATIONS')],
    ['ic', tail.lastIndexOf('STATEMENTS OF INCOME')],
    ['ic', tail.lastIndexOf('STATEMENTS OF EARNINGS')],
    ['bs', tail.lastIndexOf('BALANCE SHEET')],
    ['cf', tail.lastIndexOf('STATEMENTS OF CASH FLOWS')],
    ['cf', tail.lastIndexOf('STATEMENT OF CASH FLOWS')],
    ['x', tail.lastIndexOf('COMPREHENSIVE')],
    ['x', tail.lastIndexOf('STATEMENTS OF EQUITY')],
    ['x', tail.lastIndexOf('CHANGES IN EQUITY')],
  ].filter(([, i]) => i >= 0);
  if (!candidates.length) return null;
  const [kind] = candidates.sort((a, b) => b[1] - a[1])[0];
  return kind === 'x' ? null : kind;
}

function scaleOf(before) {
  if (/in millions|dollars in millions|\$ ?millions/i.test(before)) return 1e6;
  if (/in thousands|dollars in thousands|\$ ?thousands/i.test(before)) return 1e3;
  return null; // no explicit units caption: skipped, never assumed
}

function isoDate(monthName, day, year) {
  const mi = MONTHS.indexOf(monthName.toLowerCase());
  if (mi < 0) return null;
  return `${year}-${String(mi + 1).padStart(2, '0')}-${String(Number(day)).padStart(2, '0')}`;
}

// Column periods from the header rows (everything before the first row that
// has a label followed by numbers). Returns [{end, months}] in column order,
// months 0 for a balance sheet.
function parseHeader(rows, kind) {
  const header = [];
  let i = 0;
  for (; i < rows.length; i++) {
    const r = rows[i];
    // A data row: a text label (not a date), then cells that all parse as
    // numbers. A date row like "March 28,2026 | March 29,2025" is header.
    const looksLikeDate = /^[A-Za-z]+\s*\d{1,2},?\s*(19|20)\d{2}$/.test(r[0]);
    const values = r.length > 1 ? rowValues(r) : null;
    if (/[a-z]/i.test(r[0]) && !looksLikeDate && values && values.length && values.some((v) => !(v >= 1900 && v <= 2100))) break;
    header.push(r.join(' '));
  }
  const text = header.join(' ');
  let months = 0;
  if (kind !== 'bs') {
    if (/years? ended/i.test(text)) months = 12;
    else if (/nine months ended/i.test(text)) months = 9;
    else if (/six months ended/i.test(text)) months = 6;
    else if (/three months ended/i.test(text)) months = 3;
    else return null;
    if ((text.match(/months ended|years? ended/gi) || []).length > 1) return null; // mixed period groups: skipped
  }
  const full = [...text.matchAll(/([A-Za-z]+)\s*(\d{1,2}),\s*((?:19|20)\d{2})/g)].map((d) => isoDate(d[1], d[2], d[3])).filter(Boolean);
  if (full.length >= 2) return { periods: full.map((end) => ({ end, months })), dataStart: i };
  // "Years Ended December 31," over a bare year row
  const md = /(?:ended|as of)?\s*([A-Za-z]+)\s+(\d{1,2}),?/i.exec(text.replace(/years? ended|months ended/i, ''));
  const years = [...text.matchAll(/\b((?:19|20)\d{2})\b/g)].map((y) => y[1]);
  if (!md || !MONTHS.includes(md[1].toLowerCase()) || years.length < 2) return null;
  return { periods: years.map((y) => ({ end: isoDate(md[1], md[2], y), months })), dataStart: i };
}

// The numbers in a data row, merging "(504" + ")" and dropping "$" cells.
function rowValues(cells) {
  const out = [];
  for (const raw of cells.slice(1)) {
    const c = raw.trim();
    if (c === '$' || c === ')' || c === '%') continue;
    if (/^[—–-]+$/.test(c)) {
      out.push(0);
      continue;
    }
    const m = /^\(?\$?\s*([\d,]+(?:\.\d+)?)\s*\)?$/.exec(c);
    if (!m) return null;
    const v = Number(m[1].replace(/,/g, ''));
    out.push(c.startsWith('(') ? -v : v);
  }
  return out;
}

const ROW_RULES = {
  ic: [
    ['revenue', /^(total )?net (sales|revenues?)$|^total (net )?(sales|revenues?)$|^(net )?revenues?$|^net sales$/],
    ['operatingIncome', /^(operating income|income from operations)( \(loss\))?$/],
    ['pretax', /^(income|earnings)( \(loss\))? before (income )?tax(es)?$/],
    ['netIncomeAttributable', /^net (income|earnings)( \(loss\))? attributable to (?!non-?controlling)/],
    ['netIncome', /^net (income|earnings)( \(loss\))?$/],
  ],
  cf: [
    ['ocf', /^net cash (provided by|from|\(used (for|in)\) provided by|provided by \(used (for|in)\)) operating activities$/],
    ['capex', /^(capital expenditures|purchases? of property,? plant,? and equipment|expenditures for property,? plant,? and equipment)$/],
  ],
  bs: [
    ['cash', /^cash and cash equivalents$/],
    // "(deficit)" -- verified live: after its pre-spin financing HONA's March
    // 2026 balance sheet reads "Total (deficit) equity attributable to
    // Aerospace" at (5,618).
    ['equityAttributable', /^total (\((deficit|deficiency)\) )?(equity|stockholders'? (\((deficit|deficiency)\) )?equity|shareholders'? (\((deficit|deficiency)\) )?equity)( \((deficit|deficiency)\))? attributable to (?!non-?controlling)/],
    ['equity', /^total (\((deficit|deficiency)\) )?((stockholders'?|shareholders'?) (\((deficit|deficiency)\) )?)?equity( \((deficit|deficiency)\))?$/],
    ['debtLong', /^long-term debt(, net)?( of current portion)?$/],
    ['debtCurrent', /^(current maturities of long-term debt|current portion of long-term debt|short-term borrowings|commercial paper)$/],
  ],
};

function extractStatement(table, kind) {
  const scale = scaleOf(table.before);
  if (!scale) return null;
  const header = parseHeader(table.rows, kind);
  if (!header) return null;
  const n = header.periods.length;
  const found = {};
  for (const r of table.rows.slice(header.dataStart)) {
    const label = r[0].toLowerCase().replace(/[:\s]+$/, '').replace(/\s+/g, ' ');
    const values = r.length > 1 ? rowValues(r) : null;
    if (!values || values.length !== n) continue;
    for (const [key, re] of ROW_RULES[kind]) {
      if (found[key] || !re.test(label)) continue;
      found[key] = values.map((v) => v * scale);
      break;
    }
  }
  return { periods: header.periods, found };
}

const CONCEPTS = {
  revenue: 'us-gaap_Revenues',
  operatingIncome: 'us-gaap_OperatingIncomeLoss',
  pretax: 'us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
  netIncome: 'us-gaap_NetIncomeLoss',
  ocf: 'us-gaap_NetCashProvidedByUsedInOperatingActivities',
  capex: 'us-gaap_PaymentsToAcquirePropertyPlantAndEquipment',
  cash: 'us-gaap_CashAndCashEquivalentsAtCarryingValue',
  equity: 'us-gaap_StockholdersEquity',
  debtLong: 'us-gaap_LongTermDebtNoncurrent',
  debtCurrent: 'us-gaap_LongTermDebtCurrent',
};
const item = (key, value) => ({ concept: CONCEPTS[key], label: `${CONCEPTS[key]} (spin-off carve-out statement)`, value });

// Statements -> {annual: Map(end -> report), interim: Map(end|months -> report)}
function reportsFromDocument(html) {
  const byPeriod = new Map(); // `${end}|${months}` -> {end, months, ic:{}, cf:{}}
  const bsByEnd = new Map();
  for (const table of parseTables(html)) {
    const kind = statementKind(table.before);
    if (!kind) continue;
    const st = extractStatement(table, kind);
    if (!st) continue;
    st.periods.forEach((p, idx) => {
      if (kind === 'bs') {
        const cur = bsByEnd.get(p.end) || {};
        for (const [k, vals] of Object.entries(st.found)) if (cur[k] == null) cur[k] = vals[idx];
        bsByEnd.set(p.end, cur);
        return;
      }
      const key = `${p.end}|${p.months}`;
      const cur = byPeriod.get(key) || { end: p.end, months: p.months, ic: {}, cf: {} };
      for (const [k, vals] of Object.entries(st.found)) if (cur[kind][k] == null) cur[kind][k] = vals[idx];
      byPeriod.set(key, cur);
    });
  }
  return { periods: [...byPeriod.values()], bsByEnd };
}

function plausible(p) {
  const rev = p.ic.revenue;
  const ni = p.ic.netIncomeAttributable ?? p.ic.netIncome;
  if (!(rev > 0) || ni == null) return false;
  return Math.abs(ni) <= rev * 2; // a mis-mapped row or column, not a real year
}

function daysBetween(a, b) {
  return (Date.parse(b) - Date.parse(a)) / 86400000;
}

// The fiscal year a period belongs to, by Finnhub's convention (the year
// the fiscal year ENDS in), from the fiscal year-end month/day seen on the
// annual statements.
function fiscalYearOf(end, fyeMonthDay) {
  const [y, m, d] = end.split('-').map(Number);
  if (!fyeMonthDay) return y;
  const [fm, fd] = fyeMonthDay;
  // After the fiscal year-end (allowing a 52/53-week drift of 7 days), the
  // period belongs to the next fiscal year.
  const afterFye = m > fm || (m === fm && d > fd + 7);
  return afterFye ? y + 1 : y;
}

function toReports(doc, cik) {
  const normalizedCik = cik != null ? String(Number(cik)) : cik;
  const annual = doc.periods.filter((p) => p.months === 12 && plausible(p));
  if (!annual.length) return { quarterlyReports: [], annualReports: [] };
  const fye = annual[0].end.split('-').map(Number).slice(1);
  const build = (p) => {
    const ic = [];
    if (p.ic.revenue != null) ic.push(item('revenue', p.ic.revenue));
    const ni = p.ic.netIncomeAttributable ?? p.ic.netIncome;
    if (ni != null) ic.push(item('netIncome', ni));
    if (p.ic.operatingIncome != null) ic.push(item('operatingIncome', p.ic.operatingIncome));
    if (p.ic.pretax != null) ic.push(item('pretax', p.ic.pretax));
    const cf = [];
    if (p.cf.ocf != null) cf.push(item('ocf', p.cf.ocf));
    if (p.cf.capex != null) cf.push(item('capex', Math.abs(p.cf.capex)));
    const bsVals = doc.bsByEnd.get(p.end);
    const bs = [];
    if (bsVals) {
      const eq = bsVals.equityAttributable ?? bsVals.equity;
      if (eq != null) bs.push(item('equity', eq));
      if (bsVals.cash != null) bs.push(item('cash', bsVals.cash));
      if (bsVals.debtLong != null) bs.push(item('debtLong', bsVals.debtLong));
      if (bsVals.debtCurrent != null) bs.push(item('debtCurrent', bsVals.debtCurrent));
    }
    return { cik: normalizedCik, form: 'SPINOFF-CARVEOUT', endDate: p.end, report: { ic, cf, bs } };
  };
  const annualReports = annual.map((p) => ({ ...build(p), year: fiscalYearOf(p.end, fye) }));
  // Interim statements are year-to-date from the fiscal year start, the same
  // cumulative convention Finnhub's own quarterly reports use.
  const quarterlyReports = doc.periods
    .filter((p) => [3, 6, 9].includes(p.months) && plausible(p))
    .map((p) => ({ ...build(p), year: fiscalYearOf(p.end, fye), quarter: p.months / 3 }));
  return { quarterlyReports, annualReports };
}

// Prior-year comparatives in the company's own 10-Q XBRL, keyed by their
// real (start, end) instead of the filing's fy/fp. Year-to-date durations
// only, the cumulative shape Finnhub's quarterly reports use.
function comparativeReportsFromXbrl(gaapFacts, cik, fyeMonthDay) {
  const normalizedCik = cik != null ? String(Number(cik)) : cik;
  const pick = {
    revenue: ['RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenues', 'SalesRevenueNet'],
    netIncome: ['NetIncomeLoss', 'ProfitLoss'],
    operatingIncome: ['OperatingIncomeLoss'],
    pretax: ['IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest'],
    ocf: ['NetCashProvidedByUsedInOperatingActivities'],
    capex: ['PaymentsToAcquirePropertyPlantAndEquipment'],
  };
  const periods = new Map(); // `${start}|${end}` -> {start, end, ic, cf}
  for (const [key, concepts] of Object.entries(pick)) {
    for (const concept of concepts) {
      for (const f of gaapFacts?.[concept]?.units?.USD || []) {
        if (!f.start || !f.end || f.val == null || !/^10-[QK]/.test(f.form || '')) continue;
        const months = Math.round(daysBetween(f.start, f.end) / 30.44);
        if (![3, 6, 9].includes(months)) continue;
        const k = `${f.start}|${f.end}`;
        const cur = periods.get(k) || { start: f.start, end: f.end, months, ic: {}, cf: {} };
        const section = key === 'ocf' || key === 'capex' ? 'cf' : 'ic';
        if (cur[section][key] == null) cur[section][key] = f.val;
        periods.set(k, cur);
      }
      if ([...periods.values()].some((p) => p.ic[key] != null || p.cf[key] != null)) break; // first concept with data wins
    }
  }
  const out = [];
  for (const p of periods.values()) {
    if (!(p.ic.revenue > 0)) continue;
    // Year-to-date only: it must start at the fiscal year start, i.e. a
    // 3-month period is Q1 and a 6-month one H1. A standalone Q2 (also 3
    // months, but starting in April) is skipped -- decumulation derives it.
    const startsFy = fyeMonthDay ? daysToFyStart(p.start, fyeMonthDay) <= 7 : true;
    if (!startsFy) continue;
    const ic = [item('revenue', p.ic.revenue)];
    if (p.ic.netIncome != null) ic.push(item('netIncome', p.ic.netIncome));
    if (p.ic.operatingIncome != null) ic.push(item('operatingIncome', p.ic.operatingIncome));
    if (p.ic.pretax != null) ic.push(item('pretax', p.ic.pretax));
    const cf = [];
    if (p.cf.ocf != null) cf.push(item('ocf', p.cf.ocf));
    if (p.cf.capex != null) cf.push(item('capex', Math.abs(p.cf.capex)));
    out.push({ cik: normalizedCik, form: 'SPINOFF-XBRL-COMPARATIVE', endDate: p.end, year: fiscalYearOf(p.end, fyeMonthDay), quarter: p.months / 3, report: { ic, cf, bs: [] } });
  }
  return out;
}

function daysToFyStart(start, [fm, fd]) {
  const s = new Date(start + 'T00:00:00Z');
  let best = Infinity;
  for (const y of [s.getUTCFullYear() - 1, s.getUTCFullYear()]) {
    const fyStart = new Date(Date.UTC(y, fm - 1, fd) + 86400000); // the day after a fiscal year-end
    best = Math.min(best, Math.abs((s - fyStart) / 86400000));
  }
  return best;
}

// SEC history check: a Form 10-12B registration, and a first periodic
// report recent enough that the company's own filings can't yet supply
// history. Returns candidate information-statement document URLs.
async function recentSpinoffDocuments(cik, fetchJson) {
  const padded = String(Number(cik)).padStart(10, '0');
  const sub = await fetchJson(`${SEC_SUBMISSIONS_BASE}/CIK${padded}.json`);
  const r = sub?.filings?.recent;
  if (!r?.form) return [];
  const forms = r.form;
  if (!forms.some((f) => f === '10-12B' || f === '10-12B/A')) return [];
  const periodic = forms.map((f, i) => (/^10-[QK]$/.test(f) ? r.filingDate[i] : null)).filter(Boolean).sort();
  if (periodic.length && daysBetween(periodic[0], new Date().toISOString().slice(0, 10)) > MAX_SPINOFF_AGE_DAYS) return [];
  const docs = [];
  for (let i = 0; i < forms.length; i++) {
    if (forms[i] !== '424B3' && forms[i] !== '10-12B/A' && forms[i] !== '10-12B') continue;
    docs.push({ form: forms[i], filed: r.filingDate[i], accession: r.accessionNumber[i], primary: r.primaryDocument[i] });
  }
  // The final information statement (424B3) first, then the latest Form 10
  // amendment -- newest first within each.
  docs.sort((a, b) => (a.form === '424B3') - (b.form === '424B3') || a.filed.localeCompare(b.filed)).reverse();
  return docs.slice(0, MAX_DOCS_TO_SCAN).map((d) => ({ ...d, url: `${SEC_ARCHIVES_BASE}/${Number(cik)}/${d.accession.replace(/-/g, '')}/${d.primary}` }));
}

// Entry point. Returns {quarterlyReports, annualReports} (empty unless the
// ticker is a recent spin-off with a parseable information statement).
async function buildSpinoffReports(cik, gaapFacts, { fetchJson, fetchText, debug = false }) {
  const empty = { quarterlyReports: [], annualReports: [] };
  if (!cik) return empty;
  const docs = await recentSpinoffDocuments(cik, fetchJson);
  if (!docs.length) return empty;
  for (const d of docs) {
    let html;
    try {
      html = await fetchText(d.url);
    } catch (e) {
      if (debug) console.error('DEBUG spinoff fetch failed', d.url, e.message);
      continue;
    }
    // A Form 10 often keeps the information statement in its EX-99.1; the
    // 424B3 is the statement itself. Either way it must be a carve-out.
    if (!html || !/COMBINED\s+STATEMENTS?\s+OF\s+(OPERATIONS|INCOME|EARNINGS)/i.test(html)) continue;
    const reports = toReports(reportsFromDocument(html), cik);
    if (!reports.annualReports.length) continue;
    const fye = reports.annualReports[0].endDate.split('-').map(Number).slice(1);
    reports.quarterlyReports.push(...comparativeReportsFromXbrl(gaapFacts, cik, fye));
    if (debug) {
      console.error('DEBUG spinoff source', d.form, d.filed, d.url);
      for (const r of [...reports.annualReports, ...reports.quarterlyReports]) {
        console.error('DEBUG spinoff report', r.form, r.year, r.quarter ?? 'FY', r.endDate, JSON.stringify(r.report));
      }
    }
    return reports;
  }
  return empty;
}

module.exports = { buildSpinoffReports, _test: { parseTables, statementKind, extractStatement, reportsFromDocument, toReports, comparativeReportsFromXbrl, fiscalYearOf } };
