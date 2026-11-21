// scripts/generateEtfProfileCache.js
//
// Pre-fetches ETF Expense Ratio + AUM (net assets) from Alpha Vantage's
// ETF_PROFILE endpoint, published to the same public Gist as
// marketMetrics.json, as etfProfileCache.json. The app reads this FIRST
// (see fetchCachedEtfProfile in stock-analyzer's src/api/etfData.js) before
// ever falling back to a live, on-device Alpha Vantage call.
//
// This exists because Alpha Vantage's free tier is a hard 25 requests/DAY
// (confirmed live against their own rate-limit response — not the 500/day
// figure commonly cited, which was wrong), shared across every user of the
// app through one server-side key (see stock-analyzer-proxy/api/
// alphavantage.js). Without pre-fetching, only the first ~25 distinct ETF
// lookups anyone makes each day would ever get real data; the rest would
// silently show "—" until the quota reset. No other integrated provider
// (Finnhub, FMP, Twelve Data) exposes expense ratio/AUM on a free tier at
// all — see the ETFs feature's own design notes.
//
// Same least-recently-cached-first rotation as generateNewsCache.js, but
// budgeted by REQUEST COUNT (24/run, one under the daily cap for margin —
// same "don't hug the limit" lesson as REQUEST_SPACING_MS elsewhere in this
// repo) rather than wall-clock time, since Alpha Vantage's constraint is a
// daily count, not a per-minute rate. Expense ratio/AUM change on the order
// of months to years, never intraday, so a slow rotation is not a
// correctness problem — the point is to keep growing/refreshing coverage
// over time, not to have same-day freshness.

const fs = require('fs');
const path = require('path');

const OUTPUT_FILE = path.join(__dirname, '../etfProfileCache.json');
const GIST_ETF_PROFILE_URL = 'https://gist.githubusercontent.com/jadrayes1/5cd7f459788725521246717b9e164a8e/raw/etfProfileCache.json';

const MAX_REQUESTS_PER_RUN = 24; // Alpha Vantage's free tier is 25/day — one held back for margin
const REQUEST_SPACING_MS = 1500; // Alpha Vantage's own guidance is "1 request per second" — padded for margin

// Hand-curated, not auto-discovered — there's no free "list every US ETF"
// endpoint on any integrated provider. Covers the app's own curated
// Popular Market/Sector carousels (see POPULAR_MARKET_ETFS/
// POPULAR_SECTOR_ETFS in stock-analyzer's src/api/etfData.js — kept in
// sync manually, same cross-repo duplication precedent as
// DUAL_CLASS_ALIASES) plus a broader set of the most commonly held/
// searched US-listed ETFs across category, so a real user's search is
// very likely to already be covered. Growing this list costs nothing but
// time to fully populate (24/day) — new entries just join the back of the
// least-recently-cached queue below.
const ETF_UNIVERSE = [
  // Broad market / core index
  'SPY', 'VOO', 'IVV', 'VTI', 'QQQ', 'QQQM', 'DIA', 'IWM', 'IWB', 'IJH', 'IJR', 'VUG', 'VTV', 'VB', 'VO', 'MDY',
  // Sector SPDRs
  'XLK', 'XLF', 'XLE', 'XLV', 'XLY', 'XLP', 'XLI', 'XLB', 'XLU', 'XLRE', 'XLC',
  // Sector alternatives (Vanguard/iShares) -- mirrors SECTOR_ETF_GROUPS in
  // stock-analyzer's src/api/etfData.js, kept in sync manually (same
  // cross-repo duplication precedent as DUAL_CLASS_ALIASES)
  'VGT', 'IYW', 'VFH', 'IYF', 'VDE', 'IYE', 'VHT', 'IYH', 'VCR', 'IYC', 'VDC', 'IYK', 'VIS', 'IYJ', 'VAW', 'IYM', 'VPU', 'IDU', 'VOX', 'IYZ',
  // Sector / thematic (semis, software, biotech)
  'SOXX', 'SMH', 'IGV', 'XBI', 'IBB', 'KRE', 'XOP', 'XHB', 'ITB',
  // Bonds
  'AGG', 'BND', 'TLT', 'IEF', 'SHY', 'LQD', 'HYG', 'TIP', 'MUB', 'BNDX', 'BSV', 'BIV', 'BLV', 'SHV',
  // International
  'VEA', 'VWO', 'EFA', 'EEM', 'IEFA', 'IEMG', 'VXUS', 'ACWI', 'FXI', 'EWJ', 'EWZ', 'INDA',
  // Commodities
  'GLD', 'GLDM', 'IAU', 'SLV', 'USO', 'DBC', 'UNG',
  // Real estate
  'VNQ', 'IYR', 'SCHH',
  // Dividend / value / low-vol
  'VYM', 'SCHD', 'DVY', 'VIG', 'NOBL', 'SPLV', 'SPHD', 'HDV',
  // Growth / active thematic
  'ARKK', 'ARKG', 'ARKW', 'ARKQ', 'ARKF',
  // Leveraged / inverse (high volume, frequently searched)
  'TQQQ', 'SQQQ', 'SPXL', 'SPXS', 'SOXL', 'SOXS', 'UVXY', 'SDS', 'SSO', 'SCO', 'RAM', 'KORU',
  // Crypto-adjacent
  'BITO', 'IBIT', 'FBTC', 'GBTC',
  // ESG
  'ESGU', 'ESGV',
  // Preferred / other income
  'PFF', 'JEPI', 'JEPQ', 'SCHY',
];

function readAlphaVantageApiKey() {
  if (process.env.ALPHA_VANTAGE_API_KEY) return process.env.ALPHA_VANTAGE_API_KEY;
  throw new Error('ALPHA_VANTAGE_API_KEY env var is not set.');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  return res.json();
}

/**
 * Mirrors alphaVantage.js's own error handling in the app — Alpha Vantage
 * returns a 200 with a plain informational message instead of a real
 * payload for a bad symbol, an exhausted quota, or a non-ETF symbol, never
 * a distinguishing 4xx.
 */
async function fetchEtfProfile(symbol, apiKey) {
  const res = await fetch(`https://www.alphavantage.co/query?function=ETF_PROFILE&symbol=${symbol}&apikey=${apiKey}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  if (!data || data['Information'] || data['Error Message'] || data['Note']) {
    throw new Error(data?.['Information'] || data?.['Error Message'] || data?.['Note'] || 'No data');
  }
  const expenseRatio = data.net_expense_ratio != null ? parseFloat(data.net_expense_ratio) : null;
  const netAssets = data.net_assets != null ? parseFloat(data.net_assets) : null;
  // Verified live: Alpha Vantage's own `holdings` array is already sorted
  // descending by weight (SPY's #1 is NVDA at ~7.68%) -- .slice(0, 10) is
  // sufficient, no re-sort needed. Renamed from Alpha Vantage's own
  // `description` key to `name` here, at the single point this payload is
  // first consumed, so every downstream consumer matches the rest of the
  // app's own vocabulary (POPULAR_MARKET_ETFS, IndustryLeaders' leaders,
  // etc. all use `name`, never `description`). A non-equity ETF (e.g. a
  // futures-based commodity fund) genuinely has no constituent holdings to
  // disclose -- verified live for UCO: `holdings: []`, not a bug. Stored
  // as [] uniformly (never omitted) so every consumer only needs to check
  // .length, not existence.
  // Alpha Vantage's own `symbol`/`description` fields are sometimes the
  // literal string "n/a" (not null/missing) -- verified live across a
  // 10-ETF, 15-holding sample: `name` alone for PLTR within XLK/IGV (while
  // ARKK/ARKQ/ARKF disclose PLTR's real name fine -- a genuine per-ETF gap
  // in Alpha Vantage's own data, not something wrong with this extraction),
  // and BOTH `symbol` and `name` as "n/a" together for several holdings in
  // EEM/IEMG/ACWI/EWZ (foreign/ADR-heavy funds). `?? null` only catches
  // null/undefined, so a literal "n/a" string passed straight through --
  // for `name` the app rendered it as the holding's name; for `symbol` the
  // existing `.filter((h) => h.symbol)` never caught it either, since "n/a"
  // is truthy, so a genuinely unidentifiable holding still showed up as a
  // dead row (unusable -- nothing to tap or navigate to). Normalizing both
  // here means a real symbol-less holding is now correctly dropped by the
  // same filter it was always meant to pass through, and a name-less one
  // renders with just its real symbol -- ETFHoldings.js already hides the
  // name line entirely for a null name.
  const isAlphaVantageNA = (v) => !v || v.trim().toLowerCase() === 'n/a';
  const holdings = Array.isArray(data.holdings)
    ? data.holdings
        .slice(0, 10)
        .map((h) => ({
          symbol: isAlphaVantageNA(h.symbol) ? null : h.symbol,
          name: isAlphaVantageNA(h.description) ? null : h.description,
          weight: h.weight != null ? parseFloat(h.weight) : null,
        }))
        .filter((h) => h.symbol)
    : [];
  return {
    expenseRatio: Number.isNaN(expenseRatio) ? null : expenseRatio,
    netAssets: Number.isNaN(netAssets) ? null : netAssets,
    holdings,
  };
}

async function fetchPreviouslyPublished() {
  try {
    const data = await fetchJson(GIST_ETF_PROFILE_URL);
    return data?.profiles && typeof data.profiles === 'object' ? data.profiles : {};
  } catch {
    return {}; // first-ever run: no existing cache yet
  }
}

// symbol -> company name: the news pipeline's company profiles first (e.g.
// "Palantir Technologies Inc"), then the ticker search index.
async function fetchHoldingNames() {
  const names = new Map();
  try {
    const index = await fetchJson(GIST_ETF_PROFILE_URL.replace('etfProfileCache.json', 'tickerSearchIndex.json'));
    for (const t of index?.tickers || []) if (t?.symbol && t?.name) names.set(t.symbol, t.name);
  } catch {}
  try {
    const profiles = await fetchJson(GIST_ETF_PROFILE_URL.replace('etfProfileCache.json', 'companyProfiles.json'));
    for (const [symbol, p] of Object.entries(profiles?.profiles || {})) if (p?.name) names.set(symbol, p.name);
  } catch {}
  return names;
}

// ---------------------------------------------------------------------------
// Closed-end funds -- verified live: Destiny Tech100 (DXYZ) is type "Closed-
// End Fund", not "ETP", so the app opened it as a company and showed almost
// nothing, and Alpha Vantage has no profile for it at all (ETF_PROFILE
// returns {}). A registered closed-end fund files a quarterly N-PORT report
// with every position and its weight, plus net assets, so its profile here
// comes from SEC instead. No expense ratio: N-PORT doesn't carry one.
// ---------------------------------------------------------------------------
const SEC_USER_AGENT = 'stock-analyzer-app stock-metrics-pipeline contact:jadrayescpp@gmail.com';
const SEC_REQUEST_SPACING_MS = 130; // SEC's fair-access limit is 10 requests/second
const LISTED_MICS = new Set(['XNYS', 'XNAS', 'XASE', 'ARCX', 'BATS']);
const HOLDINGS_SHOWN = 10;

async function fetchSec(url) {
  const res = await fetch(url, { headers: { 'User-Agent': SEC_USER_AGENT }, signal: AbortSignal.timeout(30000) });
  await sleep(SEC_REQUEST_SPACING_MS);
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  return res;
}

// Exchange-listed closed-end funds with an SEC filer id: [{symbol, cik}].
async function fetchClosedEndFunds() {
  const key = process.env.FINNHUB_API_KEY;
  if (!key) return [];
  const symbols = await fetchJson(`https://finnhub.io/api/v1/stock/symbol?exchange=US&token=${key}`);
  const ciks = new Map(Object.values(await (await fetchSec('https://www.sec.gov/files/company_tickers.json')).json()).map((t) => [t.ticker, t.cik_str]));
  return symbols
    .filter((s) => s.type === 'Closed-End Fund' && LISTED_MICS.has(s.mic) && ciks.has(s.symbol))
    .map((s) => ({ symbol: s.symbol, cik: String(ciks.get(s.symbol)) }));
}

// The company a position is really in. DXYZ holds most of its stakes
// through vehicles named for them -- "Magnitude ANC III, LLC (economic
// exposure to Anthropic PBC Series B Preferred Shares)" -- and SpaceX
// through four of them plus listed SPCX shares, so positions are grouped by
// the company named inside, without its share class.
const SHARE_CLASS = /\s*(?:[-–,]\s*)?(?:\b(?:Series|Class)\s+[A-Z0-9][\w-]*\b.*|\b(?:Common|Preferred|Ordinary)\s+(?:Stock|Shares?)\b.*|\bPreferred\b.*)$/i;
function underlyingName(name) {
  const inner = String(name || '').match(/\((?:economic exposure to|invested in)\s+(.+?)\)\s*$/i);
  return (inner ? inner[1] : String(name || '')).replace(SHARE_CLASS, '').trim();
}
const normalizeCompany = (n) =>
  String(n || '')
    .toLowerCase()
    .replace(/[.,'()&-]/g, ' ')
    .replace(/\b(inc|corp|corporation|co|company|ltd|limited|plc|pbc|llc|lp|nv|n v|bv|b v|na|n a|sa|ag|se|holdings?|group)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

// N-PORT's XML escapes names -- verified live: Gabelli (GAB) holds
// "DEERE &amp; COMPANY".
const decodeXml = (s) => (s == null ? s : s.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&#(\d+);/g, (_, n) => String.fromCharCode(+n)).replace(/&amp;/g, '&'));

function parseNport(xml) {
  const tag = (block, t) => decodeXml((block.match(new RegExp(`<${t}>([^<]*)</${t}>`)) || [])[1]);
  const netAssets = parseFloat(tag(xml, 'netAssets'));
  const positions = xml
    .split('<invstOrSec>')
    .slice(1)
    .map((block) => ({
      name: tag(block, 'name'),
      ticker: (block.match(/<ticker value="([^"]+)"/) || [])[1] || null,
      assetCat: tag(block, 'assetCat'),
      pct: parseFloat(tag(block, 'pctVal')),
    }))
    // "N/A": a position the fund left unnamed (PIM) -- nothing to show.
    .filter((p) => p.name && !/^n\/?a$/i.test(p.name.trim()) && p.pct > 0);
  return { netAssets: Number.isFinite(netAssets) ? netAssets : null, positions };
}

async function fetchClosedEndFundProfile(cik, symbolByName) {
  const sub = await (await fetchSec(`https://data.sec.gov/submissions/CIK${cik.padStart(10, '0')}.json`)).json();
  const r = sub?.filings?.recent;
  let latest = null;
  for (let i = 0; i < (r?.form?.length || 0); i++) {
    if (!/^NPORT-P(\/A)?$/.test(r.form[i])) continue;
    // Newest period; for the same period, the newest filing (an amendment).
    if (!latest || r.reportDate[i] > latest.reportDate || (r.reportDate[i] === latest.reportDate && r.filingDate[i] > latest.filingDate)) {
      latest = { reportDate: r.reportDate[i], filingDate: r.filingDate[i], accession: r.accessionNumber[i] };
    }
  }
  if (!latest) throw new Error('no N-PORT report');
  const xml = await (await fetchSec(`https://www.sec.gov/Archives/edgar/data/${Number(cik)}/${latest.accession.replace(/-/g, '')}/primary_doc.xml`)).text();
  const { netAssets, positions } = parseNport(xml);
  const groups = new Map();
  for (const p of positions) {
    const cash = p.assetCat === 'STIV';
    const name = cash ? 'Cash & money market funds' : underlyingName(p.name);
    const key = cash ? 'cash' : normalizeCompany(name) || name;
    const g = groups.get(key) || { name, symbol: null, weight: 0 };
    g.weight += p.pct / 100;
    if (!cash && !g.symbol && p.ticker && /^[A-Z.]{1,6}$/.test(p.ticker)) g.symbol = p.ticker;
    groups.set(key, g);
  }
  for (const [key, g] of groups) if (!g.symbol && key !== 'cash') g.symbol = symbolByName.get(key) || null;
  const holdings = [...groups.values()]
    .sort((a, b) => b.weight - a.weight)
    .slice(0, HOLDINGS_SHOWN)
    .map((g) => ({ symbol: g.symbol, name: g.name, weight: +g.weight.toFixed(4) }));
  return { expenseRatio: null, netAssets, holdings, fundType: 'closedEnd', holdingsAsOf: latest.reportDate };
}

async function main() {
  const apiKey = readAlphaVantageApiKey();
  const cache = await fetchPreviouslyPublished();

  // Least-recently-cached first (never-cached sorts first, via epoch 0) —
  // reaches full coverage of ETF_UNIVERSE as fast as the daily budget
  // allows, then cycles back around to refresh the oldest entries (which,
  // given how rarely this data changes, is more than frequent enough).
  const priority = ETF_UNIVERSE.map((symbol) => ({
    symbol,
    fetchedAt: cache[symbol]?.fetchedAt ? new Date(cache[symbol].fetchedAt).getTime() : 0,
  }))
    .sort((a, b) => a.fetchedAt - b.fetchedAt)
    .map((x) => x.symbol);

  console.log(`${ETF_UNIVERSE.length} ETFs tracked, ${Object.keys(cache).length} already cached. Processing oldest/never-cached first, up to ${MAX_REQUESTS_PER_RUN} this run...`);

  let processed = 0;
  for (const symbol of priority) {
    if (processed >= MAX_REQUESTS_PER_RUN) {
      console.log(`Request budget (${MAX_REQUESTS_PER_RUN}) reached — stopping for this run.`);
      break;
    }

    try {
      const profile = await fetchEtfProfile(symbol, apiKey);
      cache[symbol] = { fetchedAt: new Date().toISOString(), ...profile };
      console.log(`  ${symbol}: expenseRatio=${profile.expenseRatio}, netAssets=${profile.netAssets}, holdings=${profile.holdings.length}`);
    } catch (err) {
      console.log(`  skip ${symbol}: ${err.message}`);
      // Leave any existing cache entry for this symbol untouched on failure
      // — a stale entry is strictly better than losing it over one bad
      // request (matches generateNewsCache.js's own merge philosophy).
    }

    processed++;
    await sleep(REQUEST_SPACING_MS);
  }

  // A holding the source names "n/a" or nothing gets the company name the
  // other pipelines already publish -- verified live: PLTR has no name in
  // XLK, VGT, IYW and IGV (the source discloses it only in some funds), so
  // the ETFs tab listed it as a bare symbol; HIMS, SPCX, SHOP and BN the
  // same in one fund each. Every cached ETF, not just this run's.
  const names = await fetchHoldingNames();

  // Every exchange-listed closed-end fund, each run -- SEC requests, so no
  // Alpha Vantage budget. A tracked ETF's entry is never touched, and a
  // failed fund keeps its previous entry.
  const symbolByName = new Map();
  for (const [symbol, name] of names) {
    const key = normalizeCompany(name);
    if (!key) continue;
    symbolByName.set(key, symbolByName.has(key) && symbolByName.get(key) !== symbol ? null : symbol); // two companies on one name: no guess
  }
  let funds = [];
  try {
    funds = await fetchClosedEndFunds();
  } catch (err) {
    console.log(`Closed-end fund list unavailable: ${err.message}`);
  }
  let fundsDone = 0;
  for (const { symbol, cik } of funds) {
    if (ETF_UNIVERSE.includes(symbol)) continue;
    try {
      cache[symbol] = { fetchedAt: new Date().toISOString(), ...(await fetchClosedEndFundProfile(cik, symbolByName)) };
      fundsDone++;
    } catch (err) {
      console.log(`  skip closed-end fund ${symbol}: ${err.message}`);
    }
  }
  console.log(`Closed-end funds: ${fundsDone}/${funds.length} profiled from their N-PORT reports.`);

  let named = 0;
  for (const profile of Object.values(cache)) {
    for (const h of profile?.holdings || []) {
      if (h.name || !names.has(h.symbol)) continue;
      h.name = names.get(h.symbol);
      named++;
    }
  }
  if (named) console.log(`Named ${named} holding(s) the source left unnamed.`);

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify({ generatedAt: new Date().toISOString(), profiles: cache }));
  console.log(`Done. Processed ${processed} ETFs this run. Cache now covers ${Object.keys(cache).length} funds (${ETF_UNIVERSE.length} tracked ETFs plus closed-end funds).`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
