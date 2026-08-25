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
  'TQQQ', 'SQQQ', 'SPXL', 'SPXS', 'SOXL', 'SOXS', 'UVXY', 'SDS', 'SSO',
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
  return {
    expenseRatio: Number.isNaN(expenseRatio) ? null : expenseRatio,
    netAssets: Number.isNaN(netAssets) ? null : netAssets,
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
      console.log(`  ${symbol}: expenseRatio=${profile.expenseRatio}, netAssets=${profile.netAssets}`);
    } catch (err) {
      console.log(`  skip ${symbol}: ${err.message}`);
      // Leave any existing cache entry for this symbol untouched on failure
      // — a stale entry is strictly better than losing it over one bad
      // request (matches generateNewsCache.js's own merge philosophy).
    }

    processed++;
    await sleep(REQUEST_SPACING_MS);
  }

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify({ generatedAt: new Date().toISOString(), profiles: cache }));
  console.log(`Done. Processed ${processed} ETFs this run. Cache now covers ${Object.keys(cache).length}/${ETF_UNIVERSE.length} tracked ETFs.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
