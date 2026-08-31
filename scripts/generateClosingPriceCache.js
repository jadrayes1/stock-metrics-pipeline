// scripts/generateClosingPriceCache.js
//
// Fetches Finnhub's /quote for every ticker in the pipeline's covered
// universe (read from the already-published marketMetrics.json, so this
// stays in sync with generateSectorMetrics.js's own coverage automatically
// — no separate universe fetch needed) and publishes a small, dedicated
// closingPriceCache.json to the SAME public Gist. Runs on its OWN
// schedule, right after US market close, kept entirely separate from
// generateSectorMetrics.js's own 4-calls-per-ticker budget — adding a 5th
// call there risked pushing that job's already-long runtime past GitHub
// Actions' 6-hour ceiling (a real, previously-hit problem — see that
// workflow's own comments), while a quote-only job like this one is fast
// enough (~1 call/ticker) to run as its own lightweight pass without
// touching that budget at all.
//
// This exists so the app never needs a live per-device Finnhub /quote call
// just to show a ticker's price (see src/api/stockData.js's
// fetchTickerAnalysis) — that doesn't scale past a handful of concurrent
// users on the app's one shared free-tier key. The tradeoff: the price
// shown is as of this job's last run (labeled "as of <date>'s close" in
// the app), not truly live — acceptable for a research app, not a trading
// terminal. Today's %-change stat is dropped entirely for covered tickers
// rather than shown from stale data, since a once-daily snapshot can't
// represent an inherently intraday value honestly.

const fs = require('fs');
const path = require('path');

const OUTPUT_FILE = path.join(__dirname, '../closingPriceCache.json');
const MARKET_METRICS_URL = 'https://gist.githubusercontent.com/jadrayes1/5cd7f459788725521246717b9e164a8e/raw/marketMetrics.json';
const CLOSING_PRICE_CACHE_URL = 'https://gist.githubusercontent.com/jadrayes1/5cd7f459788725521246717b9e164a8e/raw/closingPriceCache.json';

// ~54/min per key, under Finnhub's 60/min free-tier cap — same reasoning
// as generateSectorMetrics.js's own REQUEST_SPACING_MS, just a smaller
// margin since this job is a single call per ticker, not four.
const REQUEST_SPACING_MS = 1100;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readFinnhubApiKeys() {
  const keys = [];
  if (process.env.FINNHUB_API_KEY) keys.push(process.env.FINNHUB_API_KEY);
  if (process.env.FINNHUB_API_KEY_2) keys.push(process.env.FINNHUB_API_KEY_2);
  if (!keys.length) throw new Error('FINNHUB_API_KEY env var is not set.');
  return keys;
}

// Identical retry-on-429 shape to generateSectorMetrics.js's own
// fetchFinnhub — kept as a separate copy since this is a standalone script,
// same reasoning as every other duplicated helper across these scripts.
const FINNHUB_MAX_429_RETRIES = 3;
const FINNHUB_RETRY_BASE_MS = 2000;
async function fetchFinnhub(url) {
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(url);
    if (res.ok || res.status !== 429 || attempt >= FINNHUB_MAX_429_RETRIES) return res;
    const retryAfterHeader = res.headers.get('Retry-After');
    const retryAfterMs = retryAfterHeader && !Number.isNaN(Number(retryAfterHeader)) ? Number(retryAfterHeader) * 1000 : null;
    await sleep(retryAfterMs ?? FINNHUB_RETRY_BASE_MS * 2 ** attempt);
  }
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  return res.json();
}

// Same staleSymbols exclusion as src/api/sectorComparison.js's
// fetchCoveredSymbols — no point spending a call on a formally
// deregistered ticker.
async function fetchCoveredSymbols() {
  const dataset = await fetchJson(MARKET_METRICS_URL);
  const stale = new Set(dataset.staleSymbols || []);
  return Object.keys(dataset.metrics || {})
    .filter((s) => !stale.has(s))
    .sort();
}

async function fetchPreviouslyPublished() {
  try {
    const data = await fetchJson(CLOSING_PRICE_CACHE_URL);
    return data?.prices || {};
  } catch {
    // First-ever run (file doesn't exist yet), or a transient fetch
    // failure — either way, nothing to merge-protect against, not fatal.
    return {};
  }
}

async function fetchQuoteFor(symbol, apiKey) {
  const res = await fetchFinnhub(`https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(symbol)}&token=${apiKey}`);
  if (!res.ok) return null;
  const data = await res.json();
  // Finnhub returns all-zero fields for a symbol it has no real quote for
  // (delisted, illiquid, momentarily untraded) rather than a 404 — treat
  // that the same as "no data" instead of publishing a bogus $0 price.
  if (typeof data?.c !== 'number' || data.c <= 0) return null;
  return { price: data.c, asOfUnix: typeof data.t === 'number' ? data.t : null };
}

async function runWorker(symbols, apiKey, previouslyPublished, results) {
  for (const symbol of symbols) {
    await sleep(REQUEST_SPACING_MS);
    try {
      const quote = await fetchQuoteFor(symbol, apiKey);
      if (quote) {
        results[symbol] = quote;
      } else if (previouslyPublished[symbol]) {
        // Never regress a ticker to nothing over a single transient miss —
        // same "hard-failure fallback" philosophy as
        // generateSectorMetrics.js's main().
        results[symbol] = previouslyPublished[symbol];
      }
    } catch {
      if (previouslyPublished[symbol]) results[symbol] = previouslyPublished[symbol];
    }
  }
}

async function main() {
  const apiKeys = readFinnhubApiKeys();
  let symbols = await fetchCoveredSymbols();

  // Single-symbol debug/backfill mode — same TARGET_SYMBOL pattern used
  // across every other script in this repo.
  if (process.env.TARGET_SYMBOL) {
    const target = process.env.TARGET_SYMBOL.toUpperCase();
    symbols = symbols.filter((s) => s.toUpperCase() === target);
    console.log(`TARGET_SYMBOL=${target} set — restricting this run to: ${symbols.join(', ') || '(not found in universe)'}`);
  }

  console.log(`Fetching closing quotes for ${symbols.length} tickers across ${apiKeys.length} API key(s)...`);

  const previouslyPublished = await fetchPreviouslyPublished();
  console.log(`Loaded ${Object.keys(previouslyPublished).length} previously-published closing prices (merge-protected against a transient miss).`);

  const results = {};
  const workerSymbolSubsets = apiKeys.map(() => []);
  symbols.forEach((symbol, i) => workerSymbolSubsets[i % apiKeys.length].push(symbol));
  await Promise.all(apiKeys.map((key, idx) => runWorker(workerSymbolSubsets[idx], key, previouslyPublished, results)));

  // Preserve every previously-published symbol this run didn't even
  // attempt (e.g. TARGET_SYMBOL debug mode, or a symbol dropped from
  // fetchCoveredSymbols mid-migration) — same never-regress-to-nothing
  // principle as generateSectorMetrics.js's main().
  for (const [symbol, prev] of Object.entries(previouslyPublished)) {
    if (!(symbol in results)) results[symbol] = prev;
  }

  const generatedAt = new Date().toISOString();
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify({ generatedAt, prices: results }));
  console.log(`Wrote ${Object.keys(results).length} closing prices to ${OUTPUT_FILE}.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
