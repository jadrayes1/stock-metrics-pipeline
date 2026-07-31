// scripts/generateSectorMetrics.js
//
// Offline data-generation step for the sector-percentile feature. Fetches
// fundamentals + industry classification for every NASDAQ/NYSE/NYSE American
// common stock and REIT from Finnhub, and writes the result to
// src/data/marketMetrics.json, which the app reads directly at runtime — no
// live Finnhub calls, no waiting, no rate-limit exposure for the person
// using the app.
//
// Universe: Finnhub's full US symbol list (`/stock/symbol?exchange=US`),
// filtered to primary listings on NASDAQ/NYSE/NYSE American (mic codes
// XNAS/XNYS/XASE) with type "Common Stock" or "REIT". This deliberately
// excludes ~13,500 OTC/foreign-listed entries (type-tagged the same way but
// on OTC markets — thin, often duplicate foreign listings, not what a US
// sector-comparison feature wants as peers) and non-operating-company types
// (ETPs, ADRs, closed-end funds, units, rights, MLPs, trusts, etc. — these
// don't have ROIC/margin/growth fundamentals in the same sense). As of this
// writing that's ~5,145 tickers (~4,959 Common Stock + ~186 REIT).
//
// Classification is Finnhub's own `finnhubIndustry` (from `/stock/profile2`),
// not GICS sector — there's no free GICS mapping covering this many tickers
// the way there was for the S&P 500 alone. This is more granular than GICS
// sector (e.g. "Airlines" rather than "Industrials"), so peer groups for
// narrow industries can be small; that's an inherent tradeoff of broader
// coverage, not a bug.
//
// This is a plain Node script (not part of the Expo/React Native bundle —
// nothing under src/ imports it), since it needs `fs` and a longer-running
// rate-limited loop that has no place in the app's render path.
//
// Runs in a separate PUBLIC repo (jadrayes1/stock-metrics-pipeline) on a
// schedule — see that repo's .github/workflows/generate-sector-metrics.yml.
// It's public (unlike the app's private repo) specifically so this ~2-3hr
// job doesn't burn paid GitHub Actions minutes. This script itself has no
// dependency on which repo it runs in, though: `npm run
// generate-sector-metrics` still works locally too, at the same one-time
// ~2-3hr cost (two Finnhub calls per ticker, rate-limited to stay under the
// free-tier 60/min cap).

const fs = require('fs');
const path = require('path');

const CONFIG_FILE = path.join(__dirname, '../src/config.js');
const OUTPUT_FILE = path.join(__dirname, '../src/data/marketMetrics.json');
const REQUEST_SPACING_MS = 1100; // ~54/min, under Finnhub's 60/min free-tier cap

const ALLOWED_MICS = new Set(['XNAS', 'XNYS', 'XASE']); // NASDAQ, NYSE, NYSE American
const ALLOWED_TYPES = new Set(['Common Stock', 'REIT']);

function readFinnhubApiKey() {
  if (process.env.FINNHUB_API_KEY) return process.env.FINNHUB_API_KEY;
  const configSource = fs.readFileSync(CONFIG_FILE, 'utf8');
  const match = configSource.match(/FINNHUB_API_KEY\s*=\s*['"]([^'"]+)['"]/);
  if (!match) {
    throw new Error('Could not find FINNHUB_API_KEY in src/config.js, and FINNHUB_API_KEY env var is not set.');
  }
  return match[1];
}

async function fetchUniverse(apiKey) {
  const res = await fetch(`https://finnhub.io/api/v1/stock/symbol?exchange=US&token=${apiKey}`);
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching symbol universe`);
  const all = await res.json();
  return all
    .filter((s) => ALLOWED_MICS.has(s.mic) && ALLOWED_TYPES.has(s.type))
    .map((s) => s.symbol)
    .sort();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function latestQuarterly(series, field) {
  const arr = series?.[field];
  return Array.isArray(arr) && arr.length > 0 ? arr[0].v : null;
}

// Mirrors extractFinnhubMetricValues in src/utils/metrics.js — keep these
// two in sync if that mapping ever changes (see the field-name notes there
// for why current/quarterly don't share names or units).
function extractMetricValues(current, quarterly) {
  const roic = latestQuarterly(quarterly, 'roicTTM') ?? (current.roiTTM != null ? current.roiTTM / 100 : null);
  const revenueGrowth = current.revenueGrowthTTMYoy != null ? current.revenueGrowthTTMYoy / 100 : null;
  const profitMargin = current.netProfitMarginTTM != null ? current.netProfitMarginTTM / 100 : null;
  const fcfMargin = latestQuarterly(quarterly, 'fcfMargin');
  const peRatio = current.peTTM ?? null;
  const pfcfRatio = current.pfcfShareTTM ?? null;
  return { roic, revenueGrowth, profitMargin, fcfMargin, peRatio, pfcfRatio };
}

async function fetchMetricsFor(symbol, apiKey) {
  const res = await fetch(`https://finnhub.io/api/v1/stock/metric?symbol=${symbol}&metric=all&token=${apiKey}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return { current: data?.metric || {}, quarterly: data?.series?.quarterly || {} };
}

async function fetchIndustryFor(symbol, apiKey) {
  const res = await fetch(`https://finnhub.io/api/v1/stock/profile2?symbol=${symbol}&token=${apiKey}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return data?.finnhubIndustry || null;
}

async function main() {
  const apiKey = readFinnhubApiKey();
  const symbols = await fetchUniverse(apiKey);
  const totalRequests = symbols.length * 2; // industry + metrics, per ticker
  console.log(
    `Fetching industry + fundamentals for ${symbols.length} tickers ` +
      `(${totalRequests} requests, ~${Math.round((totalRequests * REQUEST_SPACING_MS) / 60000)} min)...`
  );

  const metrics = {};
  let ok = 0;
  let failed = 0;

  for (let i = 0; i < symbols.length; i++) {
    const symbol = symbols[i];
    try {
      const industry = await fetchIndustryFor(symbol, apiKey);
      await sleep(REQUEST_SPACING_MS);
      const { current, quarterly } = await fetchMetricsFor(symbol, apiKey);
      metrics[symbol] = { industry, ...extractMetricValues(current, quarterly) };
      ok++;
    } catch (err) {
      failed++;
      console.log(`  skip ${symbol}: ${err.message}`);
    }

    if ((i + 1) % 100 === 0 || i === symbols.length - 1) {
      console.log(`  ${i + 1}/${symbols.length} (${ok} ok, ${failed} failed)`);
    }

    if (i < symbols.length - 1) await sleep(REQUEST_SPACING_MS);
  }

  const output = { generatedAt: new Date().toISOString(), metrics };
  fs.mkdirSync(path.dirname(OUTPUT_FILE), { recursive: true }); // works regardless of which repo this script runs in
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2));
  console.log(`\nWrote ${ok} tickers to ${path.relative(process.cwd(), OUTPUT_FILE)} (${failed} skipped).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
