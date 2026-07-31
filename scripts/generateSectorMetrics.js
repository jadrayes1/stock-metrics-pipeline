// scripts/generateSectorMetrics.js
//
// Offline data-generation step for the sector-percentile and Industry
// Leaders features. Fetches fundamentals + industry classification for
// every NASDAQ/NYSE/NYSE American common stock and REIT from Finnhub, and
// writes the result to src/data/marketMetrics.json, which the app reads
// directly at runtime — no live Finnhub calls, no waiting, no rate-limit
// exposure for the person using the app.
//
// Also computes `industryLeaders`: the single top-composite-score ticker in
// each industry with at least MIN_INDUSTRY_PEERS peers (see
// computeIndustryLeaders below) — precomputed here rather than on-device so
// the Home screen's carousel is just reading a small pre-baked list, same
// "compute once daily, app just reads" philosophy as everything else this
// pipeline produces.
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
// the way there was for the S&P 500 alone. Verified live against the full
// universe: granularity is inconsistent, not uniformly finer than GICS
// sector as you might expect — some values are genuinely narrow ("Airlines",
// 17 tickers) while others are broad catch-alls that read like sector names
// ("Technology", 358; "Health Care", 304; "Financial Services", 301). Only
// 46 distinct values across all ~5,145 tickers. Finnhub also returns the
// literal string "N/A" for some tickers instead of omitting the field —
// treated as "no industry" (see fetchProfileFor), not a real bucket.
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

async function fetchProfileFor(symbol, apiKey) {
  const res = await fetch(`https://finnhub.io/api/v1/stock/profile2?symbol=${symbol}&token=${apiKey}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  // Verified live: Finnhub returns the literal string "N/A" for some tickers
  // (SPACs, holding companies, etc.) instead of omitting the field — that's
  // truthy in JS, so without this check those ~271 tickers (out of ~5,145)
  // silently got grouped into a meaningless "N/A" industry, once even
  // producing a nonsensical "industry leader."
  const rawIndustry = data?.finnhubIndustry;
  const industry = rawIndustry && rawIndustry.trim().toUpperCase() !== 'N/A' ? rawIndustry : null;
  return { industry, name: data?.name || null, logo: data?.logo || null };
}

const COMPARABLE_KEYS = ['roic', 'revenueGrowth', 'profitMargin', 'fcfMargin', 'peRatio', 'pfcfRatio'];
const LOWER_IS_BETTER = new Set(['peRatio', 'pfcfRatio']); // mirrors METRIC_DEFS.betterWhen in src/utils/metrics.js
const MIN_INDUSTRY_PEERS = 100; // narrower industries are excluded — a "top pick" out of a handful of peers isn't statistically meaningful

// Mirrors the percentile-rank + goodness logic in src/utils/metrics.js
// (percentileRank, percentileTone) — keep in sync if that ever changes.
function percentileRank(value, population) {
  const valid = population.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  if (value === null || value === undefined || Number.isNaN(value) || valid.length === 0) return null;
  const below = valid.filter((v) => v < value).length;
  const tied = valid.filter((v) => v === value).length;
  return ((below + 0.5 * tied) / valid.length) * 100;
}

/**
 * One "leader" per industry (>= MIN_INDUSTRY_PEERS peers only): the ticker
 * with the highest composite score, where each of the 6 comparable metrics
 * is percentile-ranked within the industry, direction-normalized ("goodness"
 * — a low P/E percentile-ranks as high goodness, same as percentileTone),
 * then averaged. A ticker missing some metrics is scored on whichever it has.
 */
function computeIndustryLeaders(metrics, profiles) {
  const byIndustry = {};
  for (const [symbol, data] of Object.entries(metrics)) {
    if (!data.industry) continue;
    (byIndustry[data.industry] ||= []).push(symbol);
  }

  const leaders = [];
  for (const [industry, symbols] of Object.entries(byIndustry)) {
    if (symbols.length < MIN_INDUSTRY_PEERS) continue;

    const populations = {};
    for (const key of COMPARABLE_KEYS) {
      populations[key] = symbols.map((s) => metrics[s][key]);
    }

    let best = null;
    for (const symbol of symbols) {
      const data = metrics[symbol];
      const goodnessScores = [];
      for (const key of COMPARABLE_KEYS) {
        const pct = percentileRank(data[key], populations[key]);
        if (pct === null) continue;
        goodnessScores.push(LOWER_IS_BETTER.has(key) ? 100 - pct : pct);
      }
      if (goodnessScores.length === 0) continue;
      const composite = goodnessScores.reduce((a, b) => a + b, 0) / goodnessScores.length;
      if (!best || composite > best.composite) best = { symbol, composite };
    }

    if (best) {
      leaders.push({
        symbol: best.symbol,
        name: profiles[best.symbol]?.name || best.symbol,
        logo: profiles[best.symbol]?.logo || null,
        industry,
        composite: best.composite,
        peerCount: symbols.length,
      });
    }
  }

  return leaders.sort((a, b) => b.peerCount - a.peerCount);
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
  const profiles = {}; // name/logo, kept out of `metrics` to avoid bloating that map — only industryLeaders needs them
  let ok = 0;
  let failed = 0;

  for (let i = 0; i < symbols.length; i++) {
    const symbol = symbols[i];
    try {
      const profile = await fetchProfileFor(symbol, apiKey);
      profiles[symbol] = profile;
      await sleep(REQUEST_SPACING_MS);
      const { current, quarterly } = await fetchMetricsFor(symbol, apiKey);
      metrics[symbol] = { industry: profile.industry, ...extractMetricValues(current, quarterly) };
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

  const industryLeaders = computeIndustryLeaders(metrics, profiles);
  console.log(`\nComputed ${industryLeaders.length} industry leaders (industries with >= ${MIN_INDUSTRY_PEERS} peers).`);

  const output = { generatedAt: new Date().toISOString(), metrics, industryLeaders };
  fs.mkdirSync(path.dirname(OUTPUT_FILE), { recursive: true }); // works regardless of which repo this script runs in
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2));
  console.log(`Wrote ${ok} tickers to ${path.relative(process.cwd(), OUTPUT_FILE)} (${failed} skipped).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
