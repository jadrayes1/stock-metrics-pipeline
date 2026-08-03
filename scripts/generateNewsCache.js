// scripts/generateNewsCache.js
//
// Offline pre-fetch of relevant news articles for the app's ticker
// universe, published to the same public Gist as marketMetrics.json (see
// scripts/generateSectorMetrics.js) as a second file, newsCache.json. The
// app reads it as a second-tier cache — see fetchNewsCached in
// src/api/stockData.js — before ever falling back to a live, on-device
// Finnhub call.
//
// This is a SEPARATE workflow/script from generateSectorMetrics.js, not a
// step bolted onto it, because Finnhub's /company-news endpoint is far
// slower and far less predictable than anything else this pipeline calls —
// verified live: 5-37+ seconds per ticker, vs. well under a second for
// profile/metric/financials-reported. Doing this for the full ~5,140-ticker
// universe in one run, even at an optimistic 8s/ticker average, would add
// 11+ hours on top of the existing ~4.5-4.8hr sector-metrics run — almost
// certainly blowing GitHub Actions' job time limit and risking that
// PRIMARY pipeline failing too if it were bolted on. Instead, this runs
// within its own fixed TIME_BUDGET_MS per invocation, processing as many
// tickers as it can (least-recently-cached first) and merging the results
// into whatever was already published, rather than requiring a single run
// to cover the whole universe. At roughly 2,000-2,500 tickers per run
// (measured, not assumed — logged at the end of each run), a daily
// schedule fully rotates through the ~5,140-ticker universe every 2-3 days;
// the on-device cache (fetchNewsCached) still covers anything not yet
// reached, or gone stale, with a live fetch.
//
// Runs in the same public repo as generateSectorMetrics.js (see that
// script's header for why public) on its own schedule, offset from the
// main pipeline to avoid both jobs competing for Finnhub's account-wide
// rate limit at once — see .github/workflows/generate-news-cache.yml.

const fs = require('fs');
const path = require('path');

const OUTPUT_FILE = path.join(__dirname, '../newsCache.json');
const GIST_NEWS_URL = 'https://gist.githubusercontent.com/jadrayes1/db6fbd96e980118d3c6a63965dc0dc39/raw/newsCache.json';
const GIST_METRICS_URL = 'https://gist.githubusercontent.com/jadrayes1/db6fbd96e980118d3c6a63965dc0dc39/raw/marketMetrics.json';

const REQUEST_SPACING_MS = 1100; // ~54/min, under Finnhub's 60/min free-tier cap — same budget as generateSectorMetrics.js
const TIME_BUDGET_MS = 5.5 * 60 * 60 * 1000; // leaves headroom under the workflow's 7hr timeout-minutes for setup/publish
const MAX_ARTICLES_PER_TICKER = 5; // mirrors MAX_RELEVANT_ARTICLES in src/api/stockData.js
const NEWS_LOOKBACK_DAYS = 14; // mirrors finnhub.fetchCompanyNews's default in src/api/finnhub.js

function readFinnhubApiKey() {
  if (process.env.FINNHUB_API_KEY) return process.env.FINNHUB_API_KEY;
  throw new Error('FINNHUB_API_KEY env var is not set.');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  return res.json();
}

async function fetchCompanyNews(symbol, apiKey) {
  const to = new Date();
  const from = new Date(to.getTime() - NEWS_LOOKBACK_DAYS * 24 * 60 * 60 * 1000);
  const fmt = (d) => d.toISOString().slice(0, 10);
  const res = await fetch(
    `https://finnhub.io/api/v1/company-news?symbol=${symbol}&from=${fmt(from)}&to=${fmt(to)}&token=${apiKey}`
  );
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return Array.isArray(data) ? data : [];
}

// Mirrors src/utils/news.js's selectRelevantArticles — see that file for
// the full rationale (Finnhub's `related` field is just an echo of the
// queried symbol, not a real relevance signal; verified live that market-
// roundup articles that never name the company still come back tagged with
// it). Kept as a separate, self-contained copy here rather than shared,
// same reasoning as extractMetricValues mirroring extractFinnhubMetricValues
// above in generateSectorMetrics.js — this is a plain Node/CommonJS script,
// not part of the app's ES module bundle.
const CORPORATE_SUFFIX_PATTERN = /\s+(inc\.?|incorporated|corp\.?|corporation|co\.?|company|ltd\.?|limited|plc|holdings?|group|sa|nv|ag|se|llc|lp)\b.*$/i;

function coreCompanyName(companyName) {
  if (!companyName) return null;
  const stripped = companyName.replace(CORPORATE_SUFFIX_PATTERN, '').trim();
  return stripped || companyName;
}

function escapeRegExp(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function selectRelevantArticles(articles, companyName, symbol, maxItems) {
  const name = coreCompanyName(companyName);
  const namePattern = name ? new RegExp(`\\b${escapeRegExp(name)}\\b`, 'i') : null;
  const tickerPattern = symbol ? new RegExp(`\\b${escapeRegExp(symbol)}\\b`, 'i') : null;
  const mentions = (text) => !!text && ((namePattern?.test(text) ?? false) || (tickerPattern?.test(text) ?? false));

  const scored = (articles || [])
    .filter((a) => a?.headline && a?.url)
    .map((article) => ({ article, relevance: mentions(article.headline) ? 2 : mentions(article.summary) ? 1 : 0 }))
    .filter((x) => x.relevance > 0);

  return scored
    .sort((a, b) => b.relevance - a.relevance || (b.article.datetime ?? 0) - (a.article.datetime ?? 0))
    .slice(0, maxItems)
    .map((x) => x.article);
}

async function main() {
  const apiKey = readFinnhubApiKey();

  console.log('Fetching current ticker universe + company names from the published sector-metrics feed...');
  const [metricsDataset, existingNewsCache] = await Promise.all([
    fetchJson(GIST_METRICS_URL),
    fetchJson(GIST_NEWS_URL).catch(() => ({ articles: {} })), // first-ever run: no existing cache yet
  ]);

  const symbols = Object.keys(metricsDataset.metrics || {}).sort();
  const cache = existingNewsCache.articles || {};

  // Company names aren't in marketMetrics.json (only industry + the 6
  // metrics — see generateSectorMetrics.js) — fetched fresh per ticker
  // below via profile2, which is fast (not the bottleneck; company-news is).

  // Least-recently-cached first (never-cached sorts first, via epoch 0) so
  // a run always makes forward progress on whatever's stalest, rather than
  // repeatedly refreshing the same tickers every run.
  const priority = symbols
    .map((symbol) => ({ symbol, fetchedAt: cache[symbol]?.fetchedAt ? new Date(cache[symbol].fetchedAt).getTime() : 0 }))
    .sort((a, b) => a.fetchedAt - b.fetchedAt)
    .map((x) => x.symbol);

  console.log(`${symbols.length} tickers in the universe, ${Object.keys(cache).length} already cached. Processing oldest/never-cached first...`);

  const startTime = Date.now();
  let processed = 0;
  let withArticles = 0;

  for (const symbol of priority) {
    if (Date.now() - startTime > TIME_BUDGET_MS) {
      console.log(`Time budget (${TIME_BUDGET_MS / 3600000}h) reached after ${processed} tickers — stopping for this run.`);
      break;
    }

    try {
      const profileRes = await fetch(`https://finnhub.io/api/v1/stock/profile2?symbol=${symbol}&token=${apiKey}`);
      const profile = profileRes.ok ? await profileRes.json() : {};
      await sleep(REQUEST_SPACING_MS);

      const rawArticles = await fetchCompanyNews(symbol, apiKey);
      const relevant = selectRelevantArticles(rawArticles, profile.name, symbol, MAX_ARTICLES_PER_TICKER);
      cache[symbol] = { fetchedAt: new Date().toISOString(), articles: relevant };
      if (relevant.length > 0) withArticles++;
    } catch (err) {
      console.log(`  skip ${symbol}: ${err.message}`);
      // Leave any existing cache entry for this symbol untouched on failure
      // — a stale entry is strictly better than losing it over one bad request.
    }

    processed++;
    if (processed % 100 === 0) {
      console.log(`  ${processed}/${priority.length} processed (${withArticles} with relevant articles), ${((Date.now() - startTime) / 60000).toFixed(0)}min elapsed`);
    }

    await sleep(REQUEST_SPACING_MS);
  }

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify({ generatedAt: new Date().toISOString(), articles: cache }));
  console.log(`Done. Processed ${processed} tickers this run (${withArticles} with relevant articles). Cache now covers ${Object.keys(cache).length}/${symbols.length} tickers total.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
