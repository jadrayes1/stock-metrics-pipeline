// scripts/generatePfcfTrendCache.js
//
// Offline pre-fetch of a P/FCF TREND (not just the latest value) for every
// ticker whose Finnhub quarterly pfcfTTM series is empty — Finnhub's own
// data gap for this specific ratio, verified live for CEG despite its other
// quarterly series (peTTM, roicTTM, netMargin) being fully populated.
// Published to the same public Gist as marketMetrics.json (see
// generateSectorMetrics.js) as a third file, pfcfTrendCache.json. The app
// reads it as a middle tier — see fetchCachedPfcfTrend in
// src/api/sectorComparison.js and fetchPfcfTrendFallback in
// src/api/stockData.js — before ever falling back to a live, on-device
// reconstruction.
//
// This is a SEPARATE workflow/script from generateSectorMetrics.js (which
// already reconstructs the FCF Margin trend inline, cheaply, since that only
// needs Finnhub) because P/FCF also needs historical PRICES, which come from
// Twelve Data (Finnhub's own price candles require a paid plan — see
// src/api/twelvedata.js) — and Twelve Data's free tier caps at 800
// calls/day, 8/min. There are roughly 1,500+ tickers with this gap, so one
// run can't cover them all: this rotates through the gap list
// (least-recently-attempted first), capped at MAX_TWELVEDATA_CALLS_PER_RUN
// per run, merging results into whatever was already published rather than
// requiring a single run to finish the whole backlog. At ~700/run, the
// initial backlog clears in a few days; after that, a run mostly just picks
// up newly-appearing gaps (a ticker's Finnhub data can regress) or refreshes
// the oldest entries.
//
// Uses its OWN Twelve Data API key (TWELVEDATA_PIPELINE_API_KEY), separate
// from the one stock-analyzer-proxy holds for the app's own live per-device
// fallback (TWELVEDATA_API_KEY on the proxy) — sharing one key would mean
// this batch job and live user traffic compete for the same 800/day budget.
// Twelve Data's free tier allows creating additional accounts at no cost;
// see this repo's README / the workflow's one-time setup notes for how to
// wire up a second key as a secret here.
//
// Runs in the same public repo as generateSectorMetrics.js (see that
// script's header for why public) on its own schedule, offset from both
// other jobs (generate-sector-metrics.yml, generate-news-cache.yml) so none
// of them compete for Finnhub's account-wide rate limit at the same time —
// see .github/workflows/generate-pfcf-trend-cache.yml.

const fs = require('fs');
const path = require('path');

const OUTPUT_FILE = path.join(__dirname, '../pfcfTrendCache.json');
const GIST_METRICS_URL = 'https://gist.githubusercontent.com/jadrayes1/db6fbd96e980118d3c6a63965dc0dc39/raw/marketMetrics.json';
const GIST_PFCF_TREND_URL = 'https://gist.githubusercontent.com/jadrayes1/db6fbd96e980118d3c6a63965dc0dc39/raw/pfcfTrendCache.json';

const FINNHUB_REQUEST_SPACING_MS = 1100; // ~54/min, under Finnhub's 60/min free-tier cap — same budget as generateSectorMetrics.js
const TWELVEDATA_REQUEST_SPACING_MS = 8000; // ~7.5/min, under Twelve Data's free-tier 8/min cap
const MAX_TWELVEDATA_CALLS_PER_RUN = 700; // leaves buffer under Twelve Data's 800/day free-tier cap for this key
const TIME_BUDGET_MS = 6 * 60 * 60 * 1000; // safety net alongside the call cap above; leaves headroom under the workflow's 7hr timeout-minutes
const QUARTERS_OF_HISTORY = 12; // mirrors src/utils/metrics.js

function readFinnhubApiKey() {
  if (process.env.FINNHUB_API_KEY) return process.env.FINNHUB_API_KEY;
  throw new Error('FINNHUB_API_KEY env var is not set.');
}

function readTwelveDataApiKey() {
  // Deliberately a DIFFERENT env var from the app proxy's TWELVEDATA_API_KEY
  // — see the file header for why this needs its own key/account.
  if (process.env.TWELVEDATA_PIPELINE_API_KEY) return process.env.TWELVEDATA_PIPELINE_API_KEY;
  throw new Error('TWELVEDATA_PIPELINE_API_KEY env var is not set.');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// Filings-based reconstruction — mirrors buildPfcfTrendFromFilingsAndPrices
// in src/utils/metrics.js. Kept as a self-contained copy (CommonJS, not part
// of the app's ES module bundle), same reasoning as generateSectorMetrics.js
// duplicating its own concept-matching helpers instead of importing them.
// ---------------------------------------------------------------------------

function findReportedRevenue(icItems) {
  const REVENUE_CONCEPTS = ['us-gaap_RevenuesNetOfInterestExpense', 'us-gaap_Revenues', 'us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax'];
  for (const concept of REVENUE_CONCEPTS) {
    const match = icItems.find((item) => item.concept === concept);
    if (match) return match.value;
  }
  const labelMatch = icItems.find((item) => /^total\b.*revenue|^revenue$/i.test(item.label || ''));
  return labelMatch ? labelMatch.value : null;
}

function findReportedOperatingCashFlowQ(cfItems) {
  const match = cfItems.find((item) => item.concept === 'us-gaap_NetCashProvidedByUsedInOperatingActivities');
  if (match) return match.value;
  const labelMatch = cfItems.find((item) => /net cash.*operating activities/i.test(item.label || ''));
  return labelMatch ? labelMatch.value : null;
}

// Mirrors findReportedCapex in src/utils/metrics.js — see that file for the
// full rationale behind each concept (Ford's distinct
// PaymentsToAcquireProductiveAssets, REITs' PaymentsForCapitalImprovements,
// NUTX's PaymentsToAcquireOtherPropertyPlantAndEquipment).
function findReportedCapexQ(cfItems) {
  for (const concept of [
    'us-gaap_PaymentsToAcquirePropertyPlantAndEquipment',
    'us-gaap_PaymentsToAcquireProductiveAssets',
    'us-gaap_PaymentsForCapitalImprovements',
    'us-gaap_PaymentsToAcquireOtherPropertyPlantAndEquipment',
  ]) {
    const match = cfItems.find((item) => item.concept === concept);
    if (match) return match.value;
  }
  const labelMatch = cfItems.find((item) =>
    /purchases? of property|payments? (for|to) acquire (other )?property|capital expenditures|capital spending|capital improvements/i.test(item.label || '')
  );
  return labelMatch ? labelMatch.value : null;
}

function findReportedDilutedShares(icItems) {
  const match = icItems.find((item) => item.concept === 'us-gaap_WeightedAverageNumberOfDilutedSharesOutstanding');
  if (match) return match.value;
  const labelMatch = icItems.find((item) => /diluted.*shares|weighted average.*diluted/i.test(item.label || ''));
  return labelMatch ? labelMatch.value : null;
}

// Mirrors decumulateYtdByYear in src/utils/metrics.js — see that file for
// the full rationale (10-Qs report P&L/cash-flow lines as year-to-date
// cumulative; each quarter is de-cumulated against the prior one; Q4 = the
// 10-K's full-year figure minus the Q3 YTD figure; CIK-filtered first to
// guard against "ticker recycling").
function decumulateYtdByYear(quarterlyReports, annualReports, findValue, section) {
  const currentCik = quarterlyReports?.[0]?.cik ?? annualReports?.[0]?.cik;
  const sameCik = (r) => currentCik == null || r.cik === currentCik;
  quarterlyReports = (quarterlyReports || []).filter(sameCik);
  annualReports = (annualReports || []).filter(sameCik);

  const ytdByYear = {};
  const annualByYear = {};

  for (const q of quarterlyReports || []) {
    if (!q?.quarter) continue;
    const value = findValue(q.report?.[section] || []);
    if (value == null) continue;
    ytdByYear[q.year] = ytdByYear[q.year] || {};
    ytdByYear[q.year][q.quarter] = value;
  }
  for (const a of annualReports || []) {
    const value = findValue(a?.report?.[section] || []);
    if (value != null) annualByYear[a.year] = value;
  }

  const standalone = {};
  for (const [yearStr, q] of Object.entries(ytdByYear)) {
    const year = Number(yearStr);
    if (q[1] != null) standalone[`${year}-1`] = q[1];
    if (q[1] != null && q[2] != null) standalone[`${year}-2`] = q[2] - q[1];
    if (q[2] != null && q[3] != null) standalone[`${year}-3`] = q[3] - q[2];
    if (q[3] != null && annualByYear[year] != null) standalone[`${year}-4`] = annualByYear[year] - q[3];
  }
  return standalone;
}

// Mirrors isConsecutiveQuarterWindow in src/utils/metrics.js — see that
// file (and generateSectorMetrics.js's own copy) for the full rationale.
function isConsecutiveQuarterWindow(window) {
  for (let i = 1; i < window.length; i++) {
    const prev = window[i - 1];
    const curr = window[i];
    const isNextQuarter = curr.year === prev.year && curr.quarter === prev.quarter + 1;
    const isNewYearQ1 = curr.year === prev.year + 1 && prev.quarter === 4 && curr.quarter === 1;
    if (!isNextQuarter && !isNewYearQ1) return false;
  }
  return true;
}

function quarterEndDate(year, quarter) {
  const month = quarter * 3;
  const lastDay = new Date(year, month, 0).getDate();
  return new Date(year, month - 1, lastDay);
}

const MAX_PRICE_MATCH_MS = 45 * 24 * 60 * 60 * 1000;

function findClosestMonthlyPrice(monthlyPrices, targetDate) {
  if (!monthlyPrices?.length) return null;
  let closest = null;
  let closestDiff = Infinity;
  for (const p of monthlyPrices) {
    const diff = Math.abs(new Date(p.date).getTime() - targetDate.getTime());
    if (diff < closestDiff) {
      closestDiff = diff;
      closest = p;
    }
  }
  return closest && closestDiff <= MAX_PRICE_MATCH_MS ? closest.close : null;
}

// Mirrors buildPfcfTrendFromFilingsAndPrices in src/utils/metrics.js, minus
// the fallbackShares last-resort (a uniform today's-share-count applied
// across all quarters when a filing has no share count anywhere) — skipped
// here to avoid this script needing its own extra Finnhub profile call per
// ticker; a filer with no per-quarter share count AND no diluted/basic
// count anywhere just doesn't get a point for that quarter, same as any
// other missing input.
function buildPfcfTrendFromFilingsAndPrices(quarterlyReports, annualReports, monthlyPrices) {
  const currentCik = quarterlyReports?.[0]?.cik ?? annualReports?.[0]?.cik;
  const sameCik = (r) => currentCik == null || r.cik === currentCik;
  quarterlyReports = (quarterlyReports || []).filter(sameCik);
  annualReports = (annualReports || []).filter(sameCik);

  const ocf = decumulateYtdByYear(quarterlyReports, annualReports, findReportedOperatingCashFlowQ, 'cf');
  const capex = decumulateYtdByYear(quarterlyReports, annualReports, findReportedCapexQ, 'cf');

  const sharesByQuarter = {};
  for (const q of quarterlyReports || []) {
    if (!q?.quarter) continue;
    const shares = findReportedDilutedShares(q.report?.ic || []);
    if (shares != null) sharesByQuarter[`${q.year}-${q.quarter}`] = shares;
  }
  for (const a of annualReports || []) {
    const shares = findReportedDilutedShares(a?.report?.ic || []);
    if (shares != null) sharesByQuarter[`${a.year}-4`] = sharesByQuarter[`${a.year}-4`] ?? shares;
  }

  const standaloneQuarters = Object.keys(ocf)
    .filter((key) => capex[key] != null && sharesByQuarter[key] > 0)
    .map((key) => {
      const [year, quarter] = key.split('-').map(Number);
      return { year, quarter, fcf: ocf[key] - capex[key], shares: sharesByQuarter[key] };
    })
    .sort((a, b) => a.year - b.year || a.quarter - b.quarter);

  const points = [];
  for (let i = 3; i < standaloneQuarters.length; i++) {
    const window = standaloneQuarters.slice(i - 3, i + 1);
    if (!isConsecutiveQuarterWindow(window)) continue;
    const ttmFcf = window.reduce((sum, q) => sum + q.fcf, 0);
    const { year, quarter, shares } = standaloneQuarters[i];
    const ttmFcfPerShare = ttmFcf / shares;
    const price = findClosestMonthlyPrice(monthlyPrices, quarterEndDate(year, quarter));
    const value = price != null && ttmFcfPerShare !== 0 ? price / ttmFcfPerShare : null;
    points.push({ label: `Q${quarter} '${String(year).slice(-2)}`, value });
  }

  return points.slice(-QUARTERS_OF_HISTORY);
}

// A fresh attempt can come back empty or narrower on a day where Finnhub or
// Twelve Data has a transient hiccup for this specific ticker — that's not
// the same fact as "this ticker's trend no longer exists." Only replaces a
// previously-cached trend when the fresh attempt surfaces a calendar
// quarter (by label) the cache doesn't already have — mirrors
// pickTrendToPublish in generateSectorMetrics.js.
function pickTrendToPublish(existingPoints, freshPoints) {
  if (!freshPoints || freshPoints.length === 0) return existingPoints || [];
  if (!existingPoints || existingPoints.length === 0) return freshPoints;
  const existingLabels = new Set(existingPoints.map((p) => p.label));
  const hasNewQuarter = freshPoints.some((p) => !existingLabels.has(p.label));
  return hasNewQuarter ? freshPoints : existingPoints;
}

async function fetchReportedFinancials(symbol, freq, apiKey) {
  const data = await fetchJson(`https://finnhub.io/api/v1/stock/financials-reported?symbol=${symbol}&freq=${freq}&token=${apiKey}`);
  return Array.isArray(data?.data) ? data.data : [];
}

async function fetchMonthlyPrices(symbol, apiKey) {
  const data = await fetchJson(`https://api.twelvedata.com/time_series?symbol=${symbol}&interval=1month&outputsize=48&apikey=${apiKey}`);
  if (data?.status !== 'ok' || !Array.isArray(data.values)) return [];
  return data.values.map((v) => ({ date: v.datetime, close: parseFloat(v.close) })).filter((v) => !Number.isNaN(v.close));
}

async function main() {
  const finnhubKey = readFinnhubApiKey();
  const twelveDataKey = readTwelveDataApiKey();

  console.log('Fetching current ticker universe + P/FCF gap list from the published sector-metrics feed...');
  const [metricsDataset, existingTrendCache] = await Promise.all([
    fetchJson(GIST_METRICS_URL),
    fetchJson(GIST_PFCF_TREND_URL).catch(() => ({ trends: {} })), // first-ever run: no existing cache yet
  ]);

  const cache = existingTrendCache.trends || {};
  const gapSymbols = Object.entries(metricsDataset.metrics || {})
    .filter(([, data]) => data.pfcfRatio == null)
    .map(([symbol]) => symbol);

  // Least-recently-attempted first (never-attempted sorts first, via epoch
  // 0) — always makes forward progress on whatever's stalest, same rotation
  // strategy as generateNewsCache.js.
  const priority = gapSymbols
    .map((symbol) => ({ symbol, attemptedAt: cache[symbol]?.fetchedAt ? new Date(cache[symbol].fetchedAt).getTime() : 0 }))
    .sort((a, b) => a.attemptedAt - b.attemptedAt)
    .map((x) => x.symbol);

  const alreadyCovered = priority.filter((s) => cache[s]?.points?.length).length;
  console.log(
    `${gapSymbols.length} tickers currently have a P/FCF gap; ${alreadyCovered} already have a cached trend. ` +
      `Processing up to ${MAX_TWELVEDATA_CALLS_PER_RUN} this run (oldest/never-attempted first)...`
  );

  const startTime = Date.now();
  let processed = 0;
  let resolved = 0;
  let twelveDataCalls = 0;

  for (const symbol of priority) {
    if (twelveDataCalls >= MAX_TWELVEDATA_CALLS_PER_RUN) {
      console.log(`Twelve Data call budget (${MAX_TWELVEDATA_CALLS_PER_RUN}) reached after ${processed} tickers — stopping for this run.`);
      break;
    }
    if (Date.now() - startTime > TIME_BUDGET_MS) {
      console.log(`Time budget reached after ${processed} tickers — stopping for this run.`);
      break;
    }

    let freshPoints = [];
    try {
      // Fully sequential, each call followed by its own spacing — NOT
      // Promise.all. A concurrent version of this exact pattern (verified
      // live earlier in this project) silently doubled the effective
      // request rate and caused widespread false-negative 429s that looked
      // like missing data. Two Finnhub calls spaced FINNHUB_REQUEST_SPACING_MS
      // apart, then one Twelve Data call spaced TWELVEDATA_REQUEST_SPACING_MS
      // after the last Finnhub call (comfortably more than either provider's
      // minimum spacing, so it doubles as this ticker's Finnhub cooldown too).
      const quarterlyReports = await fetchReportedFinancials(symbol, 'quarterly', finnhubKey);
      await sleep(FINNHUB_REQUEST_SPACING_MS);
      const annualReports = await fetchReportedFinancials(symbol, 'annual', finnhubKey);
      await sleep(TWELVEDATA_REQUEST_SPACING_MS);
      const monthlyPrices = await fetchMonthlyPrices(symbol, twelveDataKey);
      twelveDataCalls++;

      freshPoints = buildPfcfTrendFromFilingsAndPrices(quarterlyReports, annualReports, monthlyPrices);
    } catch (err) {
      console.log(`  skip ${symbol}: ${err.message}`);
      // pickTrendToPublish below falls back to whatever was already cached
      // for this symbol rather than losing it over one failed request.
    }

    const points = pickTrendToPublish(cache[symbol]?.points, freshPoints);
    cache[symbol] = { fetchedAt: new Date().toISOString(), points };
    if (points.length) resolved++;

    processed++;
    if (processed % 50 === 0) {
      console.log(`  ${processed}/${priority.length} processed (${resolved} with a trend), ${((Date.now() - startTime) / 60000).toFixed(0)}min elapsed, ${twelveDataCalls} Twelve Data calls used`);
    }
  }

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify({ generatedAt: new Date().toISOString(), trends: cache }));
  const totalCovered = Object.values(cache).filter((entry) => entry.points?.length).length;
  console.log(
    `Done. Processed ${processed} tickers this run (${resolved} resolved to a trend, ${twelveDataCalls} Twelve Data calls used). ` +
      `Cache now covers ${totalCovered}/${gapSymbols.length} gap tickers total.`
  );
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
