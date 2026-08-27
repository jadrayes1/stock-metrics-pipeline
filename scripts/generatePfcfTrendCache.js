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
const GIST_METRICS_URL = 'https://gist.githubusercontent.com/jadrayes1/5cd7f459788725521246717b9e164a8e/raw/marketMetrics.json';
const GIST_PFCF_TREND_URL = 'https://gist.githubusercontent.com/jadrayes1/5cd7f459788725521246717b9e164a8e/raw/pfcfTrendCache.json';

// Widened from 1100ms (~54.5/min, a ~9% margin) to match
// generateSectorMetrics.js's 2026-08-25 change — that pipeline's own runs
// showed a flat inter-request delay isn't a hard token-bucket, so normal
// jitter can still trip 429s even under a nominal 60/min average. This
// script's overall pacing is dominated by TWELVEDATA_REQUEST_SPACING_MS
// below regardless, so the extra margin here is nearly free.
const FINNHUB_REQUEST_SPACING_MS = 1350; // ~44.4/min, ~26% margin under Finnhub's 60/min free-tier cap — same budget as generateSectorMetrics.js
const TWELVEDATA_REQUEST_SPACING_MS = 8000; // ~7.5/min, under Twelve Data's free-tier 8/min cap
const MAX_TWELVEDATA_CALLS_PER_RUN = 700; // leaves buffer under Twelve Data's 800/day free-tier cap for this key
const TIME_BUDGET_MS = 6 * 60 * 60 * 1000; // safety net alongside the call cap above; leaves headroom under the workflow's 7hr timeout-minutes
const QUARTERS_OF_HISTORY = 12; // mirrors src/utils/metrics.js

// Tickers that changed symbol — Finnhub hasn't backfilled financials-
// reported history under the new symbol yet, even though /stock/metric and
// /stock/profile2 are fine. See RENAMED_TICKER_FINANCIALS_ALIASES in
// src/utils/metrics.js (mirrored here — keep in sync) for the full
// verified-live rationale (BNY/BK, CIK 1390777).
const RENAMED_TICKER_FINANCIALS_ALIASES = {
  BNY: 'BK',
  AD: 'USM',
  IA: 'ISSC',
};

// Mirrors FINANCIAL_INDUSTRIES/isFinancialIndustry in generateSectorMetrics.js
// (kept in sync) — verified live: BAC's real reported cash-flow statement
// has ZERO capex-related line items under any concept this file checks (no
// "purchases of premises and equipment" or similar), a genuine
// characteristic of how large banks tag their financials, not a missing
// concept name to add. buildPfcfTrendFromFilingsAndPrices and its Quarterly/
// Yearly siblings below treat missing capex as 0 for these tickers instead
// of excluding the period entirely - bank capex, on the rare filers that DO
// report it, is small relative to their OCF (tens of billions), so FCF ≈
// OCF is a reasonable approximation here, not a fabrication.
const FINANCIAL_INDUSTRIES = new Set(['Banking', 'Insurance', 'Financial Services']);
function isFinancialIndustry(industry) {
  return !!industry && FINANCIAL_INDUSTRIES.has(industry);
}

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
// NUTX's PaymentsToAcquireOtherPropertyPlantAndEquipment,
// PaymentsForFlightEquipment/PaymentsToAcquireOtherProductiveAssets for
// airlines like DAL).
function findReportedCapexQ(cfItems) {
  const concepts = [
    'us-gaap_PaymentsToAcquirePropertyPlantAndEquipment',
    'us-gaap_PaymentsToAcquireProductiveAssets',
    'us-gaap_PaymentsForCapitalImprovements',
    'us-gaap_PaymentsToAcquireOtherPropertyPlantAndEquipment',
    'us-gaap_PaymentsForFlightEquipment',
    'us-gaap_PaymentsToAcquireOtherProductiveAssets',
  ];
  // Summed rather than first-match — verified live: DAL splits its real
  // capex across TWO simultaneous lines ("Flight equipment, including
  // advance payments" + "Ground property and equipment, including
  // technology"), never a single combined figure. Returning just the
  // first match would understate real capex by ~15-20% for DAL
  // specifically; summing is a no-op for the common case where a filer
  // only ever tags one of these concepts.
  const matches = cfItems.filter((item) => concepts.includes(item.concept));
  if (matches.length) return matches.reduce((sum, item) => sum + item.value, 0);
  const labelMatch = cfItems.find((item) =>
    /purchases? of property|payments? (for|to) acquire (other )?property|capital expenditures|capital spending|capital improvements|flight equipment/i.test(item.label || '')
  );
  if (labelMatch) return labelMatch.value;
  // No capex line found -- treat as a real $0 (not missing data) ONLY when
  // this filer's cash-flow statement has no "Investing Activities" section
  // at all, rather than genuinely lacking a capex line within a section
  // that DOES exist. Verified live: Spero Therapeutics (SPRO), an early-
  // stage biotech with no property/equipment purchases of any kind, has
  // literally no investing-activities subtotal in 6 consecutive quarters
  // checked -- fcfMargin/P-FCF were permanently null for a real, current
  // company with nothing wrong except capex being genuinely zero. A filer
  // that DOES have an investing section but whose capex line just isn't
  // recognized by the patterns above still correctly returns null here --
  // this only fires when there's no investing section to have missed a
  // line in.
  const hasInvestingSection = cfItems.some((item) => /investing activities/i.test(item.label || ''));
  return hasInvestingSection ? null : 0;
}

// Mirrors findReportedDilutedShares in src/utils/metrics.js — see that file
// for the full rationale (previously no fallback at all, which starved
// P/FCF reconstruction of a share count for any filer reporting basic
// shares only; verified live: BlackSky/BKSY matched diluted shares for just
// 7 of 22 reported quarters).
function findReportedDilutedShares(icItems) {
  const match = icItems.find((item) => item.concept === 'us-gaap_WeightedAverageNumberOfDilutedSharesOutstanding');
  if (match) return match.value;
  const labelMatch = icItems.find((item) => /diluted.*shares|weighted average.*diluted/i.test(item.label || ''));
  if (labelMatch) return labelMatch.value;

  const basicMatch = icItems.find((item) => item.concept === 'us-gaap_WeightedAverageNumberOfSharesOutstandingBasic');
  if (basicMatch) return basicMatch.value;
  const basicLabelMatch = icItems.find((item) => /basic.*shares|weighted average.*basic/i.test(item.label || ''));
  if (basicLabelMatch) return basicLabelMatch.value;

  // Falls back to EarningsPerShareBasic when EarningsPerShareDiluted isn't
  // tagged at all -- verified live: SMID's real annual 10-K only discloses
  // one combined line, "Basic and diluted earnings per share" (tagged as
  // EarningsPerShareBasic — the filer has no dilutive securities, so basic
  // and diluted genuinely are the same figure, not an approximation here).
  // Without this, the yearly P/FCF trend was entirely empty despite the
  // quarterly trend working fine (quarterly reports DO tag the real share
  // count directly) — verified the derived value (netIncome/basicEps =
  // 5,293,103) against SMID's own real quarterly diluted share count
  // (5,307,000, same fiscal year) — within 0.3%.
  const netIncome = icItems.find((item) => item.concept === 'us-gaap_NetIncomeLoss');
  const eps = icItems.find((item) => item.concept === 'us-gaap_EarningsPerShareDiluted') || icItems.find((item) => item.concept === 'us-gaap_EarningsPerShareBasic');
  if (netIncome?.value != null && eps?.value) return netIncome.value / eps.value;

  return null;
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

// Mirrors buildTrailingWindows in src/utils/metrics.js — see that file for
// the full rationale (a very-recently-public company, verified live for
// CapsoVision/CV, can have fewer than 4 consecutive standalone quarters
// available at all; rather than emit nothing, fall back to whatever shorter
// run IS available, flagged `partial: true`).
function buildTrailingWindows(standaloneQuarters, maxSize = 4) {
  return standaloneQuarters.map((anchor, i) => {
    const window = [anchor];
    for (let j = i - 1; j >= 0 && window.length < maxSize; j--) {
      if (isConsecutiveQuarterWindow([standaloneQuarters[j], window[0]])) {
        window.unshift(standaloneQuarters[j]);
      } else {
        break;
      }
    }
    return { quarters: window, anchor, partial: window.length < maxSize };
  });
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
function buildPfcfTrendFromFilingsAndPrices(quarterlyReports, annualReports, monthlyPrices, isBank = false) {
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
    .filter((key) => (capex[key] != null || isBank) && sharesByQuarter[key] > 0)
    .map((key) => {
      const [year, quarter] = key.split('-').map(Number);
      return { year, quarter, fcf: ocf[key] - (capex[key] ?? 0), shares: sharesByQuarter[key] };
    })
    .sort((a, b) => a.year - b.year || a.quarter - b.quarter);

  const points = buildTrailingWindows(standaloneQuarters, 4).map(({ quarters, anchor, partial }) => {
    const ttmFcf = quarters.reduce((sum, q) => sum + q.fcf, 0);
    const { year, quarter, shares } = anchor;
    const ttmFcfPerShare = ttmFcf / shares;
    const price = findClosestMonthlyPrice(monthlyPrices, quarterEndDate(year, quarter));
    const value = price != null && ttmFcfPerShare !== 0 ? price / ttmFcfPerShare : null;
    return { label: `Q${quarter} '${String(year).slice(-2)}`, value, partial, quartersUsed: quarters.length };
  });

  return points.slice(-QUARTERS_OF_HISTORY);
}

function yearlyLabel(year) {
  return `FY '${String(year).slice(-2)}`;
}

// Standalone (non-TTM) quarterly P/FCF — mirrors buildPfcfQuarterlyFromFilingsAndPrices
// in src/utils/metrics.js. Annualized (x4), not the raw single-quarter FCF —
// P/FCF's convention divides price by a full YEAR of cash flow; dividing by
// just one quarter's FCF instead would inflate the multiple ~4x for no real
// reason and make it incomparable to the Yearly/TTM views (verified live
// building the app's equivalent tabs: AAPL's raw single-quarter P/FCF
// computed to 150-170x vs. a sensible 21-39x on an annual basis).
function buildPfcfQuarterlyFromFilingsAndPrices(quarterlyReports, annualReports, monthlyPrices, isBank = false) {
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

  const points = [];
  for (const key of Object.keys(ocf)) {
    if ((capex[key] == null && !isBank) || !(sharesByQuarter[key] > 0)) continue;
    const [year, quarter] = key.split('-').map(Number);
    const annualizedFcfPerShare = ((ocf[key] - (capex[key] ?? 0)) / sharesByQuarter[key]) * 4;
    const price = findClosestMonthlyPrice(monthlyPrices, quarterEndDate(year, quarter));
    const value = price != null && annualizedFcfPerShare !== 0 ? price / annualizedFcfPerShare : null;
    if (value != null) points.push({ year, quarter, label: `Q${quarter} '${String(year).slice(-2)}`, value });
  }
  return points
    .sort((a, b) => a.year - b.year || a.quarter - b.quarter)
    .map(({ label, value }) => ({ label, value }))
    .slice(-QUARTERS_OF_HISTORY);
}

// One P/FCF point per fiscal year, priced at that year's Dec-31-equivalent
// close — mirrors buildPfcfYearlyFromFilingsAndPrices in src/utils/metrics.js.
function buildPfcfYearlyFromFilingsAndPrices(annualReports, monthlyPrices, isBank = false) {
  const currentCik = annualReports?.[0]?.cik;
  const sameCik = (r) => currentCik == null || r.cik === currentCik;
  const filtered = (annualReports || []).filter(sameCik);

  const points = [];
  for (const a of filtered) {
    const ocf = findReportedOperatingCashFlowQ(a.report?.cf || []);
    const capex = findReportedCapexQ(a.report?.cf || []);
    const shares = findReportedDilutedShares(a.report?.ic || []);
    if (ocf == null || (capex == null && !isBank) || !(shares > 0)) continue;
    const fcfPerShare = (ocf - (capex ?? 0)) / shares;
    const price = findClosestMonthlyPrice(monthlyPrices, quarterEndDate(a.year, 4));
    const value = price != null && fcfPerShare !== 0 ? price / fcfPerShare : null;
    if (value != null) points.push({ year: a.year, label: yearlyLabel(a.year), value });
  }
  return points
    .sort((a, b) => a.year - b.year)
    .map(({ label, value }) => ({ label, value }))
    .slice(-QUARTERS_OF_HISTORY);
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

function pickCadenceTrendsToPublish(existingEntry, fresh) {
  return {
    ttm: pickTrendToPublish(existingEntry?.ttm, fresh.ttm),
    quarterly: pickTrendToPublish(existingEntry?.quarterly, fresh.quarterly),
    yearly: pickTrendToPublish(existingEntry?.yearly, fresh.yearly),
  };
}

async function fetchReportedFinancials(symbol, freq, apiKey) {
  const requestSymbol = RENAMED_TICKER_FINANCIALS_ALIASES[symbol] || symbol;
  const data = await fetchJson(`https://finnhub.io/api/v1/stock/financials-reported?symbol=${requestSymbol}&freq=${freq}&token=${apiKey}`);
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
  // A real scalar pfcfRatio does NOT mean the Quarterly/Yearly/TTM trend
  // has ever been attempted - verified live this matters: BAC and JPM (and
  // 3529 other tickers, 99.8% of the 3538 with a real pfcfRatio) have a
  // real, non-null card-level ratio computed elsewhere in the main
  // pipeline, but were NEVER queued here since the old filter only checked
  // pfcfRatio nullness - the same "card-value nullness is the wrong gap
  // signal" lesson already learned once in the foreign-filings-pipeline
  // repo's processTicker (see its own comment re: STNG), independently
  // rediscovered here. A ticker now qualifies as a gap if EITHER the
  // scalar ratio is missing OR no TTM trend has been cached yet for it.
  const gapSymbols = Object.entries(metricsDataset.metrics || {})
    .filter(([symbol, data]) => data.pfcfRatio == null || !cache[symbol]?.ttm?.length)
    .map(([symbol]) => symbol);

  // Least-recently-attempted first (never-attempted sorts first, via epoch
  // 0) — always makes forward progress on whatever's stalest, same rotation
  // strategy as generateNewsCache.js.
  const priority = gapSymbols
    .map((symbol) => ({ symbol, attemptedAt: cache[symbol]?.fetchedAt ? new Date(cache[symbol].fetchedAt).getTime() : 0 }))
    .sort((a, b) => a.attemptedAt - b.attemptedAt)
    .map((x) => x.symbol);

  const alreadyCovered = priority.filter((s) => cache[s]?.ttm?.length).length;
  console.log(
    `${gapSymbols.length} tickers currently have a P/FCF gap; ${alreadyCovered} already have a cached TTM trend. ` +
      `Processing up to ${MAX_TWELVEDATA_CALLS_PER_RUN} this run (oldest/never-attempted first) — computing Quarterly/Yearly/TTM together from the same fetched data.`
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

    let fresh = { ttm: [], quarterly: [], yearly: [] };
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

      // All three cadences reuse this SAME fetched data — no extra API
      // calls beyond the ones already budgeted above. industry is already
      // available from the metricsDataset fetched at the top of main() (the
      // main pipeline publishes it alongside pfcfRatio) — no extra fetch
      // needed to know whether this ticker is bank-like.
      const isBank = isFinancialIndustry(metricsDataset.metrics?.[symbol]?.industry);
      fresh = {
        ttm: buildPfcfTrendFromFilingsAndPrices(quarterlyReports, annualReports, monthlyPrices, isBank),
        quarterly: buildPfcfQuarterlyFromFilingsAndPrices(quarterlyReports, annualReports, monthlyPrices, isBank),
        yearly: buildPfcfYearlyFromFilingsAndPrices(annualReports, monthlyPrices, isBank),
      };
    } catch (err) {
      console.log(`  skip ${symbol}: ${err.message}`);
      // pickCadenceTrendsToPublish below falls back to whatever was already
      // cached for this symbol rather than losing it over one failed request.
    }

    const cadences = pickCadenceTrendsToPublish(cache[symbol], fresh);
    cache[symbol] = { fetchedAt: new Date().toISOString(), ...cadences };
    if (cadences.ttm.length) resolved++;

    processed++;
    if (processed % 50 === 0) {
      console.log(`  ${processed}/${priority.length} processed (${resolved} with a TTM trend), ${((Date.now() - startTime) / 60000).toFixed(0)}min elapsed, ${twelveDataCalls} Twelve Data calls used`);
    }
  }

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify({ generatedAt: new Date().toISOString(), trends: cache }));
  const totalCovered = Object.values(cache).filter((entry) => entry.ttm?.length).length;
  console.log(
    `Done. Processed ${processed} tickers this run (${resolved} resolved to a TTM trend, ${twelveDataCalls} Twelve Data calls used). ` +
      `Cache now covers ${totalCovered}/${gapSymbols.length} gap tickers total (TTM basis).`
  );
}

module.exports = {
  buildPfcfTrendFromFilingsAndPrices,
  buildPfcfQuarterlyFromFilingsAndPrices,
  buildPfcfYearlyFromFilingsAndPrices,
  fetchReportedFinancials,
  fetchMonthlyPrices,
  isFinancialIndustry,
  findReportedOperatingCashFlowQ,
  findReportedCapexQ,
  findReportedDilutedShares,
};

// Matches the require.main guard already used in the sibling
// generateForeignFilingsCache.js/generateSectorMetrics.js scripts — lets
// this file be required for direct testing (e.g. of the builder functions
// above against real fetched data) without triggering a full production
// run, which main() would otherwise do unconditionally on require.
if (require.main === module) {
  main().catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
