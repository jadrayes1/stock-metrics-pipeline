// scripts/generateSectorMetrics.js
//
// Offline data-generation step for the sector-percentile, Industry
// Leaders, and estimated-fair-value features. Fetches fundamentals +
// industry classification for every NASDAQ/NYSE/NYSE American common
// stock and REIT from Finnhub, and writes the result to
// src/data/marketMetrics.json, which the app reads directly at runtime —
// no live Finnhub calls, no waiting, no rate-limit exposure for the
// person using the app.
//
// Also computes `industryLeaders`: one ticker per industry with at least
// MIN_INDUSTRY_PEERS peers, required to have all 6 comparable metrics
// present AND rank top-quintile in at least 5 of them (see
// computeIndustryLeaders below for why — a ticker with only 1 of 6 metrics
// present was otherwise winning on a single outlier/likely-erroneous
// number), lightly biased toward tickers with more reported quarters (see
// historyWeight). Precomputed here rather than on-device so the Home
// screen's carousel is just reading a small pre-baked list, same "compute
// once daily, app just reads" philosophy as everything else this pipeline
// produces.
//
// Also computes `estimatedFairValue`: a DCF fair-value estimate built from
// Finnhub's own reported financials (see computeEstimatedFairValue below),
// used by the app as a fallback ONLY when FMP's own Fair Value can't be
// fetched (rate-limited or not cached yet) — see src/api/sectorComparison.js
// and src/screens/ResultsScreen.js. This is deliberately a fallback, not a
// replacement: sanity-checked against FMP's real numbers for AAPL/MSFT/KO/
// NVDA during development, it lands within roughly -20% to +25% of FMP's
// figure for ordinary and high-growth names, which is a genuinely different
// (not wrong, just independently-derived) number, not a precise match.
// Skipped entirely for Banking/Insurance/Financial Services tickers — FCF-
// based DCF is a poor fit for financial institutions (verified live: JPM's
// own operating cash flow is deeply negative due to loan/trading-asset
// accounting, not a normal operating cycle), so standard practice values
// those with a completely different model (dividend discount / excess
// return on book equity), which is out of scope here.
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
// It's public (unlike the app's private repo) specifically so this multi-
// hour job doesn't burn paid GitHub Actions minutes. This script itself has
// no dependency on which repo it runs in, though: `npm run
// generate-sector-metrics` still works locally too, at the same one-time
// cost (two or three Finnhub calls per ticker, rate-limited to stay under
// the free-tier 60/min cap).

const fs = require('fs');
const path = require('path');

const OUTPUT_FILE = path.join(__dirname, '../src/data/marketMetrics.json');
const REQUEST_SPACING_MS = 1100; // ~54/min, under Finnhub's 60/min free-tier cap

const ALLOWED_MICS = new Set(['XNAS', 'XNYS', 'XASE']); // NASDAQ, NYSE, NYSE American
const ALLOWED_TYPES = new Set(['Common Stock', 'REIT']);
const FINANCIAL_INDUSTRIES = new Set(['Banking', 'Insurance', 'Financial Services']);

function readFinnhubApiKey() {
  // This script runs server-side (locally or in the pipeline repo's GitHub
  // Actions workflow), never bundled into the app, so it reads the key
  // straight from the environment rather than src/config.js — the app no
  // longer keeps a real Finnhub key there at all (see src/config.js).
  if (process.env.FINNHUB_API_KEY) return process.env.FINNHUB_API_KEY;
  throw new Error('FINNHUB_API_KEY env var is not set.');
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
  // Finnhub reports this in millions of USD — converted to raw dollars where
  // it's used (computeEstimatedFairValue), to match financials-reported's
  // raw-dollar units. Pulled from the profile response specifically so the
  // DCF estimate doesn't need its own /quote call just for a price.
  const marketCapitalization = typeof data?.marketCapitalization === 'number' ? data.marketCapitalization * 1e6 : null;
  return { industry, name: data?.name || null, logo: data?.logo || null, marketCapitalization };
}

async function fetchReportedFinancialsFor(symbol, apiKey) {
  const res = await fetch(`https://finnhub.io/api/v1/stock/financials-reported?symbol=${symbol}&freq=annual&token=${apiKey}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return Array.isArray(data?.data) ? data.data : [];
}

// ---------------------------------------------------------------------------
// Estimated Fair Value (DCF) — see the file header for scope/rationale.
//
// Finnhub's `financials-reported` mirrors each filer's own XBRL tags and
// labels, which vary company to company (e.g. "Operating income" for Apple
// vs. "Operating Income (Loss)" for Microsoft). Matching by XBRL `concept`
// first (standardized across filers for common line items) with a label-
// keyword fallback is far more robust across ~4,400 different companies'
// filings than matching on label text alone.
// ---------------------------------------------------------------------------

function findByConcept(items, concepts) {
  for (const concept of concepts) {
    const match = items.find((i) => i.concept === concept);
    if (match?.value != null) return match.value;
  }
  return null;
}

function findByLabelKeywords(items, keywords, excludeKeywords = []) {
  const match = items.find((i) => {
    const label = (i.label || '').toLowerCase();
    if (i.value == null) return false;
    if (excludeKeywords.some((k) => label.includes(k))) return false;
    return keywords.some((k) => label.includes(k));
  });
  return match ? match.value : null;
}

function sumByLabelKeywords(items, includeKeywords, excludeKeywords) {
  return items
    .filter((i) => {
      const label = (i.label || '').toLowerCase();
      if (i.value == null) return false;
      if (excludeKeywords.some((k) => label.includes(k))) return false;
      return includeKeywords.some((k) => label.includes(k));
    })
    .reduce((sum, i) => sum + i.value, 0);
}

const DEBT_CONCEPTS = [
  'us-gaap_LongTermDebtNoncurrent',
  'us-gaap_LongTermDebtCurrent',
  'us-gaap_LongTermDebt',
  'us-gaap_ShortTermBorrowings',
  'us-gaap_CommercialPaper',
  'us-gaap_DebtCurrent',
  'us-gaap_SecuredDebtCurrent',
  'us-gaap_OtherLongTermDebtNoncurrent',
];
const DEBT_LABEL_KEYWORDS = ['term debt', 'commercial paper', 'notes payable', 'loans and notes payable', 'current maturities of long-term debt', 'short-term debt', 'short-term borrowings'];

function sumDebt(bsItems) {
  // Prefer concept-matched line items (dedup by concept so a value isn't
  // double-counted if it appears more than once in the raw report), falling
  // back to keyword-matched labels only for items no concept matched.
  const seenConcepts = new Set();
  let total = 0;
  let matchedAny = false;
  for (const item of bsItems) {
    if (item.value == null) continue;
    if (DEBT_CONCEPTS.includes(item.concept) && !seenConcepts.has(item.concept + item.value)) {
      seenConcepts.add(item.concept + item.value);
      total += item.value;
      matchedAny = true;
    }
  }
  if (matchedAny) return total;
  // Fallback: keyword-matched debt-instrument labels (avoids "Total
  // liabilities"-style aggregates by requiring specific instrument words).
  return sumByLabelKeywords(bsItems, DEBT_LABEL_KEYWORDS, ['total']);
}

// Everything in the cash-flow statement's operating section that ISN'T net
// income, D&A, or a non-cash reconciling item (stock comp, deferred tax,
// gains/losses, impairment, equity-method income) is treated as a working-
// capital change. Deliberately NOT computed as `OCF - NetIncome - D&A` —
// verified live that Finnhub's structured AAPL data doesn't reconcile that
// way (components summed to ~$29.8B more than the reported OCF subtotal for
// FY2025), so that shortcut would silently absorb whatever's causing the
// discrepancy into this figure. Summing the actual named items sidesteps it.
const WC_EXCLUDE_KEYWORDS = [
  'net income',
  'depreciation',
  'amortization',
  'stock-based',
  'stock based',
  'share-based',
  'share based',
  'deferred income tax',
  'deferred tax',
  'impairment',
  'gain on',
  'loss on',
  'gains on',
  'losses on',
  'equity method',
  'equity (income) loss',
  'noncontrolling',
  'net cash',
  'net change in cash',
];

function extractDcfInputs(reportedFinancials) {
  if (!reportedFinancials.length) return null;
  const annual = reportedFinancials.find((r) => r.form === '10-K') || reportedFinancials[0];
  const { ic, bs, cf } = annual.report || {};
  if (!ic || !bs || !cf) return null;

  const ebit = findByConcept(ic, ['us-gaap_OperatingIncomeLoss']) ?? findByLabelKeywords(ic, ['operating income', 'income from operations']);
  const pretaxIncome =
    findByConcept(ic, [
      'us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
      'us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
    ]) ?? findByLabelKeywords(ic, ['income before', 'income before provision for income tax']);
  const taxExpense = findByConcept(ic, ['us-gaap_IncomeTaxExpenseBenefit']) ?? findByLabelKeywords(ic, ['provision for income tax', 'income tax expense']);
  const dilutedShares =
    findByConcept(ic, ['us-gaap_WeightedAverageNumberOfDilutedSharesOutstanding']) ?? findByLabelKeywords(ic, ['diluted (in shares)', 'diluted shares', 'weighted average number of shares outstanding, diluted']);

  const da = findByConcept(cf, ['us-gaap_DepreciationDepletionAndAmortization', 'us-gaap_DepreciationAmortizationAndAccretionNet', 'us-gaap_DepreciationAndAmortization']) ?? findByLabelKeywords(cf, ['depreciation and amortization', 'depreciation, amortization']);
  const capex =
    findByConcept(cf, ['us-gaap_PaymentsToAcquirePropertyPlantAndEquipment']) ??
    findByLabelKeywords(cf, ['purchases of property', 'payments for acquisition of property', 'payments to acquire property', 'purchases related to property', 'capital expenditures']);
  const cash = findByConcept(bs, ['us-gaap_CashAndCashEquivalentsAtCarryingValue', 'us-gaap_CashAndCashEquivalentsAtFairValue', 'us-gaap_Cash']) ?? findByLabelKeywords(bs, ['cash and cash equivalents']);
  const totalDebt = sumDebt(bs);

  if (ebit == null || da == null || capex == null || dilutedShares == null || cash == null) return null;

  const taxRate = pretaxIncome != null && taxExpense != null && pretaxIncome > 0 ? Math.max(0, Math.min(0.35, taxExpense / pretaxIncome)) : 0.21; // fall back to the US statutory rate if effective rate isn't derivable

  // Sum every cf-section item up to (not including) the operating-activities
  // subtotal, excluding net income/D&A/non-cash reconciling items — the
  // remainder is working-capital-ish changes (receivables, payables,
  // inventory, deferred revenue, and any stray "other" adjustment lines).
  //
  // The subtotal is matched by XBRL concept, not label text — verified live
  // that label wording for this exact line varies by filer (Apple's own
  // 10-K says "Cash generated by operating activities," not the more common
  // "Net Cash Provided by (Used in) Operating Activities" that MSFT/KO/NVDA
  // use), while the underlying concept (NetCashProvidedByUsedInOperating
  // Activities) is standardized across all of them. Matching on label text
  // alone missed Apple's subtotal entirely and summed straight through the
  // rest of the cash-flow statement (investing/financing activities too),
  // inflating its estimate by 175% before this was caught.
  const OPERATING_SUBTOTAL_CONCEPTS = ['us-gaap_NetCashProvidedByUsedInOperatingActivities', 'us-gaap_NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'];
  let wcChangeTotal = 0;
  for (const item of cf) {
    if (OPERATING_SUBTOTAL_CONCEPTS.includes(item.concept)) break;
    if (item.value == null) continue;
    const label = (item.label || '').toLowerCase();
    // Fallback in case a filer's subtotal isn't tagged with either concept above.
    if (label.includes('net cash') && (label.includes('operating activities') || label.includes('investing activities') || label.includes('financing activities'))) break;
    if (WC_EXCLUDE_KEYWORDS.some((k) => label.includes(k))) continue;
    wcChangeTotal += item.value;
  }

  return { ebit, taxRate, da, capex, wcChange: wcChangeTotal, totalDebt, cash, dilutedShares, netDebt: totalDebt - cash };
}

const RF = 0.047; // 10-year Treasury yield — update periodically, doesn't need to be exact to the day
const ERP = 0.0445; // Damodaran's published implied US equity risk premium — update periodically
const TERMINAL_GROWTH = 0.025; // long-run nominal GDP growth convention
const DEBT_SPREAD = 0.015; // assumed spread over Rf for cost of debt (no per-ticker interest-expense data)
const GROWTH_CAP = 0.5; // caps near-term growth input — see file header; validated against FMP on AAPL/MSFT/KO/NVDA
const PROJECTION_YEARS = 10;

/**
 * Estimated Fair Value per share via a two-stage FCFF DCF: NOPAT + D&A +
 * working-capital change - CapEx, projected forward with growth fading
 * linearly from a capped near-term rate to TERMINAL_GROWTH, discounted at a
 * market-cap-weighted, CAPM-based WACC, then bridged from enterprise value
 * to equity value by subtracting net debt. See the file header for the
 * validation numbers and known limitations (still a materially different
 * estimate than FMP's own DCF, not a replica of it).
 */
function computeEstimatedFairValue(dcfInputs, current, marketCap) {
  const { ebit, taxRate, da, capex, wcChange, netDebt, dilutedShares } = dcfInputs;
  const beta = current.beta;
  if (beta == null || marketCap == null || dilutedShares <= 0) return null;

  const weightDebt = dcfInputs.totalDebt / (marketCap + dcfInputs.totalDebt);
  const weightEquity = 1 - weightDebt;
  const costOfEquity = RF + beta * ERP;
  const costOfDebtAfterTax = (RF + DEBT_SPREAD) * (1 - taxRate);
  const wacc = weightEquity * costOfEquity + weightDebt * costOfDebtAfterTax;
  if (wacc - TERMINAL_GROWTH < 0.02) return null; // guard against a degenerate/blown-up terminal value

  const growthCandidates = [current.revenueGrowth3Y, current.focfCagr5Y].filter((v) => v != null).map((v) => v / 100);
  let g1 = growthCandidates.length ? growthCandidates.reduce((a, b) => a + b, 0) / growthCandidates.length : TERMINAL_GROWTH;
  g1 = Math.max(-0.1, Math.min(GROWTH_CAP, g1));

  const nopat0 = ebit * (1 - taxRate);
  let fcff = nopat0 + da + wcChange - capex;
  let presentValueSum = 0;
  for (let year = 1; year <= PROJECTION_YEARS; year++) {
    const g = g1 - ((g1 - TERMINAL_GROWTH) * (year - 1)) / (PROJECTION_YEARS - 1);
    fcff *= 1 + g;
    presentValueSum += fcff / Math.pow(1 + wacc, year);
  }
  const terminalValue = (fcff * (1 + TERMINAL_GROWTH)) / (wacc - TERMINAL_GROWTH);
  const presentValueOfTerminal = terminalValue / Math.pow(1 + wacc, PROJECTION_YEARS);
  const equityValue = presentValueSum + presentValueOfTerminal - netDebt;
  const fairValue = equityValue / dilutedShares;
  return fairValue > 0 ? fairValue : null;
}

const COMPARABLE_KEYS = ['roic', 'revenueGrowth', 'profitMargin', 'fcfMargin', 'peRatio', 'pfcfRatio'];
const LOWER_IS_BETTER = new Set(['peRatio', 'pfcfRatio']); // mirrors METRIC_DEFS.betterWhen in src/utils/metrics.js
const MIN_INDUSTRY_PEERS = 100; // narrower industries are excluded — a "top pick" out of a handful of peers isn't statistically meaningful
// "top tier" bar for a single metric — top quintile among industry peers.
// Verified against the live full-universe data before picking this number:
// a 90th-percentile (top-decile) bar left only 2 of 15 qualifying industries
// with a leader at all — too strict to be a usable Home screen carousel.
// 80th percentile leaves 10 of 15, still a genuinely high bar (all 6 metrics
// present, top-quintile in at least MIN_TOP_METRICS of them).
const TOP_METRIC_PERCENTILE = 80;
const MIN_TOP_METRICS = 5; // must be top-tier in at least this many of the 6 metrics, not just a good average
const HISTORY_QUARTERS_FULL_WEIGHT = 8; // ~2 years — no penalty at or above this
const HISTORY_QUARTERS_FLOOR = 4; // ~1 year — a steeper penalty below this

// Mirrors the percentile-rank + goodness logic in src/utils/metrics.js
// (percentileRank, percentileTone) — keep in sync if that ever changes.
function percentileRank(value, population) {
  const valid = population.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  if (value === null || value === undefined || Number.isNaN(value) || valid.length === 0) return null;
  const below = valid.filter((v) => v < value).length;
  const tied = valid.filter((v) => v === value).length;
  return ((below + 0.5 * tied) / valid.length) * 100;
}

// A metric only counts toward MIN_TOP_METRICS if the ticker's last 3
// consecutive quarters for it were all positive — its VALUE still counts
// fully in the composite average either way (see computeIndustryLeaders),
// this only gates whether it can count as one of the "top" metrics. Verified
// live why this matters: SPRO (Spero Therapeutics) had a strong TTM
// profitMargin (24.9%) built on quarters that were mostly negative
// (-0.12, -1.36, +0.76, -27.92 — that last one from revenue collapsing to
// $0.0045/share from ~$0.60), so it doesn't reflect 3 consecutive quarters
// of real, positive performance despite the good-looking trailing number.
const CONSECUTIVE_QUARTERS_REQUIRED = 3;

// profitMargin/fcfMargin/roic/peRatio/pfcfRatio all have a direct quarterly
// series field (see QUARTERLY_FIELD_MAP in src/utils/metrics.js) — checks
// whether its 3 most recent entries are all positive.
function hasPositiveTrend(quarterlyEntries) {
  const entries = (quarterlyEntries || []).slice(0, CONSECUTIVE_QUARTERS_REQUIRED);
  if (entries.length < CONSECUTIVE_QUARTERS_REQUIRED) return false;
  return entries.every((e) => e?.v != null && e.v > 0);
}

// revenueGrowth has no direct quarterly field — derived from salesPerShare
// vs. the same quarter a year earlier, same as extractQuarterlyMetricSeries
// in src/utils/metrics.js, just checking the 3 most recent points are
// positive instead of building the full chart series.
function hasPositiveRevenueGrowthTrend(salesPerShareEntries) {
  const sales = salesPerShareEntries || [];
  const growthPoints = [];
  for (let i = 0; i < sales.length - 4 && growthPoints.length < CONSECUTIVE_QUARTERS_REQUIRED; i++) {
    const thisQuarter = sales[i];
    const yearAgo = sales[i + 4];
    growthPoints.push(thisQuarter?.v != null && yearAgo?.v ? (thisQuarter.v - yearAgo.v) / yearAgo.v : null);
  }
  if (growthPoints.length < CONSECUTIVE_QUARTERS_REQUIRED) return false;
  return growthPoints.every((v) => v != null && v > 0);
}

// Mirrors QUARTERLY_FIELD_MAP in src/utils/metrics.js.
function computeTrendQualification(quarterly) {
  return {
    roic: hasPositiveTrend(quarterly.roicTTM),
    revenueGrowth: hasPositiveRevenueGrowthTrend(quarterly.salesPerShare),
    profitMargin: hasPositiveTrend(quarterly.netMargin),
    fcfMargin: hasPositiveTrend(quarterly.fcfMargin),
    peRatio: hasPositiveTrend(quarterly.peTTM),
    pfcfRatio: hasPositiveTrend(quarterly.pfcfTTM),
  };
}

// A ticker reporting for less than ~2 years isn't excluded outright (a
// genuinely exceptional recent listing shouldn't be disqualified on that
// alone), but needs a meaningfully higher raw composite to overcome this to
// win — guards against a thinly-reported IPO/SPAC looking artificially great
// on a metric or two that hasn't been tested across a full cycle yet.
function historyWeight(historyQuarters) {
  if (historyQuarters >= HISTORY_QUARTERS_FULL_WEIGHT) return 1;
  if (historyQuarters >= HISTORY_QUARTERS_FLOOR) return 0.97;
  return 0.9;
}

/**
 * One "leader" per industry (>= MIN_INDUSTRY_PEERS peers only). A candidate
 * must have:
 *  1. All 6 comparable metrics present — verified live that a ticker with
 *     only 1 of 6 (e.g. SharonAI/SHAZ: fcfMargin present, everything else
 *     null, and that one figure a wildly implausible 2065% margin) could
 *     otherwise "win" an industry on a single outlier/likely-erroneous
 *     number instead of a genuinely complete picture.
 *  2. Top-tier (>= TOP_METRIC_PERCENTILE, currently the 80th) in at least
 *     MIN_TOP_METRICS of those 6, direction-normalized ("goodness" — a low
 *     P/E percentile-ranks as high goodness, same as percentileTone) — not
 *     just a good average across the board, but genuinely excellent in
 *     nearly everything. A metric only counts toward this if it also has
 *     3 consecutive positive quarters (see computeTrendQualification) — its
 *     value still counts fully in the composite average regardless, this
 *     only gates whether it can count as one of the "top" ones. Nothing is
 *     excluded outright for this; a candidate can still win on its other
 *     metrics if one is disqualified from counting as "top."
 * Among qualifying candidates, the winner is the highest composite score
 * (the goodness-averaged score across all 6, unaffected by rule 2's gating),
 * weighted down slightly for thin reporting history (see historyWeight). An
 * industry with no qualifying candidate at all gets no leader, rather than
 * forcing a pick.
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

    const eligible = symbols.filter((s) => COMPARABLE_KEYS.every((k) => metrics[s][k] !== null && metrics[s][k] !== undefined));

    let best = null;
    for (const symbol of eligible) {
      const data = metrics[symbol];
      const trendQualified = profiles[symbol]?.trendQualified || {};
      const goodnessScores = [];
      let topMetricCount = 0;
      for (const key of COMPARABLE_KEYS) {
        const pct = percentileRank(data[key], populations[key]);
        const goodness = LOWER_IS_BETTER.has(key) ? 100 - pct : pct;
        goodnessScores.push(goodness); // counts toward the composite regardless of the trend check below
        if (goodness >= TOP_METRIC_PERCENTILE && trendQualified[key]) topMetricCount++;
      }
      if (topMetricCount < MIN_TOP_METRICS) continue;

      const composite = goodnessScores.reduce((a, b) => a + b, 0) / goodnessScores.length;
      const adjustedScore = composite * historyWeight(profiles[symbol]?.historyQuarters || 0);
      if (!best || adjustedScore > best.adjustedScore) best = { symbol, composite, adjustedScore };
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
  console.log(
    `Fetching industry + fundamentals (+ DCF inputs for non-financial tickers) for ${symbols.length} tickers ` +
      `(two or three requests each, rate-limited to stay under Finnhub's free-tier cap — this takes several hours)...`
  );

  const metrics = {};
  const profiles = {}; // name/logo, kept out of `metrics` to avoid bloating that map — only industryLeaders needs them
  let ok = 0;
  let failed = 0;
  let dcfComputed = 0;

  for (let i = 0; i < symbols.length; i++) {
    const symbol = symbols[i];
    try {
      const profile = await fetchProfileFor(symbol, apiKey);
      await sleep(REQUEST_SPACING_MS);
      const { current, quarterly } = await fetchMetricsFor(symbol, apiKey);

      // netMargin's quarterly series is one of the more consistently-present
      // fields (see QUARTERLY_FIELD_MAP in src/utils/metrics.js) — used here
      // purely as a proxy for "how many quarters has Finnhub got on this
      // ticker," for the Industry Leaders history bias (see historyWeight).
      profiles[symbol] = {
        ...profile,
        historyQuarters: (quarterly.netMargin || []).length,
        trendQualified: computeTrendQualification(quarterly),
      };

      let estimatedFairValue = null;
      const isFinancial = profile.industry && FINANCIAL_INDUSTRIES.has(profile.industry);
      if (!isFinancial) {
        await sleep(REQUEST_SPACING_MS);
        try {
          const reportedFinancials = await fetchReportedFinancialsFor(symbol, apiKey);
          const dcfInputs = extractDcfInputs(reportedFinancials);
          if (dcfInputs) {
            estimatedFairValue = computeEstimatedFairValue(dcfInputs, current, profile.marketCapitalization);
            if (estimatedFairValue != null) dcfComputed++;
          }
        } catch {
          // A single ticker's oddly-shaped filing (or a failed request)
          // shouldn't take down the whole run — just skip its estimate,
          // same graceful-degradation philosophy as the rest of this
          // pipeline. Its sector-percentile metrics are unaffected.
        }
      }

      metrics[symbol] = { industry: profile.industry, ...extractMetricValues(current, quarterly), estimatedFairValue };
      ok++;
    } catch (err) {
      failed++;
      console.log(`  skip ${symbol}: ${err.message}`);
    }

    if ((i + 1) % 100 === 0 || i === symbols.length - 1) {
      console.log(`  ${i + 1}/${symbols.length} (${ok} ok, ${failed} failed, ${dcfComputed} with a fair-value estimate)`);
    }

    if (i < symbols.length - 1) await sleep(REQUEST_SPACING_MS);
  }

  const industryLeaders = computeIndustryLeaders(metrics, profiles);
  console.log(`\nComputed ${industryLeaders.length} industry leaders (industries with >= ${MIN_INDUSTRY_PEERS} peers).`);

  const output = { generatedAt: new Date().toISOString(), metrics, industryLeaders };
  fs.mkdirSync(path.dirname(OUTPUT_FILE), { recursive: true }); // works regardless of which repo this script runs in
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2));
  console.log(`Wrote ${ok} tickers to ${path.relative(process.cwd(), OUTPUT_FILE)} (${failed} skipped, ${dcfComputed} with a fair-value estimate).`);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { extractDcfInputs, computeEstimatedFairValue };
