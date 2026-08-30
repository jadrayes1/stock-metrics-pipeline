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

// ---------------------------------------------------------------------------
// SEC-XBRL enrichment for sparse/stale Finnhub coverage — a narrower port of
// the same mechanism already built and verified in generateSectorMetrics.js
// (fetchSecUsGaapFacts/buildSecSyntheticReports/pickDurationFact/
// findSecValueForFyFp — see that file for the full rationale on each design
// choice referenced in comments below). This was deliberately left out of
// this script's first version to bound that session's change size (see the
// cross-cadence-consistency plan's "Scope note"), which meant any ticker
// whose Finnhub financials-reported coverage is sparse or stale (verified
// live for SENEA: quarterly coverage stalls at 2022-07-02, annual at
// 2020-03-31, even though SEC's own filings are current through 2026-08-06)
// got P/FCF permanently stuck empty even after this script's own concept-
// matching fixes, since there was simply no fresher data to extract from.
//
// Narrower than generateSectorMetrics.js's copy in one respect (only cf
// items - ocf/capex - plus ic shares are synthesized, no revenue/netIncome/
// ebit/bs since P/FCF doesn't need them) and wider in another (a SHARES
// concept, which that file's ROIC/margin builders never needed but
// buildPfcfTrendFromFilingsAndPrices/its Quarterly/Yearly siblings do, via
// findReportedDilutedShares).
const SEC_COMPANYFACTS_BASE = 'https://data.sec.gov/api/xbrl/companyfacts';
const SEC_TICKERS_URL = 'https://www.sec.gov/files/company_tickers.json';
const SEC_USER_AGENT = 'stock-analyzer-app stock-metrics-pipeline contact:jadrayescpp@gmail.com';
const SEC_FETCH_TIMEOUT_MS = 30000;
// Same values as generateSectorMetrics.js's own constants of the same name —
// kept in sync deliberately, not re-derived, since both scripts are
// answering the identical question ("is this ticker's Finnhub coverage too
// sparse/stale to trust without a SEC assist?").
const SEC_ENRICHMENT_SPARSE_QUARTERLY_THRESHOLD = 4;
const SEC_ENRICHMENT_SPARSE_ANNUAL_THRESHOLD = 2;
const RECENT_GAP_SCAN_ENTRIES = 16;
const EXPECTED_QUARTERLY_GAP_DAYS = 200;
const MID_SEQUENCE_GAP_RECENT_YEARS = 2;
const SEC_OCF_CONCEPTS = ['NetCashProvidedByUsedInOperatingActivities', 'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'];
const SEC_CAPEX_CONCEPTS = ['PaymentsToAcquirePropertyPlantAndEquipment', 'PaymentsToAcquireProductiveAssets', 'PaymentsForCapitalImprovements', 'PaymentsToAcquireOtherPropertyPlantAndEquipment'];
const SEC_INVESTING_SUBTOTAL_CONCEPTS = ['NetCashProvidedByUsedInInvestingActivities', 'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations'];
// Diluted checked first, same preference order as findReportedDilutedShares
// below (basic is only a fallback there too).
const SEC_SHARES_CONCEPTS = ['WeightedAverageNumberOfDilutedSharesOutstanding', 'WeightedAverageNumberOfSharesOutstandingBasic'];
// Verified live: SENEA's OWN filing history tags this exact concept in TWO
// different scales across different years — 10-Qs filed 2023-2024 report
// val ~7000-8000 ("in thousands", i.e. really ~7-8 million shares), while
// 10-Qs filed 2025+ report val ~6,900,000-6,949,000 (already an absolute
// count) for the same real company (SENECA FOODS has always had roughly 7
// million shares outstanding — this is a filer-side XBRL scale-tagging
// drift, not a real 1000x share count change). A raw ~7000-count value fed
// straight into FCF-per-share would understate shares by ~1000x and produce
// a wildly wrong P/FCF ratio. Rather than guess at correcting the scale
// (this pipeline's hard rule is verify exactly, never estimate/infer), a
// value this far below any real NYSE/NASDAQ-listed company's plausible
// share count is simply treated as unusable, same as if it were missing —
// the newer, correctly-scaled facts still get through untouched.
const MIN_PLAUSIBLE_SHARES = 100000;

async function fetchSecJson(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), SEC_FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, { headers: { 'User-Agent': SEC_USER_AGENT, Accept: 'application/json' }, signal: controller.signal });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchSecTickerToCikMap() {
  const data = await fetchSecJson(SEC_TICKERS_URL);
  const map = new Map();
  for (const entry of Object.values(data || {})) {
    if (entry?.ticker && entry?.cik_str != null) {
      const ticker = String(entry.ticker).toUpperCase();
      const cik = String(entry.cik_str).padStart(10, '0');
      map.set(ticker, cik);
      // See generateSectorMetrics.js's own fetchSecTickerToCikMap for why
      // both spellings are registered (SEC hyphenates share-class suffixes,
      // e.g. BRK-A, while Finnhub/this script's callers use a period).
      if (ticker.includes('-')) map.set(ticker.replace(/-/g, '.'), cik);
    }
  }
  return map;
}

async function fetchSecUsGaapFacts(cik) {
  const data = await fetchSecJson(`${SEC_COMPANYFACTS_BASE}/CIK${cik}.json`);
  return data?.facts?.['us-gaap'] || {};
}

// Same cumulative-vs-exact / same-fy-fp-comparative disambiguation as
// generateSectorMetrics.js's pickDurationFact — see that function's own
// comment for the full verified-live rationale (BRK.A, ELF).
function pickDurationFact(facts, fy, fp) {
  const matches = (facts || []).filter((f) => f.fy === fy && f.fp === fp && f.start && f.end && f.val != null);
  if (!matches.length) return null;
  matches.sort((a, b) => new Date(b.end) - new Date(a.end) || new Date(a.start) - new Date(b.start) || new Date(b.filed || 0) - new Date(a.filed || 0));
  return matches[0];
}

function findSecValueForFyFp(gaapFacts, concepts, fy, fp, unit) {
  for (const concept of concepts) {
    const facts = gaapFacts[concept]?.units?.[unit] || [];
    const fact = pickDurationFact(facts, fy, fp);
    if (fact) return { value: fact.val, concept: `us-gaap_${concept}`, end: fact.end };
  }
  return null;
}

// Mirrors generateSectorMetrics.js's own hasRecentQuarterlyGap — real report
// entries here carry startDate/endDate the same way (Finnhub's financials-
// reported shape is identical across both scripts).
function hasRecentQuarterlyGap(quarterlyFinancials, scanEntries = RECENT_GAP_SCAN_ENTRIES) {
  const endDates = (quarterlyFinancials || [])
    .map((r) => (r?.endDate ? new Date(r.endDate) : null))
    .filter((d) => d instanceof Date && !isNaN(d))
    .sort((a, b) => b - a);
  if (!endDates.length) return false;
  const checkpoints = [new Date(), ...endDates.slice(0, scanEntries)];
  for (let i = 0; i < checkpoints.length - 1; i++) {
    const gapDays = (checkpoints[i] - checkpoints[i + 1]) / (1000 * 60 * 60 * 24);
    if (gapDays > EXPECTED_QUARTERLY_GAP_DAYS) return true;
  }
  return false;
}

// Mirrors generateSectorMetrics.js's own hasMidSequenceQuarterGap (the SGLY
// fix) — a missing MIDDLE quarter blocks decumulateYtdByYear from ever
// standalone-izing every later quarter in that fiscal year, even when
// hasRecentQuarterlyGap's date-gap threshold doesn't trip.
function hasMidSequenceQuarterGap(quarterlyFinancials) {
  const years = [...new Set((quarterlyFinancials || []).map((r) => r?.year).filter((y) => y != null))].sort((a, b) => b - a);
  const recentYears = new Set(years.slice(0, MID_SEQUENCE_GAP_RECENT_YEARS));
  const quartersByYear = {};
  for (const r of quarterlyFinancials || []) {
    if (!r?.year || !r?.quarter || !recentYears.has(r.year)) continue;
    quartersByYear[r.year] = quartersByYear[r.year] || new Set();
    quartersByYear[r.year].add(r.quarter);
  }
  for (const quarters of Object.values(quartersByYear)) {
    if (quarters.has(3) && !quarters.has(2)) return true;
    if (quarters.has(2) && !quarters.has(1)) return true;
  }
  return false;
}

// Builds report entries shaped exactly like the real Finnhub ones
// buildPfcfTrendFromFilingsAndPrices/its siblings already consume (only
// .year/.quarter/.cik/.report are ever read downstream) — see
// generateSectorMetrics.js's own buildSecSyntheticReports for why no
// startDate/endDate is needed and why cik must be normalized to Finnhub's
// unpadded numeric-string format (the ELF ticker-recycling-guard bug that
// fix addressed applies identically here, since decumulateYtdByYear's own
// `r.cik === currentCik` check is the same function, imported unchanged).
function buildSecSyntheticPfcfReports(gaapFacts, cik) {
  const normalizedCik = cik != null ? String(Number(cik)) : cik;
  const quarterNumber = { Q1: 1, Q2: 2, Q3: 3, Q4: 4 };

  const fyFpKeys = new Map();
  for (const concept of [...SEC_OCF_CONCEPTS, ...SEC_CAPEX_CONCEPTS]) {
    for (const fact of gaapFacts[concept]?.units?.USD || []) {
      if (fact.fy == null || !fact.fp || fact.val == null) continue;
      fyFpKeys.set(`${fact.fy}-${fact.fp}`, { fy: fact.fy, fp: fact.fp });
    }
  }
  for (const concept of SEC_SHARES_CONCEPTS) {
    for (const fact of gaapFacts[concept]?.units?.shares || []) {
      if (fact.fy == null || !fact.fp || fact.val == null) continue;
      fyFpKeys.set(`${fact.fy}-${fact.fp}`, { fy: fact.fy, fp: fact.fp });
    }
  }

  const quarterlyReports = [];
  const annualReports = [];
  for (const { fy, fp } of fyFpKeys.values()) {
    const ocfFact = findSecValueForFyFp(gaapFacts, SEC_OCF_CONCEPTS, fy, fp, 'USD');
    let capexFact = findSecValueForFyFp(gaapFacts, SEC_CAPEX_CONCEPTS, fy, fp, 'USD');
    if (!capexFact && findSecValueForFyFp(gaapFacts, SEC_INVESTING_SUBTOTAL_CONCEPTS, fy, fp, 'USD')) {
      capexFact = { concept: 'ImpliedZeroCapex', value: 0 };
    }
    const cfItems = [ocfFact, capexFact].filter(Boolean).map((r) => ({ concept: r.concept, label: `${r.concept} (SEC XBRL enrichment)`, value: r.value }));

    let sharesFact = findSecValueForFyFp(gaapFacts, SEC_SHARES_CONCEPTS, fy, fp, 'shares');
    if (sharesFact && sharesFact.value < MIN_PLAUSIBLE_SHARES) sharesFact = null; // see MIN_PLAUSIBLE_SHARES above
    const icItems = sharesFact ? [{ concept: sharesFact.concept, label: `${sharesFact.concept} (SEC XBRL enrichment)`, value: sharesFact.value }] : [];

    if (!cfItems.length && !icItems.length) continue;

    const entry = { cik: normalizedCik, form: 'SEC-XBRL', report: { ic: icItems, cf: cfItems } };
    if (fp === 'FY') annualReports.push({ ...entry, year: fy });
    else if (quarterNumber[fp]) quarterlyReports.push({ ...entry, year: fy, quarter: quarterNumber[fp] });
  }
  return { quarterlyReports, annualReports };
}

// Additive merge — real Finnhub entries always win on a (year, quarter)/
// (year) conflict, same idiom as generateSectorMetrics.js's own
// mergeSyntheticReports, minus the narrow-stub-replacement case (this script
// has no equivalent of that file's backfillRevenueGapsFromSec pre-pass, so
// every existing entry here is a real Finnhub one).
// Augments a real Finnhub entry's EMPTY sections with the synthesized
// entry's own items, rather than the old all-or-nothing "real entry
// occupies this slot, skip synthesizing" rule -- verified live: BNC's
// three most recent 10-Qs (a fiscal-year-change transition, filed as
// amended 10-Q/As) crawled with ic:0/cf:0 items on Finnhub's side (only
// balance-sheet content came through), so the old rule silently discarded
// real, current SEC data for those exact quarters just because a
// technically-real-but-empty Finnhub entry already claimed the slot. Only
// ever fills a section that's COMPLETELY empty -- a real entry with SOME
// content in a section (even partial) still wins outright for that
// section, same "real wins" philosophy as before.
function fillEmptySections(existing, synthesized) {
  if (!(existing.report.ic || []).length && (synthesized.report.ic || []).length) {
    existing.report.ic = synthesized.report.ic;
  }
  if (!(existing.report.cf || []).length && (synthesized.report.cf || []).length) {
    existing.report.cf = synthesized.report.cf;
  }
}

function mergeSyntheticPfcfReports(quarterlyReports, annualReports, synthesized) {
  const mergedQuarterlyReports = [...(quarterlyReports || [])];
  const existingQuarterByKey = new Map(mergedQuarterlyReports.filter((r) => r?.year && r?.quarter).map((r) => [`${r.year}-${r.quarter}`, r]));
  for (const r of synthesized.quarterlyReports) {
    const existing = existingQuarterByKey.get(`${r.year}-${r.quarter}`);
    if (!existing) {
      mergedQuarterlyReports.push(r);
    } else {
      fillEmptySections(existing, r);
    }
  }

  const mergedAnnualReports = [...(annualReports || [])];
  const existingAnnualByYear = new Map(mergedAnnualReports.filter((r) => r?.year).map((r) => [r.year, r]));
  for (const r of synthesized.annualReports) {
    const existing = existingAnnualByYear.get(r.year);
    if (!existing) {
      mergedAnnualReports.push(r);
    } else {
      fillEmptySections(existing, r);
    }
  }

  return { quarterlyReports: mergedQuarterlyReports, annualReports: mergedAnnualReports };
}

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
  if (labelMatch) return labelMatch.value;
  // Mirrors generateSectorMetrics.js's own findReportedOperatingCashFlowQ —
  // verified live: Village Farms (VFF) tags OCF under this concept with the
  // label "Cash Provided by (Used in) Operating Activity, Continuing
  // Operation(s)", which the label regex above doesn't match (no "net cash",
  // singular "Activity"/"Operation"). Without this, VFF's yearly/TTM P/FCF
  // were entirely empty despite real, current OCF/capex data being present.
  const continuingOpsMatch = cfItems.find((item) => item.concept === 'us-gaap_NetCashProvidedByUsedInOperatingActivitiesContinuingOperations');
  return continuingOpsMatch ? continuingOpsMatch.value : null;
}

// Mirrors findReportedCapex in src/utils/metrics.js — see that file for the
// full rationale behind each concept (Ford's distinct
// PaymentsToAcquireProductiveAssets, REITs' PaymentsForCapitalImprovements,
// NUTX's PaymentsToAcquireOtherPropertyPlantAndEquipment,
// PaymentsForFlightEquipment/PaymentsToAcquireOtherProductiveAssets for
// airlines like DAL). PaymentsToAcquireIntangibleAssets added for JTAI (Jet.AI)
// -- verified live: its 2023-2024 10-Qs tag capitalized software/IP spend
// under this standard concept instead of any PP&E variant.
function findReportedCapexQ(cfItems) {
  const concepts = [
    'us-gaap_PaymentsToAcquirePropertyPlantAndEquipment',
    'us-gaap_PaymentsToAcquireProductiveAssets',
    'us-gaap_PaymentsForCapitalImprovements',
    'us-gaap_PaymentsToAcquireOtherPropertyPlantAndEquipment',
    'us-gaap_PaymentsForFlightEquipment',
    'us-gaap_PaymentsToAcquireOtherProductiveAssets',
    'us-gaap_PaymentsToAcquireIntangibleAssets',
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
  // "deposits? (on|for) aircraft" added for JTAI (Jet.AI) -- verified live:
  // its 2025 10-Qs tag real cash paid toward acquiring aircraft under a
  // company-specific extension concept (JTAI_PaymentsForOtherDepositsOnAircraft),
  // never a standard us-gaap one, so only a label match can catch it -- same
  // class of gap "flight equipment" was added for DAL, just a different phrasing.
  const labelMatch = cfItems.find((item) =>
    /purchases? of property|payments? (for|to) acquire (other )?property|capital expenditures|capital spending|capital improvements|flight equipment|deposits? (on|for) aircraft/i.test(
      item.label || ''
    )
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
  // Verified live: SENEA tags this concept (and the basic equivalent) "in
  // thousands" across its 2022-2024 filings (val ~7000-8000 for a company
  // with ~7 million real shares outstanding), then switches to absolute
  // units in 2025+ filings for the SAME concept — a filer-side XBRL scale
  // drift, not a real share-count change. Skipping (rather than trusting)
  // any candidate below MIN_PLAUSIBLE_SHARES and falling through to the
  // next one prevents a ~1000x-understated share count from silently
  // producing a wildly wrong FCF-per-share/P-FCF ratio.
  const plausible = (v) => v != null && v >= MIN_PLAUSIBLE_SHARES;

  const match = icItems.find((item) => item.concept === 'us-gaap_WeightedAverageNumberOfDilutedSharesOutstanding');
  if (plausible(match?.value)) return match.value;
  const labelMatch = icItems.find((item) => /diluted.*shares|weighted average.*diluted/i.test(item.label || ''));
  if (plausible(labelMatch?.value)) return labelMatch.value;

  const basicMatch = icItems.find((item) => item.concept === 'us-gaap_WeightedAverageNumberOfSharesOutstandingBasic');
  if (plausible(basicMatch?.value)) return basicMatch.value;
  const basicLabelMatch = icItems.find((item) => /basic.*shares|weighted average.*basic/i.test(item.label || ''));
  if (plausible(basicLabelMatch?.value)) return basicLabelMatch.value;

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
  const derived = netIncome?.value != null && eps?.value ? netIncome.value / eps.value : null;
  if (plausible(derived)) return derived;

  return null;
}

// A real quarter is ~90-92 days; a YTD-cumulative Q2/Q3 report spans ~181/272
// days -- comfortable separation for telling the two apart from a report's
// own startDate/endDate.
const STANDALONE_QUARTER_MAX_DAYS = 120;
function reportDurationDays(report) {
  if (!report?.startDate || !report?.endDate) return null;
  const start = new Date(report.startDate);
  const end = new Date(report.endDate);
  if (isNaN(start) || isNaN(end)) return null;
  return (end - start) / (1000 * 60 * 60 * 24);
}

// Mirrors decumulateYtdByYear in src/utils/metrics.js / generateSectorMetrics.js
// — see generateSectorMetrics.js's own copy for the full verified-live
// rationale on why this can't just assume every quarter is cumulative
// (SHAK switched to already-standalone 90-day disclosures from 2022
// onward, which the old blind-subtraction logic corrupted into a negative
// "standalone" quarter).
function decumulateYtdByYear(quarterlyReports, annualReports, findValue, section) {
  const currentCik = quarterlyReports?.[0]?.cik ?? annualReports?.[0]?.cik;
  const sameCik = (r) => currentCik == null || r.cik === currentCik;
  quarterlyReports = (quarterlyReports || []).filter(sameCik);
  annualReports = (annualReports || []).filter(sameCik);

  const ytdByYear = {};
  const isStandaloneDisclosure = {};
  const annualByYear = {};

  for (const q of quarterlyReports || []) {
    if (!q?.quarter) continue;
    const value = findValue(q.report?.[section] || []);
    if (value == null) continue;
    ytdByYear[q.year] = ytdByYear[q.year] || {};
    ytdByYear[q.year][q.quarter] = value;
    const duration = reportDurationDays(q);
    isStandaloneDisclosure[q.year] = isStandaloneDisclosure[q.year] || {};
    isStandaloneDisclosure[q.year][q.quarter] = duration != null && duration <= STANDALONE_QUARTER_MAX_DAYS;
  }
  for (const a of annualReports || []) {
    const value = findValue(a?.report?.[section] || []);
    if (value != null) annualByYear[a.year] = value;
  }

  const standalone = {};
  for (const [yearStr, q] of Object.entries(ytdByYear)) {
    const year = Number(yearStr);
    const standaloneFlags = isStandaloneDisclosure[year] || {};
    const cumulativeThrough = {};
    if (q[1] != null) cumulativeThrough[1] = q[1];

    if (q[1] != null) standalone[`${year}-1`] = q[1];

    if (q[2] != null) {
      if (standaloneFlags[2]) {
        standalone[`${year}-2`] = q[2];
        if (cumulativeThrough[1] != null) cumulativeThrough[2] = cumulativeThrough[1] + q[2];
      } else {
        cumulativeThrough[2] = q[2];
        if (cumulativeThrough[1] != null) standalone[`${year}-2`] = q[2] - cumulativeThrough[1];
      }
    }

    if (q[3] != null) {
      if (standaloneFlags[3]) {
        standalone[`${year}-3`] = q[3];
        if (cumulativeThrough[2] != null) cumulativeThrough[3] = cumulativeThrough[2] + q[3];
      } else {
        cumulativeThrough[3] = q[3];
        if (cumulativeThrough[2] != null) standalone[`${year}-3`] = q[3] - cumulativeThrough[2];
      }
    }

    if (cumulativeThrough[3] != null && annualByYear[year] != null) standalone[`${year}-4`] = annualByYear[year] - cumulativeThrough[3];
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
  const [metricsDataset, existingTrendCache, secTickerToCikMap] = await Promise.all([
    fetchJson(GIST_METRICS_URL),
    fetchJson(GIST_PFCF_TREND_URL).catch(() => ({ trends: {} })), // first-ever run: no existing cache yet
    fetchSecTickerToCikMap().catch(() => new Map()), // non-fatal — SEC enrichment below just never triggers without it
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
  let priority = gapSymbols
    .map((symbol) => ({ symbol, attemptedAt: cache[symbol]?.fetchedAt ? new Date(cache[symbol].fetchedAt).getTime() : 0 }))
    .sort((a, b) => a.attemptedAt - b.attemptedAt)
    .map((x) => x.symbol);

  // Manual single-symbol override (workflow_dispatch's "symbol" input, wired
  // to this env var) — lets debugging one ticker's P/FCF gap take seconds
  // instead of a full multi-hour run through the entire gap list. Bypasses
  // the gap filter too (a ticker can be force-tested even if it currently
  // has a real pfcfRatio/cached TTM point), since the whole point is to
  // isolate exactly what happens for THIS symbol.
  if (process.env.TARGET_SYMBOL) {
    priority = [process.env.TARGET_SYMBOL.toUpperCase()];
    console.log(`TARGET_SYMBOL set — processing only ${priority[0]}, ignoring the normal gap list/rotation.`);
  }

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
      let quarterlyReports = await fetchReportedFinancials(symbol, 'quarterly', finnhubKey);
      await sleep(FINNHUB_REQUEST_SPACING_MS);
      let annualReports = await fetchReportedFinancials(symbol, 'annual', finnhubKey);

      // SEC-XBRL enrichment for sparse/stale Finnhub coverage — see this
      // file's own comment block above buildSecSyntheticPfcfReports for the
      // full rationale (SENEA verified live: Finnhub's own quarterly/annual
      // financials-reported stall at 2022/2020 respectively, while SEC's
      // filings are current). Free to check (array/date inspection); only
      // spends a real network call when a gap is actually found, so the
      // vast majority of already-healthy gap tickers pay nothing extra.
      if (
        quarterlyReports.length < SEC_ENRICHMENT_SPARSE_QUARTERLY_THRESHOLD ||
        annualReports.length < SEC_ENRICHMENT_SPARSE_ANNUAL_THRESHOLD ||
        hasRecentQuarterlyGap(quarterlyReports) ||
        hasMidSequenceQuarterGap(quarterlyReports)
      ) {
        const cik = secTickerToCikMap.get((RENAMED_TICKER_FINANCIALS_ALIASES[symbol] || symbol).toUpperCase()) || secTickerToCikMap.get(symbol.toUpperCase());
        if (cik) {
          const gaapFacts = await fetchSecUsGaapFacts(cik);
          const synthesized = buildSecSyntheticPfcfReports(gaapFacts, quarterlyReports[0]?.cik ?? annualReports[0]?.cik ?? cik);
          const merged = mergeSyntheticPfcfReports(quarterlyReports, annualReports, synthesized);
          quarterlyReports = merged.quarterlyReports;
          annualReports = merged.annualReports;
        }
      }

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
  fetchSecTickerToCikMap,
  fetchSecUsGaapFacts,
  buildSecSyntheticPfcfReports,
  mergeSyntheticPfcfReports,
  hasRecentQuarterlyGap,
  hasMidSequenceQuarterGap,
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
