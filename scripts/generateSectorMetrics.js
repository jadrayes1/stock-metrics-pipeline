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
// replacement: regression-tested against FMP's real numbers for AAPL/MSFT/
// NVDA, it lands within roughly -10% to +10% of FMP's figure (ERP and
// TERMINAL_GROWTH were tuned specifically to close this gap — see their
// comments below). Mature/low-growth names like KO remain a much wider miss
// (~-60%) regardless of WACC/horizon tuning, because the model's growth
// input is trailing historical growth, not forward/analyst-estimate growth
// — no assumption change here fixes that, it's a different-inputs problem.
// Either way this is a genuinely different (not wrong, just independently-
// derived) number, not a precise match.
// Computed for every ticker, including Banking/Insurance/Financial Services,
// though FCF-based DCF is a known poor fit for financial institutions
// (verified live: JPM's own operating cash flow is deeply negative due to
// loan/trading-asset accounting, not a normal operating cycle — its
// estimate came out 123% above FMP's own number in testing). Standard
// practice values financials with a completely different model (dividend
// discount / excess return on book equity) instead of FCF-based DCF, which
// is out of scope here — treat estimates for these industries with extra
// skepticism. Financial-industry tickers are held to a tighter DCF sanity
// bound (2.5x implied price vs. 8x for everyone else — see
// computeEstimatedFairValue) since even correctly-extracted inputs
// routinely produce an implausible estimate for this sector.
//
// Also reconstructs a full FCF Margin TREND from raw SEC filings (see
// buildFcfMarginTrendFromFilings) for any ticker where Finnhub's own
// quarterly fcfMargin series is empty OR internally gapped — mirrors the
// app's on-demand fallback (buildFcfMarginFromFilings in
// src/utils/metrics.js), published here so the app's trend chart can read a
// precomputed, cross-user-shared series instead of every device
// reconstructing the same thing live. Merge-protected against a bad run
// (see pickTrendToPublish) — a fresh attempt that comes back empty or
// narrower than what's already published leaves the existing cache
// untouched rather than regressing it. Skipped for Banking/Insurance/
// Financial Services, where FCF isn't a meaningful concept.
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
// Trend caches (native/quarterly/yearly/ttm) publish to their OWN files,
// separate from marketMetrics.json — at full-universe scale (~5,000+
// tickers x up to 6 metrics x 12 points) these run tens of megabytes,
// which both blows past the Gist API's 1MB-per-file publish limit (see the
// git-based publish step in .github/workflows/generate-sector-metrics.yml)
// and — more importantly — would otherwise bloat marketMetrics.json, which
// the app fetches on every screen, not just trend charts. Keeping trends in
// their own files lets the app fetch marketMetrics.json cheaply for
// everyday use and only pull a trend file when a trend chart actually
// needs that specific cadence (see src/api/sectorComparison.js).
const OUTPUT_TRENDS_NATIVE_FILE = path.join(__dirname, '../src/data/trendsNative.json');
const OUTPUT_TRENDS_QUARTERLY_FILE = path.join(__dirname, '../src/data/trendsQuarterly.json');
const OUTPUT_TRENDS_YEARLY_FILE = path.join(__dirname, '../src/data/trendsYearly.json');
const OUTPUT_TRENDS_TTM_FILE = path.join(__dirname, '../src/data/trendsTtm.json');
const REQUEST_SPACING_MS = 1100; // ~54/min, under Finnhub's 60/min free-tier cap

const ALLOWED_MICS = new Set(['XNAS', 'XNYS', 'XASE']); // NASDAQ, NYSE, NYSE American
const ALLOWED_TYPES = new Set(['Common Stock', 'REIT']);

// This script runs server-side (locally or in the pipeline repo's GitHub
// Actions workflow), never bundled into the app, so it reads keys straight
// from the environment rather than src/config.js — the app no longer keeps
// a real Finnhub key there at all (see src/config.js).
//
// A second, independent Finnhub account/key (FINNHUB_API_KEY_2) is
// optional. When present, the ticker universe is split across one worker
// per key, each pacing its OWN sequential Finnhub calls against its own
// key's 60/min free-tier budget independently — roughly multiplying total
// throughput by the number of keys, since the run's bottleneck is purely
// that per-key rate limit, not compute (same principle already used for
// generatePfcfTrendCache.js's separate Twelve Data pipeline key). Falls
// back to today's single-key, single-worker behavior when only
// FINNHUB_API_KEY is set — this is additive, not a required setup change.
function readFinnhubApiKeys() {
  const keys = [];
  if (process.env.FINNHUB_API_KEY) keys.push(process.env.FINNHUB_API_KEY);
  if (process.env.FINNHUB_API_KEY_2) keys.push(process.env.FINNHUB_API_KEY_2);
  if (!keys.length) throw new Error('FINNHUB_API_KEY env var is not set.');
  return keys;
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

// current.revenueGrowthTTMYoy can be entirely absent even when
// quarterly.salesPerShare has real data (verified live: CoreWeave/CRWV) —
// derives the latest YoY point directly from salesPerShare as a fallback,
// same as the app's equivalent (src/utils/metrics.js).
function latestRevenueGrowthFromQuarterly(quarterly) {
  const sales = quarterly?.salesPerShare;
  if (!Array.isArray(sales)) return null;
  const thisQuarter = sales[0];
  const yearAgo = sales[4];
  return thisQuarter?.v != null && yearAgo?.v ? (thisQuarter.v - yearAgo.v) / yearAgo.v : null;
}

// Mirrors extractFinnhubMetricValues in src/utils/metrics.js — keep these
// two in sync if that mapping ever changes (see the field-name notes there
// for why current/quarterly don't share names or units). Doesn't mirror the
// app's filings-based P/FCF and FCF Margin reconstruction fallbacks — those
// need a live Twelve Data price fetch (P/FCF) or aren't worth the extra
// financials-reported fetch for every one of ~5,140 tickers here, so a
// ticker with a Finnhub-side gap in those two shows null in this bulk
// dataset even though the app can still fill it in on-demand for that one
// ticker when someone looks it up.
function extractMetricValues(current, quarterly, impliedPrice) {
  const roic = latestQuarterly(quarterly, 'roicTTM') ?? (current.roiTTM != null ? current.roiTTM / 100 : null);
  const revenueGrowth = current.revenueGrowthTTMYoy != null ? current.revenueGrowthTTMYoy / 100 : latestRevenueGrowthFromQuarterly(quarterly);
  const profitMargin = current.netProfitMarginTTM != null ? current.netProfitMarginTTM / 100 : null;
  const fcfMargin = latestQuarterly(quarterly, 'fcfMargin');
  const peRatio = current.peTTM ?? (impliedPrice != null && current.epsTTM ? impliedPrice / current.epsTTM : null);
  const pfcfRatio = latestQuarterly(quarterly, 'pfcfTTM') ?? current.pfcfShareTTM ?? null;
  return { roic, revenueGrowth, profitMargin, fcfMargin, peRatio, pfcfRatio };
}

// Dual/multi-class share tickers where Finnhub's fundamentals-ratio engine
// (/stock/metric) only covers one class even though both trade the exact
// same underlying company — verified live (Aug 2026) for every pair below:
// the aliased ticker's own quarterly series is entirely empty (0 entries
// across every metric, not just one) while its sibling class has full,
// healthy coverage (verified live: Berkshire Hathaway/BRK.B has zero
// quarterly data at all, while BRK.A — same company, same CIK, just the
// original share class — has 99-145 quarterly points per metric). Since
// P/E, ROIC, margins, revenue growth, and P/FCF are all RATIOS, not
// per-share dollar amounts, they're identical between share classes of the
// same company regardless of the (often very different) per-share price —
// so reading the healthy sibling's ratio data isn't an approximation, it's
// the correct number. Mirrors DUAL_CLASS_ALIASES in src/utils/metrics.js —
// keep in sync. Deliberately only applied to this one call (the ratio
// engine) — profile (name/logo/marketCap/industry) and the DCF's own
// financials-reported extraction stay symbol-specific, since those
// genuinely differ (or are separately, independently sparse) per class.
const DUAL_CLASS_ALIASES = {
  'AGM.A': 'AGM',
  'BF.A': 'BF.B',
  'BIO.B': 'BIO',
  'BRK.B': 'BRK.A',
  'CRD.A': 'CRD.B',
  'GEF.B': 'GEF',
  'GTN.A': 'GTN',
  'HEI.A': 'HEI',
  'HVT.A': 'HVT',
  'LEN.B': 'LEN',
  'MKC.V': 'MKC',
  'MOG.B': 'MOG.A',
  'TAP.A': 'TAP',
  'UHAL.B': 'UHAL',
  'WSO.B': 'WSO',
};

async function fetchMetricsFor(symbol, apiKey) {
  const requestSymbol = DUAL_CLASS_ALIASES[symbol] || symbol;
  const res = await fetch(`https://finnhub.io/api/v1/stock/metric?symbol=${requestSymbol}&metric=all&token=${apiKey}`);
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
  const shareOutstanding = typeof data?.shareOutstanding === 'number' ? data.shareOutstanding * 1e6 : null;
  return { industry, name: data?.name || null, logo: data?.logo || null, marketCapitalization, shareOutstanding };
}

// marketCap ÷ shares outstanding, both already in the profile response —
// close enough to a live quote for P/E's negative-EPS fallback below
// without this pipeline needing its own /quote call for every one of
// ~5,140 tickers (verified live: for AAL, this gives $16.04 vs. the real
// quote's $16.58 — same P/E sign and magnitude either way, which is all
// this fallback needs to distinguish "negative earnings" from "no data").
function impliedPriceFromProfile(profile) {
  return profile.marketCapitalization != null && profile.shareOutstanding > 0 ? profile.marketCapitalization / profile.shareOutstanding : null;
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
  // Verified live: CVS combines debt and capital lease obligations into a
  // single concept instead of reporting them separately.
  'us-gaap_LongTermDebtAndCapitalLeaseObligationsCurrent',
  'us-gaap_LongTermDebtAndCapitalLeaseObligations',
  // Verified live: Capital One (COF) splits its debt across these three
  // concepts (Securitized debt obligations / Senior and subordinated notes
  // / Other borrowings) with none of the concepts above present at all,
  // producing totalDebt = 0 for a company that actually carries ~$50B —
  // which in turn made netDebt hugely NEGATIVE (cash minus zero debt),
  // inflating equity value by the entire "phantom" net-debt swing. Deposits
  // (a much larger balance-sheet liability for any bank) are deliberately
  // NOT included here — unlike these, deposits fund the loan book as a
  // normal part of a bank's operating liabilities, not a discretionary
  // financing choice, so they aren't "debt" in the capital-structure sense
  // this list is for.
  'us-gaap_SecuredDebt',
  'us-gaap_UnsecuredDebt',
  'us-gaap_OtherBorrowings',
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

function extractDcfInputs(reportedFinancials) {
  if (!reportedFinancials.length) return null;
  // Finnhub's financials-reported for a *ticker* (not CIK) can mix in
  // filings from a completely unrelated older company that happened to
  // share the same ticker symbol decades earlier ("ticker recycling") —
  // verified live for CEG (2010-2011 filings from three different old CIKs
  // alongside the real Constellation Energy Corp's, current since its 2022
  // spinoff from Exelon). Filtered to the most recent report's CIK before
  // picking a 10-K, so a `.find()` below can't land on the wrong company.
  const currentCik = reportedFinancials[0].cik;
  const sameCikFinancials = currentCik == null ? reportedFinancials : reportedFinancials.filter((r) => r.cik === currentCik);
  const annual = sameCikFinancials.find((r) => r.form === '10-K') || sameCikFinancials[0];
  const { ic, bs, cf } = annual.report || {};
  if (!ic || !bs || !cf) return null;

  const pretaxIncome =
    findByConcept(ic, [
      'us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
      'us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
    ]) ?? findByLabelKeywords(ic, ['income before', 'income before provision for income tax']);
  const taxExpense = findByConcept(ic, ['us-gaap_IncomeTaxExpenseBenefit']) ?? findByLabelKeywords(ic, ['provision for income tax', 'income tax expense']);
  // Falls back to basic weighted-average shares when a filer reports no
  // diluted share count at all — basic vs. diluted is a minor, not
  // disqualifying, difference for this purpose. As a last resort (verified
  // live: NUTX's 10-K has neither a diluted nor basic share COUNT line
  // anywhere, only the resulting per-share EPS dollar figures), derives it
  // from net income ÷ diluted EPS — a direct mathematical identity, not a
  // guess, since EPS is defined as net income attributable to common
  // shareholders divided by that same share count.
  const dilutedShares =
    findByConcept(ic, ['us-gaap_WeightedAverageNumberOfDilutedSharesOutstanding']) ??
    findByLabelKeywords(ic, ['diluted (in shares)', 'diluted shares', 'weighted average number of shares outstanding, diluted']) ??
    findByConcept(ic, ['us-gaap_WeightedAverageNumberOfSharesOutstandingBasic']) ??
    findByLabelKeywords(ic, ['basic (in shares)', 'basic shares', 'weighted average number of shares outstanding, basic']) ??
    (() => {
      const netIncomeToParent = findByConcept(ic, ['us-gaap_NetIncomeLoss']);
      const dilutedEps = findByConcept(ic, ['us-gaap_EarningsPerShareDiluted']);
      return netIncomeToParent != null && dilutedEps ? netIncomeToParent / dilutedEps : null;
    })();

  // FCFF is built from the officially reported Operating Cash Flow subtotal
  // (+ an after-tax interest-expense addback, − CapEx) rather than
  // reconstructing it from NOPAT + D&A + working-capital changes. That
  // approach was tried first and broke badly on direct-method filers
  // (verified live: CVS reports gross "cash receipts from customers"/"cash
  // paid to suppliers"-style line items instead of the indirect NOPAT-based
  // reconciliation most filers use — summing everything before the OCF
  // subtotal picked up those gross figures as if they were incremental
  // working-capital adjustments, producing a wcChange of $771B against a
  // $133B market cap). The OCF subtotal itself is unambiguous and required
  // to be disclosed either way, so reading it directly sidesteps the
  // direct-vs-indirect presentation difference entirely. Matched by XBRL
  // concept, not label text — verified live that label wording for this
  // exact line varies by filer (Apple's own 10-K says "Cash generated by
  // operating activities," not the more common "Net Cash Provided by (Used
  // in) Operating Activities" that MSFT/KO/NVDA use), while the underlying
  // concept is standardized across all of them.
  const OPERATING_SUBTOTAL_CONCEPTS = ['us-gaap_NetCashProvidedByUsedInOperatingActivities', 'us-gaap_NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'];
  const ocf = findByConcept(cf, OPERATING_SUBTOTAL_CONCEPTS) ?? findByLabelKeywords(cf, ['net cash provided by operating activities', 'net cash used in operating activities', 'cash generated by operating activities']);

  // Verified live: Nutex Health (NUTX) uses a fourth concept,
  // us-gaap_PaymentsToAcquireOtherPropertyPlantAndEquipment ("Payments to
  // Acquire OTHER Property, Plant, and Equipment") — the inserted "Other"
  // also breaks a plain "payments to acquire property" keyword match.
  const capex =
    findByConcept(cf, [
      'us-gaap_PaymentsToAcquirePropertyPlantAndEquipment',
      'us-gaap_PaymentsToAcquireProductiveAssets',
      'us-gaap_PaymentsToAcquireOtherPropertyPlantAndEquipment',
    ]) ??
    findByLabelKeywords(cf, [
      'purchases of property',
      'payments for acquisition of property',
      'payments to acquire property',
      'payments to acquire other property',
      'purchases related to property',
      'capital expenditures',
      'capital spending',
    ]);
  const cash = findByConcept(bs, ['us-gaap_CashAndCashEquivalentsAtCarryingValue', 'us-gaap_CashAndCashEquivalentsAtFairValue', 'us-gaap_Cash']) ?? findByLabelKeywords(bs, ['cash and cash equivalents']);
  const totalDebt = sumDebt(bs);

  // Verified live across several filers (NUTX, CRMD, CEG, KO) that
  // InterestExpenseNonoperating is a common, reusable concept for this.
  // Some filers (verified live: Apple) don't break out interest expense at
  // all, bundling it into a combined "Other income/(expense), net" line
  // with no separate figure to extract — rather than default to a $0
  // addback (which understated Apple's FCFF by its entire real interest
  // expense), estimate it as totalDebt × the same borrowing-rate assumption
  // (RF + DEBT_SPREAD) already used for WACC's cost of debt below, so this
  // isn't a new/arbitrary assumption, just applied a step earlier.
  const interestExpense =
    findByConcept(ic, ['us-gaap_InterestExpenseNonoperating', 'us-gaap_InterestExpense', 'us-gaap_InterestExpenseDebt']) ??
    findByLabelKeywords(ic, ['interest expense']) ??
    findByLabelKeywords(cf, ['interest paid']) ??
    totalDebt * (RF + DEBT_SPREAD);

  if (ocf == null || capex == null || dilutedShares == null || cash == null) return null;

  const taxRate = pretaxIncome != null && taxExpense != null && pretaxIncome > 0 ? Math.max(0, Math.min(0.35, taxExpense / pretaxIncome)) : 0.21; // fall back to the US statutory rate if effective rate isn't derivable

  return { ocf, interestExpense, taxRate, capex, totalDebt, cash, dilutedShares, netDebt: totalDebt - cash };
}

// ---------------------------------------------------------------------------
// FCF Margin reconstruction — for tickers where Finnhub's own quarterly
// fcfMargin data is missing (verified live: CoreWeave/CRWV, and ~1,367 of
// ~5,140 tickers as of a recent coverage audit). Mirrors
// buildFcfMarginFromFilings in src/utils/metrics.js (see that file for the
// full rationale behind each piece) — only the LATEST TTM value is needed
// here, not a full trend, since this pipeline stores one current value per
// metric, not a history. Needs QUARTERLY financials-reported (unlike
// extractDcfInputs above, which only needs the annual 10-K) — fetched
// separately, only for tickers with this specific gap (see the main loop),
// so this doesn't add a call for the ~3,770 tickers that don't need it.

// Mirrors REVENUE_CONCEPT_CANDIDATES/REVENUE_CONTRACT_CONCEPT_PREFIX in
// src/utils/metrics.js — see that file for the full rationale (verified
// live: SoundHound AI/SOUN uses the ...IncludingAssessedTax variant with a
// plural "Revenues" label; an open-ended prefix match catches that AND any
// future suffix variant we haven't individually seen yet, rather than
// growing an exact-match list one ticker at a time).
const REVENUE_CONCEPT_CANDIDATES = ['us-gaap_RevenuesNetOfInterestExpense', 'us-gaap_Revenues', 'us-gaap_SalesRevenueNet'];
const REVENUE_CONTRACT_CONCEPT_PREFIX = 'us-gaap_RevenueFromContractWithCustomer';
const REVENUE_CONTRACT_CONCEPT_EXCLUDE = /RemainingPerformanceObligation|Disaggregation|PolicyTextBlock|TableTextBlock|Liability|Asset|Member|Axis|Domain/i;
const REVENUE_LABEL_PATTERN = /^(total\s+)?(net\s+)?revenues?(,?\s*net)?$|^(total\s+)?net\s+sales$/i;

function findReportedRevenue(icItems) {
  for (const concept of REVENUE_CONCEPT_CANDIDATES) {
    const match = icItems.find((item) => item.concept === concept);
    if (match) return match.value;
  }
  const contractRevenueMatch = icItems.find(
    (item) => item.value != null && (item.concept || '').startsWith(REVENUE_CONTRACT_CONCEPT_PREFIX) && !REVENUE_CONTRACT_CONCEPT_EXCLUDE.test(item.concept)
  );
  if (contractRevenueMatch) return contractRevenueMatch.value;

  const labelMatch = icItems.find((item) => REVENUE_LABEL_PATTERN.test((item.label || '').trim()));
  if (labelMatch) return labelMatch.value;

  // Mirrors findReportedRevenue in src/utils/metrics.js — some banks
  // (verified live: Capital One/COF) report no combined revenue line at
  // all; Net interest income + non-interest income is the standard proxy.
  // Included here for parity even though FCF Margin reconstruction skips
  // financial-industry tickers entirely below — kept in sync regardless.
  const netInterestIncome = icItems.find((item) => item.concept === 'us-gaap_InterestIncomeExpenseNet');
  const nonInterestIncome = icItems.find((item) => item.concept === 'us-gaap_NoninterestIncome');
  if (netInterestIncome && nonInterestIncome) {
    return netInterestIncome.value + nonInterestIncome.value;
  }

  return null;
}

function findReportedOperatingCashFlowQ(cfItems) {
  const match = cfItems.find((item) => item.concept === 'us-gaap_NetCashProvidedByUsedInOperatingActivities');
  if (match) return match.value;
  const labelMatch = cfItems.find((item) => /net cash.*operating activities/i.test(item.label || ''));
  return labelMatch ? labelMatch.value : null;
}

// A separate copy from extractDcfInputs's inline capex extraction above
// (which stays annual-only, unchanged) — this one also recognizes REITs'
// PaymentsForCapitalImprovements (see findReportedCapex in
// src/utils/metrics.js for the full REIT-vs-acquisition-spending rationale).
// Verified live: Nutex Health (NUTX) uses a fourth concept,
// us-gaap_PaymentsToAcquireOtherPropertyPlantAndEquipment ("Payments to
// Acquire OTHER Property, Plant, and Equipment") — the inserted "Other"
// also broke the label-keyword fallback below.
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

// Mirrors decumulateYtdByYear in src/utils/metrics.js — a 10-Q reports P&L
// and cash-flow line items as year-to-date cumulative, so each quarter is
// de-cumulated against the prior one to get a standalone 3-month figure;
// Q4 = the 10-K's full-year figure minus the Q3 YTD figure, since there's
// no standalone Q4 filing. CIK-filtered first to guard against "ticker
// recycling" (an old, unrelated company that once shared this symbol).
function decumulateYtdByYear(quarterlyReports, annualReports, findValue, section = 'ic') {
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

// Mirrors isConsecutiveQuarterWindow in src/utils/metrics.js — a TTM figure
// sums 4 quarters, but the 4 entries at consecutive ARRAY positions aren't
// necessarily consecutive CALENDAR quarters if some were excluded upstream
// (a missing filing, or a de-cumulation gap). Verified live this mattered
// for Boeing's ROIC (a different metric, same underlying risk) — summing a
// stretched-out, non-consecutive window produced a nonsensical 10,334%
// figure. Skips any window that isn't genuinely 4 back-to-back quarters.
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

async function fetchReportedFinancialsQuarterlyFor(symbol, apiKey) {
  const res = await fetch(`https://finnhub.io/api/v1/stock/financials-reported?symbol=${symbol}&freq=quarterly&token=${apiKey}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return Array.isArray(data?.data) ? data.data : [];
}

// ---------------------------------------------------------------------------
// Trend reconstruction from raw SEC filings (Quarterly/Yearly/TTM, all 4
// metrics that don't need Twelve Data price history). Published alongside
// the metrics so the app's trend charts (MetricTrendScreen) can read a
// precomputed, cross-user-shared series instead of every device
// independently reconstructing the same thing live. Computed for every
// ticker (not gated on the native series having a gap) — see the main loop
// below — so both the native (Tier 1) and reconstructed (Tier 2/3) caches
// are always available; the app picks whichever is actually better
// (pickBetterTrend in src/utils/metrics.js), same as it already does for
// the live per-device fallback path.
// ---------------------------------------------------------------------------

const QUARTERS_OF_HISTORY = 12; // mirrors src/utils/metrics.js

function quarterLabelFromPeriod(period) {
  if (!period) return null;
  const d = new Date(period);
  if (Number.isNaN(d.getTime())) return null;
  const q = Math.floor(d.getMonth() / 3) + 1;
  return `Q${q} '${String(d.getFullYear()).slice(-2)}`;
}

// ---------------------------------------------------------------------------
// Native quarterly-series capture (all 6 metrics) — Finnhub's stock/metric
// response (already fetched for every ticker's card values) includes the
// FULL quarterly series, not just the current value extractMetricValues
// pulls out above — previously discarded after that single value was read.
// Publishing it lets the app's trend charts read a precomputed series
// instead of re-fetching stock/metric live on first view every session —
// verified live this was the ONLY thing making P/E's trend chart slow to
// load: unlike the other 5 comparable metrics, P/E has no SEC-filings
// reconstruction fallback to fall back on, so a live fetch was the ENTIRE
// story for it. Mirrors extractQuarterlyMetricSeries in src/utils/metrics.js
// (see that file for the full field-mapping rationale — current/quarterly
// field names don't match 1:1, e.g. current.roiTTM vs quarterly.roicTTM).
// ---------------------------------------------------------------------------

const NATIVE_QUARTERLY_FIELD_MAP = {
  roic: 'roicTTM',
  profitMargin: 'netMargin',
  fcfMargin: 'fcfMargin',
  peRatio: 'peTTM',
  pfcfRatio: 'pfcfTTM',
};

function extractNativeQuarterlySeries(quarterly, metricKey) {
  const series = quarterly || {};
  if (metricKey === 'revenueGrowth') {
    const sales = series.salesPerShare || [];
    const points = [];
    for (let i = 0; i < sales.length - 4 && points.length < QUARTERS_OF_HISTORY; i++) {
      const thisQuarter = sales[i];
      const yearAgo = sales[i + 4];
      const value = thisQuarter?.v != null && yearAgo?.v ? (thisQuarter.v - yearAgo.v) / yearAgo.v : null;
      points.push({ label: quarterLabelFromPeriod(thisQuarter?.period), value });
    }
    return points.reverse();
  }
  const field = NATIVE_QUARTERLY_FIELD_MAP[metricKey];
  const entries = ((field && series[field]) || []).slice(0, QUARTERS_OF_HISTORY);
  return entries.map((entry) => ({ label: quarterLabelFromPeriod(entry?.period), value: entry?.v ?? null })).reverse();
}

// One ticker's native quarterly series across all 6 metrics, keyed by
// metricKey — a metric is only included if at least one point has a real
// (non-null) value, so a ticker with nothing for, say, fcfMargin doesn't
// publish an array of all-null points.
function buildNativeTrendsForTicker(quarterly) {
  const result = {};
  for (const metricKey of ['roic', 'revenueGrowth', 'profitMargin', 'fcfMargin', 'peRatio', 'pfcfRatio']) {
    const points = extractNativeQuarterlySeries(quarterly, metricKey);
    if (points.some((p) => p.value != null)) result[metricKey] = points;
  }
  return result;
}

// Mirrors buildFcfMarginFromFilings in src/utils/metrics.js — full trend,
// not just the latest point. Reuses findReportedOperatingCashFlowQ/
// findReportedCapexQ/findReportedRevenue/decumulateYtdByYear/
// isConsecutiveQuarterWindow already defined above for the latest-value case.
function buildFcfMarginTrendFromFilings(quarterlyReports, annualReports) {
  const ocf = decumulateYtdByYear(quarterlyReports, annualReports, findReportedOperatingCashFlowQ, 'cf');
  const capex = decumulateYtdByYear(quarterlyReports, annualReports, findReportedCapexQ, 'cf');
  const revenue = decumulateYtdByYear(quarterlyReports, annualReports, findReportedRevenue, 'ic');

  // `revenue[key] != null`, not a truthy check — mirrors buildFcfMarginFromFilings
  // in src/utils/metrics.js; see that file for the full rationale (a real
  // reported $0 quarter is a genuine value that belongs in the TTM sum
  // below, not missing data to exclude).
  const standaloneQuarters = Object.keys(ocf)
    .filter((key) => capex[key] != null && revenue[key] != null)
    .map((key) => {
      const [year, quarter] = key.split('-').map(Number);
      return { year, quarter, fcf: ocf[key] - capex[key], revenue: revenue[key] };
    })
    .sort((a, b) => a.year - b.year || a.quarter - b.quarter);

  const points = buildTrailingWindows(standaloneQuarters, 4).map(({ quarters, anchor, partial }) => {
    const ttmFcf = quarters.reduce((sum, q) => sum + q.fcf, 0);
    const ttmRevenue = quarters.reduce((sum, q) => sum + q.revenue, 0);
    const { year, quarter } = anchor;
    return { label: `Q${quarter} '${String(year).slice(-2)}`, value: ttmRevenue ? ttmFcf / ttmRevenue : null, partial, quartersUsed: quarters.length };
  });
  return points.slice(-QUARTERS_OF_HISTORY);
}

// ---------------------------------------------------------------------------
// Quarterly / Yearly / TTM cadence coverage for revenueGrowth, profitMargin,
// and roic (fcfMargin's TTM already exists above; pfcfRatio's three cadences
// live in the SEPARATE pfcfTrendCache pipeline instead, since P/FCF needs
// Twelve Data price history — that provider's 800/day free-tier cap can't
// support fetching a price series for all ~5,140 tickers here). Mirrors the
// identically-named functions in src/utils/metrics.js (built for the app's
// Quarterly/Yearly/TTM trend-chart tabs) — see that file for the full
// rationale behind each piece (MAX_ABS_RATIO's near-zero-denominator
// clamp, the annualization needed for ROIC's Quarterly view, etc.).
// ---------------------------------------------------------------------------

const NET_INCOME_CONCEPT_CANDIDATES = ['us-gaap_NetIncomeLoss', 'us-gaap_ProfitLoss'];
function findReportedNetIncome(icItems) {
  for (const concept of NET_INCOME_CONCEPT_CANDIDATES) {
    const match = icItems.find((item) => item.concept === concept);
    if (match) return match.value;
  }
  const labelMatch = icItems.find((item) => /^net income/i.test(item.label || ''));
  return labelMatch ? labelMatch.value : null;
}

function findReportedEBIT(icItems) {
  const match = icItems.find((item) => item.concept === 'us-gaap_OperatingIncomeLoss');
  if (match) return match.value;
  const labelMatch = icItems.find((item) => /operating income|income from operations/i.test(item.label || ''));
  return labelMatch ? labelMatch.value : null;
}

function findReportedTotalEquity(bsItems) {
  const match = bsItems.find((item) => item.concept === 'us-gaap_StockholdersEquity');
  if (match) return match.value;
  const labelMatch = bsItems.find((item) => /total (share|stock)holders.{0,5}equity/i.test(item.label || ''));
  return labelMatch ? labelMatch.value : null;
}

function findReportedCashBalance(bsItems) {
  const match = bsItems.find((item) => item.concept === 'us-gaap_CashAndCashEquivalentsAtCarryingValue');
  if (match) return match.value;
  const labelMatch = bsItems.find((item) => /cash and cash equivalents/i.test(item.label || ''));
  return labelMatch ? labelMatch.value : null;
}

const ROIC_ASSUMED_TAX_RATE = 0.21; // matches the DCF estimate's own default simplification above

const MAX_ABS_RATIO = 10; // 1000% — see clampImplausible in src/utils/metrics.js for the full rationale
function clampImplausible(value) {
  return value != null && Math.abs(value) <= MAX_ABS_RATIO ? value : null;
}

// Generic YTD-cumulative flow-item de-cumulation across multiple fields at
// once — mirrors buildStandaloneFlowQuarters in src/utils/metrics.js.
function buildStandaloneFlowQuarters(quarterlyReports, annualReports, fieldSpecs) {
  const perField = {};
  for (const [name, { fn, section }] of Object.entries(fieldSpecs)) {
    perField[name] = decumulateYtdByYear(quarterlyReports, annualReports, fn, section);
  }
  const keys = new Set();
  for (const map of Object.values(perField)) Object.keys(map).forEach((k) => keys.add(k));

  return Array.from(keys)
    .map((key) => {
      const [year, quarter] = key.split('-').map(Number);
      const record = { year, quarter };
      for (const name of Object.keys(fieldSpecs)) record[name] = perField[name][key];
      return record;
    })
    .filter((record) => Object.keys(fieldSpecs).every((name) => record[name] != null))
    .sort((a, b) => a.year - b.year || a.quarter - b.quarter);
}

// Reads the ANNUAL report directly (no de-cumulation — a 10-K's own figures
// already ARE the full fiscal year's totals) — mirrors buildAnnualFlowPoints
// in src/utils/metrics.js.
function buildAnnualFlowPoints(annualReports, fieldSpecs) {
  const currentCik = annualReports?.[0]?.cik;
  const sameCik = (r) => currentCik == null || r.cik === currentCik;
  const filtered = (annualReports || []).filter(sameCik);

  return filtered
    .map((a) => {
      const record = { year: a.year };
      for (const [name, { fn, section }] of Object.entries(fieldSpecs)) {
        record[name] = fn(a.report?.[section] || []);
      }
      return record;
    })
    .filter((record) => Object.keys(fieldSpecs).every((name) => record[name] != null))
    .sort((a, b) => a.year - b.year);
}

function yearlyLabel(year) {
  return `FY '${String(year).slice(-2)}`;
}

// --- FCF Margin: Quarterly + Yearly (TTM already exists above) ---

function buildFcfMarginQuarterlyFromFilings(quarterlyReports, annualReports) {
  const records = buildStandaloneFlowQuarters(quarterlyReports, annualReports, {
    ocf: { fn: findReportedOperatingCashFlowQ, section: 'cf' },
    capex: { fn: findReportedCapexQ, section: 'cf' },
    revenue: { fn: findReportedRevenue, section: 'ic' },
  });
  return records
    .map((r) => ({ label: `Q${r.quarter} '${String(r.year).slice(-2)}`, value: clampImplausible(r.revenue ? (r.ocf - r.capex) / r.revenue : null) }))
    .filter((p) => p.value != null)
    .slice(-QUARTERS_OF_HISTORY);
}

function buildFcfMarginYearlyFromFilings(annualReports) {
  const records = buildAnnualFlowPoints(annualReports, {
    ocf: { fn: findReportedOperatingCashFlowQ, section: 'cf' },
    capex: { fn: findReportedCapexQ, section: 'cf' },
    revenue: { fn: findReportedRevenue, section: 'ic' },
  });
  return records
    .map((r) => ({ label: yearlyLabel(r.year), value: clampImplausible(r.revenue ? (r.ocf - r.capex) / r.revenue : null) }))
    .filter((p) => p.value != null)
    .slice(-QUARTERS_OF_HISTORY);
}

// --- Revenue Growth: Quarterly (single-quarter YoY), Yearly, TTM ---

function buildRevenueGrowthQuarterlyFromFilings(quarterlyReports, annualReports) {
  const standalone = decumulateYtdByYear(quarterlyReports, annualReports, findReportedRevenue, 'ic');
  const chronological = Object.keys(standalone)
    .map((key) => {
      const [year, quarter] = key.split('-').map(Number);
      return { year, quarter, value: standalone[key] };
    })
    .sort((a, b) => a.year - b.year || a.quarter - b.quarter);

  const points = [];
  for (let i = 4; i < chronological.length; i++) {
    const current = chronological[i];
    const yearAgo = chronological[i - 4];
    if (yearAgo.year !== current.year - 1 || yearAgo.quarter !== current.quarter) continue;
    const value = clampImplausible(yearAgo.value ? (current.value - yearAgo.value) / yearAgo.value : null);
    if (value != null) points.push({ label: `Q${current.quarter} '${String(current.year).slice(-2)}`, value });
  }
  return points.slice(-QUARTERS_OF_HISTORY);
}

function buildRevenueGrowthYearlyFromFilings(annualReports) {
  const records = buildAnnualFlowPoints(annualReports, { revenue: { fn: findReportedRevenue, section: 'ic' } });
  const points = [];
  for (let i = 1; i < records.length; i++) {
    if (records[i].year !== records[i - 1].year + 1) continue;
    const value = clampImplausible(records[i - 1].revenue ? (records[i].revenue - records[i - 1].revenue) / records[i - 1].revenue : null);
    if (value != null) points.push({ label: yearlyLabel(records[i].year), value });
  }
  return points.slice(-QUARTERS_OF_HISTORY);
}

function buildRevenueGrowthTTMFromFilings(quarterlyReports, annualReports) {
  const records = buildStandaloneFlowQuarters(quarterlyReports, annualReports, { revenue: { fn: findReportedRevenue, section: 'ic' } });
  const windows = buildTrailingWindows(records, 4).filter((w) => !w.partial);

  const ttmByKey = {};
  for (const w of windows) ttmByKey[`${w.anchor.year}-${w.anchor.quarter}`] = w.quarters.reduce((sum, q) => sum + q.revenue, 0);
  const orderedKeys = Object.keys(ttmByKey)
    .map((k) => {
      const [year, quarter] = k.split('-').map(Number);
      return { year, quarter };
    })
    .sort((a, b) => a.year - b.year || a.quarter - b.quarter);

  const points = [];
  for (let i = 4; i < orderedKeys.length; i++) {
    const curr = orderedKeys[i];
    const yearAgo = orderedKeys[i - 4];
    if (yearAgo.year !== curr.year - 1 || yearAgo.quarter !== curr.quarter) continue;
    const currTTM = ttmByKey[`${curr.year}-${curr.quarter}`];
    const yearAgoTTM = ttmByKey[`${yearAgo.year}-${yearAgo.quarter}`];
    const value = clampImplausible(yearAgoTTM ? (currTTM - yearAgoTTM) / yearAgoTTM : null);
    if (value != null) points.push({ label: `Q${curr.quarter} '${String(curr.year).slice(-2)}`, value });
  }
  return points.slice(-QUARTERS_OF_HISTORY);
}

// --- Profit Margin: Quarterly, Yearly, TTM ---

function buildProfitMarginQuarterlyFromFilings(quarterlyReports, annualReports) {
  const revenue = decumulateYtdByYear(quarterlyReports, annualReports, findReportedRevenue, 'ic');
  const netIncome = decumulateYtdByYear(quarterlyReports, annualReports, findReportedNetIncome, 'ic');
  const chronological = Object.keys(revenue)
    .filter((key) => netIncome[key] != null && revenue[key])
    .map((key) => {
      const [year, quarter] = key.split('-').map(Number);
      return { year, quarter, value: clampImplausible(netIncome[key] / revenue[key]) };
    })
    .filter((p) => p.value != null)
    .sort((a, b) => a.year - b.year || a.quarter - b.quarter);
  return chronological.map((p) => ({ label: `Q${p.quarter} '${String(p.year).slice(-2)}`, value: p.value })).slice(-QUARTERS_OF_HISTORY);
}

function buildProfitMarginYearlyFromFilings(annualReports) {
  const records = buildAnnualFlowPoints(annualReports, {
    netIncome: { fn: findReportedNetIncome, section: 'ic' },
    revenue: { fn: findReportedRevenue, section: 'ic' },
  });
  return records
    .map((r) => ({ label: yearlyLabel(r.year), value: clampImplausible(r.revenue ? r.netIncome / r.revenue : null) }))
    .filter((p) => p.value != null)
    .slice(-QUARTERS_OF_HISTORY);
}

function buildProfitMarginTTMFromFilings(quarterlyReports, annualReports) {
  const records = buildStandaloneFlowQuarters(quarterlyReports, annualReports, {
    netIncome: { fn: findReportedNetIncome, section: 'ic' },
    revenue: { fn: findReportedRevenue, section: 'ic' },
  });
  return buildTrailingWindows(records, 4)
    .map(({ quarters, anchor, partial }) => {
      const income = quarters.reduce((sum, q) => sum + q.netIncome, 0);
      const rev = quarters.reduce((sum, q) => sum + q.revenue, 0);
      const value = clampImplausible(rev ? income / rev : null);
      return value != null ? { label: `Q${anchor.quarter} '${String(anchor.year).slice(-2)}`, value, partial, quartersUsed: quarters.length } : null;
    })
    .filter(Boolean)
    .slice(-QUARTERS_OF_HISTORY);
}

// --- ROIC: Quarterly, Yearly, TTM (none of these existed in this pipeline before) ---

function buildRoicQuarterlyFromFilings(quarterlyReports, annualReports) {
  const ebitRecords = buildStandaloneFlowQuarters(quarterlyReports, annualReports, { ebit: { fn: findReportedEBIT, section: 'ic' } });

  const investedCapitalByQuarter = {};
  for (const q of quarterlyReports || []) {
    if (!q?.quarter) continue;
    const equity = findReportedTotalEquity(q.report?.bs || []);
    if (equity == null) continue;
    const debt = sumDebt(q.report?.bs || []);
    const cash = findReportedCashBalance(q.report?.bs || []) || 0;
    investedCapitalByQuarter[`${q.year}-${q.quarter}`] = debt + equity - cash;
  }

  return ebitRecords
    .filter((r) => investedCapitalByQuarter[`${r.year}-${r.quarter}`] > 0)
    .map((r) => {
      // Annualized (x4) — EBIT/NOPAT is a flow figure that shrinks to ~1/4
      // at quarterly granularity, but Invested Capital is a point-in-time
      // balance-sheet snapshot that doesn't shrink with it; see the
      // identical note in buildRoicQuarterlyFromFilings in src/utils/metrics.js.
      const annualizedNopat = r.ebit * (1 - ROIC_ASSUMED_TAX_RATE) * 4;
      const value = clampImplausible(annualizedNopat / investedCapitalByQuarter[`${r.year}-${r.quarter}`]);
      return value != null ? { label: `Q${r.quarter} '${String(r.year).slice(-2)}`, value } : null;
    })
    .filter(Boolean)
    .slice(-QUARTERS_OF_HISTORY);
}

function buildRoicYearlyFromFilings(annualReports) {
  const currentCik = annualReports?.[0]?.cik;
  const sameCik = (r) => currentCik == null || r.cik === currentCik;
  const filtered = (annualReports || []).filter(sameCik);

  const ebitRecords = buildAnnualFlowPoints(filtered, { ebit: { fn: findReportedEBIT, section: 'ic' } });

  return ebitRecords
    .map((r) => {
      const annualReport = filtered.find((a) => a.year === r.year);
      const equity = findReportedTotalEquity(annualReport?.report?.bs || []);
      if (equity == null) return null;
      const debt = sumDebt(annualReport?.report?.bs || []);
      const cash = findReportedCashBalance(annualReport?.report?.bs || []) || 0;
      const investedCapital = debt + equity - cash;
      if (!(investedCapital > 0)) return null;
      const nopat = r.ebit * (1 - ROIC_ASSUMED_TAX_RATE);
      const value = clampImplausible(nopat / investedCapital);
      return value != null ? { label: yearlyLabel(r.year), value } : null;
    })
    .filter(Boolean)
    .slice(-QUARTERS_OF_HISTORY);
}

function buildRoicTTMFromFilings(quarterlyReports, annualReports) {
  const ebit = decumulateYtdByYear(quarterlyReports, annualReports, findReportedEBIT, 'ic');

  const investedCapitalByQuarter = {};
  for (const q of quarterlyReports || []) {
    if (!q?.quarter) continue;
    const equity = findReportedTotalEquity(q.report?.bs || []);
    if (equity == null) continue;
    const debt = sumDebt(q.report?.bs || []);
    const cash = findReportedCashBalance(q.report?.bs || []) || 0;
    investedCapitalByQuarter[`${q.year}-${q.quarter}`] = debt + equity - cash;
  }

  const standaloneQuarters = Object.keys(ebit)
    .filter((key) => investedCapitalByQuarter[key] > 0)
    .map((key) => {
      const [year, quarter] = key.split('-').map(Number);
      return { year, quarter, nopat: ebit[key] * (1 - ROIC_ASSUMED_TAX_RATE), investedCapital: investedCapitalByQuarter[key] };
    })
    .sort((a, b) => a.year - b.year || a.quarter - b.quarter);

  return buildTrailingWindows(standaloneQuarters, 4)
    .map(({ quarters, anchor, partial }) => {
      const ttmNopat = quarters.reduce((sum, q) => sum + q.nopat, 0);
      const value = clampImplausible(ttmNopat / anchor.investedCapital);
      return value != null ? { label: `Q${anchor.quarter} '${String(anchor.year).slice(-2)}`, value, partial, quartersUsed: quarters.length } : null;
    })
    .filter(Boolean)
    .slice(-QUARTERS_OF_HISTORY);
}

// Gist raw URLs for the dataset this same script publishes to (see the
// workflow's git-based "Publish to gist" step) — fetched at the START of a
// run so a bad run doesn't erase a good previous one. Same gist ID
// generateNewsCache.js already reads from. Trend caches live in their own
// files, separate from marketMetrics.json — see OUTPUT_TRENDS_*_FILE above
// for why.
const GIST_METRICS_URL = 'https://gist.githubusercontent.com/jadrayes1/db6fbd96e980118d3c6a63965dc0dc39/raw/marketMetrics.json';
const GIST_TRENDS_NATIVE_URL = 'https://gist.githubusercontent.com/jadrayes1/db6fbd96e980118d3c6a63965dc0dc39/raw/trendsNative.json';
const GIST_TRENDS_QUARTERLY_URL = 'https://gist.githubusercontent.com/jadrayes1/db6fbd96e980118d3c6a63965dc0dc39/raw/trendsQuarterly.json';
const GIST_TRENDS_YEARLY_URL = 'https://gist.githubusercontent.com/jadrayes1/db6fbd96e980118d3c6a63965dc0dc39/raw/trendsYearly.json';
const GIST_TRENDS_TTM_URL = 'https://gist.githubusercontent.com/jadrayes1/db6fbd96e980118d3c6a63965dc0dc39/raw/trendsTtm.json';

async function fetchJsonSafe(url) {
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null; // file doesn't exist yet, or the feed's briefly unreachable — treated as "nothing published"
  }
}

// Fetched ONCE at the start of a run and reused for both the trend
// merge-protection (pickTrendToPublish) and the per-field metrics
// regression guard (pickMetricValue) below.
async function fetchPreviouslyPublishedDataset() {
  const asObject = (v) => (v && typeof v === 'object' ? v : {});
  const [metricsData, nativeData, quarterlyData, yearlyData, ttmData] = await Promise.all([
    fetchJsonSafe(GIST_METRICS_URL),
    fetchJsonSafe(GIST_TRENDS_NATIVE_URL),
    fetchJsonSafe(GIST_TRENDS_QUARTERLY_URL),
    fetchJsonSafe(GIST_TRENDS_YEARLY_URL),
    fetchJsonSafe(GIST_TRENDS_TTM_URL),
  ]);

  // Trend caches used to live embedded inside marketMetrics.json itself
  // (trends.native/quarterly/yearly/ttm) before they outgrew the Gist
  // API's 1MB-per-file publish limit at full-universe scale and were split
  // into their own files. Read that old embedded location as a fallback
  // for anything the new dedicated files don't have yet, so the
  // transition doesn't look like data vanished.
  const nativeTrends = { ...asObject(metricsData?.trends?.native), ...asObject(nativeData?.trends) };
  const quarterlyTrends = { ...asObject(metricsData?.trends?.quarterly), ...asObject(quarterlyData?.trends) };
  const yearlyTrends = { ...asObject(metricsData?.trends?.yearly), ...asObject(yearlyData?.trends) };
  const ttmTrends = { ...asObject(metricsData?.trends?.ttm), ...asObject(ttmData?.trends) };

  // Even older: trends.fcfMargin (pre-cadence, TTM-only) at the very top
  // level — read as a fallback for whatever ttmTrends[symbol].fcfMargin
  // doesn't already have.
  const legacyFcfMargin = asObject(metricsData?.trends?.fcfMargin);
  for (const [symbol, points] of Object.entries(legacyFcfMargin)) {
    ttmTrends[symbol] = { ...ttmTrends[symbol], fcfMargin: ttmTrends[symbol]?.fcfMargin ?? points };
  }

  return {
    metrics: asObject(metricsData?.metrics),
    nativeTrends,
    yearlyTrends,
    quarterlyTrends,
    ttmTrends,
  };
}

// Mirrors pickTrendToPublish's reasoning, for a single scalar metric value
// instead of a trend array: a fresh fetch coming back null doesn't mean the
// underlying fact went away — verified live: IAG (IAMGOLD, a Canadian
// foreign-private issuer cross-listed on NYSE American) previously had real
// values here, then Finnhub's own data for it regressed to entirely empty
// (0 quarterly entries across every metric) with no code change on our
// side. Rather than publish a sudden gap that's more likely a transient or
// upstream data-availability issue than a real fundamental change, keep
// the last known-good value per field until a fresh, non-null value
// actually replaces it. Deliberately NOT applied to estimatedFairValue —
// that field going null is frequently an intentional decision by the
// current code (the sanity guard rejecting an implausible estimate), not a
// data gap, and republishing a stale estimate the current code would
// reject defeats the point of that guard.
function pickMetricValue(existingValue, freshValue) {
  return freshValue != null ? freshValue : (existingValue ?? null);
}

// A fresh reconstruction attempt can come back empty, or narrower than
// before, on a day where Finnhub has a transient hiccup for this specific
// ticker — that's not the same fact as "this ticker's trend data no longer
// exists," and shouldn't blank out (or shrink) what's already published.
// Only replaces the cached trend when the fresh attempt actually surfaces a
// calendar quarter (by label) the cache doesn't already have — a same-or-
// narrower result leaves the previously-published trend untouched.
function pickTrendToPublish(existingPoints, freshPoints) {
  if (!freshPoints || freshPoints.length === 0) return existingPoints || [];
  if (!existingPoints || existingPoints.length === 0) return freshPoints;
  const existingLabels = new Set(existingPoints.map((p) => p.label));
  const hasNewQuarter = freshPoints.some((p) => !existingLabels.has(p.label));
  return hasNewQuarter ? freshPoints : existingPoints;
}

const RF = 0.047; // 10-year Treasury yield — update periodically, doesn't need to be exact to the day
const ERP = 0.035; // equity risk premium — near the low end of published estimates (Damodaran's implied ERP has run ~4-4.5%); chosen after regression-testing against FMP's own DCF on AAPL/MSFT/KO/NVDA showed our WACC was running high enough to systematically under-value vs. FMP across the board
const TERMINAL_GROWTH = 0.03; // slightly above pure long-run nominal GDP growth convention (2.5%) — same regression-test rationale as ERP above
const DEBT_SPREAD = 0.015; // assumed spread over Rf for cost of debt (no per-ticker interest-expense data)
const GROWTH_CAP = 0.5; // caps near-term growth input — see file header; validated against FMP on AAPL/MSFT/KO/NVDA
const PROJECTION_YEARS = 10;

// Mirrors FINANCIAL_INDUSTRIES/isFinancialIndustry in src/utils/metrics.js.
const FINANCIAL_INDUSTRIES = new Set(['Banking', 'Insurance', 'Financial Services']);
function isFinancialIndustry(industry) {
  return !!industry && FINANCIAL_INDUSTRIES.has(industry);
}

/**
 * Estimated Fair Value per share via a two-stage FCFF DCF: reported
 * Operating Cash Flow + after-tax interest expense - CapEx (see
 * extractDcfInputs for why this is built from OCF directly rather than
 * NOPAT + D&A + working-capital changes), projected forward with growth
 * fading linearly from a capped near-term rate to TERMINAL_GROWTH,
 * discounted at a market-cap-weighted, CAPM-based WACC, then bridged from
 * enterprise value to equity value by subtracting net debt. See the file
 * header for the validation numbers and known limitations (still a
 * materially different estimate than FMP's own DCF, not a replica of it).
 */
function computeEstimatedFairValue(dcfInputs, current, marketCap, industry) {
  const { ocf, interestExpense, taxRate, capex, netDebt, dilutedShares } = dcfInputs;
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

  // Standard alternative FCFF formula (Damodaran): OCF already reflects
  // whatever working-capital effect happened, regardless of how the filer
  // presents it — add back after-tax interest expense (since OCF is net of
  // actual cash interest paid, a financing effect that shouldn't reduce
  // cash flow available to both debt and equity holders), then subtract CapEx.
  let fcff = ocf + interestExpense * (1 - taxRate) - capex;
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
  if (fairValue <= 0) return null;

  // Sanity guard: verified live that NUTX's diluted-share-count fallback
  // (derived from net income ÷ diluted EPS — see extractDcfInputs) produced
  // a fair value 70x its real price ($10,320 vs. $147), almost certainly
  // from a share-count inconsistency in that filing rather than a genuine
  // "our model disagrees with the market" signal. An independently-derived
  // estimate disagreeing with the market by 2-3x is plausible; 8x+ either
  // way is far more likely a data/extraction artifact than a real thesis —
  // discard rather than surface a number this implausible.
  //
  // Tighter for Banking/Insurance/Financial Services specifically (2.5x,
  // not 8x): auditing a live run found 54 of 242 financial-industry
  // tickers still implausible (>2.5x price) even with correctly-extracted,
  // non-zero debt — not a fixable extraction bug like NUTX's, but the same
  // structural issue already documented above and for JPM: a bank's
  // operating cash flow is dominated by loan-book growth, not
  // distributable earnings, so FCFF is routinely enormous relative to
  // market cap for this sector. 8x was tuned for the NUTX-style artifact
  // case, not for a sector where the underlying formula itself is a poor
  // fit — verified live: even after fixing Capital One's actual missing
  // debt concepts, its estimate is $1,756 against a $222 price (7.9x, on
  // ~$40.6B of "FCFF" against a $133.5B market cap, a ~30% FCFF yield).
  const impliedPrice = marketCap / dilutedShares;
  const sanityMultiple = isFinancialIndustry(industry) ? 2.5 : 8;
  if (impliedPrice > 0 && (fairValue > impliedPrice * sanityMultiple || fairValue < impliedPrice / sanityMultiple)) return null;

  return fairValue;
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
      const raw = symbols.map((s) => metrics[s][key]);
      // A negative P/E or P/FCF means negative earnings/cash flow — a real
      // problem, not a bargain, even though it's numerically the smallest
      // value for a "lower is better" metric. Excluded from the ranking
      // population so it can't (a) make a money-losing company look like
      // the cheapest/best in its sector once the direction flips below, or
      // (b) distort other peers' ranks by sitting artificially low in the
      // population. A candidate's own negative value is handled explicitly
      // below instead of running it through this population at all.
      populations[key] = LOWER_IS_BETTER.has(key) ? raw.filter((v) => v == null || v >= 0) : raw;
    }

    const eligible = symbols.filter((s) => COMPARABLE_KEYS.every((k) => metrics[s][k] !== null && metrics[s][k] !== undefined));

    let best = null;
    for (const symbol of eligible) {
      const data = metrics[symbol];
      const trendQualified = profiles[symbol]?.trendQualified || {};
      const goodnessScores = [];
      let topMetricCount = 0;
      for (const key of COMPARABLE_KEYS) {
        const isNegativeLowerIsBetter = LOWER_IS_BETTER.has(key) && data[key] < 0;
        const pct = isNegativeLowerIsBetter ? 100 : percentileRank(data[key], populations[key]);
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

// Fetches and computes everything for a single ticker, using the given
// Finnhub API key. Deliberately returns a result descriptor rather than
// mutating shared state, so runWorker below can call this from N
// independent, concurrently-running loops (one per API key) without them
// stepping on each other — each worker still processes ITS tickers
// strictly sequentially, pacing every call against its own key's rate
// limit exactly as the single-worker loop always has; only the workers
// themselves run in parallel.
async function processSymbol(symbol, apiKey, ctx) {
  const { previouslyPublishedMetrics, previouslyPublishedNativeTrends, previouslyPublishedYearlyTrends, previouslyPublishedQuarterlyTrends, previouslyPublishedTtmTrends } =
    ctx;

  const profile = await fetchProfileFor(symbol, apiKey);

  // Verified live: Finnhub returns a 200 OK with every profile field empty
  // (no name, no industry, nothing) for some symbols in the exchange=US
  // universe list (example: AACO) — stale/delisted/inactive tickers Finnhub
  // itself has no real data for, not a fetch failure. Excluded from the
  // dataset entirely rather than stored with all-null metrics; checking
  // here (before the other calls) also skips those calls for a ticker we
  // already know has nothing.
  if (!profile.name) {
    return { status: 'dead' };
  }

  await sleep(REQUEST_SPACING_MS);
  const { current, quarterly } = await fetchMetricsFor(symbol, apiKey);

  // netMargin's quarterly series is one of the more consistently-present
  // fields (see QUARTERLY_FIELD_MAP in src/utils/metrics.js) — used here
  // purely as a proxy for "how many quarters has Finnhub got on this
  // ticker," for the Industry Leaders history bias (see historyWeight).
  const profileEntry = {
    ...profile,
    historyQuarters: (quarterly.netMargin || []).length,
    trendQualified: computeTrendQualification(quarterly),
  };

  let estimatedFairValue = null;
  let annualReportedFinancials = [];
  let dcfComputed = false;
  await sleep(REQUEST_SPACING_MS);
  try {
    annualReportedFinancials = await fetchReportedFinancialsFor(symbol, apiKey);
    const dcfInputs = extractDcfInputs(annualReportedFinancials);
    if (dcfInputs) {
      estimatedFairValue = computeEstimatedFairValue(dcfInputs, current, profile.marketCapitalization, profile.industry);
      if (estimatedFairValue != null) dcfComputed = true;
    }
  } catch {
    // A single ticker's oddly-shaped filing (or a failed request) shouldn't
    // take down the whole run — just skip its estimate, same graceful-
    // degradation philosophy as the rest of this pipeline. Its sector-
    // percentile metrics are unaffected.
  }

  const values = extractMetricValues(current, quarterly, impliedPriceFromProfile(profile));

  // Tier 1 — native quarterly series (all 6 metrics), straight from the
  // stock/metric response already fetched above for the current values.
  // Zero extra API cost — this is data already in hand, just not
  // previously published. See buildNativeTrendsForTicker's own comments
  // for why this matters (it's the whole fix for P/E's trend chart, which
  // has no reconstruction fallback of its own).
  const freshNativeTrends = buildNativeTrendsForTicker(quarterly);
  const previousNativeForSymbol = previouslyPublishedNativeTrends[symbol] || {};
  const mergedNativeForSymbol = {};
  for (const key of ['roic', 'revenueGrowth', 'profitMargin', 'fcfMargin', 'peRatio', 'pfcfRatio']) {
    const published = pickTrendToPublish(previousNativeForSymbol[key], freshNativeTrends[key]);
    if (published.length) mergedNativeForSymbol[key] = published;
  }

  // Tier 2/3 — Quarterly/Yearly/TTM reconstruction from raw SEC filings,
  // for the 4 metrics that don't need Twelve Data price history. Quarterly
  // financials-reported is now fetched for EVERY ticker (not just ones
  // with a native-data gap) so these caches cover the full universe, not
  // just gap tickers — a deliberate, longer run in exchange for every
  // metric's trend chart loading from a precomputed cache instead of
  // live-reconstructing on first tap. annualReportedFinancials is already
  // fetched above for the DCF estimate, at no extra cost either way.
  let quarterlyFinancials = [];
  await sleep(REQUEST_SPACING_MS);
  try {
    quarterlyFinancials = await fetchReportedFinancialsQuarterlyFor(symbol, apiKey);
  } catch {
    // Leave quarterlyFinancials empty — every builder below degrades
    // gracefully to "nothing new," and pickTrendToPublish falls back to
    // whatever was already published rather than losing it.
  }

  const isBankLike = isFinancialIndustry(profile.industry);
  const yearlyBuilders = {
    revenueGrowth: () => buildRevenueGrowthYearlyFromFilings(annualReportedFinancials),
    profitMargin: () => buildProfitMarginYearlyFromFilings(annualReportedFinancials),
    fcfMargin: () => (isBankLike ? [] : buildFcfMarginYearlyFromFilings(annualReportedFinancials)),
    roic: () => buildRoicYearlyFromFilings(annualReportedFinancials),
  };
  const quarterlyBuilders = {
    revenueGrowth: () => buildRevenueGrowthQuarterlyFromFilings(quarterlyFinancials, annualReportedFinancials),
    profitMargin: () => buildProfitMarginQuarterlyFromFilings(quarterlyFinancials, annualReportedFinancials),
    fcfMargin: () => (isBankLike ? [] : buildFcfMarginQuarterlyFromFilings(quarterlyFinancials, annualReportedFinancials)),
    roic: () => buildRoicQuarterlyFromFilings(quarterlyFinancials, annualReportedFinancials),
  };
  const ttmBuilders = {
    revenueGrowth: () => buildRevenueGrowthTTMFromFilings(quarterlyFinancials, annualReportedFinancials),
    profitMargin: () => buildProfitMarginTTMFromFilings(quarterlyFinancials, annualReportedFinancials),
    fcfMargin: () => (isBankLike ? [] : buildFcfMarginTrendFromFilings(quarterlyFinancials, annualReportedFinancials)),
    roic: () => buildRoicTTMFromFilings(quarterlyFinancials, annualReportedFinancials),
  };

  const mergedYearlyForSymbol = {};
  const mergedQuarterlyForSymbol = {};
  const mergedTtmForSymbol = {};
  const previousYearlyForSymbol = previouslyPublishedYearlyTrends[symbol] || {};
  const previousQuarterlyForSymbol = previouslyPublishedQuarterlyTrends[symbol] || {};
  const previousTtmForSymbol = previouslyPublishedTtmTrends[symbol] || {};

  let fcfMarginReconstructed = false;

  for (const key of Object.keys(yearlyBuilders)) {
    let fresh = [];
    try {
      fresh = yearlyBuilders[key]();
    } catch {
      // A single metric's oddly-shaped filing shouldn't take down the
      // others — pickTrendToPublish falls back to the previous run.
    }
    const published = pickTrendToPublish(previousYearlyForSymbol[key], fresh);
    if (published.length) mergedYearlyForSymbol[key] = published;
  }
  for (const key of Object.keys(quarterlyBuilders)) {
    let fresh = [];
    try {
      fresh = quarterlyBuilders[key]();
    } catch {
      // Same graceful-degradation philosophy as above.
    }
    const published = pickTrendToPublish(previousQuarterlyForSymbol[key], fresh);
    if (published.length) mergedQuarterlyForSymbol[key] = published;
  }
  for (const key of Object.keys(ttmBuilders)) {
    let fresh = [];
    try {
      fresh = ttmBuilders[key]();
    } catch {
      // Same graceful-degradation philosophy as above.
    }
    const published = pickTrendToPublish(previousTtmForSymbol[key], fresh);
    if (published.length) {
      mergedTtmForSymbol[key] = published;
      if (key === 'fcfMargin' && values.fcfMargin == null) {
        values.fcfMargin = published[published.length - 1].value;
        fcfMarginReconstructed = true;
      }
    }
  }

  // Regression guard: a fresh null for one of the 6 comparable metrics
  // doesn't overwrite a previously-published real value for the same field
  // (see pickMetricValue above) — applied per field, not as an all-or-
  // nothing choice, so a ticker with one genuinely new gap (say, a metric
  // Finnhub just stopped reporting) still gets its OTHER, still-healthy
  // fields refreshed normally.
  const previous = previouslyPublishedMetrics[symbol];
  if (previous) {
    for (const key of COMPARABLE_KEYS) {
      values[key] = pickMetricValue(previous[key], values[key]);
    }
  }

  return {
    status: 'ok',
    profile: profileEntry,
    metrics: { industry: profile.industry, ...values, estimatedFairValue },
    nativeTrends: Object.keys(mergedNativeForSymbol).length ? mergedNativeForSymbol : null,
    yearlyTrends: Object.keys(mergedYearlyForSymbol).length ? mergedYearlyForSymbol : null,
    quarterlyTrends: Object.keys(mergedQuarterlyForSymbol).length ? mergedQuarterlyForSymbol : null,
    ttmTrends: Object.keys(mergedTtmForSymbol).length ? mergedTtmForSymbol : null,
    dcfComputed,
    fcfMarginReconstructed,
  };
}

// Sequentially processes one worker's slice of the ticker universe against
// one Finnhub API key, pacing every call by REQUEST_SPACING_MS exactly as
// the original single-worker loop did — the only thing that changed is
// that main() now runs one of these per available key, concurrently.
async function runWorker(workerId, symbolSubset, apiKey, ctx) {
  const result = {
    metrics: {},
    profiles: {},
    nativeTrends: {},
    yearlyTrends: {},
    quarterlyTrends: {},
    ttmTrends: {},
    ok: 0,
    failed: 0,
    dead: 0,
    dcfComputed: 0,
    fcfMarginReconstructed: 0,
  };

  for (let i = 0; i < symbolSubset.length; i++) {
    const symbol = symbolSubset[i];
    try {
      const r = await processSymbol(symbol, apiKey, ctx);
      if (r.status === 'dead') {
        result.dead++;
      } else {
        result.profiles[symbol] = r.profile;
        result.metrics[symbol] = r.metrics;
        if (r.nativeTrends) result.nativeTrends[symbol] = r.nativeTrends;
        if (r.yearlyTrends) result.yearlyTrends[symbol] = r.yearlyTrends;
        if (r.quarterlyTrends) result.quarterlyTrends[symbol] = r.quarterlyTrends;
        if (r.ttmTrends) result.ttmTrends[symbol] = r.ttmTrends;
        if (r.dcfComputed) result.dcfComputed++;
        if (r.fcfMarginReconstructed) result.fcfMarginReconstructed++;
        result.ok++;
      }
    } catch (err) {
      result.failed++;
      console.log(`  [key ${workerId}] skip ${symbol}: ${err.message}`);
    }

    const processedSoFar = result.ok + result.failed + result.dead;
    if (processedSoFar % 100 === 0 || i === symbolSubset.length - 1) {
      console.log(
        `  [key ${workerId}] ${processedSoFar}/${symbolSubset.length} (${result.ok} ok, ${result.failed} failed, ${result.dead} dead/no-data, ` +
          `${result.dcfComputed} with a fair-value estimate, ${result.fcfMarginReconstructed} FCF margins reconstructed)`
      );
    }

    if (i < symbolSubset.length - 1) await sleep(REQUEST_SPACING_MS);
  }

  return result;
}

async function main() {
  const apiKeys = readFinnhubApiKeys();
  const symbols = await fetchUniverse(apiKeys[0]);
  console.log(
    `Fetching industry + fundamentals + DCF inputs for ${symbols.length} tickers across ${apiKeys.length} API key(s) ` +
      `(four Finnhub calls each — the fourth is quarterly financials-reported, now fetched for every ticker — rate-limited to stay under Finnhub's free-tier cap per key)...`
  );

  const previouslyPublished = await fetchPreviouslyPublishedDataset();
  const { metrics: previouslyPublishedMetrics } = previouslyPublished;
  console.log(
    `Loaded ${Object.keys(previouslyPublishedMetrics).length} previously-published tickers' metrics ` +
      `(merge-protected against a bad run — see pickMetricValue/pickTrendToPublish).`
  );

  const ctx = {
    previouslyPublishedMetrics,
    previouslyPublishedNativeTrends: previouslyPublished.nativeTrends,
    previouslyPublishedYearlyTrends: previouslyPublished.yearlyTrends,
    previouslyPublishedQuarterlyTrends: previouslyPublished.quarterlyTrends,
    previouslyPublishedTtmTrends: previouslyPublished.ttmTrends,
  };

  // Split the universe across one worker per key, interleaved (round-robin
  // by index) rather than contiguous halves — cheap insurance against any
  // systematic clustering in the alphabetically-sorted universe (e.g. a
  // run of consecutive delisted tickers) skewing one worker's actual
  // workload heavier than the other's.
  const workerSymbolSubsets = apiKeys.map(() => []);
  symbols.forEach((symbol, i) => workerSymbolSubsets[i % apiKeys.length].push(symbol));

  const workerResults = await Promise.all(apiKeys.map((key, idx) => runWorker(idx + 1, workerSymbolSubsets[idx], key, ctx)));

  const metrics = {};
  const profiles = {}; // name/logo, kept out of `metrics` to avoid bloating that map — only industryLeaders needs them
  // Every trend series this pipeline publishes, by cadence — native (straight
  // from Finnhub's own quarterly series, all 6 metrics), and quarterly/
  // yearly/ttm (reconstructed from SEC filings, for the 4 metrics that don't
  // need Twelve Data price history — see the file header for why pfcfRatio's
  // three cadences live in the separate pfcfTrendCache pipeline instead).
  // Each is `{ [symbol]: { [metricKey]: points } }`.
  const nativeTrends = {};
  const yearlyTrends = {};
  const quarterlyTrends = {};
  const ttmTrends = {};
  let ok = 0;
  let failed = 0;
  let dead = 0;
  let dcfComputed = 0;
  let fcfMarginReconstructed = 0;

  // Each worker owns a disjoint slice of `symbols`, so these merges never
  // collide.
  for (const r of workerResults) {
    Object.assign(metrics, r.metrics);
    Object.assign(profiles, r.profiles);
    Object.assign(nativeTrends, r.nativeTrends);
    Object.assign(yearlyTrends, r.yearlyTrends);
    Object.assign(quarterlyTrends, r.quarterlyTrends);
    Object.assign(ttmTrends, r.ttmTrends);
    ok += r.ok;
    failed += r.failed;
    dead += r.dead;
    dcfComputed += r.dcfComputed;
    fcfMarginReconstructed += r.fcfMarginReconstructed;
  }

  const industryLeaders = computeIndustryLeaders(metrics, profiles);
  console.log(`\nComputed ${industryLeaders.length} industry leaders (industries with >= ${MIN_INDUSTRY_PEERS} peers).`);

  const generatedAt = new Date().toISOString();
  // marketMetrics.json stays small — no trends embedded (see
  // OUTPUT_TRENDS_*_FILE above for why they moved to their own files).
  // Unformatted JSON.stringify (no indentation) everywhere below too —
  // pretty-printing was adding meaningful overhead for no benefit on a
  // machine-generated, never-hand-edited file, and every byte matters now
  // that these files run into the tens of megabytes at full-universe scale.
  const output = { generatedAt, metrics, industryLeaders };
  fs.mkdirSync(path.dirname(OUTPUT_FILE), { recursive: true }); // works regardless of which repo this script runs in
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output));
  fs.writeFileSync(OUTPUT_TRENDS_NATIVE_FILE, JSON.stringify({ generatedAt, trends: nativeTrends }));
  fs.writeFileSync(OUTPUT_TRENDS_QUARTERLY_FILE, JSON.stringify({ generatedAt, trends: quarterlyTrends }));
  fs.writeFileSync(OUTPUT_TRENDS_YEARLY_FILE, JSON.stringify({ generatedAt, trends: yearlyTrends }));
  fs.writeFileSync(OUTPUT_TRENDS_TTM_FILE, JSON.stringify({ generatedAt, trends: ttmTrends }));
  console.log(
    `Wrote ${ok} tickers to ${path.relative(process.cwd(), OUTPUT_FILE)} ` +
      `(${failed} failed, ${dead} dead/no-data excluded, ${dcfComputed} with a fair-value estimate, ${fcfMarginReconstructed} FCF margins freshly reconstructed). ` +
      `Trend caches published: ${Object.keys(nativeTrends).length} native, ${Object.keys(quarterlyTrends).length} quarterly, ` +
      `${Object.keys(yearlyTrends).length} yearly, ${Object.keys(ttmTrends).length} ttm.`
  );
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { extractDcfInputs, computeEstimatedFairValue, readFinnhubApiKeys, processSymbol, runWorker };
