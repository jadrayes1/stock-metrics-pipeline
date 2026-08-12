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
// Also computes `industryLeaders`: up to MAX_LEADERS_PER_INDUSTRY tickers
// per industry with at least MIN_INDUSTRY_PEERS peers, each required to
// have all 6 comparable metrics present AND rank top-quintile in at least
// 5 of them (see computeIndustryLeaders below for why — a ticker with only
// 1 of 6 metrics present was otherwise winning on a single outlier/likely-
// erroneous number), lightly biased toward tickers with more reported
// quarters (see historyWeight). One grouped entry per industry
// ({industry, peerCount, leaders: [...]}), not a flat per-ticker list — see
// IndustryLeaders.js for the per-industry carousel this renders as.
// Precomputed here rather than on-device so the Home screen's carousels are
// just reading a small pre-baked list, same "compute once daily, app just
// reads" philosophy as everything else this pipeline produces.
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

// Tickers that changed symbol (not a dual-class-share situation — same
// company, same CIK, Finnhub just hasn't backfilled financials-reported
// history under the new symbol yet) — verified live (Aug 2026): BNY (Bank
// of New York Mellon, renamed from BK) has 0 annual and only 1 quarterly
// financials-reported entry under "BNY", while "BK" — same CIK 1390777 —
// has 15 years of annual and 47 quarters of history. /stock/metric and
// /stock/profile2 are unaffected (BNY's own quarterly ratio series and
// profile are both healthy) — this is specific to financials-reported,
// unlike DUAL_CLASS_ALIASES above which covers the ratio engine instead.
// AD (Array Digital Infrastructure) is the same pattern, verified live:
// SEC confirms it's the renamed United States Cellular Corp (CIK 821130,
// former name on record "UNITED STATES CELLULAR CORP" through 2025-07-25)
// — "AD" itself has only 2 annual financials-reported entries, while
// "USM" has 44 quarters of real history under the same CIK.
// Mirrored in src/utils/metrics.js — keep in sync.
const RENAMED_TICKER_FINANCIALS_ALIASES = {
  BNY: 'BK',
  AD: 'USM',
};

// financials-reported also needs the dual-class redirect (DUAL_CLASS_ALIASES
// above, which was deliberately scoped to only the /stock/metric ratio
// engine when first added) — verified live this session that the gap is
// just as real here: HEI.A has 0 quarterly financials-reported entries
// while HEI has 47; MOG.B has 0 while MOG.A has 8. Same underlying
// reasoning as DUAL_CLASS_ALIASES: revenue/net income/OCF are company-wide
// totals, identical between share classes regardless of per-share price,
// so redirecting is the correct number, not an approximation. Checked
// after RENAMED_TICKER_FINANCIALS_ALIASES so the two never conflict (no
// ticker is currently in both maps).
function resolveFinancialsReportedSymbol(symbol) {
  return RENAMED_TICKER_FINANCIALS_ALIASES[symbol] || DUAL_CLASS_ALIASES[symbol] || symbol;
}

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
  const requestSymbol = resolveFinancialsReportedSymbol(symbol);
  const res = await fetch(`https://finnhub.io/api/v1/stock/financials-reported?symbol=${requestSymbol}&freq=annual&token=${apiKey}`);
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

// Hoisted to module scope (previously local to extractDcfInputs) so the SEC
// XBRL enrichment code below can reuse the same concept list rather than
// duplicating it — see extractDcfInputs' own OCF extraction, and
// findReportedOperatingCashFlowQ below (a separate, quarterly-oriented
// copy with its own label fallback), for why direct-vs-indirect cash-flow
// presentation makes matching by concept the reliable path here.
const OPERATING_SUBTOTAL_CONCEPTS = ['us-gaap_NetCashProvidedByUsedInOperatingActivities', 'us-gaap_NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'];

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

// ---------------------------------------------------------------------------
// SEC XBRL fallback for revenue-growth gaps — Finnhub's own financials-
// reported crawl occasionally skips a single quarter entirely (verified
// live: Dominion Energy/D has NO Q1 2024 report at all, jumping straight
// from Q4 2023 to Q2 2024). revenueGrowth is the only one of this
// pipeline's reconstructed metrics that needs a YoY (year-ago) comparable
// quarter to compute a rate — profitMargin/fcfMargin/roic are self-
// contained per-period ratios, no prior-year pairing needed — so a single
// missing quarter silently breaks every revenueGrowth point that would
// need to reach across it, while leaving the other three metrics
// untouched. SEC's own companyfacts API has the real figure (verified
// live: D's real standalone Q1 2024 revenue, $3,632,000,000, filed
// 2024-05-02, an exact match to the value later re-reported as a
// comparative column in three subsequent SEC filings). This fills ONLY
// the specific missing quarter(s) actually needed — detected first via
// free array inspection, so a SEC request is only spent on tickers that
// actually have a gap — not a general re-architecture of this pipeline's
// Finnhub-based approach.
const SEC_TICKERS_URL = 'https://www.sec.gov/files/company_tickers.json';
const SEC_COMPANYFACTS_BASE = 'https://data.sec.gov/api/xbrl/companyfacts';
const SEC_USER_AGENT = 'stock-analyzer-app stock-metrics-pipeline contact:jadrayescpp@gmail.com';
const SEC_FETCH_TIMEOUT_MS = 30000; // see extractFilingTextFacts.js in the sibling foreign-filings-pipeline repo for why a bare fetch() needs an explicit ceiling
const SEC_REVENUE_CONCEPTS = ['Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax', 'RevenueFromContractWithCustomerIncludingAssessedTax', 'SalesRevenueNet', 'RevenuesNetOfInterestExpense'];

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
      // SEC's own convention hyphenates share-class suffixes (BRK-A), while
      // Finnhub (and every symbol this file's callers pass in) uses a
      // period (BRK.A) - verified live this silently broke the SEC-XBRL
      // enrichment lookup for BRK.A itself (ctx.secTickerToCikMap.get('BRK.A')
      // returned undefined even though SEC's own list has the company under
      // 'BRK-A', so the enrichment path never fired at all despite the
      // sparse-data check correctly triggering). Registering both spellings
      // here fixes every current and future lookup site at once, not just
      // the one this was caught on.
      if (ticker.includes('-')) map.set(ticker.replace(/-/g, '.'), cik);
    }
  }
  return map;
}

// Finds (year, quarter) pairs present in quarterlyReports whose (year-1,
// quarter) counterpart is missing — a gap that would block a YoY revenue-
// growth comparison for that quarter. Each gap's expected {start, end} is
// inferred from the EXISTING report's own dates, shifted back one year —
// only valid for calendar-aligned fiscal quarters, but since the caller
// requires an EXACT date match against SEC's real data, a wrong guess here
// (a non-calendar fiscal year) just fails to find a match rather than
// silently filling in a wrong period.
function findRevenueGapsNeedingBackfill(quarterlyReports) {
  const present = new Set((quarterlyReports || []).filter((r) => r?.year && r?.quarter).map((r) => `${r.year}-${r.quarter}`));
  const shiftYear = (dateStr) => {
    const d = new Date(dateStr);
    return new Date(Date.UTC(d.getUTCFullYear() - 1, d.getUTCMonth(), d.getUTCDate())).toISOString().slice(0, 10);
  };
  const gaps = [];
  for (const r of quarterlyReports || []) {
    if (!r?.year || !r?.quarter || !r.startDate || !r.endDate) continue;
    if (present.has(`${r.year - 1}-${r.quarter}`)) continue;
    gaps.push({ year: r.year - 1, quarter: r.quarter, expectedStart: shiftYear(r.startDate), expectedEnd: shiftYear(r.endDate) });
  }
  return gaps;
}

// SEC facts keyed "start|end" -> value, across every candidate revenue
// concept, for an exact-date lookup against each gap found above.
async function fetchSecRevenueFactsByPeriod(cik) {
  const data = await fetchSecJson(`${SEC_COMPANYFACTS_BASE}/CIK${cik}.json`);
  const gaap = data?.facts?.['us-gaap'] || {};
  const byPeriod = new Map();
  for (const concept of SEC_REVENUE_CONCEPTS) {
    const facts = gaap[concept]?.units?.USD || [];
    for (const f of facts) {
      if (f.val == null || !f.start || !f.end) continue;
      const key = `${f.start}|${f.end}`;
      if (!byPeriod.has(key)) byPeriod.set(key, f.val);
    }
  }
  return byPeriod;
}

// Finds fiscal years with quarterly coverage in quarterlyReports whose
// matching ANNUAL report is entirely missing from annualReports — blocks
// Q4 derivation (Q4 = full-year total - 9mo YTD) for that year, and
// therefore every TTM window that would need to anchor on or span past
// it. Verified live: Dominion Energy/D's own Finnhub annual reports jump
// straight from 2022 to 2025, missing 2023 and 2024 entirely, even though
// it has real quarterly reports for both years. Expected {start, end} is
// inferred from an existing annual report's own dates, shifted to the
// missing year — same reasoning as findRevenueGapsNeedingBackfill above.
function findAnnualRevenueGapsNeedingBackfill(quarterlyReports, annualReports) {
  const annualYears = new Set((annualReports || []).filter((r) => r?.year).map((r) => r.year));
  const quarterlyYears = new Set((quarterlyReports || []).filter((r) => r?.year && r?.quarter).map((r) => r.year));
  const referenceAnnual = (annualReports || []).find((r) => r.startDate && r.endDate);
  if (!referenceAnnual) return [];
  const shiftYear = (dateStr, deltaYears) => {
    const d = new Date(dateStr);
    return new Date(Date.UTC(d.getUTCFullYear() + deltaYears, d.getUTCMonth(), d.getUTCDate())).toISOString().slice(0, 10);
  };
  const gaps = [];
  for (const year of quarterlyYears) {
    if (annualYears.has(year)) continue;
    const delta = year - referenceAnnual.year;
    gaps.push({ year, expectedStart: shiftYear(referenceAnnual.startDate, delta), expectedEnd: shiftYear(referenceAnnual.endDate, delta) });
  }
  return gaps;
}

// Backfills gaps in both quarterlyReports' AND annualReports' revenue
// coverage from SEC's own companyfacts API — see the section header
// comment above. Synthetic entries are shaped just enough for
// findReportedRevenue/decumulateYtdByYear/buildAnnualFlowPoints to work
// completely unmodified (a single us-gaap_Revenues item in the ic
// section, matching the real reports' own cik so decumulateYtdByYear's
// sameCik filter keeps it) — never a fabricated value, only a real
// SEC-filed figure at an exact date match. Returns
// {quarterlyReports, annualReports}, both unchanged when nothing to fill.
async function backfillRevenueGapsFromSec(symbol, cik, quarterlyReports, annualReports) {
  const quarterlyGaps = findRevenueGapsNeedingBackfill(quarterlyReports);
  const annualGaps = findAnnualRevenueGapsNeedingBackfill(quarterlyReports, annualReports);
  if (!quarterlyGaps.length && !annualGaps.length) return { quarterlyReports, annualReports };
  if (!cik) return { quarterlyReports, annualReports };

  const byPeriod = await fetchSecRevenueFactsByPeriod(cik);
  if (!byPeriod.size) return { quarterlyReports, annualReports };

  const referenceCik = quarterlyReports[0]?.cik ?? annualReports?.[0]?.cik;
  const filledQuarters = [];
  for (const gap of quarterlyGaps) {
    const value = byPeriod.get(`${gap.expectedStart}|${gap.expectedEnd}`);
    if (value == null) continue;
    filledQuarters.push({
      year: gap.year,
      quarter: gap.quarter,
      cik: referenceCik,
      startDate: `${gap.expectedStart} 00:00:00`,
      endDate: `${gap.expectedEnd} 00:00:00`,
      report: { ic: [{ concept: 'us-gaap_Revenues', label: 'Revenues (SEC XBRL fallback)', value }] },
    });
  }
  const filledAnnuals = [];
  for (const gap of annualGaps) {
    const value = byPeriod.get(`${gap.expectedStart}|${gap.expectedEnd}`);
    if (value == null) continue;
    filledAnnuals.push({
      year: gap.year,
      cik: referenceCik,
      startDate: `${gap.expectedStart} 00:00:00`,
      endDate: `${gap.expectedEnd} 00:00:00`,
      report: { ic: [{ concept: 'us-gaap_Revenues', label: 'Revenues (SEC XBRL fallback)', value }] },
    });
  }
  if (filledQuarters.length || filledAnnuals.length) {
    console.log(`  ${symbol}: backfilled ${filledQuarters.length} quarterly + ${filledAnnuals.length} annual revenue gap(s) from SEC (Finnhub crawl gap)`);
  }
  return {
    quarterlyReports: filledQuarters.length ? [...quarterlyReports, ...filledQuarters] : quarterlyReports,
    annualReports: filledAnnuals.length ? [...(annualReports || []), ...filledAnnuals] : annualReports,
  };
}

// ---------------------------------------------------------------------------
// Broad SEC XBRL enrichment — for tickers whose Finnhub financials-reported
// coverage is severely sparse (verified live: Berkshire Hathaway/BRK.A has
// exactly 1 historical report ever), not just missing an isolated quarter
// next to otherwise-healthy data (the case backfillRevenueGapsFromSec above
// already handles). The gap-finders above both need an EXISTING report to
// date-shift from — BRK.A has none to anchor annual gaps, and only one to
// anchor a single backward-looking quarterly gap — so they structurally
// can't reach a ticker this sparse. This fetches SEC's companyfacts once
// per sparse ticker (free, same call the narrow fallback already makes)
// and synthesizes report entries for every concept across ic/cf/bs, using
// SEC's own fy/fp fields to assign year/quarter directly (no date-shift
// guessing needed — SEC already labels every fact with its fiscal quarter).
// ---------------------------------------------------------------------------

const SEC_ENRICHMENT_SPARSE_QUARTERLY_THRESHOLD = 4; // can't complete even one TTM window below this
const SEC_ENRICHMENT_SPARSE_ANNUAL_THRESHOLD = 2; // can't compute a single YoY annual point below this

// Same 2 concepts as NET_INCOME_CONCEPT_CANDIDATES further down in this
// file (kept as a plain literal here, not a reference to that const,
// purely because it's declared after this point and JS's temporal dead
// zone would throw on a forward reference at module-init time) — keep
// these two lists in sync if either changes.
const SEC_NET_INCOME_CONCEPTS = ['NetIncomeLoss', 'ProfitLoss'];
const SEC_EBIT_CONCEPTS = ['OperatingIncomeLoss'];
// Mirrors extractDcfInputs' own pretaxIncome concepts above — used the same
// way findReportedEBIT falls back to pre-tax income for bank filers with no
// standard operating-income line.
const SEC_PRETAX_INCOME_CONCEPTS = [
  'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
  'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
];
const SEC_OCF_CONCEPTS = OPERATING_SUBTOTAL_CONCEPTS.map((c) => c.replace(/^us-gaap_/, ''));
const SEC_CAPEX_CONCEPTS = ['PaymentsToAcquirePropertyPlantAndEquipment', 'PaymentsToAcquireProductiveAssets', 'PaymentsForCapitalImprovements', 'PaymentsToAcquireOtherPropertyPlantAndEquipment'];
const SEC_EQUITY_CONCEPTS = ['StockholdersEquity'];
const SEC_DEBT_CONCEPTS = DEBT_CONCEPTS.map((c) => c.replace(/^us-gaap_/, ''));
const SEC_CASH_CONCEPTS = ['CashAndCashEquivalentsAtCarryingValue', 'CashAndCashEquivalentsAtFairValue', 'Cash'];

async function fetchSecUsGaapFacts(cik) {
  const data = await fetchSecJson(`${SEC_COMPANYFACTS_BASE}/CIK${cik}.json`);
  return data?.facts?.['us-gaap'] || {};
}

// Flow (ic/cf) concepts: Finnhub's own quarterly reports are YTD-cumulative
// (a Q2 report's value is the 6-month total; decumulateYtdByYear subtracts
// consecutive cumulative values to get a standalone quarter) — so for the
// unmodified decumulateYtdByYear to treat a synthesized quarter correctly,
// it must be given the CUMULATIVE fact, not the exact-quarter one. SEC
// tags both under the same fy/fp for many concepts (verified live for
// BRK.A's NetIncomeLoss: both a start=2026-04-01/end=2026-06-30 exact-Q2
// fact AND a start=2026-01-01/end=2026-06-30 cumulative fact exist,
// identically fy=2026/fp="Q2") — the cumulative one always has the
// EARLIER start date for the same fy/fp, since it spans more months.
// Known accepted risk: a smaller filer that only tags the exact-quarter
// fact (no cumulative one) for some concept would be misread as
// cumulative here, under-counting after de-cumulation — this pipeline has
// no per-point reconciliation cross-check today (unlike the foreign-
// filings pipeline's extractor), so this isn't caught automatically.
function pickDurationFact(facts, fy, fp) {
  const matches = (facts || []).filter((f) => f.fy === fy && f.fp === fp && f.start && f.end && f.val != null);
  if (!matches.length) return null;
  matches.sort((a, b) => new Date(a.start) - new Date(b.start) || new Date(b.filed || 0) - new Date(a.filed || 0));
  return matches[0];
}

// Balance-sheet (bs) concepts are instant (point-in-time, no `start`) —
// take whichever matches fy/fp, most-recently-filed if more than one does
// (a later filing's own comparative figure for the same period, not a
// restatement we'd want to prefer over the original — but any real
// disclosed value for the period is acceptable here, unlike flow items
// where picking the wrong SHAPE, not just the wrong instance, corrupts
// de-cumulation).
function pickInstantFact(facts, fy, fp) {
  const matches = (facts || []).filter((f) => f.fy === fy && f.fp === fp && !f.start && f.end && f.val != null);
  if (!matches.length) return null;
  matches.sort((a, b) => new Date(b.filed || 0) - new Date(a.filed || 0));
  return matches[0];
}

function findSecValueForFyFp(gaapFacts, concepts, fy, fp, kind) {
  for (const concept of concepts) {
    const facts = gaapFacts[concept]?.units?.USD || [];
    const fact = kind === 'instant' ? pickInstantFact(facts, fy, fp) : pickDurationFact(facts, fy, fp);
    if (fact) return { value: fact.val, concept: `us-gaap_${concept}` };
  }
  return null;
}

// Every real (fy, fp) pair present across the income-statement-relevant
// concepts — fp is already SEC's own quarter label (Q1-Q4, or "FY" for the
// full year), so no date-shift guessing is needed the way the narrow
// gap-finders above need one (this is exactly what lets this reach a
// ticker like BRK.A with almost no Finnhub anchor to shift from).
function collectSecFyFpKeys(gaapFacts, seedConcepts) {
  const keys = new Map();
  for (const concept of seedConcepts) {
    for (const fact of gaapFacts[concept]?.units?.USD || []) {
      if (fact.fy == null || !fact.fp || fact.val == null) continue;
      keys.set(`${fact.fy}-${fact.fp}`, { fy: fact.fy, fp: fact.fp });
    }
  }
  return Array.from(keys.values());
}

// Builds report entries shaped exactly like the real Finnhub ones the 12
// existing Quarterly/Yearly/TTM builders already consume — no
// startDate/endDate (only the OLD narrow gap-finders above read those; the
// builders only read .year/.quarter/.cik/.report), so these plug in with
// zero changes to any downstream builder, de-cumulation, or trailing-
// window logic.
function buildSecSyntheticReports(gaapFacts, cik) {
  const fyFpKeys = collectSecFyFpKeys(gaapFacts, [...SEC_REVENUE_CONCEPTS, ...SEC_NET_INCOME_CONCEPTS, ...SEC_EBIT_CONCEPTS, ...SEC_PRETAX_INCOME_CONCEPTS]);
  const quarterlyReports = [];
  const annualReports = [];
  const quarterNumber = { Q1: 1, Q2: 2, Q3: 3, Q4: 4 };

  for (const { fy, fp } of fyFpKeys) {
    const icItems = [findSecValueForFyFp(gaapFacts, SEC_REVENUE_CONCEPTS, fy, fp, 'duration'), findSecValueForFyFp(gaapFacts, SEC_NET_INCOME_CONCEPTS, fy, fp, 'duration')]
      .filter(Boolean)
      .map((r) => ({ concept: r.concept, label: `${r.concept} (SEC XBRL enrichment)`, value: r.value }));
    const ebitFact = findSecValueForFyFp(gaapFacts, SEC_EBIT_CONCEPTS, fy, fp, 'duration') || findSecValueForFyFp(gaapFacts, SEC_PRETAX_INCOME_CONCEPTS, fy, fp, 'duration');
    if (ebitFact) icItems.push({ concept: ebitFact.concept, label: `${ebitFact.concept} (SEC XBRL enrichment)`, value: ebitFact.value });

    const cfItems = [findSecValueForFyFp(gaapFacts, SEC_OCF_CONCEPTS, fy, fp, 'duration'), findSecValueForFyFp(gaapFacts, SEC_CAPEX_CONCEPTS, fy, fp, 'duration')]
      .filter(Boolean)
      .map((r) => ({ concept: r.concept, label: `${r.concept} (SEC XBRL enrichment)`, value: r.value }));

    const bsItems = [findSecValueForFyFp(gaapFacts, SEC_EQUITY_CONCEPTS, fy, fp, 'instant'), findSecValueForFyFp(gaapFacts, SEC_CASH_CONCEPTS, fy, fp, 'instant')].filter(Boolean);
    for (const debtConcept of SEC_DEBT_CONCEPTS) {
      const debtFact = findSecValueForFyFp(gaapFacts, [debtConcept], fy, fp, 'instant');
      if (debtFact) bsItems.push(debtFact); // one item per matched debt concept — sumDebt already sums across concepts, not pre-summed here
    }
    const bsReportItems = bsItems.map((r) => ({ concept: r.concept, label: `${r.concept} (SEC XBRL enrichment)`, value: r.value }));

    if (!icItems.length && !cfItems.length && !bsReportItems.length) continue;
    const entry = { cik, form: 'SEC-XBRL', report: { ic: icItems, cf: cfItems, bs: bsReportItems } };
    if (fp === 'FY') annualReports.push({ ...entry, year: fy });
    else if (quarterNumber[fp]) quarterlyReports.push({ ...entry, year: fy, quarter: quarterNumber[fp] });
  }
  return { quarterlyReports, annualReports };
}

// Additive merge — real Finnhub entries always win on a (year, quarter)/
// (year) conflict, since they typically carry more validated line items
// already; SEC-synthesized entries only fill in combos Finnhub has none of
// at all (mirrors backfillRevenueGapsFromSec's own additive-only pattern
// above, just keyed for a broader merge rather than a specific gap list).
function mergeSyntheticReports(quarterlyReports, annualReports, synthesized) {
  const existingQuarterKeys = new Set((quarterlyReports || []).filter((r) => r?.year && r?.quarter).map((r) => `${r.year}-${r.quarter}`));
  const existingAnnualYears = new Set((annualReports || []).filter((r) => r?.year).map((r) => r.year));
  const newQuarters = synthesized.quarterlyReports.filter((r) => !existingQuarterKeys.has(`${r.year}-${r.quarter}`));
  const newAnnuals = synthesized.annualReports.filter((r) => !existingAnnualYears.has(r.year));
  return {
    quarterlyReports: newQuarters.length ? [...(quarterlyReports || []), ...newQuarters] : quarterlyReports || [],
    annualReports: newAnnuals.length ? [...(annualReports || []), ...newAnnuals] : annualReports || [],
  };
}

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
  if (labelMatch) return labelMatch.value;
  // Same class of gap as findReportedEBIT's pretax fallback: the label
  // regex above can't match buildSecSyntheticReports' concept-name-derived
  // labels (no spaces) - checked by exact concept name for the OTHER
  // OPERATING_SUBTOTAL_CONCEPTS entry (ContinuingOperations variant), which
  // buildSecSyntheticReports can also synthesize under when a filer only
  // tags that one and not the primary concept above.
  const continuingOpsMatch = cfItems.find((item) => item.concept === 'us-gaap_NetCashProvidedByUsedInOperatingActivitiesContinuingOperations');
  return continuingOpsMatch ? continuingOpsMatch.value : null;
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
  const requestSymbol = resolveFinancialsReportedSymbol(symbol);
  const res = await fetch(`https://finnhub.io/api/v1/stock/financials-reported?symbol=${requestSymbol}&freq=quarterly&token=${apiKey}`);
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
  if (labelMatch) return labelMatch.value;

  // Banks and other financial-services filers don't report a standard
  // "Operating Income" line at all — verified live: Bank of New York
  // Mellon has zero years with us-gaap_OperatingIncomeLoss across 15 years
  // of filings. For these, pre-tax income IS the closest EBIT-equivalent —
  // interest income/expense are core banking operations for a bank, not a
  // separate financing layer sitting below an operating-income line the
  // way they are for an industrial company (same reasoning already behind
  // fcfMargin being skipped entirely for banks — see isFinancialIndustry).
  // Matched by LABEL, not a fixed concept name, since filers tag this
  // under inconsistent extension concepts (BNY's is a company-specific
  // bk_IncomeLossFromContinuing... extension, not a standard us-gaap one).
  // Mirrored in src/utils/metrics.js — keep in sync.
  const preTaxMatch = icItems.find((item) => /pre-?tax income|income before.*tax/i.test((item.label || '').trim()));
  if (preTaxMatch) return preTaxMatch.value;

  // SEC-XBRL-enrichment-synthesized entries (buildSecSyntheticReports)
  // carry a concept-name-derived label ("us-gaap_IncomeLoss...BeforeIncome
  // Taxes... (SEC XBRL enrichment)"), not natural spaced text - the label
  // regex above can never match it even though the underlying value is the
  // same pretax-income fallback. Verified live: BRK.A only got a ROIC
  // value for the 2 years (2011, 2012) it happens to tag the real
  // OperatingIncomeLoss concept; every other year (2009-2010, 2013-2025)
  // had a real pretax-income value present in the ic array that this exact-
  // concept check now catches. Checked by concept name, matching how
  // buildSecSyntheticReports itself labeled the fact.
  const preTaxConceptMatch = icItems.find((item) => SEC_PRETAX_INCOME_CONCEPTS.some((c) => item.concept === `us-gaap_${c}`));
  return preTaxConceptMatch ? preTaxConceptMatch.value : null;
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
  if (labelMatch) return labelMatch.value;
  // Same class of gap as findReportedEBIT's pretax fallback: the label
  // regex above can't match buildSecSyntheticReports' concept-name-derived
  // labels (no spaces) - checked by exact concept name for the other two
  // SEC_CASH_CONCEPTS entries, which buildSecSyntheticReports can also
  // synthesize under when a filer tags cash under one of those instead of
  // the primary concept above.
  const altMatch = bsItems.find((item) => item.concept === 'us-gaap_CashAndCashEquivalentsAtFairValue' || item.concept === 'us-gaap_Cash');
  return altMatch ? altMatch.value : null;
}

// ---------------------------------------------------------------------------
// Cross-cadence derivation — fills a missing quarter/year using ONLY real
// arithmetic on already-known real (or SEC-enriched, from above) facts,
// never fabrication: a missing annual total = sum of 4 known real quarters;
// a missing single quarter = the real annual total minus the 3 other known
// quarters. decumulateYtdByYear above already derives Q4 specifically this
// way (annual minus the Q3 YTD cumulative) as part of ordinary
// de-cumulation; this generalizes to whichever ONE of Q1-Q4 is actually
// missing, operating on the STANDALONE quarters decumulateYtdByYear already
// produces (so it never touches or duplicates that existing logic — a
// quarter it already filled is simply not "missing" by the time this runs).
// Runs after Stage 2's SEC enrichment, before the 12 Quarterly/Yearly/TTM
// builders — so a ticker like BRK.A, once enriched with real SEC facts,
// gets any further one-off cadence gaps closed by pure arithmetic on top.
// ---------------------------------------------------------------------------

function deriveMissingQuarterFromAnnual(standaloneByKey, annualByYear) {
  const enriched = { ...standaloneByKey };
  const years = new Set([...Object.keys(annualByYear).map(Number), ...Object.keys(standaloneByKey).map((k) => Number(k.split('-')[0]))]);
  for (const year of years) {
    if (annualByYear[year] == null) continue;
    const known = [1, 2, 3, 4].filter((q) => enriched[`${year}-${q}`] != null);
    const missing = [1, 2, 3, 4].filter((q) => enriched[`${year}-${q}`] == null);
    if (known.length !== 3 || missing.length !== 1) continue;
    enriched[`${year}-${missing[0]}`] = annualByYear[year] - known.reduce((sum, q) => sum + enriched[`${year}-${q}`], 0);
  }
  return enriched;
}

function deriveMissingAnnualFromQuarters(standaloneByKey, annualByYear) {
  const enriched = { ...annualByYear };
  const years = new Set(Object.keys(standaloneByKey).map((k) => Number(k.split('-')[0])));
  for (const year of years) {
    if (enriched[year] != null) continue;
    const quarters = [1, 2, 3, 4].map((q) => standaloneByKey[`${year}-${q}`]);
    if (quarters.some((v) => v == null)) continue;
    enriched[year] = quarters.reduce((sum, v) => sum + v, 0);
  }
  return enriched;
}

// One flow field's derivation pass, returning only what's NEWLY derived
// (not already real) for synthesis by deriveCrossCadenceReports below.
function deriveFlowField(quarterlyReports, annualReports, fn, section) {
  const standalone = decumulateYtdByYear(quarterlyReports, annualReports, fn, section);
  const currentCik = quarterlyReports?.[0]?.cik ?? annualReports?.[0]?.cik;
  const sameCik = (r) => currentCik == null || r.cik === currentCik;
  const annualByYear = {};
  for (const a of (annualReports || []).filter(sameCik)) {
    const value = fn(a.report?.[section] || []);
    if (value != null) annualByYear[a.year] = value;
  }

  // Per-year, these two states are mutually exclusive (a year with all 4
  // real quarters has nothing left to fill; a year with exactly 3 has no
  // annual to derive from summing quarters) — the ordering here doesn't
  // change correctness, just runs the annual pass first defensively.
  const withAnnuals = deriveMissingAnnualFromQuarters(standalone, annualByYear);
  const withQuarters = deriveMissingQuarterFromAnnual(standalone, withAnnuals);

  const newQuarters = new Map();
  for (const key of Object.keys(withQuarters)) {
    if (standalone[key] == null) newQuarters.set(key, withQuarters[key]);
  }
  const newAnnuals = new Map();
  for (const yearStr of Object.keys(withAnnuals)) {
    const year = Number(yearStr);
    if (annualByYear[year] == null) newAnnuals.set(year, withAnnuals[year]);
  }
  return { newQuarters, newAnnuals };
}

// Orchestrates derivation across the 5 flow fields feeding revenueGrowth/
// profitMargin/fcfMargin/roic's EBIT half, appending newly-derived entries
// to quarterlyReports/annualReports in the same report shape the 12
// builders already consume. ROIC's invested-capital half (bs section) is
// deliberately NOT derived here — a balance-sheet snapshot isn't additive
// across quarters the way a flow is — so a derived EBIT quarter with no
// matching real/SEC-enriched bs entry simply falls out of
// buildRoicQuarterlyFromFilings' own investedCapitalByQuarter > 0 filter,
// same as it does today.
const CROSS_CADENCE_FLOW_FIELDS = [
  { name: 'revenue', fn: findReportedRevenue, section: 'ic', concept: 'us-gaap_Revenues' },
  { name: 'netIncome', fn: findReportedNetIncome, section: 'ic', concept: 'us-gaap_NetIncomeLoss' },
  { name: 'ebit', fn: findReportedEBIT, section: 'ic', concept: 'us-gaap_OperatingIncomeLoss' },
  { name: 'ocf', fn: findReportedOperatingCashFlowQ, section: 'cf', concept: 'us-gaap_NetCashProvidedByUsedInOperatingActivities' },
  { name: 'capex', fn: findReportedCapexQ, section: 'cf', concept: 'us-gaap_PaymentsToAcquirePropertyPlantAndEquipment' },
];

function deriveCrossCadenceReports(quarterlyReports, annualReports) {
  const currentCik = quarterlyReports?.[0]?.cik ?? annualReports?.[0]?.cik;
  // key -> {ic:[], cf:[]} accumulator across fields, so multiple derived
  // fields for the SAME quarter/year land in one report entry, matching
  // how a real filing's report carries every line item together.
  const quarterlyAccum = new Map();
  const annualAccum = new Map();

  for (const { name, fn, section, concept } of CROSS_CADENCE_FLOW_FIELDS) {
    const { newQuarters, newAnnuals } = deriveFlowField(quarterlyReports, annualReports, fn, section);
    for (const [key, value] of newQuarters) {
      if (!quarterlyAccum.has(key)) quarterlyAccum.set(key, { ic: [], cf: [] });
      quarterlyAccum.get(key)[section].push({ concept, label: `${name} (derived cross-cadence)`, value });
    }
    for (const [year, value] of newAnnuals) {
      if (!annualAccum.has(year)) annualAccum.set(year, { ic: [], cf: [] });
      annualAccum.get(year)[section].push({ concept, label: `${name} (derived cross-cadence)`, value });
    }
  }

  const derivedQuarters = Array.from(quarterlyAccum.entries()).map(([key, report]) => {
    const [year, quarter] = key.split('-').map(Number);
    return { year, quarter, cik: currentCik, form: 'DERIVED', derivedCrossCadence: true, report };
  });
  const derivedAnnuals = Array.from(annualAccum.entries()).map(([year, report]) => ({ year, cik: currentCik, form: 'DERIVED', derivedCrossCadence: true, report }));

  return {
    quarterlyReports: derivedQuarters.length ? [...(quarterlyReports || []), ...derivedQuarters] : quarterlyReports || [],
    annualReports: derivedAnnuals.length ? [...(annualReports || []), ...derivedAnnuals] : annualReports || [],
  };
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

  // Looks up the year-ago quarter by its actual (year, quarter) key, not by
  // a fixed "4 array positions back" offset — verified live this matters:
  // Dominion Energy/D has a 2009-to-2022 gap in its own reported history
  // (unrelated to any specific missing quarter), which throws the
  // positional offset off for every quarter from 2022 onward. The old code
  // correctly VALIDATED that a wrong positional guess didn't match (year-1,
  // same quarter) and skipped it — but skipped the quarter entirely instead
  // of actually looking up the real year-ago record, silently losing every
  // point past the first gap in a ticker's history.
  const byKey = new Map(chronological.map((c) => [`${c.year}-${c.quarter}`, c]));
  const points = [];
  for (const current of chronological) {
    const yearAgo = byKey.get(`${current.year - 1}-${current.quarter}`);
    if (!yearAgo) continue;
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

  // Key-based year-ago lookup, not a fixed positional offset — same fix
  // and same reasoning as buildRevenueGrowthQuarterlyFromFilings above.
  const points = [];
  for (const curr of orderedKeys) {
    const yearAgoKey = `${curr.year - 1}-${curr.quarter}`;
    if (!ttmByKey[yearAgoKey]) continue;
    const currTTM = ttmByKey[`${curr.year}-${curr.quarter}`];
    const yearAgoTTM = ttmByKey[yearAgoKey];
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

function buildRoicQuarterlyFromFilings(quarterlyReports, annualReports, isBank) {
  const ebitRecords = buildStandaloneFlowQuarters(quarterlyReports, annualReports, { ebit: { fn: findReportedEBIT, section: 'ic' } });

  const investedCapitalByQuarter = {};
  for (const q of quarterlyReports || []) {
    if (!q?.quarter) continue;
    const equity = findReportedTotalEquity(q.report?.bs || []);
    if (equity == null) continue;
    const debt = sumDebt(q.report?.bs || []);
    // Cash isn't netted out for bank filers — verified live: a bank's
    // reported cash balance includes central-bank reserves and interbank
    // deposits, core OPERATING assets, not idle/excess cash the way it is
    // for an industrial company. Netting it out can collapse invested
    // capital to a fraction of real equity, wildly inflating ROIC (verified
    // live on the IFRS equivalent of this same formula: BMO's ROIC computed
    // to 34-57% instead of its real ~3%). Debt is untouched (already ~0 for
    // most banks — see sumDebt's own deliberate exclusion of deposits).
    const cash = isBank ? 0 : findReportedCashBalance(q.report?.bs || []) || 0;
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

function buildRoicYearlyFromFilings(annualReports, isBank) {
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
      // See buildRoicQuarterlyFromFilings above for why cash isn't netted
      // out for bank filers.
      const cash = isBank ? 0 : findReportedCashBalance(annualReport?.report?.bs || []) || 0;
      const investedCapital = debt + equity - cash;
      if (!(investedCapital > 0)) return null;
      const nopat = r.ebit * (1 - ROIC_ASSUMED_TAX_RATE);
      const value = clampImplausible(nopat / investedCapital);
      return value != null ? { label: yearlyLabel(r.year), value } : null;
    })
    .filter(Boolean)
    .slice(-QUARTERS_OF_HISTORY);
}

function buildRoicTTMFromFilings(quarterlyReports, annualReports, isBank) {
  const ebit = decumulateYtdByYear(quarterlyReports, annualReports, findReportedEBIT, 'ic');

  const investedCapitalByQuarter = {};
  for (const q of quarterlyReports || []) {
    if (!q?.quarter) continue;
    const equity = findReportedTotalEquity(q.report?.bs || []);
    if (equity == null) continue;
    const debt = sumDebt(q.report?.bs || []);
    // See buildRoicQuarterlyFromFilings above for why cash isn't netted
    // out for bank filers.
    const cash = isBank ? 0 : findReportedCashBalance(q.report?.bs || []) || 0;
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
      const rawNopat = quarters.reduce((sum, q) => sum + q.nopat, 0);
      // A partial window (< 4 quarters) sums fewer than a full year of
      // NOPAT, but investedCapital is still a full-year-scale balance-sheet
      // snapshot — left unannualized, a 1-quarter partial window understates
      // ROIC ~4x relative to a genuine trailing-twelve-month figure (same
      // reasoning buildRoicQuarterlyFromFilings above already applies to a
      // single quarter). Verified live in the foreign-filings-pipeline
      // sibling of this function: an unannualized single-quarter value slipped
      // under the sanity clamp while the properly-annualized quarterly figure
      // for the same quarter was correctly clamped as implausible.
      const ttmNopat = partial ? rawNopat * (4 / quarters.length) : rawNopat;
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

// ---------------------------------------------------------------------------
// Cross-cadence recency consistency — a Quarterly/Yearly/TTM trend built
// independently per cadence can end up anchored to wildly different eras
// for the same ticker (verified live: IAG's yearly.revenueGrowth reached
// FY'25 while quarterly.revenueGrowth was still stuck at Q3/Q4 2017) even
// though every point in both is individually real — each cadence just
// truncates to its own last QUARTERS_OF_HISTORY points with no check on
// how OLD that most-recent point actually is. This is the backstop AFTER
// the SEC enrichment (above) and cross-cadence derivation (above) have
// already maximized real data reuse — it only ever removes a series whose
// newest point is genuinely stale, never trims or alters a fresh one.
// ---------------------------------------------------------------------------

const RECENCY_CUTOFF_MONTHS = 18; // generous enough to absorb normal annual/quarterly filing lag, well short of "years stale"

// Coarse by design (quarter/year-END approximation from a label like
// "Q3 '17" or "FY '25") — adequate for an 18-month freshness check, not
// meant to be date-exact. Mirrors this file's own yearlyLabel/
// quarterLabelFromPeriod formats (the only two label shapes ever
// published here) — see those functions above.
function parseLabelToApproxDate(label) {
  if (!label) return null;
  const yearMatch = label.match(/'(\d{2})$/);
  if (!yearMatch) return null;
  const year = 2000 + Number(yearMatch[1]);
  const quarterMatch = label.match(/^Q([1-4])\s/);
  if (quarterMatch) {
    const quarterEndMonth = { 1: 2, 2: 5, 3: 8, 4: 11 }[Number(quarterMatch[1])]; // 0-indexed month of quarter-end
    return new Date(Date.UTC(year, quarterEndMonth + 1, 0)); // last real day of that month
  }
  if (label.startsWith("FY '")) return new Date(Date.UTC(year, 11, 31));
  return null;
}

// A label that fails to parse fails OPEN (treated as recent) — never
// silently drop real data over a parsing gap; the label shapes this file
// publishes are a closed, known set, so an unparseable label here would
// itself be a bug worth surfacing, not a reason to discard real points.
function isRecentEnough(points, cutoffMonths = RECENCY_CUTOFF_MONTHS) {
  if (!Array.isArray(points) || !points.length) return false;
  const dates = points.map((p) => parseLabelToApproxDate(p.label)).filter(Boolean);
  if (!dates.length) return true;
  const cutoff = new Date();
  cutoff.setUTCMonth(cutoff.getUTCMonth() - cutoffMonths);
  return Math.max(...dates.map((d) => d.getTime())) >= cutoff.getTime();
}

// Wraps pickTrendToPublish with the recency floor on BOTH sides of the
// comparison. Gating `existingPoints` too (not just the final result) is
// required, not optional — mirrors the FY-label migration lesson from the
// sibling foreign-filings pipeline: a fresh run that recomputes the exact
// same stale labels never trips pickTrendToPublish's hasNewQuarter check,
// so a stale already-published series would survive forever if only the
// fresh/final side were ever checked.
function pickCadenceMetric(existingPoints, freshPoints) {
  const recentExisting = isRecentEnough(existingPoints) ? existingPoints : [];
  const picked = pickTrendToPublish(recentExisting, freshPoints);
  return isRecentEnough(picked) ? picked : [];
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
const MAX_LEADERS_PER_INDUSTRY = 5; // show up to this many qualifying tickers per industry, not just the single best

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
 * Up to MAX_LEADERS_PER_INDUSTRY qualifying tickers per industry (>=
 * MIN_INDUSTRY_PEERS peers only), one grouped entry per industry rather than
 * a flat per-ticker list — see IndustryLeaders.js for the per-industry
 * carousel this shape is meant to render. A candidate must have:
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
 * Among qualifying candidates, ranked by composite score (the goodness-
 * averaged score across all 6, unaffected by rule 2's gating), weighted
 * down slightly for thin reporting history (see historyWeight) — the top
 * MAX_LEADERS_PER_INDUSTRY of those make the cut. An industry with no
 * qualifying candidate at all gets no entry, rather than forcing a pick;
 * one with fewer than MAX_LEADERS_PER_INDUSTRY qualifiers just shows
 * however many genuinely qualify, never padded with a non-qualifying pick.
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

    const qualifying = [];
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
      qualifying.push({ symbol, composite, adjustedScore });
    }

    if (qualifying.length) {
      qualifying.sort((a, b) => b.adjustedScore - a.adjustedScore);
      const top = qualifying.slice(0, MAX_LEADERS_PER_INDUSTRY);
      leaders.push({
        industry,
        peerCount: symbols.length,
        leaders: top.map((t) => ({
          symbol: t.symbol,
          name: profiles[t.symbol]?.name || t.symbol,
          logo: profiles[t.symbol]?.logo || null,
          composite: t.composite,
        })),
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

  // SEC XBRL fallback for a Finnhub crawl gap that would otherwise block
  // revenueGrowth's YoY comparison — see backfillRevenueGapsFromSec's own
  // comment for the full rationale (verified live for Dominion Energy/D).
  // Free to check (array inspection); only spends a request when a real
  // gap is actually found.
  try {
    const cik = ctx.secTickerToCikMap?.get(symbol.toUpperCase());
    if (cik) {
      const backfilled = await backfillRevenueGapsFromSec(symbol, cik, quarterlyFinancials, annualReportedFinancials);
      quarterlyFinancials = backfilled.quarterlyReports;
      annualReportedFinancials = backfilled.annualReports;
    }
  } catch {
    // Non-fatal — same graceful-degradation philosophy as the fetch above.
  }

  // SEC-XBRL broad enrichment — for tickers whose Finnhub coverage is too
  // sparse to ever complete even one TTM window or one YoY annual point
  // (SEC_ENRICHMENT_SPARSE_QUARTERLY_THRESHOLD/SEC_ENRICHMENT_SPARSE_ANNUAL_THRESHOLD
  // above) — verified live for BRK.A, whose own financials-reported
  // coverage is exactly 1 historical report. The narrow gap-finder above
  // needs an existing report to date-shift from, so it structurally can't
  // reach a ticker this sparse; this fetches SEC's own companyfacts
  // directly and synthesizes report entries from its fy/fp-labeled facts
  // instead. Only spends the extra fetch for tickers below either
  // threshold — the ~4990 already-healthy tickers pay nothing extra.
  if (quarterlyFinancials.length < SEC_ENRICHMENT_SPARSE_QUARTERLY_THRESHOLD || annualReportedFinancials.length < SEC_ENRICHMENT_SPARSE_ANNUAL_THRESHOLD) {
    if (process.env.DEBUG_SEC_ENRICHMENT) console.error('DEBUG enrichment triggered for', symbol, 'q=', quarterlyFinancials.length, 'a=', annualReportedFinancials.length);
    try {
      const cik = ctx.secTickerToCikMap?.get(symbol.toUpperCase());
      if (process.env.DEBUG_SEC_ENRICHMENT) console.error('DEBUG cik lookup', symbol.toUpperCase(), '->', cik);
      if (cik) {
        const gaapFacts = await fetchSecUsGaapFacts(cik);
        if (process.env.DEBUG_SEC_ENRICHMENT) console.error('DEBUG gaapFacts concepts', Object.keys(gaapFacts).length);
        const synthesized = buildSecSyntheticReports(gaapFacts, cik);
        if (process.env.DEBUG_SEC_ENRICHMENT) console.error('DEBUG synthesized', synthesized.quarterlyReports.length, synthesized.annualReports.length);
        const merged = mergeSyntheticReports(quarterlyFinancials, annualReportedFinancials, synthesized);
        quarterlyFinancials = merged.quarterlyReports;
        annualReportedFinancials = merged.annualReports;
        if (process.env.DEBUG_SEC_ENRICHMENT) console.error('DEBUG merged totals', quarterlyFinancials.length, annualReportedFinancials.length);
      }
    } catch (e) {
      if (process.env.DEBUG_SEC_ENRICHMENT) console.error('DEBUG enrichment threw', e.message, e.stack);
      // Non-fatal — same graceful-degradation philosophy as the fetches above.
    }
  }

  // Cross-cadence derivation — fills a missing quarter/year using ONLY real
  // arithmetic on already-known real (or SEC-enriched, from above) facts,
  // never fabrication (see deriveCrossCadenceReports' own comment for the
  // full design). Runs after SEC enrichment, before the 12 builders, so a
  // ticker like BRK.A - now enriched with real SEC facts above - gets any
  // remaining one-off cadence gaps closed on top. A pure local computation
  // (no network calls) - cheap enough to run for every ticker unconditionally;
  // for an already-complete ticker every per-field/per-year condition simply
  // never matches, so this is a no-op rather than a cost.
  try {
    const derived = deriveCrossCadenceReports(quarterlyFinancials, annualReportedFinancials);
    if (process.env.DEBUG_SEC_ENRICHMENT) {
      console.error('DEBUG cross-cadence derived', derived.quarterlyReports.length - quarterlyFinancials.length, 'new quarters,', derived.annualReports.length - annualReportedFinancials.length, 'new annuals');
    }
    quarterlyFinancials = derived.quarterlyReports;
    annualReportedFinancials = derived.annualReports;
  } catch (e) {
    if (process.env.DEBUG_SEC_ENRICHMENT) console.error('DEBUG cross-cadence derivation threw', e.message, e.stack);
    // Non-fatal — same graceful-degradation philosophy as the fetches above.
  }

  const isBankLike = isFinancialIndustry(profile.industry);
  const yearlyBuilders = {
    revenueGrowth: () => buildRevenueGrowthYearlyFromFilings(annualReportedFinancials),
    profitMargin: () => buildProfitMarginYearlyFromFilings(annualReportedFinancials),
    fcfMargin: () => (isBankLike ? [] : buildFcfMarginYearlyFromFilings(annualReportedFinancials)),
    roic: () => buildRoicYearlyFromFilings(annualReportedFinancials, isBankLike),
  };
  const quarterlyBuilders = {
    revenueGrowth: () => buildRevenueGrowthQuarterlyFromFilings(quarterlyFinancials, annualReportedFinancials),
    profitMargin: () => buildProfitMarginQuarterlyFromFilings(quarterlyFinancials, annualReportedFinancials),
    fcfMargin: () => (isBankLike ? [] : buildFcfMarginQuarterlyFromFilings(quarterlyFinancials, annualReportedFinancials)),
    roic: () => buildRoicQuarterlyFromFilings(quarterlyFinancials, annualReportedFinancials, isBankLike),
  };
  const ttmBuilders = {
    revenueGrowth: () => buildRevenueGrowthTTMFromFilings(quarterlyFinancials, annualReportedFinancials),
    profitMargin: () => buildProfitMarginTTMFromFilings(quarterlyFinancials, annualReportedFinancials),
    fcfMargin: () => (isBankLike ? [] : buildFcfMarginTrendFromFilings(quarterlyFinancials, annualReportedFinancials)),
    roic: () => buildRoicTTMFromFilings(quarterlyFinancials, annualReportedFinancials, isBankLike),
  };

  const mergedYearlyForSymbol = {};
  const mergedQuarterlyForSymbol = {};
  const mergedTtmForSymbol = {};
  const previousYearlyForSymbol = previouslyPublishedYearlyTrends[symbol] || {};
  const previousQuarterlyForSymbol = previouslyPublishedQuarterlyTrends[symbol] || {};
  const previousTtmForSymbol = previouslyPublishedTtmTrends[symbol] || {};

  let cardValuesReconstructed = false;

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
      // Backfill the CARD value too, not just the trend cache, whenever
      // Finnhub's native series has nothing for this field — same
      // reasoning as the original fcfMargin-only version of this check,
      // just generalized to all 4 TTM-reconstructible metrics. Verified
      // live: BNY (Bank of New York Mellon) had a real revenueGrowth/
      // profitMargin/roic TTM reconstruction available but a null card
      // value, because only fcfMargin got backfilled here before.
      if (values[key] == null) {
        values[key] = published[published.length - 1].value;
        cardValuesReconstructed = true;
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
    cardValuesReconstructed,
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
    cardValuesReconstructed: 0,
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
        if (r.cardValuesReconstructed) result.cardValuesReconstructed++;
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
          `${result.dcfComputed} with a fair-value estimate, ${result.cardValuesReconstructed} card values backfilled from reconstruction)`
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

  // Fetched once for the whole run — used only to fill a Finnhub crawl gap
  // in revenueGrowth's YoY comparison from SEC's own free companyfacts API
  // (see backfillRevenueGapsFromSec) — a SEC request is only spent per-
  // ticker when a real gap is detected, this map just resolves the CIK.
  const secTickerToCikMap = await fetchSecTickerToCikMap();
  console.log(`Loaded ${secTickerToCikMap.size} ticker->CIK mappings from SEC for the revenue-gap fallback.`);

  const ctx = {
    previouslyPublishedMetrics,
    previouslyPublishedNativeTrends: previouslyPublished.nativeTrends,
    previouslyPublishedYearlyTrends: previouslyPublished.yearlyTrends,
    previouslyPublishedQuarterlyTrends: previouslyPublished.quarterlyTrends,
    previouslyPublishedTtmTrends: previouslyPublished.ttmTrends,
    secTickerToCikMap,
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
  let cardValuesReconstructed = 0;

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
    cardValuesReconstructed += r.cardValuesReconstructed;
  }

  const industryLeaders = computeIndustryLeaders(metrics, profiles);
  const totalLeaderTickers = industryLeaders.reduce((sum, entry) => sum + entry.leaders.length, 0);
  console.log(`\nComputed leaders for ${industryLeaders.length} industries (>= ${MIN_INDUSTRY_PEERS} peers each), ${totalLeaderTickers} leader tickers total.`);

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
      `(${failed} failed, ${dead} dead/no-data excluded, ${dcfComputed} with a fair-value estimate, ${cardValuesReconstructed} card values freshly backfilled from reconstruction). ` +
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

module.exports = {
  extractDcfInputs,
  computeEstimatedFairValue,
  readFinnhubApiKeys,
  processSymbol,
  runWorker,
  findRevenueGapsNeedingBackfill,
  fetchSecRevenueFactsByPeriod,
  backfillRevenueGapsFromSec,
  findAnnualRevenueGapsNeedingBackfill,
  fetchSecTickerToCikMap,
  buildRevenueGrowthQuarterlyFromFilings,
  buildRevenueGrowthTTMFromFilings,
  buildRevenueGrowthYearlyFromFilings,
  resolveFinancialsReportedSymbol,
  fetchSecUsGaapFacts,
  buildSecSyntheticReports,
  mergeSyntheticReports,
  findSecValueForFyFp,
  deriveMissingQuarterFromAnnual,
  deriveMissingAnnualFromQuarters,
  deriveCrossCadenceReports,
  parseLabelToApproxDate,
  isRecentEnough,
  pickCadenceMetric,
  computeIndustryLeaders,
};
