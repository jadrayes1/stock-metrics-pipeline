// scripts/auditMetricsCoverage.js
//
// Fast, read-only audit of the 6 comparable metrics (roic, revenueGrowth,
// profitMargin, fcfMargin, peRatio, pfcfRatio) in the dataset
// generateSectorMetrics.js just produced. No new API calls — this just
// scans the already-fetched data and reports which tickers are missing
// which fields, so gaps can be investigated and fixed (existing fallback
// or new data source) without re-deriving "how many tickers have a hole"
// by hand each time.
//
// Runs as an extra step in the same daily pipeline job, right after
// generateSectorMetrics.js, and publishes its output (coverageAudit.json)
// to the same Gist as a second file — see
// .github/workflows/generate-sector-metrics.yml.

const fs = require('fs');
const path = require('path');

const INPUT_FILE = process.argv[2] || path.join(__dirname, '../src/data/marketMetrics.json');
const TRENDS_DIR = path.dirname(INPUT_FILE); // trendsNative/Quarterly/Yearly/Ttm.json are written alongside marketMetrics.json by the same run
const OUTPUT_FILE = path.join(__dirname, '../coverageAudit.json');

// Mirrors COMPARABLE_KEYS in generateSectorMetrics.js / COMPARABLE_METRIC_KEYS
// in src/utils/metrics.js — keep in sync if that ever changes.
const COMPARABLE_KEYS = ['roic', 'revenueGrowth', 'profitMargin', 'fcfMargin', 'peRatio', 'pfcfRatio'];

// The Quarterly/Yearly/TTM cadence's own recency floor (pickCadenceMetric /
// isRecentEnough in generateSectorMetrics.js) can silently drop an
// otherwise-real trend series to empty once its newest point goes stale —
// without checking these files directly, that drop was completely invisible
// here: the SCALAR field above (data[k]) is usually still populated from a
// DIFFERENT cadence (see NATIVE_CADENCE_BY_METRIC in stock-analyzer's
// src/api/stockData.js — e.g. roic's scalar card value comes from ttm), so a
// Quarterly-only gap never tripped the scalar-only check this file used to
// be limited to. Confirmed live: PDD's quarterly roic/fcfMargin trends are
// genuinely empty (the recency floor correctly dropped stale Q4'21 data)
// but PDD never showed up in tickersWithGaps at all.
//
// peRatio/pfcfRatio are deliberately excluded here — they're published to a
// separate cache (pfcfTrendCache.json, generatePfcfTrendCache.js, a
// different repo workflow with its own rotating-budget schedule) that isn't
// written to this run's local src/data directory, so it's out of scope for
// a same-run, no-new-fetches audit like this one.
const CADENCE_TREND_METRICS = ['roic', 'revenueGrowth', 'profitMargin', 'fcfMargin'];
const CADENCE_TREND_FILES = {
  native: 'trendsNative.json',
  quarterly: 'trendsQuarterly.json',
  yearly: 'trendsYearly.json',
  ttm: 'trendsTtm.json',
};

function loadTrends(filename) {
  const filePath = path.join(TRENDS_DIR, filename);
  if (!fs.existsSync(filePath)) return {};
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return parsed.trends || {};
}

function main() {
  const dataset = JSON.parse(fs.readFileSync(INPUT_FILE, 'utf8'));
  const metrics = dataset.metrics || {};
  const totalTickers = Object.keys(metrics).length;

  const trendsByCadence = Object.fromEntries(
    Object.entries(CADENCE_TREND_FILES).map(([cadence, filename]) => [cadence, loadTrends(filename)])
  );

  const missingCounts = Object.fromEntries(COMPARABLE_KEYS.map((k) => [k, 0]));
  const missingCadenceCounts = Object.fromEntries(
    Object.keys(CADENCE_TREND_FILES).map((cadence) => [cadence, Object.fromEntries(CADENCE_TREND_METRICS.map((m) => [m, 0]))])
  );
  const tickersWithGaps = [];

  for (const [symbol, data] of Object.entries(metrics)) {
    const missingFields = COMPARABLE_KEYS.filter((k) => data[k] === null || data[k] === undefined);

    const cadenceGaps = [];
    for (const [cadence, trends] of Object.entries(trendsByCadence)) {
      const symbolTrends = trends[symbol] || {};
      for (const metric of CADENCE_TREND_METRICS) {
        const points = symbolTrends[metric];
        if (!Array.isArray(points) || points.length === 0) cadenceGaps.push({ cadence, metric });
      }
    }

    for (const f of missingFields) missingCounts[f]++;
    for (const { cadence, metric } of cadenceGaps) missingCadenceCounts[cadence][metric]++;

    if (missingFields.length === 0 && cadenceGaps.length === 0) continue;
    tickersWithGaps.push({ symbol, industry: data.industry || null, missingFields, cadenceGaps });
  }

  // Grouped by industry so a systemic, industry-wide root cause (e.g. FCF
  // Margin being structurally unavailable for banks) shows up as one clear
  // pattern to investigate, rather than requiring hundreds of individual
  // tickers to be checked one at a time to notice the same thing.
  const byIndustryAndField = {};
  const byIndustryAndCadenceField = {};
  for (const { industry, missingFields, cadenceGaps } of tickersWithGaps) {
    const key = industry || 'Unknown';
    byIndustryAndField[key] = byIndustryAndField[key] || {};
    for (const f of missingFields) {
      byIndustryAndField[key][f] = (byIndustryAndField[key][f] || 0) + 1;
    }
    byIndustryAndCadenceField[key] = byIndustryAndCadenceField[key] || {};
    for (const { cadence, metric } of cadenceGaps) {
      byIndustryAndCadenceField[key][cadence] = byIndustryAndCadenceField[key][cadence] || {};
      byIndustryAndCadenceField[key][cadence][metric] = (byIndustryAndCadenceField[key][cadence][metric] || 0) + 1;
    }
  }

  tickersWithGaps.sort(
    (a, b) =>
      b.missingFields.length + b.cadenceGaps.length - (a.missingFields.length + a.cadenceGaps.length) ||
      a.symbol.localeCompare(b.symbol)
  );

  const totalWithAtLeastOneCadenceGap = tickersWithGaps.filter((t) => t.cadenceGaps.length > 0).length;

  const output = {
    generatedAt: new Date().toISOString(),
    sourceGeneratedAt: dataset.generatedAt || null,
    totalTickers,
    totalWithAtLeastOneGap: tickersWithGaps.length,
    totalWithAtLeastOneCadenceGap,
    missingCounts,
    missingCadenceCounts,
    byIndustryAndField,
    byIndustryAndCadenceField,
    tickersWithGaps,
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2));

  console.log(`Audited ${totalTickers} tickers.`);
  console.log(`${tickersWithGaps.length} (${((tickersWithGaps.length / totalTickers) * 100).toFixed(1)}%) have at least one missing metric or empty cadence trend.`);
  console.log('Missing counts by field (scalar):');
  for (const [field, count] of Object.entries(missingCounts)) {
    console.log(`  ${field}: ${count}`);
  }
  console.log('Empty-trend counts by cadence/field:');
  for (const [cadence, counts] of Object.entries(missingCadenceCounts)) {
    for (const [field, count] of Object.entries(counts)) {
      console.log(`  ${cadence}.${field}: ${count}`);
    }
  }
  console.log(`Wrote ${path.relative(process.cwd(), OUTPUT_FILE)}`);
}

main();
