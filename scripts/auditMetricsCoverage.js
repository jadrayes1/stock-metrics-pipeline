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
const OUTPUT_FILE = path.join(__dirname, '../coverageAudit.json');

// Mirrors COMPARABLE_KEYS in generateSectorMetrics.js / COMPARABLE_METRIC_KEYS
// in src/utils/metrics.js — keep in sync if that ever changes.
const COMPARABLE_KEYS = ['roic', 'revenueGrowth', 'profitMargin', 'fcfMargin', 'peRatio', 'pfcfRatio'];

function main() {
  const dataset = JSON.parse(fs.readFileSync(INPUT_FILE, 'utf8'));
  const metrics = dataset.metrics || {};
  const totalTickers = Object.keys(metrics).length;

  const missingCounts = Object.fromEntries(COMPARABLE_KEYS.map((k) => [k, 0]));
  const tickersWithGaps = [];

  for (const [symbol, data] of Object.entries(metrics)) {
    const missingFields = COMPARABLE_KEYS.filter((k) => data[k] === null || data[k] === undefined);
    if (missingFields.length === 0) continue;
    for (const f of missingFields) missingCounts[f]++;
    tickersWithGaps.push({ symbol, industry: data.industry || null, missingFields });
  }

  // Grouped by industry so a systemic, industry-wide root cause (e.g. FCF
  // Margin being structurally unavailable for banks) shows up as one clear
  // pattern to investigate, rather than requiring hundreds of individual
  // tickers to be checked one at a time to notice the same thing.
  const byIndustryAndField = {};
  for (const { industry, missingFields } of tickersWithGaps) {
    const key = industry || 'Unknown';
    byIndustryAndField[key] = byIndustryAndField[key] || {};
    for (const f of missingFields) {
      byIndustryAndField[key][f] = (byIndustryAndField[key][f] || 0) + 1;
    }
  }

  tickersWithGaps.sort((a, b) => b.missingFields.length - a.missingFields.length || a.symbol.localeCompare(b.symbol));

  const output = {
    generatedAt: new Date().toISOString(),
    sourceGeneratedAt: dataset.generatedAt || null,
    totalTickers,
    totalWithAtLeastOneGap: tickersWithGaps.length,
    missingCounts,
    byIndustryAndField,
    tickersWithGaps,
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2));

  console.log(`Audited ${totalTickers} tickers.`);
  console.log(`${tickersWithGaps.length} (${((tickersWithGaps.length / totalTickers) * 100).toFixed(1)}%) have at least one missing metric.`);
  console.log('Missing counts by field:');
  for (const [field, count] of Object.entries(missingCounts)) {
    console.log(`  ${field}: ${count}`);
  }
  console.log(`Wrote ${path.relative(process.cwd(), OUTPUT_FILE)}`);
}

main();
