// scripts/applyDcfCap.js
//
// Caps an outlier DCF estimatedFairValue at the analyst-consensus high price
// target, when one is available and our estimate exceeds it. Runs as an
// extra step right after scripts/fetchDcfCapTargets.py (which resolves
// analyst targets, via yfinance, only for the candidates
// generateSectorMetrics.js already flagged as >= DCF_CAP_CANDIDATE_MULTIPLE
// above the current price — see that file) and before "Publish to gist" —
// see .github/workflows/generate-sector-metrics.yml.
//
// Deliberately conservative: a ticker with no resolved analyst target (a
// failed/skipped yfinance lookup, or simply no coverage) is left untouched
// — this only ever lowers an estimate that's both flagged AND has a real
// target to check against, never removes or nulls an estimate outright.

const fs = require('fs');
const path = require('path');

const METRICS_FILE = process.argv[2] || path.join(__dirname, '../src/data/marketMetrics.json');
const TARGETS_FILE = process.argv[3] || path.join(__dirname, '../analystPriceTargets.json');

function applyDcfCap(metrics, targets) {
  let capped = 0;
  for (const [symbol, target] of Object.entries(targets)) {
    const high = target?.high;
    const entry = metrics[symbol];
    if (typeof high !== 'number' || !entry || entry.estimatedFairValue == null) continue;
    if (entry.estimatedFairValue > high) {
      entry.estimatedFairValue = high;
      capped++;
    }
  }
  return capped;
}

function main() {
  if (!fs.existsSync(TARGETS_FILE)) {
    console.log(`${TARGETS_FILE} not found — skipping DCF cap (no analyst targets to apply).`);
    return;
  }

  const dataset = JSON.parse(fs.readFileSync(METRICS_FILE, 'utf8'));
  const { targets } = JSON.parse(fs.readFileSync(TARGETS_FILE, 'utf8'));

  const capped = applyDcfCap(dataset.metrics || {}, targets || {});
  fs.writeFileSync(METRICS_FILE, JSON.stringify(dataset));

  console.log(
    `Capped ${capped} of ${Object.keys(targets || {}).length} resolved analyst targets ` +
      `(estimatedFairValue exceeded the analyst high) in ${path.relative(process.cwd(), METRICS_FILE)}.`
  );
}

if (require.main === module) {
  main();
}

module.exports = { applyDcfCap };
