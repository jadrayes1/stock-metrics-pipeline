// scripts/applyDcfCap.js
//
// Consumes scripts/fetchDcfCapTargets.py's resolved analyst targets and
// applies one of two treatments per ticker, based on the 'reason' that
// ticker was flagged with in dcfCapCandidates.json (see
// generateSectorMetrics.js):
//   - 'cap': our own estimatedFairValue exceeded the real analyst high —
//     cap it there.
//   - 'fallback': DCF wasn't computable at all for this ticker — fill
//     estimatedFairValue with the real analyst mid-range ((high+low)/2, or
//     mean/median if only one of those is available), tagged with
//     estimatedFairValueSource: 'analystConsensus' so it stays
//     distinguishable from a real DCF output (the app's own Fair Value
//     (DCF) label/description otherwise implies "not a price target," which
//     an analyst-consensus figure very much is).
//
// Runs right after fetchDcfCapTargets.py and before "Publish to gist" — see
// .github/workflows/generate-sector-metrics.yml.
//
// Deliberately conservative: a ticker with no resolved analyst target (a
// failed/skipped yfinance lookup, or simply no coverage) is left exactly as
// it was — this never removes or nulls an estimate outright, only lowers a
// flagged outlier or fills a genuine gap, both gated on having a real
// target to act on.

const fs = require('fs');
const path = require('path');

const METRICS_FILE = process.argv[2] || path.join(__dirname, '../src/data/marketMetrics.json');
const TARGETS_FILE = process.argv[3] || path.join(__dirname, '../analystPriceTargets.json');

function computeMidRange(target) {
  if (typeof target.high === 'number' && typeof target.low === 'number') return (target.high + target.low) / 2;
  if (typeof target.mean === 'number') return target.mean;
  if (typeof target.median === 'number') return target.median;
  return null;
}

function applyDcfCap(metrics, targets) {
  let capped = 0;
  let filled = 0;
  for (const [symbol, target] of Object.entries(targets)) {
    const entry = metrics[symbol];
    if (!entry) continue;

    if (target?.reason === 'cap') {
      const high = target.high;
      if (typeof high === 'number' && entry.estimatedFairValue != null && entry.estimatedFairValue > high) {
        entry.estimatedFairValue = high;
        capped++;
      }
    } else if (target?.reason === 'fallback') {
      if (entry.estimatedFairValue == null) {
        const midRange = computeMidRange(target);
        if (midRange != null) {
          entry.estimatedFairValue = midRange;
          entry.estimatedFairValueSource = 'analystConsensus';
          filled++;
        }
      }
    }
  }
  return { capped, filled };
}

function main() {
  if (!fs.existsSync(TARGETS_FILE)) {
    console.log(`${TARGETS_FILE} not found — skipping DCF cap/fallback (no analyst targets to apply).`);
    return;
  }

  const dataset = JSON.parse(fs.readFileSync(METRICS_FILE, 'utf8'));
  const { targets } = JSON.parse(fs.readFileSync(TARGETS_FILE, 'utf8'));

  const { capped, filled } = applyDcfCap(dataset.metrics || {}, targets || {});
  fs.writeFileSync(METRICS_FILE, JSON.stringify(dataset));

  console.log(
    `Applied ${Object.keys(targets || {}).length} resolved analyst targets to ${path.relative(process.cwd(), METRICS_FILE)}: ` +
      `${capped} outlier DCF estimates capped, ${filled} missing DCF estimates filled with an analyst mid-range.`
  );
}

if (require.main === module) {
  main();
}

module.exports = { applyDcfCap, computeMidRange };
