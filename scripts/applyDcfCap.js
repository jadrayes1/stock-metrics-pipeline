// scripts/applyDcfCap.js
//
// Consumes scripts/fetchDcfCapTargets.py's resolved analyst targets and
// applies one of two treatments per ticker, based on the 'reason' that
// ticker was flagged with in dcfCapCandidates.json (see
// generateSectorMetrics.js):
//   - 'cap': our own estimatedFairValue exceeded the real analyst high —
//     cap it there.
//   - 'floor': the mirror -- our estimate fell below the real analyst low
//     -- raise it there, tagged estimatedFairValueSource: 'dcfFloored'.
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

// Must match generateSectorMetrics.js's constant of the same name.
const DCF_CAP_CANDIDATE_MULTIPLE = 1.8;

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
  let floored = 0;
  let filled = 0;
  for (const [symbol, target] of Object.entries(targets)) {
    const entry = metrics[symbol];
    if (!entry) continue;

    if (target?.reason === 'cap') {
      const high = target.high;
      if (typeof high === 'number' && entry.estimatedFairValue != null && entry.estimatedFairValue > high) {
        entry.estimatedFairValue = high;
        // Tagged the same way a 'fallback' fill already is, for the same
        // reason: generateSectorMetrics.js's own next run needs to tell a
        // "this was capped against a real analyst high" value apart from a
        // genuinely fresh DCF output, so it can preserve THIS value instead
        // of wiping it back to that day's raw (still-outlier) DCF estimate
        // before this workflow gets a chance to re-cap it again.
        entry.estimatedFairValueSource = 'dcfCapped';
        capped++;
      }
    } else if (target?.reason === 'floor') {
      // Raising a value is the riskier direction (like 'fallback' below), so
      // the same degenerate-coverage guard applies, plus one more: never
      // raise past the cap's own ceiling (DCF_CAP_CANDIDATE_MULTIPLE x
      // price) -- a stale target far above the price (JTAI's $80 at ~50x)
      // would otherwise turn a low DCF into a high outlier.
      const low = target.low;
      const degenerateCoverage = typeof target.high === 'number' && typeof low === 'number' && target.high === low;
      const ceiling = typeof target.currentPrice === 'number' ? target.currentPrice * DCF_CAP_CANDIDATE_MULTIPLE : null;
      if (typeof low === 'number' && low > 0 && !degenerateCoverage && (ceiling == null || low <= ceiling) && entry.estimatedFairValue != null && entry.estimatedFairValue < low) {
        entry.estimatedFairValue = low;
        entry.estimatedFairValueSource = 'dcfFloored';
        floored++;
      }
    } else if (target?.reason === 'fallback') {
      if (entry.estimatedFairValue == null) {
        // Skip a target where high === low (both present) -- verified
        // live: JTAI's real yfinance analyst_price_targets returns
        // high=low=mean=median=$80 from exactly ONE analyst opinion, a
        // stale figure never adjusted for a later reverse split, ~50x the
        // real price. Independent analysts essentially never land on the
        // exact same price, so an identical high/low is a reliable, free
        // (no extra API call needed) signal of single/degenerate coverage
        // not worth trusting as a fair-value fill. Doesn't affect the
        // 'cap' path above -- that only ever LOWERS an already-too-high
        // DCF estimate, a fundamentally safer use of a shaky target than
        // fabricating a brand-new estimate from it.
        const degenerateCoverage = typeof target.high === 'number' && typeof target.low === 'number' && target.high === target.low;
        const midRange = computeMidRange(target);
        if (midRange != null && !degenerateCoverage) {
          entry.estimatedFairValue = midRange;
          entry.estimatedFairValueSource = 'analystConsensus';
          filled++;
        }
      }
    }
  }
  return { capped, floored, filled };
}

function main() {
  if (!fs.existsSync(TARGETS_FILE)) {
    console.log(`${TARGETS_FILE} not found — skipping DCF cap/fallback (no analyst targets to apply).`);
    return;
  }

  const dataset = JSON.parse(fs.readFileSync(METRICS_FILE, 'utf8'));
  const { targets } = JSON.parse(fs.readFileSync(TARGETS_FILE, 'utf8'));

  const { capped, floored, filled } = applyDcfCap(dataset.metrics || {}, targets || {});
  fs.writeFileSync(METRICS_FILE, JSON.stringify(dataset));

  console.log(
    `Applied ${Object.keys(targets || {}).length} resolved analyst targets to ${path.relative(process.cwd(), METRICS_FILE)}: ` +
      `${capped} outlier DCF estimates capped, ${floored} raised to the analyst low, ${filled} missing DCF estimates filled with an analyst mid-range.`
  );
}

if (require.main === module) {
  main();
}

module.exports = { applyDcfCap, computeMidRange };
