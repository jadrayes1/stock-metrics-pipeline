// Surgical single-symbol publish path, used only when generate-sector-
// metrics.yml is dispatched with a `symbol` input (see TARGET_SYMBOL in
// generateSectorMetrics.js). A TARGET_SYMBOL run's local output files are
// NOT safe to publish wholesale the way a normal full-universe run's are:
// `metrics` and the 4 trend caches have a fallback-restore path (any
// previously-published ticker not touched this run gets its old data
// copied back in), but `profiles` (and therefore `industryLeaders`, which
// is computed from it) and `dcfCapCandidates` do not -- a TARGET_SYMBOL
// run's own marketMetrics.json would have industryLeaders reflecting only
// the one processed ticker's industry, and dcfCapCandidates.json would be
// missing every other ticker's candidate entirely.
//
// This script instead pulls just the target symbol's fresh entry out of
// each local file and merges it into a clone of the CURRENTLY-PUBLISHED
// gist, leaving every other ticker (and industryLeaders/coverageAudit
// entirely) untouched. coverageAudit.json is a whole-universe aggregate,
// not per-ticker keyed -- there's no clean single-symbol merge for it, so
// it's simply left unpublished this run (refreshed naturally by tomorrow's
// full run).
const fs = require('fs');
const path = require('path');

const target = (process.env.TARGET_SYMBOL || '').toUpperCase();
if (!target) {
  console.error('mergeSingleSymbolIntoGist.js requires TARGET_SYMBOL to be set.');
  process.exit(1);
}

const GIST_CLONE_DIR = 'gist-clone';

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}
function writeJson(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj));
}

// marketMetrics.json — merge metrics[target] + staleSymbols membership only.
const freshMetrics = readJson('src/data/marketMetrics.json');
const gistMetricsPath = path.join(GIST_CLONE_DIR, 'marketMetrics.json');
const gistMetrics = readJson(gistMetricsPath);

if (freshMetrics.metrics[target]) {
  gistMetrics.metrics[target] = freshMetrics.metrics[target];
} else {
  delete gistMetrics.metrics[target];
}
gistMetrics.staleSymbols = (gistMetrics.staleSymbols || []).filter((s) => s !== target);
if ((freshMetrics.staleSymbols || []).includes(target)) {
  gistMetrics.staleSymbols.push(target);
}
// industryLeaders intentionally left untouched — see file header.
gistMetrics.generatedAt = freshMetrics.generatedAt;
writeJson(gistMetricsPath, gistMetrics);
console.log(`Merged ${target} into marketMetrics.json (industryLeaders left as-is).`);

// The 4 trend caches — merge trends[target] only.
for (const file of ['trendsNative.json', 'trendsQuarterly.json', 'trendsYearly.json', 'trendsTtm.json']) {
  const fresh = readJson(path.join('src/data', file));
  const gistPath = path.join(GIST_CLONE_DIR, file);
  const gist = readJson(gistPath);
  if (fresh.trends[target]) {
    gist.trends[target] = fresh.trends[target];
  } else {
    delete gist.trends[target];
  }
  gist.generatedAt = fresh.generatedAt;
  writeJson(gistPath, gist);
  console.log(`Merged ${target} into ${file}.`);
}

// dcfCapCandidates.json — merge candidates[target] only (every other
// ticker's candidate must survive untouched, or apply-dcf-cap.yml's next
// workflow_run-triggered pass would silently stop reprocessing them).
const freshCandidates = readJson('dcfCapCandidates.json');
const gistCandidatesPath = path.join(GIST_CLONE_DIR, 'dcfCapCandidates.json');
const gistCandidates = readJson(gistCandidatesPath);
if (freshCandidates.candidates[target]) {
  gistCandidates.candidates[target] = freshCandidates.candidates[target];
} else {
  delete gistCandidates.candidates[target];
}
gistCandidates.generatedAt = freshCandidates.generatedAt;
writeJson(gistCandidatesPath, gistCandidates);
console.log(`Merged ${target} into dcfCapCandidates.json.`);

console.log('coverageAudit.json left unpublished this run (whole-universe aggregate, no clean single-symbol merge).');
