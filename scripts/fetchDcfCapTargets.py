#!/usr/bin/env python3
"""Fetches analyst consensus price targets (via yfinance) for the tickers
scripts/generateSectorMetrics.js flagged as needing one, for one of two
reasons (see the 'reason' field on each candidate):
  - 'cap': our own estimatedFairValue is at least DCF_CAP_CANDIDATE_MULTIPLE
    (2.0x, see that file) above the current price — the real analyst high
    is used as a ceiling (scripts/applyDcfCap.js).
  - 'fallback': DCF wasn't computable at all for this ticker — the real
    analyst mid-range ((high+low)/2, or mean/median if only one of those is
    available) is used to fill estimatedFairValue rather than leaving it
    empty, tagged with estimatedFairValueSource so it stays distinguishable
    from a real DCF output.
Only flagged candidates are looked up, not the full ~5,000-ticker universe,
to keep scrape volume against Yahoo's undocumented endpoints (yfinance has
no official rate limit or SLA, and can be throttled without warning,
especially from a shared CI IP range) as low as possible.

A missing/failed lookup for a given ticker is never fatal — it just means
that ticker's DCF estimate goes unchecked/unfilled for this run
(scripts/applyDcfCap.js leaves it as-is). Every other metric is entirely
unaffected by a failure here.

Usage: python3 scripts/fetchDcfCapTargets.py
Reads:  dcfCapCandidates.json (repo root, written by generateSectorMetrics.js)
Writes: analystPriceTargets.json (repo root)
"""

import json
import os
import random
import sys
import time

import yfinance as yf

# Unbuffered stdout/stderr -- verified live 2026-08-26: this script's print()
# calls were fully buffered (not line-buffered) once GitHub Actions
# redirects stdout to a file rather than a TTY, so EVERY progress/error line
# for the whole run only appeared in the log at process exit, all stamped
# with the same exit timestamp. That made a real ~72-minute-and-growing
# bottleneck (see TIME_BUDGET_SECONDS below) completely invisible while
# investigating why the overall pipeline kept hitting GitHub's 6-hour job
# ceiling -- this fixes the blind spot itself, not just the timing bug it
# was hiding.
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
CANDIDATES_FILE = os.path.join(REPO_ROOT, "dcfCapCandidates.json")
OUTPUT_FILE = os.path.join(REPO_ROOT, "analystPriceTargets.json")

# Conservative pacing against an unofficial, undocumented endpoint — no
# published rate limit to target, just spacing requests out defensively.
REQUEST_SLEEP_SECONDS = 1.0

# Verified live 2026-08-26: 4,154 of 5,161 tickers (80%!) were flagged as
# candidates in one real run -- at 1 request/sec that alone is ~70 minutes
# BEFORE real per-request network latency. This used to run as a step
# inside the main sector-metrics job, where it was a direct contributor to
# that job repeatedly hitting GitHub Actions' hard 6-hour per-job ceiling
# (confirmed live: two consecutive runs cancelled at exactly 6h0m20s) --
# fixed THEN by capping this at 45 minutes, mirroring generateNewsCache.js's
# own unbounded-candidate-set budget.
#
# Since then, this step was pulled out into its own standalone workflow
# (apply-dcf-cap.yml) specifically so it no longer has to share a time
# budget with the main pipeline at all -- see that workflow's own comment.
# The budget here is now sized to comfortably finish the WHOLE candidate
# list every run (current volume needs ~1.75hrs at 1 req/sec; 3hrs leaves
# room for real growth) rather than to protect a neighbor's timing. Still
# not fully unbounded -- a real ceiling is cheap insurance against a
# genuinely runaway candidate count or Yahoo-side slowdown, and
# applyDcfCap.js already treats a missing analyst target as a no-op (leaves
# whatever estimatedFairValue was already there), so hitting this budget on
# a bad day is still not a correctness problem.
TIME_BUDGET_SECONDS = 3 * 60 * 60


def main():
    if not os.path.exists(CANDIDATES_FILE):
        print(f"{CANDIDATES_FILE} not found — nothing to do.")
        with open(OUTPUT_FILE, "w") as f:
            json.dump({"generatedAt": None, "targets": {}}, f)
        return

    with open(CANDIDATES_FILE) as f:
        candidates = json.load(f).get("candidates", {})

    # Manual single-symbol override (apply-dcf-cap.yml's "symbol"
    # workflow_dispatch input) -- lets debugging one ticker's cap/fallback
    # outcome take a couple minutes instead of the full multi-hour candidate
    # sweep. Bypasses the normal candidate list entirely; if the symbol
    # isn't a current candidate at all, still looks it up under a synthetic
    # 'cap' reason (a real target is only ever applied by applyDcfCap.js
    # when it actually improves on the existing estimate, so a wrong guess
    # here is harmless -- see that file's own conservative gating).
    target_symbol = os.environ.get("TARGET_SYMBOL", "").strip().upper()
    if target_symbol:
        candidates = {target_symbol: candidates.get(target_symbol, {"reason": "cap"})}
        print(f"TARGET_SYMBOL set — processing only {target_symbol}, ignoring the normal candidate list.")

    # Shuffled, not alphabetical -- this script has no persistent state
    # between runs (unlike generateNewsCache.js's least-recently-cached-
    # first rotation), so a fixed order combined with a time budget would
    # mean alphabetically-early tickers always get processed and
    # alphabetically-late ones (Z-*, etc.) never do. A random seed each run
    # gives every candidate a fair, roughly-equal chance of being covered
    # over successive daily runs instead.
    symbols = list(candidates.keys())
    random.shuffle(symbols)
    print(f"{len(symbols)} candidates to look up (budget: {TIME_BUDGET_SECONDS / 60:.0f} min).")

    start_time = time.monotonic()
    targets = {}
    failed = 0
    processed = 0
    for symbol in symbols:
        if time.monotonic() - start_time > TIME_BUDGET_SECONDS:
            print(f"Time budget ({TIME_BUDGET_SECONDS / 60:.0f} min) reached after {processed}/{len(symbols)} — stopping for this run.")
            break

        try:
            info = yf.Ticker(symbol).analyst_price_targets
            high = info.get("high") if info else None
            low = info.get("low") if info else None
            mean = info.get("mean") if info else None
            median = info.get("median") if info else None
            if high is not None or low is not None or mean is not None or median is not None:
                targets[symbol] = {
                    "reason": candidates[symbol].get("reason"),
                    "high": high,
                    "low": low,
                    "mean": mean,
                    "median": median,
                }
        except Exception as err:
            failed += 1
            print(f"  skip {symbol}: {err}", file=sys.stderr)

        processed += 1
        if processed % 25 == 0 or processed == len(symbols):
            print(f"  {processed}/{len(symbols)} ({len(targets)} resolved, {failed} failed)")

        time.sleep(REQUEST_SLEEP_SECONDS)

    with open(OUTPUT_FILE, "w") as f:
        json.dump({"generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "targets": targets}, f)

    print(f"Wrote {len(targets)} analyst price targets to {OUTPUT_FILE} ({failed} lookups failed/skipped).")


if __name__ == "__main__":
    main()
