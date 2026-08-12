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
import sys
import time

import yfinance as yf

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
CANDIDATES_FILE = os.path.join(REPO_ROOT, "dcfCapCandidates.json")
OUTPUT_FILE = os.path.join(REPO_ROOT, "analystPriceTargets.json")

# Conservative pacing against an unofficial, undocumented endpoint — no
# published rate limit to target, just spacing requests out defensively.
REQUEST_SLEEP_SECONDS = 1.0


def main():
    if not os.path.exists(CANDIDATES_FILE):
        print(f"{CANDIDATES_FILE} not found — nothing to do.")
        with open(OUTPUT_FILE, "w") as f:
            json.dump({"generatedAt": None, "targets": {}}, f)
        return

    with open(CANDIDATES_FILE) as f:
        candidates = json.load(f).get("candidates", {})

    symbols = sorted(candidates.keys())
    print(f"{len(symbols)} candidates to look up.")

    targets = {}
    failed = 0
    for i, symbol in enumerate(symbols):
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

        if (i + 1) % 25 == 0 or i == len(symbols) - 1:
            print(f"  {i + 1}/{len(symbols)} ({len(targets)} resolved, {failed} failed)")

        if i < len(symbols) - 1:
            time.sleep(REQUEST_SLEEP_SECONDS)

    with open(OUTPUT_FILE, "w") as f:
        json.dump({"generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "targets": targets}, f)

    print(f"Wrote {len(targets)} analyst price targets to {OUTPUT_FILE} ({failed} lookups failed/skipped).")


if __name__ == "__main__":
    main()
