# stock-metrics-pipeline

Daily data pipeline for [stock-analyzer](https://github.com/jadrayes1/stock-analyzer)'s
sector/industry-percentile feature. Fetches industry classification and
fundamentals (ROIC, revenue growth, profit margin, FCF margin, P/E, P/FCF)
for every NASDAQ/NYSE/NYSE American common stock and REIT from
[Finnhub](https://finnhub.io/) (~5,145 tickers as of this writing — see
`scripts/generateSectorMetrics.js` for exactly what's included and why), and
publishes the result to a public Gist that the app reads at runtime.

This is a separate repo from the app on purpose — see the comment header in
`.github/workflows/generate-sector-metrics.yml` for why (short version: the
app repo is private, this job takes ~2-3 hours/day, and private repos have
limited free GitHub Actions minutes while public ones don't).

## Setup

1. `FINNHUB_API_KEY=your-key npm run generate` locally once to confirm it
   works. Takes ~2-3 hours.
2. Add three repository secrets (Settings → Secrets and variables → Actions):
   - `FINNHUB_API_KEY` — your Finnhub key
   - `GIST_ID` — the target Gist's ID
   - `GIST_TOKEN` — a **classic** Personal Access Token scoped to only
     `gist` (github.com/settings/tokens — not the fine-grained token page;
     fine-grained tokens' Gists permission can end up read-only, which fails
     as an unhelpful HTTP 404 on write rather than a clear permission error)
3. Trigger the workflow once manually (Actions tab → Run workflow) rather
   than waiting for the daily schedule.

## Files

```
scripts/generateSectorMetrics.js   — the actual fetch + rate-limiting logic
src/data/                            — generation output lands here locally (gitignored)
.github/workflows/                     — the daily schedule + gist publish
```
