# Market Data Backend — Summary

**Status:** Working and tested. The simulator is ready for downstream use. The Massive (real data) source now works, after the fixes in PR #4. Some second-review findings are still open (see below and `MARKET_DATA_REVIEW.md`). `MARKET_DATA_DESIGN.md` describes the target state; most of its Section 14 changes are not applied yet.

## What Was Built

A complete market data subsystem in `backend/app/market/` (8 modules, ~500 lines) providing live price simulation and real market data via a unified interface.

### Architecture

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Polygon.io REST poller (when MASSIVE_API_KEY set)
        │
        ▼
   PriceCache (thread-safe, in-memory)
        │
        ├──→ SSE stream endpoint (/api/stream/prices)
        ├──→ Portfolio valuation
        └──→ Trade execution
```

### Modules

| File | Purpose |
|------|---------|
| `models.py` | `PriceUpdate` — immutable frozen dataclass (ticker, price, previous_price, timestamp, change, direction) |
| `interface.py` | `MarketDataSource` — abstract base class defining `start/stop/add_ticker/remove_ticker/get_tickers` |
| `cache.py` | `PriceCache` — thread-safe price store with version counter for SSE change detection |
| `seed_prices.py` | Realistic seed prices, per-ticker GBM params (drift/volatility), correlation groups |
| `simulator.py` | `GBMSimulator` (Geometric Brownian Motion with Cholesky-correlated moves) + `SimulatorDataSource` |
| `massive_client.py` | `MassiveDataSource` — REST polling client for Polygon.io via the `massive` package |
| `factory.py` | `create_market_data_source()` — selects simulator or Massive based on `MASSIVE_API_KEY` env var |
| `stream.py` | `create_stream_router()` — FastAPI SSE endpoint factory using version-based change detection |

### Key Design Decisions

- **Strategy pattern** — both data sources implement the same ABC; downstream code is source-agnostic
- **PriceCache as single point of truth** — producers write, consumers read; no direct coupling
- **GBM with correlated moves** — Cholesky decomposition of sector-based correlation matrix; tech stocks correlate at 0.6, finance at 0.5, cross-sector at 0.3
- **Random shock events** — ~0.1% chance per tick per ticker of a 2-5% move for visual drama
- **SSE over WebSockets** — simpler, one-way push, universal browser support

## Test Suite

**83 tests, all passing.** 7 test modules in `backend/tests/market/` (as of 2026-11-06).

| Module | Tests | Coverage |
|--------|-------|----------|
| test_models.py | 11 | models.py: 100% |
| test_cache.py | 15 | cache.py: 100% |
| test_simulator.py | 19 | simulator.py: 98% |
| test_simulator_source.py | 10 | (integration tests) |
| test_factory.py | 7 | factory.py: 100% |
| test_massive.py | 19 | massive_client.py: 95% (built with real `TickerSnapshot` SDK models) |
| test_stream.py | 2 | stream.py: 42% (router only; SSE generator untested) |

Overall coverage: 92%.

## Second Code Review (2026-11-06)

Full findings are in `MARKET_DATA_REVIEW.md`. Each fix below has a test that failed before the change (PR #4):

1. **Massive never cached a price (P1)**: the code read `last_trade.timestamp`, which the SDK doesn't have. New `extract_price()` uses `sip_timestamp` (nanoseconds) and falls back to minute/day/previous-day close (Starter plan has no last trade). The old tests used `MagicMock`, which hid the bug.
2. **SSE router shared across calls (P1)**: `create_stream_router()` now builds a new router on each call.
3. **Massive poll race (P2)**: a ticker removed mid-poll is no longer written back to the cache.
4. **Cache removal invisible to SSE (P2)**: `PriceCache.remove()` now bumps `version`.

**Still open:**

- The simulator doesn't uppercase tickers (Massive does).
- `SimulatorDataSource.add_ticker()` before `start()` is dropped.
- No invalid-ticker signal from `add_ticker()`. Decide this before building the watchlist API.
- No SSE heartbeat, and no daily change % field.
- Small P3 cleanups (deprecated `conftest.py` fixture, `rich` as a core dependency).

## First Code Review & Fixes Applied

The first code review identified 7 issues. All were resolved:

1. **pyproject.toml build config** — added `[tool.hatch.build.targets.wheel] packages = ["app"]`
2. **Lazy imports removed** — `massive` is a core dependency; imports moved to top level
3. **SSE return type fixed** — `_generate_events` annotated as `AsyncGenerator[str, None]`
4. **Public `get_tickers()`** — added to `GBMSimulator` to avoid private attribute access
5. **Correlation constants cleaned up** — removed unused `DEFAULT_CORR`, consolidated into `CROSS_GROUP_CORR`
6. **Unused test imports removed** — `pytest`, `math`, `asyncio` cleaned from 4 test files
7. **Massive test mocks fixed** — `source._client` set in tests, patches target correct names

## Demo

A Rich terminal demo is available at `backend/market_data_demo.py`:

```bash
cd backend
uv run market_data_demo.py
```

Displays a live-updating dashboard with all 10 tickers, sparklines, color-coded direction arrows, and an event log for notable price moves. Runs 60 seconds or until Ctrl+C.

## Usage for Downstream Code

```python
from app.market import PriceCache, create_market_data_source

# Startup
cache = PriceCache()
source = create_market_data_source(cache)  # Reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT", ...])

# Read prices
update = cache.get("AAPL")          # PriceUpdate or None
price = cache.get_price("AAPL")     # float or None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

# Dynamic watchlist
await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")

# Shutdown
await source.stop()
```
