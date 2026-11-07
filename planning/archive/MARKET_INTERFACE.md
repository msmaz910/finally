# Unified Market Data Interface

How FinAlly gets stock prices from one Python API, regardless of the source:

- `MASSIVE_API_KEY` set and non-empty → **Massive REST poller** (see `MASSIVE_API.md`)
- Otherwise → **GBM simulator** (see `MARKET_SIMULATOR.md`)

This design is **already implemented** in `backend/app/market/`. This document is the contract downstream code (portfolio, watchlist, chat, SSE) should rely on, plus required fixes found during the 2026-11-04 API research (Section 7).

---

## 1. Design in One Picture

```
          create_market_data_source(cache)      reads MASSIVE_API_KEY
                       │
        ┌──────────────┴──────────────┐
        ▼                             ▼
SimulatorDataSource            MassiveDataSource
(GBM, every 0.5 s)             (REST snapshot, every 15 s)
        │  writes                     │  writes
        └──────────────┬──────────────┘
                       ▼
                  PriceCache  (thread-safe, in memory, versioned)
                       │  reads
     ┌─────────────────┼──────────────────┬──────────────┐
     ▼                 ▼                  ▼              ▼
SSE /api/stream   GET /api/portfolio   POST trade    LLM chat context
```

The core idea: **producers write to the cache, consumers read from the cache.** No downstream code ever calls the simulator or Massive directly for a price. This is the Strategy pattern: two interchangeable sources behind one interface.

---

## 2. Public API

Everything is imported from one package:

```python
from app.market import (
    PriceUpdate,                # immutable price snapshot
    PriceCache,                 # shared in-memory store
    MarketDataSource,           # abstract interface
    create_market_data_source,  # factory (env-var driven)
    create_stream_router,       # FastAPI SSE router
)
```

### 2.1 `PriceUpdate` (`models.py`)

Frozen dataclass, one ticker at one moment.

| Field / property | Type | Meaning |
|---|---|---|
| `ticker` | `str` | e.g. `"AAPL"` |
| `price` | `float` | Latest price, rounded to 2 dp |
| `previous_price` | `float` | Price at the previous update |
| `timestamp` | `float` | Unix **seconds** |
| `change` | `float` | `price - previous_price` |
| `change_percent` | `float` | Percent change vs. previous update |
| `direction` | `str` | `"up"`, `"down"`, or `"flat"` |
| `to_dict()` | `dict` | JSON-ready form used by SSE |

Note: `change` is tick-to-tick, not vs. the day's open. The frontend's "daily change %" should be computed against the first price it received (or a session open if one is added later).

### 2.2 `PriceCache` (`cache.py`)

| Method | Returns | Used by |
|---|---|---|
| `update(ticker, price, timestamp=None)` | `PriceUpdate` | Data sources only |
| `get(ticker)` | `PriceUpdate \| None` | Watchlist API |
| `get_price(ticker)` | `float \| None` | Trades, portfolio valuation |
| `get_all()` | `dict[str, PriceUpdate]` | SSE, LLM context |
| `remove(ticker)` | `None` | Data sources only |
| `version` | `int` | SSE change detection |
| `ticker in cache`, `len(cache)` | | Validation |

`version` increments on every write. The SSE loop compares it to the last value it sent and only pushes when something changed.

### 2.3 `MarketDataSource` (`interface.py`)

```python
class MarketDataSource(ABC):
    async def start(self, tickers: list[str]) -> None: ...   # call once
    async def stop(self) -> None: ...                        # safe to repeat
    async def add_ticker(self, ticker: str) -> None: ...     # no-op if present
    async def remove_ticker(self, ticker: str) -> None: ...  # also clears cache
    def get_tickers(self) -> list[str]: ...
```

Both sources seed the cache inside `start()` so the first SSE event and first trade have a price immediately.

### 2.4 Factory (`factory.py`)

```python
def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    return SimulatorDataSource(price_cache=price_cache)
```

Returns an **unstarted** source. The caller awaits `start()`.

---

## 3. Wiring It Into FastAPI

Use the FastAPI `lifespan` context manager (the current API; `@app.on_event` is deprecated):

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.market import PriceCache, create_market_data_source, create_stream_router


price_cache = PriceCache()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start market data on boot, stop it on shutdown."""
    source = create_market_data_source(price_cache)
    await source.start(tracked_tickers())   # watchlist ∪ open positions, from SQLite
    app.state.price_cache = price_cache
    app.state.market_source = source
    yield
    await source.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
```

The cache is created at module level because `create_stream_router` needs it when the router is built, before the lifespan runs.

Route handlers read prices through `request.app.state.price_cache`.

---

## 4. Tracked Tickers Rule (Watchlist ∪ Positions)

From PLAN.md Section 6: the source must track every watchlist ticker **and** every held ticker. `remove_ticker()` also deletes the price from the cache, so callers must check first:

```python
async def untrack_if_unused(ticker: str, source: MarketDataSource) -> None:
    """Stop tracking a ticker only when it is neither watched nor held."""
    if not in_watchlist(ticker) and not has_open_position(ticker):
        await source.remove_ticker(ticker)
```

| Event | Action |
|---|---|
| Watchlist add | `await source.add_ticker(t)` |
| Buy (manual or chat) | `await source.add_ticker(t)` (no-op if already tracked) |
| Watchlist remove | `untrack_if_unused(t)` |
| Position fully sold | `untrack_if_unused(t)` |
| App start | `start(watchlist ∪ positions)` |

---

## 5. Consumer Examples

**Trade execution** (needs a current price):

```python
price = request.app.state.price_cache.get_price(ticker)
if price is None:
    raise HTTPException(400, f"No price available for {ticker}")
```

**Portfolio valuation:**

```python
def total_value(cash: float, positions: list[Position], cache: PriceCache) -> float:
    """Cash plus market value of every position at the latest cached price."""
    return cash + sum(p.quantity * (cache.get_price(p.ticker) or p.avg_cost) for p in positions)
```

**SSE event payload** (one event contains all tickers):

```
data: {"AAPL": {"ticker": "AAPL", "price": 190.42, "previous_price": 190.38,
                "timestamp": 1730736012.3, "change": 0.04,
                "change_percent": 0.021, "direction": "up"}, ...}
```

---

## 6. Behavior Differences Between Sources

| | Simulator | Massive |
|---|---|---|
| Update cadence | 0.5 s | 15 s default (`poll_interval`) |
| Prices | Synthetic GBM | Real (delay depends on plan) |
| Unknown ticker added | Gets a random seed price ($50-$300) | Absent from results, so no price |
| Market closed | Keeps moving | Price stays flat |
| Needs network / key | No | Yes |

Downstream code must treat "ticker has no price" (`get_price() is None`) as a normal case, mainly for Massive.

---

## 7. Required Fixes Found During Research

These were proven against the real `massive` 2.2.0 SDK, not guessed.

### 7.1 Bug: `last_trade.timestamp` does not exist (Massive source)

`massive_client.py` reads `snap.last_trade.timestamp / 1000.0`.

- The real `LastTrade` model has **no `timestamp` field**; it has `sip_timestamp`, in **nanoseconds**.
- Evidence: `hasattr(TickerSnapshot.from_dict({...}).last_trade, "timestamp")` returns `False`.
- Effect: every snapshot raises `AttributeError`, is logged as "Skipping snapshot", and **no real prices ever reach the cache**.
- Why tests pass: `test_massive.py` builds snapshots with `MagicMock`, which accepts any attribute name.

**Fix:** use the fallback extraction from `MASSIVE_API.md` Section 5 (`last_trade.price` → `min.close` → `day.close` → `prev_day.close`, timestamp from `sip_timestamp` or `updated` divided by `1e9`). Build test snapshots with `TickerSnapshot.from_dict(...)` instead of `MagicMock`, so tests use the real field names.

### 7.2 Gap: `last_trade` is `None` on the Starter plan

Starter includes snapshots but not trades, so `last_trade` is `None`. The same fallback in 7.1 covers this.

### 7.3 Decision needed: free (Basic) keys cannot call the snapshot endpoint

The current code assumes a free key can poll snapshots every 15 s. In reality a Basic key gets a 403 on every poll. Options:

| Option | How | Pros | Cons |
|---|---|---|---|
| **A. Document it** (recommended) | README says real data needs Starter or higher; log a clear error on 403 | Zero new code; simplest | Free-key users see no prices |
| B. Fall back to simulator on 403 | Factory or poller catches the first 403 and switches source | App always works | Hides the fact the key does not give real data |
| C. EOD mode on 403 | Poll `get_grouped_daily_aggs` (1 call) for yesterday's closes | Real prices on free plan | Prices never move; dull demo; more code |

Recommendation: **A** now. The simulator is the default and the better demo; real data is an optional extra.

### 7.4 Minor: SSE docstring

`stream.py` docstring says it "streams all tracked ticker prices every ~500ms". It actually checks every 500 ms and pushes only when `PriceCache.version` changed (PLAN.md review note 3).

---

## 8. Testing the Interface

- Both sources must pass the same contract tests: after `start(["AAPL"])`, `cache.get("AAPL")` is not `None`; after `remove_ticker("AAPL")`, it is `None`; `get_tickers()` reflects adds and removes.
- Factory: empty or whitespace `MASSIVE_API_KEY` → simulator; any value → Massive.
- Massive: use real SDK models (`TickerSnapshot.from_dict`) in fixtures, never bare `MagicMock` snapshots.
