# Market Data Backend — Detailed Design

The full design for FinAlly's market data subsystem: one Python API that serves live prices from either a **GBM simulator** (default) or the **Massive REST API** (when `MASSIVE_API_KEY` is set), plus the SSE stream and the rule that decides which tickers are tracked.

Every code block below is a complete module or a complete function. All of them were run together on 2026-11-06 as one package: **83 tests pass and `ruff check` is clean**. The FastAPI wiring in Section 11 was also started with uvicorn and checked with curl.

**How this relates to what is already built.** `backend/app/market/` already exists (see `MARKET_DATA_SUMMARY.md`). This document is the target version of that code. It keeps the same architecture and public API, and fixes the problems found during the Massive API research (`MARKET_INTERFACE.md` Section 7) and in a code read for this design. Section 14 lists each change, so the code can be updated one file at a time.

Related docs: `MARKET_INTERFACE.md` (the contract), `MARKET_SIMULATOR.md` (why the math works), `MASSIVE_API.md` (endpoint research).

---

## Contents

1. Goals and Non-Goals
2. Architecture
3. Module Layout
4. `models.py`: `PriceUpdate` and ticker normalization
5. `cache.py`: `PriceCache`
6. `interface.py`: `MarketDataSource`
7. Simulator: `seed_prices.py` and `simulator.py`
8. Massive: `massive_client.py`
9. `factory.py`: choosing the source
10. `stream.py`: SSE endpoint
11. `tracking.py` and FastAPI wiring
12. Configuration
13. Failure Modes
14. Changes from the Current Code
15. Testing
16. Answers to PLAN.md Review Notes

---

## 1. Goals and Non-Goals

**Goals**

| # | Requirement | Source |
|---|---|---|
| G1 | One interface, two interchangeable sources, chosen by env var | PLAN §6 |
| G2 | Simulator: GBM, ~500 ms ticks, correlated sectors, random 2-5% events | PLAN §6 |
| G3 | Massive: REST polling of all tickers in one call, interval configurable | PLAN §6 |
| G4 | One in-memory cache: latest price, previous price, timestamp per ticker | PLAN §6 |
| G5 | `GET /api/stream/prices` SSE: ticker, price, previous price, timestamp, direction | PLAN §6 |
| G6 | Tracked tickers = watchlist ∪ open positions | PLAN §6 |
| G7 | Every tracked ticker has a price as soon as it is added (trades need it) | PLAN §8 trade flow |
| G8 | Daily change % for the watchlist | PLAN §10 |

**Non-goals:** price history storage (the frontend builds sparklines from the stream), multi-user, market hours, bid/ask spreads, WebSockets, and Massive's EOD-only mode for free keys (Section 8.4).

---

## 2. Architecture

```
                 create_market_data_source(cache)        reads MASSIVE_API_KEY
                              │
            ┌─────────────────┴─────────────────┐
            ▼                                   ▼
   SimulatorDataSource                  MassiveDataSource
   GBMSimulator.step() every 0.5 s      get_snapshot_all() every 15 s
   (asyncio task)                       (asyncio task → worker thread)
            │ cache.update()                    │ cache.update()
            └─────────────────┬─────────────────┘
                              ▼
                ┌──────────────────────────────┐
                │ PriceCache                   │  dict[ticker → PriceUpdate]
                │ threading.Lock, version int  │
                └──────────────────────────────┘
                              │ get / get_price / get_all / version
      ┌──────────────┬────────┴───────┬───────────────┬───────────────┐
      ▼              ▼                ▼               ▼               ▼
 SSE stream    GET /portfolio   POST /trade    GET /watchlist   LLM context

 sync_tracked_tickers(source, watchlist, positions)
   called by the watchlist, trade and chat code after every change
   → source.add_ticker() / source.remove_ticker()
```

**Rules that hold everywhere**

1. **Producers write, consumers read.** Only the active source calls `cache.update()` or `cache.remove()`. Routes, the SSE loop and the LLM only read the cache.
2. **Exactly one source runs per process.** It is created in the FastAPI `lifespan` and stored on `app.state.market_source`.
3. **Ticker strings are normalized at the boundary.** `normalize_ticker()` uppercases and validates. Every source method calls it, so `"aapl"` and `"AAPL"` can never become two entries.
4. **"No price" is a normal state.** `cache.get_price(t)` can return `None` (mostly with Massive), and callers must handle it.

### Concurrency model

| Actor | Runs on | Touches |
|---|---|---|
| Simulator loop | event loop (asyncio task) | `GBMSimulator`, `PriceCache` |
| Massive poll | event loop; the HTTP call runs in a worker thread via `asyncio.to_thread` | `PriceCache` (from the loop only) |
| SSE generators (one per client) | event loop | `PriceCache` reads |
| Route handlers | event loop | `PriceCache` reads, `source.add/remove_ticker` |

All writes happen on the event loop. `PriceCache` still uses a `threading.Lock`, so sync code in a threadpool (e.g. a plain `def` FastAPI route) can read it safely. The lock is held only for a dict operation, so it never blocks the loop in any meaningful way.

One real race exists with Massive: a ticker can be removed while an HTTP request is in flight. Section 8 handles it by re-checking the tracked set before writing.

---

## 3. Module Layout

```
backend/app/market/
├── __init__.py         public API re-exports
├── models.py           PriceUpdate, normalize_ticker()
├── cache.py            PriceCache
├── interface.py        MarketDataSource (ABC)
├── seed_prices.py      simulator constants (unchanged)
├── simulator.py        GBMSimulator + SimulatorDataSource
├── massive_client.py   extract_price() + MassiveDataSource
├── factory.py          create_market_data_source()
├── stream.py           create_stream_router(), generate_events()
└── tracking.py         sync_tracked_tickers()           ← new
```

`__init__.py`:

```python
"""Market data subsystem for FinAlly."""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate, normalize_ticker
from .stream import create_stream_router
from .tracking import sync_tracked_tickers

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
    "normalize_ticker",
    "sync_tracked_tickers",
]
```

Downstream code imports only from `app.market`, never from the submodules. The one exception is tests.

---

## 4. `models.py`: `PriceUpdate` and ticker normalization

```python
"""Data models for market data."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field

_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def normalize_ticker(raw: str) -> str:
    """Uppercase and strip a ticker symbol; raise ValueError if it is not well-formed.

    Accepts e.g. "aapl", " BRK.B ", "RDS-A". Rejects "", "123", "AAPL;DROP".
    This is a format check only; whether the symbol really trades is
    decided by the data source (see MarketDataSource.add_ticker).
    """
    ticker = raw.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    return ticker


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds
    prev_close: float | None = None  # Reference for "daily change"

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' versus the previous update."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    @property
    def day_change_percent(self) -> float | None:
        """Percent change versus prev_close, or None if no reference is known."""
        if not self.prev_close:
            return None
        return round((self.price - self.prev_close) / self.prev_close * 100, 4)

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "prev_close": self.prev_close,
            "day_change_percent": self.day_change_percent,
        }
```

**Design notes**

- **Frozen + slots**: a `PriceUpdate` is a value. The cache swaps whole objects and never mutates them, so readers can hold a reference without copying it.
- **Two kinds of change.** `change` / `change_percent` / `direction` compare to the *previous tick*, and drive the green/red flash. `day_change_percent` compares to `prev_close`, and drives the watchlist's "daily change %" column. Before this design, the frontend had to make up a reference price itself.
- **`prev_close` meaning by source**: Massive uses the real previous session close (`prevDay.c`). The simulator uses the seed price at startup, which is "the close before the app opened".
- **Ticker regex** `^[A-Z][A-Z0-9.\-]{0,9}$`: accepts `AAPL`, `BRK.B`, `RDS-A`; rejects empty strings, leading digits, spaces and punctuation such as `"AAPL; DROP"`. It is a format check only: `"Apple"` normalizes to a well-formed `APPLE`. Whether a symbol *exists* is decided by the source (Section 6: `add_ticker` returns `bool`).

SSE / REST JSON for one ticker:

```json
{
  "ticker": "AAPL",
  "price": 190.42,
  "previous_price": 190.38,
  "timestamp": 1730736012.3,
  "change": 0.04,
  "change_percent": 0.021,
  "direction": "up",
  "prev_close": 188.0,
  "day_change_percent": 1.2872
}
```

---

## 5. `cache.py`: `PriceCache`

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Latest price per ticker. One writer (the active data source), many readers.

    `version` increases on every change (update or remove) so the SSE loop
    can push only when something actually changed.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        prev_close: float | None = None,
    ) -> PriceUpdate:
        """Record a new price and return the resulting PriceUpdate.

        previous_price comes from the prior entry (equal to price on the first
        write, so direction starts as 'flat'). prev_close is sticky: if not
        given, the existing reference is kept, or the first price seen is used.
        """
        with self._lock:
            prev = self._prices.get(ticker)
            if prev_close is None:
                prev_close = prev.prev_close if prev else round(price, 2)
            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=prev.price if prev else round(price, 2),
                timestamp=timestamp if timestamp is not None else time.time(),
                prev_close=round(prev_close, 2),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow copy of every current price (safe to iterate)."""
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Design notes**

- **`version`** is a global counter, not one per ticker. The SSE loop only needs to know "did anything change since I last sent?" and always sends the full snapshot (10-50 tickers, about 2-8 KB), which is simpler than diffs and cheap at this size.
- **`remove()` bumps `version`**, so connected clients learn about a removal on the next check (0.5 s). Before, with Massive, a removed ticker stayed on screen until the next poll (15 s).
- **`prev_close` is sticky.** The simulator loop calls `update(ticker, price)` without it, and the reference set at seed time is kept.
- **`timestamp if timestamp is not None`** rather than `timestamp or time.time()`: the old version would silently replace a timestamp of `0` with the current time.
- **Rounding** happens here, once, to cents. Sources keep full precision internally (the simulator relies on this so rounding error does not accumulate).

Usage:

```python
cache = PriceCache()
cache.update("AAPL", 190.004, prev_close=188.0)   # first write: direction "flat"
cache.update("AAPL", 190.25)                      # direction "up", prev_close still 188.0
cache.get_price("AAPL")      # 190.25
cache.get("AAPL").day_change_percent   # 1.1968
"AAPL" in cache, len(cache)  # True, 1
cache.remove("AAPL")         # version += 1
```

---

## 6. `interface.py`: `MarketDataSource`

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    A source owns a background task that writes into a shared PriceCache.
    Downstream code never asks a source for a price; it reads the cache.

    All ticker arguments are normalized with normalize_ticker() by the
    implementation, so callers may pass "aapl" or " AAPL ".
    """

    #: Short identifier for logs and /api/health ("simulator" or "massive").
    name: str = "unknown"

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Seed the cache for `tickers`, then start the background task.

        Call exactly once. When it returns, every ticker the source can price
        is already in the cache.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task. Safe to call more than once."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> bool:
        """Start tracking `ticker` (no-op if already tracked).

        Tries to price it immediately and returns True if the cache now has a
        price for it, False otherwise (e.g. Massive does not know the symbol).
        A False result leaves the ticker tracked; the caller decides whether
        to keep it or call remove_ticker().
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking `ticker` and delete it from the cache. No-op if absent.

        Callers must apply the tracked-tickers rule first (see tracking.py).
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers, in insertion order."""
```

**Contract** (tested for both sources in `tests/market/test_contract.py`, see Section 15):

| Call | Guarantee |
|---|---|
| `start(tickers)` | Duplicates and case are collapsed. On return, every priceable ticker is in the cache. |
| `add_ticker(t)` | Raises `ValueError` for a malformed symbol. Returns `True` once `t` has a price. Idempotent. |
| `remove_ticker(t)` | Removes from the source **and** the cache. Idempotent. |
| `get_tickers()` | Copy of the tracked list, normalized, in insertion order. |
| `stop()` | Cancels the task. Idempotent. No cache writes afterwards. |
| `name` | `"simulator"` or `"massive"`, shown by `/api/health`. |

**Why `add_ticker` returns `bool`:** it answers PLAN review note 5 (an invalid ticker from the user or the LLM). The simulator can price anything, so it returns `True`. Massive returns `False` when the symbol is missing from the snapshot results, which is how Massive reports an unknown ticker. The caller turns that into an error message (Section 11).

---

## 7. Simulator: `seed_prices.py` and `simulator.py`

### 7.1 `seed_prices.py` (unchanged)

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

### 7.2 The math in brief

Each tick, for each ticker `i` (full derivation in `MARKET_SIMULATOR.md`):

```
z      = L @ N(0, I)                         L = cholesky(correlation matrix)
S_i   *= exp((mu_i - sigma_i²/2)·dt + sigma_i·√dt·z_i)
dt     = 0.5 s / (252 · 6.5 · 3600 s)  ≈ 8.48e-8
with probability 0.001:  S_i *= 1 ± U(0.02, 0.05)      (shock event)
```

Worked example for AAPL (sigma 0.22, mu 0.05), one tick with `z = 1.0`:

```
drift     = (0.05 - 0.0242) · 8.48e-8 = 2.19e-9
diffusion = 0.22 · √8.48e-8 · 1.0     = 6.41e-5
190.00 · exp(6.41e-5) = 190.0122  →  shown as 190.01
```

### 7.3 `GBMSimulator` (pure math, unchanged)

Synchronous and free of I/O, so it can be unit-tested and benchmarked without asyncio.

```python
class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)

    Where:
        S(t)   = current price
        mu     = annualized drift (expected return)
        sigma  = annualized volatility
        dt     = time step as fraction of a trading year
        Z      = correlated standard normal random variable

    The tiny dt (~8.5e-8 for 500ms ticks over 252 trading days * 6.5h/day)
    produces sub-cent moves per tick that accumulate naturally over time.
    """

    # 500ms expressed as a fraction of a trading year
    # 252 trading days * 6.5 hours/day * 3600 seconds/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        # Per-ticker state
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}

        # Cholesky decomposition of the correlation matrix (for correlated moves)
        self._cholesky: np.ndarray | None = None

        # Initialize all starting tickers
        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        This is the hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        # Generate n independent standard normal draws
        z_independent = np.random.standard_normal(n)

        # Apply Cholesky to get correlated draws
        if self._cholesky is not None:
            z_correlated = self._cholesky @ z_independent
        else:
            z_correlated = z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu = params["mu"]
            sigma = params["sigma"]

            # GBM: S(t+dt) = S(t) * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick per ticker
            # With 10 tickers at 2 ticks/sec, expect an event ~every 50 seconds
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker,
                    shock_magnitude * 100,
                    "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the ticker correlation matrix.

        Called whenever tickers are added or removed. O(n^2) but n < 50.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        # Build the correlation matrix
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Determine correlation between two tickers based on sector grouping.

        Correlation structure:
          - Same tech sector:   0.6
          - Same finance sector: 0.5
          - TSLA with anything: 0.3 (it does its own thing)
          - Cross-sector:       0.3
          - Unknown tickers:    0.3
        """
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is in tech set but behaves independently
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR

        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR

        return CROSS_GROUP_CORR
```

### 7.4 `SimulatorDataSource` (async adapter)

Imports at the top of `simulator.py`:

```python
from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .models import normalize_ticker
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)
```

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by GBMSimulator.

    A background asyncio task calls step() every `update_interval` seconds
    and writes each new price to the PriceCache.
    """

    name = "simulator"

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        normalized = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
        self._sim = GBMSimulator(normalized, event_probability=self._event_prob)
        for ticker in self._sim.get_tickers():
            self._seed_cache(ticker)  # SSE and trades have a price before tick 1
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(normalized))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> bool:
        ticker = normalize_ticker(ticker)
        if self._sim is None:
            return False
        if ticker not in self._sim.get_tickers():
            self._sim.add_ticker(ticker)
            self._seed_cache(ticker)
            logger.info("Simulator: added %s", ticker)
        return ticker in self._cache

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    def _seed_cache(self, ticker: str) -> None:
        price = self._sim.get_price(ticker) if self._sim else None
        if price is not None:
            # The seed price doubles as "previous close" for daily change %.
            self._cache.update(ticker=ticker, price=price, prev_close=price)

    async def _run_loop(self) -> None:
        """Step, write, sleep, forever. One bad tick must never kill the task."""
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

**Design notes**

- `start()` seeds the cache **before** creating the task, so the first SSE event and the first trade never see an empty cache.
- `add_ticker()` seeds the cache immediately with `prev_close` = seed price, so `day_change_percent` starts at 0 for a newly added ticker.
- The `try/except` in `_run_loop` is deliberate: an unexpected error (e.g. a `LinAlgError` from a bad correlation config) is logged and the next tick tries again. Prices never freeze silently.
- `stop()` cancels and awaits the task, so after it returns nothing writes to the cache.

---

## 8. Massive: `massive_client.py`

### 8.1 Which endpoint and why

One call to the **Full Market Snapshot** (`GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=...`, SDK `get_snapshot_all`) returns every tracked ticker. No other endpoint gives live prices for many tickers in one call (comparison in `MASSIVE_API.md` §4).

Plan matters (verified 2026-11-04):

| Plan | Snapshot? | `lastTrade` present? | What the poller uses |
|---|---|---|---|
| Basic (free) | **No, 403** | — | Nothing. Logs one clear error (8.4) |
| Starter | Yes, 15-min delayed | No (`None`) | `min.close` |
| Developer | Yes, 15-min delayed | Yes | `last_trade.price` |
| Advanced | Yes, real-time | Yes | `last_trade.price` |

### 8.2 Code

```python
"""Massive (formerly Polygon.io) REST poller for real market data."""

from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.exceptions import BadResponse
from massive.rest.models import SnapshotMarketType, TickerSnapshot

from .cache import PriceCache
from .interface import MarketDataSource
from .models import normalize_ticker

logger = logging.getLogger(__name__)

NANOS_PER_SECOND = 1_000_000_000
MILLIS_PER_SECOND = 1_000


def extract_price(snap: TickerSnapshot) -> tuple[float, float] | None:
    """Return (price, unix_seconds) from a snapshot, or None if it has no price.

    Fields vary by plan and time of day:
      - last_trade is None on Starter (no trades entitlement)
      - day.close is 0 / missing before the open
    so fall back: last_trade -> min.close -> day.close -> prev_day.close.
    """
    updated = (snap.updated or 0) / NANOS_PER_SECOND
    trade = snap.last_trade
    if trade and trade.price:
        ts = (trade.sip_timestamp or 0) / NANOS_PER_SECOND
        return float(trade.price), ts or updated
    if snap.min and snap.min.close:
        ts = (snap.min.timestamp or 0) / MILLIS_PER_SECOND  # minute bars use ms
        return float(snap.min.close), updated or ts
    for bar in (snap.day, snap.prev_day):
        if bar and bar.close:
            return float(bar.close), updated
    return None


def extract_prev_close(snap: TickerSnapshot) -> float | None:
    """Previous session close: the reference for daily change %."""
    if snap.prev_day and snap.prev_day.close:
        return float(snap.prev_day.close)
    return None


class MassiveDataSource(MarketDataSource):
    """Polls the Full Market Snapshot endpoint for all tracked tickers in one call.

    Needs a Starter plan or higher; a free (Basic) key gets 403 on snapshots.
    Paid plans have no call limit, so poll_interval is about freshness, not quota.
    """

    name = "massive"

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None
        self._last_error: str | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
        await self._poll_once(self._tickers)  # cache is warm before we return
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(self._tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> bool:
        ticker = normalize_ticker(ticker)
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            # Price it now (one extra call) so a trade right after
            # "add to watchlist" does not wait up to poll_interval.
            await self._poll_once([ticker])
            logger.info("Massive: added %s (priced=%s)", ticker, ticker in self._cache)
        return ticker in self._cache

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- internals ---

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self, tickers: list[str] | None = None) -> int:
        """Fetch one snapshot batch and write it to the cache. Never raises.

        `tickers` defaults to everything tracked. Returns tickers written.
        """
        tickers = list(self._tickers) if tickers is None else tickers
        if not tickers or self._client is None:
            return 0
        try:
            snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except BadResponse as e:
            self._log_error_once(f"Massive API error (check plan / key): {e}")
            return 0
        except Exception as e:  # network errors, timeouts, ...
            self._log_error_once(f"Massive poll failed: {e!r}")
            return 0
        self._last_error = None

        written = 0
        tracked = set(self._tickers)  # re-read: tickers may be removed mid-request
        for snap in snapshots:
            if snap.ticker not in tracked:
                continue
            extracted = extract_price(snap)
            if extracted is None:
                logger.debug("Massive: no price in snapshot for %s", snap.ticker)
                continue
            price, ts = extracted
            current = self._cache.get(snap.ticker)
            if current and current.timestamp == ts and current.price == round(price, 2):
                continue  # market closed / no new trade: don't bump the version
            self._cache.update(snap.ticker, price, ts, prev_close=extract_prev_close(snap))
            written += 1
        logger.debug("Massive poll: wrote %d/%d tickers", written, len(tickers))
        return written

    def _fetch_snapshots(self, tickers: list[str]) -> list[TickerSnapshot]:
        """Blocking SDK call; always run via asyncio.to_thread."""
        assert self._client is not None
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=tickers,
        )

    def _log_error_once(self, message: str) -> None:
        """Log a failure at ERROR the first time, DEBUG while it repeats."""
        if message != self._last_error:
            logger.error(message)
            self._last_error = message
        else:
            logger.debug(message)
```

### 8.3 Design notes

- **`extract_price` fallback chain** fixes the bug in `MARKET_INTERFACE.md` §7.1: the SDK's `LastTrade` has no `timestamp` attribute. The current code reads it, raises `AttributeError` for every snapshot, and so never writes a real price. The fixed code uses `sip_timestamp` (**nanoseconds**), and the fallback covers Starter, where `last_trade` is `None`. Units: `updated` and `sip_timestamp` are ns; `min.timestamp` is ms.
- **Prices for new tickers right away**: `add_ticker` makes a one-ticker snapshot call. Without it, "add PYPL to watchlist, then buy PYPL" would fail for up to 15 s with "no price". Paid plans have no call limit, so the extra call costs nothing.
- **Remove-during-poll race**: `_poll_once` copies the ticker list for the request, then re-reads `self._tickers` before writing. A ticker removed mid-request is not brought back into the cache.
- **No pointless pushes**: when the market is closed, every poll returns the same price and timestamp. The poller skips those, so `version` does not change and SSE stays quiet (heartbeats only).
- **Errors never escape `_poll_once`**. The SDK already retries 429/5xx three times (urllib3), so the poller does not add its own retry. It simply tries again next interval. Repeated identical errors are logged at ERROR once, then at DEBUG, so a bad key does not flood the log every 15 s.
- **Blocking SDK** runs in `asyncio.to_thread`. The default executor has plenty of threads for one request every few seconds.

### 8.4 Policy for free (Basic) keys

Recommendation A from `MARKET_INTERFACE.md` §7.3: **document it, do not work around it.** With a free key, the snapshot call returns 403 and the log shows:

```
ERROR app.market.massive_client: Massive API error (check plan / key): {"status":"NOT_AUTHORIZED", ...}
```

`/api/health` reports `"market_data": "massive"`, and watchlist prices are missing. The README should say: *"Real market data needs a Massive Starter plan or higher. Leave `MASSIVE_API_KEY` empty to use the simulator."* If falling back to the simulator becomes desirable later, the place to do it is the factory: try one `get_snapshot_all(["AAPL"])` call, and on `BadResponse` return a `SimulatorDataSource` instead.

---

## 9. `factory.py`: choosing the source

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)

DEFAULT_MASSIVE_POLL_SECONDS = 15.0
MIN_MASSIVE_POLL_SECONDS = 1.0


def _poll_interval_from_env() -> float:
    raw = os.environ.get("MASSIVE_POLL_INTERVAL", "").strip()
    if not raw:
        return DEFAULT_MASSIVE_POLL_SECONDS
    try:
        return max(MIN_MASSIVE_POLL_SECONDS, float(raw))
    except ValueError:
        logger.warning("Ignoring invalid MASSIVE_POLL_INTERVAL=%r", raw)
        return DEFAULT_MASSIVE_POLL_SECONDS


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Pick the data source from the environment. Returns it unstarted.

    MASSIVE_API_KEY non-empty -> MassiveDataSource, else SimulatorDataSource.
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        interval = _poll_interval_from_env()
        logger.info("Market data source: Massive API (poll every %.1fs)", interval)
        return MassiveDataSource(api_key, price_cache, poll_interval=interval)
    logger.info("Market data source: GBM simulator")
    return SimulatorDataSource(price_cache)
```

| `MASSIVE_API_KEY` | `MASSIVE_POLL_INTERVAL` | Result |
|---|---|---|
| unset, `""`, `"  "` | ignored | `SimulatorDataSource`, 0.5 s ticks |
| `"abc123"` | unset | `MassiveDataSource`, 15 s |
| `"abc123"` | `"5"` | `MassiveDataSource`, 5 s |
| `"abc123"` | `"0.1"` | clamped to 1 s |
| `"abc123"` | `"fast"` | warning logged, 15 s |

The 15 s default is safe on any plan. With Starter or higher, `MASSIVE_POLL_INTERVAL=5` makes the UI feel much more alive. Note that the data itself is still 15 minutes delayed below the Advanced plan.

---

## 10. `stream.py`: SSE endpoint

```python
"""SSE endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

CHECK_INTERVAL = 0.5  # how often to look at PriceCache.version
HEARTBEAT_INTERVAL = 15.0  # keep idle connections alive through proxies


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Build a fresh router bound to `price_cache` (safe to call more than once)."""
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """Push every tracked price whenever the cache changes.

        Event format (one event holds all tickers):
            data: {"AAPL": {"ticker": "AAPL", "price": 190.5, ...}, ...}
        """
        return StreamingResponse(
            generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


async def generate_events(
    price_cache: PriceCache,
    request: Request,
    check_interval: float = CHECK_INTERVAL,
    heartbeat_interval: float = HEARTBEAT_INTERVAL,
) -> AsyncGenerator[str, None]:
    """Yield SSE frames. Checks the cache every `check_interval` seconds and
    sends a full snapshot only when `version` changed; otherwise sends a
    comment line every `heartbeat_interval` seconds."""
    yield "retry: 1000\n\n"  # browser reconnects after 1 s
    last_version = -1
    idle = 0.0
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)
    try:
        while not await request.is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                prices = price_cache.get_all()
                payload = {t: u.to_dict() for t, u in prices.items()}
                yield f"id: {version}\ndata: {json.dumps(payload)}\n\n"
                idle = 0.0
            elif idle >= heartbeat_interval:
                yield ": ping\n\n"
                idle = 0.0
            await asyncio.sleep(check_interval)
            idle += check_interval
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("SSE client disconnected: %s", client)
```

**Design notes**

- **Router built inside the factory.** The current code creates `router` at module level, so a second `create_stream_router()` call (e.g. one app per test) registers `/prices` twice on the same object. A fresh router per call removes that trap.
- **Push on change, not on a timer** (PLAN review note 3). The loop checks `version` every 0.5 s and sends only when it changed. With the simulator that is every tick; with Massive it is once per poll.
- **Heartbeat** `: ping` (an SSE comment, ignored by `EventSource`) every 15 s of silence. This keeps proxies and load balancers (App Runner, nginx) from closing an idle stream, which matters with Massive after market close.
- **`id: <version>`** lets the browser report `Last-Event-ID` on reconnect. The server does not use it today, because every event is a full snapshot, but it helps when debugging.
- **`retry: 1000`**: EventSource reconnects 1 s after a drop. The first event after reconnecting is a full snapshot, so the client never needs to "catch up".

Wire format (two ticks and a heartbeat):

```
retry: 1000

id: 62
data: {"AAPL": {"ticker": "AAPL", "price": 189.99, "previous_price": 190.01, ...}, "MSFT": {...}}

id: 72
data: {"AAPL": {"ticker": "AAPL", "price": 190.02, "previous_price": 189.99, ...}, "MSFT": {...}}

: ping
```

Minimal frontend consumer (for reference; the Frontend Engineer owns the real code):

```ts
type PriceUpdate = {
  ticker: string; price: number; previous_price: number; timestamp: number;
  change: number; change_percent: number; direction: "up" | "down" | "flat";
  prev_close: number | null; day_change_percent: number | null;
};

const es = new EventSource("/api/stream/prices");
es.onopen = () => setStatus("connected");
es.onerror = () => setStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
es.onmessage = (e) => {
  const prices: Record<string, PriceUpdate> = JSON.parse(e.data);
  for (const p of Object.values(prices)) {
    appendSparklinePoint(p.ticker, p.timestamp, p.price);
    if (p.direction !== "flat") flash(p.ticker, p.direction);
  }
};
```

A ticker missing from an event has been removed. The client should replace its whole price map with each event, not merge into it.

---

## 11. `tracking.py` and FastAPI wiring

### 11.1 The tracked-tickers rule

PLAN §6: the source must track **watchlist ∪ open positions**, and must not drop a ticker that is still held. Instead of each route deciding when to call `add_ticker` / `remove_ticker`, which is easy to get wrong in one of five places, every route calls one idempotent function with the current state from the database:

```python
"""Tracked tickers = watchlist ∪ open positions (PLAN.md Section 6)."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from .interface import MarketDataSource
from .models import normalize_ticker

logger = logging.getLogger(__name__)


async def sync_tracked_tickers(
    source: MarketDataSource,
    watchlist: Iterable[str],
    positions: Iterable[str],
) -> set[str]:
    """Make the source track exactly watchlist ∪ positions. Idempotent.

    Call after any change to either set (watchlist add/remove, trade, chat
    actions), passing the current state read from the database. Adds what
    is missing, removes what is no longer needed, never removes a held
    ticker. Returns the tickers that are tracked but have no price.
    """
    wanted = {normalize_ticker(t) for t in (*watchlist, *positions)}
    current = set(source.get_tickers())

    unpriced: set[str] = set()
    for ticker in sorted(wanted - current):
        if not await source.add_ticker(ticker):
            unpriced.add(ticker)
    for ticker in sorted(current - wanted):
        await source.remove_ticker(ticker)

    if unpriced:
        logger.warning("Tracked but unpriced: %s", ", ".join(sorted(unpriced)))
    return unpriced
```

| Event | Call after the DB write |
|---|---|
| App start | `source.start(sorted(watchlist ∪ positions))` |
| `POST /api/watchlist` | `sync_tracked_tickers(source, watchlist, positions)` |
| `DELETE /api/watchlist/{t}` | same; `t` stays tracked if still held |
| `POST /api/portfolio/trade` buy of an unwatched ticker | same; adds `t` |
| Sell that closes a position | same; removes `t` unless watchlisted |
| Chat actions (several trades / watchlist changes) | same, **once** after all actions |

Cost: two set operations over at most ~50 tickers. Nothing is added or removed unless something changed.

**Buying a ticker that is not tracked yet.** The trade route needs a price before it writes the position, so it calls `await source.add_ticker(t)` first. If that returns `False`, it rejects the trade. It calls `sync_tracked_tickers` after the trade is written.

### 11.2 `main.py` wiring (market data parts)

The watchlist and positions below are in-memory stand-ins for the SQLite tables. The Backend agent replaces them with DB queries.

```python
"""FastAPI entry point (market data wiring only)."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from app.market import (
    PriceCache,
    create_market_data_source,
    create_stream_router,
    normalize_ticker,
    sync_tracked_tickers,
)

DEFAULT_WATCHLIST = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "NFLX"]

price_cache = PriceCache()
_watchlist = list(DEFAULT_WATCHLIST)  # stand-in for the SQLite watchlist table
_positions: dict[str, float] = {}  # stand-in for the positions table


@asynccontextmanager
async def lifespan(app: FastAPI):
    source = create_market_data_source(price_cache)
    await source.start(sorted(set(_watchlist) | set(_positions)))
    app.state.price_cache = price_cache
    app.state.market_source = source
    try:
        yield
    finally:
        await source.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(create_stream_router(price_cache))


@app.get("/api/health")
async def health(request: Request) -> dict:
    source = request.app.state.market_source
    return {"status": "ok", "market_data": source.name, "tickers": len(source.get_tickers())}


class AddTicker(BaseModel):
    ticker: str


@app.post("/api/watchlist")
async def add_to_watchlist(body: AddTicker, request: Request) -> dict:
    try:
        ticker = normalize_ticker(body.ticker)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    source = request.app.state.market_source
    if ticker not in _watchlist:
        _watchlist.append(ticker)
    unpriced = await sync_tracked_tickers(source, _watchlist, _positions)
    if ticker in unpriced:
        _watchlist.remove(ticker)  # unknown to the data source: roll back
        await sync_tracked_tickers(source, _watchlist, _positions)
        raise HTTPException(404, f"No market data for {ticker}")
    update = request.app.state.price_cache.get(ticker)
    return {"ticker": ticker, "price": update.price}


@app.delete("/api/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str, request: Request) -> dict:
    ticker = normalize_ticker(ticker)
    if ticker in _watchlist:
        _watchlist.remove(ticker)
    await sync_tracked_tickers(request.app.state.market_source, _watchlist, _positions)
    return {"ticker": ticker, "still_tracked": ticker in _positions}
```

What this looks like from the outside (actual output from the run on 2026-11-06, simulator):

```bash
curl -s localhost:8000/api/health
# {"status":"ok","market_data":"simulator","tickers":10}
curl -s -XPOST localhost:8000/api/watchlist -H 'content-type: application/json' -d '{"ticker":"pypl"}'
# {"ticker":"PYPL","price":233.47}
curl -s -XPOST localhost:8000/api/watchlist -H 'content-type: application/json' -d '{"ticker":"1bad"}'
# {"detail":"Invalid ticker symbol: '1bad'"}
curl -s -XDELETE localhost:8000/api/watchlist/PYPL
# {"ticker":"PYPL","still_tracked":false}
```

### 11.3 Consumer patterns

**Trade execution** (needs a price; `MARKET_INTERFACE.md` §5):

```python
source = request.app.state.market_source
cache = request.app.state.price_cache
ticker = normalize_ticker(body.ticker)
if ticker not in cache and not await source.add_ticker(ticker):
    raise HTTPException(400, f"No price available for {ticker}")
price = cache.get_price(ticker)
# ... validate cash/shares, write trade + position ...
await sync_tracked_tickers(source, watchlist_tickers(), position_tickers())
```

**Portfolio valuation** (fall back to cost basis when there is no price, so totals never crash):

```python
def position_value(qty: float, avg_cost: float, ticker: str, cache: PriceCache) -> float:
    price = cache.get_price(ticker)
    return qty * (price if price is not None else avg_cost)
```

**LLM context** (one compact line per ticker):

```python
def price_context(cache: PriceCache) -> str:
    rows = []
    for t, u in sorted(cache.get_all().items()):
        day = f"{u.day_change_percent:+.2f}%" if u.day_change_percent is not None else "n/a"
        rows.append(f"{t}: ${u.price:.2f} (day {day})")
    return "\n".join(rows)
```

---

## 12. Configuration

| Variable | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | empty | Non-empty → Massive; otherwise simulator |
| `MASSIVE_POLL_INTERVAL` | `15` | Seconds between Massive polls (min 1). Suggest `5` on paid plans. **New**; add to `.env.example` |

Simulator settings (`update_interval=0.5`, `event_probability=0.001`) are constructor arguments, not env vars. Nobody needs to change them at runtime, and tests set them directly.

---

## 13. Failure Modes

| Situation | Behavior | User sees |
|---|---|---|
| Simulator step raises | Logged with traceback; next tick tries again | One missed tick |
| Massive 401/403 (bad key, free plan) | ERROR once, then DEBUG; retried each interval | No prices; `/api/health` says `massive` |
| Massive 429 / 5xx | SDK retries 3×; otherwise logged; next interval | Prices pause |
| Network down / timeout (10 s) | Logged; next interval | Prices pause, last values kept |
| Unknown ticker (Massive) | `add_ticker` → `False`; route returns 404 and rolls back | "No market data for XYZ" |
| Malformed ticker | `normalize_ticker` raises `ValueError` → 422 | Validation message |
| Ticker removed mid-poll | Write skipped | Nothing |
| Market closed (Massive) | Unchanged snapshots skipped; heartbeats keep SSE open | Static prices, still "connected" |
| SSE client disconnects | Generator exits on `is_disconnected()` or cancellation | — |
| App restart (simulator) | Prices restart from seed values | P&L can jump (documented limitation) |

---

## 14. Changes from the Current Code

Apply these to `backend/app/market/` to reach this design. Each row can be its own commit.

| # | File | Change | Why |
|---|---|---|---|
| 1 | `massive_client.py` | Add `extract_price()` / `extract_prev_close()`; stop reading `last_trade.timestamp` | **Bug:** attribute does not exist; no real prices ever reached the cache |
| 2 | `massive_client.py` | `add_ticker` polls the new ticker immediately and returns `bool` | Trades right after a watchlist add; invalid-ticker detection |
| 3 | `massive_client.py` | Skip writes for tickers removed mid-request; pass a copy of the ticker list to the thread | Race fix |
| 4 | `massive_client.py` | Skip unchanged snapshots; log repeated errors once | Quiet SSE and logs when closed or misconfigured |
| 5 | `models.py` | Add `normalize_ticker()`, `prev_close`, `day_change_percent`; extend `to_dict()` | Consistent symbols; daily change % |
| 6 | `cache.py` | `prev_close` argument (sticky); `remove()` bumps version; `timestamp is not None` check; read `version` under the lock | Removal visible to SSE; correctness |
| 7 | `interface.py` | `name` attribute; `add_ticker` returns `bool`; documented normalization | Health endpoint; invalid tickers |
| 8 | `simulator.py` | `SimulatorDataSource` normalizes tickers, dedupes in `start()`, seeds `prev_close`, returns `bool` | Matches the contract |
| 9 | `stream.py` | Router created per call; heartbeat; `id:` lines; public `generate_events` | Duplicate-route trap; idle proxies; testability |
| 10 | `factory.py` | `MASSIVE_POLL_INTERVAL` env var | Faster polling on paid plans |
| 11 | `tracking.py` (new) | `sync_tracked_tickers()` | One place for the watchlist ∪ positions rule |
| 12 | `tests/market/test_massive.py` | Rewrite with `TickerSnapshot.from_dict` | `MagicMock` hid bug #1 |
| 13 | `tests/market/` | Add `test_contract.py`, `test_stream.py` | Same contract for both sources; SSE behavior |

**Compatibility:** the public API stays backward-compatible for callers. `to_dict()` only gains keys, and `add_ticker`'s new return value can be ignored. The only break is in private test hooks (`_poll_once` now takes an optional ticker list), which is why the Massive tests get rewritten.

---

## 15. Testing

The existing simulator, cache and model tests pass unchanged against this design. New and rewritten tests:

### 15.1 Massive tests use real SDK models

`MagicMock` accepts any attribute, which is exactly how bug #1 went unnoticed. Fixtures build `TickerSnapshot` from the JSON the API really returns:

```python
"""MassiveDataSource tests using real SDK models (never bare MagicMock snapshots)."""

from unittest.mock import MagicMock, patch

from massive.exceptions import BadResponse
from massive.rest.models import TickerSnapshot

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource, extract_price

NS = 1_000_000_000


def snap(ticker, *, last=None, minute=None, day=None, prev=None, updated_s=1_730_736_012):
    """Build a TickerSnapshot from the JSON shape the API really returns."""
    d = {"ticker": ticker, "updated": updated_s * NS}
    if last is not None:
        d["lastTrade"] = {"p": last, "t": updated_s * NS}
    if minute is not None:
        d["min"] = {"c": minute, "t": updated_s * 1000}
    if day is not None:
        d["day"] = {"c": day}
    if prev is not None:
        d["prevDay"] = {"c": prev}
    return TickerSnapshot.from_dict(d)


def make_source(tickers):
    cache = PriceCache()
    source = MassiveDataSource("test-key", cache, poll_interval=60)
    source._client = MagicMock()
    source._tickers = list(tickers)
    return source, cache


class TestExtractPrice:
    def test_last_trade_preferred(self):
        assert extract_price(snap("AAPL", last=190.42, minute=190.3, day=190.0)) == (
            190.42,
            1_730_736_012.0,
        )

    def test_starter_plan_without_last_trade_uses_minute_bar(self):
        assert extract_price(snap("AAPL", minute=190.3, day=190.0))[0] == 190.3

    def test_before_open_falls_back_to_prev_close(self):
        assert extract_price(snap("AAPL", day=0, prev=188.0))[0] == 188.0

    def test_no_price_at_all(self):
        assert extract_price(TickerSnapshot.from_dict({"ticker": "ZZZZ"})) is None

    def test_timestamp_is_seconds_not_nanoseconds(self):
        _, ts = extract_price(snap("AAPL", last=1.0))
        assert 1e9 < ts < 1e10
```

Polling tests (excerpt):

```python
    async def test_unchanged_snapshot_does_not_bump_version(self):
        source, cache = make_source(["AAPL"])
        s = snap("AAPL", last=190.5)
        with patch.object(source, "_fetch_snapshots", return_value=[s]):
            await source._poll_once()
            v = cache.version
            assert await source._poll_once() == 0
        assert cache.version == v

    async def test_ticker_removed_during_request_is_not_rewritten(self):
        source, cache = make_source(["AAPL", "TSLA"])

        def fetch(tickers):
            source._tickers.remove("TSLA")  # simulate remove_ticker() mid-flight
            return [snap("AAPL", last=1.0), snap("TSLA", last=2.0)]

        with patch.object(source, "_fetch_snapshots", side_effect=fetch):
            await source._poll_once()
        assert "AAPL" in cache and "TSLA" not in cache
```

### 15.2 Contract tests run against both sources

```python
"""Contract tests every MarketDataSource must pass."""

from unittest.mock import patch

import pytest
from massive.rest.models import TickerSnapshot

from app.market import PriceCache, sync_tracked_tickers
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


def fake_snapshots(tickers):
    return [
        TickerSnapshot.from_dict({"ticker": t, "lastTrade": {"p": 100.0, "t": 1_730_736_012 * 10**9}})
        for t in tickers
    ]


@pytest.fixture(params=["simulator", "massive"])
async def source_and_cache(request):
    cache = PriceCache()
    if request.param == "simulator":
        source = SimulatorDataSource(cache, update_interval=60, event_probability=0)
        yield source, cache
        await source.stop()
    else:
        source = MassiveDataSource("test-key", cache, poll_interval=60)
        with patch.object(MassiveDataSource, "_fetch_snapshots", side_effect=lambda t: fake_snapshots(t)):
            yield source, cache
            await source.stop()


async def test_start_seeds_cache(source_and_cache):
    source, cache = source_and_cache
    await source.start(["aapl", "MSFT", "AAPL"])
    assert source.get_tickers() == ["AAPL", "MSFT"]
    assert cache.get("AAPL") is not None and cache.get("MSFT") is not None


async def test_add_and_remove(source_and_cache):
    source, cache = source_and_cache
    await source.start(["AAPL"])
    assert await source.add_ticker(" pypl ") is True
    assert "PYPL" in source.get_tickers() and "PYPL" in cache
    await source.remove_ticker("PYPL")
    assert "PYPL" not in source.get_tickers() and "PYPL" not in cache


async def test_invalid_ticker_rejected(source_and_cache):
    source, _ = source_and_cache
    await source.start([])
    with pytest.raises(ValueError):
        await source.add_ticker("AAPL; DROP TABLE")


async def test_sync_keeps_held_ticker(source_and_cache):
    source, cache = source_and_cache
    await source.start(["AAPL", "TSLA"])
    # TSLA removed from watchlist but still held -> stays tracked and priced
    await sync_tracked_tickers(source, watchlist=["AAPL"], positions=["TSLA"])
    assert set(source.get_tickers()) == {"AAPL", "TSLA"}
    # TSLA fully sold -> untracked
    await sync_tracked_tickers(source, watchlist=["AAPL"], positions=[])
    assert source.get_tickers() == ["AAPL"] and "TSLA" not in cache
```

### 15.3 SSE generator

```python
"""SSE generator tests."""

import json

from app.market import PriceCache
from app.market.stream import generate_events


class FakeRequest:
    client = None

    def __init__(self, polls_before_disconnect):
        self._left = polls_before_disconnect

    async def is_disconnected(self):
        self._left -= 1
        return self._left < 0


async def collect(cache, polls, **kw):
    return [f async for f in generate_events(cache, FakeRequest(polls), check_interval=0, **kw)]


async def test_sends_snapshot_then_nothing_until_change():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    frames = await collect(cache, polls=3, heartbeat_interval=999)
    assert frames[0] == "retry: 1000\n\n"
    data = [f for f in frames if "data:" in f]
    assert len(data) == 1
    body = json.loads(data[0].split("data: ", 1)[1])
    assert body["AAPL"]["price"] == 190.0


async def test_removal_triggers_push():
    cache = PriceCache()
    cache.update("AAPL", 1.0)
    v = cache.version
    cache.remove("AAPL")
    assert cache.version == v + 1


async def test_heartbeat_when_idle():
    frames = await collect(PriceCache(), polls=3, heartbeat_interval=0)
    assert ": ping\n\n" in frames
```

### 15.4 Running

```bash
cd backend && uv sync --extra dev && uv run pytest -q
```

Result for this design: `83 passed`. The live terminal demo (`uv run market_data_demo.py`) is unaffected.

---

## 16. Answers to PLAN.md Review Notes

| Note | Answer in this design |
|---|---|
| 3. SSE section says "regular cadence" | Pushes on `PriceCache.version` change, checked every 0.5 s, plus 15 s heartbeats (Section 10). PLAN §6 should be updated to match. |
| 5. LLM proposes an invalid ticker | `normalize_ticker()` rejects malformed symbols. `add_ticker()` returns `False` for symbols the source cannot price. The chat code reports both back to the LLM like a failed trade (Sections 6, 11.3). |
| (§10) daily change % | `prev_close` / `day_change_percent` on every update (Section 4). |
