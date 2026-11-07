# Market Simulator: Approach and Code Structure

The default market data source. It generates realistic, correlated, live-moving stock prices with no API key and no network. It is **already implemented** in `backend/app/market/simulator.py` and `seed_prices.py`; this document explains how and why it works. Numbers below were measured by running the actual code on 2026-11-04.

---

## 1. Goals

| Requirement (PLAN.md Section 6) | How it is met |
|---|---|
| Realistic price paths | Geometric Brownian Motion (GBM) |
| ~500 ms updates | Background asyncio task, `update_interval=0.5` |
| Tech stocks move together | Correlated random draws via Cholesky decomposition |
| Occasional drama | Random 2-5% shock events |
| Realistic starting prices | `SEED_PRICES` table |
| Same interface as Massive | `SimulatorDataSource(MarketDataSource)` |

---

## 2. The Math: Geometric Brownian Motion

GBM is the standard textbook model for stock prices (it underlies Black-Scholes). Each step multiplies the price by a random factor:

```
S(t+dt) = S(t) * exp( (mu - sigma²/2) * dt  +  sigma * sqrt(dt) * Z )
                      └──── drift ────┘        └── random shock ──┘
```

| Symbol | Meaning | Example |
|---|---|---|
| `S(t)` | Current price | 190.00 |
| `mu` | Annual drift (expected return) | 0.05 = 5%/yr |
| `sigma` | Annual volatility | 0.22 = 22%/yr |
| `dt` | Step size as a fraction of a trading year | 8.48e-8 |
| `Z` | Standard normal random number (correlated across tickers) | |

Why GBM fits:

- Prices **can never go negative** (we multiply by `exp(...)`, which is always positive).
- Moves are **proportional**: a $800 stock moves more dollars than a $190 stock at the same volatility.
- The `- sigma²/2` term corrects the drift so the *average* growth rate equals `mu`.

### Time step

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600   # 5,896,800
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR    # ≈ 8.48e-8
```

Each 500 ms tick represents 500 ms of real trading time, so volatility looks realistic in real time:

| Ticker | sigma | Per-tick std. dev. | Per-hour std. dev. |
|---|---|---|---|
| V | 0.17 | 0.0050% (~$0.014) | 0.42% |
| AAPL | 0.22 | 0.0064% (~$0.012) | 0.54% |
| NVDA | 0.40 | 0.0116% (~$0.093) | 0.99% |
| TSLA | 0.50 | 0.0146% (~$0.036) | 1.24% |

Prices are rounded to cents for display, so some ticks show no change: measured 5% (NVDA) to 36% (JPM) of ticks are "flat". That is realistic, and it means price flashes are frequent but not constant.

---

## 3. Correlated Moves: Cholesky Decomposition

Real stocks do not move independently. To make tech stocks rise and fall together:

1. Build a **correlation matrix** `C` (n × n) from sector rules.
2. Compute its **Cholesky factor** `L`, so that `L @ L.T == C`.
3. Every tick: draw `n` independent normals `z`, then `z_correlated = L @ z`.

The resulting draws have exactly the correlations in `C`.

Correlation rules (`seed_prices.py`):

| Pair | Correlation |
|---|---|
| Both in tech (AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX) | 0.6 |
| Both in finance (JPM, V) | 0.5 |
| Anything with TSLA | 0.3 (it "does its own thing") |
| Cross-sector or unknown ticker | 0.3 |

Measured realized correlations over 4,000 ticks: AAPL-MSFT 0.58, JPM-V 0.45, AAPL-JPM 0.27, TSLA-AAPL 0.29, which match the targets within sampling noise. The matrix is positive definite (smallest eigenvalue 0.4), so Cholesky always succeeds for these rules.

The matrix is rebuilt whenever a ticker is added or removed. That costs O(n³), which is trivial for n < 50.

---

## 4. Random Shock Events

For visual drama, each ticker on each tick has a small chance of a sudden jump:

```python
if random.random() < event_probability:          # default 0.001
    shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
    price *= 1 + shock
```

With 10 tickers at 2 ticks/second, that is 0.02 events/second, so **about one event every 50 seconds** somewhere on the watchlist. Set `event_probability=0` for deterministic tests.

---

## 5. Seed Data and Parameters (`seed_prices.py`)

| Ticker | Seed price | sigma | mu | Character |
|---|---|---|---|---|
| AAPL | 190 | 0.22 | 0.05 | |
| GOOGL | 175 | 0.25 | 0.05 | |
| MSFT | 420 | 0.20 | 0.05 | |
| AMZN | 185 | 0.28 | 0.05 | |
| TSLA | 250 | 0.50 | 0.03 | Very volatile, low correlation |
| NVDA | 800 | 0.40 | 0.08 | Volatile, strong drift |
| META | 500 | 0.30 | 0.05 | |
| JPM | 195 | 0.18 | 0.04 | Calm bank |
| V | 280 | 0.17 | 0.04 | Calm payments |
| NFLX | 600 | 0.35 | 0.05 | |

Tickers not in the table (e.g. `PYPL` added via chat) get `DEFAULT_PARAMS` (`sigma=0.25, mu=0.05`) and a random seed price between $50 and $300, with 0.3 correlation to everything.

Prices restart from the seed values each time the app starts. They are not persisted.

---

## 6. Code Structure

Two classes with a clear split: **pure math** vs. **async plumbing**.

```
seed_prices.py          constants only: prices, params, correlation groups
simulator.py
├── GBMSimulator        pure, synchronous, no I/O; easy to unit test
│   ├── step() -> dict[str, float]     advance all tickers one tick
│   ├── add_ticker(t) / remove_ticker(t)   rebuilds Cholesky
│   ├── get_price(t) / get_tickers()
│   ├── _add_ticker_internal(t)        batch add without rebuild
│   ├── _rebuild_cholesky()
│   └── _pairwise_correlation(t1, t2)  sector rules
└── SimulatorDataSource(MarketDataSource)   async adapter to the shared interface
    ├── start(tickers)    build GBMSimulator, seed cache, launch loop task
    ├── stop()            cancel task
    ├── add_ticker(t)     add to sim + seed cache immediately
    ├── remove_ticker(t)  remove from sim + cache
    └── _run_loop()       step → cache.update → sleep(0.5), forever
```

### Core loop

```python
async def _run_loop(self) -> None:
    """Core loop: step the simulation, write to cache, sleep."""
    while True:
        try:
            for ticker, price in self._sim.step().items():
                self._cache.update(ticker=ticker, price=price)
        except Exception:
            logger.exception("Simulator step failed")
        await asyncio.sleep(self._interval)
```

The `try/except` is deliberate: one bad tick must never kill the background task, or prices would silently freeze for every user.

### One `step()`

```python
z = self._cholesky @ np.random.standard_normal(n)       # correlated draws
for i, ticker in enumerate(self._tickers):
    mu, sigma = self._params[ticker]["mu"], self._params[ticker]["sigma"]
    drift = (mu - 0.5 * sigma**2) * self._dt
    diffusion = sigma * math.sqrt(self._dt) * z[i]
    self._prices[ticker] *= math.exp(drift + diffusion)
    # ... optional shock event ...
```

Internal prices keep full precision; only the returned values are rounded to cents, so rounding error never accumulates.

---

## 7. Usage

```python
from app.market import PriceCache
from app.market.simulator import SimulatorDataSource, GBMSimulator

# As a data source (normal app use)
cache = PriceCache()
source = SimulatorDataSource(cache)              # update_interval=0.5, event_probability=0.001
await source.start(["AAPL", "TSLA"])
await source.add_ticker("PYPL")                  # priced immediately
await source.stop()

# Pure math (tests, notebooks)
sim = GBMSimulator(["AAPL", "MSFT"], event_probability=0)
for _ in range(10):
    print(sim.step())
```

Live terminal demo: `cd backend && uv run market_data_demo.py`

---

## 8. Testing Approach

- **Determinism:** `np.random.seed(...)` + `random.seed(...)` + `event_probability=0`.
- **Math:** prices stay positive over thousands of steps; mean log-return ≈ `(mu - sigma²/2) * dt`.
- **Correlation:** realized correlation of log-returns over many steps ≈ target (within ±0.05).
- **Events:** with `event_probability=1`, every tick moves at least 2%.
- **Dynamic tickers:** add/remove rebuilds the matrix; `step()` returns exactly the current tickers.
- **Data source:** after `start()`, the cache holds every ticker before the first sleep.

Existing tests: `backend/tests/market/test_simulator.py` (17) and `test_simulator_source.py` (10).

---

## 9. Known Limitations (acceptable for a demo)

- Trades 24/7; there are no market hours or overnight gaps.
- No bid/ask spread; trades fill at the last simulated price.
- Prices reset to seed values on every restart, so held positions may show sudden P&L jumps after a restart.
- Constant volatility (no volatility clustering).
