# Market Data Backend — Code Review

**Date:** 2026-11-06
**Scope:** `backend/app/market/` (8 modules), `backend/tests/` (73 tests), checked against `PLAN.md`, `MARKET_DATA_SUMMARY.md`, `MARKET_INTERFACE.md`, `MARKET_SIMULATOR.md`, `MASSIVE_API.md` and `MARKET_DATA_DESIGN.md`.
**Method:** I read every source and test file, ran the suite with coverage and lint, then reproduced each suspected bug with a script that uses the real code and the real `massive` SDK models (no mocks for the parts under test).

> **Update 2026-11-06:** P1-1, P1-2 and P2-2 are **fixed** (each with a test that failed before the fix). Suite: 81 passed, 92% coverage, lint clean. The other findings are still open.

---

## 1. Verdict

The **simulator path is solid and ready** for downstream work. The code is clean, short and well documented, and the GBM math and correlation model are correct.

The **Massive (real data) path does not work**: it never writes a price to the cache. The SSE router also has a structural trap. `MARKET_DATA_SUMMARY.md` says "all issues resolved", but none of the 13 changes listed in `MARKET_DATA_DESIGN.md` Section 14 have been applied to the code yet (`tracking.py` doesn't exist, and `massive_client.py` still reads `last_trade.timestamp`).

| Area | Status |
|---|---|
| Models, cache, factory | Good; two small cache fixes needed |
| GBM simulator | Good |
| Massive client | ~~**Broken** (P1)~~ Fixed; race condition still open |
| SSE stream | ~~Duplicate-route trap~~ Fixed; generator still untested |
| Tests | All pass; Massive tests now use real SDK models; SSE generator has no tests |

---

## 2. Test Results

```
uv run --extra dev pytest --cov=app
73 passed, 73 warnings in 1.08s

app/market/cache.py           100%
app/market/factory.py         100%
app/market/interface.py       100%
app/market/massive_client.py   94%
app/market/models.py          100%
app/market/simulator.py        98%
app/market/stream.py           33%   <- SSE generator and route untested
TOTAL                          91%

uv run --extra dev ruff check app/ tests/
All checks passed!
```

- **Warnings:** all 73 come from `tests/conftest.py`: `asyncio.DefaultEventLoopPolicy` is deprecated (removal in Python 3.16). The fixture isn't needed with `asyncio_mode = "auto"`, so it can simply be deleted.
- **Environment note:** the tests ran on Python 3.14.4 locally, but the Dockerfile in PLAN.md targets 3.12. That's fine (`requires-python >=3.12`), but CI should test 3.12 too.
- The summary doc says 84% coverage and 56% for `massive_client.py`; the current numbers above are higher. Update the summary.

---

## 3. Findings

Priority: **P1** = broken or blocks a feature, **P2** = real bug, limited impact, **P3** = cleanup.

### P1-1. Massive source never produces a price — FIXED

> **Fixed:** added `extract_price()` (last trade via `sip_timestamp` in ns, then minute/day/previous-day close). `test_massive.py` now builds `TickerSnapshot.from_dict(...)` fixtures, plus 4 new fallback tests. The repro now caches `190.42`.

`massive_client.py:103` reads `snap.last_trade.timestamp`. The real SDK `LastTrade` model has no `timestamp` attribute (it has `sip_timestamp`, in **nanoseconds**, not milliseconds).

**Evidence** (real `TickerSnapshot.from_dict`, fed through `_poll_once`):

```
Skipping snapshot for AAPL: 'LastTrade' object has no attribute 'timestamp'
hasattr(last_trade,'timestamp'): False | sip_timestamp: 1730736012345678900
cache after real-model poll: None
Starter plan last_trade: None
```

- Every snapshot hits the `AttributeError` branch, so the cache stays empty. With a key set, the app shows no prices and trades fail.
- On the Starter plan `last_trade` is `None`, so even the corrected attribute name is not enough. A fallback is needed (`last_trade` -> `min.close` -> `day.close` -> `prev_day.close`).
- **Why tests pass:** `test_massive.py` builds snapshots with `MagicMock`, which accepts any attribute name. The test `test_timestamp_conversion` "proves" the wrong unit (ms).

**Fix:** apply `MARKET_DATA_DESIGN.md` §14 rows 1 and 12: add `extract_price()`, and rewrite the Massive tests with `TickerSnapshot.from_dict(...)`.

### P1-2. `create_stream_router` reuses one module-level router — FIXED

> **Fixed:** the router is now created inside the function. New `tests/market/test_stream.py` checks that each call returns a new router with a single route.

`stream.py:17` creates `router` at import time, and each call to `create_stream_router()` adds another `/prices` route to that **same** object.

**Evidence:**

```
same router object: True | routes: ['/api/stream/prices', '/api/stream/prices']
```

FastAPI matches the first route, so a second app (for example, a second `TestClient` app in the API tests) silently streams from the **first** app's cache. This will cause confusing test failures once the API layer is built. **Fix:** create the `APIRouter` inside the function (design §14 row 9).

### P2-1. Massive: a ticker removed during a poll comes back as a stale "zombie"

`_poll_once` awaits the network call in a thread. If `remove_ticker()` runs during that await, the results loop then writes the removed ticker back into the cache. The poller no longer tracks it, so it stays in the cache **forever** with a frozen price, and it shows in the SSE stream and the LLM context.

**Evidence:**

```
GOOGL removed mid-poll -> tracked: False | still in cache: 175.0
```

A related issue: `self._tickers` is passed by reference to the worker thread while `add_ticker` appends to it in place. **Fix:** pass a copy to the thread; skip results for tickers no longer tracked (design §14 row 3). The simulator does not have this problem, because its step and its cache writes happen with no `await` in between.

### P2-2. `PriceCache.remove()` does not bump `version` — FIXED

> **Fixed:** `remove()` bumps `version` when the ticker was present (no bump for an unknown ticker). Two new tests in `test_cache.py`.

**Evidence:** `version bumps on remove: False`.

The SSE loop only sends when `version` changes. With the simulator this is hidden (the next tick bumps it 0.5 s later). With Massive, a removed ticker stays on the user's screen for up to 15 s. **Fix:** increment `_version` in `remove()` (one line).

### P2-3. The simulator doesn't normalize ticker case, and Massive does

**Evidence:**

```
sim add 'aapl' after start -> ['AAPL', 'aapl']
```

`MassiveDataSource.add_ticker` uppercases and strips the ticker, but `SimulatorDataSource` doesn't, so the same call behaves differently depending on the source. In simulator mode, `aapl` becomes a second, independent ticker with a random price. The fix belongs in one shared `normalize_ticker()` (design §14 rows 5 and 8). The API layer should also normalize, but the market layer shouldn't depend on that.

### P2-4. `SimulatorDataSource.add_ticker` before `start()` is silently dropped

**Evidence:** `sim add before start -> []`. Massive accepts it. This is the same contract with two behaviors. It's low risk if the lifespan always calls `start()` first, but contract tests (design §14 row 13) should pin one behavior.

### P2-5. Massive has no way to tell the caller a ticker is invalid

`add_ticker` returns `None`, and an unknown symbol just never gets a price. In simulator mode, any string gets a random price. This affects PLAN.md review note 5 (invalid tickers from the LLM): the watchlist/trade routes need a validation rule. The design proposes `add_ticker -> bool` with an immediate poll (§14 rows 2, 7). Decide this before building the watchlist API.

### P3 — Cleanups

| # | Location | Issue |
|---|---|---|
| 1 | `stream.py:86` | `except asyncio.CancelledError` swallows the cancellation instead of re-raising. Log, then `raise`. |
| 2 | `stream.py` | No heartbeat. With Massive (15 s gaps) or a closed market, an idle connection can be dropped by proxies. Send a `: ping` comment about every 15 s. |
| 3 | `stream.py:30` | Docstring says "streams every ~500ms"; it actually pushes only on version change (MARKET_INTERFACE §7.4). |
| 4 | `cache.py:30` | `timestamp or time.time()` treats `0` as missing. Use `is not None`. |
| 5 | `cache.py:66` | `version` is read without the lock. Safe under the GIL today; taking the lock costs nothing and matches the rest of the class. |
| 6 | `models.py` | `change` / `change_percent` are tick-to-tick. The watchlist needs **daily** change % (PLAN §10). There's no `prev_close` / session-open field yet (design §14 row 5). |
| 7 | `massive_client.py:118` | A bad key or free plan (403) logs an ERROR every 15 s forever. Log once, then DEBUG (design §14 row 4). |
| 8 | `pyproject.toml` | `rich` is a core dependency, but only `market_data_demo.py` uses it. Move it to the `dev` extra to keep the Docker image lean. |
| 9 | `tests/conftest.py` | Deprecated `event_loop_policy` fixture; delete it (removes all 73 warnings). |
| 10 | `simulator.py` | Uses both `random` and `np.random` global state, so seeding a test needs two calls. Acceptable; worth a note in test helpers. |
| 11 | `tests/test_simulator.py` | No statistical tests of the math (mean log-return, realized correlation), though `MARKET_SIMULATOR.md` §8 lists them as the testing approach. |

---

## 4. What Is Done Well

- **Clear separation:** `GBMSimulator` is pure, synchronous math; `SimulatorDataSource` is a thin async adapter. This makes the simulator easy to test and reason about.
- **Correct GBM:** the Itô correction (`mu - sigma²/2`), full-precision internal prices with rounding only on output, and a sensible `dt` for real-time ticks.
- **Sound correlation model:** the sector-based matrix is positive definite for these rules, so Cholesky can't fail.
- **Single source of truth:** the producer -> `PriceCache` -> consumers design matches PLAN.md §6 and supports the "watchlist ∪ positions" rule cleanly.
- **Resilience:** one bad tick or failed poll is logged and never kills the background task.
- **Sync SDK handled correctly:** the Massive client runs in `asyncio.to_thread`, so it doesn't block the event loop.
- **Small modules, good docstrings,** lint-clean, and consistent naming throughout.

---

## 5. Documentation Accuracy

| Doc | Issue |
|---|---|
| `MARKET_DATA_SUMMARY.md` | Says "complete... all issues resolved" and "73 tests, 84% coverage". The Massive bug is open, design §14 isn't applied yet, and coverage is now 91%. Change the status to reflect this. |
| `MARKET_DATA_DESIGN.md` | Accurate and thorough; its §14 matches this review's findings. It is the target state, not the current state; its header says so, but the summary doc does not. |
| `MARKET_INTERFACE.md` §3 | Shows `app.include_router(create_stream_router(price_cache))` at module level. That's fine once, but combined with P1-2 it breaks multi-app tests. |
| `PLAN.md` §6 | Still says SSE pushes "at a regular cadence (~500ms)" (PLAN review note 3). |

---

## 6. Recommended Next Steps

Ordered so each step is small and can be checked on its own:

1. ~~**Massive fix:** add `extract_price()` and rewrite `test_massive.py` with real SDK models.~~ Done.
2. ~~**Router fix:** move `APIRouter(...)` inside `create_stream_router`.~~ Done. Still to do: extend `test_stream.py` to cover the generator (first event, no resend on unchanged version, exit on disconnect).
3. ~~**Cache:** `remove()` bumps version.~~ Done. Still to do: the `is not None` timestamp check (P3-4).
4. **Massive race:** copy the ticker list for the thread, and skip removed tickers.
5. **Ticker normalization** shared by both sources, plus contract tests run against both.
6. **Decide** on invalid-ticker handling (`add_ticker -> bool`) before building the watchlist API.
7. Cleanups (P3), then update `MARKET_DATA_SUMMARY.md`.

Steps 1-3 matter most: they fix the broken feature and the trap that will hit the API layer next. Steps 4-7 can follow as the watchlist and portfolio routes are built.
