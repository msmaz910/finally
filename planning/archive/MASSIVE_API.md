# Massive API Reference (formerly Polygon.io)

Research notes for retrieving real-time and end-of-day (EOD) US stock prices for multiple tickers. Verified on 2026-11-04 against the official docs and the installed `massive` Python SDK (v2.2.0).

Polygon.io rebranded as **Massive** in early 2026. Old Polygon keys, accounts, and endpoints keep working; the API host is now `https://api.massive.com`.

---

## 1. Setup

```bash
uv add massive          # already a dependency in backend/pyproject.toml
```

```python
from massive import RESTClient

client = RESTClient()                     # reads MASSIVE_API_KEY from the environment
client = RESTClient(api_key="your-key")   # or pass it explicitly
```

Useful `RESTClient` defaults (from SDK source):

| Option | Default | Notes |
|---|---|---|
| `api_key` | `os.getenv("MASSIVE_API_KEY")` | Raises `AuthError` if missing |
| `base` | `https://api.massive.com` | |
| `connect_timeout` / `read_timeout` | `10.0` s | |
| `retries` | `3` | Auto-retries 429, 5xx (urllib3, backoff 0.1) |
| `pagination` | `True` | `list_*` methods auto-follow `next_url` |

The client is **synchronous** (urllib3). In an async app, call it via `asyncio.to_thread(...)`.

**Errors:** any non-200 response raises `massive.exceptions.BadResponse` (message = response body). A missing key raises `AuthError`.

Raw HTTP equivalent (any endpoint): append `?apiKey=KEY` or send header `Authorization: Bearer KEY`.

---

## 2. Plans and Rate Limits

This decides which endpoints FinAlly can actually use.

| Plan | Price | Rate limit | Stock data recency | Snapshot endpoints | Last trade |
|---|---|---|---|---|---|
| **Basic (free)** | $0 | **5 calls/min** | **End-of-day only** | **Not included** | Not included |
| Starter | $29/mo | Unlimited | 15-min delayed | Included | Not included |
| Developer | $79/mo | Unlimited | 15-min delayed | Included | Included |
| Advanced | $199/mo | Unlimited | Real-time | Included | Included |

Key consequences:

- **A free key cannot call the snapshot endpoints.** Free users only get EOD endpoints (previous close, grouped daily, daily open/close, historical bars).
- On the free plan, a trading day's bars become available after midnight ET. Requesting the current day earlier returns HTTP 403.
- Exceeding the rate limit returns HTTP 429.
- Snapshot fields `lastTrade` / `lastQuote` are only populated on plans that include trades/quotes (Developer and up). On Starter they are absent and the SDK sets them to `None`.

---

## 3. Endpoints for Multiple Tickers

### 3.1 Full Market Snapshot (best for live polling)

`GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,...`

One call returns the latest state for every requested ticker. No limit on the number of tickers. Plans: Starter and up.

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient()
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "MSFT", "NVDA"],
)

for snap in snapshots:
    last = snap.last_trade.price if snap.last_trade else None
    print(snap.ticker, last, snap.day.close, snap.prev_day.close, snap.todays_change_percent)
```

Response (abridged) and the SDK attribute each key maps to:

```json
{
  "status": "OK",
  "count": 1,
  "tickers": [{
    "ticker": "AAPL",
    "day":     {"o": 189.1, "h": 191.0, "l": 188.7, "c": 190.4, "v": 41200000, "vw": 190.1},
    "prevDay": {"o": 187.5, "h": 188.9, "l": 186.8, "c": 188.0, "v": 52000000, "vw": 187.9},
    "min":     {"o": 190.3, "h": 190.5, "l": 190.2, "c": 190.4, "v": 51000, "t": 1730736000000},
    "lastTrade": {"p": 190.42, "s": 100, "t": 1730736012345678900, "x": 4},
    "lastQuote": {"p": 190.41, "s": 2, "P": 190.43, "S": 3, "t": 1730736012345000000},
    "todaysChange": 2.42,
    "todaysChangePerc": 1.287,
    "updated": 1730736012345678900
  }]
}
```

| JSON key | SDK attribute (`TickerSnapshot`) | Notes |
|---|---|---|
| `ticker` | `ticker` | |
| `day` | `day` (`Agg`: `open/high/low/close/volume/vwap`) | Current session bar |
| `prevDay` | `prev_day` (`Agg`) | Previous session bar |
| `min` | `min` (`MinuteSnapshot`) | Latest minute bar |
| `lastTrade.p` | `last_trade.price` | `None` on Starter |
| `lastTrade.t` | `last_trade.sip_timestamp` | **Nanoseconds**. There is no `.timestamp` attribute |
| `todaysChange` | `todays_change` | vs. previous close |
| `todaysChangePerc` | `todays_change_percent` | |
| `updated` | `updated` | **Nanoseconds** |

Timestamp conversion: `seconds = nanoseconds / 1_000_000_000`.

### 3.2 Unified Snapshot (`/v3/snapshot`)

Alternative snapshot for mixed asset classes. Max **250 tickers** per call, paginated. Plans: Starter and up. Field names are already snake_case (`last_trade.price`, `session.close`, `session.previous_close`, `session.change_percent`).

```python
for s in client.list_universal_snapshots(ticker_any_of=["AAPL", "MSFT"], limit=250):
    print(s.ticker, s.session.close, s.session.change_percent)
```

Not needed for FinAlly (stocks only); 3.1 is simpler.

### 3.3 Single Ticker Snapshot

`GET /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}` returns one `TickerSnapshot`. Same plans as 3.1. Useful to validate a single ticker.

```python
snap = client.get_snapshot_ticker(SnapshotMarketType.STOCKS, "AAPL")
```

### 3.4 Grouped Daily Bars (best for EOD, works on free plan)

`GET /v2/aggs/grouped/locale/us/market/stocks/{date}`

**One call returns the daily OHLCV bar for every US stock** on a date. Filter locally to the tickers you want. Available on all plans, including Basic.

```python
bars = client.get_grouped_daily_aggs(date="2026-11-03", adjusted=True)
wanted = {"AAPL", "MSFT", "NVDA"}
closes = {b.ticker: b.close for b in bars if b.ticker in wanted}
```

Response: `results[]` with `T` (ticker), `o`, `h`, `l`, `c`, `v`, `vw`, `n`, `t` (Unix **milliseconds**, end of window). SDK model: `GroupedDailyAgg` with `ticker`, `open`, `high`, `low`, `close`, `volume`, `vwap`, `timestamp`.

Dates with no trading (weekends, holidays) return an empty list, so step back one day at a time until results appear.

### 3.5 Previous Day Bar (per ticker, works on free plan)

`GET /v2/aggs/ticker/{ticker}/prev` returns the previous session's OHLCV. One call per ticker, so 10 tickers cost 10 calls (2 minutes on the free plan). Prefer 3.4 for multiple tickers.

```python
prev = client.get_previous_close_agg("AAPL")
print(prev[0].close)   # SDK returns a list with one PreviousCloseAgg
```

### 3.6 Daily Open/Close (per ticker, per date)

`GET /v1/open-close/{ticker}/{date}` returns open, close, high, low, pre-market and after-hours prices for one ticker on one date.

```python
d = client.get_daily_open_close_agg("AAPL", "2026-11-03")
print(d.open, d.close, d.pre_market, d.after_hours)
```

### 3.7 Historical Bars (per ticker)

`GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}`. Useful later for seeding charts with history.

```python
for bar in client.list_aggs("AAPL", 1, "day", "2026-10-01", "2026-11-03", limit=50000):
    print(bar.timestamp, bar.close)   # timestamp in milliseconds
```

### 3.8 Last Trade (per ticker)

`GET /v2/last/trade/{ticker}`. Developer plan and up. One call per ticker, so the snapshot endpoint is better for multiple tickers.

```python
t = client.get_last_trade("AAPL")
print(t.price, t.sip_timestamp)   # nanoseconds
```

---

## 4. Which Endpoint for Which Job

| Need | Endpoint | Calls for 10 tickers | Free plan? |
|---|---|---|---|
| Live-ish prices for watchlist | Full Market Snapshot (3.1) | 1 | No |
| Latest EOD closes | Grouped Daily (3.4) | 1 | Yes |
| Previous close for one ticker | Previous Day Bar (3.5) | 10 | Yes |
| Price history for a chart | Historical Bars (3.7) | 10 | Yes (2 yrs) |

---

## 5. Complete Example: Robust Price Extraction

Snapshot fields vary by plan and time of day (`last_trade` is `None` on Starter; `day` can be empty before the open). Fall back through the available fields:

```python
import asyncio
from massive import RESTClient
from massive.rest.models import SnapshotMarketType


def snapshot_price(snap) -> tuple[float, float] | None:
    """Return (price, unix_seconds) from a TickerSnapshot, or None if no price is available."""
    ts = (snap.updated or 0) / 1e9
    if snap.last_trade and snap.last_trade.price:
        return snap.last_trade.price, snap.last_trade.sip_timestamp / 1e9
    for bar in (snap.min, snap.day, snap.prev_day):
        if bar and bar.close:
            return bar.close, ts
    return None


async def fetch_prices(client: RESTClient, tickers: list[str]) -> dict[str, float]:
    """Fetch the latest prices for many tickers in one API call."""
    snaps = await asyncio.to_thread(
        client.get_snapshot_all, SnapshotMarketType.STOCKS, tickers
    )
    return {s.ticker: p[0] for s in snaps if (p := snapshot_price(s))}
```

EOD example for a free key:

```python
from datetime import date, timedelta


def latest_closes(client: RESTClient, tickers: set[str]) -> dict[str, float]:
    """Return the most recent available daily close for each ticker (1-5 API calls)."""
    day = date.today() - timedelta(days=1)
    for _ in range(5):
        bars = client.get_grouped_daily_aggs(date=day.isoformat())
        if bars:
            return {b.ticker: b.close for b in bars if b.ticker in tickers}
        day -= timedelta(days=1)
    return {}
```

---

## 6. Gotchas

1. **Free plan has no snapshot access.** `get_snapshot_all` raises `BadResponse` (403 / NOT_AUTHORIZED) on a Basic key.
2. **Nanosecond timestamps** on snapshots and trades (`updated`, `sip_timestamp`); **millisecond** timestamps on aggregate bars (`t`).
3. **No `.timestamp` on `LastTrade`.** Use `.sip_timestamp`. A `MagicMock` in tests will hide this mistake, because mocks accept any attribute.
4. **Tickers are case-sensitive.** Always send uppercase.
5. Unknown tickers are simply missing from snapshot results (no error), so a missing ticker means "invalid or no data".
6. The SDK is synchronous. Wrap calls in `asyncio.to_thread` inside FastAPI.

---

## Sources

- [Full Market Snapshot](https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot.md)
- [Unified Snapshot](https://massive.com/docs/rest/stocks/snapshots/unified-snapshot.md)
- [Daily Market Summary (Grouped Daily)](https://massive.com/docs/rest/stocks/aggregates/daily-market-summary.md)
- [Previous Day Bar](https://massive.com/docs/rest/stocks/aggregates/previous-day-bar.md)
- [Last Trade](https://massive.com/docs/rest/stocks/trades-quotes/last-trade.md)
- [Docs index (llms.txt)](https://massive.com/docs/llms.txt)
- [Snapshot ticker limits (KB)](https://massive.com/knowledge-base/article/what-is-the-max-number-of-tickers-i-can-pass-through-massives-snapshot)
- [Free plan previous-day timing (KB)](https://massive.com/knowledge-base/article/on-the-free-basic-plan-when-can-i-pull-the-previous-trading-days-data)
- [Massive vs Alpha Vantage (plan limits)](https://massive.com/compare/massive-vs-alpha-vantage.md)
- [massive-com/client-python](https://github.com/massive-com/client-python) (SDK source: `rest/__init__.py`, `rest/base.py`, `rest/snapshot.py`, `rest/aggs.py`, `rest/models/`)
