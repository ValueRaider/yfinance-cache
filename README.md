# yfinance-cache
Caching wrapper for `yfinance` module. Intelligent caching, not dumb caching of web requests. This means only updating cache if (i) missing and (ii) new data expected.

Only price data fully implemented. Uses [exchange schedule](https://github.com/gerrymanoim/exchange_calendars) to know when new price data available. '1d' price data always fetched from `start` date to today (i.e. ignores `end`), as need to know all dividends and stock splits since `start`.

## Interface
Interaction almost identical to yfinance. Differences highlighted underneath code:

```python
import yfinance_cache as yfc

msft = yfc.Ticker("MSFT")

# get stock info
msft.info

# get historical market data
hist = msft.history(period="max")
...
# etc. See yfinance documentation for full API
```

#### Refreshing cache
```python
msft.history(interval="1d", max_age="1h", trigger_at_market_close=False, ...)
```
`max_age` controls when to update cache. If market is still open and `max_age` time has passed since last fetch, then today's cached price data will be refreshed. If `trigger_at_market_close=True` then refresh also triggered if market has closed since last fetch. Must be `Timedelta` or equivalent `str`, defaults to half of interval. 

#### Adjusting price
Price can be adjusted for stock splits, dividends, or both.
```python
msft.history(..., adjust_splits=True, adjust_divs=True)
```

#### Verifying cache
Cached prices can be compared against latest Yahoo Finance data, and correct differences:
```python
# Verify prices of one ticker symbol
msft.verify_cached_prices(
	rtol=0.0001,  # relative tolerance for differences
	vol_rtol=0.005,  # relative tolerance specifically for Volume
	correct=False,  # delete incorrect cached data?
	discard_old=False,  # if cached data too old to check (e.g. 30m), assume incorrect and delete?
	quiet=True,  # enable to print nothing, disable to print summary detail of why cached data wrong
	debug=False,  # enable even more detail for debugging 
	debug_interval=None)  # only verify this interval (note: 1d always verified)

# Verify prices of entire cache, ticker symbols processed alphabetically. Recommend using `requests_cache` session.
yfc.verify_cached_tickers_prices(
	session=None,  # recommend you provide a requests_cache here
	rtol=0.0001,
	vol_rtol=0.005,
	correct=False,
	resume_from_tkr=None,  # in case you aborted verification, can jump ahead to this ticker symbol. Append '+1' to start AFTER the ticker
	debug_tkr=None,  # only verify this ticker symbol
	debug_interval=None)
```

With latest version the only genuine differences you should see are tiny Volume differences (~0.5%). Seems Yahoo is still adjusting Volume over 24 hours after that day ended, e.g. updating Monday Volume on Wednesday.

If you see big differences in the OHLC price of recent intervals (last few days), probably Yahoo is wrong! Since fetching that price data on day / day after, Yahoo has messed up their data - at least this is my experience. Cross-check against TradingView or stock exchange website.

## Installation

Available on PIP: `pip install yfinance_cache`

## Limitations

- only price data is checked if refresh needed
- `Tickers` class and `download()` not available - use `Ticker.history()`
- intraday pre/post price data not available
