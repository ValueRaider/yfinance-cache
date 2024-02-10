# yfinance-cache

Persistent caching wrapper for `yfinance` module. Intelligent caching, not dumb caching of web requests - only update cache where missing/outdated and new data expected. Idea is to minimise fetch frequency and quantity - Yahoo API officially only cares about frequency, but I'm guessing they also care about server load from scrapers.

Cache auto-update implemented for:
- prices
- calendar
- shares
- info

Everything else cached once but never updated (unless you delete their files).
Financials auto-update will be implemented soon ...

Persistent cache stored in your user cache folder:
- Windows = C:/Users/\<USER\>/AppData/Local/py-yfinance-cache
- Linux = /home/\<USER\>/.cache/py-yfinance-cache
- MacOS = /Users/\<USER\>/Library/Caches/py-yfinance-cache

## Interface

Interaction almost identical to yfinance, listed is attributes with auto-update:

```python
import yfinance_cache as yfc

msft = yfc.Ticker("MSFT")
msft.info
msft.calendar
msft.get_shares(start='2024-01-01')
msft.history(period="1wk")
yfc.download("MSFT AMZN", period="1wk")

# See yfinance documentation for full API
```

### Price data differences

Other people have implemented price caches, but none adjust cached data for new stock splits or dividends.
YFC does. Price can be adjusted for stock splits, dividends, or both:

```python
msft.history(..., adjust_splits=True, adjust_divs=True)
```

Price repair is force-enabled, to prevent bad Yahoo data corrupting cache.
See [yfinance Wiki](https://github.com/ranaroussi/yfinance/wiki/Price-repair) for detail.

Returned table has 2 new columns:
- `FetchDate` = when data was fetched
- `Final?` = `true` if don't expect future fetches to change

## Aging

Concept of `max age` controls when cached data is updated.
If `max age` time has passed since last fetch then cache is updated.
Value must be `Timedelta` or equivalent `str`.

#### Price data aging

``` python
df = msft.history(interval="1d", max_age="1h", trigger_at_market_close=False, ...)
```

With price data, YFC also considers how long exchange been open since last fetch, 
using [exchange_calendars](https://github.com/gerrymanoim/exchange_calendars).
Only if market been open long enough since last fetch, 
or if `trigger_at_market_close=True` and market since closed, 
is cache refreshed.
`max_age` defaults to half of interval.

#### Shares aging

``` python
df = msft.shares(..., max_age='60d')
```

#### Property aging

For data obtained from `Ticker` properties not functions, max age set in YFC options.
Implemented to behave like `pandas.options`, except YFC options are persistent.

``` python
>>> import yfinance_cache as yfc
>>> yfc.options
{
    "max_ages": {
        "calendar": "7d",
        "info": "180d"
    }
}
>>> yfc.options.max_ages.calendar = '30d'
>>> yfc.options
{
    "max_ages": {
        "calendar": "30d",
        "info": "180d"
    }
}
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
	halt_on_fail=True,  # stop verifying on first fail
	resume_from_tkr=None,  # in case you aborted verification, can jump ahead to this ticker symbol. Append '+1' to start AFTER the ticker
	debug_tkr=None,  # only verify this ticker symbol
	debug_interval=None)
```

With latest version the only genuine differences you should see are tiny Volume differences (~0.5%). Seems Yahoo is still adjusting Volume over 24 hours after that day ended, e.g. updating Monday Volume on Wednesday.

If you see big differences in the OHLC price of recent intervals (last few days), probably Yahoo is wrong! Since fetching that price data on day / day after, Yahoo has messed up their data - at least this is my experience. Cross-check against TradingView or stock exchange website.

## Performance

For each ticker, YFC basically performs 2 tasks:

1 - check if fetch needed

2 - fetch data and integrate into cache

Throughput on 1 thread decent CPU: task 1 @ ~60/sec, task 2 @ ~5/sec.

## Installation

Available on PIP: `pip install yfinance_cache`

## Limitations

- only price data is checked if refresh needed
- intraday pre/post price data not available
