# yfinance-cache

Persistent caching wrapper for `yfinance` module. Intelligent caching, not dumb caching of web requests - only update cache where missing/outdated and new data expected. Idea is to minimise fetch frequency and quantity - Yahoo API officially only cares about frequency, but I'm guessing they also care about server load from scrapers.

Persistent cache stored in your user cache folder:
- Windows = C:/Users/\<USER\>/AppData/Local/py-yfinance-cache
- Linux = /home/\<USER\>/.cache/py-yfinance-cache
- MacOS = /Users/\<USER\>/Library/Caches/py-yfinance-cache

## Install

Available via PIP: `pip install yfinance_cache`

## Interface

Interaction almost identical to yfinance

```python
import yfinance_cache as yfc
dat = yfc.Ticker("MSFT")
dat.history(period="1wk")
dat.calendar
# etc
```

Supported properties and functions on `Ticker` class:
```
isin                income_stmt                analyst_price_targets      insider_purchases         
history()           balance_sheet              earnings_estimate          insider_roster_holders    
history_metadata    cashflow                   earnings_history           insider_transactions      
info                quarterly_income_stmt      eps_revisions              institutional_holders     
options             quarterly_balance_sheet    eps_trend                  major_holders             
option_chain()      quarterly_cashflow         growth_estimates           mutualfund_holders        
news                calendar                   recommendations                                      
                    get_earnings_dates()       recommendations_summary                              
                    get_release_dates()        revenue_estimate                                     
                                               sustainability                                       
                                               upgrades_downgrades
```

### Price data differences

Other people have implemented price caches, but none adjust cached data for new stock splits or dividends.
YFC does. Price can be adjusted for stock splits, dividends, or both:

```python
dat.history(..., adjust_splits=True, adjust_divs=True)
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
df = dat.history(interval="1d", max_age="1h", trigger_at_market_close=False, ...)
```

With price data, YFC also considers how long exchange been open since last fetch, 
using [exchange_calendars](https://github.com/gerrymanoim/exchange_calendars).
Only if market been open long enough since last fetch, 
or if `trigger_at_market_close=True` and market since closed, 
is cache refreshed.
`max_age` defaults to half of interval.

#### Shares aging

``` python
df = dat.shares(..., max_age='60d')
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
        ...
    }
}
>>> yfc.options.max_ages.calendar = '30d'
>>> yfc.options
{
    "max_ages": {
        "calendar": "30d",
        ...
    }
}
```

## Financials

Financials updates are handled different because they don't age.
Instead, YFC analyses earnings dates to determine exactly when next earnings will be, 
or if Yahoo data is incomplete then YFC will predict.
You can inspect this schedule in new function `dat.get_release_dates()`.

## Verifying cache

Cached prices can be compared against latest Yahoo Finance data, and correct differences:

```python
# Verify prices of one ticker symbol
dat.verify_cached_prices(
	rtol=0.0001,  # relative tolerance for differences
	vol_rtol=0.005,  # relative tolerance specifically for Volume
	correct=[False|'one'|'all'],  # delete incorrect cached data? 'one' = stop after correcting first incorrect prices table ; 'all' = correct all tickers & intervals
	discard_old=False,  # if cached data too old to check (e.g. 30m), assume incorrect and delete?
	quiet=True,  # enable to print nothing, disable to print summary detail of why cached data wrong
	debug=False,  # enable even more detail for debugging 
	debug_interval=None)  # only verify this interval (note: 1d always verified)

# Verify prices of entire cache, ticker symbols processed alphabetically. Recommend using `requests_cache` session.
yfc.verify_cached_tickers_prices(
	session=None,  # recommend you provide a requests_cache here if debugging
	rtol=0.0001,
	vol_rtol=0.005,
	correct=[False|'one'|'all'],
	halt_on_fail=True,  # stop verifying on first fail
	resume_from_tkr=None,  # in case you aborted verification, can jump ahead to this ticker symbol. Append '+1' to start AFTER the ticker
	debug_tkr=None,  # only verify this ticker symbol
	debug_interval=None)
```

These return `False` if difference detected else `True`, regardless of if difference was corrected.

- to scan for first data mismatch but not correct: `yfc.verify_cached_tickers_prices()`. 

- to fix all data issues: `yfc.verify_cached_tickers_prices(correct='all', halt_on_fail=False)`

I hope latest version 0.6.2 fixed the last bugs in applying new dividend-adjustments and splits to cached prices (`Adj Close` etc).
Only genuine differences in not-adjusted prices are Volume differences (~0.5%) - 
Yahoo sometimes changes Volume over 24 hours after that day ended e.g. updating Monday Volume on Wednesday, 
sometimes weeks later!

If you see big differences in the OHLC price of recent intervals (last few days), probably Yahoo is wrong.
Since fetching that price data on day / day after, Yahoo has messed up their data - at least this is my experience.
Cross-check against TradingView or stock exchange website.
