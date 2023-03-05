# yfinance-cache
Caching wrapper for yfinance module. Intelligent caching, not dumb caching of web requests:
- If requested data not in cache, `yfinance` is called
- If all requested data is in cache, return that
- If some in cache but some missing, ask `yfinance` for the missing data

Additional logic decides if cached data needs refresh.

## Interface
Interaction is almost identical to yfinance. Differences are highlighted underneath code:

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
msft.history(interval="1d", max_age="1h", ...)
```
`max_age` controls when to refresh cached data to avoid spam. If market is still open and `max_age` time has passed since last fetch, then today's cached price data will be refreshed. 
Must be `Timedelta` or equivalent `str`. Defaults to half of interval. Refresh also triggered if market has closed since last fetch.

#### Adjusting price
Price can be adjusted for stock splits, dividends, or both. `yfinance` only allows control of dividends adjustment via `auto_adjust`. How Yahoo adjusts for dividends is slightly mysterious so djusted prices are slightly different to Yahoo (tiny relative error ~1e-7)
```python
msft.history(..., adjust_splits=True, adjust_divs=True)
```

#### Verifying cache
Cached prices can be compared against latest Yahoo Finance data, and correct differences:
```python
# Verify prices of one ticker symbol
msft.verify_cached_prices(
	correct=False,  # delete incorrect cached data?
	discard_old=False,  # if cached data too old to check (e.g. 30m), assume incorrect and delete?
	quiet=True,  # disable to print summary detail of why cached data wrong
	debug=False,  # enable even more detail for debugging 
	debug_interval=None)  # only verify this interval (note: 1d always verified)

# Verify prices of entire cache, ticker symbols processed alphabetically. Recommend using `requests_cache` session.
yfc.verify_cached_tickers_prices(
	session=None,  # recommend you provide a requests_cache here
	resume_from_tkr=None,  # in case you aborted verification, can jump ahead
	debug_tkr=None,  # only verify this ticker symbol
	debug_interval=None)
```

## Installation

Available on PIP: `pip install yfinance_cache`

## Known issues / pending tasks

- Considering adding a 'verify' function, checking all cached data against Yahoo.
- Add refresh check to financials data, then to earnings dates.

## Limitations

Code is being actively developed so some features missing:

- only price data is checked if refresh needed
- `Tickers` class and `download()` not available - use `Ticker.history()`
- pre/post price data not available
