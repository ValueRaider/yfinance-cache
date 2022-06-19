# yfinance-cache
Caching wrapper for yfinance module. Intelligent caching, not dumb caching of web requests:
- If requested data not in cache, `yfinance` is called
- If all requested data is in cache, return that
- If some in cache but some missing, ask `yfinance` for the missing data

Additional logic decides if cached data needs refresh.

## Interface
Interaction will be identical to yfinance:

```python
import yfinance_cache as yf

msft = yf.Ticker("MSFT")

# get stock info
msft.info

# get historical market data
hist = msft.history(period="max")
...
# etc. See yfinance documentation for full API
```

One difference is in fetching price history:
```python
msft = yf.Ticker(interval="1d", max_age=datetime.timedelta(hours=1), ...)
```
`max_age` controls when to refresh cached data to avoid spam. If market is still open and `max_age` time has passed since last fetch, then today's cached price data will be refreshed. 
Defaults to half of interval. Refresh also triggered if market closed since last fetch.

## Limitations

Code is being actively developed so some features missing:

- only price data is checked if refresh needed
- `Tickers` class and `download()` not available - use `Ticker.history()`
- pre/post price data not available