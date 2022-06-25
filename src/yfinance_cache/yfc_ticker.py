import yfinance as yf

from . import yfc_cache_manager as yfcm
from . import yfc_dat as yfcd
from . import yfc_utils as yfcu
from . import yfc_time as yfct

import pandas as pd
import numpy as np
import datetime, time
from zoneinfo import ZoneInfo

from pprint import pprint

import numpy as np
def np_and(x, y, z=None):
	if not z is None:
		return np.logical_and(x, np.logical_and(y, z))
	else:	
		return np.logical_and(x, y)
def np_or(x, y, z=None):
	if not z is None:
		return np.logical_or(np.logical_or(x, y), z)
	else:
		return np.logical_or(x, y)
def np_not(x):
	return np.logical_not(x)
def np_max(x, y, z=None, zz=None):
	if not zz is None:
		return np.maximum(np.maximum(np.maximum(x, y), z), zz)
	elif not z is None:
		return np.maximum(np.maximum(x, y), z)
	return np.maximum(x, y)
def np_min(x, y, z=None, zz=None):
	if not zz is None:
		return np.minimum(np.minimum(np.minimum(x, y), z), zz)
	elif not z is None:
		return np.minimum(np.minimum(x, y), z)
	return np.minimum(x, y)

class Ticker:
	def __init__(self, ticker, session=None):
		self.ticker = ticker.upper()

		self.session = session
		self.dat = yf.Ticker(self.ticker, session=self.session)

		self._yf_lag = None

		self._history = {}

		self._info = None

		self._splits = None

		self._financials = None
		self._quarterly_financials = None

		self._major_holders = None

		self._institutional_holders = None

		self._balance_sheet = None
		self._quarterly_balance_sheet = None

		self._cashflow = None
		self._quarterly_cashflow = None

		self._earnings = None
		self._quarterly_earnings = None

		self._sustainability = None

		self._recommendations = None

		self._calendar = None

		self._isin = None

		self._options = None

		self._news = None

	def history(self, 
				interval=yfcd.Interval.Days1, 
				max_age=None, # defaults to half of interval
				period=None, 
				start=None, end=None, prepost=False, actions=True,
				auto_adjust=True, back_adjust=False,
				proxy=None, rounding=False, 
				tz=None,
				**kwargs):

		if prepost:
			raise Exception("pre and post-market caching currently not implemented. If you really need it raise an issue on Github")

		exchange = self.info['exchange']
		tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
		yfct.SetExchangeTzName(exchange, self.info["exchangeTimezoneName"])

		# Type checks
		if (not max_age is None) and (not isinstance(max_age, datetime.timedelta)):
			raise Exception("Argument 'max_age' must be timedelta")
		if not period is None:
			if not isinstance(period, Period):
				raise Exception("'period' must be a 'Period' value")
		if isinstance(interval, str):
			if not interval in yfcd.intervalStrToEnum.keys():
				raise Exception("'interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
			interval = yfcd.intervalStrToEnum[interval]
		if not isinstance(interval, yfcd.Interval):
			raise Exception("'interval' must be yfcd.Interval")
		if not start is None:
			if isinstance(start, str):
				start = datetime.datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=tz_exchange)
			elif isinstance(start, datetime.date) and not isinstance(start, datetime.datetime):
				start = datetime.datetime.combine(start, datetime.time(0), tz_exchange)
			elif not isinstance(start, datetime.datetime):
				raise Exception("Argument 'start' must be str, date or datetime")
			if start.tzinfo is None:
				start = start.replace(tzinfo=tz_exchange)
			if start.dst() is None:
				raise Exception("Argument 'start' tzinfo must be DST-aware")

		if not end is None:
			if isinstance(end, str):
				end_d = datetime.datetime.strptime(end, "%Y-%m-%d").date()
				end = datetime.datetime.combine(end_d, datetime.time(23, 59), tz_exchange)
			elif isinstance(end, datetime.date) and not isinstance(end, datetime.datetime):
				end = datetime.datetime.combine(end, datetime.time(23, 59), tz_exchange)
			elif not isinstance(end, datetime.datetime):
				raise Exception("Argument 'end' must be str, date or datetime")
			if end.tzinfo is None:
				end = end.replace(tzinfo=tz_exchange)
			if end.dst() is None:
				raise Exception("Argument 'end' tzinfo must be DST-aware")

		if (not period is None) and (not start is None):
			raise Exception("Don't set both 'period' and 'start' arguments")

		if auto_adjust and back_adjust:
			raise Exception("Only enable one of 'auto_adjust' and 'back_adjust")

		# 'prepost' not doing anything in yfinance

		if max_age is None:
			if interval == yfcd.Interval.Days1:
				max_age = datetime.timedelta(hours=4)
			elif interval in [yfcd.Interval.Days5, yfcd.Interval.Week]:
				max_age = datetime.timedelta(hours=60)
			else:
				max_age = 0.5*yfcd.intervalToTimedelta[interval]

		dt_now = datetime.datetime.now().astimezone()
		if end is None:
			end = dt_now

		if start is None and period is None:
			start = datetime.datetime.combine(end.date(), datetime.time(), tzinfo=tz_exchange)

		if period is None:
			pstr = ""
		else:
			pstr = periodToString[period]
		istr = yfcd.intervalToString[interval]

		## Trigger an estimation of Yahoo data delay:
		yf_lag = self.yf_lag

		h = None
		if not self._history is None:
			if interval in self._history.keys():
				h = self._history[interval]
		if (h is None) and yfcm.IsDatumCached(self.ticker, "history-"+istr):
			self._history[interval] = yfcm.ReadCacheDatum(self.ticker, "history-"+istr)
			h = self._history[interval]

		if not h is None:
			if not ("Dividends" in h.columns and "Stock Splits" in h.columns):
				## Force a fetch
				h = None

		if h is None:
			h = self._fetchYfHistory(pstr, interval, start, end, prepost, proxy, tz, kwargs)

		else:
			## Performance TODO: only check expiry on datapoints not marked 'final'
			## - need to improve 'expiry check' performance, is 3-4x slower than fetching from YF

			f_final = h["Final?"].values
			n = h.shape[0]

			h_interval_dts = [yfct.ConvertToDatetime(dt, tz=tz_exchange) for dt in h.index]
			# fetch_dts = [yfct.ConvertToDatetime(dt, tz=tz_exchange) for dt in h["FetchDate"]]
			# expired = yfct.IsPriceDatapointExpired_batch(h_interval_dts, fetch_dts, max_age, exchange, interval, yf_lag=self.yf_lag)
			expired = [False]*n
			for idx in np.where(~f_final)[0]:
				h_interval_dt = yfct.ConvertToDatetime(h.index[idx], tz=tz_exchange)
				fetch_dt = yfct.ConvertToDatetime(h["FetchData"].values[idx], tz=tz_exchange)
				expired[idx] = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, max_age, exchange, interval, yf_lag=self.yf_lag)
			if isinstance(expired, list):
				expired = np.array(expired)
			if sum(expired) > 0:
				h = h[~expired]
				h_interval_dts = np.array(h_interval_dts)[~expired]
				fetch_dts = np.array(fetch_dts)[~expired]

			## Potential perf improvement: tag rows as fully contiguous to avoid searching for gaps
			h_intervals = np.array([yfct.GetTimestampCurrentInterval(exchange, idt, interval, allowLateDailyData=True,weeklyUseYahooDef=True) for idt in h_interval_dts])
			h_intervals = [x["interval_open"] for x in h_intervals]
			sched = yfct.GetExchangeSchedule(exchange, start.date(), end.date())
			intervals = yfct.GetScheduleIntervals(sched, interval, start, end)
			ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, start, end, interval, h_intervals, minDistanceThreshold=5)
			if ranges_to_fetch is None:
				ranges_to_fetch = []

			interval_td = yfcd.intervalToTimedelta[interval]
			if len(ranges_to_fetch) > 0:
				for r in ranges_to_fetch:
					firstInterval = r[0]
					lastInterval = r[1]

					istart = firstInterval
					s = yfct.GetTimestampCurrentSession(exchange, lastInterval)
					iend = lastInterval + interval_td
					iend = min(iend, s["market_close"])

					h2 = self._fetchYfHistory(pstr, interval, istart, iend, prepost, proxy, tz, kwargs)

					## If a timepoint is in both h and h2, drop from h:
					f_duplicate = h.index.isin(h2.index)
					## Actually, why?
					if sum(f_duplicate) > 0:
						print("h:")
						print(h)
						print("h2:")
						print(h2)
						raise Exception("Duplicate dates between cached df and newly fetched data")
					h = h[np_not(f_duplicate)]

					h = pd.concat([h, h2])

				h.sort_index(inplace=True)

		if h is None:
			raise Exception("history() is exiting without price data")

		# Cache
		self._history[interval] = h
		yfcm.StoreCacheDatum(self.ticker, "history-"+istr, self._history[interval])

		# Present table for user:
		h = h[np.logical_and(h.index>=start, h.index<=end)].copy()
		if h.shape[0] == 0:
			raise Exception("history() exiting without price data in date range")
		if not actions:
			h = h.drop(["Dividends","Stock Splits"], axis=1)
		else:
			if not "Dividends" in h.columns:
				raise Exception("Dividends column missing from table")
		if auto_adjust:
			df = yf.utils.auto_adjust(h[["Open","Close","Adj Close","Low","High","Volume"]])
			for c in ["Open","Close","Low","High","Volume"]:
				h[c] = df[c]
		elif back_adjust:
			df = yf.utils.back_adjust(h[["Open","Close","Adj Close","Low","High","Volume"]])
			for c in ["Open","Close","Low","High","Volume"]:
				h[c] = df[c]
		if rounding:
			# Round to 4 sig-figs
			h[["Open","Close","Low","High"]] = np.round(h[["Open","Close","Low","High"]], yfcu.CalculateRounding(h["Close"][h.shape[0]-1], 4))

		return h

	def _fetchYfHistory(self, pstr, interval, start, end, prepost, proxy, tz, kwargs):
		exchange = self.dat.info["exchange"]
		istr = yfcd.intervalToString[interval]

		# print("{}: fetching {} -> {}".format(self.ticker, start, end))

		df = self.dat.history(period=pstr, 
							interval=istr, 
							start=start, end=end, 
							prepost=prepost, 
							actions=True, # Always fetch
							auto_adjust=False, # store raw data, adjust myself
							back_adjust=False, # store raw data, adjust myself
							proxy=proxy, 
							rounding=False, # store raw data, round myself
							tz=tz, kwargs=kwargs)
		fetch_dt = pd.Timestamp.now().tz_localize("UTC")

		## Sometimes Yahoo appends most recent price to table. Remove any out-of-range data:
		## NOTE: YF has a bug-fix pending merge: https://github.com/ranaroussi/yfinance/pull/1012
		if df.index[-1] > end:
			df = df[0:df.shape[0]-1]

		## Verify that all datetimes match up with actual intervals:
		intervals = np.array([yfct.GetTimestampCurrentInterval(exchange, i, interval, allowLateDailyData=True,weeklyUseYahooDef=True) for i in df.index])
		f_na = intervals==None
		if sum(f_na) > 0:
			for idx in np.where(f_na)[0]:
				dt = df.index[idx]
				print("Failed to map: {} (exchange{}, xcal={})".format(dt, exchange, yfcd.exchangeToXcalExchange[exchange]))
			raise Exception("Problem with dates returned by Yahoo, see above")
		interval_closes = np.array([i["interval_close"] for i in intervals])

		## TODO: replace df.index with 'interval opens'. But currently getting errors wth this:
		# df.index = [i["interval_open"] for i in intervals]

		n = df.shape[0]
		if interval in [yfcd.Interval.Days1, yfcd.Interval.Days5, yfcd.Interval.Week]:
			# The time between intervalEnd and midnight is ambiguous. Yahoo shouldn't have new data, 
			# but with daily candles it can. So treat midnight as threshold for final data.
			data_final = np.array([fetch_dt.date() > intervals[i]["interval_close"].date() for i in range(n)])
		else:
			data_final = fetch_dt >= (interval_closes+self.yf_lag)
		df["Final?"] = data_final

		df["FetchDate"] = fetch_dt

		return df

	@property
	def info(self):
		if not self._info is None:
			return self._info

		if yfcm.IsDatumCached(self.ticker, "info"):
			self._info = yfcm.ReadCacheDatum(self.ticker, "info")
			return self._info

		self._info = self.dat.info
		yfcm.StoreCacheDatum(self.ticker, "info", self._info)

		yfct.SetExchangeTzName(self._info["exchange"], self._info["exchangeTimezoneName"])

		return self._info

	@property
	def splits(self):
		if not self._splits is None:
			return self._splits

		if yfcm.IsDatumCached(self.ticker, "splits"):
			self._splits = yfcm.ReadCacheDatum(self.ticker, "splits")
			return self._splits

		self._splits = self.dat.splits
		yfcm.StoreCacheDatum(self.ticker, "splits", self._splits)
		return self._splits

	@property
	def financials(self):
		if not self._financials is None:
			return self._financials

		if yfcm.IsDatumCached(self.ticker, "financials"):
			self._financials = yfcm.ReadCacheDatum(self.ticker, "financials")
			return self._financials

		self._financials = self.dat.financials
		yfcm.StoreCacheDatum(self.ticker, "financials", self._financials)
		return self._financials

	@property
	def quarterly_financials(self):
		if not self._quarterly_financials is None:
			return self._quarterly_financials

		if yfcm.IsDatumCached(self.ticker, "quarterly_financials"):
			self._quarterly_financials = yfcm.ReadCacheDatum(self.ticker, "quarterly_financials")
			return self._quarterly_financials

		self._quarterly_financials = self.dat.quarterly_financials
		yfcm.StoreCacheDatum(self.ticker, "quarterly_financials", self._quarterly_financials)
		return self._quarterly_financials

	@property
	def major_holders(self):
		if not self._major_holders is None:
			return self._major_holders

		if yfcm.IsDatumCached(self.ticker, "major_holders"):
			self._major_holders = yfcm.ReadCacheDatum(self.ticker, "major_holders")
			return self._major_holders

		self._major_holders = self.dat.major_holders
		yfcm.StoreCacheDatum(self.ticker, "major_holders", self._major_holders)
		return self._major_holders

	@property
	def institutional_holders(self):
		if not self._institutional_holders is None:
			return self._institutional_holders

		if yfcm.IsDatumCached(self.ticker, "institutional_holders"):
			self._institutional_holders = yfcm.ReadCacheDatum(self.ticker, "institutional_holders")
			return self._institutional_holders

		self._institutional_holders = self.dat.institutional_holders
		yfcm.StoreCacheDatum(self.ticker, "institutional_holders", self._institutional_holders)
		return self._institutional_holders

	@property
	def balance_sheet(self):
		if not self._balance_sheet is None:
			return self._balance_sheet

		if yfcm.IsDatumCached(self.ticker, "balance_sheet"):
			self._balance_sheet = yfcm.ReadCacheDatum(self.ticker, "balance_sheet")
			return self._balance_sheet

		dat = yf.Ticker(self.ticker, session=self.session)
		self._balance_sheet = self.dat.balance_sheet
		yfcm.StoreCacheDatum(self.ticker, "balance_sheet", self._balance_sheet)
		return self._balance_sheet

	@property
	def quarterly_balance_sheet(self):
		if not self._quarterly_balance_sheet is None:
			return self._quarterly_balance_sheet

		if yfcm.IsDatumCached(self.ticker, "quarterly_balance_sheet"):
			self._quarterly_balance_sheet = yfcm.ReadCacheDatum(self.ticker, "quarterly_balance_sheet")
			return self._quarterly_balance_sheet

		dat = yf.Ticker(self.ticker, session=self.session)
		self._quarterly_balance_sheet = self.dat.quarterly_balance_sheet
		yfcm.StoreCacheDatum(self.ticker, "quarterly_balance_sheet", self._quarterly_balance_sheet)
		return self._quarterly_balance_sheet

	@property
	def cashflow(self):
		if not self._cashflow is None:
			return self._cashflow

		if yfcm.IsDatumCached(self.ticker, "cashflow"):
			self._cashflow = yfcm.ReadCacheDatum(self.ticker, "cashflow")
			return self._cashflow

		dat = yf.Ticker(self.ticker, session=self.session)
		self._cashflow = self.dat.cashflow
		yfcm.StoreCacheDatum(self.ticker, "cashflow", self._cashflow)
		return self._cashflow

	@property
	def quarterly_cashflow(self):
		if not self._quarterly_cashflow is None:
			return self._quarterly_cashflow

		if yfcm.IsDatumCached(self.ticker, "quarterly_cashflow"):
			self._quarterly_cashflow = yfcm.ReadCacheDatum(self.ticker, "quarterly_cashflow")
			return self._quarterly_cashflow

		dat = yf.Ticker(self.ticker, session=self.session)
		self._quarterly_cashflow = self.dat.quarterly_cashflow
		yfcm.StoreCacheDatum(self.ticker, "quarterly_cashflow", self._quarterly_cashflow)
		return self._quarterly_cashflow

	@property
	def earnings(self):
		if not self._earnings is None:
			return self._earnings

		if yfcm.IsDatumCached(self.ticker, "earnings"):
			self._earnings = yfcm.ReadCacheDatum(self.ticker, "earnings")
			return self._earnings

		dat = yf.Ticker(self.ticker, session=self.session)
		self._earnings = self.dat.earnings
		yfcm.StoreCacheDatum(self.ticker, "earnings", self._earnings)
		return self._earnings

	@property
	def quarterly_earnings(self):
		if not self._quarterly_earnings is None:
			return self._quarterly_earnings

		if yfcm.IsDatumCached(self.ticker, "quarterly_earnings"):
			self._quarterly_earnings = yfcm.ReadCacheDatum(self.ticker, "quarterly_earnings")
			return self._quarterly_earnings

		dat = yf.Ticker(self.ticker, session=self.session)
		self._quarterly_earnings = self.dat.quarterly_earnings
		yfcm.StoreCacheDatum(self.ticker, "quarterly_earnings", self._quarterly_earnings)
		return self._quarterly_earnings

	@property
	def sustainability(self):
		if not self._sustainability is None:
			return self._sustainability

		if yfcm.IsDatumCached(self.ticker, "sustainability"):
			self._sustainability = yfcm.ReadCacheDatum(self.ticker, "sustainability")
			return self._sustainability

		dat = yf.Ticker(self.ticker, session=self.session)
		self._sustainability = self.dat.sustainability
		yfcm.StoreCacheDatum(self.ticker, "sustainability", self._sustainability)
		return self._sustainability

	@property
	def recommendations(self):
		if not self._recommendations is None:
			return self._recommendations

		if yfcm.IsDatumCached(self.ticker, "recommendations"):
			self._recommendations = yfcm.ReadCacheDatum(self.ticker, "recommendations")
			return self._recommendations

		dat = yf.Ticker(self.ticker, session=self.session)
		self._recommendations = self.dat.recommendations
		yfcm.StoreCacheDatum(self.ticker, "recommendations", self._recommendations)
		return self._recommendations

	@property
	def calendar(self):
		if not self._calendar is None:
			return self._calendar

		if yfcm.IsDatumCached(self.ticker, "calendar"):
			self._calendar = yfcm.ReadCacheDatum(self.ticker, "calendar")
			return self._calendar

		dat = yf.Ticker(self.ticker, session=self.session)
		self._calendar = self.dat.calendar
		yfcm.StoreCacheDatum(self.ticker, "calendar", self._calendar)
		return self._calendar

	@property
	def inin(self):
		if not self._inin is None:
			return self._inin

		if yfcm.IsDatumCached(self.ticker, "inin"):
			self._inin = yfcm.ReadCacheDatum(self.ticker, "inin")
			return self._inin

		dat = yf.Ticker(self.ticker, session=self.session)
		self._inin = self.dat.inin
		yfcm.StoreCacheDatum(self.ticker, "inin", self._inin)
		return self._inin

	@property
	def options(self):
		if not self._options is None:
			return self._options

		if yfcm.IsDatumCached(self.ticker, "options"):
			self._options = yfcm.ReadCacheDatum(self.ticker, "options")
			return self._options

		dat = yf.Ticker(self.ticker, session=self.session)
		self._options = self.dat.options
		yfcm.StoreCacheDatum(self.ticker, "options", self._options)
		return self._options

	@property
	def news(self):
		if not self._news is None:
			return self._news

		if yfcm.IsDatumCached(self.ticker, "news"):
			self._news = yfcm.ReadCacheDatum(self.ticker, "news")
			return self._news

		dat = yf.Ticker(self.ticker, session=self.session)
		self._news = self.dat.news
		yfcm.StoreCacheDatum(self.ticker, "news", self._news)
		return self._news

	@property
	def yf_lag(self):
		if not self._yf_lag is None:
			return self._yf_lag

		exchange_str = "exchange-{0}".format(self.info["exchange"])
		if yfcm.IsDatumCached(exchange_str, "yf_lag"):
			self._yf_lag = yfcm.ReadCacheDatum(exchange_str, "yf_lag")
			return self._yf_lag

		## Have to calculate lag from YF data.
		## To avoid circular logic will call YF directly, not use my cache. Because cache requires knowing lag.

		dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
		if not yfct.IsTimestampInActiveSession(self.info["exchange"], dt_now):
			## Exchange closed so used hardcoded delay, ...
			self._yf_lag = yfcd.exchangeToYfLag[self.info["exchange"]]

			## ... but only until next session starts +1H:
			s = yfct.GetTimestampNextSession(self.info["exchange"], dt_now)
			expiry = s["market_open"] + datetime.timedelta(hours=1)

			yfcm.StoreCacheDatum(exchange_str, "yf_lag", self._yf_lag, expiry=expiry)
			return self._yf_lag

		## Calculate actual delay from live market data, and cache with expiry in 4 weeks

		## Get last hour of 5m price data:
		start_dt = dt_now-datetime.timedelta(hours=1)
		df_5mins = self.dat.history(interval="5m", start=start_dt, end=dt_now)
		if df_5mins.shape[0] == 0:
			# raise Exception("Failed to fetch 5m data for tkr={}, start={}".format(self.ticker, start_dt))
			print("WARNING: Failed to fetch 5m data for tkr={} so setting yf_lag to hardcoded default".format(self.ticker, start_dt))
			self._yf_lag = yfcd.exchangeToYfLag[self.info["exchange"]]
			return self._yf_lag
		df_5mins_lastDt = df_5mins.index[df_5mins.shape[0]-1].to_pydatetime()
		df_5mins_lastDt = df_5mins_lastDt.astimezone(ZoneInfo("UTC"))

		## Now 10 minutes of 1m price data around the last 5m candle:
		dt2_start = df_5mins_lastDt - datetime.timedelta(minutes=5)
		dt2_end = df_5mins_lastDt + datetime.timedelta(minutes=5)
		df_1mins = self.dat.history(interval="1m", start=dt2_start, end=dt2_end)
		df_1mins_lastDt = df_1mins.index[df_1mins.shape[0]-1].to_pydatetime()

		lag = dt_now - df_1mins_lastDt
		if lag > datetime.timedelta(minutes=40):
			raise Exception("{0}: calculated YF lag as {1}, seems excessive".format(self.ticker, lag))
		if lag < datetime.timedelta(seconds=0):
			raise Exception("{0}: calculated YF lag as {1}, seems negative".format(self.ticker, lag))
		self._yf_lag = lag
		yfcm.StoreCacheDatum(exchange_str, "yf_lag", self._yf_lag, expiry=dt_now+datetime.timedelta(days=28))
		return self._yf_lag
