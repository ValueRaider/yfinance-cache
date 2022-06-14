import yfinance as yf

import yfc_cache_manager as yfcm
from yfc_utils import *
from yfc_time import *

import datetime, time

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

		# self._history = None
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
				interval=Interval.Days1, 
				max_age=None, # defaults to half of interval
				period=None, 
				start=None, end=None, prepost=False, actions=True,
				auto_adjust=True, back_adjust=False,
				proxy=None, rounding=False, 
				# tz=None, ## I will handle timezones, just make your dates tz-aware
				**kwargs):

		if prepost:
			raise Exception("pre and post-market caching currently not implemented. If you really need it raise an issue on Github")

		# Type checks
		if (not max_age is None) and (not isinstance(max_age, datetime.timedelta)):
			raise Exception("Argument 'max_age' must be timedelta")
		if not period is None:
			if not isinstance(period, Period):
				raise Exception("'period' must be a 'Period' value")
		if not isinstance(interval, Interval):
			raise Exception("'interval' must be Interval")
		if not start is None:
			if (not isinstance(start, str)) and (not isinstance(start, datetime.datetime)):
				raise Exception("Argument 'start' must be str or datetime")
			if isinstance(start, str):
				# start = datetime.datetime.strptime(start, "%Y-%m-%d").astimezone()
				tz_exchange = GetExchangeTimezone(self.info['exchange'])
				start = datetime.datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=tz_exchange)
				if start.dst() is None:
					raise Exception("Failed to set DST of start date")
			else:
				if start.tzinfo is None:
					raise Exception("Argument 'start' tzinfo must be tz-aware")
				if start.dst() is None:
					raise Exception("Argument 'start' tzinfo must be DST-aware")

		if not end is None:
			if (not isinstance(end, str)) and (not isinstance(end, datetime.datetime)):
				raise Exception("Argument 'end' must be str or datetime")
			if isinstance(end, str):
				endDay = datetime.date.fromisoformat(end)
				# end = datetime.datetime.combine(endDay, datetime.time(hour=23, minute=59, second=59)).astimezone()
				tz_exchange = GetExchangeTimezone(self.info['exchange'])
				end = datetime.datetime.combine(endDay, datetime.time(hour=23, minute=59, second=59)).replace(tzinfo=tz_exchange)
				if end.dst() is None:
					raise Exception("Failed to set DST of end date")
			else:
				if end.tzinfo is None:
					raise Exception("Argument 'end' tzinfo must be tz-aware")
				if end.dst() is None:
					raise Exception("Argument 'end' tzinfo must be DST-aware")

		if (not period is None) and (not start is None):
			raise Exception("Don't set both 'period' and 'start' arguments")

		# 'prepost' not doing anything in yfinance

		if max_age is None:
			max_age = 0.5*intervalToTimedelta[interval]

		dt_now = datetime.datetime.now().astimezone()
		if end is None:
			end = dt_now
			# end = datetime.date.today()

		if start is None and period is None:
			# session = GetTimestampMostRecentSession(self.info['exchange'], end)
			# start = session["market_open"]
			tz_exchange = GetExchangeTimezone(self.info['exchange'])
			start = datetime.datetime.combine(end.date(), datetime.time(), tzinfo=tz_exchange)
			# start = end



		start = start.astimezone(ZoneInfo("UTC"))
		end   = end.astimezone(ZoneInfo("UTC"))
		# tz    = ZoneInfo("UTC")
		tz = "UTC"
		# print("start = {0} ; end = {1}".format(start, end))
		
		# tz_local = GetSystemTz()
		tz_exchange = GetExchangeTimezone(self.info['exchange'])

		if period is None:
			pstr = ""
		else:
			pstr = periodToString[period]
		istr = intervalToString[interval]

		h = None
		if not self._history is None:
			if interval in self._history.keys():
				h = self._history[interval]
		if (h is None) and yfcm.IsDatumCached(self.ticker, "history-"+istr):
			self._history[interval] = yfcm.ReadCacheDatum(self.ticker, "history-"+istr)
			h = self._history[interval]

		if h is None:
			# Intercept these arguments:
			# - actions - always store 'dividends' and 'stock splits', drop columns on retrieval
			# - auto_adjust - call yfinance auto_adjust() on DataFrame
			# - back_adjust - call yfinance back_adjust() on DataFrame
			# - rounding - round on retrieval using _np.round()
			# - tz - call Pandas tz_localize() on DataFrame

			## Left with one data dimension: interval

			# print("period={0}".format(pstr))
			# print("interval={0}".format(istr))
			# print("start={0}".format(start))
			# print("end={0}".format(end))

			h = self.dat.history(period=pstr, 
								interval=istr, 
								start=start, end=end, 
								prepost=prepost, actions=actions,
								auto_adjust=auto_adjust, back_adjust=back_adjust,
								proxy=proxy, rounding=rounding, tz=tz, kwargs=kwargs)
			h["FetchDate"] = pd.Timestamp.now()

			## Sometimes YF appends most recent price to table. Remove any out-of-range data:
			if h.index[-1] > end:
				h = h[0:h.shape[0]-1]

			## Performance TODO: mark appropriate rows as final				
			## - requires knowing YF lag. Calibration is best, once per exchange (e.g. LSE is 15min)

			self._history[interval] = h
			yfcm.StoreCacheDatum(self.ticker, "history-"+istr, self._history[interval])
			return h

		## Performance TODO: only check expiry on datapoints not marked 'final'
		## - need to improve 'expiry check' performance, is 3-4x slower than fetching from YF

		h_intervals = [ConvertToDatetime(dt, tz=tz_exchange) for dt in h.index]
		fetch_dts = [ConvertToDatetime(dt, tz=tz_exchange) for dt in h["FetchDate"]]
		expired = IsPriceDatapointExpired_batch(h_intervals, fetch_dts, datetime.timedelta(minutes=30), self.info['exchange'], interval, yf_lag=self.yf_lag)
		if sum(expired) > 0:
			f = np_not(expired)
			h = h[f]
			h_intervals = np.array(h_intervals)[f]
			fetch_dts = np.array(fetch_dts)[f]

		sched = GetExchangeSchedule(self.info['exchange'], start.date(), end.date())

		intervals = GetScheduleIntervals(sched, interval, start, end)

		ranges_to_fetch = IdentifyMissingIntervalRanges(self.info['exchange'], start, end, interval, h_intervals, minDistanceThreshold=5)
		if ranges_to_fetch is None:
			ranges_to_fetch = []

		interval_td = intervalToTimedelta[interval]

		if len(ranges_to_fetch) > 0:
			for r in ranges_to_fetch:
				firstInterval = r[0]
				lastInterval = r[1]

				istart = firstInterval
				iend = lastInterval + interval_td

				istart = istart.astimezone(ZoneInfo("UTC"))
				iend   = iend.astimezone(ZoneInfo("UTC"))

				# print("Will fetch {0} -> {1} (tz={2})".format(istart, iend, tz))

				## Intercept these arguments:
				# - actions - always store 'dividends' and 'stock splits', drop columns on retrieval
				# - auto_adjust - call yfinance auto_adjust() on DataFrame
				# - back_adjust - call yfinance back_adjust() on DataFrame
				# - rounding - round on retrieval, using _np.round()
				# - tz - call Pandas tz_localize() on DataFrame
				## Left with one data dimension: interval

				h2 = self.dat.history(period=pstr, 
									interval=istr, 
									start=istart, end=iend, 
									prepost=prepost, actions=actions,
									auto_adjust=auto_adjust, back_adjust=back_adjust,
									proxy=proxy, rounding=rounding, tz=tz, kwargs=kwargs)
				h2["FetchDate"] = pd.Timestamp.now()

				## Sometimes YF appends most recent price to table. Remove any out-of-range data:
				if h2.index[-1] > end:
					h2 = h2[0:h2.shape[0]-1]

				# f = np.logical_and(h2.index >= start, h2.index < end)
				# h2 = h2[f]

				## Performance TODO: mark appropriate rows as final				
				## - requires knowing YF lag. Calibration is best, once per exchange (e.g. LSE is 15min)

				## If a timepoint is in both h and h2, drop from h:
				f_duplicate = h.index.isin(h2.index)
				h = h[np_not(f_duplicate)]

				h = pd.concat([h, h2])

			h.sort_index(inplace=True)
			self._history[interval] = h
			yfcm.StoreCacheDatum(self.ticker, "history-"+istr, self._history[interval])

		if h is None:
			raise Exception("history() is exiting without price data")

		h_in_range = h[np.logical_and(h.index>=start, h.index<=end)]
		if h_in_range.shape[0] == 0:
			raise Exception("history() exiting without price data in range")
		return h_in_range

	@property
	def info(self):
		if not self._info is None:
			return self._info

		if yfcm.IsDatumCached(self.ticker, "info"):
			self._info = yfcm.ReadCacheDatum(self.ticker, "info")
			return self._info

		self._info = self.dat.info
		yfcm.StoreCacheDatum(self.ticker, "info", self._info)
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
		if not IsTimestampInActiveSession(self.info["exchange"], dt_now):
			## Exchange closed so used hardcoded delay, ...
			self._yf_lag = exchangeToYfLag[self.info["exchange"]]

			## ... but only until next session starts +1H:
			s = GetTimestampNextSession(self.info["exchange"], dt_now)
			expiry = s["market_open"] + timedelta(hours=1)

			yfcm.StoreCacheDatum(exchange_str, "yf_lag", self._yf_lag, expiry=expiry)
			return self._yf_lag

		## Calculate actual delay from live market data, and cache with expiry in 4 weeks

		## Get last hour of 5m price data:
		df_5mins = self.dat.history(interval="5m", start=dt_now-timedelta(hours=1), end=dt_now)
		df_5mins_lastDt = df_5mins.index[df_5mins.shape[0]-1].to_pydatetime()
		df_5mins_lastDt = df_5mins_lastDt.astimezone(ZoneInfo("UTC"))

		## Now 10 minutes of 1m price data around the last 5m candle:
		dt2_end = df_5mins_lastDt + timedelta(minutes=5)
		dt2_start = dt2_end-timedelta(minutes=10)
		df_1mins = self.dat.history(interval="1m", start=dt2_start, end=dt2_end)
		df_1mins_lastDt = df_1mins.index[df_1mins.shape[0]-1].to_pydatetime()

		lag = dt_now - df_1mins_lastDt
		if lag > timedelta(minutes=20):
			raise Exception("{0}: calculated YF lag as {1}, seems excessive".format(self.ticker, lag))
		if lag < timedelta(seconds=0):
			raise Exception("{0}: calculated YF lag as {1}, seems negative".format(self.ticker, lag))
		self._yf_lag = lag
		yfcm.StoreCacheDatum(exchange_str, "yf_lag", self._yf_lag, expiry=dt_now+timedelta(days=28))
		return self._yf_lag
