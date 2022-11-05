import yfinance as yf

from . import yfc_cache_manager as yfcm
from . import yfc_dat as yfcd
from . import yfc_utils as yfcu
from . import yfc_time as yfct

from time import perf_counter
import pandas as pd
import numpy as np
from scipy import ndimage as _ndimage
import datetime, time
from zoneinfo import ZoneInfo

from pprint import pprint


import numpy as np
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

		self._debug = False
		# self._debug = True
		self._trace = False
		# self._trace = True
		self._trace_depth = 0

	def history(self, 
				interval="1d", 
				max_age=None, # defaults to half of interval
				period=None, 
				start=None, end=None, prepost=False, actions=True,
				adjust_splits=True, adjust_divs=True,
				keepna=False,
				proxy=None, rounding=False, 
				tz=None,
				**kwargs):

		if prepost:
			raise Exception("pre and post-market caching currently not implemented. If you really need it raise an issue on Github")

		debug = self._debug
		# debug = True

		if debug:
			print("")
			print("YFC: history(tkr={}, interval={}, period={}, start={}, end={}, max_age={}, adjust_splits={})".format(self.ticker, interval, period, start, end, max_age, adjust_splits))
		elif self._trace:
			self._trace_depth += 1
			print(" "*self._trace_depth + "YFC: history(tkr={}, interval={}, period={}, start={}, end={}, max_age={}, adjust_splits={})".format(self.ticker, interval, period, start, end, max_age, adjust_splits))

		td_1d = datetime.timedelta(days=1)
		exchange = self.info['exchange']
		tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
		yfct.SetExchangeTzName(exchange, self.info["exchangeTimezoneName"])
		dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))

		# Type checks
		if (not max_age is None) and (not isinstance(max_age, datetime.timedelta)):
			raise Exception("Argument 'max_age' must be timedelta")
		if not period is None:
			if isinstance(period, str):
				if not period in yfcd.periodStrToEnum.keys():
					raise Exception("'period' if str must be one of: {}".format(yfcd.periodStrToEnum.keys()))
				period = yfcd.periodStrToEnum[period]
			if not isinstance(period, yfcd.Period):
				raise Exception("'period' must be a yfcd.Period")
		if isinstance(interval, str):
			if not interval in yfcd.intervalStrToEnum.keys():
				raise Exception("'interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
			interval = yfcd.intervalStrToEnum[interval]
		if not isinstance(interval, yfcd.Interval):
			raise Exception("'interval' must be yfcd.Interval")
		start_d = None ; end_d = None
		if not start is None:
			if isinstance(start, str):
				start_d = datetime.datetime.strptime(start, "%Y-%m-%d").date()
				start = datetime.datetime.combine(start_d, datetime.time(0), tz_exchange)
			elif isinstance(start, datetime.date) and not isinstance(start, datetime.datetime):
				start_d = start
				start = datetime.datetime.combine(start, datetime.time(0), tz_exchange)
			elif not isinstance(start, datetime.datetime):
				raise Exception("Argument 'start' must be str, date or datetime")
			if start.tzinfo is None:
				start = start.replace(tzinfo=tz_exchange)
			else:
				start = start.astimezone(tz_exchange)
			if start.dst() is None:
				raise Exception("Argument 'start' tzinfo must be DST-aware")
			if start > dt_now:
				return None
		if not end is None:
			if isinstance(end, str):
				end_d = datetime.datetime.strptime(end, "%Y-%m-%d").date()
				end = datetime.datetime.combine(end_d, datetime.time(0), tz_exchange)
			elif isinstance(end, datetime.date) and not isinstance(end, datetime.datetime):
				end_d = end
				end = datetime.datetime.combine(end, datetime.time(0), tz_exchange)
			elif not isinstance(end, datetime.datetime):
				raise Exception("Argument 'end' must be str, date or datetime")
			if end.tzinfo is None:
				end = end.replace(tzinfo=tz_exchange)
			else:
				end = end.astimezone(tz_exchange)
			if end.dst() is None:
				raise Exception("Argument 'end' tzinfo must be DST-aware")
		if (not period is None) and (not start is None):
			raise Exception("Don't set both 'period' and 'start' arguments")

		if interval == yfcd.Interval.Week:
			# Note: if start is on weekend then Yahoo can return weekly data starting 
			#       on Saturday. This breaks YFC, start must be Monday! So fix here:
			if start is None:
				# Working with simple dates, easy
				if start_d.weekday() == 5:
					start_d += datetime.timedelta(days=2)
				elif start_d.weekday() == 6:
					start_d += datetime.timedelta(days=1)
			else:
				wd = start.astimezone(tz_exchange).weekday()
				if wd in [5,6]:
					start_d = start.astimezone(tz_exchange).date() + datetime.timedelta(days=7-wd)
					start = datetime.datetime.combine(start_d, datetime.time(0), tz_exchange)

		# 'prepost' not doing anything in yfinance

		if max_age is None:
			if interval == yfcd.Interval.Days1:
				max_age = datetime.timedelta(hours=4)
			elif interval == yfcd.Interval.Week:
				max_age = datetime.timedelta(hours=60)
			elif interval == yfcd.Interval.Months1:
				max_age = datetime.timedelta(days=15)
			elif interval == yfcd.Interval.Months3:
				max_age = datetime.timedelta(days=45)
			else:
				max_age = 0.5*yfcd.intervalToTimedelta[interval]

		if (interval in self._history) and (not self._history[interval] is None):
			h = self._history[interval]
		else:
			h = self._getCachedPrices(interval)
			if not h is None:
				self._history[interval] = h
		h_cache_key = "history-"+yfcd.intervalToString[interval]

		# Handle missing dates, or dependencies between date arguments
		if not period is None:
			if h is None:
				# Use period
				pstr = yfcd.periodToString[period]
				start = None ; end = None
			else:
				# Map period to start->end range so logic can intelligently fetch missing data
				pstr = None
				d_now = dt_now.astimezone(tz_exchange).date()
				sched = yfct.GetExchangeSchedule(exchange, d_now-(7*td_1d), d_now+td_1d)
				# Discard days that haven't opened yet
				sched = sched[(sched["open"]+self.yf_lag)<=dt_now]
				last_open_day = sched["open"][-1].date()
				end = datetime.datetime.combine(last_open_day+td_1d, datetime.time(0), tz_exchange)
				end_d = end.date()
				if period == yfcd.Period.Max:
					start = datetime.datetime.combine(datetime.date(yfcd.yf_min_year, 1, 1), datetime.time(0), tz_exchange)
				else:
					start = yfct.DtSubtractPeriod(end, period)
				while not yfct.ExchangeOpenOnDay(exchange, start.date()):
					start += td_1d
				start_d = start.date()
		else:
			pstr = None
			if end is None:
				end = datetime.datetime.combine(dt_now.date() + td_1d, datetime.time(0), tz_exchange)
			if start is None:
				start = datetime.datetime.combine(end.date()-td_1d, datetime.time(0), tz_exchange)

		if debug:
			print("- start={} , end={}".format(start, end))

		if (not start is None) and start==end:
			return None

		if not start is None:
			try:
				sched = yfct.GetExchangeSchedule(exchange, start.date(), start.date()+7*td_1d)
			except Exception as e:
				if "Need to add mapping" in str(e):
					raise Exception("Need to add mapping of exchange {} to xcal (ticker={})".format(self.info["exchange"], self.ticker))
				else:
					raise
			if sched is None:
				raise Exception("sched is None for date range {}->{} and ticker {}".format(start.date(), start.date()+4*td_1d, self.ticker))
			if sched["open"][0] > dt_now:
				# Requested date range is in future
				return None

		if ((start_d is None) or (end_d is None)) and (not start is None) and (not end is None):
			# if start_d/end_d not set then start/end are datetimes, so need to inspect
			# schedule opens/closes to determine days
			sched = yfct.GetExchangeSchedule(exchange, start.date(), end.date()+td_1d)
			n = sched.shape[0]
			if start < sched["open"][0]:
				start_d = start.date()
			else:
				start_d = start.date() +td_1d
			if end >= sched["close"][n-1]:
				end_d = end.date()+td_1d
			else:
				end_d = end.date()

		if (not start is None) and (not end is None):
			listing_date = yfcm.ReadCacheDatum(self.ticker, "listing_date")
			if (not listing_date is None) and (not isinstance(listing_date, datetime.date)):
				raise Exception("listing_date = {} ({}) should be a date".format(listing_date, type(listing_date)))
			if not listing_date is None:
				if start_d < listing_date:
					start_d = listing_date
					start = datetime.datetime.combine(listing_date, datetime.time(0), tz_exchange)

		interday = interval in [yfcd.Interval.Days1,yfcd.Interval.Week,yfcd.Interval.Months1,yfcd.Interval.Months3]

		## Trigger an estimation of Yahoo data delay:
		yf_lag = self.yf_lag

		d_tomorrow = dt_now.astimezone(tz_exchange).date() +td_1d
		dt_tomorrow = datetime.datetime.combine(d_tomorrow, datetime.time(0), tz_exchange)
		h_lastAdjustD = None
		if h is None:
			if not period is None:
				h = self._fetchYfHistory(pstr, interval, None, None, prepost, proxy, kwargs)
			else:
				if interval == yfcd.Interval.Days1:
					# Ensure daily always up-to-now
					h = self._fetchYfHistory(pstr, interval, start_d, d_tomorrow, prepost, proxy, kwargs)
				else:
					if interday:
						h = self._fetchYfHistory(pstr, interval, start_d, end_d, prepost, proxy, kwargs)
					else:
						h = self._fetchYfHistory(pstr, interval, start, end, prepost, proxy, kwargs)
			if h is None:
				raise Exception("{}: Failed to fetch date range {}->{}".format(self.ticker, start, end))

			# Adjust
			if interval == yfcd.Interval.Days1:
				h = self._processYahooAdjustment(h, interval)
				h_lastAdjustD = h.index[-1].date()
			else:
				# Ensure h is split-adjusted. Sometimes Yahoo returns unadjusted data
				h_lastDt = h.index[-1].to_pydatetime()
				next_day = yfct.GetTimestampNextInterval(exchange, h_lastDt, yfcd.Interval.Days1)["interval_open"]
				if next_day > dt_now.astimezone(tz_exchange).date():
					h = self._processYahooAdjustment(h, interval)
					h_lastAdjustD = h_lastDt.date()
				else:
					try:
						df_daily = self.history(start=next_day, interval=yfcd.Interval.Days1, max_age=td_1d, auto_adjust=False)
					except yfcd.NoPriceDataInRangeException:
						df_daily = None
					except Exception as e:
						if "Failed to fetch date range" in str(e):
							df_daily = None
						else:
							raise
					if (df_daily is None) or (df_daily.shape[0]==0):
						h = self._processYahooAdjustment(h, interval)
						h_lastAdjustD = h_lastDt.date()
					else:
						h = self._processYahooAdjustment(h, interval)
						h_lastAdjustD = df_daily.index[-1].date()

		else:
			## Performance TODO: only check expiry on datapoints not marked 'final'
			## - need to improve 'expiry check' performance, is 3-4x slower than fetching from YF

			if not "CSF" in h.columns:
				raise Exception("{}: Cached price data missing 'CSF' column, need to flush cache".format(self.ticker))

			n = h.shape[0]
			if n>0:
				if debug:
					print("- h lastDt = {}".format(h.index[-1]))
				elif self._trace:
					print(" "*self._trace_depth + "- h lastDt = {}".format(h.index[-1]))
			if interday:
				if isinstance(h.index[0], pd.Timestamp):
					h_interval_dts = h.index.date
				else:
					h_interval_dts = h.index
			else:
				h_interval_dts = [yfct.ConvertToDatetime(dt, tz=tz_exchange) for dt in h.index]
			h_interval_dts = np.array(h_interval_dts)
			if interval == yfcd.Interval.Days1:
				# Daily data is always contiguous so only need to check last row
				h_interval_dt = h_interval_dts[-1]
				fetch_dt = yfct.ConvertToDatetime(h["FetchDate"].iloc[-1], tz=tz_exchange)
				last_expired = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, max_age, exchange, interval, yf_lag=self.yf_lag)
				if last_expired:
					# Drop last row because expired
					h = h.drop(h.index[-1])
					h_interval_dts = h_interval_dts[0:n-1]
					n -= 1
			else:
				expired = np.array([False]*n)
				f_final = h["Final?"].values
				for idx in np.where(~f_final)[0]:
					h_interval_dt = h_interval_dts[idx]
					fetch_dt = yfct.ConvertToDatetime(h["FetchDate"][idx], tz=tz_exchange)
					expired[idx] = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, max_age, exchange, interval, yf_lag=self.yf_lag)
				if expired.any():
					h = h.drop(h.index[expired])
					h_interval_dts = h_interval_dts[~expired]
			#
			## Potential perf improvement: tag rows as fully contiguous to avoid searching for gaps
			# h_intervals = np.array([yfct.GetTimestampCurrentInterval(exchange, idt, interval, weeklyUseYahooDef=True) for idt in h_interval_dts])
			h_intervals = yfct.GetTimestampCurrentInterval_batch(exchange, h_interval_dts, interval, weeklyUseYahooDef=True)
			if h_intervals is None:
				h_intervals = pd.DataFrame(data={"interval_open":[], "interval_close":[]})
			f_na = h_intervals["interval_open"].isna().values
			if f_na.any():
				print(h[f_na])
				raise Exception("Bad rows found in prices table")
				if debug:
					print("- found bad rows, deleting:")
					print(h[f_na])
				h = h[~f_na].copy()
				h_intervals = h_intervals[~f_na]
			if h_intervals.shape[0]>0 and isinstance(h_intervals["interval_open"][0], datetime.datetime):
				h_interval_opens = [x.to_pydatetime().astimezone(tz_exchange) for x in h_intervals["interval_open"]]
			else:
				h_interval_opens = h_intervals["interval_open"].values

			if interval == yfcd.Interval.Days1:
				if len(h_intervals)==0:
					ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, start, end, interval, h_interval_opens, weeklyUseYahooDef=True, minDistanceThreshold=5)
				else:
					# Ensure that daily data always up-to-date to now
					h_start = h_intervals["interval_open"].iloc[0]
					h_end = h_intervals["interval_close"].iloc[h_intervals.shape[0]-1]
					if not isinstance(h_start, datetime.datetime):
						h_start = datetime.datetime.combine(h_start, datetime.time(0), tz_exchange)
						h_end = datetime.datetime.combine(h_end, datetime.time(0), tz_exchange)
					#
					if h_start <= start:
						rangePre_to_fetch = None
					else:
						try:
							rangePre_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, start, h_start, interval, None, weeklyUseYahooDef=True, minDistanceThreshold=5)
						except yfcd.NoIntervalsInRangeException:
							rangePre_to_fetch = None
					if not rangePre_to_fetch is None:
						if len(rangePre_to_fetch) > 1:
							raise Exception("Expected only one element in rangePre_to_fetch[], but = {}".format(rangePre_to_fetch))
						rangePre_to_fetch = rangePre_to_fetch[0]
					#
					target_end = dt_now
					d = dt_now.astimezone(tz).date()
					sched = yfct.GetExchangeSchedule(exchange, d, d+td_1d)
					if (not sched is None) and (sched.shape[0]>0) and (dt_now > sched["open"].iloc[0]):
						target_end = sched["close"].iloc[0]+datetime.timedelta(hours=2)
					if h_end < target_end:
						try:
							rangePost_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, h_end, target_end, interval, None, weeklyUseYahooDef=True, minDistanceThreshold=5)
						except yfcd.NoIntervalsInRangeException:
							rangePost_to_fetch = None
					else:
						rangePost_to_fetch = None
					ranges_to_fetch = []
					if not rangePost_to_fetch is None:
						if len(rangePost_to_fetch) > 1:
							raise Exception("Expected only one element in rangePost_to_fetch[], but = {}".format(rangePost_to_fetch))
						rangePost_to_fetch = rangePost_to_fetch[0]
					if not rangePre_to_fetch is None:
						ranges_to_fetch.append(rangePre_to_fetch)
					if not rangePost_to_fetch is None:
						ranges_to_fetch.append(rangePost_to_fetch)
			else:
				try:
					ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, start, end, interval, h_interval_opens, weeklyUseYahooDef=True, minDistanceThreshold=5)
					if ranges_to_fetch is None:
						ranges_to_fetch = []
				except yfcd.NoIntervalsInRangeException:
					ranges_to_fetch = []
				except:
					print("Ticker =",self.ticker)
					raise
			# Prune ranges in future:
			for i in range(len(ranges_to_fetch)-1, -1, -1):
				x = ranges_to_fetch[i][0]
				if isinstance(x,(datetime.datetime, pd.Timestamp)) and x>dt_now:
					if debug:
						print("- deleting future range:", ranges_to_fetch[i])
					del ranges_to_fetch[i]
				elif datetime.datetime.combine(x, datetime.time(0),tzinfo=tz_exchange) > dt_now:
					if debug:
						print("- deleting future range:", ranges_to_fetch[i])
					del ranges_to_fetch[i]
			# Important that ranges_to_fetch in reverse order!
			ranges_to_fetch.sort(key=lambda x:x[0], reverse=True)
			if debug:
				print("- ranges_to_fetch:")
				pprint(ranges_to_fetch)

			interval_td = yfcd.intervalToTimedelta[interval]
			if len(ranges_to_fetch) > 0:
				if h.shape[0]>0:
					# Ensure only one range max is after cached data:
					h_last_dt = h.index[-1].to_pydatetime()
					if not isinstance(ranges_to_fetch[0][0], datetime.datetime):
						h_last_dt = h_last_dt.astimezone(tz_exchange).date()
					n = 0
					for r in ranges_to_fetch:
						if r[0] > h_last_dt:
							n += 1
					if n > 1:
						print("ranges_to_fetch:")
						pprint(ranges_to_fetch)
						raise Exception("ranges_to_fetch contains {} ranges that occur after h_last_dt={}, expected 1 max".format(n, h_last_dt))

				# last_adjust_d = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
				quiet = not period is None # YFC generated date range so don't print message
				if debug:
					quiet = False
				# quiet = not debug
				if interval == yfcd.Interval.Days1:
					h = self._fetchAndAddRanges_contiguous(h, pstr, interval, ranges_to_fetch, prepost, proxy, kwargs, quiet=quiet)
					h_lastAdjustD = h.index[-1].date()
				else:
					h = self._fetchAndAddRanges_sparse(h, pstr, interval, ranges_to_fetch, prepost, proxy, kwargs, quiet=quiet)
					# h_lastAdjustD = self._getCachedPrices(yfcd.Interval.Days1).index[-1].date()
					h_lastAdjustD = self._history[yfcd.Interval.Days1].index[-1].date()

				## TODO: scan all of 'h' for price outliers, because YF won't catch all e.g. when fetching one row:
				##       1) apply 2d median filter to h split-adjusted
				##       2) use as mask to correct h

		if (h is None) or h.shape[0]==0:
			raise Exception("history() is exiting without price data")

		f_dups = h.index.duplicated()
		if f_dups.any():
			raise Exception("{}: These timepoints have been duplicated: {}".format(self.ticker, h.index[f_dups]))

		# Cache
		self._history[interval] = h
		yfcm.StoreCacheDatum(self.ticker, h_cache_key, self._history[interval])
		if not h_lastAdjustD is None:
			if debug:
				print("- writing LastAdjustD={} to md of {}/{}".format(h_lastAdjustD, self.ticker, h_cache_key))
			yfcm.WriteCacheMetadata(self.ticker, h_cache_key, "LastAdjustD", h_lastAdjustD)

		# Present table for user:
		if (not start is None) and (not end is None):
			f_outside_range = (h.index<pd.Timestamp(start)) | (h.index>=pd.Timestamp(end))
			if f_outside_range.any():
				h = h.drop(h.index[f_outside_range])
		if not keepna:
			price_data_cols = [c for c in yfcd.yf_data_cols if c in h.columns]
			mask_nan_or_zero = (h[price_data_cols].isna()|(h[price_data_cols]==0)).all(axis=1)
			h = h.drop(mask_nan_or_zero.index[mask_nan_or_zero])
		if h.shape[0] == 0:
			return None
		if not actions:
			h = h.drop(["Dividends","Stock Splits"], axis=1)
		else:
			if not "Dividends" in h.columns:
				raise Exception("Dividends column missing from table")
		if adjust_splits:
			for c in ["Open","Close","Low","High","Dividends"]:
				h[c] *= h["CSF"]
			h["Volume"] /= h["CSF"]
		if adjust_divs:
			for c in ["Open","Close","Low","High"]:
				h[c] *= h["CDF"]
		else:
			h["Adj Close"] = h["Close"] * h["CDF"]
		h = h.drop(["CSF","CDF"], axis=1)
		if rounding:
			# Round to 4 sig-figs
			h[["Open","Close","Low","High"]] = np.round(h[["Open","Close","Low","High"]], yfcu.CalculateRounding(h["Close"][h.shape[0]-1], 4))

		if debug:
			print("YFC: history() returning")
			# print(h[["Close","Volume"]])
			cols = [c for c in ["Close","Dividends","Volume","CDF","CSF"] if c in h.columns]
			print(h[cols])
			f = h["Dividends"]!=0.0
			if f.any():
				print("- dividends:")
				print(h.loc[f, cols])
			print("")
		elif self._trace:
			print(" "*self._trace_depth + "YFC: history() returning")
			self._trace_depth -= 1

		return h

	def _fetchYfHistory(self, pstr, interval, start, end, prepost, proxy, kwargs):
		debug = self._debug
		# debug = True

		log_msg = "YFC: {}: _fetchYfHistory(interval={} , pstr={} , start={} , end={})".format(self.ticker, interval, pstr, start, end)
		if debug:
			print("")
			print(log_msg)
		elif self._trace:
			self._trace_depth += 1
			print(" "*self._trace_depth + log_msg)
		# else:
		# 	print(log_msg)

		period = None
		if not pstr is None:
			period = yfcd.periodStrToEnum[pstr]
			if (not start is None) and (not end is None):
				# start/end take precedence over pstr
				# But keep 'period', useful to know
				pstr = None

		exchange = self.info["exchange"]
		tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
		td = yfcd.intervalToTimedelta[interval]
		istr = yfcd.intervalToString[interval]
		interday = interval in [yfcd.Interval.Days1,yfcd.Interval.Week,yfcd.Interval.Months1,yfcd.Interval.Months3]
		td_1d = datetime.timedelta(days=1)

		fetch_start = start
		fetch_end = end
		if not end is None:
			# If 'fetch_end' in future then cap to exchange midnight
			dtnow = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
			dtnow_exchange = dtnow.astimezone(tz_exchange)
			if isinstance(end, datetime.datetime):
				end_dt = end
				end_d = end.astimezone(tz_exchange).date()
			else:
				end_d = end
				end_dt = datetime.datetime.combine(end, datetime.time(0), tz_exchange)
			if isinstance(start, datetime.datetime):
				start_dt = start
				start_d = start.astimezone(tz_exchange).date()
			else:
				start_d = start
				start_dt = datetime.datetime.combine(start, datetime.time(0), tz_exchange)
			if end_dt > dtnow:
				exchange_midnight_dt = datetime.datetime.combine(dtnow_exchange.date()+td_1d, datetime.time(0), tz_exchange)
				if isinstance(end, datetime.datetime):
					fetch_end = exchange_midnight_dt
				else:
					fetch_end = exchange_midnight_dt.date()

			if (not fetch_start is None) and (fetch_end <= fetch_start):
				return None
		if isinstance(start, datetime.datetime) and (start.time() > datetime.time(3)) and (not interday):
			# if start = start time of trading day, then Volume will be incorrect.
			# Need to be 20 minutes earlier to get Yahoo to return correct volume.
			# And rather than lookup trading day start, just shift any datetime
			fetch_start -= datetime.timedelta(minutes=20)
		if interval == yfcd.Interval.Week:
			# Ensure aligned to week start:
			fetch_start -= datetime.timedelta(days=fetch_start.weekday())

		if interday:
			if interval == yfcd.Interval.Days1:
				listing_date_check_tol = datetime.timedelta(days=7)
			elif interval == yfcd.Interval.Week:
				listing_date_check_tol = datetime.timedelta(days=14)
			elif interval == yfcd.Interval.Months1:
				listing_date_check_tol = datetime.timedelta(days=35)
			elif interval == yfcd.Interval.Months3:
				listing_date_check_tol = datetime.timedelta(days=35*3)

		if debug:
			if not pstr is None:
				print("YFC: {}: fetching {} period".format(self.ticker, pstr))
			else:
				if (not isinstance(fetch_start,datetime.datetime)) or fetch_start.time() == datetime.time(0):
					start_str = fetch_start.strftime("%Y-%m-%d")
				else:
					start_str = fetch_start.strftime("%Y-%m-%d %H:%M:%S")
				if (not isinstance(fetch_end,datetime.datetime)) or fetch_end.time() == datetime.time(0):
					end_str = fetch_end.strftime("%Y-%m-%d")
				else:
					end_str = fetch_end.strftime("%Y-%m-%d %H:%M:%S")
				print("YFC: {}: fetching {} {} -> {}".format(self.ticker, yfcd.intervalToString[interval], start_str, end_str))

		first_fetch_failed=False ; ex=None
		df=None
		try:
			if debug:
				print("- fetch_start={} ; fetch_end={}".format(fetch_start, fetch_end))
			df = self.dat.history(period=pstr, 
								interval=istr, 
								start=fetch_start, end=fetch_end, 
								prepost=prepost, 
								actions=True, # Always fetch
								keepna=True, 
								auto_adjust=False, # store raw data, adjust myself
								back_adjust=False, # store raw data, adjust myself
								proxy=proxy, 
								rounding=False, # store raw data, round myself
								tz=None, # store raw data, localize myself
								raise_errors=True,
								kwargs=kwargs)
			if debug:
				if df is None:
					print("- YF returned None")
				else:
					print("- YF returned table:")
					print(df[["Close","Dividends","Volume"]])
			if not fetch_start is None:
				if isinstance(fetch_start, (datetime.datetime,pd.Timestamp)):
					df = df[df.index>=fetch_start]
				else:
					df = df[df.index.date>=fetch_start]
		except Exception as e:
			first_fetch_failed = True
			if "Data doesn't exist for startDate" in str(e):
				ex = yfcd.NoPriceDataInRangeException(self.ticker,istr,start,end)
			elif "No data found for this date range" in str(e):
				ex = yfcd.NoPriceDataInRangeException(self.ticker,istr,start,end)
			else:
				raise e

		fetch_dt_utc = datetime.datetime.utcnow()

		second_fetch_failed=False
		if interday:
			df_backup = df
			if first_fetch_failed and (not fetch_end is None):
				# Try with wider date range, maybe entire range is just before listing date
				if debug:
					print("- retrying YF fetch with wider date range")

				fetch_start -= 2*listing_date_check_tol
				fetch_end += 2*listing_date_check_tol
				if debug:
					print("- first fetch failed, trying again with longer range: {} -> {}".format(fetch_start, fetch_end))
				try:
					df = self.dat.history(period=pstr, 
										interval=istr, 
										start=fetch_start, end=fetch_end, 
										prepost=prepost, 
										actions=True, # Always fetch
										keepna=True, 
										auto_adjust=False, # store raw data, adjust myself
										back_adjust=False, # store raw data, adjust myself
										proxy=proxy, 
										rounding=False, # store raw data, round myself
										tz=None, # store raw data, localize myself
										raise_errors=True,
										kwargs=kwargs)
				except Exception as e:
					if "Data doesn't exist for startDate" in str(e):
						second_fetch_failed = True
					elif "No data found for this date range" in str(e):
						second_fetch_failed = True
					else:
						raise e

				if debug:
					print("- second fetch returned:")
					print(df)

			if first_fetch_failed:
				if second_fetch_failed:
					# Hopefully code never comes here
					raise ex
				else:
					# Requested date range was just before stock listing date, 
					# but wider range crosses over so can continue
					pass

			# Detect listing day
			found_listing_day=False
			listing_day=None
			if df.shape[0]>0:
				if pstr == "max":
					found_listing_day = True
				else:
					tol = listing_date_check_tol
					if not fetch_start is None:
						fetch_start_d = fetch_start.date() if isinstance(fetch_start, datetime.datetime) else fetch_start
						if (df.index[0].date() - fetch_start_d) > tol:
							# Yahoo returned data starting significantly after requested start date, indicates
							# request is before stock listed on exchange
							found_listing_day = True
					else:
						start_expected = yfct.DtSubtractPeriod(fetch_dt_utc.date()+td_1d, yfcd.periodStrToEnum[pstr])
						if interval == yfcd.Interval.Week:
							start_expected -= datetime.timedelta(days=start_expected.weekday())
						if (df.index[0].date() - start_expected) > tol:
							found_listing_day = True
				if debug:
					print("- found_listing_day = {}".format(found_listing_day))
				if found_listing_day:
					listing_day = df.index[0].date()
					if debug:
						print("YFC: inferred listing_date = {}".format(listing_day))
					yfcm.StoreCacheDatum(self.ticker, "listing_date", listing_day)

				if (not listing_day is None) and first_fetch_failed:
					if end <= listing_day:
						# Aha! Requested date range was entirely before listing
						if debug:
							print("- requested date range was before listing date")
						return None

			df = df_backup

			# Check that weekly aligned to Monday. If not, shift start date back 
			# and re-fetch
			if interval==yfcd.Interval.Week and df.shape[0]>0 and (df.index[0].weekday()!=0):
				# Despite fetch_start aligned to Monday, sometimes Yahoo returns weekly 
				# data starting a different day. Shifting back a little fixes
				fetch_start -= datetime.timedelta(days=2)
				#
				# fetch_start -= datetime.timedelta(days=fetch_start.weekday())
				if debug:
					print("- weekly data not aligned to Monday, re-fetching from {}".format(fetch_start))
				df = self.dat.history(period=pstr, interval=istr, 
									start=fetch_start, end=fetch_end, 
									prepost=prepost, actions=True, keepna=True, 
									auto_adjust=False, back_adjust=False, 
									proxy=proxy, 
									rounding=False, # store raw data, round myself
									tz=None, # store raw data, localize myself
									raise_errors=True,
									kwargs=kwargs)

				if interval==yfcd.Interval.Week and (df.index[0].weekday()!=0):
					print("Date range requested: {} -> {}".format(fetch_start, fetch_end))
					print(df)
					raise Exception("Weekly data returned by YF doesn't begin Monday but {}".format(df.index[0].weekday()))

		if (not df is None) and (df.index.tz is not None) and (not isinstance(df.index.tz, ZoneInfo)):
			# Convert to ZoneInfo
			df.index = df.index.tz_convert(tz_exchange)

		if debug:
			if df is None:
				print("YFC: YF returned None")
			else:
				# pass
				print("YFC: YF returned table:")
				print(df[["Close","Dividends","Volume"]])

		if pstr is None:
			if df is None:
				received_interval_starts = None
			else:
				if interday:
					received_interval_starts = df.index.date
				else:
					received_interval_starts = df.index.to_pydatetime()
			try:
				intervals_missing_df = yfct.IdentifyMissingIntervals(exchange, start, end, interval, received_interval_starts)
			except yfcd.NoIntervalsInRangeException:
				intervals_missing_df = None
			# Remove missing intervals today:
			if (intervals_missing_df is not None) and intervals_missing_df.shape[0] > 0:
				## If very few intervals and not today (so Yahoo should have data), 
				## then assume no trading occurred and insert NaN rows.
				## Normally Yahoo has already filled with NaNs but sometimes they forget/are late
				nm = intervals_missing_df.shape[0]
				if (interday and nm==1) or ((not interday) and nm<=2):
					if debug:
						print("- found missing intervals, inserting nans:")
						print(intervals_missing_df)
					df_missing = pd.DataFrame(data={k:[np.nan]*nm for k in yfcd.yf_data_cols}, index=intervals_missing_df["open"])
					df_missing.index = pd.to_datetime(df_missing.index)
					if interday:
						df_missing.index = df_missing.index.tz_localize(tz_exchange)
					for c in ["Volume","Dividends","Stock Splits"]:
						df_missing[c] = 0
					if df is None:
						df = df_missing
					else:
						df = pd.concat([df,df_missing], sort=True).sort_index()
						df.index = pd.to_datetime(df.index, utc=True).tz_convert(tz_exchange)

		## Improve tolerance to calendar missing a recent new holiday:
		if (df is None) or df.shape[0]==0:
			return None

		n = df.shape[0]

		fetch_dt = fetch_dt_utc.replace(tzinfo=ZoneInfo("UTC"))

		df = self._repairZeroPrices(df, interval)
		df = self._repairUnitMixups(df, interval)

		if (n > 0) and (pstr is None):
			## Remove any out-of-range data:
			## NOTE: YF has a bug-fix pending merge: https://github.com/ranaroussi/yfinance/pull/1012
			if not end is None:
				if interday:
					df = df[df.index.date < end_d]
				else:
					df = df[df.index < end_dt]
				n = df.shape[0]
			#
			# And again for pre-start data:
			if not start is None:
				if interday:
					df = df[df.index.date >= start_d]
				else:
					df = df[df.index >= start_dt]
				n = df.shape[0]

		if n == 0:
			raise yfcd.NoPriceDataInRangeException(self.ticker,istr,start,end)
		else:
			## Verify that all datetimes match up with actual intervals:
			if interday:
				if not (df.index.time == datetime.time(0)).all():
					print(df)
					raise Exception("Interday data contains times in index")
				intervalStarts = df.index.date
			else:
				intervalStarts = df.index.to_pydatetime()
			#
			intervals = yfct.GetTimestampCurrentInterval_batch(exchange, intervalStarts, interval, weeklyUseYahooDef=True)
			f_na = intervals["interval_open"].isna().values
			if f_na.any():
				if not interday:
					## For some exchanges (e.g. JSE) Yahoo returns intraday timestamps right on market close. Remove them.
					df2 = df.copy() ; df2["_date"] = df2.index.date ; df2["_intervalStart"] = df2.index
					sched = yfct.GetExchangeSchedule(exchange, df2["_date"].min(), df2["_date"].max()+td_1d)
					rename_cols = {"open":"market_open","close":"market_close"}
					sched.columns = [rename_cols[c] if c in rename_cols else c for c in sched.columns]
					sched_df = sched.copy()
					sched_df["_date"] = sched_df.index.date
					df2 = df2.merge(sched_df, on="_date", how="left")
					f_drop = (df2["Volume"]==0).values & ((df2["_intervalStart"]==df2["market_close"]).values)
					if f_drop.any():
						if debug:
							print("- dropping 0-volume rows starting at market close")
						intervalStarts = intervalStarts[~f_drop]
						intervals = intervals[~f_drop]
						df = df[~f_drop]
						n = df.shape[0]
						f_na = intervals["interval_open"].isna().values
				if f_na.any():
					## For some national holidays when exchange closed, Yahoo fills in row. Clue is 0 volume.
					## Solution = drop:
					f_na_zeroVol = f_na & (df["Volume"]==0).values
					if f_na_zeroVol.any():
						if debug:
							print("- dropping {} 0-volume rows with no matching interval".format(sum(f_na_zeroVol)))
						f_drop = f_na_zeroVol
						intervalStarts = intervalStarts[~f_drop]
						intervals = intervals[~f_drop]
						df = df[~f_drop]
						n = df.shape[0]
						f_na = intervals["interval_open"].isna().values
					## TODO ... another clue is row is identical to previous trading day
					if f_na.any():
						f_drop = np.array([False]*n)
						for i in np.where(f_na)[0]:
							if i > 0:
								dt = df.index[i]
								last_dt = df.index[i-1]
								if (df.loc[dt,yfcd.yf_data_cols] == df.loc[last_dt,yfcd.yf_data_cols]).all():
									f_drop[i] = True
						if f_drop.any():
							if debug:
								print("- dropping rows with no interval that are identical to previous row")
							intervalStarts = intervalStarts[~f_drop]
							intervals = intervals[~f_drop]
							df = df[~f_drop]
							n = df.shape[0]
							f_na = intervals["interval_open"].isna().values
				if f_na.any():
					ctr = 0
					for idx in np.where(f_na)[0]:
						dt = df.index[idx]
						ctr += 1
						if ctr < 10:
							print("Failed to map: {} (exchange={}, xcal={})".format(dt, exchange, yfcd.exchangeToXcalExchange[exchange]))
							print(df.loc[dt])
						elif ctr == 10:
							print("- stopped printing at 10 failures")
					raise Exception("Problem with dates returned by Yahoo, see above")

		df = df.copy()

		lastDataDts = yfct.CalcIntervalLastDataDt_batch(exchange, intervalStarts, interval)
		data_final = fetch_dt >= lastDataDts
		df["Final?"] = data_final

		df["FetchDate"] = pd.Timestamp(fetch_dt_utc).tz_localize("UTC")

		if debug:
			print(df)
			print("_fetchYfHistory() returning")
		elif self._trace:
			print(" "*self._trace_depth + "_fetchYfHistory() returning")
			self._trace_depth -= 1

		return df

	def _getCachedPrices(self, interval):
		if isinstance(interval, str):
			if not interval in yfcd.intervalStrToEnum.keys():
				raise Exception("'interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
			interval = yfcd.intervalStrToEnum[interval]

		istr = yfcd.intervalToString[interval]

		h = None

		if yfcm.IsDatumCached(self.ticker, "history-"+istr):
			h = yfcm.ReadCacheDatum(self.ticker, "history-"+istr)

		if not h is None and h.shape[0]==0:
			h = None

		return h

	def _fetchAndAddRanges_contiguous(self, h, pstr, interval, ranges_to_fetch, prepost, proxy, kwargs, quiet=False):
		# Fetch each range, appending/prepending to cached data
		if (ranges_to_fetch is None) or len(ranges_to_fetch) == 0:
			return h

		debug = self._debug
		# debug = True

		if debug:
			print("_fetchAndAddRanges_contiguous()")
			print("- ranges_to_fetch:")
			pprint(ranges_to_fetch)
		elif self._trace:
			self._trace_depth += 1
			print(" "*self._trace_depth + "_fetchAndAddRanges_contiguous()")

		tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
		if h.shape[0]==0:
			h_first_dt = None ; h_last_dt = None
		else:
			h_first_dt = h.index[0].to_pydatetime()
			h_last_dt = h.index[-1].to_pydatetime()
			if not isinstance(ranges_to_fetch[0][0], datetime.datetime):
				h_first_dt = h_first_dt.astimezone(tz_exchange).date()
				h_last_dt = h_last_dt.astimezone(tz_exchange).date()
		td_1d = datetime.timedelta(days=1)
		istr = yfcd.intervalToString[interval]

		# Because data should be contiguous, then ranges should meet some conditions:
		if len(ranges_to_fetch) > 2:
			pprint(ranges_to_fetch)
			raise Exception("For contiguous data generated {}>2 ranges".format(len(ranges_to_fetch)))
		if h.shape[0]==0 and len(ranges_to_fetch)>1:
			raise Exception("For contiguous data generated {} ranges, but h is empty".format(len(ranges_to_fetch)))
		range_pre = None ; range_post = None
		if h.shape[0]==0 and len(ranges_to_fetch)==1:
			range_pre = ranges_to_fetch[0]
		else:
			n_pre=0 ; n_post=0
			for r in ranges_to_fetch:
				if r[0] > h_last_dt:
					n_post += 1
					range_post = r
				elif r[0] < h_first_dt:
					n_pre += 1
					range_pre = r
			if n_pre > 1:
				pprint(ranges_to_fetch)
				raise Exception("For contiguous data generated {}>1 ranges before h_first_dt".format(n_pre))
			if n_post > 1:
				pprint(ranges_to_fetch)
				raise Exception("For contiguous data generated {}>1 ranges after h_last_dt".format(n_post))

		h2_pre = None ; h2_post = None
		if not range_pre is None:
			r = range_pre
			check_for_listing = False
			try:
				h2_pre = self._fetchYfHistory(pstr, interval, r[0], r[1], prepost, proxy, kwargs)
			except yfcd.NoPriceDataInRangeException:
				if interval == yfcd.Interval.Days1 and r[1]-r[0]==td_1d:
					## If only trying to fetch 1 day of 1d data, then print warning instead of exception.
					## Could add additional condition of dividend previous day (seems to mess up table).
					if not quiet:
						print("WARNING: No {}-price data fetched for ticker {} between dates {} -> {}".format(yfcd.intervalToString[interval], self.ticker, r[0], r[1]))
					h2_pre = None
				elif (range_post is None) and (r[1]-r[0] < td_1d*7) and (r[1]-r[0] > td_1d*3):
					## Small date range, potentially trying to fetch before listing data
					check_for_listing = True
					h2_pre = None
				else:
					raise

			if check_for_listing:
				df = None
				try:
					df = self._fetchYfHistory(pstr, interval, r[0], r[1]+td_1d*7, prepost, proxy, kwargs)
				except:
					# Discard
					pass
				if not df is None:
					# Then the exception above occurred because trying to fetch before listing dated!
					yfcm.StoreCacheDatum(self.ticker, "listing_date", h.index[0].date())
				else:
					# Then the exception above was genuine and needs to be propagated
					raise yfcd.NoPriceDataInRangeException(self.ticker,istr,r[0],r[1])
		if not range_post is None:
			r = range_post
			try:
				h2_post = self._fetchYfHistory(pstr, interval, r[0], r[1], prepost, proxy, kwargs)
			except yfcd.NoPriceDataInRangeException:
				## If only trying to fetch 1 day of 1d data, then print warning instead of exception.
				## Could add additional condition of dividend previous day (seems to mess up table).
				if interval == yfcd.Interval.Days1 and r[1]-r[0]==td_1d:
					if not quiet:
						print("WARNING: No {}-price data fetched for ticker {} between dates {} -> {}".format(yfcd.intervalToString[interval], self.ticker, r[0], r[1]))
					h2_post = None
				else:
					raise

		if not h2_post is None:
			## UPDATE: only need duplicate check for noncontiguous data
			# ## If a timepoint is in both h and h2, drop from h. This is possible because of 
			# ## threshold in IdentifyMissingIntervalRanges(), allowing re-fetch of cached data 
			# ## if it reduces total number of web requests
			# f_duplicate = h.index.isin(h2_pre.index)
			# h = h[~f_duplicate]

			# De-adjust the new data, and backport any new events in cached data
			# Note: Yahoo always returns split-adjusted price, so reverse it

			if debug:
				print("- appending new data")

			# Simple append to bottom of table
			# 1) adjust h2_post
			h2_post = self._processYahooAdjustment(h2_post, interval)
			if debug:
				print("- h2_post:")
				print(h2_post)

			# 2) backport h2_post splits across entire h table
			h2_csf = yfcu.GetCSF0(h2_post)
			if h2_csf != 1.0:
				if debug:
					print("- backporting new data CSF={} across cached".format(h2_csf))
				h["CSF"] *= h2_csf
				if not h2_pre is None:
					h2_pre["CSF"] *= h2_csf

			# 2) backport h2_post divs across entire h table
			h2_cdf = yfcu.GetCDF0(h2_post)
			if h2_cdf != 1.0:
				if debug:
					print("- backporting new data CDF={} across cached".format(h2_cdf))
				h["CDF"] *= h2_cdf
				# Note: don't need to backport across h2_pre because already 
				#       contains dividend adjustment (via 'Adj Close')

			try:
				h = pd.concat([h, h2_post])
			except:
				print(self.ticker)
				print("h:")
				print(h.iloc[h.shape[0]-10:])
				print("h2_post:")
				print(h2_post)
				raise

		if not h2_pre is None:
			if debug:
				print("- prepending new data")

			# Simple prepend to top of table
			if h.shape[0]==0:
				post_csf = 1.0
			else:
				post_csf = yfcu.GetCSF0(h)
			h2_pre = self._processYahooAdjustment(h2_pre, interval, post_csf=post_csf)

			try:
				h = pd.concat([h, h2_pre])
			except:
				print(self.ticker)
				print("h:")
				print(h.iloc[h.shape[0]-10:])
				print("h2_pre:")
				print(h2_pre)
				raise

		h.index = pd.to_datetime(h.index, utc=True).tz_convert(tz_exchange)
		h = h.sort_index()

		if debug:
			print("- h:")
			print(h)
			print("_fetchAndAddRanges_contiguous() returning")
		elif self._trace:
			print(" "*self._trace_depth + "_fetchAndAddRanges_contiguous() returning")
			self._trace_depth -= 1

		return h

	def _fetchAndAddRanges_sparse(self, h, pstr, interval, ranges_to_fetch, prepost, proxy, kwargs, quiet=False):
		# Fetch each range, but can be careless regarding de-adjust because
		# getting events from the carefully-managed daily data
		if (ranges_to_fetch is None) or len(ranges_to_fetch) == 0:
			return h

		debug = self._debug
		# debug = True

		if debug:
			print("_fetchAndAddRanges_sparse()")
		elif self._trace:
			self._trace_depth += 1
			print(" "*self._trace_depth + "_fetchAndAddRanges_sparse()")

		tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
		td_1d = datetime.timedelta(days=1)

		h_cache_key = "history-"+yfcd.intervalToString[interval]
		h_lastAdjustD = yfcm.ReadCacheMetadata(self.ticker, h_cache_key, "LastAdjustD")
		if h_lastAdjustD is None:
			raise Exception("h_lastAdjustD is None")

		h_lastAdjustDt = datetime.datetime.combine(h_lastAdjustD+td_1d, datetime.time(0), tzinfo=tz_exchange)

		# Calculate cumulative adjustment factors for events that occurred since last adjustment, 
		# and apply to h:
		dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
		first_day_since_adjust = yfct.GetTimestampNextInterval(self.info["exchange"], h_lastAdjustDt, yfcd.Interval.Days1)["interval_open"]
		if first_day_since_adjust > dt_now.astimezone(tz_exchange).date():
			cdf=1.0 ; csf=1.0
		else:
			if debug:
				print("- first_day_since_adjust = {}".format(first_day_since_adjust))
			df_since = self.history(start=first_day_since_adjust, interval=yfcd.Interval.Days1, max_age=td_1d, auto_adjust=False)
			if df_since is None:
				cdf=1.0 ; csf=1.0
			else:
				df_since = self._getCachedPrices(interval) ; df_since = df_since[df_since.index.date>=first_day_since_adjust]
				if df_since.shape[0]==0:
					cdf=1.0 ; csf=1.0
				else:
					cdf = yfcu.GetCDF0(df_since)
					csf = yfcu.GetCSF0(df_since)

		# Backport adjustment factors to h:
		h["CSF"] *= csf
		h["CDF"] *= cdf

		# Ensure have daily data covering all ranges_to_fetch, so they can be de-splitted
		r_start_earliest = ranges_to_fetch[0][0]
		for rstart,rend in ranges_to_fetch:
			r_start_earliest = min(rstart, r_start_earliest)
		r_start_earliest_d = r_start_earliest.date() if isinstance(r_start_earliest, datetime.datetime) else r_start_earliest
		if debug:
			print("- r_start_earliest = {}".format(r_start_earliest))
		df_daily = self.history(start=r_start_earliest_d, interval=yfcd.Interval.Days1, max_age=td_1d)

		# Fetch each range, and adjust for splits that occurred after
		for rstart,rend in ranges_to_fetch:
			if debug:
				print("- fetching {} -> {}".format(rstart, rend))
			try:
				h2 = self._fetchYfHistory(pstr, interval, rstart, rend, prepost, proxy, kwargs)
			except yfcd.NoPriceDataInRangeException:
				## If only trying to fetch 1 day of 1d data, then print warning instead of exception.
				## Could add additional condition of dividend previous day (seems to mess up table).
				if interval == yfcd.Interval.Days1 and r[1]-r[0]==td_1d:
					if not quiet:
						print("WARNING: No {}-price data fetched for ticker {} between dates {} -> {}".format(yfcd.intervalToString[interval], self.ticker, r[0], r[1]))
					h2 = None
					continue
				else:
					raise

			if h2 is None:
				raise Exception("YF returned None for: tkr={}, interval={}, start={}, end={}".format(self.ticker, interval, rstart, rend))

			# Ensure h2 is split-adjusted. Sometimes Yahoo returns unadjusted data
			h2 = self._processYahooAdjustment(h2, interval)
			if debug:
				print("- h2 adjusted:")
				print(h2[["Close","Dividends","Volume","CSF","CDF"]])

			h = pd.concat([h, h2])
			h.index = pd.to_datetime(h.index, utc=True).tz_convert(tz_exchange)

		h = h.sort_index()

		if debug:
			print("_fetchAndAddRanges_sparse() returning")
		elif self._trace:
			print(" "*self._trace_depth + "_fetchAndAddRanges_sparse() returning")
			self._trace_depth -= 1

		return h

	def _processYahooAdjustment(self, df, interval, post_csf=None):
		# Yahoo returns data split-adjusted so reverse that.
		#
		# Except for hourly/minute data, Yahoo isn't consistent with adjustment:
		# - prices only split-adjusted if date range contains a split
		# - dividend appears split-adjusted
		# Easy to fix using daily data:
		# - divide prices by daily to determine if split-adjusted
		# - copy dividends from daily

		# Finally, add 'CSF' & 'CDF' columns to allow cheap on-demand adjustment

		if not isinstance(df, pd.DataFrame):
			raise Exception("'df' must be pd.DataFrame not {}".format(type(df)))
		if not isinstance(interval, yfcd.Interval):
			raise Exception("'interval' must be yfcd.Interval not {}".format(type(interval)))
		if (not post_csf is None) and not isinstance(post_csf, (float,int,np.int64)):
			raise Exception("'post_csf' if set must be scalar numeric not {}".format(type(post_csf)))

		debug = False
		debug = self._debug
		# debug = True

		if debug:
			print("")
			print("_processYahooAdjustment(interval={}, post_csf={}), {}->{}".format(interval, post_csf, df.index[0], df.index[-1]))
			print(df[["Close","Dividends","Volume"]])
		elif self._trace:
			self._trace_depth += 1
			print(" "*self._trace_depth + "_processYahooAdjustment(interval={}, post_csf={}), {}->{}".format(interval, post_csf, df.index[0], df.index[-1]))

		cdf = None
		csf = None

		n = df.shape[0]

		# Step 1: ensure intraday price data is always split-adjusted
		interday = interval in [yfcd.Interval.Days1,yfcd.Interval.Week,yfcd.Interval.Months1,yfcd.Interval.Months3]
		td_1d = datetime.timedelta(days=1)
		td_7d = datetime.timedelta(days=7)
		if not interday:
			# Get daily price data during and after 'df'
			df_daily = self.history(start=df.index[0].date(), interval=yfcd.Interval.Days1, adjust_divs=False)
			f_post = df_daily.index.date > df.index[-1].date()
			df_daily_during = df_daily[~f_post].copy()
			df_daily_post = df_daily[f_post].copy()
			df_daily_during_d = df_daily_during.copy()
			df_daily_during_d.index = df_daily_during_d.index.date ; df_daily_during_d.index.name = "_date"

			# Also get raw daily data from cache
			df_daily_raw = self._history[yfcd.Interval.Days1] ; df_daily_raw=df_daily_raw[df_daily_raw.index.date>=df.index[0].date()]
			f_post = df_daily_raw.index.date > df.index[-1].date()
			df_daily_raw_during = df_daily_raw[~f_post].copy()
			df_daily_raw_post = df_daily_raw[f_post].copy()
			df_daily_raw_during_d = df_daily_raw_during.copy()
			df_daily_raw_during_d.index = df_daily_raw_during_d.index.date ; df_daily_raw_during_d.index.name = "_date"

			if df_daily_post.shape[0] == 0:
				csf_post = 1.0
			else:
				csf_post = yfcu.GetCSF0(df_daily_post)
			expectedRatio = 1.0/csf_post

			# Merge 'df' with daily data to compare and infer adjustment
			df_aggByDay = df.copy()
			df_aggByDay["_date"] = df_aggByDay.index.date
			df_aggByDay = df_aggByDay.groupby("_date").agg(
				Low=("Low", "min"),
				High=("High", "max"),
				Open=("Open", "first"),
				Close=("Close", "last"))
			data_cols = ["Open","Close","Low","High"]
			df2 = pd.merge(df_aggByDay, df_daily_during_d, how="left", on="_date", validate="one_to_one", suffixes=("","_day"))
			## If 'df' has not been split-adjusted by Yahoo, but it should have been, 
			## then the inferred split-adjust ratio should be close to 1.0/post_csf.
			## Apply a few sanity tests against inferred ratio - not NaN, low variance
			df3 = df2[~df2["Close"].isna()]
			if df3.shape[0]==0:
				ss_ratio = expectedRatio
				stdev_pct = 0.0
			elif df.shape[0]==1:
				ss_ratio = df2["Close"].iloc[0]/df2["Close_day"].iloc[0]
				stdev_pct = 0.0
			else:
				ratios = df2[data_cols].values/df2[[dc+"_day" for dc in data_cols]].values
				ratios[df2[data_cols].isna()] = 1.0
				ss_ratio = np.mean(ratios)
				stdev_pct = np.std(ratios)/ss_ratio
			#
			if stdev_pct > 0.05:
				cols_to_print = []
				for dc in data_cols:
					df2[dc+"_r"] = df2[dc]/df2[dc+"_day"]
					cols_to_print.append(dc)
					cols_to_print.append(dc+"_day")
					cols_to_print.append(dc+"_r")
				print(df2[cols_to_print])
				raise Exception("STDEV % of estimated stock-split ratio is {}%, should be near zero".format(round(stdev_pct*100, 1)))

			if abs(1.0-ss_ratio/expectedRatio)>0.05:
				cols_to_print = []
				for dc in data_cols:
					df2[dc+"_r"] = df2[dc]/df2[dc+"_day"]
					cols_to_print.append(dc)
					cols_to_print.append(dc+"_day")
					cols_to_print.append(dc+"_r")
				print(df2[cols_to_print])
				raise Exception("ss_ratio={} != expected_ratio={}".format(ss_ratio, expectedRatio))
			ss_ratio = expectedRatio
			ss_ratioRcp = 1.0/ss_ratio
			#
			price_data_cols = ["Open","Close","Adj Close","Low","High"]
			if ss_ratio > 1.01:
				for c in price_data_cols:
					df[c] *= ss_ratioRcp
				if debug:
					print("Applying 1:{.2f} stock-split".format(ss_ratio))
			elif ss_ratioRcp > 1.01:
				for c in price_data_cols:
					df[c] *= ss_ratio
				if debug:
					print("Applying {.2f}:1 reverse-split-split".format(ss_ratioRcp))
			# Note: volume always returned unadjusted

			# Yahoo messes up dividend adjustment too so copy correct dividend from daily, 
			# but only to first time periods of each day:
			df = df.drop("Dividends",axis=1)
			df["_date"] = df.index.date
			# - get first times
			df["_time"] = df.index.time
			df_openTimes = df[["_date","_time"]].groupby("_date",as_index=False,group_keys=False).min().rename(columns={"_time":"_open_time"})
			df = df.drop("_time",axis=1)
			# - merge
			df["_indexBackup"] = df.index
			df = pd.merge(df, df_daily_during_d[["Dividends"]], how="left", on="_date", validate="many_to_one")
			df = pd.merge(df, df_openTimes, how="left", on="_date")
			df.index=df["_indexBackup"] ; df.index.name=None
			# - correct dividends
			df.loc[df.index.time!=df["_open_time"], "Dividends"] = 0.0
			df = df.drop("_open_time",axis=1)
			# Copy over CSF and CDF too from daily
			df = pd.merge(df, df_daily_raw_during_d[["CDF","CSF"]], how="left", on="_date", validate="many_to_one")
			df.index=df["_indexBackup"] ; df.index.name=None ; df=df.drop(["_indexBackup","_date"],axis=1)
			cdf = df["CDF"]
			df["Adj Close"] = df["Close"]*cdf
			csf = df["CSF"]

			if df_daily_post.shape[0] > 0:
				post_csf = yfcu.GetCSF0(df_daily_post)

		elif interval == yfcd.Interval.Week:
			df_daily = self.history(start=df.index[-1].date()+td_7d, interval=yfcd.Interval.Days1)
			if (not df_daily is None) and df_daily.shape[0] > 0:
				post_csf = yfcu.GetCSF0(df_daily)
				if debug:
					print("- post_csf of daily date range {}->{} = {}".format(df_daily.index[0], df_daily.index[-1], post_csf))

		elif interval in [yfcd.Interval.Months1,yfcd.Interval.Months3]:
			raise Exception("not implemented")

		if debug:
			print("- post_csf =",post_csf)

		# If 'df' does not contain all stock splits until present, then
		# set 'post_csf' to cumulative stock split factor just after last 'df' date
		last_dt = df.index[-1]
		dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
		thr = 5
		if interval == yfcd.Interval.Week:
			thr = 10 # Extend threshold for weekly data
		if (dt_now-last_dt) > datetime.timedelta(days=thr):
			if post_csf is None:
				raise Exception("Data is older than {} days, need to set 'post_csf' arg to capture all stock splits since".format(thr))

		# Cumulative dividend factor
		if cdf is None:
			f_nna = ~df["Close"].isna()
			if not f_nna.any():
				cdf = 1.0
			else:
				cdf = np.full(df.shape[0], np.nan)
				cdf[f_nna] = df.loc[f_nna,"Adj Close"] / df.loc[f_nna,"Close"]
				cdf = pd.Series(cdf).fillna(method="bfill").fillna(method="ffill").values
		
		# Cumulative stock-split factor
		if csf is None:
			ss = df["Stock Splits"].copy()
			ss[(ss==0.0)|ss.isna()] = 1.0
			ss_rcp = 1.0/ss
			csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True).shift(-1, fill_value=1.0)
			if not post_csf is None:
				csf *= post_csf
		csf_rcp = 1.0/csf

		# Reverse Yahoo's split adjustment:
		data_cols = ["Open","High","Low","Close","Dividends"]
		for dc in data_cols:
			df[dc] = df[dc] * csf_rcp
		if not interday:
			# Don't need to de-split volume data because Yahoo always returns interday volume unadjusted
			pass
		else:
			df["Volume"] *= csf

		# Drop 'Adj Close', replace with scaling factors:
		df = df.drop("Adj Close",axis=1)
		df["CSF"] = csf
		df["CDF"] = cdf

		if debug:
			print("- unadjusted:")
			print(df[["Close","Dividends","Volume","CSF","CDF"]])
			f = df["Dividends"]!=0.0
			if f.any():
				print("- dividends:")
				print(df.loc[f, ["Close","Dividends","Volume","CSF","CDF"]])
			print("")

		if debug:
			print("_processYahooAdjustment() returning")
			print(df[["Close","Dividends","Volume","CSF"]])
		elif self._trace:
			print(" "*self._trace_depth + "_processYahooAdjustment() returning")

		return df
		
	def _reconstructInterval(self, df_row, interval, bad_fields):
		if isinstance(df_row, pd.DataFrame) or not isinstance(df_row, pd.Series):
			raise Exception("'df_row' must be a Pandas Series not", type(df_row))
		if not isinstance(bad_fields, (list,set,np.ndarray)):
			raise Exception("'bad_fields' must be a list/set not", type(bad_fields))

		idx = df_row.name
		start = idx.date()

		if self._trace:
			self._trace_depth += 1
			print(" "*self._trace_depth + "_reconstructInterval(interval={}, idx={}, bad_fields={})".format(interval, idx, bad_fields))

		data_cols = [c for c in ["Open","High","Low","Close","Adj Close"] if c in df_row.index]

		# If interval is weekly then can construct with daily. But if smaller intervals then 
		# restricted to recent times:
		# - daily = hourly restricted to last 730 days
		sub_interval = None
		td_range = None
		if interval == yfcd.Interval.Week:
			# Correct by fetching week of daily data
			sub_interval = yfcd.Interval.Days1
			td_range = datetime.timedelta(days=7)
		elif interval == yfcd.Interval.Days1:
			# Correct by fetching day of hourly data
			sub_interval = yfcd.Interval.Hours1
			td_range = datetime.timedelta(days=1)
		else:
			print("WARNING: Have not implemented repair for '{}' interval. Contact developers".format(interval))
			return df_row

		if sub_interval==yfcd.Interval.Hours1 and (datetime.date.today()-start) > datetime.timedelta(days=729):
			# Don't bother requesting more price data, Yahoo will reject
			return None
		else:
			new_vals = {}

			if sub_interval==yfcd.Interval.Hours1:
				df_fine = self.history(start=start, end=start+td_range, interval=sub_interval, adjust_splits=True, adjust_divs=False)
			else:
				df_fine = self.history(start=start-td_range, end=start+td_range, interval=sub_interval, adjust_splits=True, adjust_divs=False)

			# First, check whether df_fine has different split-adjustment than df_row.
			# If it is different, then adjust df_fine to match df_row
			good_fields = list(set(data_cols)-set(bad_fields)-set("Adj Close"))
			if len(good_fields)==0:
				raise Exception("No good fields, so cannot determine whether different split-adjustment. Contact developers")
			# median = df_row.loc[good_fields].median()
			# median_fine = np.median(df_fine[good_fields].values)
			# ratio = median/median_fine
			# Better method to calculate split-adjustment:
			df_fine_from_idx = df_fine[df_fine.index>=idx]
			ratios = []
			for f in good_fields:
				if f=="Low":
					ratios.append(df_row[f] / df_fine_from_idx[f].min())
				elif f=="High":
					ratios.append(df_row[f] / df_fine_from_idx[f].max())
				elif f=="Open":
					ratios.append(df_row[f] / df_fine_from_idx[f].iloc[0])
				elif f=="Close":
					ratios.append(df_row[f] / df_fine_from_idx[f].iloc[-1])
			ratio = np.mean(ratios)
			#
			ratio_rcp = round(1.0/ratio, 1) ; ratio = round(ratio, 1)
			if ratio==1 and ratio_rcp==1:
				# Good!
				pass
			else:
				if ratio>1:
					# data has different split-adjustment than fine-grained data
					# Adjust fine-grained to match
					df_fine[data_cols] *= ratio
				elif ratio_rcp>1:
					# data has different split-adjustment than fine-grained data
					# Adjust fine-grained to match
					df_fine[data_cols] *= 1.0/ratio_rcp

			if sub_interval != yfcd.Interval.Hours1:
				df_last_week = df_fine[df_fine.index<idx]
				df_fine = df_fine[df_fine.index>=idx]

			if "High" in bad_fields:
				new_vals["High"] = df_fine["High"].max()
			if "Low" in bad_fields:
				new_vals["Low"] = df_fine["Low"].min()
			if "Open" in bad_fields:
				if sub_interval != yfcd.Interval.Hours1 and idx != df_fine.index[0]:
					# Exchange closed Monday. In this case, Yahoo sets Open to last week close
					new_vals["Open"] = df_last_week["Close"][-1]
					if "Low" in new_vals:
						new_vals["Low"] = min(new_vals["Open"], new_vals["Low"])
					elif new_vals["Open"] < df_row["Low"]:
						new_vals["Low"] = new_vals["Open"]
				else:
					new_vals["Open"] = df_fine["Open"].iloc[0]
			if "Close" in bad_fields:
				new_vals["Close"] = df_fine["Close"].iloc[-1]
				# Assume 'Adj Close' also corrupted, easier than detecting whether true
				new_vals["Adj Close"] = df_fine["Adj Close"].iloc[-1]

		if self._trace:
			self._trace_depth += 1
			print(" "*self._trace_depth + "_reconstructInterval() returning")

		return new_vals

	def _repairUnitMixups(self, df, interval):
		# Sometimes Yahoo returns few prices in cents/pence instead of $/£
		# I.e. 100x bigger
		# Easy to detect and fix, just look for outliers = ~100x local median

		if df.shape[0] == 0:
			return df
		if df.shape[0] == 1:
			# Need multiple rows to confidently identify outliers
			return df

		if self._trace:
			self._trace_depth += 1
			print(" "*self._trace_depth + "_repairUnitMixups(interval={})".format(interval))

		df2 = df.copy()

		# Only import scipy if users actually want function. To avoid
		# adding it to dependencies.
		from scipy import ndimage as _ndimage

		data_cols = ["High","Open","Low","Close"]  # Order important, separate High from Low
		data_cols = [c for c in data_cols if c in df2.columns]
		median = _ndimage.median_filter(df2[data_cols].values, size=(3,3), mode='wrap')

		if (median==0).any():
			print("Ticker =",self.ticker)
			print("yf =",yf)
			print("df:")
			print(df)
			raise Exception("median contains zeroes, why?")
		ratio = df2[data_cols].values/median
		ratio_rounded = (ratio/20).round()*20 # round ratio to nearest 20
		f = (ratio_rounded)==100

		# Store each mixup:
		mixups = {}
		for j in range(len(data_cols)):
			fj = f[:,j]
			if fj.any():
				dc = data_cols[j]
				for i in np.where(fj)[0]:
					idx = df2.index[i]
					if not idx in mixups:
						mixups[idx] = {"data":df2.loc[idx,data_cols], "fields":set([dc])}
					else:
						mixups[idx]["fields"].add(dc)
		n_mixups = len(mixups)

		if len(mixups) > 0:
			# This first pass will correct all errors in Open/Close/AdjClose columns.
			# It will also attempt to correct Low/High columns, but only if can get price data.
			for idx in sorted(list(mixups.keys())):
				m = mixups[idx]
				new_values = self._reconstructInterval(df2.loc[idx], interval, m["fields"])
				if not new_values is None:
					for k in new_values:
						df2.loc[idx, k] = new_values[k]
					del mixups[idx]

			# This second pass will *crudely* "fix" any remaining errors in High/Low
			# simply by ensuring they don't contradict e.g. Low = 100x High
			if len(mixups)>0:
				for idx in sorted(list(mixups.keys())):
					m = mixups[idx]
					row = df2.loc[idx,["Open","Close"]]
					if "High" in m["fields"]:
						df2.loc[idx,"High"] = row.max()
						m["fields"].remove("High")
					if "Low" in m["fields"]:
						df2.loc[idx,"Low"] = row.min()
						m["fields"].remove("Low")

					if len(m["fields"])==0:
						del mixups[idx]

			n_fixed = n_mixups - len(mixups)
			print("{}: fixed {} currency unit mixups in {} price data".format(self.ticker, n_fixed, interval))
			if len(mixups)>0:
				print("    ... and failed to correct {}".format(len(mixups)))

		if self._trace:
			print(" "*self._trace_depth + "_repairUnitMixups() returning")
			self._trace_depth -= 1

		return df2

	def _repairZeroPrices(self, df, interval):
		# Sometimes Yahoo returns prices=0 when obviously wrong e.g. Volume>0 and Close>0.
		# Easy to detect and fix

		if df.shape[0] == 0:
			return df
		if df.shape[0] == 1:
			# Need multiple rows to confidently identify outliers
			return df

		if self._trace:
			self._trace_depth += 1
			print(" "*self._trace_depth + "_repairZeroPrices(interval={}, date_range={}->{})".format(interval, df.index[0], df.index[-1]))

		df2 = df.copy()

		data_cols = ["Open","High","Low","Close"]
		data_cols = [c for c in data_cols if c in df2.columns]
		f_zeroes = (df2[data_cols]==0.0).values.any(axis=1)

		n_fixed = 0
		for i in np.where(f_zeroes)[0]:
			idx = df2.index[i]
			df_row = df2.loc[idx]
			bad_fields = df2.columns[df_row.values==0.0].values
			new_values = self._reconstructInterval(df2.loc[idx], interval, bad_fields)
			if not new_values is None:
				for k in new_values:
					df2.loc[idx, k] = new_values[k]
				n_fixed += 1

		if n_fixed>0:
			print("{}: fixed {} price=0.0 errors in {} price data".format(self.ticker, n_fixed, interval))

		if self._trace:
			print(" "*self._trace_depth + "_repairZeroPrices() returning")
			self._trace_depth -= 1

		return df2

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
			if self._yf_lag:
				return self._yf_lag

		# ## Have to calculate lag from YF data.
		# ## To avoid circular logic will call YF directly, not use my cache. Because cache requires knowing lag.
		# dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
		# if not yfct.IsTimestampInActiveSession(self.info["exchange"], dt_now):
		# 	## Exchange closed so used hardcoded delay, ...
		# 	self._yf_lag = yfcd.exchangeToYfLag[self.info["exchange"]]

		# 	## ... but only until next session starts +1H:
		# 	s = yfct.GetTimestampNextSession(self.info["exchange"], dt_now)
		# 	expiry = s["open"] + datetime.timedelta(hours=1)

		# 	yfcm.StoreCacheDatum(exchange_str, "yf_lag", self._yf_lag, expiry=expiry)
		# 	return self._yf_lag

		# ## Calculate actual delay from live market data, and cache with expiry in 4 weeks

		# specified_lag = yfcd.exchangeToYfLag[self.info["exchange"]]

		# ## Because some stocks go days without any volume, need to 
		# ## be sure today has volume
		# start_d = dt_now.date()-datetime.timedelta(days=7)
		# end_d = dt_now.date()+datetime.timedelta(days=1)
		# df_1d = self.dat.history(interval="1d", start=start_d, end=end_d)
		# start_d = df_1d.index[-1].date()
		# if start_d != dt_now.date():
		# 	self._yf_lag = specified_lag
		# 	return self._yf_lag

		# ## Get last hour of 5m price data:
		# start_dt = dt_now-datetime.timedelta(hours=1)
		# try:
		# 	df_5mins = self.dat.history(interval="5m", start=start_dt, end=dt_now, raise_errors=True)
		# 	df_5mins = df_5mins[df_5mins["Volume"]>0]
		# except:
		# 	df_5mins = None
		# if (df_5mins is None) or (df_5mins.shape[0] == 0):
		# 	# raise Exception("Failed to fetch 5m data for tkr={}, start={}".format(self.ticker, start_dt))
		# 	# print("WARNING: Failed to fetch 5m data for tkr={} so setting yf_lag to hardcoded default".format(self.ticker, start_dt))
		# 	self._yf_lag = specified_lag
		# 	return self._yf_lag
		# df_5mins_lastDt = df_5mins.index[df_5mins.shape[0]-1].to_pydatetime()
		# df_5mins_lastDt = df_5mins_lastDt.astimezone(ZoneInfo("UTC"))

		# ## Now 15 minutes of 1m price data around the last 5m candle:
		# dt2_start = df_5mins_lastDt - datetime.timedelta(minutes=10)
		# dt2_end = df_5mins_lastDt + datetime.timedelta(minutes=5)
		# df_1mins = self.dat.history(interval="1m", start=dt2_start, end=dt2_end, raise_errors=True)
		# dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
		# df_1mins_lastDt = df_1mins.index[df_1mins.shape[0]-1].to_pydatetime()

		# lag = dt_now - df_1mins_lastDt
		# ## Update: ignore all large lags
		# # if lag > datetime.timedelta(minutes=40):
		# # 	raise Exception("{}: calculated YF lag as {}, seems excessive".format(self.ticker, lag))
		# if lag < datetime.timedelta(seconds=0):
		# 	print("dt_now = {} (tz={})".format(dt_now, dt_now.tzinfo))
		# 	print("df_1mins:")
		# 	print(df_1mins)
		# 	raise Exception("{}: calculated YF lag as {}, seems negative".format(self.ticker, lag))
		# expiry_td = datetime.timedelta(days=28)
		# if (lag > (2*specified_lag)) and (lag-specified_lag)>datetime.timedelta(minutes=2):
		# 	if df_1mins["Volume"][df_1mins.shape[0]-1] == 0:
		# 		## Ticker has low volume, ignore larger-than-expected lag. Just reduce the expiry, in case tomorrow has more volume
		# 		expiry_td = datetime.timedelta(days=1)
		# 	else:
		# 		#print("df_5mins:")
		# 		#print(df_5mins)
		# 		#raise Exception("{}: calculated YF lag as {}, greatly exceeds the specified lag {}".format(self.ticker, lag, specified_lag))
		# 		self._yf_lag = specified_lag
		# 		return self._yf_lag
		# self._yf_lag = lag
		# yfcm.StoreCacheDatum(exchange_str, "yf_lag", self._yf_lag, expiry=dt_now+expiry_td)
		# return self._yf_lag

		# Just use specified lag
		specified_lag = yfcd.exchangeToYfLag[self.info["exchange"]]
		self._yf_lag = specified_lag
		return self._yf_lag
