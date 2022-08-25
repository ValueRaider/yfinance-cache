import yfinance as yf

from . import yfc_cache_manager as yfcm
from . import yfc_dat as yfcd
from . import yfc_utils as yfcu
from . import yfc_time as yfct

from time import perf_counter
import pandas as pd
import numpy as np
import datetime, time
from zoneinfo import ZoneInfo
import pytz

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

		debug = False
		# debug = True

		if debug:
			print("YFC: history(tkr={}, interval={}, period={}, start={}, end={}, max_age={})".format(self.ticker, interval, period, start, end, max_age))

		td_1d = datetime.timedelta(days=1)
		exchange = self.info['exchange']
		tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
		yfct.SetExchangeTzName(exchange, self.info["exchangeTimezoneName"])

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
				raise Exception("Argument 'start' must be str, date or datetime")#
			if start.tzinfo is None:
				start = start.replace(tzinfo=tz_exchange)
			else:
				start = start.astimezone(tz_exchange)
			if start.dst() is None:
				raise Exception("Argument 'start' tzinfo must be DST-aware")
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

		# 'prepost' not doing anything in yfinance

		if max_age is None:
			if interval == yfcd.Interval.Days1:
				max_age = datetime.timedelta(hours=4)
			elif interval in [yfcd.Interval.Days5, yfcd.Interval.Week]:
				max_age = datetime.timedelta(hours=60)
			else:
				max_age = 0.5*yfcd.intervalToTimedelta[interval]

		if (interval in self._history) and (not self._history[interval] is None):
			h = self._history[interval]
		else:
			h = self._getCachedPrices(interval)
			if not h is None:
				# ## Force fetch if columns missing
				# if not ("Dividends" in h.columns and "Stock Splits" in h.columns):
				# 	h = None
				# elif not "CSF" in h.columns:
				# 	h = None
				self._history[interval] = h
		h_cache_key = "history-"+yfcd.intervalToString[interval]

		# Handle missing dates, or dependencies between date arguments
		dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
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
				# sched = sched[sched["market_open"]<=dt_now]
				sched = sched[(sched["market_open"]+self.yf_lag)<=dt_now]
				if debug:
					print("- sched:")
					print(sched)
					print(sched["market_open"][-1].tz_convert("Europe/London"))
				last_open_day = sched["market_open"][-1].date()
				if debug:
					print("- last_open_day = {}".format(last_open_day))
				end = datetime.datetime.combine(last_open_day+td_1d, datetime.time(0), tz_exchange)
				if debug:
					print("- end = {}".format(end))
				end_d = end.date()
				if period == yfcd.Period.Max:
					start = datetime.datetime.combine(datetime.date(yfcd.yf_min_year, 1, 1), datetime.time(0), tz_exchange)
				else:
					start = yfct.DtSubtractPeriod(end, period)
				# ctr = 0
				# while not yfct.ExchangeOpenOnDay(exchange, start.date()):
				# 	start -= td_1d
				# 	ctr += 1
				# 	if ctr > 5:
				# 		ctr = -1 ; break
				# if ctr==-1:
				# 	# Search forward instead
				# 	while not yfct.ExchangeOpenOnDay(exchange, start.date()):
				# 		start += td_1d
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

		if ((start_d is None) or (end_d is None)) and (not start is None) and (not end is None):
			# if start_d/end_d not set then start/end are datetimes, so need to inspect
			# schedule opens/closes to determine days
			sched = yfct.GetExchangeSchedule(exchange, start.date(), end.date()+datetime.timedelta(days=1))
			n = sched.shape[0]
			if start < sched["market_open"][0]:
				start_d = start.date()
			else:
				start_d = start.date() +datetime.timedelta(days=1)
			if end >= sched["market_close"][n-1]:
				end_d = end.date()+datetime.timedelta(days=1)
			else:
				end_d = end.date()

		if (not start is None) and (not end is None):
			listing_date = yfcm.ReadCacheDatum(self.ticker, "listing_date")
			if (not listing_date is None) and (not isinstance(listing_date, datetime.date)):
				raise Exception("listing_date = {} ({}) should be a date".format(listing_date, type(listing_date)))
			if not listing_date is None:
				if debug:
					print("- capping start_d={} by listing_date={}".format(start_d, listing_date))
				if start_d < listing_date:
					start_d = listing_date
					start = datetime.datetime.combine(listing_date, datetime.time(0), tz_exchange)

		interday = (interval in [yfcd.Interval.Days1,yfcd.Interval.Days5,yfcd.Interval.Week])

		## Trigger an estimation of Yahoo data delay:
		yf_lag = self.yf_lag

		d_tomorrow = dt_now.astimezone(tz_exchange).date() +datetime.timedelta(days=1)
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
				raise Exception("Fetch of {}->{} failed".format(start, end))

			# Adjust
			if interval == yfcd.Interval.Days1:
				h = yfcu.ReverseYahooBackAdjust(h)
				h_lastAdjustD = h.index[-1].date()
			else:
				h_lastDt = h.index[-1]
				s = yfct.GetTimestampNextSession(exchange, h_lastDt.to_pydatetime())
				if s["market_open"] > dt_now:
					h = yfcu.ReverseYahooBackAdjust(h)
					h_lastAdjustD = h_lastDt.date()
				else:
					# df_daily = self.history(start=h_lastDt.date()+td_1d, interval=yfcd.Interval.Days1, max_age=td_1d, auto_adjust=False)
					next_day = max(h_lastDt.date()+td_1d, s["market_open"].date())
					df_daily = self.history(start=next_day, interval=yfcd.Interval.Days1, max_age=td_1d, auto_adjust=False)
					if (df_daily is None) or (df_daily.shape[0]==0):
						h = yfcu.ReverseYahooBackAdjust(h)
						h_lastAdjustD = h_lastDt.date()
					else:
						ss = df_daily["Stock Splits"].copy()
						ss[ss==0.0] = 1.0
						ss_rcp = 1.0/ss
						csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True).shift(-1, fill_value=1.0)
						post_csf = csf[0]
						if debug:
							print("- post_csf = {}".format(post_csf))
						h = yfcu.ReverseYahooBackAdjust(h, post_csf=post_csf)
						h_lastAdjustD = df_daily.index[-1].date()
			if debug:
				print("- h_lastAdjustD = {}".format(h_lastAdjustD))

		else:
			## Performance TODO: only check expiry on datapoints not marked 'final'
			## - need to improve 'expiry check' performance, is 3-4x slower than fetching from YF

			if not "CSF" in h.columns:
				raise Exception("{}: Cached price data missing 'CSF' column, need to flush cache".format(self.ticker))

			n = h.shape[0]
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
				h_interval_dt = h_interval_dts[n-1]
				fetch_dt = yfct.ConvertToDatetime(h["FetchDate"][n-1], tz=tz_exchange)
				last_expired = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, max_age, exchange, interval, yf_lag=self.yf_lag)
				if last_expired:
					# Drop last row because expired
					h = h[0:n-1]
					h_interval_dts = h_interval_dts[0:n-1]
			else:
				expired = np.array([False]*n)
				f_final = h["Final?"].values
				for idx in np.where(~f_final)[0]:
					h_interval_dt = h_interval_dts[idx]
					fetch_dt = yfct.ConvertToDatetime(h["FetchDate"][idx], tz=tz_exchange)
					expired[idx] = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, max_age, exchange, interval, yf_lag=self.yf_lag)
				if expired.any():
					h = h[~expired]
					h_interval_dts = h_interval_dts[~expired]
			#
			## Potential perf improvement: tag rows as fully contiguous to avoid searching for gaps
			# h_intervals = np.array([yfct.GetTimestampCurrentInterval(exchange, idt, interval, weeklyUseYahooDef=True) for idt in h_interval_dts])
			h_intervals = yfct.GetTimestampCurrentInterval_batch(exchange, h_interval_dts, interval, weeklyUseYahooDef=True)
			f_na = h_intervals == None
			if f_na.any():
				print(h)
				raise Exception("Bad rows found in prices table")
				if debug:
					print("- found bad rows, deleting:")
					print(h[f_na])
				h = h[~f_na]
				h_intervals = h_intervals[~f_na]
			h_interval_opens = [x["interval_open"] for x in h_intervals]

			if interval == yfcd.Interval.Days1:
				# Ensure that daily data always up-to-date to now
				h_start = h_intervals[0]["interval_open"]
				h_end = h_intervals[-1]["interval_close"]
				if not isinstance(h_start, datetime.datetime):
					h_start = datetime.datetime.combine(h_start, datetime.time(0), tz_exchange)
					h_end = datetime.datetime.combine(h_end, datetime.time(0), tz_exchange)
				if debug:
					print("- h_end = {}".format(h_end))
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
				try:
					rangePost_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, h_end, dt_now+datetime.timedelta(days=1), interval, None, weeklyUseYahooDef=True, minDistanceThreshold=5)
				except yfcd.NoIntervalsInRangeException:
					rangePost_to_fetch = None
				if not rangePost_to_fetch is None:
					if len(rangePost_to_fetch) > 1:
						raise Exception("Expected only one element in rangePost_to_fetch[], but = {}".format(rangePost_to_fetch))
					rangePost_to_fetch = rangePost_to_fetch[0]
				if debug:
					print("- rangePre_to_fetch:  {}".format(rangePre_to_fetch))
					print("- rangePost_to_fetch: {}".format(rangePost_to_fetch))
				ranges_to_fetch = []
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
			# Important that ranges_to_fetch in reverse order!
			if debug:
				print("- ranges_to_fetch:")
				pprint(ranges_to_fetch)
			ranges_to_fetch.sort(key=lambda x:x[0], reverse=True)

			interval_td = yfcd.intervalToTimedelta[interval]
			if len(ranges_to_fetch) > 0:
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
				if interval == yfcd.Interval.Days1:
					h = self._fetchAndAddRanges_contiguous(h, pstr, interval, ranges_to_fetch, prepost, proxy, kwargs, quiet=quiet)
					h_lastAdjustD = h.index[-1].date()
				else:
					h = self._fetchAndAddRanges_sparse(h, pstr, interval, ranges_to_fetch, prepost, proxy, kwargs, quiet=quiet)
					# h_lastAdjustD = self._getCachedPrices(yfcd.Interval.Days1).index[-1].date()
					h_lastAdjustD = self._history[yfcd.Interval.Days1].index[-1].date()

		if h is None:
			raise Exception("history() is exiting without price data")

		# Cache
		self._history[interval] = h
		yfcm.StoreCacheDatum(self.ticker, h_cache_key, self._history[interval])
		if not h_lastAdjustD is None:
			yfcm.WriteCacheMetadata(self.ticker, h_cache_key, "LastAdjustD", h_lastAdjustD)

		# Present table for user:
		if (not start is None) and (not end is None):
			h = h[np.logical_and(h.index>=pd.Timestamp(start), h.index<pd.Timestamp(end))].copy()
		if not keepna:
			h = h[~h["Close"].isna()].copy()
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
		h = h.drop(["CSF","CDF"], axis=1)
		if rounding:
			# Round to 4 sig-figs
			h[["Open","Close","Low","High"]] = np.round(h[["Open","Close","Low","High"]], yfcu.CalculateRounding(h["Close"][h.shape[0]-1], 4))

		return h

	def _fetchYfHistory(self, pstr, interval, start, end, prepost, proxy, kwargs):
		debug = False
		# debug = True

		if debug:
			print("YFC: _fetchYfHistory(pstr={} , start={} , end={})".format(pstr, start, end))

		period = None
		if not pstr is None:
			period = yfcd.periodStrToEnum[pstr]
			if (not start is None) and (not end is None):
				# start/end take precedence over pstr
				# But keep 'period', useful to know
				pstr = None

		exchange = self.dat.info["exchange"]
		tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
		istr = yfcd.intervalToString[interval]
		interday = (interval in [yfcd.Interval.Days1,yfcd.Interval.Days5,yfcd.Interval.Week])
		td_1d = datetime.timedelta(days=1)

		if not end is None:
			dtnow = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
			dtnow_exchange = dtnow.astimezone(tz_exchange)
			if isinstance(end, datetime.datetime):
				end_dt = end
				end_d = end.astimezone(tz_exchange).date()
			else:
				end_d = end
				end_dt = datetime.datetime.combine(end, datetime.time(0), tz_exchange)
			if end_dt > dtnow:
				# Cap 'end' to exchange midnight
				exchange_midnight_dt = datetime.datetime.combine(dtnow_exchange.date()+datetime.timedelta(days=1), datetime.time(0), tz_exchange)
				if isinstance(end, datetime.datetime):
					end = exchange_midnight_dt
				else:
					end = exchange_midnight_dt.date()
			if (not start is None) and (end <= start):
				return None

		if debug:
			if not pstr is None:
				print("YFC: {}: fetching {} period".format(self.ticker, pstr))
			else:
				if (not isinstance(start,datetime.datetime)) or start.time() == datetime.time(0):
					start_str = start.strftime("%Y-%m-%d")
				else:
					start_str = start.strftime("%Y-%m-%d %H:%M:%S")
				if (not isinstance(end,datetime.datetime)) or end.time() == datetime.time(0):
					end_str = end.strftime("%Y-%m-%d")
				else:
					end_str = end.strftime("%Y-%m-%d %H:%M:%S")
				print("YFC: {}: fetching {} {} -> {}".format(self.ticker, yfcd.intervalToString[interval], start_str, end_str))

		try:
			df = self.dat.history(period=pstr, 
								interval=istr, 
								start=start, end=end, 
								prepost=prepost, 
								actions=True, # Always fetch
								keepna=True, 
								auto_adjust=False, # store raw data, adjust myself
								back_adjust=False, # store raw data, adjust myself
								proxy=proxy, 
								rounding=False, # store raw data, round myself
								tz=None, # store raw data, localize myself
								kwargs=kwargs)
		except Exception as e:
			se = str(e)
			if "Data doesn't exist for startDate" in se:
				raise yfcd.NoPriceDataInRangeException(self.ticker,istr,start,end)
			elif "No data found for this date range" in se:
				# If date range is one day, then create NaN rows and ignore error
				if interday and (pstr is None):
					sched = yfct.GetExchangeSchedule(exchange, start, end)
					if sched.shape[0] == 1:
						df = pd.DataFrame(data={k:[np.nan] for k in yfcd.yf_data_cols}, index=[pd.Timestamp(sched["market_open"][0].date()).tz_localize(self.info["exchangeTimezoneName"])])
						for c in ["Volume","Dividends","Stock Splits"]:
							df[c] = 0
					else:
						raise e
				else:
					raise e
			else:
				raise e

		fetch_dt_utc = datetime.datetime.utcnow()

		found_listing_day = False
		if pstr == "max":
			found_listing_day = True
		elif yfcd.intervalToTimedelta[interval] >= datetime.timedelta(days=1):
			if not start is None:
				start_d = start.date() if isinstance(start, datetime.datetime) else start
				if (df.index[0].date() - start_d) > datetime.timedelta(days=14):
					# Yahoo returned data starting significantly after requested start date, indicates
					# request is before stock listed on exchange
					found_listing_day = True
			else:
				start_expected = yfct.DtSubtractPeriod(fetch_dt_utc.date()+td_1d, yfcd.periodStrToEnum[pstr])
				if (df.index[0].date() - start_expected) > datetime.timedelta(days=14):
					found_listing_day = True
		if found_listing_day:
			if debug:
				print("YFC: inferred listing_date = {}".format(df.index[0].date()))
			yfcm.StoreCacheDatum(self.ticker, "listing_date", df.index[0].date())

		n = df.shape[0]
		if n == 0 and (pstr is None) and (end-start) < datetime.timedelta(days=7):
			## If a very short date range was requested, it is possible that 
			## for each day no volume occurred. In this scenario Yahoo returns nothing.
			## To solve, slightly extend the date range, then Yahoo will return empty rows
			## for the 0-volume days.
			start2 = start - datetime.timedelta(days=3)
			end2 = end + datetime.timedelta(days=3)
			df = self.dat.history(period=pstr, 
								interval=istr, 
								start=start2, end=end2, 
								prepost=prepost, 
								actions=True,
								keepna=True, 
								auto_adjust=False,
								back_adjust=False,
								proxy=proxy, 
								rounding=False,
								tz=None,
								kwargs=kwargs)
			fetch_dt_utc = datetime.datetime.utcnow()
			if interday:
				df = df[(df.index.date >= start) & (df.index.date < end)]
			else:
				df = df[(df.index >= start) & (df.index < end)]
			n = df.shape[0]
		fetch_dt = fetch_dt_utc.replace(tzinfo=ZoneInfo("UTC"))

		if debug:
			print("YFC: YF returned table:")
			# with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.precision', 3, ):
			print(df)

		if n > 0 and pstr is None:
			## Remove any out-of-range data:
			## NOTE: YF has a bug-fix pending merge: https://github.com/ranaroussi/yfinance/pull/1012
			end_dt = end if isinstance(end,datetime.datetime) else datetime.datetime.combine(end, datetime.time(0), tz_exchange)
			drop_last = False
			if interday:
				end_d = end_dt.astimezone(tz_exchange).date()
				drop_last = df.index[-1].date() > end_d
			else:
				drop_last = df.index[-1] > end_dt
			if drop_last:
				df = df[0:n-1]
				n -= 1
			#
			# And again for pre-start data:
			start_dt = start if isinstance(start,datetime.datetime) else datetime.datetime.combine(start, datetime.time(0), tz_exchange)
			drop_first = False
			if interday:
				start_d = start_dt.astimezone(tz_exchange).date()
				drop_first = df.index[0].date() < start_d
			else:
				drop_first = df.index[0] < start_dt
			if drop_first:
				df = df[1:n]
				n -= 1

		if n == 0:
			## Shouldn't need to reconstruct with 'keepna' argument!
			raise yfcd.NoPriceDataInRangeException(self.ticker,istr,start,end)
			##
			## TODO: Yahoo sometimes missing 1d data around dividend dates. Can fix by
			##       fetching hourly data and aggregating.
			##       - Verify reconstruction against actual 1d data. May need pre/post dat.
			##		 - Live test with tkr JSE.L on 2022-06-17.
			print("- reconstructing empty df")
			##
			## Sometimes data missing because Yahoo simply doesn't have trades on that day
			## e.g. low-volume stocks on Toronto exchange. Need to prevent spamming by 
			## creating row of NaNs with FetchDate.
			##
			## Note: discussion on Github about keeping NaN rows, so I shouldn't need to refill myself
			##
			print("- end = {}".format(end))
			intervals = yfct.GetExchangeScheduleIntervals(exchange, interval, start, end)
			print("- intervals:")
			print(intervals)
			n = len(intervals)
			intervalStarts = [x[0] for x in intervals]
			d = {c:[pd.NaT]*n for c in ["Open","Close","Adj Close","Low","High","Volume","Dividends","Stock Splits"]}
			d["Volume"]=[0]*n ; d["Dividends"]=[0]*n ; d["Stock Splits"]=[0]*n
			# df = pd.DataFrame(data=d, index=intervalStarts)
			intervalStarts_pd = [pd.Timestamp(x, tz=self.info["exchangeTimezoneName"]) for x in intervalStarts]
			df = pd.DataFrame(data=d, index=intervalStarts_pd)
			if interday:
				df.index.name = "Date"
			else:
				df.index.name = "Datetime"
			print("- rebuilt df:")
			print(df)
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
			f_na = intervals==None
			if f_na.any():
				if not interday:
					## For some exchanges (e.g. JSE) Yahoo returns intraday timestamps right on market close. Remove them.
					df2 = df.copy() ; df2["_date"] = df2.index.date ; df2["_intervalStart"] = df2.index
					sched = yfct.GetExchangeSchedule(exchange, df2["_date"].min(), df2["_date"].max()+td_1d)
					sched_df = sched
					sched_df["_date"] = sched_df.index.date
					df2 = df2.merge(sched_df, on="_date", how="left")
					f_drop = (df2["Volume"]==0).values & ((df2["_intervalStart"]<df2["market_open"]).values | (df2["_intervalStart"]>=df2["market_close"]).values)
					if f_drop.any():
						intervalStarts = intervalStarts[~f_drop]
						intervals = intervals[~f_drop]
						df = df[~f_drop]
						n = df.shape[0]
						f_na = intervals==None
				if f_na.any():
					## For some national holidays when exchange closed, Yahoo fills in row. Clue is 0 volume.
					## Solution = drop:
					f_na_zeroVol = f_na & (df["Volume"]==0)
					if f_na_zeroVol.any():
						f_drop = f_na_zeroVol
						intervalStarts = intervalStarts[~f_drop]
						intervals = intervals[~f_drop]
						df = df[~f_drop]
						n = df.shape[0]
						f_na = intervals==None
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
							intervalStarts = intervalStarts[~f_drop]
							intervals = intervals[~f_drop]
							df = df[~f_drop]
							n = df.shape[0]
							f_na = intervals==None
				if f_na.any():
					ctr = 0
					for idx in np.where(f_na)[0]:
						dt = df.index[idx]
						ctr += 1
						if ctr < 10:
							print("Failed to map: {} (exchange{}, xcal={})".format(dt, exchange, yfcd.exchangeToXcalExchange[exchange]))
						elif ctr == 10:
							print("- stopped printing at 10 failures")
					raise Exception("Problem with dates returned by Yahoo, see above")
			interval_closes = np.array([i["interval_close"] for i in intervals])

		lastDataDts = yfct.CalcIntervalLastDataDt_batch(exchange, intervalStarts, interval)
		data_final = fetch_dt >= lastDataDts
		df["Final?"] = data_final

		df["FetchDate"] = pd.Timestamp(fetch_dt_utc).tz_localize("UTC")

		if debug:
			print("_fetchYfHistory() returning")

		return df

	def _getCachedPrices(self, interval):
		if isinstance(interval, str):
			if not interval in yfcd.intervalStrToEnum.keys():
				raise Exception("'interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
			interval = yfcd.intervalStrToEnum[interval]

		istr = yfcd.intervalToString[interval]

		h = None

		# if not self._history is None:
		# 	if interval in self._history.keys():
		# 		h = self._history[interval]
		# if (h is None) and yfcm.IsDatumCached(self.ticker, "history-"+istr):
		# 	self._history[interval] = yfcm.ReadCacheDatum(self.ticker, "history-"+istr)
		# 	h = self._history[interval]

		if yfcm.IsDatumCached(self.ticker, "history-"+istr):
			h = yfcm.ReadCacheDatum(self.ticker, "history-"+istr)

		return h

	def _fetchAndAddRanges_contiguous(self, h, pstr, interval, ranges_to_fetch, prepost, proxy, kwargs, quiet=False):
		# Fetch each range, appending/prepending to cached data
		if (ranges_to_fetch is None) or len(ranges_to_fetch) == 0:
			return h

		debug = False
		# debug = True

		if debug:
			print("_fetchAndAddRanges_contiguous()")
			print("- ranges_to_fetch:")
			pprint(ranges_to_fetch)

		tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
		h_first_dt = h.index[0].to_pydatetime()
		h_last_dt = h.index[-1].to_pydatetime()
		if not isinstance(ranges_to_fetch[0][0], datetime.datetime):
			h_first_dt = h_first_dt.astimezone(tz_exchange).date()
			h_last_dt = h_last_dt.astimezone(tz_exchange).date()
		td_1d = datetime.timedelta(days=1)

		# Because data should be contiguous, then ranges should meet some conditions:
		if len(ranges_to_fetch) > 2:
			pprint(ranges_to_fetch)
			raise Exception("For contiguous data generated {}>2 ranges".format(len(ranges_to_fetch)))
		n_pre=0 ; n_post=0
		range_pre = None ; range_post = None
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
			try:
				h2_pre = self._fetchYfHistory(pstr, interval, r[0], r[1], prepost, proxy, kwargs)
			except yfcd.NoPriceDataInRangeException:
				## If only trying to fetch 1 day of 1d data, then print warning instead of exception.
				## Could add additional condition of dividend previous day (seems to mess up table).
				if interval == yfcd.Interval.Days1 and r[1]-r[0]==td_1d:
					if not quiet:
						print("WARNING: No {}-price data fetched for ticker {} between dates {} -> {}".format(yfcd.intervalToString[interval], self.ticker, r[0], r[1]))
					h2 = None
				else:
					raise
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
					h2 = None
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

			if debug:
				print("- h2_post:")
				h2_post["A/C"] = h2_post["Adj Close"] / h2_post["Close"]
				with pd.option_context('display.precision', 10):
					print(h2_post[["Open","Close","Adj Close","A/C"]])

			# Simple append to bottom of table
			# 1) adjust h2_post
			h2_post = yfcu.ReverseYahooBackAdjust(h2_post)

			if debug:
				print("- h2_post:")
				print(h2_post[["Open","Close","Dividends","CDF","Stock Splits","CSF"]])

			# 2) backport h2_post splits across entire h table
			h2_csf = h2_post["CSF"][0]
			ss0 = h2_post["Stock Splits"][0]
			if ss0 != 0.0:
				h2_csf *= 1.0/ss0
			if h2_csf != 1.0:
				if debug:
					print("- backporting new data CSF={} across cached".format(h2_csf))
				h["CSF"] *= h2_csf
				if not h2_pre is None:
					h2_pre["CSF"] *= h2_csf

			if debug:
				with pd.option_context('display.precision', 8, ):
					print(h[h.index.date==datetime.date(2022,7,29)][["Open","Close","Dividends","CDF","Stock Splits","CSF"]])

			# 2) backport h2_post divs across entire h table
			h2_cdf = h2_post["CDF"][0]
			div0 = h2_post["Dividends"][0]
			if div0 != 0.0:
				x = h["Close"][-1]
				h2_cdf *= (x-div0)/x
			if h2_cdf != 1.0:
				if debug:
					print("- backporting new data CDF={} across cached".format(h2_cdf))
				# h["CDF"] *= h2_cdf
				# Try manually adjusting each row using formula:
				# - adjust last row of h:
				i = h.shape[0]-1
				close = h["Close"][i]
				adjClose = h2_post["CDF"][0] * (close - h2_post["Dividends"][0])
				cdf = adjClose/close
				h.loc[h.index[i],"CDF"] = cdf
				# - adjust all other rows in h:
				for i in range(h.shape[0]-2, -1, -1):
					close = h["Close"][i]
					adjClose = h["CDF"][i+1] * (h["Close"][i] - h["Dividends"][i+1])
					cdf = adjClose/close
					h.loc[h.index[i],"CDF"] = cdf
				# Note: don't need to backport across h2_pre because already 
				#       contains dividend adjustment (via 'Adj Close')

			if debug:
				with pd.option_context('display.precision', 8, ):
					print(h[h.index.date==datetime.date(2022,7,29)][["Open","Close","Dividends","CDF","Stock Splits","CSF"]])

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
			idx = np.argmax(h.index > h2_pre.index[-1])
			post_csf = h["CSF"][idx]
			ss0 = h["Stock Splits"][idx]
			if ss0 != 0.0:
				post_csf *= 1.0/ss0
			h2_pre = yfcu.ReverseYahooBackAdjust(h2_pre, post_csf=post_csf)

			try:
				h = pd.concat([h, h2_pre])
			except:
				print(self.ticker)
				print("h:")
				print(h.iloc[h.shape[0]-10:])
				print("h2_pre:")
				print(h2_pre)
				raise

		h = h.sort_index()
		if debug:
			print("- last row:")
			n = h.shape[0]
			print(h.iloc[n-1:n][["Open","Close","Volume","Dividends"]])

		return h

	def _fetchAndAddRanges_sparse(self, h, pstr, interval, ranges_to_fetch, prepost, proxy, kwargs, quiet=False):
		# Fetch each range, but can be careless regarding de-adjust because
		# getting events from the carefully-managed daily data
		if (ranges_to_fetch is None) or len(ranges_to_fetch) == 0:
			return h

		debug = False
		# debug = True

		if debug:
			print("_fetchAndAddRanges_sparse()")
			print("- ranges_to_fetch:")
			pprint(ranges_to_fetch)

		tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
		h_last_dt = h.index[-1].to_pydatetime()
		h_last_d = h_last_dt.date()
		if not isinstance(ranges_to_fetch[0][0], datetime.datetime):
			h_last_dt = h_last_dt.astimezone(tz_exchange).date()

		h_cache_key = "history-"+yfcd.intervalToString[interval]
		h_lastAdjustD = yfcm.ReadCacheMetadata(self.ticker, h_cache_key, "LastAdjustDt")
		if h_lastAdjustD is None:
			raise Exception("h_lastAdjustD is None")

		# Calculate cumulative adjustment factors for events that occurred since last adjustment, 
		# and apply to h:
		# first_day_since_adjust = yfct.GetTimestampNextSession(self.info["exchange"], h_lastAdjustD)["market_open"].date()
		first_day_since_adjust = h_lastAdjustD + datetime.timedelta(days=1)
		dtnow = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
		if first_day_since_adjust > dtnow:
			csf = 1.0
			csf = 1.0
		else:
			# Actually need 1 day of overlap to get correct dividend adjustment factor - 
			# in case first new row has dividend, see how Yahoo adjust instead of calculating myself
			start = first_day_since_adjust - timedelta(days=1)
			while not yfct.ExchangeOpenOnDay(self.info["exchange"], start):
				start -= timedelta(days=1)
			df_since = self.history(start=start, interval=yfcd.Interval.Days1, max_age=datetime.timedelta(days=1), auto_adjust=False)
			# f = df_since.index.date==start
			# df_overlap = df_since[f]
			# if df_overlap.shape[0] == 0:
			# 	raise Exception("df_overlap is empty")
			# df_since = df_since[~f]
			df_overlap = df_since.iloc[0]
			df_since = df_since.iloc[1:]
			ss = df_since["Stock Splits"].copy() ; ss[ss==0.0] = 1.0
			csf = (1.0/ss).sort_index(ascending=False).cumprod().sort_index(ascending=True)
			ss0 = ss[0]
			if ss0 != 0.0:
				csf *= 1.0/ss0
			cdf = df_overlap["Adj Close"]/df_overlap["Close"]

		# Backport adjustment factors to h:
		h["CSF"] *= csf
		h["CDF"] *= cdf

		# Ensure have daily data covering all ranges_to_fetch, so they can be de-splitted
		r_start_earliest = ranges_to_fetch[0][0]
		for rstart,rend in ranges_to_fetch:
			r_start_earliest = min(rstart, r_start_earliest)
		r_start_earliest_d = r_start_earliest.date() if isinstance(r_start_earliest, datetime) else r_start_earliest_d
		df_daily = self.history(start=r_start_earliest, interval=yfcd.Interval.Days1, max_age=datetime.timedelta(days=1))

		# Fetch each range, and adjust for splits that occurred after
		for rstart,rend in ranges_to_fetch:
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

				# Get CSF on first day after this range:
				idx = np.argmax(df_daily.index > h2.index[-1])
				post_csf = df_daily["CSF"][idx]
				ss0 = df_daily["Stock Splits"][idx]
				if ss0 != 0.0:
					post_csf *= 1.0/ss0

				# De-adjust h2 (using splits data from df_daily)
				h2 = yfcu.ReverseYahooBackAdjust(h2, post_csf=post_csf)

			try:
				h = pd.concat([h, h2])
			except:
				print(self.ticker)
				print("h:")
				print(h.iloc[h.shape[0]-10:])
				print("h2:")
				print(h2)
				raise

		h = h.sort_index()
		return h

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

		specified_lag = yfcd.exchangeToYfLag[self.info["exchange"]]

		## Because some stocks go days without any volume, need to 
		## be sure today has volume
		start_d = dt_now.date()-datetime.timedelta(days=7)
		end_d = dt_now.date()+datetime.timedelta(days=1)
		df_1d = self.dat.history(interval="1d", start=start_d, end=end_d)
		start_d = df_1d.index[-1].date()
		if start_d != dt_now.date():
			self._yf_lag = specified_lag
			return self._yf_lag

		## Get last hour of 5m price data:
		start_dt = dt_now-datetime.timedelta(hours=1)
		try:
			df_5mins = self.dat.history(interval="5m", start=start_dt, end=dt_now)
			df_5mins = df_5mins[df_5mins["Volume"]>0]
		except:
			df_5mins = None
		if (df_5mins is None) or (df_5mins.shape[0] == 0):
			# raise Exception("Failed to fetch 5m data for tkr={}, start={}".format(self.ticker, start_dt))
			# print("WARNING: Failed to fetch 5m data for tkr={} so setting yf_lag to hardcoded default".format(self.ticker, start_dt))
			self._yf_lag = specified_lag
			return self._yf_lag
		df_5mins_lastDt = df_5mins.index[df_5mins.shape[0]-1].to_pydatetime()
		df_5mins_lastDt = df_5mins_lastDt.astimezone(ZoneInfo("UTC"))

		## Now 15 minutes of 1m price data around the last 5m candle:
		dt2_start = df_5mins_lastDt - datetime.timedelta(minutes=10)
		dt2_end = df_5mins_lastDt + datetime.timedelta(minutes=5)
		df_1mins = self.dat.history(interval="1m", start=dt2_start, end=dt2_end)
		dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
		df_1mins_lastDt = df_1mins.index[df_1mins.shape[0]-1].to_pydatetime()

		lag = dt_now - df_1mins_lastDt
		if lag > datetime.timedelta(minutes=40):
			raise Exception("{}: calculated YF lag as {}, seems excessive".format(self.ticker, lag))
		if lag < datetime.timedelta(seconds=0):
			print("dt_now = {} (tz={})".format(dt_now, dt_now.tzinfo))
			print("df_1mins:")
			print(df_1mins)
			raise Exception("{}: calculated YF lag as {}, seems negative".format(self.ticker, lag))
		expiry_td = datetime.timedelta(days=28)
		if (lag > (2*specified_lag)) and (lag-specified_lag)>datetime.timedelta(minutes=2):
			if df_1mins["Volume"][df_1mins.shape[0]-1] == 0:
				## Ticker has low volume, ignore larger-than-expected lag. Just reduce the expiry, in case tomorrow has more volume
				expiry_td = datetime.timedelta(days=1)
			else:
				#print("df_5mins:")
				#print(df_5mins)
				#raise Exception("{}: calculated YF lag as {}, greatly exceeds the specified lag {}".format(self.ticker, lag, specified_lag))
				self._yf_lag = specified_lag
				return self._yf_lag
		self._yf_lag = lag
		yfcm.StoreCacheDatum(exchange_str, "yf_lag", self._yf_lag, expiry=dt_now+expiry_td)
		return self._yf_lag
