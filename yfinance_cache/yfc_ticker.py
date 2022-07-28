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
				auto_adjust=True, back_adjust=False,
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
				start_d = datetime.datetime.strptime(start, "%Y-%m-%d")
				start = start_d.replace(tzinfo=tz_exchange)
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

		h = self._getCachedPrices(interval)
		if not h is None:
			if not ("Dividends" in h.columns and "Stock Splits" in h.columns):
				## Force a fetch
				h = None

		# Handle missing dates, or dependencies between date arguments
		dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
		# print("h is None = {}".format(h is None))
		if not period is None:
			if h is None:
				# Use period
				start = None ; end = None
			else:
				sched = yfct.GetExchangeSchedule(exchange, dt_now.date()-(7*td_1d), dt_now.date()+td_1d)
				last_open_day = sched["market_open"][-1].date()
				tomorrow = datetime.datetime.combine(last_open_day+td_1d, datetime.time(0), tz_exchange)
				end = tomorrow
				sched = yfct.GetExchangeSchedule(exchange, dt_now.date(), dt_now.date()+td_1d)
				if (not sched is None) and (len(sched["market_open"]) > 0) and (dt_now < sched["market_open"][0]):
					if debug:
						print("- decrementing 'end' by 1d")
					end -= td_1d
				end_d = end.date()
				if period == yfcd.Period.Max:
					start = datetime.datetime.combine(datetime.date(yfcd.yf_min_year, 1, 1), datetime.time(0), tz_exchange)
				else:
					start = yfct.DtSubtractPeriod(end, period)
				ctr = 0
				while not yfct.ExchangeOpenOnDay(exchange, start.date()):
					start -= td_1d
					ctr += 1
					if ctr > 5:
						ctr = -1 ; break
				if ctr==-1:
					# Search forward instead
					while not yfct.ExchangeOpenOnDay(exchange, start.date()):
						start += td_1d
				start_d = start.date()
		else:
			if end is None:
				end = datetime.datetime.combine(dt_now.date() + td_1d, datetime.time(0), tz_exchange)
			if start is None:
				start = datetime.datetime.combine(end.date()-td_1d, datetime.time(0), tz_exchange)

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

		if period is None:
			pstr = None
		else:
			pstr = yfcd.periodToString[period]
		istr = yfcd.intervalToString[interval]

		interday = (interval in [yfcd.Interval.Days1,yfcd.Interval.Days5,yfcd.Interval.Week])

		## Trigger an estimation of Yahoo data delay:
		yf_lag = self.yf_lag

		if h is None:
			if interday:
				h = self._fetchYfHistory(pstr, interval, start_d, end_d, prepost, proxy, kwargs)
			else:
				h = self._fetchYfHistory(pstr, interval, start, end, prepost, proxy, kwargs)

		else:
			## Performance TODO: only check expiry on datapoints not marked 'final'
			## - need to improve 'expiry check' performance, is 3-4x slower than fetching from YF

			f_final = h["Final?"].values
			n = h.shape[0]
			if interday:
				if isinstance(h.index[0], pd.Timestamp):
					h_interval_dts = h.index.date
				else:
					h_interval_dts = h.index
			else:
				h_interval_dts = [yfct.ConvertToDatetime(dt, tz=tz_exchange) for dt in h.index]
			expired = np.array([False]*n)
			for idx in np.where(~f_final)[0]:
				h_interval_dt = h_interval_dts[idx]
				fetch_dt = yfct.ConvertToDatetime(h["FetchDate"][idx], tz=tz_exchange)
				expired[idx] = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, max_age, exchange, interval, yf_lag=self.yf_lag)
			h_interval_dts = np.array(h_interval_dts)
			if sum(expired) > 0:
				h = h[~expired]
				h_interval_dts = h_interval_dts[~expired]
			## Potential perf improvement: tag rows as fully contiguous to avoid searching for gaps
			# h_intervals = np.array([yfct.GetTimestampCurrentInterval(exchange, idt, interval, weeklyUseYahooDef=True) for idt in h_interval_dts])
			h_intervals = yfct.GetTimestampCurrentInterval_batch(exchange, h_interval_dts, interval, weeklyUseYahooDef=True)
			f_na = h_intervals == None
			if sum(f_na) > 0:
				print(h)
				raise Exception("Bad rows found in prices table")
				if debug:
					print("- found bad rows, deleting:")
					print(h[f_na])
				h = h[~f_na]
				h_intervals = h_intervals[~f_na]
			h_intervals = [x["interval_open"] for x in h_intervals]
			try:
				ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, start, end, interval, h_intervals, weeklyUseYahooDef=True, minDistanceThreshold=5)
			except yfcd.NoIntervalsInRangeException:
				ranges_to_fetch = None
			if ranges_to_fetch is None:
				ranges_to_fetch = []
			if debug:
				print("- ranges_to_fetch:")
				pprint(ranges_to_fetch)

			interval_td = yfcd.intervalToTimedelta[interval]
			if len(ranges_to_fetch) > 0:
				for r in ranges_to_fetch:
					rstart = r[0]
					rend = r[1]

					try:
						h2 = self._fetchYfHistory(pstr, interval, rstart, rend, prepost, proxy, kwargs)
					except yfcd.NoPriceDataInRangeException:
						## If only trying to fetch 1 day of 1d data, then print warning instead of exception.
						## Could add additional condition of dividend previous day (seems to mess up table).
						if interval == yfcd.Interval.Days1 and r[1]-r[0]==td_1d:
							print("WARNING: No {}-price data fetched for ticker {} between dates {} -> {}".format(yfcd.intervalToString[interval], self.ticker, r[0], r[1]))
							h2 = None
							continue
						else:
							raise

					if not h2 is None:
						if h2.index.tz != h.index.tz:
							raise Exception("New data tz={} != cached data tz={}".format(h2.index.tz, h.index.tz))

						## If a timepoint is in both h and h2, drop from h. This is possible because of 
						## threshold in IdentifyMissingIntervalRanges(), allowing re-fetch of cached data 
						## if it reduces total number of web requests
						f_duplicate = h.index.isin(h2.index)
						h = h[~f_duplicate]

						try:
							h = pd.concat([h, h2])
						except:
							print(self.ticker)
							print("h:")
							print(h.iloc[h.shape[0]-10:])
							print("h2:")
							print(h2)
							raise

				h.sort_index(inplace=True)

		if h is None:
			raise Exception("history() is exiting without price data")

		# Cache
		self._history[interval] = h
		yfcm.StoreCacheDatum(self.ticker, "history-"+istr, self._history[interval])

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
		if auto_adjust:
			df = yf.utils.auto_adjust(h[["Open","Close","Adj Close","Low","High","Volume"]])
			for c in ["Open","Close","Low","High","Volume"]:
				h[c] = df[c]
			h = h.drop(["Adj Close"], axis=1)
		elif back_adjust:
			df = yf.utils.back_adjust(h[["Open","Close","Adj Close","Low","High","Volume"]])
			for c in ["Open","Close","Low","High","Volume"]:
				h[c] = df[c]
			h = h.drop(["Adj Close"], axis=1)
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

		# if debug:
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
			if sum(f_na) > 0:
				if not interday:
					## For some exchanges (e.g. JSE) Yahoo returns intraday timestamps right on market close. Remove them.
					df2 = df.copy() ; df2["_date"] = df2.index.date ; df2["_intervalStart"] = df2.index
					sched = yfct.GetExchangeSchedule(exchange, df2["_date"].min(), df2["_date"].max()+td_1d)
					sched_df = sched
					sched_df["_date"] = sched_df.index.date
					df2 = df2.merge(sched_df, on="_date", how="left")
					f_drop = (df2["Volume"]==0).values & ((df2["_intervalStart"]<df2["market_open"]).values | (df2["_intervalStart"]>=df2["market_close"]).values)
					if sum(f_drop) > 0:
						intervalStarts = intervalStarts[~f_drop]
						intervals = intervals[~f_drop]
						df = df[~f_drop]
						n = df.shape[0]
						f_na = intervals==None
				if sum(f_na) > 0:
					## For some national holidays when exchange closed, Yahoo fills in row. Clue is 0 volume.
					## Solution = drop:
					f_na_zeroVol = f_na & (df["Volume"]==0)
					if sum(f_na_zeroVol) > 0:
						f_drop = f_na_zeroVol
						intervalStarts = intervalStarts[~f_drop]
						intervals = intervals[~f_drop]
						df = df[~f_drop]
						n = df.shape[0]
						f_na = intervals==None
					## TODO ... another clue is row is identical to previous trading day
					if sum(f_na) > 0:
						f_drop = np.array([False]*n)
						for i in np.where(f_na)[0]:
							if i > 0:
								dt = df.index[i]
								last_dt = df.index[i-1]
								if (df.loc[dt,yfcd.yf_data_cols] == df.loc[last_dt,yfcd.yf_data_cols]).all():
									f_drop[i] = True
						if sum(f_drop) > 0:
							intervalStarts = intervalStarts[~f_drop]
							intervals = intervals[~f_drop]
							df = df[~f_drop]
							n = df.shape[0]
							f_na = intervals==None
				if sum(f_na) > 0:
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
		interday = (interval in [yfcd.Interval.Days1,yfcd.Interval.Days5,yfcd.Interval.Week])

		h = None
		if not self._history is None:
			if interval in self._history.keys():
				h = self._history[interval]
		if (h is None) and yfcm.IsDatumCached(self.ticker, "history-"+istr):
			self._history[interval] = yfcm.ReadCacheDatum(self.ticker, "history-"+istr)
			h = self._history[interval]

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
