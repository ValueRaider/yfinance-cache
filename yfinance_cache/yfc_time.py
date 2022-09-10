from pprint import pprint

from datetime import datetime, date, time, timedelta
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
import pytz

import holidays
import exchange_calendars as xcal

import pandas as pd
import numpy as np

from . import yfc_dat as yfcd
from . import yfc_cache_manager as yfcm


def TypeCheckStr(var, varName):
	if not isinstance(var, str):
		raise Exception("'{}' must be str not {}".format(varName, type(var)))
def TypeCheckBool(var, varName):
	if not isinstance(var, bool):
		raise Exception("'{}' must be bool not {}".format(varName, type(var)))
def TypeCheckDateEasy(var, varName):
	if isinstance(var, pd.Timestamp):
		# While Pandas missing support for 'zoneinfo' must deny
		raise Exception("'{}' must be date not {}".format(varName, type(var)))
	if not (isinstance(var, date) or isinstance(var, datetime)):
		raise Exception("'{}' must be date not {}".format(varName, type(var)))
def TypeCheckDateStrict(var, varName):
	if isinstance(var, pd.Timestamp):
		# While Pandas missing support for 'zoneinfo' must deny
		raise Exception("'{}' must be date not {}".format(varName, type(var)))
	if not (isinstance(var, date) and not isinstance(var, datetime)):
		raise Exception("'{}' must be date not {}".format(varName, type(var)))
def TypeCheckDatetime(var, varName):
	if isinstance(var, pd.Timestamp):
		# While Pandas missing support for 'zoneinfo' must deny
		raise Exception("'{}' must be datetime not {}".format(varName, type(var)))
	if not isinstance(var, datetime):
		raise Exception("'{}' must be datetime not {}".format(varName, type(var)))
	if var.tzinfo is None:
		raise Exception("'{}' if datetime must be timezone-aware".format(varName))
def TypeCheckTimedelta(var, varName):
	if not isinstance(var, timedelta):
		raise Exception("'{}' must be timedelta not {}".format(varName, type(var)))
def TypeCheckInterval(var, varName):
	if not isinstance(var, yfcd.Interval):
		raise Exception("'{}' must be yfcd.Interval not {}".format(varName, type(var)))
def TypeCheckIntervalDt(dt, interval, varName, strict=True):
	if interval in [yfcd.Interval.Days1, yfcd.Interval.Days5, yfcd.Interval.Week]:
		if strict:
			TypeCheckDateStrict(dt, varName)
		else:
			TypeCheckDateEasy(dt, varName)
	else:
		TypeCheckDatetime(dt, varName)
def TypeCheckPeriod(var, varName):
	if not isinstance(var, yfcd.Period):
		raise Exception("'{}' must be yfcd.Period not {}".format(varName, type(var)))
def TypeCheckNpArray(var, varName):
	if not isinstance(var, np.ndarray):
		raise Exception("'{}' must be numpy array not {}".format(varName, type(var)))

exchangeTzCache = {}
def GetExchangeTzName(exchange):
	TypeCheckStr(exchange, "exchange")

	if not exchange in exchangeTzCache:
		tz = yfcm.ReadCacheDatum("exchange-"+exchange, "tz")
		if tz is None:
			raise Exception("Do not know timezone for exchange '{}'".format(exchange))
		exchangeTzCache[exchange] = tz
	else:
		tz = exchangeTzCache[exchange]
	return tz
def SetExchangeTzName(exchange, tz):
	TypeCheckStr(exchange, "exchange")
	TypeCheckStr(tz, "tz")
	
	tzc = yfcm.ReadCacheDatum("exchange-"+exchange, "tz")
	if not tzc is None:
		if tzc != tz:
			## Different names but maybe same tz
			tzc_zi = ZoneInfo(tzc)
			tz_zi = ZoneInfo(tz)
			dt = datetime.now()
			if tz_zi.utcoffset(dt) != tzc_zi.utcoffset(dt):
				print("tz_zi = {} ({})".format(tz_zi, type(tz_zi)))
				print("tzc_zi = {} ({})".format(tzc_zi, type(tzc_zi)))
				raise Exception("For exchange '{}', new tz {} != cached tz {}".format(exchange, tz, tzc))
	else:
		exchangeTzCache[exchange] = tz
		yfcm.StoreCacheDatum("exchange-"+exchange, "tz", tz)


def GetExchangeDataDelay(exchange):
	TypeCheckStr(exchange, "exchange")

	d = yfcm.ReadCacheDatum("exchange-"+exchange, "yf_lag")
	if d is None:
		d = yfcd.exchangeToYfLag[exchange]
	return d


def GetCalendar(exchange):
	return xcal.get_calendar(yfcd.exchangeToXcalExchange[exchange], start=str(yfcd.yf_min_year))

def ExchangeOpenOnDay(exchange, d):
	TypeCheckStr(exchange, "exchange")
	TypeCheckDateStrict(d, "d")

	if not exchange in yfcd.exchangeToXcalExchange:
		raise Exception("Need to add mapping of exchange {} to xcal".format(exchange))
	cal = GetCalendar(exchange)

	return d.isoformat() in cal.schedule.index


def GetExchangeSchedule(exchange, start_d, end_d):
	TypeCheckStr(exchange, "exchange")
	TypeCheckDateStrict(start_d, "start_d")
	TypeCheckDateStrict(end_d, "end_d")

	if start_d >= end_d:
		raise Exception("start_d={} must < end_d={}".format(start_d, end_d))

	if not exchange in yfcd.exchangeToXcalExchange:
		raise Exception("Need to add mapping of exchange {} to xcal".format(exchange))
	cal = GetCalendar(exchange)

	sched = None
	## loc[] is inclusive, but end_d should be treated as exclusive:
	sched = cal.schedule[start_d:end_d-timedelta(days=1)]
	if (sched is None) or sched.shape[0] == 0:
		return None

	tz = GetExchangeTzName(exchange)
	df = sched[["open","close"]].copy()
	df["open"] = df["open"].dt.tz_convert(tz)
	df["close"] = df["close"].dt.tz_convert(tz)
	df = df.rename(columns={"open":"market_open","close":"market_close"})
	return df


def GetExchangeScheduleIntervals(exchange, interval, start, end, weeklyUseYahooDef=True):
	TypeCheckStr(exchange, "exchange")
	if start >= end:
		raise Exception("start={} must be < end={}".format(start, end))
	TypeCheckDateEasy(start, "start")
	TypeCheckDateEasy(end, "end")

	debug = False
	# debug = True

	if debug:
		print("GetExchangeScheduleIntervals()")

	if not exchange in yfcd.exchangeToXcalExchange:
		raise Exception("Need to add mapping of exchange {} to xcal".format(exchange))
	cal = GetCalendar(exchange)

	tz = ZoneInfo(GetExchangeTzName(exchange))
	if not isinstance(start, datetime):
		start_dt = datetime.combine(start, time(0), tz)
		start_d = start
	else:
		start_dt = start
		start_d = start.astimezone(tz).date()
	if not isinstance(end, datetime):
		end_dt = datetime.combine(end, time(0), tz)
		end_d = end
	else:
		end_dt = end
		end_d = end.astimezone(tz).date()
	dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))

	if debug:
		print("- start_d={}, end_d={}".format(start_d, end_d))

	istr = yfcd.intervalToString[interval]
	if istr.endswith('h') or istr.endswith('m'):
		ti = cal.trading_index(start_d.isoformat(), end_d.isoformat(), period=istr, intervals=True, force_close=True)
		if len(ti) == 0:
			return None
		times = []
		for i in ti:
			if i.left < start_dt:
				continue
			if i.right > end_dt:
				break
			if i.left > dt_now:
				break
			times.append((i.left.to_pydatetime().astimezone(tz), i.right.to_pydatetime().astimezone(tz)))
		return times
	elif interval == yfcd.Interval.Days1:
		s = cal.schedule.loc[start_d.isoformat():(end_d-timedelta(days=1)).isoformat()]
		s = s[s["open"]<=dt_now]
		if s.shape[0] == 0:
			return None
		open_days = [dt.to_pydatetime().astimezone(tz).date() for dt in s["open"]]
		ranges = [(d, d+timedelta(days=1)) for d in open_days]
		return ranges
	elif interval in [yfcd.Interval.Days5, yfcd.Interval.Week]:
		open_dts = cal.schedule.loc[start_d.isoformat():(end_d-timedelta(days=1)).isoformat()]["open"]
		open_dts = open_dts[open_dts<=dt_now]
		if len(open_dts) == 0:
			return None
		open_days = [dt.to_pydatetime().astimezone(tz).date() for dt in open_dts]
		if debug:
			print("- open_days:")
			pprint(open_days)

		first_week_start_idx = None
		if cal.previous_session(open_days[0]).weekday() > open_days[0].weekday():
			# First dt is start of working week
			first_week_start_idx = 0
		else:
			# search for next week
			inf_loop_ctr = 10
			for i in range(len(open_days)):
				inf_loop_ctr -= 1
				if inf_loop_ctr == 0:
					raise Exception("Infinite loop detected")
				if open_days[i].weekday() < open_days[0].weekday():
					first_week_start_idx = i
					break
		if first_week_start_idx is None:
			if debug:
				print("first_week_start_idx is None")
			return None
		open_days = open_days[first_week_start_idx:]

		last_week_end_idx = None
		if cal.next_session(open_days[-1]).weekday() < open_days[-1].weekday():
			# Last dt is end of working week
			last_week_end_idx = len(open_days)-1
		else:
			# search for last week
			inf_loop_ctr = 10
			for i in range(len(open_days)-1, -1, -1):
				inf_loop_ctr -= 1
				if inf_loop_ctr == 0:
					raise Exception("Infinite loop detected")
				if open_days[i].weekday() > open_days[-1].weekday():
					last_week_end_idx = i
					break
		if last_week_end_idx is None:
			if debug:
				print("last_week_end_idx is None")
			return None
		open_days = open_days[:last_week_end_idx+1]

		if debug:
			print("- open_days:")
			print(open_days)

		# Now, open_dts contains only full working weeks
		week_ranges = []
		week_start = open_days[0]
		week_end = None
		for i in range(1,len(open_days)-1):
			if open_days[i].weekday() < open_days[i-1].weekday():
				week_end = open_days[i-1]
				week_ranges.append((week_start, week_end+timedelta(days=1)))
				week_start = open_days[i]
		week_ranges.append((week_start, open_days[-1]+timedelta(days=1)))

		if debug:
			print("week_ranges:")
			print(week_ranges)

		if weeklyUseYahooDef:
			week_ranges2 = []
			for r in week_ranges:
				ws = r[0] ; ws -= timedelta(days=ws.weekday())
				we = r[1] ; we += timedelta(days=5-we.weekday())
				week_ranges2.append((ws, we))
			week_ranges = week_ranges2

			if debug:
				print("week_ranges2:")
				print(week_ranges2)

		return week_ranges

	else:
		raise Exception("Need to implement for interval={}".format(interval))


def IsTimestampInActiveSession(exchange, ts):
	TypeCheckStr(exchange, "exchange")
	TypeCheckDatetime(ts, "ts")

	cal = GetCalendar(exchange)
	try:
		s = cal.schedule.loc[ts.date().isoformat()]
	except:
		return False
	return s["open"] <= ts and ts < s["close"]


def GetTimestampCurrentSession(exchange, ts):
	TypeCheckStr(exchange, "exchange")
	TypeCheckDatetime(ts, "ts")

	cal = GetCalendar(exchange)
	try:
		s = cal.schedule.loc[ts.date().isoformat()]
	except:
		return None
	tz = ZoneInfo(GetExchangeTzName(exchange))
	o = s["open"].to_pydatetime().astimezone(tz)
	c = s["close"].to_pydatetime().astimezone(tz)
	if o <= ts and ts < c:
		return {"market_open":o, "market_close":c}
	else:
		return None


def GetTimestampMostRecentSession(exchange, ts):
	TypeCheckStr(exchange, "exchange")
	TypeCheckDatetime(ts, "ts")

	## If 'ts' is currently in an active session then that is most recent

	s = GetTimestampCurrentSession(exchange, ts)
	if not s is None:
		return s
	sched = GetExchangeSchedule(exchange, ts.date()-timedelta(days=6), ts.date()+timedelta(days=1))
	for i in range(sched.shape[0]-1, -1, -1):
		if sched["market_open"][i] <= ts:
			tz = ZoneInfo(GetExchangeTzName(exchange))
			return {"market_open":sched["market_open"][i].to_pydatetime().astimezone(tz), "market_close":sched["market_close"][i].to_pydatetime().astimezone(tz)}
	raise Exception("Failed to find most recent '{0}' session for ts = {1}".format(exchange, ts))


def GetTimestampNextSession(exchange, ts):
	TypeCheckStr(exchange, "exchange")
	TypeCheckDatetime(ts, "ts")

	sched = GetExchangeSchedule(exchange, ts.date(), ts.date()+timedelta(days=7))
	for i in range(sched.shape[0]):
		if ts < sched["market_open"][i]:
			tz = ZoneInfo(GetExchangeTzName(exchange))
			return {"market_open":sched["market_open"][i].to_pydatetime().astimezone(tz), "market_close":sched["market_close"][i].to_pydatetime().astimezone(tz)}
	raise Exception("Failed to find next '{0}' session for ts = {1}".format(exchange, ts))


def GetTimestampCurrentInterval(exchange, ts, interval, weeklyUseYahooDef=True):
	TypeCheckStr(exchange, "exchange")
	TypeCheckIntervalDt(ts, interval, "ts", strict=False)
	TypeCheckInterval(interval, "interval")
	TypeCheckBool(weeklyUseYahooDef, "weeklyUseYahooDef")

	# For day and week intervals, the time component is ignored (set to 0).

	debug = False
	# debug = True

	if debug:
		print("GetTimestampCurrentInterval(ts={}, interval={}, weeklyUseYahooDef={})".format(ts, interval, weeklyUseYahooDef))

	i = None

	tz_name = GetExchangeTzName(exchange)
	tz = ZoneInfo(tz_name)
	if interval in [yfcd.Interval.Days5, yfcd.Interval.Week]:
		# Treat week intervals as special case, contiguous from first weekday open to last weekday open. 
		# Not necessarily Monday->Friday because of public holidays.
		# Unless 'weeklyUseYahooDef' is true, which means range from Monday to Friday.
		if isinstance(ts,datetime):
			ts_day = ts.astimezone(tz).date()
		else:
			ts_day = ts
		weekStart = ts_day - timedelta(days=ts_day.weekday())
		weekEnd = weekStart + timedelta(days=5)
		if debug:
			print("- weekStart = {}".format(weekStart))
		if not weeklyUseYahooDef:
			weekSched = GetExchangeSchedule(exchange, weekStart, weekEnd)
			weekStart = weekSched["market_open"][0].date()
			weekEnd = weekSched["market_close"][-1].date()+timedelta(days=1)
			# weekStart = weekSched["market_open"][0].tz_convert(tz_name).date()
			# weekEnd = weekSched["market_close"][-1].tz_convert(tz_name).date()+timedelta(days=1)
		intervalStart = weekStart
		intervalEnd = weekEnd
		if debug:
			print("- intervalStart = {}".format(intervalStart))
			print("- intervalEnd = {}".format(intervalEnd))
		if ts_day >= intervalStart:
			if (ts_day < intervalEnd):
				i = {"interval_open":intervalStart, "interval_close":intervalEnd}

	elif interval == yfcd.Interval.Days1:
		if isinstance(ts, datetime):
			ts_day = ts.astimezone(tz).date()
		else:
			ts_day = ts
		if debug:
			print("- ts_day: {}".format(ts_day))
		if ExchangeOpenOnDay(exchange, ts_day):
			if debug:
				print("- exchange open")
			daySched = GetExchangeSchedule(exchange, ts_day, ts_day+timedelta(days=1))
			i = {"interval_open":daySched["market_open"][0].date(), "interval_close":daySched["market_close"][0].date()+timedelta(days=1)}
		else:
			if debug:
				print("- exchange closed")

	else:
		if IsTimestampInActiveSession(exchange, ts):
			td = yfcd.intervalToTimedelta[interval]
			## Try with exchange_calendars
			cal = GetCalendar(exchange)
			dt_utc = ts.astimezone(ZoneInfo("UTC"))
			tis = cal.trading_index(dt_utc-td, dt_utc+td, period=yfcd.intervalToString[interval], intervals=True, force_close=True)
			idx = -1
			for i in range(len(tis)):
				if tis[i].left <= ts and ts < tis[i].right:
					idx = i
					break
			if idx == -1:
				return None
			intervalStart = tis[idx].left.to_pydatetime().astimezone(tz)
			intervalEnd = tis[idx].right.to_pydatetime().astimezone(tz)

			i = {"interval_open":intervalStart, "interval_close":intervalEnd}

	return i

def GetTimestampCurrentInterval_batch(exchange, ts, interval, weeklyUseYahooDef=True):
	TypeCheckStr(exchange, "exchange")
	if isinstance(ts,list):
		ts = np.array(ts)
	TypeCheckNpArray(ts, "ts")
	TypeCheckIntervalDt(ts[0], interval, "ts", strict=False)
	TypeCheckInterval(interval, "interval")
	TypeCheckBool(weeklyUseYahooDef, "weeklyUseYahooDef")

	# For day and week intervals, the time component is ignored (set to 0).

	debug = False
	# debug = True

	if debug:
		print("GetTimestampCurrentInterval_batch(ts[0]={}, interval={}, weeklyUseYahooDef={})".format(ts[0], interval, weeklyUseYahooDef))

	n = len(ts)
	tz = ZoneInfo(GetExchangeTzName(exchange))
	intervals = [None]*n

	td_1d = timedelta(days=1)
	tz_name = GetExchangeTzName(exchange)
	tz = ZoneInfo(tz_name)
	tz_pytz = pytz.timezone(tz_name)
	ts_is_datetimes = isinstance(ts[0],datetime)
	if ts_is_datetimes:
		ts_day = [t.astimezone(tz).date() for t in ts]
		# ts_day = list(map(lambda x: x.astimezone(tz).date(), ts))
	else:
		ts_day = ts

	if interval in [yfcd.Interval.Days5, yfcd.Interval.Week]:
		# Treat week intervals as special case, contiguous from first weekday open to last weekday open. 
		# Not necessarily Monday->Friday because of public holidays.
		# Unless 'weeklyUseYahooDef' is true, which means range from Monday to Friday.
		#
		t0 = ts_day[0]  ; t0 -= timedelta(days=t0.weekday())+timedelta(days=7)
		tl = ts_day[-1] ; tl += timedelta(days=6-tl.weekday())+timedelta(days=7)
		sched = GetExchangeSchedule(exchange, t0, tl)
		sched["day"] = sched["market_open"].dt.date

		if weeklyUseYahooDef:
			# Monday -> Saturday regardless of exchange schedule
			weekSchedStart = [d-timedelta(days=d.weekday()) if d.weekday()<=4 else None for d in ts_day]
			weekSchedEnd = [ws+timedelta(days=5) if (not ws is None) else None for ws in weekSchedStart]
		else:
			# Create a simple weekly schedule:
			sched["deltaF"] = sched["day"].diff().dt.days
			sched["deltaB"] = sched["day"].diff(periods=-1).dt.days
			sched = sched.drop("day",axis=1)
			sched = sched[(sched["deltaF"]>1.0) | (sched["deltaB"]<-1.0)]
			sched["Monday"] = (sched["market_open"] - pd.to_timedelta(sched["market_open"].dt.weekday, unit='d')).dt.date
			sched_starts = sched[sched["deltaF"] > 1.0].drop(["deltaF","deltaB","market_close"],axis=1)
			sched_ends = sched[sched["deltaB"] < -1.0].drop(["deltaF","deltaB","market_open"],axis=1)
			sched_starts.index = sched_starts["Monday"]
			sched_ends.index = sched_ends["Monday"]
			sched_starts = sched_starts.drop("Monday",axis=1)
			sched_ends = sched_ends.drop("Monday",axis=1)
			week_sched = sched_starts.join(sched_ends, how="inner")
			week_sched = week_sched.rename(columns={"market_open":"open","market_close":"close"})
			#
			week_sched["open_day"] = week_sched["open"].dt.date
			week_sched["close_day"] = week_sched["close"].dt.date+timedelta(days=1)
			#
			weekSchedStart = np.array([None]*n)
			weekSchedEnd = np.array([None]*n)
			i_t = 0 ; i_s = 0
			td = ts_day[i_t]
			left  = pd.to_datetime(week_sched["open_day"].values ).tz_localize(tz_pytz)
			right = pd.to_datetime(week_sched["close_day"].values).tz_localize(tz_pytz)
			week_sched = pd.IntervalIndex.from_arrays(left, right, closed="left")
			if ts_is_datetimes:
				ts_pytz = pd.to_datetime([t.astimezone(tz_pytz) for t in ts])
			else:
				ts_pytz = pd.to_datetime(ts).tz_localize(tz_pytz)
			idx = week_sched.get_indexer(ts_pytz)
			f = idx!=-1
			#
			weekSchedStart[f] = week_sched.left[idx[f]].date
			weekSchedEnd[f] = week_sched.right[idx[f]].date

		if isinstance(ts[0], datetime):
			ts = [pd.Timestamp(t.astimezone(tz_pytz)) for t in ts] # Convert to Pandas-friendly
		intervals = pd.DataFrame(data={"interval_open":weekSchedStart, "interval_close":weekSchedEnd}, index=ts)

	elif interval == yfcd.Interval.Days1:
		t0 = ts_day[0]
		tl = ts_day[len(ts_day)-1]
		sched = GetExchangeSchedule(exchange, t0, tl+timedelta(days=1))
		#
		ts_day = pd.to_datetime(ts_day)
		ts_day_df = pd.DataFrame(index=ts_day)
		intervals = pd.merge(ts_day_df, sched, how="left", left_index=True, right_index=True)
		intervals = intervals.rename(columns={"market_open":"interval_open", "market_close":"interval_close"})
		# tz = ZoneInfo("UTC")
		# intervals = [None if pd.isnull(x["market_open"].iloc[i]) else {"interval_open":x["market_open"].iloc[i].to_pydatetime().astimezone(tz), "interval_close":x["market_close"].iloc[i].to_pydatetime().astimezone(tz)} for i in range(len(ts))]
		# pprint(intervals[0:4])
		# quit()
		intervals["interval_open"] = intervals["interval_open"].dt.date
		intervals["interval_close"] = intervals["interval_close"].dt.date +td_1d

	else:
		td = yfcd.intervalToTimedelta[interval]
		cal = GetCalendar(exchange)
		tz_utc = pytz.timezone("UTC")
		ts = [pd.Timestamp(t.astimezone(tz_pytz)) for t in ts] # Convert to Pandas-friendly for .loc[] below
		ts_utc = [t.astimezone(tz_utc) for t in ts] # Convert to Pandas-friendly for .loc[] below
		t0 = ts_utc[0]
		tl = ts_utc[len(ts_utc)-1]
		tis = cal.trading_index(t0-td, tl+td, period=yfcd.intervalToString[interval], intervals=True, force_close=True)
		# tis_df = pd.DataFrame(data={"left":tis.left, "right":tis.right}, index=tis)
		# tis_df["left"] = tis_df["left"].dt.tz_convert(tz_name)
		# tis_df["right"] = tis_df["right"].dt.tz_convert(tz_name)
		#
		idx = tis.get_indexer(ts_utc)
		#
		intervals = pd.DataFrame(index=ts)
		intervals["interval_open"] = pd.NaT
		intervals["interval_close"] = pd.NaT
		f = idx!=-1
		intervals.loc[f, "interval_open"] = tis.left[idx[f]].tz_convert(tz_name)
		intervals.loc[f, "interval_close"] = tis.right[idx[f]].tz_convert(tz_name)

	if debug:
		print("GetTimestampCurrentInterval_batch() returning:")
	return intervals

def GetTimestampNextInterval(exchange, ts, interval, weeklyUseYahooDef=True):
	TypeCheckStr(exchange, "exchange")
	TypeCheckIntervalDt(ts, interval, "ts", strict=False)
	TypeCheckInterval(interval, "interval")
	TypeCheckBool(weeklyUseYahooDef, "weeklyUseYahooDef")

	td_1d = timedelta(days=1)
	tz_name = GetExchangeTzName(exchange)
	tz = ZoneInfo(tz_name)
	if interval in [yfcd.Interval.Days1, yfcd.Interval.Days5, yfcd.Interval.Week]:
		if interval == yfcd.Interval.Days1:
			next_day = ts.astimezone(tz).date() + td_1d
			s = GetTimestampNextSession(exchange, datetime.combine(next_day, time(0), tz))
			interval_open  = s["market_open"].date()
			interval_close = interval_open+td_1d
		else:
			## Calculate next Monday to get next week schedule
			ts_d = ts.date()
			next_monday = ts_d + timedelta(days=7-ts_d.weekday())

			sched = GetExchangeSchedule(exchange, next_monday, next_monday+timedelta(days=5))
			interval_open  = sched["market_open"][0].date()
			interval_close = sched["market_close"][-1].date() + td_1d
			if weeklyUseYahooDef:
				interval_open -= timedelta(days=interval_open.weekday()) # shift back to Monday
				interval_close += timedelta(days=5-interval_close.weekday()) # shift forward to Saturday
		return {"interval_open":interval_open, "interval_close":interval_close}

	interval_td = yfcd.intervalToTimedelta[interval]
	c = GetTimestampCurrentInterval(exchange, ts, interval)
	if c is None or not IsTimestampInActiveSession(exchange, c["interval_close"]):
		next_interval_start = GetTimestampNextSession(exchange, ts)["market_open"]
	else:
		next_interval_start = c["interval_close"]
	return {"interval_open":next_interval_start, "interval_close":next_interval_start+interval_td}


def CalcIntervalLastDataDt(exchange, intervalStart, interval, yf_lag=None):
	# When does Yahoo stop receiving data for this interval?
	TypeCheckStr(exchange, "exchange")
	TypeCheckIntervalDt(intervalStart, interval, "intervalStart", strict=False)
	TypeCheckInterval(interval, "interval")

	debug = False
	# debug = True

	if debug:
		print("CalcIntervalLastDataDt(intervalStart={}, interval={})".format(intervalStart, interval))

	if not yf_lag is None:
		TypeCheckTimedelta(yf_lag, "yf_lag")
	else:
		yf_lag = GetExchangeDataDelay(exchange)

	tz = ZoneInfo(GetExchangeTzName(exchange))

	irange = GetTimestampCurrentInterval(exchange, intervalStart, interval)
	if irange is None:
		raise Exception("Failed to map {} to interval".format(intervalStart))
	if debug:
		print("- irange:")
		pprint(irange)

	if isinstance(irange["interval_open"],datetime):
		intervalSched = GetExchangeSchedule(exchange, irange["interval_open"].astimezone(tz).date(), irange["interval_close"].astimezone(tz).date()+timedelta(days=1))
	else:
		intervalSched = GetExchangeSchedule(exchange, irange["interval_open"], irange["interval_close"])
	if debug:
		print("- intervalSched:")
		pprint(intervalSched)

	intervalEnd = irange["interval_close"]
	if isinstance(intervalEnd, datetime):
		intervalEnd_dt = intervalEnd
	else:
		intervalEnd_dt = datetime.combine(intervalEnd, time(0), ZoneInfo(GetExchangeTzName(exchange)))

	lastDataDt = min(intervalEnd_dt, intervalSched["market_close"][-1]) +yf_lag

	# For some exchanges, Yahoo has trades that occurred soon afer official market close, e.g. Johannesburg:
	if exchange in ["JNB"]:
		late_data_allowance = timedelta(minutes=15)
	else:
		late_data_allowance = timedelta(0)

	if (interval in [yfcd.Interval.Days1, yfcd.Interval.Days5, yfcd.Interval.Week]) or (intervalEnd_dt==intervalSched["market_close"][-1]):
		## Is daily/weekly interval or last interval of day:
		lastDataDt += late_data_allowance

	if debug:
		print("CalcIntervalLastDataDt() returning {}".format(lastDataDt))

	return lastDataDt

def CalcIntervalLastDataDt_batch(exchange, intervalStart, interval, yf_lag=None):
	# When does Yahoo stop receiving data for this interval?
	TypeCheckStr(exchange, "exchange")
	if isinstance(intervalStart,list):
		intervalStart = np.array(intervalStart)
	TypeCheckNpArray(intervalStart, "intervalStart")
	TypeCheckIntervalDt(intervalStart[0], interval, "intervalStart", strict=False)
	TypeCheckInterval(interval, "interval")

	if not yf_lag is None:
		TypeCheckTimedelta(yf_lag, "yf_lag")
	else:
		yf_lag = GetExchangeDataDelay(exchange)

	n = len(intervalStart)
	tz_name = GetExchangeTzName(exchange)
	tz = ZoneInfo(tz_name)

	intervals = GetTimestampCurrentInterval_batch(exchange, intervalStart, interval)
	if isinstance(intervals["interval_open"].iloc[0], datetime):
		iopens = intervals["interval_open"]
		icloses = intervals["interval_close"]
	else:
		iopens = intervals["interval_open"].values
		icloses = intervals["interval_close"].values

	marketCloses = np.array([None]*n)
	iopen0 = iopens[0]
	iclosel = icloses[len(icloses)-1]
	if isinstance(iopen0, datetime):
		if isinstance(iopen0, pd.Timestamp):
			iopen0 = iopen0.to_pydatetime()
			iclosel = iclosel.to_pydatetime()
		sched = GetExchangeSchedule(exchange, iopen0.astimezone(tz).date(), iclosel.astimezone(tz).date()+timedelta(days=1))
	else:
		sched = GetExchangeSchedule(exchange, iopen0, iclosel+timedelta(days=1))
	sched["day"] = sched["market_open"].dt.date
	is_dt = isinstance(iopens[0], datetime)
	if is_dt:
		iopen0 = iopens[0]
		if isinstance(iopen0, pd.Timestamp):
			# iclose_days = [i.to_pydatetime().astimezone(tz).date() for i in icloses]
			tz_pytz = pytz.timezone(tz_name)
			iclose_days = [i.astimezone(tz_pytz).date() for i in icloses]
		else:
			iclose_days = [i.astimezone(tz).date() for i in icloses]
	else:
		iclose_days = [i-timedelta(days=1) for i in icloses]
	icloses_df = pd.DataFrame(data={"iopen":iopens, "iclose":icloses, "day":iclose_days})
	icloses_df2 = icloses_df.merge(sched[["day","market_close"]], on="day", how="left")
	f_na = icloses_df2["market_close"].isna()
	if f_na.any():
		if interval in [yfcd.Interval.Week, yfcd.Interval.Months1, yfcd.Interval.Months3]:
			# Search back a little
			attempts = 4 # Worst-case = 9/11, have to search back from Friday to Monday to find actual open day
			while f_na.any() and attempts > 0:
				icloses_df2 = icloses_df2.drop("market_close",axis=1)
				icloses_df2.loc[f_na,"day"] -= timedelta(days=1)
				icloses_df2 = icloses_df2.merge(sched[["day","market_close"]], on="day", how="left")
				attempts -= 1
				f_na = icloses_df2["market_close"].isna()
		if f_na.any():
			raise Exception("Lost data in merge")
	icloses_df = icloses_df2
	marketCloses = icloses_df["market_close"].dt.to_pydatetime()

	if (marketCloses==None).any():
		raise Exception("Failed to map some intervals to schedule")

	dc0 = icloses[0]
	if isinstance(dc0, datetime):
		intervalEnd_dt = [x.to_pydatetime().astimezone(tz) for x in icloses]
	else:
		intervalEnd_dt = [datetime.combine(dc, time(0), tz) for dc in icloses]

	lastDataDt = np.minimum(intervalEnd_dt, marketCloses) +yf_lag

	# For some exchanges, Yahoo has trades that occurred soon afer official market close, e.g. Johannesburg:
	if exchange in ["JNB"]:
		late_data_allowance = timedelta(minutes=15)
		if interval in [yfcd.Interval.Days1, yfcd.Interval.Days5, yfcd.Interval.Week]:
			lastDataDt += late_data_allowance
		else:
			lastDataDt[intervalEnd_dt == marketCloses] += late_data_allowance

	return lastDataDt


def IsPriceDatapointExpired(intervalStart, fetch_dt, max_age, exchange, interval, triggerExpiryOnClose=True, yf_lag=None, dt_now=None):
	TypeCheckIntervalDt(intervalStart, interval, "intervalStart", strict=False)
	TypeCheckDatetime(fetch_dt, "fetch_dt")
	TypeCheckTimedelta(max_age, "max_age")
	TypeCheckStr(exchange, "exchange")
	TypeCheckInterval(interval, "interval")
	TypeCheckBool(triggerExpiryOnClose, "triggerExpiryOnClose")

	debug = False
	# debug = True

	if debug:
		print("") ; print("")
		print("IsPriceDatapointExpired(intervalStart={}, fetch_dt={}, max_age={}, triggerExpiryOnClose={}, dt_now={})".format(intervalStart, fetch_dt, max_age, triggerExpiryOnClose, dt_now))

	if not dt_now is None:
		TypeCheckDatetime(dt_now, "dt_now")
	else:
		dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))

	if not yf_lag is None:
		TypeCheckTimedelta(yf_lag, "yf_lag")
	else:
		yf_lag = GetExchangeDataDelay(exchange)
	if debug:
		print("yf_lag = {}".format(yf_lag))

	irange = GetTimestampCurrentInterval(exchange, intervalStart, interval)
	if debug:
		print("- irange = {}".format(irange))

	if irange is None:
		print("market open? = {}".format(IsTimestampInActiveSession(exchange, intervalStart)))
		raise Exception("Failed to map '{}'' to '{}' interval range".format(intervalStart, interval))

	intervalEnd = irange["interval_close"]
	if isinstance(intervalEnd, datetime):
		intervalEnd_d = intervalEnd.date()
	else:
		intervalEnd_d = intervalEnd
	if debug:
		print("- intervalEnd_d = {0}".format(intervalEnd_d))

	lastDataDt = CalcIntervalLastDataDt(exchange, intervalStart, interval, yf_lag)
	if debug:
		print("- lastDataDt = {}".format(lastDataDt))

	# Decide if was fetched after last Yahoo update
	if interval in [yfcd.Interval.Days1, yfcd.Interval.Days5, yfcd.Interval.Week]:
		if fetch_dt >= lastDataDt:
			## interval already closed before fetch, nothing to do.
			if debug:
				print("- fetch_dt > lastDataDt so return FALSE")
			return False
	else:
		interval_already_closed = fetch_dt > lastDataDt
		if interval_already_closed:
			## interval already closed before fetch, nothing to do.
			if debug:
				print("- fetch_dt > lastDataDt so return FALSE")
			return False

	expire_dt = fetch_dt+max_age
	if debug:
		print("- expire_dt = {0}".format(expire_dt))
	if expire_dt < lastDataDt and expire_dt <= dt_now:
		if debug:
			print("- expire_dt < lastDataDt and expire_dt <= dt_now so return TRUE")
		return True

	if triggerExpiryOnClose:
		if debug:
			print("- checking if triggerExpiryOnClose ...")
			print("- - fetch_dt            = {}".format(fetch_dt))
			print("- - lastDataDt = {}".format(lastDataDt))
			print("- - dt_now              = {}".format(dt_now))
		if (fetch_dt < lastDataDt) and (lastDataDt <= dt_now):
			## Even though fetched data hasn't fully aged, the candle has since closed so treat as expired
			if debug:
				print("- triggerExpiryOnClose and interval closed so return TRUE")
			return True
		if interval in [yfcd.Interval.Days1, yfcd.Interval.Days5, yfcd.Interval.Week]:
			## If last fetch was anytime within interval, even post-market, 
			## and dt_now is next day (or later) then trigger
			if fetch_dt.date() <= intervalEnd_d and dt_now.date() > intervalEnd_d:
				if debug:
					print("- triggerExpiryOnClose and interval midnight passed since fetch so return TRUE")
				return True

	if debug:
		print("- reached end of function, returning FALSE")
	return False


def IdentifyMissingIntervalRanges(exchange, start, end, interval, knownIntervalStarts, weeklyUseYahooDef=True, minDistanceThreshold=5):
	TypeCheckStr(exchange, "exchange")
	TypeCheckDateEasy(start, "start")
	TypeCheckDateEasy(end, "end")
	if start >= end:
		raise Exception("start={} must be < end={}".format(start, end))
	if not knownIntervalStarts is None:
		if not isinstance(knownIntervalStarts, list) and not isinstance(knownIntervalStarts, np.ndarray):
			raise Exception("'knownIntervalStarts' must be list or numpy array not {0}".format(type(knownIntervalStarts)))
		if interval in [yfcd.Interval.Days1, yfcd.Interval.Days5, yfcd.Interval.Week]:
			## Must be date or datetime
			TypeCheckDateEasy(knownIntervalStarts[0], "knownIntervalStarts")
			if isinstance(knownIntervalStarts[0],datetime) and knownIntervalStarts[0].tzinfo is None:
				raise Exception("'knownIntervalStarts' datetimes must be timezone-aware")
		else:
			## Must be datetime
			TypeCheckDatetime(knownIntervalStarts[0], "knownIntervalStarts")
			if knownIntervalStarts[0].tzinfo is None:
				raise Exception("'knownIntervalStarts' dates must be timezone-aware")

	debug = False
	# debug = True

	if debug:
		print("IdentifyMissingIntervalRanges()")
		print("- start={}, end={}".format(start, end))
		print("- knownIntervalStarts:")
		pprint(knownIntervalStarts)

	intervals = GetExchangeScheduleIntervals(exchange, interval, start, end, weeklyUseYahooDef)
	if intervals is None or len(intervals) == 0:
		raise yfcd.NoIntervalsInRangeException(interval, start, end)
	if debug:
		print("- intervals:")
		pprint(intervals)

	if not knownIntervalStarts is None:
		if isinstance(intervals[0], datetime) or isinstance(intervals[0], pd.Timestamp):
			intervalStarts = [i[0].timestamp() for i in intervals]
			knownIntervalStarts = [x.timestamp() for x in knownIntervalStarts]
		else:
			intervalStarts = [i[0] for i in intervals]

		intervalStarts = np.array(intervalStarts)
		knownIntervalStarts = np.array(knownIntervalStarts)
		if intervalStarts.dtype.hasobject or knownIntervalStarts.dtype.hasobject:
			## Apparently not optimised in numpy, faster to DIY
			## https://github.com/numpy/numpy/issues/14997#issuecomment-560516888
			knownIntervalStarts_set = set(knownIntervalStarts)
			intervals_missing_data = ~np.array([elem in knownIntervalStarts_set for elem in intervalStarts])
		else:
			intervals_missing_data = np.isin(intervalStarts, knownIntervalStarts, invert=True)
	else:
		intervals_missing_data = np.ones(len(intervals))

	if debug:
		print("- intervals_missing_data:")
		pprint(intervals_missing_data)

	## Merge together near ranges if the distance between is below threshold.
	## This is to reduce web requests
	i_true = np.where(intervals_missing_data)[0]
	for i in range(len(i_true)-1):
		i0 = i_true[i]
		i1 = i_true[i+1]
		if i1-i0 <= minDistanceThreshold+1:
			## Mark all intervals between as missing, thus merging together 
			## the pair of missing ranges
			intervals_missing_data[i0+1:i1] = 1

	if debug:
		print("- intervals_missing_data:")
		pprint(intervals_missing_data)

	## Scan for contiguous sets of missing intervals:
	ranges = []
	i_true = np.where(intervals_missing_data)[0]
	if len(i_true) > 0:
		start = None ; end = None
		for i in range(len(intervals_missing_data)):
			v = intervals_missing_data[i]
			if v:
				if start is None:
					start = i ; end = i
				else:
					if i == (end+1):
						end = i
					else:
						r = (intervals[start][0], intervals[end][1])
						ranges.append(r)
						start = i ; end = i

			if i == (len(intervals_missing_data)-1):
				r = (intervals[start][0], intervals[end][1])
				ranges.append(r)

	if debug:
		print("ranges:")
		pprint(ranges)

	if len(ranges) == 0:
		return None
	return ranges


def ConvertToDatetime(dt, tz=None):
	## Convert numpy.datetime64 -> pandas.Timestamp -> python datetime
	if isinstance(dt, np.datetime64):
		dt = pd.Timestamp(dt)
	if isinstance(dt, pd.Timestamp):
		dt = dt.to_pydatetime()
	if not tz is None:
		if dt.tzinfo is None:
			dt = dt.replace(tzinfo=tz)
		else:
			dt = dt.astimezone(tz)
	return dt


def DtSubtractPeriod(dt, period):
	TypeCheckDateEasy(dt, "dt")
	TypeCheckPeriod(period, "period")

	if period==yfcd.Period.Ytd:
		if isinstance(dt, datetime):
			return datetime(dt.year, 1, 1, tzinfo=dt.tzinfo)
		else:
			return date(dt.year, 1, 1)

	if period==yfcd.Period.Days1:
		rd = relativedelta(days=1)
	elif period==yfcd.Period.Days5:
		rd = relativedelta(days=7)
	elif period==yfcd.Period.Months1:
		rd = relativedelta(months=1)
	elif period==yfcd.Period.Months3:
		rd = relativedelta(months=3)
	elif period==yfcd.Period.Months6:
		rd = relativedelta(months=6)
	elif period==yfcd.Period.Years1:
		rd = relativedelta(years=1)
	elif period==yfcd.Period.Years2:
		rd = relativedelta(years=2)
	elif period==yfcd.Period.Years5:
		rd = relativedelta(years=5)
	elif period==yfcd.Period.Years10:
		rd = relativedelta(years=10)
	else:
		raise Exception("Unknown period value '{}'".format(period))

	return dt - rd


def GetSystemTz():
	dt = datetime.utcnow().astimezone()

	# tz = dt.tzinfo
	tzn = dt.tzname()
	if tzn == "BST":
		## Confirmed that ZoneInfo figures out DST
		tzn = "GB"
	tz = ZoneInfo(tzn)
	return tz

