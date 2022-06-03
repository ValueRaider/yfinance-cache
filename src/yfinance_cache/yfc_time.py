from enum import Enum
import pandas as pd

import numpy as np
def np_not(x):
	return np.logical_not(x)

from datetime import datetime, date, time, timedelta

import holidays
import pandas_market_calendars as mcal

from zoneinfo import ZoneInfo

from pprint import pprint

class Period(Enum):
	Days1 = 0
	Days5 = 1
	Months1 = 10
	Months3 = 11
	Months6 = 12
	Years1 = 20
	Years2 = 21
	Years5 = 22
	Years10 = 23
	Ytd = 24
	Max = 30
periodToString = {}
periodToString[Period.Days1] = "1d"
periodToString[Period.Days5] = "5d"
periodToString[Period.Months1] = "1m"
periodToString[Period.Months3] = "3m"
periodToString[Period.Months6] = "6m"
periodToString[Period.Years1] = "1y"
periodToString[Period.Years2] = "2y"
periodToString[Period.Years5] = "5y"
periodToString[Period.Years10] = "10y"
periodToString[Period.Ytd] = "ytd"
periodToString[Period.Max] = "max"


class Interval(Enum):
	Mins1 = 0
	Mins2 = 1
	Mins5 = 2
	Mins15 = 3
	Mins30 = 4
	Mins60 = 5
	Mins90 = 6
	Hours1 = 10
	Days1 = 20
	Days5 = 21
	Week = 30
	Months1 = 40
	Months3 = 41
intervalToString = {}
intervalToString[Interval.Mins1] = "1m"
intervalToString[Interval.Mins2] = "2m"
intervalToString[Interval.Mins5] = "5m"
intervalToString[Interval.Mins15] = "15m"
intervalToString[Interval.Mins30] = "30m"
intervalToString[Interval.Mins60] = "60m"
intervalToString[Interval.Mins90] = "90m"
intervalToString[Interval.Hours1] = "1h"
intervalToString[Interval.Days1] = "1d"
intervalToString[Interval.Days5] = "5d"
intervalToString[Interval.Week] = "1wk"
intervalToString[Interval.Months1] = "1mo"
intervalToString[Interval.Months3] = "3mo"
intervalToTimedelta = {}
intervalToTimedelta[Interval.Mins1] = timedelta(minutes=1)
intervalToTimedelta[Interval.Mins2] = timedelta(minutes=2)
intervalToTimedelta[Interval.Mins5] = timedelta(minutes=5)
intervalToTimedelta[Interval.Mins15] = timedelta(minutes=15)
intervalToTimedelta[Interval.Mins30] = timedelta(minutes=30)
intervalToTimedelta[Interval.Mins60] = timedelta(minutes=60)
intervalToTimedelta[Interval.Mins90] = timedelta(minutes=90)
intervalToTimedelta[Interval.Hours1] = timedelta(hours=1)
intervalToTimedelta[Interval.Days1] = timedelta(days=1)
intervalToTimedelta[Interval.Days5] = timedelta(days=5)
intervalToTimedelta[Interval.Week] = timedelta(days=7)
# intervalToTimedelta[Interval.Months1] = None ## irregular time interval
# intervalToTimedelta[Interval.Months3] = None ## irregular time interval


exchangeToMarket = {}
exchangeToMarket["NMS"] = "us_market"
exchangeToMarket["LSE"] = "gb_market"

marketToTimezone = {}
marketToTimezone["us_market"] = ZoneInfo('US/Eastern')
marketToTimezone["gb_market"] = ZoneInfo('Europe/London')

exchangeToMcalExchange = {}
exchangeToMcalExchange["NMS"] = "NYSE"
exchangeToMcalExchange["LSE"] = "LSE"

# Cache mcal schedules, 10x speedup:
mcalScheduleCache = {}


def GetExchangeSchedule(exchange, start_dt, end_dt):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(start_dt, date):
		raise Exception("'start_dt' must be datetime.date")
	if not isinstance(end_dt, date):
		raise Exception("'end_dt' must be datetime.date")

	market = exchangeToMarket[exchange]
	tz = marketToTimezone[market]

	## Lazy-load from cache:
	if exchange in mcalScheduleCache:
		if start_dt in mcalScheduleCache[exchange]:
			if end_dt in mcalScheduleCache[exchange][start_dt]:
				return mcalScheduleCache[exchange][start_dt][end_dt]

	exchange_cal = mcal.get_calendar(exchangeToMcalExchange[exchange])
	sched = exchange_cal.schedule(start_date=start_dt.isoformat(), end_date=end_dt.isoformat())

	if sched.shape[0] == 0:
		# sched = None
		sched = None
	else:
		# sched.index = sched.index.tz_localize(tz)
		# # sched["market_open"]  = sched["market_open" ].dt.tz_convert(tz)
		# # sched["market_close"] = sched["market_close"].dt.tz_convert(tz)
		# sched["market_open" ] = sched["market_open" ].dt.to_pydatetime()
		# sched["market_close"] = sched["market_close"].dt.to_pydatetime()

		opens = [d.to_pydatetime().astimezone(tz) for d in sched["market_open" ]]
		closes = [d.to_pydatetime().astimezone(tz) for d in sched["market_close"]]
		## Note: don't attempt to put datetime into pd.DataFrame, Pandas pukes
		sched = {"market_open":opens, "market_close":closes}

	## Store in cache:
	if not exchange in mcalScheduleCache:
		mcalScheduleCache[exchange] = {}
	if not start_dt in mcalScheduleCache[exchange]:
		mcalScheduleCache[exchange][start_dt] = {}
	mcalScheduleCache[exchange][start_dt][end_dt] = sched

	return sched


def GetScheduleIntervals(schedule, interval):
	if (not isinstance(schedule, dict)) and (schedule.keys() != ["market_close", "market_open"]):
		raise Exception("'schedule' must be a dict with two keys: ['market_close', 'market_open']")
	if not isinstance(interval, Interval):
		raise Exception("'interval' must be Interval")

	interval_td = intervalToTimedelta[interval]
	intervals = []
	opens  = schedule["market_open"]
	closes = schedule["market_close"]
	# for dt in schedule["market_open"]:
	for i in range(len(opens)):
		dt = opens[i]
		day = dt.date()
		while dt < closes[i]:
			intervals.append(dt)
			dt += interval_td

	return intervals

def GetExchangeTimezone(exchange):
	if not exchange in exchangeToMarket:
		raise Exception("'{0}' is not an exchange".format(exchange))
	return marketToTimezone[exchangeToMarket[exchange]]


def IsTimestampInActiveSession(exchange, dt):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(dt, datetime):
		raise Exception("'dt' must be datetime.datetime")
	if dt.tzinfo is None:
		raise Exception("'dt' must be timezone-aware")

	sched = GetExchangeSchedule(exchange, dt.date(), dt.date())
	# if sched is None or sched.shape[0] == 0:
	if sched is None or len(sched["market_open"]) == 0:
		return False

	# open0  = sched["market_open" ][0]
	# print("open0 = {0} (tz={1})".format(open0, open0.tzinfo))
	# close0 = sched["market_close"][0]
	# print("close0 = {0} (tz={1})".format(close0, close0.tzinfo))

	return sched["market_open"][0] <= dt and dt < sched["market_close"][0]


def GetTimestampCurrentSession(exchange, dt):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(dt, datetime):
		raise Exception("'dt' must be datetime.datetime")
	if dt.tzinfo is None:
		raise Exception("'dt' must be timezone-aware")

	sched = GetExchangeSchedule(exchange, dt.date(), dt.date())
	if sched is None or len(sched["market_open"]) == 0:
		return None

	if dt >= sched["market_open"][0] and dt < sched["market_close"][0]:
		# return sched.iloc[0]
		return {"market_open":sched["market_open"][0], "market_close":sched["market_close"][0]}
	else:
		return None


def GetTimestampMostRecentSession(exchange, dt):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(dt, datetime):
		raise Exception("'dt' must be datetime.datetime")
	if dt.tzinfo is None:
		raise Exception("'dt' must be timezone-aware")

	## If 'dt' is currently in an active session then that is most recent

	s = GetTimestampCurrentSession(exchange, dt)
	if not s is None:
		return s
	sched = GetExchangeSchedule(exchange, dt.date()-timedelta(days=7), dt.date())
	# for i in range(sched.shape[0]-1, -1, -1):
	# 	if sched["market_open"][i] <= dt:
	# 		return sched.iloc[i]
	for i in range(len(sched["market_open"])-1, -1, -1):
		if sched["market_open"][i] <= dt:
			return {"market_open":sched["market_open"][i], "market_close":sched["market_close"][i]}
	raise Exception("Failed to find most recent '{0}' session for dt = {1}".format(exchange, dt))


def GetTimestampNextSession(exchange, dt):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(dt, datetime):
		raise Exception("'dt' must be datetime.datetime")
	if dt.tzinfo is None:
		raise Exception("'dt' must be timezone-aware")

	sched = GetExchangeSchedule(exchange, dt.date(), dt.date()+timedelta(days=7))
	# for i in range(sched.shape[0]):
	# 	if dt < sched["market_open"][i]:
	# 		return sched.iloc[i]
	for i in range(len(sched["market_open"])):
		if dt < sched["market_open"][i]:
			return {"market_open":sched["market_open"][i], "market_close":sched["market_close"][i]}
	raise Exception("Failed to find next '{0}' session for dt = {1}".format(exchange, dt))


def ExchangeOpenOnDay(exchange, dt):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(dt, date):
		raise Exception("'dt' must be datetime.date")

	market = exchangeToMarket[exchange]
	tz = marketToTimezone[market]

	exchange_cal = mcal.get_calendar(exchangeToMcalExchange[exchange])
	# exchange_days = exchange_cal.valid_days(start_date=dt.date().isoformat(), end_date=dt.date().isoformat())
	exchange_days = exchange_cal.valid_days(start_date=dt.isoformat(), end_date=dt.isoformat())

	# return dt in [ed.replace(tzinfo=tz) for ed in exchange_days]
	return dt in [date(year=ed.year, month=ed.month, day=ed.day) for ed in exchange_days]



def GetTimestampCurrentInterval(exchange, dt, interval):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(dt, datetime):
		raise Exception("'dt' must be datetime.datetime")
	if dt.tzinfo is None:
		raise Exception("'dt' must be timezone-aware")
	if not isinstance(interval, Interval):
		raise Exception("'interval' must be Interval")

	if interval in [Interval.Days5, Interval.Week]:
		## Treat week intervals as special case, 
		## because will treat range as contiguous from Monday open to Friday close, 
		## even if market closed at current time.
		dt_weekStart = FloorDatetime(dt, interval)
		weekSched = GetExchangeSchedule(exchange, dt_weekStart.date(), (dt_weekStart+timedelta(days=4)).date())
		intervalStart = weekSched["market_open"][0]
		intervalEnd = weekSched["market_close"][-1]
		if dt < intervalStart or dt >= intervalEnd:
			return None
		else:
			return {"interval_open":intervalStart, "interval_close":intervalEnd}

	if IsTimestampInActiveSession(exchange, dt):
		s = GetTimestampCurrentSession(exchange, dt)
		if interval == Interval.Days1:
			intervalStart = s["market_open"]
			intervalEnd = s["market_close"]

		## If I decide that a week interval is only valid while market also open at that time, 
		## then uncomment this code.
		# elif interval in [Interval.Days5, Interval.Week]:
		# 	dt_weekStart = FloorDatetime(dt, interval)
		# 	weekSched = GetExchangeSchedule(exchange, dt_weekStart.date(), (dt_weekStart+timedelta(days=4)).date())
		# 	intervalStart = weekSched["market_open"][0]
		# 	intervalEnd = weekSched["market_close"][-1]

		else:
			intervalStart = FloorDatetime(dt, interval, s["market_open"])
			intervalEnd = intervalStart + intervalToTimedelta[interval]
			intervalEnd = min(intervalEnd, s["market_close"])
	else:
		return None

	return {"interval_open":intervalStart, "interval_close":intervalEnd}


def GetTimestampMostRecentInterval(exchange, dt, interval):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(dt, datetime):
		raise Exception("'dt' must be datetime.datetime")
	if dt.tzinfo is None:
		raise Exception("'dt' must be timezone-aware")
	if not isinstance(interval, Interval):
		raise Exception("'interval' must be Interval")

	i = GetTimestampCurrentInterval(exchange, dt, interval)

	if not i is None:
		return i
	else:
		interval_td = intervalToTimedelta[interval]
		s = GetTimestampMostRecentSession(exchange, dt)
		if interval == Interval.Days1:
			intervalStart = s["market_open"]
			intervalEnd = s["market_close"]

		elif interval in [Interval.Days5, Interval.Week]:
			dt_lastWeekStart = FloorDatetime(dt-timedelta(days=2), interval)
			lastWeekSched = GetExchangeSchedule(exchange, dt_lastWeekStart.date(), (dt_lastWeekStart+timedelta(days=4)).date())
			intervalStart = lastWeekSched["market_open"][0]
			intervalEnd = lastWeekSched["market_close"][-1]

		else:
			intervalStart = s["market_close"] - interval_td
			intervalEnd = s["market_close"]

	return {"interval_open":intervalStart, "interval_close":intervalEnd}


def GetTimestampNextInterval(exchange, dt, interval):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(dt, datetime):
		raise Exception("'dt' must be datetime.datetime")
	if dt.tzinfo is None:
		raise Exception("'dt' must be timezone-aware")
	if not isinstance(interval, Interval):
		raise Exception("'interval' must be Interval")

	if interval in [Interval.Days1, Interval.Days5, Interval.Week]:
		if interval == Interval.Days1:
			s = GetTimestampNextSession(exchange, dt)
			interval_open  = s["market_open"]
			interval_close = s["market_close"]
			# if dt >= int
		else:
			## Calculate next Monday to get next week schedule
			d = dt.date()
			wd = d.weekday()
			d += timedelta(days=7-wd)
			sched = GetExchangeSchedule(exchange, d, d+timedelta(days=4))
			interval_open  = sched["market_open"][0]
			interval_close = sched["market_close"][-1]
		return {"interval_open":interval_open, "interval_close":interval_close}

	lastInterval = GetTimestampMostRecentInterval(exchange, dt, interval)
	lastIntervalStart = lastInterval["interval_open"]
	interval_td = intervalToTimedelta[interval]
	next_interval_start = lastIntervalStart + interval_td
	if not IsTimestampInActiveSession(exchange, next_interval_start):
		s = GetTimestampNextSession(exchange, next_interval_start)
		next_interval_start = s["market_open"]
	next_interval_end = next_interval_start+interval_td
	return {"interval_open":next_interval_start, "interval_close":next_interval_end}


def FloorDatetime(dt, interval, firstIntervalStart=None):
	if not isinstance(dt, datetime):
		raise Exception("'dt' must be datetime.datetime")
	if not isinstance(interval, Interval):
		raise Exception("'interval' must be Interval")
	if not firstIntervalStart is None:
		if (not isinstance(firstIntervalStart, datetime)) and (not isinstance(firstIntervalStart, time)):
			raise Exception("'firstIntervalStart' must be datetime.datetime or .time")
		if isinstance(firstIntervalStart, datetime) and firstIntervalStart.tzinfo is None:
			raise Exception("'firstIntervalStart' if datetime.datetime must be timezone-aware")

	# print("FloorDatetime(dt={0}, interval={1}, firstIntervalStart={2})".format(dt, interval, firstIntervalStart))

	offset = None
	if not firstIntervalStart is None:
		if isinstance(firstIntervalStart, time):
			# firstIntervalStart = datetime.combine(dt.date(), firstIntervalStart)
			firstIntervalStart = datetime.combine(dt.date(), firstIntervalStart, tzinfo=dt.tzinfo)
		# offset = firstIntervalStart - FloorDatetime(firstIntervalStart, interval)
		td = firstIntervalStart - FloorDatetime(firstIntervalStart, interval)
		offset = timedelta(seconds=td.seconds, microseconds=td.microseconds)

	if not offset is None:
		dt -= offset

	if interval == Interval.Mins1:
		dtf = dt.replace(second=0, microsecond=0)

	elif interval in [Interval.Mins2, Interval.Mins5, Interval.Mins15, Interval.Mins30]:
		dtf = dt.replace(second=0, microsecond=0)
		if interval == Interval.Mins2:
			m = 2
		elif interval == Interval.Mins5:
			m = 5
		elif interval == Interval.Mins15:
			m = 15
		elif interval == Interval.Mins30:
			m = 30
		r = dtf.minute%m
		if r != 0:
			dtf = dtf.replace(minute=dtf.minute - r)

	elif interval in [Interval.Mins60, Interval.Hours1]:
		dtf = dt.replace(minute=0, second=0, microsecond=0)

	elif interval == Interval.Mins90:
		dtf = dt.replace(second=0, microsecond=0)
		r = (dtf.hour*60 + dtf.minute)%90
		if r != 0:
			dtf -= timedelta(minutes=r)

	elif interval == Interval.Days1:
		# dtf = datetime.combine(dt.date(), time(hour=0, minute=0))
		dtf = datetime.combine(dt.date(), time(hour=0, minute=0), tzinfo=dt.tzinfo)
		## Rely on offset to set time correctly

	elif interval in [Interval.Days5, Interval.Week]:
		# dtf = datetime.combine(dt.date(), time(hour=0, minute=0))
		dtf = datetime.combine(dt.date(), time(hour=0, minute=0), tzinfo=dt.tzinfo)
		## Rely on offset to set time correctly
		if dtf.weekday() != 0:
			dtf -= timedelta(days=dtf.weekday())

	else:
		raise Exception("Implement flooring for interval: {0}".format(interval))

	if not offset is None:
		dtf += offset
	# print("offset = {0}".format(offset))

	return dtf


def IsPriceDatapointExpired(intervalStart_dt, fetch_dt, max_age, exchange, interval, triggerExpiryOnClose=True, yf_lag=timedelta(seconds=15), dt_now=None):
	if not isinstance(intervalStart_dt, datetime):
		raise Exception("'intervalStart_dt' must be datetime.datetime")
	if intervalStart_dt.tzinfo is None:
		raise Exception("'intervalStart_dt' must be timezone-aware")
	if not isinstance(fetch_dt, datetime):
		raise Exception("'fetch_dt' must be datetime.datetime")
	if fetch_dt.tzinfo is None:
		raise Exception("'fetch_dt' must be timezone-aware")
	if not isinstance(max_age, timedelta):
		raise Exception("'max_age' must be timedelta")
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(interval, Interval):
		raise Exception("'interval' must be Interval")
	if not triggerExpiryOnClose is None:
		if not isinstance(triggerExpiryOnClose, bool):
			raise Exception("'triggerExpiryOnClose' must be bool")
	if not yf_lag is None:
		if not isinstance(yf_lag, timedelta):
			raise Exception("'yf_lag' must be timedelta")
	if not dt_now is None:
		if not isinstance(dt_now, datetime):
			raise Exception("'dt_now' must be datetime.datetime")
		if dt_now.tzinfo is None:
			raise Exception("'dt_now' must be timezone-aware")

	debug = False
	# debug = True

	if debug:
		print("IsPriceDatapointExpired(intervalStart_dt={0}, fetch_dt={1}, max_age={2}, dt_now={3})".format(intervalStart_dt, fetch_dt, max_age, dt_now))

	target_interval = GetTimestampCurrentInterval(exchange, intervalStart_dt, interval)
	if target_interval is None:
		raise Exception("intervalStart_dt is not in an interval")
	intervalEnd_dt = target_interval["interval_close"]
	if debug:
		print("intervalEnd_dt = {0}".format(intervalEnd_dt))
	if fetch_dt > intervalEnd_dt:
		## Interval already closed before fetch, nothing to do.
		if debug:
			print("fetch_dt > intervalEnd_dt so return FALSE")
		return False

	if dt_now is None:
		dt_now = datetime.datetime.now().astimezone()
	if debug:
		print("dt_now = {0}".format(dt_now))

	expire_dt = fetch_dt+max_age
	if debug:
		print("expire_dt = {0}".format(expire_dt))

	if expire_dt < intervalEnd_dt and expire_dt <= dt_now:
		if debug:
			print("expire_dt < intervalEnd_dt and expire_dt <= dt_now so return TRUE")
		return True

	if triggerExpiryOnClose:
		if yf_lag is None:
			yf_lag = timedelta(minutes=1)
		if dt_now >= (intervalEnd_dt+yf_lag):
			## Even though fetched data hasn't fully aged, the candle has since closed so treat as expired
			if debug:
				print("triggerExpiryOnClose and interval closed so return TRUE")
			return True

	if debug:
		print("reached end of function, returning FALSE")
	return False


def IsPriceDatapointExpired_batch(intervalStart_dts, fetch_dts, max_age, exchange, interval, triggerExpiryOnClose=True, yf_lag=timedelta(seconds=15), dt_now=None):
	if not isinstance(intervalStart_dts, list) and not isinstance(intervalStart_dts[0], datetime):
		raise Exception("'intervalStart_dts' must be list of datetime.datetime")
	if intervalStart_dts[0].tzinfo is None:
		raise Exception("'intervalStart_dts' must be timezone-aware")
	if not isinstance(fetch_dts, list) and not isinstance(fetch_dts[0], datetime):
		raise Exception("'fetch_dts' must be list of datetime.datetime")
	if fetch_dts[0].tzinfo is None:
		raise Exception("'fetch_dts' must be timezone-aware")
	if not isinstance(max_age, timedelta):
		raise Exception("'max_age' must be timedelta")
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(interval, Interval):
		raise Exception("'interval' must be Interval")
	if not triggerExpiryOnClose is None:
		if not isinstance(triggerExpiryOnClose, bool):
			raise Exception("'triggerExpiryOnClose' must be bool")
	if not yf_lag is None:
		if not isinstance(yf_lag, timedelta):
			raise Exception("'yf_lag' must be timedelta")
	if not dt_now is None:
		if not isinstance(dt_now, datetime):
			raise Exception("'dt_now' must be datetime.datetime")
		if dt_now.tzinfo is None:
			raise Exception("'dt_now' must be timezone-aware")

	debug = False
	# debug = True

	n = len(fetch_dts)

	expired = np.array([False]*n)
	fetch_dts = np.array(fetch_dts)
	# print(expired)

	if dt_now is None:
		dt_now = datetime.now().astimezone()

	intervals = list(map(GetTimestampCurrentInterval, [exchange]*n, intervalStart_dts, [interval]*n))
	interval_closes = np.array([i["interval_close"] for i in intervals])

	# interval_fetched_after_close = list(map(lambda x,y: GetTimestampCurrentInterval(exchange,x,interval)["interval_close"] <= y, intervalStart_dts, fetch_dts))
	interval_fetched_after_close = interval_closes <= fetch_dts
	if debug:
		print("interval_fetched_after_close: {0}".format(interval_fetched_after_close))
		# intervals = list(map(GetTimestampCurrentInterval, [exchange]*n,intervalStart_dts,[interval]*n))
		# print("intervals:")
		# print(intervals)

	# candleClosed = interval_closes <= dt_now
	if yf_lag is None:
		yf_lag = timedelta(minutes=1)
	candleClosed = (interval_closes+yf_lag) <= dt_now
	candleClosedSinceFetch = np.logical_and(np.logical_not(interval_fetched_after_close), candleClosed)

	## TODO: MAYBE improve performance of below by using 'interval_fetched_after_close' as a mask

	# Is expiry before now?
	expire_dts = fetch_dts + max_age
	expiry_in_past = expire_dts <= dt_now

	if debug:
		print("expire_dts: {0}".format(expire_dts))
		print("expiry_in_past: {0}".format(expiry_in_past))

	expiry_before_candle_close = expire_dts < interval_closes
	if debug:
		print("expiry_before_candle_close: {0}".format(expiry_before_candle_close))

	should_refetch = np.logical_and(expiry_in_past, expiry_before_candle_close)
	if triggerExpiryOnClose:
		should_refetch = np.logical_or(candleClosedSinceFetch, should_refetch)

	return list(should_refetch)


def IdentifyMissingIntervalRanges(exchange, start, end, interval, knownIntervals, minDistanceThreshold=5):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(start, date):
		raise Exception("'start' must be datetime.date")
	if not isinstance(end, date):
		raise Exception("'end' must be datetime.date")
	if knownIntervals != None:
		if not isinstance(knownIntervals, list) and not isinstance(knownIntervals, np.ndarray):
			raise Exception("'knownIntervals' must be list or numpy array")
		if not isinstance(knownIntervals[0], datetime):
			raise Exception("'knownIntervals' must be list of datetime.datetime")
		if knownIntervals[0].tzinfo is None:
			raise Exception("'knownIntervals' dates must be timezone-aware")

	sched = GetExchangeSchedule(exchange, start, end)
	# print("")
	# print("sched:")
	# pprint(sched)

	intervals = GetScheduleIntervals(sched, interval)
	# print("")
	# print("intervals:")
	# pprint(intervals)

	if not knownIntervals is None:
		intervals_missing_data = np_not(np.isin(intervals, knownIntervals))
	else:
		intervals_missing_data = np.array([True]*len(intervals))
	# print("")
	# print("intervals_missing_data:")
	# pprint(intervals_missing_data)

	## Merge together near ranges if the distance between is below threshold.
	## This is to reduce web requests
	i_true = np.where(intervals_missing_data==True)[0]
	# print("")
	# print("i_true:")
	# pprint(i_true)
	for i in range(len(i_true)-1):
		i0 = i_true[i]
		i1 = i_true[i+1]
		if i1-i0 <= minDistanceThreshold+1:
			## Mark all intervals between as missing, thus merging together 
			## the pair of missing ranges
			intervals_missing_data[i0+1:i1] = True
	# print("")
	# print("intervals_missing_data:")
	# pprint(intervals_missing_data)

	## Scan for contiguous sets of missing intervals:
	ranges = []
	i_true = np.where(intervals_missing_data==True)[0]
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
						r = (intervals[start], intervals[end])
						ranges.append(r)
						start = i ; end = i

			if i == (len(intervals_missing_data)-1):
				r = (intervals[start], intervals[end])
				ranges.append(r)

	if len(ranges) == 0:
		return None
	return ranges


def ConvertToDatetime(dt, tz=None):
	## Convert numpy.datetime64 -> pandas.Timestamp -> python datetime
	if isinstance(dt, np.datetime64):
		dt2 = pd.Timestamp(dt)
		dt = dt2
	if isinstance(dt, pd.Timestamp):
		dt2 = dt.to_pydatetime()
		dt = dt2
	## Update: keep pd.Timestamp, handles timezones better than datetime
	## Update 2: Python has improved timezone handling with ZoneUtil
	if not tz is None:
		dt = dt.replace(tzinfo=tz)
	return dt


def GetSystemTz():
	dt = datetime.utcnow().astimezone()
	# print("dt.tzinfo = {0}".format(dt.tzinfo))
	# print("dt.name() = {0}".format(dt.name()))

	# tz = dt.tzinfo
	tzn = dt.tzname()
	if tzn == "BST":
		## Confirmed that ZoneInfo figures out DST
		tzn = "GB"
	tz = ZoneInfo(tzn)
	return tz

