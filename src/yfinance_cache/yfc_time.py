from enum import Enum
import pandas as pd
from numpy import datetime64

import datetime

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
intervalToTimedelta[Interval.Mins1] = datetime.timedelta(minutes=1)
intervalToTimedelta[Interval.Mins2] = datetime.timedelta(minutes=2)
intervalToTimedelta[Interval.Mins5] = datetime.timedelta(minutes=5)
intervalToTimedelta[Interval.Mins15] = datetime.timedelta(minutes=15)
intervalToTimedelta[Interval.Mins30] = datetime.timedelta(minutes=30)
intervalToTimedelta[Interval.Mins60] = datetime.timedelta(minutes=60)
intervalToTimedelta[Interval.Mins90] = datetime.timedelta(minutes=90)
intervalToTimedelta[Interval.Hours1] = datetime.timedelta(hours=1)
intervalToTimedelta[Interval.Days1] = datetime.timedelta(days=1)
intervalToTimedelta[Interval.Days5] = datetime.timedelta(days=5)
intervalToTimedelta[Interval.Week] = datetime.timedelta(days=7)
# intervalToTimedelta[Interval.Months1] = None ## irregular time interval
# intervalToTimedelta[Interval.Months3] = None ## irregular time interval


exchangeToMarket = {}
exchangeToMarket["NMS"] = "us_market"

marketToTimezone = {}
# marketToTimezone["us_market"] = pytz.timezone('US/Eastern')
marketToTimezone["us_market"] = ZoneInfo('US/Eastern')

exchangeToMcalExchange = {}
exchangeToMcalExchange["NMS"] = "NYSE"

# Cache mcal schedules, 10x speedup:
mcalScheduleCache = {}

# def PdTimestampCombine(d, t, tz):
# 	# datetime timezone model sucks, force use pytz:
# 	if (not t.tzinfo is None) and (tz is None):
# 		raise Exception("Provide a pytz timezone object to replace datetime's timezone")
# 	t = t.replace(tzinfo=None)

# 	pd_dt = pd.Timestamp.combine(d, t).replace(tzinfo=tz)

# 	return pd_dt


def GetExchangeSchedule(exchange, start_dt, end_dt):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	if not isinstance(start_dt, datetime.date):
		raise Exception("'start_dt' must be datetime.date")
	if not isinstance(end_dt, datetime.date):
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
		## Note: don't attempt to put datetime into pd.DataFrame, pandas dies
		sched = {"market_open":opens, "market_close":closes}

	## Store in cache:
	if not exchange in mcalScheduleCache:
		mcalScheduleCache[exchange] = {}
	if not start_dt in mcalScheduleCache[exchange]:
		mcalScheduleCache[exchange][start_dt] = {}
	mcalScheduleCache[exchange][start_dt][end_dt] = sched

	return sched


def GetExchangeTimezone(exchange):
	if not exchange in exchangeToMarket:
		raise Exception("'{0}' is not an exchange".format(exchange))
	return marketToTimezone[exchangeToMarket[exchange]]


def IsTimestampInActiveSession(exchange, dt):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	# if not isinstance(dt, pd.Timestamp):
	# 	raise Exception("'dt' must be pd.Timestamp")
	if not isinstance(dt, datetime.datetime):
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
	# if not isinstance(dt, pd.Timestamp):
	# 	raise Exception("'dt' must be pd.Timestamp")
	if not isinstance(dt, datetime.datetime):
		raise Exception("'dt' must be datetime.datetime")
	if dt.tzinfo is None:
		raise Exception("'dt' must be timezone-aware")

	sched = GetExchangeSchedule(exchange, dt.date(), dt.date())
	# if sched is None or sched.shape[0] == 0:
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
	# if not isinstance(dt, pd.Timestamp):
	# 	raise Exception("'dt' must be pd.Timestamp")
	if not isinstance(dt, datetime.datetime):
		raise Exception("'dt' must be datetime.datetime")
	if dt.tzinfo is None:
		raise Exception("'dt' must be timezone-aware")

	## If 'dt' is currently in an active session then that is most recent

	s = GetTimestampCurrentSession(exchange, dt)
	if not s is None:
		return s
	sched = GetExchangeSchedule(exchange, dt.date()-datetime.timedelta(days=7), dt.date())
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
	# if not isinstance(dt, pd.Timestamp):
	# 	raise Exception("'dt' must be pd.Timestamp")
	if not isinstance(dt, datetime.datetime):
		raise Exception("'dt' must be datetime.datetime")
	if dt.tzinfo is None:
		raise Exception("'dt' must be timezone-aware")

	sched = GetExchangeSchedule(exchange, dt.date(), dt.date()+datetime.timedelta(days=7))
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
	# if not isinstance(dt, pd.Timestamp):
	# 	raise Exception("'dt' must be pd.Timestamp")
	if not isinstance(dt, datetime.date):
		raise Exception("'dt' must be date.datetime")
	if dt.tzinfo is None:
		raise Exception("'dt' must be timezone-aware")

	market = exchangeToMarket[exchange]
	tz = marketToTimezone[market]

	exchange_cal = mcal.get_calendar(exchangeToMcalExchange[exchange])
	# exchange_days = exchange_cal.valid_days(start_date=dt.date().isoformat(), end_date=dt.date().isoformat())
	exchange_days = exchange_cal.valid_days(start_date=dt.isoformat(), end_date=dt.isoformat())

	# return dt in [ed.replace(tzinfo=tz) for ed in exchange_days]
	return dt in [datetime.date(year=ed.year, month=ed.month, day=ed.day) for ed in exchange_days]


def CalculateNextDataTimepoint(exchange, dt, interval):
	if not isinstance(exchange, str):
		raise Exception("'exchange' must be str")
	# if not isinstance(dt, pd.Timestamp):
	# 	raise Exception("'dt' must be pd.Timestamp")
	if not isinstance(dt, datetime.datetime):
		raise Exception("'dt' must be datetime.datetime")
	if dt.tzinfo is None:
		raise Exception("'dt' must be timezone-aware")
	if not isinstance(interval, Interval):
		raise Exception("'interval' must be Interval")
	# print("CalculateNextDataTimepoint({0}, {1}, {2})".format(exchange, dt, interval))

	market = exchangeToMarket[exchange]
	interval_td = intervalToTimedelta[interval]

	mostRecentSession = GetTimestampCurrentSession(exchange, dt)
	if mostRecentSession is None:
		mostRecentSession = GetTimestampMostRecentSession(exchange, dt)
	if interval in [Interval.Days1, Interval.Days5]:
		lastIntervalStart = mostRecentSession["market_open"]
	else:
		if dt >= mostRecentSession["market_close"]:
			lastIntervalStart = mostRecentSession["market_close"] - interval_td
		else:
			# lastIntervalStart = dt.floor(interval_td)
			lastIntervalStart = FloorDatetime(dt, interval)

	next_interval_start = lastIntervalStart + interval_td

	if IsTimestampInActiveSession(exchange, next_interval_start):
		return next_interval_start
	else:
		return GetTimestampNextSession(exchange, next_interval_start)["market_open"]


def CalculateIntervalEndDatetime(market, intervalStart, interval):
	## TODO: redo this
	## - ensure intervalStart is rounded to interval before calculation below
	## - use mcal
	## - type checks


	# print("Function({0}, {1})".format(market, intervalStart))

	# dt_now = datetime.now().astimezone()
	dt_now = pd.Timestamp.now()

	market_tz = None
	market_open = None
	market_close = None
	hdays = []
	if market == "us_market":
		market_tz = marketToTimezone[market]
		## TODO: Lookup market open/close from module
		market_open = datetime.time(9, 30, 0)
		market_close = datetime.time(16, 0, 0)
		hdays = holidays.US(years=[dt_now.year-1, dt_now.year, dt_now.year+1])
	else:
		raise Exception("Unsupported market '{0}'".format(market))

	if interval == Interval.Mins1:
		last_data_interval_end = intervalStart + datetime.timedelta(minutes=1)
	elif interval == Interval.Mins2:
		last_data_interval_end = intervalStart + datetime.timedelta(minutes=2)
	elif interval == Interval.Mins5:
		last_data_interval_end = intervalStart + datetime.timedelta(minutes=5)
	elif interval == Interval.Mins15:
		last_data_interval_end = intervalStart + datetime.timedelta(minutes=15)
	elif interval == Interval.Mins30:
		last_data_interval_end = intervalStart + datetime.timedelta(minutes=30)
	elif interval == Interval.Mins60 or interval == Interval.Hours1:
		last_data_interval_end = intervalStart + datetime.timedelta(hours=1)
	elif interval == Interval.Mins90:
		last_data_interval_end = intervalStart + datetime.timedelta(hours=1, minutes=30)
	elif interval == Interval.Days1:
		last_data_interval_end = PdTimestampCombine(intervalStart.date(), market_close, market_tz)-datetime.timedelta(seconds=1)
	else:
		raise Exception("Unsupported interval '{0}'".format(interval))
	# print(" last_data_interval_end = {0}".format(last_data_interval_end))
	# print("  type = {0}".format(type(last_data_interval_end)))
	# print("  tz = {0}".format(last_data_interval_end.tzinfo))

	return last_data_interval_end


## TODO: unit tests
def FloorDatetime(dt, interval):
	if not isinstance(dt, datetime.datetime):
		raise Exception("'dt' must be datetime.datetime")
	if not isinstance(interval, Interval):
		raise Exception("'interval' must be Interval")

	if interval == Interval.Hours1:
		dtf = dt.replace(minute=0, second=0, microsecond=0)
	else:
		raise Exception("Implement flooring for interval: {0}".format(interval))

	return dtf


def ConvertToDatetime(dt, tz=None):
	## Convert numpy.datetime64 -> pandas.Timestamp -> python datetime
	if isinstance(dt, datetime64):
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
	dt = datetime.datetime.utcnow().astimezone()
	# tz = dt.tzinfo
	tz = ZoneInfo(dt.tzname())
	return tz

