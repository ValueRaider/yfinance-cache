from enum import Enum
import os
# import json
# from pandas import Timestamp, to_datetime
import pandas as pd
from numpy import datetime64

from datetime import datetime, date, time, timedelta, timezone
import holidays, pytz
from tzlocal import get_localzone


class OperatingSystem(Enum):
	Windows = 1
	Linux = 2
	OSX = 3

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

def GetOperatingSystem():
	if os.name == "nt":
		return OperatingSystem.Windows
	elif os.name == "posix":
		return OperatingSystem.Linux
	else:
		raise Exception("Unknwon os.name = '{0}'".format(os.name))


def JsonEncodeValue(value):
	if isinstance(value, datetime):
		return value.isoformat()
	raise TypeError()


def JsonDecodeDict(value):
	for k in value.keys():
		try:
			value[k] = datetime.fromisoformat(value[k])
		except:
			pass
	return value


def CalculateNextDataTimepoint(market, lastDate, interval):
	# print("NewPriceDataExpected({0}, {1}, {2})".format(market, lastDate, interval))

	dt_now = datetime.now().astimezone()

	market_tz = None
	market_open = None
	market_close = None
	hdays = []
	if market == "us_market":
		market_tz = pytz.timezone('US/Eastern')
		market_open = time(9, 30, 0)
		market_close = time(16, 0, 0)
		hdays = holidays.US(years=[dt_now.year-1, dt_now.year, dt_now.year+1])
	else:
		raise Exception("Unsupported market '{0}'".format(market))

	lastDate = ConvertToDatetime(lastDate, market_tz)

	## Determine if new data is expected
	if interval == Interval.Mins1:
		next_data_timepoint = lastDate + timedelta(minutes=1)
	elif interval == Interval.Mins2:
		next_data_timepoint = lastDate + timedelta(minutes=2)
	elif interval == Interval.Mins5:
		next_data_timepoint = lastDate + timedelta(minutes=5)
	elif interval == Interval.Mins15:
		next_data_timepoint = lastDate + timedelta(minutes=15)
	elif interval == Interval.Mins30:
		next_data_timepoint = lastDate + timedelta(minutes=30)
	elif interval == Interval.Mins60 or interval == Interval.Hours1:
		next_data_timepoint = lastDate + timedelta(hours=1)
	elif interval == Interval.Mins90:
		next_data_timepoint = lastDate + timedelta(hours=1, minutes=30)
	elif interval in [Interval.Days1, Interval.Days5]:
		if interval == Interval.Days1:
			next_data_day = lastDate.date() + timedelta(days=1)
		elif interval == Interval.Days5:
			next_data_day = lastDate.date() + timedelta(days=5)
		next_data_timepoint = datetime.combine(next_data_day, market_open, market_tz)
	else:
		raise Exception("Unsupported interval '{0}'".format(interval))
	# print(" next_data_timepoint = {0}".format(next_data_timepoint))
	# print("  type = {0}".format(type(next_data_timepoint)))
	# print("  tz = {0}".format(next_data_timepoint.tzinfo))
	if next_data_timepoint.time() >= market_close:
		next_data_day = next_data_timepoint.date()
		while (next_data_day.weekday() > 4) or (next_data_day in hdays):
			next_data_day += timedelta(days=1)
		next_data_timepoint = datetime.combine(next_data_day, market_open, market_tz)
		# print(" next_data_timepoint = {0}".format(next_data_timepoint))
		# print("  type = {0}".format(type(next_data_timepoint)))
		# print("  tz = {0}".format(next_data_timepoint.tzinfo))
	return next_data_timepoint



def CalculateIntervalEndDatetime(market, intervalStart, interval):
	# print("Function({0}, {1})".format(market, intervalStart))

	dt_now = datetime.now().astimezone()

	market_tz = None
	market_open = None
	market_close = None
	hdays = []
	if market == "us_market":
		market_tz = pytz.timezone('US/Eastern')
		market_open = time(9, 30, 0)
		market_close = time(16, 0, 0)
		hdays = holidays.US(years=[dt_now.year-1, dt_now.year, dt_now.year+1])
	else:
		raise Exception("Unsupported market '{0}'".format(market))

	if interval == Interval.Mins1:
		last_data_interval_end = intervalStart + timedelta(minutes=1)
	elif interval == Interval.Mins2:
		last_data_interval_end = intervalStart + timedelta(minutes=2)
	elif interval == Interval.Mins5:
		last_data_interval_end = intervalStart + timedelta(minutes=5)
	elif interval == Interval.Mins15:
		last_data_interval_end = intervalStart + timedelta(minutes=15)
	elif interval == Interval.Mins30:
		last_data_interval_end = intervalStart + timedelta(minutes=30)
	elif interval == Interval.Mins60 or interval == Interval.Hours1:
		last_data_interval_end = intervalStart + timedelta(hours=1)
	elif interval == Interval.Mins90:
		last_data_interval_end = intervalStart + timedelta(hours=1, minutes=30)
	elif interval == Interval.Days1:
		last_data_interval_end = datetime.combine(intervalStart.date(), market_close, market_tz)-timedelta(seconds=1)
	else:
		raise Exception("Unsupported interval '{0}'".format(interval))
	# print(" last_data_interval_end = {0}".format(last_data_interval_end))
	# print("  type = {0}".format(type(last_data_interval_end)))
	# print("  tz = {0}".format(last_data_interval_end.tzinfo))

	return last_data_interval_end


def EnsureIndexHasTime(df, market, interval):
	market_open = None
	if market == "us_market":
		market_open = time(9, 30, 0)
	else:
		raise Exception("Unsupported market '{0}'".format(market))

	newIndex = []
	for idt in df.index:
		if idt.time() == time(0,0,0):
			idt = pd.to_datetime("{0} {1}".format(idt.date(), market_open))
		newIndex += [idt]
	df = df.set_index(pd.DatetimeIndex(newIndex))
	return df


def ConvertToDatetime(dt, tz=None):
	## Convert numpy.datetime64 -> pandas.Timestamp -> python datetime
	if isinstance(dt, datetime64):
		dt2 = pd.Timestamp(dt)
		# print("Converted np.datetime64 -> pd.Timestamp: {0} -> {1}".format(dt, dt2))
		dt = dt2
	if isinstance(dt, pd.Timestamp):
		dt2 = dt.to_pydatetime()
		# print("Converted pd.Timestamp -> datetime: {0} -> {1}".format(dt, dt2))
		dt = dt2
	if not tz is None:
		dt = dt.replace(tzinfo=tz)
	return dt