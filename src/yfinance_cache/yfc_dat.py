from enum import Enum
from datetime import timedelta
from zoneinfo import ZoneInfo

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
exchangeToMarket["LSE"] = "gb_market"
exchangeToMarket["NMS"] = "us_market"
exchangeToMarket["NYQ"] = "us_market"

marketToTimezone = {}
marketToTimezone["gb_market"] = ZoneInfo('Europe/London')
marketToTimezone["us_market"] = ZoneInfo('US/Eastern')

exchangeToMcalExchange = {}
exchangeToMcalExchange["LSE"] = "LSE"
exchangeToMcalExchange["NMS"] = "NASDAQ"
exchangeToMcalExchange["NYQ"] = "NYSE"
