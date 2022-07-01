import unittest
from pprint import pprint

import yfinance as yf

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo

import requests_cache

import sys

class TestYfAssumptions(unittest.TestCase):
	def setUp(self):
		self.tkr = "INTC"

		self.session = None
		self.session = requests_cache.CachedSession('yfinance.cache')
		self.session.headers['User-agent'] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0"

		self.dat = yf.Ticker(self.tkr, session=self.session)

		self.day = date(year=2022, month=1, day=10)

		self.exchange = "NMS"
		self.market_tz = ZoneInfo('US/Eastern')
		self.exchangeOpenTime = time(hour=9, minute=30)
		self.exchangeLastHrInt = time(hour=15, minute=30)
		self.exchangeCloseTime = time(hour=16, minute=0)


	def test_minutes(self):
		i = "1m"

		## For minute data must use dates within last 30 days
		day = datetime.today().date()
		day -= timedelta(days=1)
		while not yfct.ExchangeOpenOnDay(self.exchange, day):
			day -= timedelta(days=1)
		sched = yfct.GetExchangeSchedule(self.exchange, day, day+timedelta(days=1))

		startDt = sched["market_open"][0]
		endDt   = sched["market_open"][0]+timedelta(minutes=1)
		# endDt   = sched["market_open"][0]+timedelta(minutes=2)
		df = self.dat.history(interval=i, start=startDt.astimezone(ZoneInfo("UTC")), end=endDt.astimezone(ZoneInfo("UTC")))
		df = df[df.index<=endDt]
		intervals = list(df.index.to_pydatetime())
		answers = [sched["market_open"][0]]
		self.assertEqual(intervals, answers)


	def test_hours(self):
		i = "1h"

		startDt = datetime.combine(self.day, self.exchangeOpenTime,  tzinfo=self.market_tz)
		endDt   = datetime.combine(self.day, self.exchangeCloseTime, tzinfo=self.market_tz)
		df = self.dat.history(interval=i, start=startDt.astimezone(ZoneInfo("UTC")), end=endDt.astimezone(ZoneInfo("UTC")), tz=None)
		df = df[df.index<=endDt]
		intervals = df.index.to_pydatetime()
		intervals = list(intervals)
		answers = []
		for h in [0,1,2,3,4,5,6]:
			answers.append(datetime.combine(self.day, self.exchangeOpenTime, tzinfo=self.market_tz)+timedelta(hours=h))
		self.assertEqual(intervals, answers)

		startDt = datetime.combine(self.day, self.exchangeOpenTime, tzinfo=self.market_tz)
		endDt   = datetime.combine(self.day, self.exchangeOpenTime, tzinfo=self.market_tz)+timedelta(hours=1)
		df = self.dat.history(interval=i, start=startDt.astimezone(ZoneInfo("UTC")), end=endDt.astimezone(ZoneInfo("UTC")), tz=None)
		df = df[df.index<=endDt]
		intervals = df.index.to_pydatetime()
		intervals = list(intervals)
		answers = []
		answers.append(datetime.combine(self.day, self.exchangeOpenTime, tzinfo=self.market_tz))
		self.assertEqual(intervals, answers)

	def tearDown(self):
		self.session.close()

if __name__ == '__main__':
    unittest.main()
