import unittest
from pprint import pprint

import yfinance as yf

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct
from .context import yfc_utils as yfcu
from .context import session_gbl

from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo

import appdirs
import requests_cache

import sys, os

class TestYfAssumptions(unittest.TestCase):
	def setUp(self):
		self.tkr = "INTC"

		self.session = session_gbl

		self.dat = yf.Ticker(self.tkr, session=self.session)

		self.day = date(year=2024, month=1, day=10)

		self.exchange = "NMS"
		self.market_tz = ZoneInfo('US/Eastern')
		self.exchangeOpenTime = time(hour=9, minute=30)
		self.exchangeLastHrInt = time(hour=15, minute=30)
		self.exchangeCloseTime = time(hour=16, minute=0)

	def tearDown(self):
		self.session.close()


	def test_minutes(self):
		i = "1m"

		## For minute data must use dates within last 30 days
		day = datetime.today().date()
		day -= timedelta(days=1)
		while not yfct.ExchangeOpenOnDay(self.exchange, day):
			day -= timedelta(days=1)
		sched = yfct.GetExchangeSchedule(self.exchange, day, day+timedelta(days=1))

		startDt = sched["open"].iloc[0]
		endDt   = sched["open"].iloc[0]+timedelta(minutes=1)
		df = self.dat.history(interval=i, start=startDt, end=endDt+timedelta(minutes=5))
		df = df[df.index<endDt]
		intervals = list(df.index.to_pydatetime())
		answers = [sched["open"].iloc[0]]
		try:
			self.assertEqual(intervals, answers)
		except:
			print("df:")
			print(df)
			print("answer:")
			print(answers)
			raise


	def test_hours(self):
		i = "1h"

		startDt = datetime.combine(self.day, self.exchangeOpenTime,  tzinfo=self.market_tz)
		endDt   = datetime.combine(self.day, self.exchangeCloseTime, tzinfo=self.market_tz)
		df = self.dat.history(interval=i, start=startDt, end=endDt)
		df = df[df.index<=endDt]
		intervals = df.index.to_pydatetime()
		intervals = list(intervals)
		answers = []
		for h in [0,1,2,3,4,5,6]:
			answers.append(datetime.combine(self.day, self.exchangeOpenTime, tzinfo=self.market_tz)+timedelta(hours=h))
		self.assertEqual(intervals, answers)

		startDt = datetime.combine(self.day, self.exchangeOpenTime, tzinfo=self.market_tz)
		endDt   = datetime.combine(self.day, self.exchangeOpenTime, tzinfo=self.market_tz)+timedelta(hours=1)
		df = self.dat.history(interval=i, start=startDt, end=endDt)
		df = df[df.index<=endDt]
		intervals = df.index.to_pydatetime()
		intervals = list(intervals)
		answers = []
		answers.append(datetime.combine(self.day, self.exchangeOpenTime, tzinfo=self.market_tz))
		self.assertEqual(intervals, answers)


if __name__ == '__main__':
    unittest.main()

    # # Run tests sequentially:
    # import inspect
    # test_src = inspect.getsource(TestYfAssumptions)
    # unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: (
    #     test_src.index(f"def {x}") - test_src.index(f"def {y}")
    # )
    # unittest.main(verbosity=2)
