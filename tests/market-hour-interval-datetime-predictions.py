import unittest

import sys
sys.path.append("/home/gonzo/Repos/yfinance-cache/src/yfinance_cache")
from yfc_time import *

import datetime
from zoneinfo import ZoneInfo

class Test_USMarket_HourInterval_DatetimePredictions(unittest.TestCase):

    def setUp(self):
        self.interval = Interval.Hours1
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')

        self.monday = datetime.date(year=2022, month=2, day=7)

        self.early_morning = datetime.time(hour=8, minute=0)
        self.midday = datetime.time(hour=12, minute=0)
        self.late_evening = datetime.time(hour=19, minute=0)
        self.start_times = [self.early_morning, self.midday, self.late_evening]

    def test_next_hour_not_weekend(self):
        # Does not return weekend
        for weekday in range(5):
            for time in self.start_times:
                lastDate = datetime.datetime.combine(self.monday+datetime.timedelta(days=weekday), time, self.market_tz)
                nextdt = CalculateNextDataTimepoint(self.exchange, lastDate, self.interval)
                try:
                    self.assertNotEqual(nextdt.weekday(), 5)
                    self.assertNotEqual(nextdt.weekday(), 6)
                except:
                    print("lastDate = {0}".format(lastDate))
                    print("nextdt = {0}".format(nextdt))
                    raise

    def test_next_hour_is_different(self):
        # Does not return same day:
        for weekday in range(5):
            for time in self.start_times:
                lastDate = datetime.datetime.combine(self.monday+datetime.timedelta(days=weekday), time, self.market_tz)
                nextdt = CalculateNextDataTimepoint(self.exchange, lastDate, self.interval)
                try:
                    self.assertNotEqual(lastDate, nextdt)
                except:
                    print("lastDate = {0}".format(lastDate))
                    print("nextdt = {0}".format(nextdt))
                    raise

    def test_next_hour_is_in_future(self):
        # Does not return same day:
        for weekday in range(5):
            for time in self.start_times:
                lastDate = datetime.datetime.combine(self.monday+datetime.timedelta(days=weekday), time, self.market_tz)
                nextdt = CalculateNextDataTimepoint(self.exchange, lastDate, self.interval)
                try:
                    self.assertTrue(nextdt > lastDate)
                except:
                    print("lastDate = {0}".format(lastDate))
                    print("nextdt = {0}".format(nextdt))
                    raise

    def test_next_hour_is_nearest_hour(self):
        # Returns soonest next hour
        for weekday in range(5):
            for time in self.start_times:
                lastDate = datetime.datetime.combine(self.monday+datetime.timedelta(days=weekday), time, self.market_tz)
                nextdt = CalculateNextDataTimepoint(self.exchange, lastDate, self.interval)

                if lastDate.time() < datetime.time(hour=9, minute=30, tzinfo=self.market_tz):
                    answer = datetime.datetime.combine(self.monday+datetime.timedelta(days=weekday), datetime.time(hour=9, minute=30), self.market_tz)
                elif lastDate.time() >= datetime.time(hour=17, minute=0, tzinfo=self.market_tz):
                    answer = datetime.datetime.combine(self.monday+datetime.timedelta(days=weekday+1), datetime.time(hour=9, minute=30), self.market_tz)
                else:
                    answer = lastDate + datetime.timedelta(hours=1)
                while answer.weekday() > 4:
                    answer += datetime.timedelta(days=1)

                try:
                    self.assertEqual(nextdt.date(), answer.date())
                except:
                    print("weekday = {0}".format(weekday))
                    print("lastDate = {0}".format(lastDate))
                    print("nextdt = {0}".format(nextdt))
                    print("answer = {0}".format(answer))
                    raise

if __name__ == '__main__':
    unittest.main()
