import unittest

import sys
sys.path.append("/home/gonzo/Repos/yfinance-cache/src/yfinance_cache")
from yfc_utils import *

import datetime, pytz

class Test_USMarket_DayInterval_DatetimePredictions(unittest.TestCase):

    def setUp(self):
        self.interval = Interval.Days1
        self.market = "us_market"
        self.market_tz = pytz.timezone('US/Eastern')

        self.monday = datetime.date(year=2022, month=2, day=7)

        self.early_morning = datetime.time(hour=8, minute=0)
        self.midday = datetime.time(hour=12, minute=0)
        self.late_evening = datetime.time(hour=19, minute=0)
        self.start_times = [self.early_morning, self.midday, self.late_evening]

    def test_next_day_not_weekend(self):
        # Does not return weekend
        for weekday in range(7):
            for time in self.start_times:
                lastDate = datetime.datetime.combine(self.monday+datetime.timedelta(days=weekday), time)
                nextdt = CalculateNextDataTimepoint(self.market, lastDate, self.interval)
                try:
                    self.assertNotEqual(nextdt.weekday(), 5)
                    self.assertNotEqual(nextdt.weekday(), 6)
                except:
                    print("lastDate = {0}".format(lastDate))
                    print("nextdt = {0}".format(nextdt))
                    raise

    def test_next_day_is_different(self):
        # Does not return same day:
        for weekday in range(7):
            for time in self.start_times:
                lastDate = datetime.datetime.combine(self.monday+datetime.timedelta(days=weekday), time)
                nextdt = CalculateNextDataTimepoint(self.market, lastDate, self.interval)
                try:
                    self.assertNotEqual(lastDate, nextdt)
                except:
                    print("lastDate = {0}".format(lastDate))
                    print("nextdt = {0}".format(nextdt))
                    raise

    def test_next_day_is_in_future(self):
        # Does not return same day:
        for weekday in range(7):
            for time in self.start_times:
                lastDate = datetime.datetime.combine(self.monday+datetime.timedelta(days=weekday), time, tzinfo=self.market_tz)
                nextdt = CalculateNextDataTimepoint(self.market, lastDate, self.interval)
                try:
                    self.assertTrue(nextdt > lastDate)
                except:
                    print("lastDate = {0}".format(lastDate))
                    print("nextdt = {0}".format(nextdt))
                    raise

    def test_next_day_is_nearest_weekday(self):
        # Returns soonest next weekday
        for weekday in range(7):
            for time in self.start_times:
                lastDate = datetime.datetime.combine(self.monday+datetime.timedelta(days=weekday), time)
                nextdt = CalculateNextDataTimepoint(self.market, lastDate, self.interval)

                if lastDate.weekday() >= 4:
                    answer = datetime.datetime(year=2022, month=2, day=14, hour=9, minute=30, tzinfo=self.market_tz)
                else:
                    answer = datetime.datetime(year=2022, month=2, day=7+weekday+1, hour=9, minute=30, tzinfo=self.market_tz)
                try:
                    self.assertEqual(nextdt, answer)
                except:
                    print("weekday = {0}".format(weekday))
                    print("lastDate = {0}".format(lastDate))
                    print("nextdt = {0}".format(nextdt))
                    print("answer = {0}".format(answer))
                    raise



if __name__ == '__main__':
    unittest.main()
