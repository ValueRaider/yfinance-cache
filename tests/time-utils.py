import unittest

import sys
sys.path.append("/home/gonzo/Repos/yfinance-cache/src/yfinance_cache")
from yfc_time import *

from datetime import datetime, date, time, timedelta

class TestTimeUtils(unittest.TestCase):

    def setUp(self):
        self.day = date(year=2022, month=1, day=1)
        self.week_start = date(year=2022, month=1, day=3)

        self.exchangeOpenTime = time(hour=9, minute=30)
        self.market_tz = ZoneInfo('US/Eastern')

    def test_dt_flooring_1min(self):
        interval = Interval.Mins1

        dates = [] ; answers = []
        for ms in [0, 1, 5, 45]:
            dates.append(  datetime.combine(self.day, time(hour=9, minute=ms, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=ms), tzinfo=self.market_tz))

        for i in range(len(dates)):
            dt = dates[i]
            answer = answers[i]
            dt_floored = FloorDatetime(dt, interval)
            try:
                self.assertEqual(dt_floored, answer)
                self.assertEqual(dt.tzinfo, dt_floored.tzinfo)
            except:
                print("interval = {0}".format(interval))
                print("dt = {0}".format(dt))
                print("dt_floored = {0}".format(dt_floored))
                print("answer = {0}".format(answer))
                raise

    def test_dt_flooring_2min(self):
        interval = Interval.Mins2

        dates = [] ; answers = []
        for ms in [0, 1, 5, 45]:
            dates.append(  datetime.combine(self.day, time(hour=9, minute=0, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=0), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=1, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=0), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=2, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=2), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=59, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=58), tzinfo=self.market_tz))

        for i in range(len(dates)):
            dt = dates[i]
            answer = answers[i]
            dt_floored = FloorDatetime(dt, interval)
            try:
                self.assertEqual(dt_floored, answer)
                self.assertEqual(dt.tzinfo, dt_floored.tzinfo)
            except:
                print("interval = {0}".format(interval))
                print("dt = {0}".format(dt))
                print("dt_floored = {0}".format(dt_floored))
                print("answer = {0}".format(answer))
                raise

    def test_dt_flooring_5min(self):
        interval = Interval.Mins5

        dates = [] ; answers = []
        for ms in [0, 1, 5, 45]:
            dates.append(  datetime.combine(self.day, time(hour=9, minute=0, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=0), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=4, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=0), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=5, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=5), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=6, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=5), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=59, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=55), tzinfo=self.market_tz))

        for i in range(len(dates)):
            dt = dates[i]
            answer = answers[i]
            dt_floored = FloorDatetime(dt, interval)
            try:
                self.assertEqual(dt_floored, answer)
                self.assertEqual(dt.tzinfo, dt_floored.tzinfo)
            except:
                print("interval = {0}".format(interval))
                print("dt = {0}".format(dt))
                print("dt_floored = {0}".format(dt_floored))
                print("answer = {0}".format(answer))
                raise

    def test_dt_flooring_15min(self):
        interval = Interval.Mins15

        dates = [] ; answers = []
        for ms in [0, 1, 5, 45]:
            dates.append(  datetime.combine(self.day, time(hour=9, minute=0, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=0), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=14, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=0), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=15, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=15), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=26, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=15), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=59, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=45), tzinfo=self.market_tz))

        for i in range(len(dates)):
            dt = dates[i]
            answer = answers[i]
            dt_floored = FloorDatetime(dt, interval)
            try:
                self.assertEqual(dt_floored, answer)
                self.assertEqual(dt.tzinfo, dt_floored.tzinfo)
            except:
                print("interval = {0}".format(interval))
                print("dt = {0}".format(dt))
                print("dt_floored = {0}".format(dt_floored))
                print("answer = {0}".format(answer))
                raise

    def test_dt_flooring_30min(self):
        interval = Interval.Mins30

        dates = [] ; answers = []
        for ms in [0, 1, 5, 45]:
            dates.append(  datetime.combine(self.day, time(hour=9, minute=0, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=0), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=29, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=0), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=30, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=30), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=45, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=30), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=59, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=30), tzinfo=self.market_tz))

        for i in range(len(dates)):
            dt = dates[i]
            answer = answers[i]
            dt_floored = FloorDatetime(dt, interval)
            try:
                self.assertEqual(dt_floored, answer)
                self.assertEqual(dt.tzinfo, dt_floored.tzinfo)
            except:
                print("interval = {0}".format(interval))
                print("dt = {0}".format(dt))
                print("dt_floored = {0}".format(dt_floored))
                print("answer = {0}".format(answer))
                raise

    def test_dt_flooring_1h(self):
        intervals = [Interval.Mins60, Interval.Hours1]
        dates = []
        answer = datetime.combine(self.day, time(hour=9, minute=0, tzinfo=self.market_tz))
        for ms in [0, 1, 5, 45]:
            dates.append(  datetime.combine(self.day, time(hour=9, minute=0,  second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            dates.append(  datetime.combine(self.day, time(hour=9, minute=29, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            dates.append(  datetime.combine(self.day, time(hour=9, minute=30, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            dates.append(  datetime.combine(self.day, time(hour=9, minute=45, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            dates.append(  datetime.combine(self.day, time(hour=9, minute=59, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
        for i in range(len(dates)):
            for interval in intervals:
                dt = dates[i]
                dt_floored = FloorDatetime(dt, interval)
                try:
                    self.assertEqual(dt_floored, answer)
                    self.assertEqual(dt.tzinfo, dt_floored.tzinfo)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0}".format(dt))
                    print("dt_floored = {0}".format(dt_floored))
                    print("answer = {0}".format(answer))
                    raise

    def test_dt_flooring_90min(self):
        interval = Interval.Mins90

        dates = [] ; answers = []
        for ms in [0, 1, 5, 45]:
            dates.append(  datetime.combine(self.day, time(hour=9, minute=30, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=30), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=9, minute=45, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=30), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=10, minute=45, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=30), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=11, minute=0, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=11, minute=0), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=11, minute=45, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=11, minute=0), tzinfo=self.market_tz))

            dates.append(  datetime.combine(self.day, time(hour=12, minute=29, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=11, minute=0), tzinfo=self.market_tz))
            dates.append(  datetime.combine(self.day, time(hour=12, minute=30, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=12, minute=30), tzinfo=self.market_tz))
            dates.append(  datetime.combine(self.day, time(hour=12, minute=31, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=12, minute=30), tzinfo=self.market_tz))

        for i in range(len(dates)):
            dt = dates[i]
            answer = answers[i]
            dt_floored = FloorDatetime(dt, interval, self.exchangeOpenTime)
            try:
                self.assertEqual(dt_floored, answer)
                self.assertEqual(dt.tzinfo, dt_floored.tzinfo)
            except:
                print("interval = {0}".format(interval))
                print("dt = {0}".format(dt))
                print("dt_floored = {0}".format(dt_floored))
                print("answer = {0}".format(answer))
                raise

    def test_dt_flooring_1d(self):
        interval = Interval.Days1

        weekdays = [0,1,2,3,4]
        dates = [] ; answers = []
        for wd in weekdays:
            week_dt = self.week_start+timedelta(days=wd)
            dates.append(  datetime.combine(week_dt, time(hour=9,  minute=45), tzinfo=self.market_tz))
            answers.append(datetime.combine(week_dt, time(hour=9, minute=30), tzinfo=self.market_tz))

        for i in range(len(dates)):
            answer = answers[i]
            dt = dates[i]
            dt_floored = FloorDatetime(dt, interval, self.exchangeOpenTime)
            try:
                self.assertEqual(dt_floored, answer)
                self.assertEqual(dt.tzinfo, dt_floored.tzinfo)
            except:
                print("interval = {0}".format(interval))
                print("dt = {0}".format(dt))
                print("dt_floored = {0}".format(dt_floored))
                print("answer = {0}".format(answer))
                raise

    def test_dt_flooring_5d(self):
        intervals = [Interval.Days5, Interval.Week]

        answer = datetime.combine(self.week_start, time(hour=9, minute=30), tzinfo=self.market_tz)
        weekdays = [0,1,2,3,4]
        dates = []
        for wd in weekdays:
            week_dt = self.week_start+timedelta(days=wd)
            dates.append(  datetime.combine(week_dt, time(hour=9,  minute=45), tzinfo=self.market_tz))
            dates.append(  datetime.combine(week_dt, time(hour=15, minute=30), tzinfo=self.market_tz))

        for interval in intervals:
            for i in range(len(dates)):
                dt = dates[i]
                dt_floored = FloorDatetime(dt, interval, self.exchangeOpenTime)
                try:
                    self.assertEqual(dt_floored, answer)
                    self.assertEqual(dt.tzinfo, dt_floored.tzinfo)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0}".format(dt))
                    print("dt_floored = {0}".format(dt_floored))
                    print("answer = {0}".format(answer))
                    raise

    # def test_dt_flooring_1month(self):
    #     raise Exception("test_dt_flooring_1month() not implemented")

    # def test_dt_flooring_3months(self):
    #     raise Exception("test_dt_flooring_3months() not implemented")

if __name__ == '__main__':
    unittest.main()