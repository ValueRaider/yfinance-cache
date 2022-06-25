import unittest

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

import pandas as pd
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

class TestTimeUtils(unittest.TestCase):

    def setUp(self):
        self.day = date(year=2022, month=1, day=1)
        self.week_start = date(year=2022, month=1, day=3)

        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.exchangeOpenTime = time(hour=9, minute=30)
        self.exchangeCloseTime = time(hour=16, minute=0)

    def test_dt_flooring_1min(self):
        interval = yfcd.Interval.Mins1

        dates = [] ; answers = []
        for ms in [0, 1, 5, 45]:
            dates.append(  datetime.combine(self.day, time(hour=9, minute=ms, second=ms, microsecond=ms*1000), tzinfo=self.market_tz))
            answers.append(datetime.combine(self.day, time(hour=9, minute=ms), tzinfo=self.market_tz))

        for i in range(len(dates)):
            dt = dates[i]
            answer = answers[i]
            dt_floored = yfct.FloorDatetime(dt, interval)
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
        interval = yfcd.Interval.Mins2

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
            dt_floored = yfct.FloorDatetime(dt, interval)
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
        interval = yfcd.Interval.Mins5

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
            dt_floored = yfct.FloorDatetime(dt, interval)
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
        interval = yfcd.Interval.Mins15

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
            dt_floored = yfct.FloorDatetime(dt, interval)
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
        interval = yfcd.Interval.Mins30

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
            dt_floored = yfct.FloorDatetime(dt, interval)
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
        intervals = [yfcd.Interval.Mins60, yfcd.Interval.Hours1]
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
                dt_floored = yfct.FloorDatetime(dt, interval)
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
        interval = yfcd.Interval.Mins90

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
            dt_floored = yfct.FloorDatetime(dt, interval, self.exchangeOpenTime)
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
        interval = yfcd.Interval.Days1

        weekdays = [0,1,2,3,4]
        dates = [] ; answers = []
        for wd in weekdays:
            week_dt = self.week_start+timedelta(days=wd)
            dates.append(  datetime.combine(week_dt, time(hour=9,  minute=45), tzinfo=self.market_tz))
            answers.append(datetime.combine(week_dt, time(hour=9, minute=30), tzinfo=self.market_tz))

        for i in range(len(dates)):
            answer = answers[i]
            dt = dates[i]
            dt_floored = yfct.FloorDatetime(dt, interval, self.exchangeOpenTime)
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
        intervals = [yfcd.Interval.Days5, yfcd.Interval.Week]

        weekdays = [0,1,2,3,4,5,6]
        answer = datetime.combine(self.week_start, time(hour=9, minute=30), tzinfo=self.market_tz)
        dates = []
        for wd in weekdays:
            week_dt = self.week_start+timedelta(days=wd)
            dates.append(  datetime.combine(week_dt, time(hour=9,  minute=45), tzinfo=self.market_tz))
            dates.append(  datetime.combine(week_dt, time(hour=15, minute=30), tzinfo=self.market_tz))

        for interval in intervals:
            for i in range(len(dates)):
                dt = dates[i]
                dt_floored = yfct.FloorDatetime(dt, interval, self.exchangeOpenTime)
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

    def test_ConvertToDatetime(self):
        zi_utc = ZoneInfo("UTC")
        zi_usa = ZoneInfo("US/Eastern")

        dt1 = datetime.combine(date(2022, 2, 8), time(14, 30), zi_utc)
        # dt1 = dt1.astimezone(zi_usa)
        pdt = pd.Timestamp(dt1)
        
        dt2 = yfct.ConvertToDatetime(pdt, tz=zi_usa)
        try:
            self.assertEqual(dt2.astimezone(zi_utc), dt1)
        except:
            print(dt1)
            print(dt2)
            raise

if __name__ == '__main__':
    unittest.main()
