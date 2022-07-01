import unittest

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

## 2022 calendar:
## X* = day X is USA public holiday that closed NYSE
##  -- February --
##  Mo   Tu   We   Th   Fr   Sa   Su
##  7    8    9    10   11   12   13
##  14   15   16   17   18   19   20
##  21*  22   23   24   25   26   27
##  28

class Test_Market_Intervals(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.market_close_time = time(hour=16, minute=0)

        self.monday        = date(year=2022, month=2, day=14)
        self.tuesday       = date(year=2022, month=2, day=15)
        self.wednesday     = date(year=2022, month=2, day=16)
        self.thursday      = date(year=2022, month=2, day=17)
        self.friday        = date(year=2022, month=2, day=18)
        self.saturday      = date(year=2022, month=2, day=19)
        self.sunday        = date(year=2022, month=2, day=20)
        self.monday_pubHol = date(year=2022, month=2, day=21)

    def test_GetTimestampCurrentInterval_open(self):
        intervals = []
        intervals.append(yfcd.Interval.Mins1)
        intervals.append(yfcd.Interval.Mins2)
        intervals.append(yfcd.Interval.Mins5)
        intervals.append(yfcd.Interval.Mins15)
        intervals.append(yfcd.Interval.Mins30)
        intervals.append(yfcd.Interval.Mins60)
        intervals.append(yfcd.Interval.Mins90)
        intervals.append(yfcd.Interval.Hours1)
        times = []
        times.append(time(hour=9, minute=30))
        times.append(time(hour=15, minute=30))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for weekday in range(5):
                for t in times:
                    ## dt at start of interval:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    answer = {"interval_open":datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)}
                    answer["interval_close"] = answer["interval_open"]+interval_td
                    if answer["interval_close"].time() > self.market_close_time:
                        answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

                    ## dt in middle of interval:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    dt += interval_td * 0.5
                    market_close_dt = datetime.combine(dt.date(), self.market_close_time, self.market_tz)
                    if dt >= market_close_dt:
                        dt = market_close_dt - timedelta(minutes=1)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    answer = {"interval_open":datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)}
                    answer["interval_close"] = answer["interval_open"]+interval_td
                    if answer["interval_close"].time() > self.market_close_time:
                        answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

        intervals = []
        intervals.append(yfcd.Interval.Days1)
        intervals.append(yfcd.Interval.Days5)
        intervals.append(yfcd.Interval.Week)
        times = []
        times.append(time(hour=9, minute=30))
        times.append(time(hour=15, minute=30))
        for i in range(len(intervals)):
            interval = intervals[i]
            for weekday in range(5):
                for t in times:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    if interval == yfcd.Interval.Days1:
                        answer = {"interval_open":self.monday+timedelta(days=weekday),
                                "interval_close":self.tuesday+timedelta(days=weekday)}
                    else:
                        answer = {"interval_open":self.monday,
                                "interval_close":self.saturday}
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

        ## weeklyUseYahooDef = False
        intervals = []
        intervals.append(yfcd.Interval.Days5)
        intervals.append(yfcd.Interval.Week)
        d = self.monday_pubHol+timedelta(days=1) # Tuesday after public holiday
        times = []
        times.append(time(hour=8))
        times.append(time(hour=18))
        answer = {"interval_open":d, "interval_close":d+timedelta(days=4)}
        for i in range(len(intervals)):
            interval = intervals[i]
            for day in range(4):
                for t in times:
                    dt = datetime.combine(d+timedelta(days=day), t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, weeklyUseYahooDef=False)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

        ## weeklyUseYahooDef = True
        intervals = []
        intervals.append(yfcd.Interval.Days5)
        intervals.append(yfcd.Interval.Week)
        d = self.monday_pubHol
        times = []
        times.append(time(hour=8))
        times.append(time(hour=18))
        answer = {"interval_open":d, "interval_close":d+timedelta(days=5)}
        for i in range(len(intervals)):
            interval = intervals[i]
            for day in range(5):
                for t in times:
                    dt = datetime.combine(d+timedelta(days=day), t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, weeklyUseYahooDef=True)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

    def test_GetTimestampCurrentInterval_closed(self):
        answer = None

        ## Before/after market hours
        times = []
        times.append(time(hour=9, minute=29))
        times.append(time(hour=16, minute=0))
        intervals = []
        intervals.append(yfcd.Interval.Mins1)
        intervals.append(yfcd.Interval.Mins2)
        intervals.append(yfcd.Interval.Mins5)
        intervals.append(yfcd.Interval.Mins15)
        intervals.append(yfcd.Interval.Mins30)
        intervals.append(yfcd.Interval.Mins60)
        intervals.append(yfcd.Interval.Mins90)
        intervals.append(yfcd.Interval.Hours1)
        for i in range(len(intervals)):
            interval = intervals[i]
            for weekday in [0,1,2,3,4]:
                for t in times:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

        ## Weekend, at times that would be open if weekday
        times = []
        times.append(time(hour=9, minute=30))
        times.append(time(hour=15, minute=30))
        intervals = []
        intervals.append(yfcd.Interval.Mins1)
        intervals.append(yfcd.Interval.Mins2)
        intervals.append(yfcd.Interval.Mins5)
        intervals.append(yfcd.Interval.Mins15)
        intervals.append(yfcd.Interval.Mins30)
        intervals.append(yfcd.Interval.Mins60)
        intervals.append(yfcd.Interval.Mins90)
        intervals.append(yfcd.Interval.Hours1)
        intervals.append(yfcd.Interval.Days1)
        for i in range(len(intervals)):
            interval = intervals[i]
            for weekday in [5,6]:
                for t in times:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

        ## Public holiday, at times that would be open if weekday
        times = []
        times.append(time(hour=9, minute=30))
        times.append(time(hour=15, minute=30))
        intervals = []
        intervals.append(yfcd.Interval.Mins1)
        intervals.append(yfcd.Interval.Mins2)
        intervals.append(yfcd.Interval.Mins5)
        intervals.append(yfcd.Interval.Mins15)
        intervals.append(yfcd.Interval.Mins30)
        intervals.append(yfcd.Interval.Mins60)
        intervals.append(yfcd.Interval.Mins90)
        intervals.append(yfcd.Interval.Hours1)
        intervals.append(yfcd.Interval.Days1)
        for i in range(len(intervals)):
            interval = intervals[i]
            for t in times:
                dt = datetime.combine(self.monday_pubHol, t, self.market_tz)
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0}".format(dt))
                    print("intervalRange:")
                    pprint(intervalRange)
                    print("answer:")
                    pprint(answer)
                    raise

        ## Handle week-intervals separately
        intervals = []
        intervals.append(yfcd.Interval.Days5)
        intervals.append(yfcd.Interval.Week)
        ## 1) weeklyUseYahooDef=False
        dates = []
        dates.append(datetime.combine(self.saturday, time(hour=10, minute=0), self.market_tz))
        dates.append(datetime.combine(self.sunday, time(hour=10, minute=0), self.market_tz))
        dates.append(datetime.combine(self.monday_pubHol, time(hour=10, minute=0), self.market_tz))
        for i in range(len(intervals)):
            interval = intervals[i]
            for dt in dates:
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, weeklyUseYahooDef=False)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0}".format(dt))
                    print("intervalRange:")
                    pprint(intervalRange)
                    print("answer:")
                    pprint(answer)
                    raise
        ## 2) weeklyUseYahooDef=True, so self.monday_pubHol treated as in-interval
        dates = []
        dates.append(datetime.combine(self.saturday, time(hour=10, minute=0), self.market_tz))
        dates.append(datetime.combine(self.sunday, time(hour=10, minute=0), self.market_tz))
        for i in range(len(intervals)):
            interval = intervals[i]
            for dt in dates:
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, weeklyUseYahooDef=True)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0}".format(dt))
                    print("intervalRange:")
                    pprint(intervalRange)
                    print("answer:")
                    pprint(answer)
                    raise

    def test_GetTimestampCurrentInterval_weeklyOnPubHoliday(self):
        intervals = [yfcd.Interval.Days5, yfcd.Interval.Week]
        d = date(2022,2,21)
        for interval in intervals:
            intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, d, interval, weeklyUseYahooDef=False)
            answer = None
            try:
                self.assertEqual(intervalRange, answer)
            except:
                print("interval = {0}".format(interval))
                print("d = {0}".format(d))
                print("intervalRange:")
                pprint(intervalRange)
                print("answer:")
                pprint(answer)
                raise

            intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, d, interval, weeklyUseYahooDef=True)
            answer = {"interval_open":date(2022,2,21), "interval_close":date(2022,2,26)}
            try:
                self.assertEqual(intervalRange, answer)
            except:
                print("interval = {0}".format(interval))
                print("d = {0}".format(d))
                print("intervalRange:")
                pprint(intervalRange)
                print("answer:")
                pprint(answer)
                raise

    def test_GetExchangeScheduleIntervals_hourly(self):
        interval = yfcd.Interval.Hours1

        start_dt = datetime.combine(self.monday, time(11,30), self.market_tz)
        end_dt = datetime.combine(self.monday, time(15,30), self.market_tz)
        answer = [(datetime.combine(self.monday, time(11,30), self.market_tz), datetime.combine(self.monday, time(12,30), self.market_tz)), 
                  (datetime.combine(self.monday, time(12,30), self.market_tz), datetime.combine(self.monday, time(13,30), self.market_tz)),
                  (datetime.combine(self.monday, time(13,30), self.market_tz), datetime.combine(self.monday, time(14,30), self.market_tz)),
                  (datetime.combine(self.monday, time(14,30), self.market_tz), datetime.combine(self.monday, time(15,30), self.market_tz))]
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt)
        try:
            self.assertEqual(response, answer)
        except:
            print("response:")
            pprint(response)
            print("answer:")
            pprint(answer)
            raise

        start_dt = datetime.combine(self.monday, time(11,0), self.market_tz)
        end_dt = datetime.combine(self.monday, time(15,0), self.market_tz)
        answer = [(datetime.combine(self.monday, time(11,30), self.market_tz), datetime.combine(self.monday, time(12,30), self.market_tz)), 
                  (datetime.combine(self.monday, time(12,30), self.market_tz), datetime.combine(self.monday, time(13,30), self.market_tz)),
                  (datetime.combine(self.monday, time(13,30), self.market_tz), datetime.combine(self.monday, time(14,30), self.market_tz))]
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt)
        try:
            self.assertEqual(response, answer)
        except:
            print("response:")
            pprint(response)
            print("answer:")
            pprint(answer)
            raise

        start_dt = datetime.combine(self.monday, time(11,30), self.market_tz)
        end_dt = datetime.combine(self.monday, time(18,30), self.market_tz)
        answer = [(datetime.combine(self.monday, time(11,30), self.market_tz), datetime.combine(self.monday, time(12,30), self.market_tz)), 
                  (datetime.combine(self.monday, time(12,30), self.market_tz), datetime.combine(self.monday, time(13,30), self.market_tz)),
                  (datetime.combine(self.monday, time(13,30), self.market_tz), datetime.combine(self.monday, time(14,30), self.market_tz)),
                  (datetime.combine(self.monday, time(14,30), self.market_tz), datetime.combine(self.monday, time(15,30), self.market_tz)),
                  (datetime.combine(self.monday, time(15,30), self.market_tz), datetime.combine(self.monday, time(16,0), self.market_tz))]
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt)
        try:
            self.assertEqual(response, answer)
        except:
            print("response:")
            pprint(response)
            print("answer:")
            pprint(answer)
            raise

    def test_GetExchangeScheduleIntervals_daily(self):
        interval = yfcd.Interval.Days1

        start_d = self.monday
        end_d = self.friday
        answer = [(self.monday, self.tuesday),
                (self.tuesday, self.wednesday),
                (self.wednesday, self.thursday),
                (self.thursday, self.friday)]
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d)
        try:
            self.assertEqual(response, answer)
        except:
            print("response:")
            pprint(response)
            print("answer:")
            pprint(answer)
            raise

    def test_GetExchangeScheduleIntervals_weekly(self):
        intervals = [yfcd.Interval.Days5, yfcd.Interval.Week]
        for interval in intervals:
            start_d = date(2022,2,14)
            end_d = date(2022,2,27)
            answer = [(date(2022,2,14), date(2022,2,19)),
                    (date(2022,2,22), date(2022,2,26))]
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d)
            try:
                self.assertEqual(response, answer)
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise

            start_d = date(2022,2,9)
            end_d = date(2022,3,1)
            answer = [(date(2022,2,14), date(2022,2,19)),
                    (date(2022,2,22), date(2022,2,26))]
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d)
            try:
                self.assertEqual(response, answer)
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise

if __name__ == '__main__':
    unittest.main()
