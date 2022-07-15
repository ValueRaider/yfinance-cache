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

class Test_Market_Intervals_USA(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.market_close_time = time(hour=16, minute=0)

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
            for d in [14,15,16,17,18]:
                day = date(2022,2,d)
                for t in times:
                    ## dt at start of interval:
                    dt = datetime.combine(day, t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    answer = {"interval_open":datetime.combine(day, t, self.market_tz),
                             "interval_close":datetime.combine(day, t, self.market_tz)+interval_td}
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
                    dt = datetime.combine(day, t, self.market_tz) + 0.5*interval_td
                    market_close_dt = datetime.combine(day, self.market_close_time, self.market_tz)
                    if dt >= market_close_dt:
                        dt = market_close_dt - timedelta(minutes=1)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    answer = {"interval_open":datetime.combine(day, t, self.market_tz),
                             "interval_close":datetime.combine(day, t, self.market_tz)+interval_td}
                    if answer["interval_close"].time() > self.market_close_time:
                        answer["interval_close"] = datetime.combine(day, self.market_close_time, self.market_tz)
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
            for d in [14,15,16,17,18]:
                day = date(2022,2,d)
                for t in times:
                    dt = datetime.combine(day, t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    if interval == yfcd.Interval.Days1:
                        answer = {"interval_open":day,
                                "interval_close":day+timedelta(days=1)}
                    else:
                        answer = {"interval_open":date(2022,2,14),
                                "interval_close":date(2022,2,19)}
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
        day = date(2022,2,22) # Tuesday after public holiday
        times = []
        times.append(time(hour=8))
        times.append(time(hour=18))
        answer = {"interval_open":day, "interval_close":day+timedelta(days=4)}
        for i in range(len(intervals)):
            interval = intervals[i]
            for d in [22,23,24,25]:
                day = date(2022,2,d)
                for t in times:
                    dt = datetime.combine(day, t, self.market_tz)
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
        day = date(2022,2,21) # Public holiday
        times = []
        times.append(time(hour=8))
        times.append(time(hour=18))
        answer = {"interval_open":day, "interval_close":day+timedelta(days=5)}
        for i in range(len(intervals)):
            interval = intervals[i]
            for d in [21,22,23,24,25]:
                day = date(2022,2,d)
                for t in times:
                    dt = datetime.combine(day, t, self.market_tz)
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
            for d in [14,15,16,17,18]:
                day = date(2022,2,d)
                for t in times:
                    dt = datetime.combine(day, t, self.market_tz)
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
            for d in [19,20]:
                day = date(2022,2,d)
                for t in times:
                    dt = datetime.combine(day, t, self.market_tz)
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
        day = date(2022,2,21)
        for i in range(len(intervals)):
            interval = intervals[i]
            for t in times:
                dt = datetime.combine(day, t, self.market_tz)
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
        for d in [19,20,21]:
            day = date(2022,2,d)
            dt = datetime.combine(day, time(hour=10, minute=0), self.market_tz)
            for i in range(len(intervals)):
                interval = intervals[i]
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
        for d in [19,20]:
            day = date(2022,2,d)
            dt = datetime.combine(day, time(hour=10, minute=0), self.market_tz)
            for i in range(len(intervals)):
                interval = intervals[i]
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
        day = date(2022,2,21)
        for interval in intervals:
            intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, day, interval, weeklyUseYahooDef=False)
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

            intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, day, interval, weeklyUseYahooDef=True)
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

        day = date(2022,2,14)

        start_dt = datetime.combine(day, time(11,30), self.market_tz)
        end_dt   = datetime.combine(day, time(15,30), self.market_tz)
        answer = [(datetime.combine(day, time(11,30), self.market_tz), datetime.combine(day, time(12,30), self.market_tz)), 
                  (datetime.combine(day, time(12,30), self.market_tz), datetime.combine(day, time(13,30), self.market_tz)),
                  (datetime.combine(day, time(13,30), self.market_tz), datetime.combine(day, time(14,30), self.market_tz)),
                  (datetime.combine(day, time(14,30), self.market_tz), datetime.combine(day, time(15,30), self.market_tz))]
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt, weeklyUseYahooDef=False)
        try:
            self.assertEqual(response, answer)
        except:
            print("response:")
            pprint(response)
            print("answer:")
            pprint(answer)
            raise

        start_dt = datetime.combine(day, time(11, 0), self.market_tz)
        end_dt   = datetime.combine(day, time(15, 0), self.market_tz)
        answer = [(datetime.combine(day, time(11,30), self.market_tz), datetime.combine(day, time(12,30), self.market_tz)), 
                  (datetime.combine(day, time(12,30), self.market_tz), datetime.combine(day, time(13,30), self.market_tz)),
                  (datetime.combine(day, time(13,30), self.market_tz), datetime.combine(day, time(14,30), self.market_tz))]
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt, weeklyUseYahooDef=False)
        try:
            self.assertEqual(response, answer)
        except:
            print("response:")
            pprint(response)
            print("answer:")
            pprint(answer)
            raise

        start_dt = datetime.combine(day, time(11,30), self.market_tz)
        end_dt   = datetime.combine(day, time(18,30), self.market_tz)
        answer = [(datetime.combine(day, time(11,30), self.market_tz), datetime.combine(day, time(12,30), self.market_tz)), 
                  (datetime.combine(day, time(12,30), self.market_tz), datetime.combine(day, time(13,30), self.market_tz)),
                  (datetime.combine(day, time(13,30), self.market_tz), datetime.combine(day, time(14,30), self.market_tz)),
                  (datetime.combine(day, time(14,30), self.market_tz), datetime.combine(day, time(15,30), self.market_tz)),
                  (datetime.combine(day, time(15,30), self.market_tz), datetime.combine(day, time(16, 0), self.market_tz))]
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt, weeklyUseYahooDef=False)
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

        start_d = date(2022,2,14)
        end_d = date(2022,2,19)
        answer = [(date(2022,2,d),date(2022,2,d+1)) for d in [14,15,16,17,18]]
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
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
            end_d   = date(2022,2,27)
            answer = [(date(2022,2,14), date(2022,2,19)),
                      (date(2022,2,22), date(2022,2,26))]
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
            try:
                self.assertEqual(response, answer)
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise

            start_d = date(2022,2,9)
            end_d   = date(2022,3,1)
            answer = [(date(2022,2,14), date(2022,2,19)),
                      (date(2022,2,22), date(2022,2,26))]
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
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
