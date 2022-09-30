import unittest

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

import pandas as pd
import numpy as np

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

## 2022 calendar:
## X* = day X is public holiday that closed exchange
##  -- April --
##  Mo   Tu   We   Th   Fr   Sa   Su
##  4    5    6    7    8    9    10
##  11   12   13   14   15*  16   17
##  18*  19   20   21   22   23   24
##  25*

class Test_Market_Intervals_ASX(unittest.TestCase):

    def setUp(self):
        self.market = "au_market"
        self.exchange = "ASX"
        self.tz = 'Australia/Sydney'
        self.market_tz = ZoneInfo(self.tz)
        self.market_open_time  = time(10,0)
        self.market_close_time = time(16,0)


    def test_GetScheduleIntervals_hourly(self):
        interval = yfcd.Interval.Hours1

        for d in [4,5,6,7,8]:
            start_d = date(2022,4,d)
            end_d   = date(2022,4,d+1)
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
            answer_times = [(time(10,0), time(11,0)), 
                            (time(11,0), time(12,0)), 
                            (time(12,0), time(13,0)), 
                            (time(13,0), time(14,0)), 
                            (time(14,0), time(15,0)), 
                            (time(15,0), time(16,0))]
            answer_times.append((time(16,0), time(16,11))) # Only ASX does this AFAIK
            left  = [datetime.combine(start_d, t1, self.market_tz) for t1,t2 in answer_times]
            right = [datetime.combine(start_d, t2, self.market_tz) for t1,t2 in answer_times]
            answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
            try:
                self.assertTrue((response==answer).all())
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise

            start_d = date(2022,4,d)
            start_dt = datetime.combine(start_d, time(11,0), tzinfo=self.market_tz)
            end_dt = datetime.combine(start_d, time(14,0), tzinfo=self.market_tz)
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt, weeklyUseYahooDef=False)
            answer_times = [(time(11,0), time(12,0)),
                            (time(12,0), time(13,0)),
                            (time(13,0), time(14,0))]
            left  = [datetime.combine(start_d, t1, self.market_tz) for t1,t2 in answer_times]
            right = [datetime.combine(start_d, t2, self.market_tz) for t1,t2 in answer_times]
            answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
            try:
                self.assertTrue((response==answer).all())
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise

            start_d = date(2022,4,d)
            start_dt = datetime.combine(start_d, time(10,30), tzinfo=self.market_tz)
            end_dt = datetime.combine(start_d, time(13,30), tzinfo=self.market_tz)
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt, weeklyUseYahooDef=False)
            answer_times = [(time(11,0), time(12,0)),
                            (time(12,0), time(13,0))]
            left  = [datetime.combine(start_d, t1, self.market_tz) for t1,t2 in answer_times]
            right = [datetime.combine(start_d, t2, self.market_tz) for t1,t2 in answer_times]
            answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
            try:
                self.assertTrue((response==answer).all())
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise

    def test_GetScheduleIntervals_dayWeek(self):
        interval = yfcd.Interval.Days1
        start_d = date(2022,4,4) # Monday
        end_d = date(2022,4,16) # next Saturday
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
        answer_days = [date(2022,4,4), date(2022,4,5), date(2022,4,6), date(2022,4,7), date(2022,4,8), 
                        date(2022,4,11), date(2022,4,12), date(2022,4,13), date(2022,4,14)]
        answer_days = np.array(answer_days)
        answer = yfcd.DateIntervalIndex.from_arrays(answer_days, answer_days+timedelta(days=1), closed="left")
        try:
            self.assertTrue(response==answer)
        except:
            print("response:")
            pprint(response)
            print("answer:")
            pprint(answer)
            raise

        intervals = [yfcd.Interval.Days5, yfcd.Interval.Week]
        start_d = date(2022,4,4) # Monday
        end_d = date(2022,4,16) # next Saturday
        answer = yfcd.DateIntervalIndex.from_arrays([date(2022,4,4),date(2022,4,11)], [date(2022,4,9),date(2022,4,15)], closed="left")
        for interval in intervals:
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
            try:
                self.assertTrue(response==answer)
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise
        # Start/end dates are in middle of week:
        start_d = date(2022,4,6) # Wednesday
        end_d = date(2022,4,20) # Wednesday
        answer = yfcd.DateIntervalIndex.from_arrays([date(2022,4,11)], [date(2022,4,15)], closed="left")
        for interval in intervals:
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
            try:
                self.assertTrue(response==answer)
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise


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
        times.append(time(10,0))
        times.append(time(13,0))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in [4,5,6,7,8]:
                day = date(2022,4,d)
                for t in times:
                    ## dt at start of interval:
                    dt = datetime.combine(day, t, self.market_tz)
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

                    ## dt in middle of interval:
                    dt = datetime.combine(day, t, self.market_tz)
                    dt += interval_td * 0.5
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
        times.append(time(10,0))
        times.append(time(15,0))
        times.append(time(16,5)) # Auction
        times.append(time(16,10)) # Auction
        for i in range(len(intervals)):
            interval = intervals[i]
            for d in [4,5,6,7,8]:
                day = date(2022,4,d)
                for t in times:
                    dt = datetime.combine(day, t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    if interval == yfcd.Interval.Days1:
                        answer = {"interval_open":day, "interval_close":day+timedelta(days=1)}
                    else:
                        answer = {"interval_open":date(2022,4,4), "interval_close":date(2022,4,9)}
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
        d = date(2022,4,19) # Tuesday after public holiday
        times = []
        times.append(time(8))
        times.append(time(14))
        times.append(time(18))
        answer = {"interval_open":d, "interval_close":d+timedelta(days=4)}
        for i in range(len(intervals)):
            interval = intervals[i]
            for d in [19,20,21,22]:
                day = date(2022,4,d)
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
        d = date(2022,4,18) # Public holiday
        times = []
        times.append(time(8))
        times.append(time(14))
        times.append(time(18))
        answer = {"interval_open":d, "interval_close":d+timedelta(days=5)}
        for i in range(len(intervals)):
            interval = intervals[i]
            for d in [18,19,20,21,22]:
                day = date(2022,4,d)
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

    def test_GetTimestampCurrentInterval_open_auction(self):
        t = time(16,10)
        intervals = []
        intervals.append(yfcd.Interval.Mins1)
        intervals.append(yfcd.Interval.Mins2)
        intervals.append(yfcd.Interval.Mins5)
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in [4,5,6,7,8]:
                day = date(2022,4,d)
                dt = datetime.combine(day, t, self.market_tz)
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                answer = {"interval_open":datetime.combine(day, t, self.market_tz), 
                         "interval_close":datetime.combine(day, time(16,11), self.market_tz)}
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

        t = time(16,0)
        intervals = []
        intervals.append(yfcd.Interval.Mins15)
        intervals.append(yfcd.Interval.Mins30)
        intervals.append(yfcd.Interval.Mins60)
        intervals.append(yfcd.Interval.Mins90)
        intervals.append(yfcd.Interval.Hours1)
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in [4,5,6,7,8]:
                day = date(2022,4,d)
                dt = datetime.combine(day, t, self.market_tz)
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                answer = {"interval_open":datetime.combine(day, t, self.market_tz), 
                         "interval_close":datetime.combine(day, time(16,11), self.market_tz)}
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

        t = time(16,5)
        intervals = []
        intervals.append(yfcd.Interval.Mins1)
        intervals.append(yfcd.Interval.Mins2)
        intervals.append(yfcd.Interval.Mins5)
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in [4,5,6,7,8]:
                day = date(2022,4,d)
                dt = datetime.combine(day, t, self.market_tz)
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                answer = None
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
        times.append(time(0,15))
        times.append(time(9,59))
        times.append(time(16,11))
        times.append(time(23,59))
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
            for d in [4,5,6,7,8]:
                day = date(2022,4,d)
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

        ## Weekend
        times = []
        times.append(time(0,0))
        times.append(time(10,0))
        times.append(time(13,0))
        times.append(time(23,59))
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
            for d in [9,10]:
                day = date(2022,4,d)
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

        ## Public holiday
        times = []
        times.append(time(0,0))
        times.append(time(10,0))
        times.append(time(13,0))
        times.append(time(23,59))
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
        day = date(2022,4,18)
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
        t = time(10,minute=0)
        ## 1) weeklyUseYahooDef=False
        for d in [16,17,18]:
            day = date(2022,4,d)
            dt = datetime.combine(day, t, self.market_tz)
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
        for d in [16,17]:
            day = date(2022,4,d)
            dt = datetime.combine(day, t, self.market_tz)
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
        d = date(2022,4,18)
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
            answer = {"interval_open":date(2022,4,18), "interval_close":date(2022,4,23)}
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

    def test_GetTimestampCurrentInterval_hourly_batch(self):
        interval = yfcd.Interval.Hours1

        dts = []
        for d in range(7,21):
            dts.append(datetime(2022,2, d, 9,29,tzinfo=self.market_tz))
            dts.append(datetime(2022,2, d, 9,30,tzinfo=self.market_tz))
            for h in range(10,16):
                dts.append(datetime(2022,2, d, h,30,tzinfo=self.market_tz))
            dts.append(datetime(2022,2, d,16, 0,tzinfo=self.market_tz))
            dts.append(datetime(2022,2, d,16, 1,tzinfo=self.market_tz))

        responses = yfct.GetTimestampCurrentInterval_batch(self.exchange, dts, interval)
        for i in range(responses.shape[0]):
            response = responses.iloc[i]
            answer = yfct.GetTimestampCurrentInterval(self.exchange, dts[i], interval)
            if answer is None:
                try:
                    self.assertTrue(pd.isnull(response["interval_open"]))
                    self.assertTrue(pd.isnull(response["interval_close"]))
                except:
                    print("Test fail with dt={}".format(dts[i]))
                    raise
            else:
                try:
                    self.assertEqual(response["interval_open"], answer["interval_open"])
                    self.assertEqual(response["interval_close"], answer["interval_close"])
                except:
                    print("Test fail with dt={}".format(dts[i]))
                    print("response:")
                    pprint(response)
                    print("answer:")
                    pprint(answer)
                    raise

    def test_GetTimestampCurrentInterval_daily_batch(self):
        interval = yfcd.Interval.Days1

        dts = []
        for d in range(7,21):
            dts.append(datetime(2022,2, d, 9,29,tzinfo=self.market_tz))
            dts.append(datetime(2022,2, d, 9,30,tzinfo=self.market_tz))
            for h in range(10,16):
                dts.append(datetime(2022,2, d, h,30,tzinfo=self.market_tz))
            dts.append(datetime(2022,2, d,16, 1,tzinfo=self.market_tz))

        responses = yfct.GetTimestampCurrentInterval_batch(self.exchange, dts, interval)
        for i in range(responses.shape[0]):
            response = responses.iloc[i]
            answer = yfct.GetTimestampCurrentInterval(self.exchange, dts[i], interval)
            if answer is None:
                try:
                    self.assertTrue(pd.isnull(response["interval_open"]))
                    self.assertTrue(pd.isnull(response["interval_close"]))
                except:
                    print("Test fail with dt={}".format(dts[i]))
                    raise
            else:
                try:
                    self.assertEqual(response["interval_open"], answer["interval_open"])
                    self.assertEqual(response["interval_close"], answer["interval_close"])
                except:
                    print("Test fail with dt={}".format(dts[i]))
                    raise

    def test_GetTimestampCurrentInterval_weekly_batch(self):
        interval = yfcd.Interval.Week

        dts = []
        for d in range(7,21):
            dts.append(datetime(2022,2, d, 9,29,tzinfo=self.market_tz))
            dts.append(datetime(2022,2, d, 9,30,tzinfo=self.market_tz))
            for h in range(10,16):
                dts.append(datetime(2022,2, d, h,30,tzinfo=self.market_tz))
            dts.append(datetime(2022,2, d,16, 1,tzinfo=self.market_tz))

        # weeklyUseYahooDef=True
        responses = yfct.GetTimestampCurrentInterval_batch(self.exchange, dts, interval, weeklyUseYahooDef=True)
        for i in range(responses.shape[0]):
            response = responses.iloc[i]
            answer = yfct.GetTimestampCurrentInterval(self.exchange, dts[i], interval, weeklyUseYahooDef=True)
            if answer is None:
                try:
                    self.assertTrue(pd.isnull(response["interval_open"]))
                    self.assertTrue(pd.isnull(response["interval_close"]))
                except:
                    print("Test fail with dt={}".format(dts[i]))
                    raise
            else:
                try:
                    self.assertEqual(response["interval_open"], answer["interval_open"])
                    self.assertEqual(response["interval_close"], answer["interval_close"])
                except:
                    print("Test fail with dt={}".format(dts[i]))
                    raise

        # weeklyUseYahooDef=False
        responses = yfct.GetTimestampCurrentInterval_batch(self.exchange, dts, interval, weeklyUseYahooDef=False)
        for i in range(responses.shape[0]):
            response = responses.iloc[i]
            answer = yfct.GetTimestampCurrentInterval(self.exchange, dts[i], interval, weeklyUseYahooDef=False)
            if answer is None:
                try:
                    self.assertTrue(pd.isnull(response["interval_open"]))
                    self.assertTrue(pd.isnull(response["interval_close"]))
                except:
                    print("Test fail with dt={}".format(dts[i]))
                    raise
            else:
                try:
                    self.assertEqual(response["interval_open"], answer["interval_open"])
                    self.assertEqual(response["interval_close"], answer["interval_close"])
                except:
                    print("Test fail with dt={}".format(dts[i]))
                    raise

if __name__ == '__main__':
    unittest.main()

    # # Run tests sequentially:
    # import inspect
    # test_src = inspect.getsource(Test_Market_Intervals_ASX)
    # unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: (
    #     test_src.index(f"def {x}") - test_src.index(f"def {y}")
    # )
    # unittest.main(verbosity=2)
