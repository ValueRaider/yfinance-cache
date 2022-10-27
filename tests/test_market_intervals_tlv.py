import unittest

# import sys ; sys.path.insert(0, "/home/gonzo/ReposForks/exchange_calendars.dev")

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

import pandas as pd
import numpy as np

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

## 2022 calendar:
## X* = public holiday
##  -- March --
##  Su   Mo   Tu   We   Th   Fr   Sa
##  -    -    1    2    3    4    5
##  6    7    8    9    10   11   12
##  13   14   15   16   17*  18*  19
##  20   21   22   23   24   25   26
##  27   28   29   30   31

class Test_Market_Intervals_TLV(unittest.TestCase):

    def setUp(self):
        self.exchange = "TLV"
        self.tz = 'Asia/Jerusalem'
        self.market_tz = ZoneInfo(self.tz)
        yfct.SetExchangeTzName(self.exchange, self.tz)

        self.market_open = time(9,59)
        self.market_close = time(17,15)
        self.market_close_sunday = time(15,40)


    def test_GetScheduleIntervals_align(self):
        # Test all interval sizes, that start time = expected

        d = 6
        start_d = date(2022,3,d)
        end_d = date(2022,3,d+1)

        aligned_start_times = {}
        aligned_start_times[yfcd.Interval.Mins1]  = time(9,59)
        aligned_start_times[yfcd.Interval.Mins2]  = time(9,58)
        aligned_start_times[yfcd.Interval.Mins5]  = time(9,55)
        aligned_start_times[yfcd.Interval.Mins15] = time(9,45)
        aligned_start_times[yfcd.Interval.Mins30] = time(9,30)
        # Note: Yahoo aligns these larger intervals to 30m:
        aligned_start_times[yfcd.Interval.Mins60] = time(9,30)
        aligned_start_times[yfcd.Interval.Mins90] = time(9,30)
        aligned_start_times[yfcd.Interval.Hours1] = time(9,30)

        for interval in aligned_start_times.keys():
            t = aligned_start_times[interval]
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d)
            self.assertEqual(response[0].left.time(), t)


    def test_GetScheduleIntervals_hourly(self):
        interval = yfcd.Interval.Hours1

        for d in [6,7,8,9,10]:
            start_d = date(2022,3,d)
            end_d   = date(2022,3,d+1)

            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d)
            answer_times = [(time( 9,30), time(10,30)), 
                            (time(10,30), time(11,30)), 
                            (time(11,30), time(12,30)), 
                            (time(12,30), time(13,30)), 
                            (time(13,30), time(14,30)), 
                            (time(14,30), time(15,30))]
            if d == 6:
                # Sunday
                answer_times += [(time(15,30), time(15,51))]
            else:
                answer_times += [(time(15,30), time(16,30)), 
                                 (time(16,30), time(17,26))]
            left  = [datetime.combine(start_d, t1, self.market_tz) for t1,t2 in answer_times]
            right = [datetime.combine(start_d, t2, self.market_tz) for t1,t2 in answer_times]
            answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
            try:
                self.assertTrue((response==answer).all())
            except:
                print("response:")
                for r in response:
                    print(r)
                print("answer:")
                for r in answer:
                    print(r)
                raise

            start_dt = datetime.combine(start_d, time(11,30), tzinfo=self.market_tz)
            end_dt   = datetime.combine(start_d, time(14,30), tzinfo=self.market_tz)
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt)
            answer_times = [(time(11,30), time(12,30)),
                            (time(12,30), time(13,30)),
                            (time(13,30), time(14,30))]
            left  = [datetime.combine(start_d, t1, self.market_tz) for t1,t2 in answer_times]
            right = [datetime.combine(start_d, t2, self.market_tz) for t1,t2 in answer_times]
            answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
            try:
                self.assertTrue((response==answer).all())
            except:
                print("response:") ; pprint(response)
                print("answer:")   ; pprint(answer)
                raise

            start_dt = datetime.combine(start_d, time(11,0), tzinfo=self.market_tz)
            end_dt   = datetime.combine(start_d, time(14,0), tzinfo=self.market_tz)
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt)
            answer_times = [(time(11,30), time(12,30)),
                            (time(12,30), time(13,30))]
            left  = [datetime.combine(start_d, t1, self.market_tz) for t1,t2 in answer_times]
            right = [datetime.combine(start_d, t2, self.market_tz) for t1,t2 in answer_times]
            answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
            try:
                self.assertTrue((response==answer).all())
            except:
                print("response:") ; pprint(response)
                print("answer:")   ; pprint(answer)
                raise

    def test_GetScheduleIntervals_dayWeek(self):
        interval = yfcd.Interval.Days1
        start_d = date(2022,3,6) # Sunday
        end_d   = date(2022,3,18) # next Friday
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d)
        answer_days = [date(2022,3,6),  date(2022,3,7),  date(2022,3,8),  date(2022,3,9), date(2022,3,10),
                       date(2022,3,13), date(2022,3,14), date(2022,3,15), date(2022,3,16)]
        answer_days = np.array(answer_days)
        answer = yfcd.DateIntervalIndex.from_arrays(answer_days, answer_days+timedelta(days=1), closed="left")
        try:
            self.assertTrue(response==answer)
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise


        interval = yfcd.Interval.Week
        # weeklyUseYahooDef = False
        start_d = date(2022,3,6) # Sunday
        end_d   = date(2022,3,19) # next Saturday
        answer = yfcd.DateIntervalIndex([yfcd.DateInterval(date(2022,3,6), date(2022,3,11),closed="left"),
                                         yfcd.DateInterval(date(2022,3,13),date(2022,3,17),closed="left")])
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
        try:
            self.assertTrue(response==answer)
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        # - start/end dates are in middle of week:
        start_d = date(2022,3,1) # Tuesday
        end_d   = date(2022,3,15) # Tuesday
        answer = yfcd.DateIntervalIndex([yfcd.DateInterval(date(2022,3,6),date(2022,3,11),closed="left")])
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
        try:
            self.assertTrue(response==answer)
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        # weeklyUseYahooDef = True
        # - because TLV working weeks start Sunday, much potential for code to break with weeklyUseYahooDef=True
        start_ds = [date(2022,3,d) for d in [6,5,4]]
        end_ds   = [date(2022,3,d) for d in [19,20]]
        answer = yfcd.DateIntervalIndex([yfcd.DateInterval(date(2022,3,7),date(2022,3,14),closed="left")])
        for start_d in start_ds:
            for end_d in end_ds:
                response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=True)
                try:
                    self.assertTrue(response==answer)
                except:
                    print("response:") ; pprint(response)
                    print("answer:")   ; pprint(answer)
                    raise
        end_d = date(2022,3,21)
        answer = yfcd.DateIntervalIndex([yfcd.DateInterval(date(2022,3,7), date(2022,3,14),closed="left"),
                                         yfcd.DateInterval(date(2022,3,14),date(2022,3,21),closed="left")])
        for start_d in start_ds:
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=True)
            try:
                self.assertTrue(response==answer)
            except:
                print("response:") ; pprint(response)
                print("answer:")   ; pprint(answer)
                raise
        # - start/end dates are in middle of week:
        start_d = date(2022,3,1) # Tuesday
        end_d   = date(2022,3,15) # Tuesday
        answer = yfcd.DateIntervalIndex([yfcd.DateInterval(date(2022,3,7),date(2022,3,14),closed="left")])
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=True)
        try:
            self.assertTrue(response==answer)
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
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

        ## dt between open (9:59am) and 10am
        t = self.market_open
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            # This interval will shift back no earlier than 9:30am, matching Yahoo:
            start_shiftback = min(interval_td, timedelta(minutes=30))
            for d in [6,7,8,9,10]:
                day = date(2022,3,d)
                market_close = self.market_close_sunday if d==6 else self.market_close

                dt = datetime.combine(day, t, self.market_tz)
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)

                answer = {"interval_open":datetime.combine(day, time(10), self.market_tz)-start_shiftback}
                answer["interval_close"] = answer["interval_open"]+interval_td
                if answer["interval_close"].time() > market_close:
                    answer["interval_close"] = datetime.combine(day, market_close, self.market_tz)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

        t = time(12, 30)
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in [6,7,8,9,10]:
                day = date(2022,3,d)
                market_close = self.market_close_sunday if d==6 else self.market_close

                ## dt at start of interval:
                dt = datetime.combine(day, t, self.market_tz)
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)

                answer = {"interval_open":datetime.combine(day, time(12,30), self.market_tz), 
                         "interval_close":datetime.combine(day, time(12,30), self.market_tz)+interval_td}
                if answer["interval_close"].time() > market_close:
                    answer["interval_close"] = datetime.combine(day, market_close, self.market_tz)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

                ## dt in middle of interval:
                dt = datetime.combine(day, t, self.market_tz) + 0.5*interval_td
                market_close_dt = datetime.combine(day, market_close, self.market_tz)
                if dt >= market_close_dt:
                    dt = market_close_dt - timedelta(minutes=1)
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                answer = {"interval_open":datetime.combine(day, t, self.market_tz),
                         "interval_close":datetime.combine(day, t, self.market_tz)+interval_td}
                if answer["interval_close"].time() > market_close:
                    answer["interval_close"] = datetime.combine(day, market_close, self.market_tz)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

        ## weeklyUseYahooDef = True
        intervals = []
        intervals.append(yfcd.Interval.Days1)
        intervals.append(yfcd.Interval.Week)
        times = []
        times.append(self.market_open)
        times.append(time(15, 30))
        for i in range(len(intervals)):
            interval = intervals[i]
            if interval == yfcd.Interval.Days1:
                ds = [7,8,9,10,13]
            else:
                ds = range(7,14)
            for d in ds:
                day = date(2022,3,d)
                for t in times:
                    dt = datetime.combine(day, t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, weeklyUseYahooDef=True)
                    if interval == yfcd.Interval.Days1:
                        answer = {"interval_open":day, "interval_close":day+timedelta(days=1)}
                    else:
                        answer = {"interval_open":date(2022,3,7), "interval_close":date(2022,3,14)}
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval =", interval)
                        print("dt =", dt)
                        print("intervalRange:") ; pprint(intervalRange)
                        print("answer:")        ; pprint(answer)
                        raise

        interval = yfcd.Interval.Week
        times = []
        times.append(time(8))
        times.append(time(18))
        ## weeklyUseYahooDef = False
        ##  - with TLV, this means Sunday is start of week
        answer = {"interval_open":date(2022,3,13), "interval_close":date(2022,3,17)}
        for d in range(13,17):
            day = date(2022,3,d)
            for t in times:
                dt = datetime.combine(day, t, self.market_tz)
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, weeklyUseYahooDef=False)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise
        ## weeklyUseYahooDef = True
        answer = {"interval_open":date(2022,3,14), "interval_close":date(2022,3,21)}
        for d in range(14,21):
            day = date(2022,3,d)
            for t in times:
                dt = datetime.combine(day, t, self.market_tz)
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, weeklyUseYahooDef=True)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

    def test_GetTimestampCurrentInterval_open_auction(self):
        # Sunday:
        day = date(2022,3,6)
        auction_end = time(15,51)
        t = time(15,49)
        intervals = [] ; answers = []
        intervals.append(yfcd.Interval.Mins1)  ; answers.append(time(15,49))
        intervals.append(yfcd.Interval.Mins2)  ; answers.append(time(15,48))
        intervals.append(yfcd.Interval.Mins5)  ; answers.append(time(15,45))
        intervals.append(yfcd.Interval.Mins15) ; answers.append(time(15,45))
        intervals.append(yfcd.Interval.Mins30) ; answers.append(time(15,30))
        intervals.append(yfcd.Interval.Mins60) ; answers.append(time(15,30))
        intervals.append(yfcd.Interval.Mins90) ; answers.append(time(15,30))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            dt = datetime.combine(day, t, self.market_tz)
            intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
            answer = {"interval_open":datetime.combine(day, answers[i], self.market_tz), 
                      "interval_close":datetime.combine(day, auction_end, self.market_tz)}
            try:
                self.assertEqual(intervalRange, answer)
            except:
                print("interval =", interval)
                print("dt =", dt)
                print("intervalRange:") ; pprint(intervalRange)
                print("answer:")        ; pprint(answer)
                raise

        # Rest-of-week:
        auction_end = time(17,26)
        t = time(17,24)
        intervals = [] ; answers = []
        intervals.append(yfcd.Interval.Mins1)  ; answers.append(time(17,24))
        intervals.append(yfcd.Interval.Mins2)  ; answers.append(time(17,24))
        intervals.append(yfcd.Interval.Mins5)  ; answers.append(time(17,20))
        intervals.append(yfcd.Interval.Mins15) ; answers.append(time(17,15))
        intervals.append(yfcd.Interval.Mins30) ; answers.append(time(17,0))
        intervals.append(yfcd.Interval.Mins60) ; answers.append(time(16,30))
        intervals.append(yfcd.Interval.Mins90) ; answers.append(time(17,0))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in [7,8,9,10]:
                day = date(2022,3,d)
                dt = datetime.combine(day, t, self.market_tz)
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                answer = {"interval_open":datetime.combine(day, answers[i], self.market_tz), 
                          "interval_close":datetime.combine(day, auction_end, self.market_tz)}
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise


    def test_GetTimestampCurrentInterval_closed(self):
        answer = None

        ## Before/after market hours
        times = []
        times.append(time(9, 0))
        times.append(time(17, 30))
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
            for d in [6,7,8,9,10]:
                day = date(2022,3,d)
                for t in times:
                    if d==6 and t==time(17,30):
                        t = time(15,55)

                    dt = datetime.combine(day, t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval =", interval)
                        print("dt =", dt)
                        print("intervalRange:") ; pprint(intervalRange)
                        print("answer:")        ; pprint(answer)
                        raise

        ## Sabbath, at times that would be open if weekday
        times = []
        times.append(self.market_open)
        times.append(time(12, 30))
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
            for d in [11,12]:
                day = date(2022,3,d)
                for t in times:
                    dt = datetime.combine(day, t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval =", interval)
                        print("dt =", dt)
                        print("intervalRange:") ; pprint(intervalRange)
                        print("answer:")        ; pprint(answer)
                        raise

        ## Public holiday, at times that would be open if weekday
        times = []
        times.append(self.market_open)
        times.append(time(12, 30))
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
        day = date(2022,3,17)
        for i in range(len(intervals)):
            interval = intervals[i]
            for t in times:
                dt = datetime.combine(day, t, self.market_tz)
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

        ## Handle week-intervals separately
        intervals = yfcd.Interval.Week
        t = time(10,0)
        ## 1) weeklyUseYahooDef=False
        for d in [11,12]:
            day = date(2022,3,d)
            dt = datetime.combine(day, t, self.market_tz)
            intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, weeklyUseYahooDef=False)
            try:
                self.assertEqual(intervalRange, answer)
            except:
                print("interval =", interval)
                print("dt =", dt)
                print("intervalRange:") ; pprint(intervalRange)
                print("answer:")        ; pprint(answer)
                raise

    def test_GetTimestampCurrentInterval_weeklyOnPubHoliday(self):
        interval = yfcd.Interval.Week
        day = date(2022,3,17)
        intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, day, interval, weeklyUseYahooDef=False)
        answer = None
        try:
            self.assertEqual(intervalRange, answer)
        except:
            print("interval =", interval)
            print("dt =", dt)
            print("intervalRange:") ; pprint(intervalRange)
            print("answer:")        ; pprint(answer)
            raise

    def test_GetTimestampCurrentInterval_hourly_batch(self):
        interval = yfcd.Interval.Hours1

        dts = []
        for d in range(6,21):
            dts.append(datetime(2022,3, d, 9, 58,tzinfo=self.market_tz))
            dts.append(datetime(2022,3, d, 9, 59,tzinfo=self.market_tz))
            dts.append(datetime(2022,3, d, 10,00,tzinfo=self.market_tz))
            for h in range(10,18):
                dts.append(datetime(2022,3, d, h,30,tzinfo=self.market_tz))

        responses = yfct.GetTimestampCurrentInterval_batch(self.exchange, dts, interval)
        for i in range(responses.shape[0]):
            response = responses.iloc[i]
            answer = yfct.GetTimestampCurrentInterval(self.exchange, dts[i], interval)
            if answer is None:
                try:
                    self.assertTrue(pd.isnull(response["interval_open"]))
                    self.assertTrue(pd.isnull(response["interval_close"]))
                except:
                    print("Test fail with dt=", dts[i])
                    raise
            else:
                try:
                    self.assertEqual(response["interval_open"], answer["interval_open"])
                    self.assertEqual(response["interval_close"], answer["interval_close"])
                except:
                    print("Test fail with dt=", dts[i])
                    raise

    def test_GetTimestampCurrentInterval_daily_batch(self):
        interval = yfcd.Interval.Days1

        dts = []
        for d in range(6,21):
            dts.append(datetime(2022,3, d, 9, 58,tzinfo=self.market_tz))
            dts.append(datetime(2022,3, d, 9, 59,tzinfo=self.market_tz))
            dts.append(datetime(2022,3, d, 10,00,tzinfo=self.market_tz))
            for h in range(10,18):
                dts.append(datetime(2022,3, d, h,30,tzinfo=self.market_tz))

        responses = yfct.GetTimestampCurrentInterval_batch(self.exchange, dts, interval)
        for i in range(responses.shape[0]):
            response = responses.iloc[i]
            answer = yfct.GetTimestampCurrentInterval(self.exchange, dts[i], interval)
            if answer is None:
                try:
                    self.assertTrue(pd.isnull(response["interval_open"]))
                    self.assertTrue(pd.isnull(response["interval_close"]))
                except:
                    print("Test fail with dt=", dts[i])
                    raise
            else:
                try:
                    self.assertEqual(response["interval_open"], answer["interval_open"])
                    self.assertEqual(response["interval_close"], answer["interval_close"])
                except:
                    print("Test fail with dt=", dts[i])
                    raise

    def test_GetTimestampCurrentInterval_weekly_batch(self):
        interval = yfcd.Interval.Week

        dts = []
        for d in range(6,21):
            dts.append(datetime(2022,3, d, 9, 58,tzinfo=self.market_tz))
            dts.append(datetime(2022,3, d, 9, 59,tzinfo=self.market_tz))
            dts.append(datetime(2022,3, d, 10,00,tzinfo=self.market_tz))
            for h in range(10,18):
                dts.append(datetime(2022,3, d, h,30,tzinfo=self.market_tz))

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
                    print("Test fail with dt=", dts[i])
                    raise
            else:
                try:
                    self.assertEqual(response["interval_open"], answer["interval_open"])
                    self.assertEqual(response["interval_close"], answer["interval_close"])
                except:
                    print("Test fail with dt=", dts[i])
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
                    print("Test fail with dt=", dts[i])
                    raise
            else:
                try:
                    self.assertEqual(response["interval_open"], answer["interval_open"])
                    self.assertEqual(response["interval_close"], answer["interval_close"])
                except:
                    print("Test fail with dt=", dts[i])
                    raise


    def test_GetTimestampNextInterval_open(self):
        ## If during day session, next interval is in same session:

        t = self.market_open
        intervals = [] ; answers = []
        intervals.append(yfcd.Interval.Mins1)  ; answers.append(time(10,0))
        intervals.append(yfcd.Interval.Mins2)  ; answers.append(time(10,0))
        intervals.append(yfcd.Interval.Mins5)  ; answers.append(time(10,0))
        intervals.append(yfcd.Interval.Mins15) ; answers.append(time(10,0))
        intervals.append(yfcd.Interval.Mins30) ; answers.append(time(10,0))
        intervals.append(yfcd.Interval.Mins60) ; answers.append(time(10,30))
        intervals.append(yfcd.Interval.Hours1) ; answers.append(time(10,30))
        intervals.append(yfcd.Interval.Mins90) ; answers.append(time(11,0))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in [6,7,8,9,10]:
                dt = datetime.combine(date(2022,3,d), t, self.market_tz)
                intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
                answer = {}
                answer["interval_open"] = datetime.combine(date(2022,3,d), answers[i], self.market_tz)
                answer["interval_close"] = answer["interval_open"]+interval_td

                if d==6:
                    market_close = self.market_close_sunday
                else:
                    market_close = self.market_close
                if answer["interval_close"].time() > market_close:
                    answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)

                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

        t = time(13,30)
        intervals = [] ; answers = []
        intervals.append(yfcd.Interval.Mins1)  ; answers.append(time(13,31))
        intervals.append(yfcd.Interval.Mins2)  ; answers.append(time(13,32))
        intervals.append(yfcd.Interval.Mins5)  ; answers.append(time(13,35))
        intervals.append(yfcd.Interval.Mins15) ; answers.append(time(13,45))
        intervals.append(yfcd.Interval.Mins30) ; answers.append(time(14,0))
        intervals.append(yfcd.Interval.Mins60) ; answers.append(time(14,30))
        intervals.append(yfcd.Interval.Hours1) ; answers.append(time(14,30))
        intervals.append(yfcd.Interval.Mins90) ; answers.append(time(14,0))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in [6,7,8,9,10]:
                dt = datetime.combine(date(2022,3,d), t, self.market_tz)
                intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
                answer = {}
                answer["interval_open"] = datetime.combine(date(2022,3,d), answers[i], self.market_tz)
                answer["interval_close"] = answer["interval_open"]+interval_td

                if d==6:
                    market_close = self.market_close_sunday
                else:
                    market_close = self.market_close
                if answer["interval_close"].time() > market_close:
                    answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
                
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

        # If during the final interval of session, next interval is next day first interval
        intervals = [] ; starts = []
        intervals.append(yfcd.Interval.Mins1)  ; starts.append(time(9,59))
        intervals.append(yfcd.Interval.Mins2)  ; starts.append(time(9,58))
        intervals.append(yfcd.Interval.Mins5)  ; starts.append(time(9,55))
        intervals.append(yfcd.Interval.Mins15) ; starts.append(time(9,45))
        intervals.append(yfcd.Interval.Mins30) ; starts.append(time(9,30))
        intervals.append(yfcd.Interval.Mins60) ; starts.append(time(9,30))
        intervals.append(yfcd.Interval.Hours1) ; starts.append(time(9,30))
        intervals.append(yfcd.Interval.Mins90) ; starts.append(time(9,30))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]

            for d in [6,7,8,9,10]:
                times = []
                m = min(15, interval_td.seconds//60//2)
                if d==6:
                    times.append(time(15,49))
                    times.append(time(15,49-m))
                else:
                    times.append(time(17,24))
                    times.append(time(17,24-m))

                for t in times:
                    dt = datetime.combine(date(2022,3,d), t, self.market_tz)
                    intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

                    next_day = date(2022,3,d+1)
                    while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                        next_day += timedelta(days=1)

                    answer = {}
                    answer["interval_open"] = datetime.combine(next_day, starts[i], self.market_tz)
                    answer["interval_close"] = answer["interval_open"]+interval_td
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval =", interval)
                        print("dt =", dt)
                        print("intervalRange:") ; pprint(intervalRange)
                        print("answer:")        ; pprint(answer)
                        raise

        # If a day interval, is next working day regardless of today
        interval = yfcd.Interval.Days1
        times = []
        times.append(self.market_open)
        times.append(time(15, 30))
        # for d in [6,7,8,9,10]:
        for d in range(6,21):
            for t in times:
                dt = datetime.combine(date(2022,3,d), t, self.market_tz)
                intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
                next_day = date(2022,3,d+1)
                while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                    next_day += timedelta(days=1)
                answer = {}
                answer["interval_open"] = next_day
                answer["interval_close"] = next_day+timedelta(days=1)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

        interval = yfcd.Interval.Week
        times = []
        times.append(self.market_open)
        times.append(time(15, 30))
        ## weeklyUseYahooDef = False
        # If a week interval, is next working week regardless of today
        # Next week
        answer = {}
        answer["interval_open"] = date(2022,3,13)
        answer["interval_close"] = date(2022,3,17)
        for d in [6,7,8,9,10]:
            for t in times:
                dt = datetime.combine(date(2022,3,d), t, self.market_tz)
                intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval, weeklyUseYahooDef=False)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

        ## weeklyUseYahooDef = True
        # If a week interval, is next working week regardless of today
        # Next week
        answer = {}
        answer["interval_open"] = date(2022,3,14) # Start on Monday despite holiday
        answer["interval_close"] = date(2022,3,21)
        for d in [7,8,9,10,11,12]:
            for t in times:
                dt = datetime.combine(date(2022,3,d), t, self.market_tz)
                intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval, weeklyUseYahooDef=True)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

    def test_GetTimestampNextInterval_closed(self):
        intervals = [] ; answers = []
        intervals.append(yfcd.Interval.Mins1)  ; answers.append(time(9,59))
        intervals.append(yfcd.Interval.Mins2)  ; answers.append(time(9,58))
        intervals.append(yfcd.Interval.Mins5)  ; answers.append(time(9,55))
        intervals.append(yfcd.Interval.Mins15) ; answers.append(time(9,45))
        intervals.append(yfcd.Interval.Mins30) ; answers.append(time(9,30))
        intervals.append(yfcd.Interval.Mins60) ; answers.append(time(9,30))
        intervals.append(yfcd.Interval.Mins90) ; answers.append(time(9,30))
        intervals.append(yfcd.Interval.Hours1) ; answers.append(time(9,30))

        ## If in morning before market open, next interval next session first interval:
        t = time(9,0)
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in [6,7,8,9,10]:
                dt = datetime.combine(date(2022,3,d), t, self.market_tz)
                intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

                answer_day = date(2022,3,d)
                answer = {}
                answer["interval_open"]  = datetime.combine(answer_day, answers[i], self.market_tz)
                answer["interval_close"] = answer["interval_open"]+interval_td
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise
        ## If in afternoon after market close, next interval is next session first interval:
        intervals = []
        intervals.append(yfcd.Interval.Mins1)
        intervals.append(yfcd.Interval.Mins2)
        intervals.append(yfcd.Interval.Mins5)
        intervals.append(yfcd.Interval.Mins15)
        intervals.append(yfcd.Interval.Mins30)
        intervals.append(yfcd.Interval.Mins60)
        intervals.append(yfcd.Interval.Mins90)
        intervals.append(yfcd.Interval.Hours1)
        t = time(18,0)
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in [6,7,8,9,10]:
                dt = datetime.combine(date(2022,3,d), t, self.market_tz)
                intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

                next_day = date(2022,3,d+1)
                while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                    next_day += timedelta(days=1)
                answer = {}
                answer["interval_open"]  = datetime.combine(next_day, answers[i], self.market_tz)
                answer["interval_close"] = answer["interval_open"]+interval_td
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

        # If day interval, is next working day regardless of today
        interval = yfcd.Interval.Days1
        times = []
        times.append(time(11, 0))
        times.append(time(13, 0))
        for d in [6,7,8,9,10]:
            for t in times:
                dt = datetime.combine(date(2022,3,d), t, self.market_tz)
                intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

                next_day = date(2022,3,d+1)
                while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                    next_day += timedelta(days=1)
                answer = {}
                answer["interval_open"]  = next_day
                answer["interval_close"] = next_day+timedelta(days=1)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval =", interval)
                    print("dt =", dt)
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

        # If a week interval, is next week regardless of today
        interval = yfcd.Interval.Week
        dates = []
        dates.append(datetime.combine(date(2022,3,9),  time(12, 0), self.market_tz))
        dates.append(datetime.combine(date(2022,3,12), time(12, 0), self.market_tz))
        for dt in dates:
            intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval, weeklyUseYahooDef=True)
            answer = {}
            answer["interval_open"]  = date(2022,3,14)
            answer["interval_close"] = date(2022,3,21)
            try:
                self.assertEqual(intervalRange, answer)
            except:
                print("interval =", interval)
                print("dt =", dt)
                print("intervalRange:") ; pprint(intervalRange)
                print("answer:")        ; pprint(answer)
                raise


if __name__ == '__main__':
    unittest.main()

    # # Run tests sequentially:
    # import inspect
    # test_src = inspect.getsource(Test_Market_Intervals_TLV)
    # unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: (
    #     test_src.index(f"def {x}") - test_src.index(f"def {y}")
    # )
    # unittest.main(verbosity=2)
