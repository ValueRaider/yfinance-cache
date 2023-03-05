import unittest

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

import pandas as pd
import numpy as np

from datetime import datetime, date, time, timedelta
dtc = datetime.combine
from zoneinfo import ZoneInfo

from pprint import pprint

# 2022 calendar:
# X* = day X is USA public holiday that closed NYSE
#  -- February --
#  Mo   Tu   We   Th   Fr   Sa   Su
#  7    8    9    10   11   12   13
#  14   15   16   17   18   19   20
#  21*  22   23   24   25   26   27
#  28


class Test_Market_Intervals_USA(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('America/New_York')

        self.market_open = time(9, 30)
        self.market_close = time(16)

        self.td_1d = timedelta(days=1)

    def test_GetScheduleIntervals_hourly(self):
        interval = yfcd.Interval.Hours1

        for d in range(14, 19):
            start_d = date(2022, 2, d)
            end_d = date(2022, 2, d+1)

            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d)
            answer_times = [(time(9, 30), time(10, 30)),
                            (time(10, 30), time(11, 30)),
                            (time(11, 30), time(12, 30)),
                            (time(12, 30), time(13, 30)),
                            (time(13, 30), time(14, 30)),
                            (time(14, 30), time(15, 30)),
                            (time(15, 30), time(16))]
            left = [dtc(start_d, t1, self.market_tz) for t1, t2 in answer_times]
            right = [dtc(start_d, t2, self.market_tz) for t1, t2 in answer_times]
            answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
            try:
                self.assertTrue(response.equals(answer))
            except:
                print("response:") ; pprint(response)
                print("answer:")   ; pprint(answer)
                raise

            start_dt = dtc(start_d, time(11, 30), self.market_tz)
            end_dt = dtc(start_d, time(14, 30), self.market_tz)
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt)
            answer_times = [(time(11, 30), time(12, 30)),
                            (time(12, 30), time(13, 30)),
                            (time(13, 30), time(14, 30))]
            left = [dtc(start_d, t1, self.market_tz) for t1, t2 in answer_times]
            right = [dtc(start_d, t2, self.market_tz) for t1, t2 in answer_times]
            answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
            try:
                self.assertTrue(response.equals(answer))
            except:
                print("response:") ; pprint(response)
                print("answer:")   ; pprint(answer)
                raise

            start_dt = dtc(start_d, time(11), self.market_tz)
            end_dt = dtc(start_d, time(14), self.market_tz)
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt)
            answer_times = [(time(11, 30), time(12, 30)),
                            (time(12, 30), time(13, 30))]
            left = [dtc(start_d, t1, self.market_tz) for t1, t2 in answer_times]
            right = [dtc(start_d, t2, self.market_tz) for t1, t2 in answer_times]
            answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
            try:
                self.assertTrue(response.equals(answer))
            except:
                print("response:") ; pprint(response)
                print("answer:")   ; pprint(answer)
                raise

    def test_GetScheduleIntervals_daily(self):
        interval = yfcd.Interval.Days1

        discardTimes = True
        start_d = date(2022, 2, 14)  # Monday
        end_d = date(2022, 2, 26)  # next Saturday
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, discardTimes)
        answer_days = [date(2022, 2, 14), date(2022, 2, 15), date(2022, 2, 16), date(2022, 2, 17), date(2022, 2, 18),  # Mondays
                                          date(2022, 2, 22), date(2022, 2, 23), date(2022, 2, 24), date(2022, 2, 25)]  # Mondays
        answer_days = np.array(answer_days)
        answer = yfcd.DateIntervalIndex.from_arrays(answer_days, answer_days+self.td_1d, closed="left")
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise

        discardTimes = False
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, discardTimes)
        left = [dtc(d, self.market_open, self.market_tz) for d in answer_days]
        right = [dtc(d, self.market_close, self.market_tz) for d in answer_days]
        answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise

    def test_GetScheduleIntervals_weekly(self):
        interval = yfcd.Interval.Week

        # Simple case: 2 full weeks
        start_d = date(2022, 2, 14)  # Monday
        end_d = date(2022, 2, 28)  # Monday + 2 weeks
        week7days = False ; discardTimes = False
        left = [dtc(date(2022, 2, 14), self.market_open, self.market_tz),  # Monday open
                dtc(date(2022, 2, 22), self.market_open, self.market_tz)]  # Tuesday open (Monday holiday)
        right = [dtc(date(2022, 2, 18), self.market_close, self.market_tz),  # Friday close
                 dtc(date(2022, 2, 25), self.market_close, self.market_tz)]  # Friday close
        answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, discardTimes, week7days)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        week7days = False ; discardTimes = True
        left = [date(2022, 2, 14), date(2022, 2, 22)]  # Monday, next Tuesday (Monday holiday)
        right = [date(2022, 2, 19), date(2022, 2, 26)]  # Saturdays
        answer = yfcd.DateIntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, discardTimes, week7days)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        week7days = True ; discardTimes = True
        answer = yfcd.DateIntervalIndex([yfcd.DateInterval(date(2022, 2, 14), date(2022, 2, 21), closed="left"),  # Mondays
                                         yfcd.DateInterval(date(2022, 2, 21), date(2022, 2, 28), closed="left")])  # Mondays
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, discardTimes, week7days)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise

        # Weekend cut off:
        start_d = date(2022, 2, 14)  # Monday
        end_d = date(2022, 2, 26)  # Saturday
        week7days = False ; discardTimes = False
        left = [dtc(date(2022, 2, 14), self.market_open, self.market_tz),  # Monday open
                dtc(date(2022, 2, 22), self.market_open, self.market_tz)]  # Tuesday open (Monday holiday)
        right = [dtc(date(2022, 2, 18), self.market_close, self.market_tz),  # Friday close
                 dtc(date(2022, 2, 25), self.market_close, self.market_tz)]  # Friday close
        answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, discardTimes, week7days)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        week7days = False ; discardTimes = True
        left = [date(2022, 2, 14), date(2022, 2, 22)]  # Monday, next Tuesday (Monday holiday)
        right = [date(2022, 2, 19), date(2022, 2, 26)]  # Saturdays
        answer = yfcd.DateIntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, discardTimes, week7days)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        week7days = True ; discardTimes = True
        left = [date(2022, 2, 14)]  # Monday
        right = [date(2022, 2, 21)]  # Monday
        answer = yfcd.DateIntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, discardTimes, week7days)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise

        # Monday holiday
        start_d = date(2022, 2, 22)  # Tuesday (Monday holiday)
        end_d = date(2022, 2, 28)  # next Monday
        week7days = False ; discardTimes = True
        left = [date(2022, 2, 22)]  # Tuesday (Monday holiday)
        right = [date(2022, 2, 26)]  # Saturday
        answer = yfcd.DateIntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, discardTimes, week7days)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        week7days = True ; discardTimes = True
        answer = None
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise

    def test_GetTimestampCurrentInterval_open(self):
        # Intraday
        intervals = []
        intervals.append(yfcd.Interval.Mins1)
        intervals.append(yfcd.Interval.Mins2)
        intervals.append(yfcd.Interval.Mins5)
        intervals.append(yfcd.Interval.Mins15)
        intervals.append(yfcd.Interval.Mins30)
        intervals.append(yfcd.Interval.Mins60)
        intervals.append(yfcd.Interval.Mins90)
        intervals.append(yfcd.Interval.Hours1)
        times = [self.market_open, time(15, 30)]
        for interval in intervals:
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in range(14, 19):
                day = date(2022, 2, d)
                for t in times:
                    # dt at start of interval:
                    dt = dtc(day, t, self.market_tz)
                    response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    answer = {"interval_open": dtc(day, t, self.market_tz),
                              "interval_close": dtc(day, t, self.market_tz) + interval_td}
                    if answer["interval_close"].time() > self.market_close:
                        answer["interval_close"] = dtc(day, self.market_close, self.market_tz)
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} dt={dt}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise

                    # dt in middle of interval:
                    dt = dtc(day, t, self.market_tz) + 0.5*interval_td
                    market_close_dt = dtc(day, self.market_close, self.market_tz)
                    if dt >= market_close_dt:
                        dt = market_close_dt - timedelta(minutes=1)
                    response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    answer = {"interval_open": dtc(day, t, self.market_tz),
                              "interval_close": dtc(day, t, self.market_tz) + interval_td}
                    if answer["interval_close"].time() > self.market_close:
                        answer["interval_close"] = dtc(day, self.market_close, self.market_tz)
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} dt={dt}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise

        # Daily
        discardTimes = True
        interval = yfcd.Interval.Days1
        times = [time(4), self.market_open, time(15, 30), time(20)]
        for d in range(22, 26):
            day = date(2022, 2, d)

            answer = {"interval_open": day, "interval_close": day+self.td_1d}
            response = yfct.GetTimestampCurrentInterval(self.exchange, day, interval, discardTimes)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} day={day}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise

            for t in times:
                dt = dtc(day, t, self.market_tz)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        discardTimes = False
        interval = yfcd.Interval.Days1
        times = [self.market_open, time(15, 30)]
        for d in range(22, 26):
            day = date(2022, 2, d)
            answer = {"interval_open": dtc(day, self.market_open, self.market_tz),
                      "interval_close": dtc(day, self.market_close, self.market_tz)}
            for t in times:
                dt = dtc(day, t, self.market_tz)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

        # Weekly
        interval = yfcd.Interval.Week
        discardTimes = True ; week7days = True
        times = [time(4), self.market_open, time(15, 30), time(20)]
        answer = {"interval_open": date(2022, 2, 21), "interval_close": date(2022, 2, 28)}  # Monday -> next Monday
        for d in range(21, 28):
            day = date(2022, 2, d)

            response = yfct.GetTimestampCurrentInterval(self.exchange, day, interval, discardTimes, week7days)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} day={day}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise

            for t in times:
                dt = dtc(day, t, self.market_tz)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        discardTimes = True ; week7days = False
        answer = {"interval_open": date(2022, 2, 22), "interval_close": date(2022, 2, 26)}  # Tuesday (Monday holiday) -> Saturday
        for d in range(22, 26):  # 21st is holiday
            day = date(2022, 2, d)

            response = yfct.GetTimestampCurrentInterval(self.exchange, day, interval, discardTimes, week7days)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} day={day}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise

            for t in times:
                dt = dtc(day, t, self.market_tz)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        discardTimes = False ; week7days = False
        times = [time(4), self.market_open, time(15, 30), time(20)]
        answer = {"interval_open": dtc(date(2022, 2, 22), self.market_open, self.market_tz),  # Tuesday open (Monday holiday)
                  "interval_close": dtc(date(2022, 2, 25), self.market_close, self.market_tz)}  # Friday close
        for d in range(22, 26):  # 21th is holiday
            for t in times:
                if d == 22 and t < self.market_open:
                    continue
                if d == 25 and t >= self.market_close:
                    continue
                dt = dtc(date(2022, 2, d), t, self.market_tz)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

    def test_GetTimestampCurrentInterval_closed(self):
        answer = None

        # Before/after market hours
        times = [time(0, 15), time(9), time(16, 45), time(23, 59)]
        intervals = []
        intervals.append(yfcd.Interval.Mins1)
        intervals.append(yfcd.Interval.Mins2)
        intervals.append(yfcd.Interval.Mins5)
        intervals.append(yfcd.Interval.Mins15)
        intervals.append(yfcd.Interval.Mins30)
        intervals.append(yfcd.Interval.Mins60)
        intervals.append(yfcd.Interval.Mins90)
        intervals.append(yfcd.Interval.Hours1)
        for interval in intervals:
            for d in range(14, 19):
                day = date(2022, 2, d)
                for t in times:
                    dt = dtc(day, t, self.market_tz)
                    response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} dt={dt}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise
        interval = yfcd.Interval.Days1
        discardTimes = False
        for d in range(14, 19):
            day = date(2022, 2, d)
            for t in times:
                dt = dtc(day, t, self.market_tz)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        interval = yfcd.Interval.Week
        discardTimes = False ; week7days = False
        # Note: don't care what happens outside of trading from Monday evening -> Friday morning.
        for d, t in [ (22, time(4)), (26, time(20))]:
            day = date(2022, 2, d)
            dt = dtc(day, t, self.market_tz)
            response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes, week7days)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} dt={dt}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise

        # Weekend
        times = [time(0), time(10), time(13), time(23, 59)]
        intervals = []
        intervals.append(yfcd.Interval.Mins1)
        intervals.append(yfcd.Interval.Mins2)
        intervals.append(yfcd.Interval.Mins5)
        intervals.append(yfcd.Interval.Mins15)
        intervals.append(yfcd.Interval.Mins30)
        intervals.append(yfcd.Interval.Mins60)
        intervals.append(yfcd.Interval.Mins90)
        intervals.append(yfcd.Interval.Hours1)
        for interval in intervals:
            for d in [19, 20]:
                day = date(2022, 2, d)
                for t in times:
                    dt = dtc(day, t, self.market_tz)
                    response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} dt={dt}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise
        interval = yfcd.Interval.Days1
        for discardTimes in [False, True]:
            for d in [19, 20]:
                day = date(2022, 2, d)

                if discardTimes:
                    response = yfct.GetTimestampCurrentInterval(self.exchange, day, interval, discardTimes)
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} dt={dt}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise

                for t in times:
                    dt = dtc(day, t, self.market_tz)
                    response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes)
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} dt={dt}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise
        interval = yfcd.Interval.Week
        week7days = False
        for discardTimes in [False, True]:
            for d in [19, 20]:
                day = date(2022, 2, d)

                if discardTimes:
                    response = yfct.GetTimestampCurrentInterval(self.exchange, day, interval, discardTimes, week7days)
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} day={day}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise

                for t in times:
                    dt = dtc(day, t, self.market_tz)
                    response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes, week7days)
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} dt={dt}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise

        # Public holiday, at times that would be open if weekday
        times = [self.market_open, time(15, 30)]
        intervals = []
        intervals.append(yfcd.Interval.Mins1)
        intervals.append(yfcd.Interval.Mins2)
        intervals.append(yfcd.Interval.Mins5)
        intervals.append(yfcd.Interval.Mins15)
        intervals.append(yfcd.Interval.Mins30)
        intervals.append(yfcd.Interval.Mins60)
        intervals.append(yfcd.Interval.Mins90)
        intervals.append(yfcd.Interval.Hours1)
        day = date(2022, 2, 21)
        for interval in intervals:
            for t in times:
                dt = dtc(day, t, self.market_tz)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        interval = yfcd.Interval.Days1
        for discardTimes in [False, True]:
            if discardTimes:
                response = yfct.GetTimestampCurrentInterval(self.exchange, day, interval, discardTimes)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} day={day}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
            for t in times:
                dt = dtc(day, t, self.market_tz)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        interval = yfcd.Interval.Week
        week7days = False
        t = time(10)
        for discardTimes in [False, True]:
            for d in [19, 20, 21]:
                day = date(2022, 2, d)

                if discardTimes:
                    response = yfct.GetTimestampCurrentInterval(self.exchange, day, interval, discardTimes, week7days)
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} day={day}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise

                dt = dtc(day, t, self.market_tz)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

    def test_GetTimestampCurrentInterval_hourly_batch(self):
        interval = yfcd.Interval.Hours1

        dts = []
        for d in range(7, 21):
            d = date(2022, 2, d)
            dts.append(dtc(d, time(9, 29), self.market_tz))
            dts.append(dtc(d, time(9, 30), self.market_tz))
            for h in range(10, 16):
                dts.append(dtc(d, time(h, 30), self.market_tz))
            dts.append(dtc(d, time(16, 1), self.market_tz))

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
                    print("response:") ; pprint(response)
                    print("answer:")   ; pprint(answer)
                    raise

    def test_GetTimestampCurrentInterval_daily_batch(self):
        interval = yfcd.Interval.Days1

        days = []
        dts = []
        for d in range(7, 21):
            d = date(2022, 2, d)
            days.append(d)
            dts.append(dtc(d, time(9, 29), self.market_tz))
            dts.append(dtc(d, time(9, 30), self.market_tz))
            for h in range(10, 16):
                dts.append(dtc(d, time(h, 30), self.market_tz))
            dts.append(dtc(d, time(16, 1), self.market_tz))

        # test date types
        discardTimes = True
        responses = yfct.GetTimestampCurrentInterval_batch(self.exchange, days, interval, discardTimes)
        for i in range(responses.shape[0]):
            response = responses.iloc[i]
            answer = yfct.GetTimestampCurrentInterval(self.exchange, days[i], interval, discardTimes)
            if answer is None:
                try:
                    self.assertTrue(pd.isnull(response["interval_open"]))
                    self.assertTrue(pd.isnull(response["interval_close"]))
                except:
                    print(f"Test fail with day=days[i], discardTimes={discardTimes}")
                    print("- response:")
                    print(response)
                    print("- answer:")
                    print(answer)
                    raise
            else:
                try:
                    self.assertEqual(response["interval_open"], answer["interval_open"])
                    self.assertEqual(response["interval_close"], answer["interval_close"])
                except:
                    print("Test fail with day=", days[i])
                    raise

        # test datetime types
        for discardTimes in [False, True]:
            responses = yfct.GetTimestampCurrentInterval_batch(self.exchange, dts, interval, discardTimes)
            for i in range(responses.shape[0]):
                response = responses.iloc[i]
                answer = yfct.GetTimestampCurrentInterval(self.exchange, dts[i], interval, discardTimes)
                if answer is None:
                    try:
                        self.assertTrue(pd.isnull(response["interval_open"]))
                        self.assertTrue(pd.isnull(response["interval_close"]))
                    except:
                        print(f"Test fail with dt=dts[i], discardTimes={discardTimes}")
                        print("- response:")
                        print(response)
                        print("- answer:")
                        print(answer)
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

        days = []
        dts = []
        for d in range(7, 21):
            d = date(2022, 2, d)
            days.append(d)
            dts.append(dtc(d, time(9, 29), self.market_tz))
            dts.append(dtc(d, time(9, 30), self.market_tz))
            for h in range(10, 16):
                dts.append(dtc(d, time(h, 30), self.market_tz))
            dts.append(dtc(d, time(16, 1), self.market_tz))

        # test day type
        discardTimes = True
        for week7days in [False, True]:
            responses = yfct.GetTimestampCurrentInterval_batch(self.exchange, days, interval, discardTimes, week7days)
            for i in range(responses.shape[0]):
                response = responses.iloc[i]
                answer = yfct.GetTimestampCurrentInterval(self.exchange, days[i], interval, discardTimes, week7days)
                if answer is None:
                    try:
                        self.assertTrue(pd.isnull(response["interval_open"]))
                        self.assertTrue(pd.isnull(response["interval_close"]))
                    except:
                        print("Test fail with day=", days[i])
                        raise
                else:
                    try:
                        self.assertEqual(response["interval_open"], answer["interval_open"])
                        self.assertEqual(response["interval_close"], answer["interval_close"])
                    except:
                        print("Test fail with day=", days[i])
                        print("response:")
                        print("{} -> {}".format(response["interval_open"], response["interval_close"]))
                        print("answer:")
                        print("{} -> {}".format(answer["interval_open"], answer["interval_close"]))
                        raise

        # test datetime type
        for week7days in [False, True]:
            for discardTimes in [False, True]:
                if week7days and not discardTimes:
                    continue
                responses = yfct.GetTimestampCurrentInterval_batch(self.exchange, dts, interval, discardTimes, week7days)
                for i in range(responses.shape[0]):
                    response = responses.iloc[i]
                    answer = yfct.GetTimestampCurrentInterval(self.exchange, dts[i], interval, discardTimes, week7days)
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
                            print("response:")
                            print("{} -> {}".format(response["interval_open"], response["interval_close"]))
                            print("answer:")
                            print("{} -> {}".format(answer["interval_open"], answer["interval_close"]))
                            raise

    def test_GetTimestampNextInterval_intraday(self):
        intraday_intervals = []
        intraday_intervals.append(yfcd.Interval.Mins1)
        intraday_intervals.append(yfcd.Interval.Mins2)
        intraday_intervals.append(yfcd.Interval.Mins5)
        intraday_intervals.append(yfcd.Interval.Mins15)
        intraday_intervals.append(yfcd.Interval.Mins30)
        intraday_intervals.append(yfcd.Interval.Mins60)
        intraday_intervals.append(yfcd.Interval.Hours1)
        intraday_intervals.append(yfcd.Interval.Mins90)

        # If in morning before market open, next interval is same day session first interval:
        t = time(9, 15)
        for interval in intraday_intervals:
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in range(14, 19):
                d = date(2022, 2, d)
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

                answer_day = d
                answer = {"interval_open": dtc(answer_day, self.market_open, self.market_tz)}
                if interval == yfcd.Interval.Days1:
                    answer["interval_close"] = dtc(answer_day, self.market_close, self.market_tz)
                else:
                    answer["interval_close"] = answer["interval_open"] + interval_td
                    if answer["interval_close"].time() > self.market_close:
                        answer["interval_close"] = dtc(answer["interval_close"].date(), self.market_close, self.market_tz)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        # If intraday during day session, next interval is in same session:
        times = [self.market_open, time(13, 30)]
        for interval in intraday_intervals:
            if interval == yfcd.Interval.Mins90:
                # Handle separately
                continue
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in range(14, 19):
                d = date(2022, 2, d)
                for t in times:
                    # dt at start of interval:
                    dt = dtc(d, t, self.market_tz)
                    response = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
                    answer = {}
                    answer["interval_open"] = dt + interval_td
                    answer["interval_close"] = answer["interval_open"] + interval_td
                    if answer["interval_close"].time() > self.market_close:
                        answer["interval_close"] = dtc(answer["interval_close"].date(), self.market_close, self.market_tz)
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} dt={dt}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise

                    # dt in middle of interval:
                    dt = dtc(d, t, self.market_tz)
                    dt += interval_td * 0.5
                    market_close_dt = dtc(dt.date(), self.market_close, self.market_tz)
                    if dt >= market_close_dt:
                        dt = market_close_dt - timedelta(minutes=1)
                    response = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
                    answer = {}
                    answer["interval_open"] = dtc(d, t, self.market_tz) + interval_td
                    answer["interval_close"] = answer["interval_open"] + interval_td
                    if answer["interval_close"].time() > self.market_close:
                        answer["interval_close"] = dtc(answer["interval_close"].date(), self.market_close, self.market_tz)
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} dt={dt}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise
        interval = yfcd.Interval.Mins90
        times = []
        answers = []
        times.append(time(9, 30))  ; answers.append({"interval_open": time(11), "interval_close": time(12, 30)})
        times.append(time(9, 45))  ; answers.append({"interval_open": time(11), "interval_close": time(12, 30)})
        times.append(time(13, 30)) ; answers.append({"interval_open": time(14), "interval_close": time(15, 30)})
        times.append(time(13, 45)) ; answers.append({"interval_open": time(14), "interval_close": time(15, 30)})
        for i in range(len(times)):
            t = times[i]
            answer_t = answers[i]
            for d in range(14, 19):
                d = date(2022, 2, d)
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
                answer = {}
                answer["interval_open"] = dtc(d, answer_t["interval_open"], self.market_tz)
                answer["interval_close"] = dtc(d, answer_t["interval_close"], self.market_tz)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

        # If during the final interval of session, next interval is next day first interval
        for interval in intraday_intervals:
            interval_td = yfcd.intervalToTimedelta[interval]

            times = []
            times.append(time(15, 59))
            m = min(15, interval_td.seconds//60//2)
            times.append(time(15, 59-m))
            for d in range(14, 19):
                d = date(2022, 2, d)
                next_day = d + self.td_1d
                if next_day.weekday() in [5, 6]:
                    next_day += timedelta(days=7-next_day.weekday())
                while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                    next_day += self.td_1d
                for t in times:
                    dt = dtc(d, t, self.market_tz)
                    response = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

                    answer = {}
                    answer["interval_open"] = dtc(next_day, self.market_open, self.market_tz)
                    answer["interval_close"] = answer["interval_open"] + interval_td
                    if answer["interval_close"].time() > self.market_close:
                        answer["interval_close"] = dtc(answer["interval_close"].date(), self.market_close, self.market_tz)
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} dt={dt}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise

        # If in evening after market close, next interval is next session first interval:
        t = self.market_close
        for interval in intraday_intervals:
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in range(14, 19):
                d = date(2022, 2, d)
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

                next_day = d + self.td_1d
                if next_day.weekday() in [5, 6]:
                    next_day += timedelta(days=7-next_day.weekday())
                while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                    next_day += self.td_1d
                answer = {}
                answer["interval_open"] = dtc(next_day, self.market_open, self.market_tz)
                answer["interval_close"] = answer["interval_open"] + interval_td
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

    def test_GetTimestampNextInterval_daily(self):
        interval = yfcd.Interval.Days1

        # ts is datetime
        discardTimes = False
        # - morning -> today
        # - evening -> next day
        times = [time(4), self.market_open, time(15, 30), time(20)]
        for d in range(14, 19):
            d = date(2022, 2, d)

            next_day = d + self.td_1d
            if next_day.weekday() in [5, 6]:
                next_day += timedelta(days=7-next_day.weekday())
            while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                next_day += self.td_1d
            for t in times:
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes)

                if t < self.market_open:
                    answer = {"interval_open": dtc(d, self.market_open, self.market_tz),
                              "interval_close": dtc(d, self.market_close, self.market_tz)}
                else:
                    answer = {"interval_open": dtc(next_day, self.market_open, self.market_tz),
                              "interval_close": dtc(next_day, self.market_close, self.market_tz)}
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt} discardTimes={discardTimes}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        # - weekend -> Monday morning
        answer = {"interval_open": dtc(date(2022, 2, 14), self.market_open, self.market_tz),
                  "interval_close": dtc(date(2022, 2, 14), self.market_close, self.market_tz)}
        for d in [12, 13]:
            d = date(2022, 2, d)
            for t in times:
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt} discardTimes={discardTimes}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        discardTimes = True
        # next working day regardless of time
        times = [time(4), self.market_open, time(15, 30), time(20)]
        for d in range(14, 19):
            d = date(2022, 2, d)
            next_day = d + self.td_1d
            while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                next_day += self.td_1d
            for t in times:
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes)
                answer = {"interval_open": next_day, "interval_close": next_day + self.td_1d}
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt} discardTimes={discardTimes}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

        # ts is date
        # - any day -> next working day
        discardTimes = True
        for d in range(12, 19):
            d = date(2022, 2, d)

            next_day = d + self.td_1d
            if next_day.weekday() in [5, 6]:
                next_day += timedelta(days=7-next_day.weekday())
            while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                next_day += self.td_1d

            response = yfct.GetTimestampNextInterval(self.exchange, d, interval, discardTimes)
            answer = {"interval_open": next_day, "interval_close": next_day + self.td_1d}
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} d={d} discardTimes={discardTimes}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise

        # ts is Monday holiday -> Tuesday
        d = date(2022, 2, 21)
        discardTimes = True
        answer = {"interval_open": date(2022, 2, 22), "interval_close": date(2022, 2, 23)}
        response = yfct.GetTimestampNextInterval(self.exchange, d, interval, discardTimes)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} d={d} discardTimes={discardTimes}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise
        dts = [dtc(d, t, self.market_tz) for t in times]
        discardTimes = False
        answer = {"interval_open": dtc(date(2022, 2, 22), self.market_open, self.market_tz),
                  "interval_close": dtc(date(2022, 2, 22), self.market_close, self.market_tz)}
        for dt in dts:
            response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} d={d} discardTimes={discardTimes}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise

    def test_GetTimestampNextInterval_weekly_typeDate(self):
        interval = yfcd.Interval.Week

        # Weekend:
        discardTimes = False ; week7days = False
        answer = {"interval_open": dtc(date(2022, 2, 14), self.market_open, self.market_tz),  # Monday open
                  "interval_close": dtc(date(2022, 2, 18), self.market_close, self.market_tz)}  # Friday close
        for d in [12, 13]:
            d = date(2022, 2, d)
            response = yfct.GetTimestampNextInterval(self.exchange, d, interval, discardTimes, week7days)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} d={d}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise
        discardTimes = True ; week7days = False
        answer = {"interval_open": date(2022, 2, 14), "interval_close": date(2022, 2, 19)}  # Monday -> Saturday
        for d in [12, 13]:
            d = date(2022, 2, d)
            response = yfct.GetTimestampNextInterval(self.exchange, d, interval, discardTimes, week7days)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} d={d}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise
        discardTimes = True ; week7days = True
        answer = {"interval_open": date(2022, 2, 14), "interval_close": date(2022, 2, 21)}  # Monday -> next Monday
        for d in [12, 13]:
            d = date(2022, 2, d)
            response = yfct.GetTimestampNextInterval(self.exchange, d, interval, discardTimes, week7days)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} d={d}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise

        # Week day
        discardTimes = False ; week7days = False
        answer = {"interval_open": dtc(date(2022, 2, 14), self.market_open, self.market_tz),  # Monday
                  "interval_close": dtc(date(2022, 2, 18), self.market_close, self.market_tz)}  # Friday
        for d in range(7, 12):
            d = date(2022, 2, d)
            response = yfct.GetTimestampNextInterval(self.exchange, d, interval, discardTimes, week7days)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} d={d}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise
        discardTimes = True ; week7days = True
        answer = {"interval_open": date(2022, 2, 14), "interval_close": date(2022, 2, 21)}  # Monday -> next Monday
        for d in range(7, 12):
            d = date(2022, 2, d)
            response = yfct.GetTimestampNextInterval(self.exchange, d, interval, discardTimes, week7days)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} d={d}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise
        discardTimes = True ; week7days = False
        answer = {"interval_open": date(2022, 2, 14), "interval_close": date(2022, 2, 19)}  # Monday -> Saturday
        for d in range(7, 12):
            d = date(2022, 2, d)
            response = yfct.GetTimestampNextInterval(self.exchange, d, interval, discardTimes, week7days)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} d={d}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise

        # Monday holiday
        d = date(2022, 2, 21)
        discardTimes = False ; week7days = False
        answer = {"interval_open": dtc(date(2022, 2, 22), self.market_open, self.market_tz),  # Tuesday open
                  "interval_close": dtc(date(2022, 2, 25), self.market_close, self.market_tz)}  # Friday close
        response = yfct.GetTimestampNextInterval(self.exchange, d, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} d={d}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise
        discardTimes = True ; week7days = True
        answer = {"interval_open": date(2022, 2, 28), "interval_close": date(2022, 3, 7)}  # next Monday -> next next Monday
        response = yfct.GetTimestampNextInterval(self.exchange, d, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} d={d}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise
        discardTimes = True ; week7days = False
        answer = {"interval_open": date(2022, 2, 22), "interval_close": date(2022, 2, 26)}  # Tuesday -> Saturday
        response = yfct.GetTimestampNextInterval(self.exchange, d, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} d={d}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise

    def test_GetTimestampNextInterval_weekly_typeDatetime(self):
        interval = yfcd.Interval.Week

        times = [time(1), time(12), time(15), time(20)]

        # Weekend
        discardTimes = False ; week7days = False
        answer = {"interval_open": dtc(date(2022, 2, 14), self.market_open, self.market_tz),  # Monday open
                  "interval_close": dtc(date(2022, 2, 18), self.market_close, self.market_tz)}  # Friday close
        for d in [12, 13]:
            d = date(2022, 2, d)
            for t in times:
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        discardTimes = True ; week7days = False
        answer = {"interval_open": date(2022, 2, 14), "interval_close": date(2022, 2, 19)}  # Monday -> Saturday
        for d in [12, 13]:
            d = date(2022, 2, d)
            for t in times:
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        discardTimes = True ; week7days = True
        answer = {"interval_open": date(2022, 2, 14), "interval_close": date(2022, 2, 21)}  # Monday -> next Monday
        for d in [12, 13]:
            d = date(2022, 2, d)
            for t in times:
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

        # Monday morning
        dt = dtc(date(2022, 2, 7), time(6), self.market_tz)
        discardTimes = False ; week7days = False
        answer = {"interval_open": dtc(date(2022, 2, 7), self.market_open, self.market_tz), # Monday open
                  "interval_close": dtc(date(2022, 2, 11), self.market_close, self.market_tz)}  # Friday close
        response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} dt={dt}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise
        discardTimes = True ; week7days = False
        answer = {"interval_open": date(2022, 2, 14), "interval_close": date(2022, 2, 19)}  # Next Monday -> Saturday
        response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} dt={dt}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise
        discardTimes = True ; week7days = True
        answer = {"interval_open": date(2022, 2, 14), "interval_close": date(2022, 2, 21)}  # Next Monday -> next next Monday
        response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} dt={dt}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise

        # Monday holiday
        dt = dtc(date(2022, 2, 21), time(12), self.market_tz)
        discardTimes = False ; week7days = False
        answer = {"interval_open": dtc(date(2022, 2, 22), self.market_open, self.market_tz),  # Tuesday open
                  "interval_close": dtc(date(2022, 2, 25), self.market_close, self.market_tz)}  # Friday close
        response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} dt={dt}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise
        discardTimes = True ; week7days = False
        answer = {"interval_open": date(2022, 2, 22), "interval_close": date(2022, 2, 26)}  # Tuesday -> Saturday
        response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} dt={dt}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise
        discardTimes = True ; week7days = True
        answer = {"interval_open": date(2022, 2, 28), "interval_close": date(2022, 3, 7)}  # next Monday -> next next Monday
        response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} dt={dt}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise

        # During working week
        discardTimes = False ; week7days = False
        answer = {"interval_open": dtc(date(2022, 2, 14), self.market_open, self.market_tz),  # Monday open
                  "interval_close": dtc(date(2022, 2, 18), self.market_close, self.market_tz)}  # Friday close
        for d in range(7, 13):
            d = date(2022, 2, d)
            for t in times:
                if d == date(2022, 2, 7) and t < self.market_open:
                    continue
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        discardTimes = True ; week7days = False
        answer = {"interval_open": date(2022, 2, 14), "interval_close": date(2022, 2, 19)}  # Monday -> Saturday
        for d in range(7, 13):
            d = date(2022, 2, d)
            for t in times:
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        discardTimes = True ; week7days = True
        answer = {"interval_open": date(2022, 2, 14), "interval_close": date(2022, 2, 21)}  # Monday -> next Monday
        for d in range(7, 13):
            d = date(2022, 2, d)
            for t in times:
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

    def test_GetTimestampNextInterval_hourly_batch(self):
        interval = yfcd.Interval.Hours1

        dts = []
        for d in range(7, 21):
            d = date(2022, 2, d)
            dts.append(dtc(d, time(9, 29), self.market_tz))
            dts.append(dtc(d, time(9, 30), self.market_tz))
            for h in range(10, 16):
                dts.append(dtc(d, time(h, 30), self.market_tz))
            dts.append(dtc(d, time(16, 1), self.market_tz))

        responses = yfct.GetTimestampNextInterval_batch(self.exchange, dts, interval)
        for i in range(responses.shape[0]):
            response = responses.iloc[i]
            answer = yfct.GetTimestampNextInterval(self.exchange, dts[i], interval)
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
                    print("response:") ; pprint(response)
                    print("answer:")   ; pprint(answer)
                    raise

    def test_GetTimestampNextInterval_daily_batch(self):
        interval = yfcd.Interval.Days1

        days = []
        dts = []
        for d in range(7, 21):
            d = date(2022, 2, d)
            days.append(d)
            dts.append(dtc(d, time(9, 29), self.market_tz))
            dts.append(dtc(d, time(9, 30), self.market_tz))
            for h in range(10, 16):
                dts.append(dtc(d, time(h, 30), self.market_tz))
            dts.append(dtc(d, time(16, 1), self.market_tz))

        for discardTimes in [False, True]:
            responses = yfct.GetTimestampNextInterval_batch(self.exchange, days, interval, discardTimes)
            for i in range(responses.shape[0]):
                response = responses.iloc[i]
                answer = yfct.GetTimestampNextInterval(self.exchange, days[i], interval, discardTimes)
                if answer is None:
                    try:
                        self.assertTrue(pd.isnull(response["interval_open"]))
                        self.assertTrue(pd.isnull(response["interval_close"]))
                    except:
                        print("Test fail with day=", days[i])
                        raise
                else:
                    try:
                        self.assertEqual(response["interval_open"], answer["interval_open"])
                        self.assertEqual(response["interval_close"], answer["interval_close"])
                    except:
                        print(f"Test fail with day={days[i]} discardTimes={discardTimes}")
                        print("response:") ; pprint(response)
                        print("answer:")   ; pprint(answer)
                        raise

            responses = yfct.GetTimestampNextInterval_batch(self.exchange, dts, interval, discardTimes)
            for i in range(responses.shape[0]):
                response = responses.iloc[i]
                answer = yfct.GetTimestampNextInterval(self.exchange, dts[i], interval, discardTimes)
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
                        print(f"Test fail with dt={dts[i]} discardTimes={discardTimes}")
                        print("response:") ; pprint(response)
                        print("answer:")   ; pprint(answer)
                        raise

    def test_GetTimestampNextInterval_weekly_batch(self):
        interval = yfcd.Interval.Week

        days = []
        dts = []
        for d in range(7, 21):
            d = date(2022, 2, d)
            days.append(d)
            dts.append(dtc(d, time(9, 29), self.market_tz))
            dts.append(dtc(d, time(9, 30), self.market_tz))
            for h in range(10, 16):
                dts.append(dtc(d, time(h, 30), self.market_tz))
            dts.append(dtc(d, time(16, 1), self.market_tz))

        for discardTimes in [False, True]:
            for week7days in [False, True]:
                if week7days and not discardTimes:
                    # Skip
                    continue

                responses = yfct.GetTimestampNextInterval_batch(self.exchange, days, interval, discardTimes, week7days)
                for i in range(responses.shape[0]):
                    response = responses.iloc[i]
                    answer = yfct.GetTimestampNextInterval(self.exchange, days[i], interval, discardTimes, week7days)
                    if answer is None:
                        try:
                            self.assertTrue(pd.isnull(response["interval_open"]))
                            self.assertTrue(pd.isnull(response["interval_close"]))
                        except:
                            print("Test fail with day=", days[i])
                            raise
                    else:
                        try:
                            self.assertEqual(response["interval_open"], answer["interval_open"])
                            self.assertEqual(response["interval_close"], answer["interval_close"])
                        except:
                            print(f"Test fail with day={days[i]} discardTimes={discardTimes}")
                            print("response:") ; print(f"{response['interval_open']} -> {response['interval_close']}")
                            print("answer:") ; print(f"{answer['interval_open']} -> {answer['interval_close']}")
                            raise

                responses = yfct.GetTimestampNextInterval_batch(self.exchange, dts, interval, discardTimes, week7days)
                for i in range(responses.shape[0]):
                    response = responses.iloc[i]
                    answer = yfct.GetTimestampNextInterval(self.exchange, dts[i], interval, discardTimes, week7days)
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
                            print(f"Test fail with dt={dts[i]} discardTimes={discardTimes}")
                            print("response:") ; print(f"{response['interval_open']} -> {response['interval_close']}")
                            print("answer:") ; print(f"{answer['interval_open']} -> {answer['interval_close']}")
                            raise


if __name__ == '__main__':
    unittest.main()
