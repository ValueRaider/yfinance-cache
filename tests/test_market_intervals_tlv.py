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
# X* = public holiday
#  -- March --
#  Mo   Tu   We   Th   Fr   Sa   Su
#  -    1    2    3    4    5    6
#  7    8    9    10   11   12   13
#  14   15   16   17*  18*  19   20
#  21   22   23   24   25   26   27
#  28   29   30   31
#  -- September --
#  Mo   Tu   We   Th   Fr   Sa   Su
#  -    -    -    1    2    3    4
#  5    6    7    8    9    10   11
#  12   13   14   15   16   17   18
#  19   20   21   22   23   24   25*
#  26*  27*  28   29   30
#
#  -- March --
#  Su   Mo   Tu   We   Th   Fr   Sa
#  -    -    1    2    3    4    5
#  6    7    8    9    10   11   12
#  13   14   15   16   17*  18*  19
#  20   21   22   23   24   25   26
#  27   28   29   30   31
#  -- September --
#  Su   Mo   Tu   We   Th   Fr   Sa
#  -    -    -    -    1    2    3
#  4    5    6    7    8    9    10
#  11   12   13   14   15   16   17
#  18   19   20   21   22   23   24
#  25*  26*  27*  28   29   30


class Test_Market_Intervals_TLV(unittest.TestCase):

    def setUp(self):
        self.exchange = "TLV"
        self.tz = 'Asia/Jerusalem'
        self.market_tz = ZoneInfo(self.tz)
        yfct.SetExchangeTzName(self.exchange, self.tz)

        self.market_open = time(9, 59)
        self.regular_close = time(17, 15)
        self.auction_start = time(17, 24)
        self.auction_end = time(17, 26)
        self.market_close = self.auction_end

        self.regular_close_sunday = time(15, 40)
        self.auction_start_sunday = time(15, 49)
        self.auction_end_sunday = time(15, 51)
        self.market_close_sunday = self.auction_end_sunday

        self.td_1d = timedelta(days=1)

    def test_GetScheduleIntervals_align(self):
        # Test all interval sizes, that start time is aligned to match Yahoo

        d = 6
        start_d = date(2022, 3, d)
        end_d = date(2022, 3, d+1)

        aligned_start_times = {}
        aligned_start_times[yfcd.Interval.Mins1] = time(9, 59)
        aligned_start_times[yfcd.Interval.Mins2] = time(9, 58)
        aligned_start_times[yfcd.Interval.Mins5] = time(9, 55)
        aligned_start_times[yfcd.Interval.Mins15] = time(9, 45)
        aligned_start_times[yfcd.Interval.Mins30] = time(9, 30)
        # Note: Yahoo aligns these larger intervals to 30m:
        aligned_start_times[yfcd.Interval.Mins60] = time(9, 30)
        aligned_start_times[yfcd.Interval.Mins90] = time(9, 30)
        aligned_start_times[yfcd.Interval.Hours1] = time(9, 30)

        for interval in aligned_start_times.keys():
            t = aligned_start_times[interval]
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d)
            try:
                self.assertEqual(response[0].left.time(), t)
            except:
                print("- interval =", interval)
                raise

    def test_GetScheduleIntervals_hourly(self):
        interval = yfcd.Interval.Hours1

        for d in range(6, 11):
            start_d = date(2022, 3, d)
            end_d = date(2022, 3, d+1)

            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d)
            answer_times = [(time(9, 30), time(10, 30)),
                            (time(10, 30), time(11, 30)),
                            (time(11, 30), time(12, 30)),
                            (time(12, 30), time(13, 30)),
                            (time(13, 30), time(14, 30)),
                            (time(14, 30), time(15, 30))]
            if d == 6:
                # Sunday
                answer_times += [(time(15, 30), time(15, 51))]
            else:
                answer_times += [(time(15, 30), time(16, 30)),
                                 (time(16, 30), time(17, 26))]
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
        start_d = date(2022, 3, 6)  # Sunday
        end_d = date(2022, 3, 18)  # next Friday
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, discardTimes)
        answer_days = [date(2022, 3, 6),  date(2022, 3, 7),  date(2022, 3, 8),  date(2022, 3, 9), date(2022, 3, 10),
                       date(2022, 3, 13), date(2022, 3, 14), date(2022, 3, 15), date(2022, 3, 16)]
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
        right = [dtc(d, self.auction_end, self.market_tz) if d.weekday() < 6 else dtc(d, self.auction_end_sunday, self.market_tz) for d in answer_days]
        answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise

    def test_GetScheduleIntervals_weekly(self):
        base_args = {'interval': yfcd.Interval.Week,
                     'exchange': self.exchange,
                     'weekForceStartMonday': False}

        # Simple case: 2 full weeks
        args = dict(base_args)
        args["start"] = date(2022, 9, 4)  # Sunday
        args["end"] = date(2022, 9, 18)  # Sunday + 2 weeks
        args['week7days'] = False ; args['discardTimes'] = False
        left = [dtc(date(2022, 9, 4), self.market_open, self.market_tz),  # Sunday open
                dtc(date(2022, 9, 11), self.market_open, self.market_tz)]  # Sunday open
        right = [dtc(date(2022, 9, 8), self.auction_end, self.market_tz),  # Thursday close
                 dtc(date(2022, 9, 15), self.auction_end, self.market_tz)]  # Thursday close
        answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(**args)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        args['week7days'] = False ; args['discardTimes'] = True
        left = [date(2022, 9, 4), date(2022, 9, 11)]  # Sundays
        right = [date(2022, 9, 9), date(2022, 9, 16)]  # Fridays
        answer = yfcd.DateIntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(**args)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        week7days = True ; discardTimes = True
        args['week7days'] = True ; args['discardTimes'] = True
        answer = yfcd.DateIntervalIndex([yfcd.DateInterval(date(2022, 9, 5), date(2022, 9, 12), closed="left")])  # Monday -> next Monday
        response = yfct.GetExchangeScheduleIntervals(**args)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise

        # Weekend cut off
        args = dict(base_args)
        args["start"] = date(2022, 9, 4)  # Sunday
        args["end"] = date(2022, 9, 17)  # next Saturday
        args['week7days'] = False ; args['discardTimes'] = False
        left = [dtc(date(2022, 9, 4), self.market_open, self.market_tz),  # Sunday opens
                dtc(date(2022, 9, 11), self.market_open, self.market_tz)]
        right = [dtc(date(2022, 9, 8), self.market_close, self.market_tz),  # Thursday closes
                 dtc(date(2022, 9, 15), self.market_close, self.market_tz)]
        answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(**args)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        args['week7days'] = False ; args['discardTimes'] = True
        left = [date(2022, 9, 4), date(2022, 9, 11)]  # Sundays
        right = [date(2022, 9, 9), date(2022, 9, 16)]  # Fridays
        answer = yfcd.DateIntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(**args)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        args['week7days'] = True ; args['discardTimes'] = True
        left = [date(2022, 9, 5)]  # Monday
        right = [date(2022, 9, 12)]  # next Monday
        answer = yfcd.DateIntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(**args)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise

        # Monday holiday
        args = dict(base_args)
        args["start"] = date(2022, 9, 28)  # Wednesday (Mon-Tues holidays)
        args["end"] = date(2022, 10, 3)  # next Monday
        args['week7days'] = False ; args['discardTimes'] = False
        left = [dtc(date(2022, 9, 28), self.market_open, self.market_tz)]  # Wednesday open
        right = [dtc(date(2022, 9, 29), self.market_close, self.market_tz)]  # Thursday close
        answer = pd.IntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(**args)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        args['week7days'] = False ; args['discardTimes'] = True
        left = [date(2022, 9, 28)]  # Wednesday (Sun-Tue holidays)
        right = [date(2022, 9, 30)]  # Friday
        answer = yfcd.DateIntervalIndex.from_arrays(left, right, closed="left")
        response = yfct.GetExchangeScheduleIntervals(**args)
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("response:") ; pprint(response)
            print("answer:")   ; pprint(answer)
            raise
        args['week7days'] = True ; args['discardTimes'] = True
        answer = None
        response = yfct.GetExchangeScheduleIntervals(**args)
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
        # dt between open (9:59am) and 10am
        t = self.market_open
        for interval in intervals:
            interval_td = yfcd.intervalToTimedelta[interval]
            # This interval will shift back no earlier than 9:30am, matching Yahoo:
            start_shiftback = min(interval_td, timedelta(minutes=30))
            for d in range(6, 11):
                day = date(2022, 3, d)
                market_close = self.market_close_sunday if d == 6 else self.market_close

                dt = dtc(day, t, self.market_tz)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)

                answer = {"interval_open": dtc(day, time(10), self.market_tz)-start_shiftback}
                answer["interval_close"] = answer["interval_open"]+interval_td
                if answer["interval_close"].time() > self.market_close:
                    answer["interval_close"] = dtc(day, self.market_close, self.market_tz)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        # during day
        t = time(12, 30)
        for interval in intervals:
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in range(6, 11):
                day = date(2022, 3, d)
                market_close = self.market_close_sunday if d == 6 else self.market_close

                # dt at start of interval:
                dt = dtc(day, t, self.market_tz)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)

                answer = {"interval_open": dtc(day, time(12, 30), self.market_tz),
                          "interval_close": dtc(day, time(12, 30), self.market_tz)+interval_td}
                if answer["interval_close"].time() > market_close:
                    answer["interval_close"] = dtc(day, market_close, self.market_tz)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

                # dt in middle of interval:
                dt = dtc(day, t, self.market_tz) + 0.5*interval_td
                market_close_dt = dtc(day, market_close, self.market_tz)
                if dt >= market_close_dt:
                    dt = market_close_dt - timedelta(minutes=1)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                answer = {"interval_open": dtc(day, t, self.market_tz),
                          "interval_close": dtc(day, t, self.market_tz) + interval_td}
                if answer["interval_close"].time() > market_close:
                    answer["interval_close"] = dtc(day, market_close, self.market_tz)
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
        for d in range(6, 11):
            day = date(2022, 3, d)

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
        for d in range(6, 11):
            day = date(2022, 3, d)
            if day == date(2022, 3, 6):
                answer = {"interval_open": dtc(day, self.market_open, self.market_tz),
                          "interval_close": dtc(day, self.market_close_sunday, self.market_tz)}
            else:
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
        answer = {"interval_open": date(2022, 3, 7), "interval_close": date(2022, 3, 14)}  # Monday -> next Monday
        for d in range(7, 14):
            day = date(2022, 3, d)

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
        answer = {"interval_open": date(2022, 3, 6), "interval_close": date(2022, 3, 11)}  # Sunday -> Friday
        for d in range(6, 11):
            day = date(2022, 3, d)

            response = yfct.GetTimestampCurrentInterval(self.exchange, day, interval, discardTimes, week7days)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} day={day}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise

            for t in times:
                if day == date(2022, 3, 6) and t < self.market_open:
                    continue
                elif day == date(2023, 3, 10) and t >= self.market_close:
                    continue
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
        answer = {"interval_open": dtc(date(2022, 3, 6), self.market_open, self.market_tz),  # Sunday open
                  "interval_close": dtc(date(2022, 3, 10), self.market_close, self.market_tz)}  # Thursday close
        for d in range(6, 11):
            for t in times:
                if d == 6 and t < self.market_open:
                    continue
                elif d == 10 and t >= self.market_close:
                    continue
                dt = dtc(date(2022, 3, d), t, self.market_tz)
                response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

    def test_GetTimestampCurrentInterval_open_auction(self):
        # Sunday:
        day = date(2022, 3, 6)
        auction_end = self.auction_end_sunday
        t = time(15, 49)
        intervals = [] ; answers = []
        intervals.append(yfcd.Interval.Mins1)  ; answers.append(time(15, 49))
        intervals.append(yfcd.Interval.Mins2)  ; answers.append(time(15, 48))
        intervals.append(yfcd.Interval.Mins5)  ; answers.append(time(15, 45))
        intervals.append(yfcd.Interval.Mins15) ; answers.append(time(15, 45))
        intervals.append(yfcd.Interval.Mins30) ; answers.append(time(15, 30))
        intervals.append(yfcd.Interval.Mins60) ; answers.append(time(15, 30))
        intervals.append(yfcd.Interval.Mins90) ; answers.append(time(15, 30))
        for i in range(len(intervals)):
            interval = intervals[i]
            dt = dtc(day, t, self.market_tz)
            intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
            answer = {"interval_open": dtc(day, answers[i], self.market_tz),
                      "interval_close": dtc(day, auction_end, self.market_tz)}
            try:
                self.assertEqual(intervalRange, answer)
            except:
                print(f"interval={interval} dt={dt}")
                print("intervalRange:") ; pprint(intervalRange)
                print("answer:")        ; pprint(answer)
                raise

        # Rest-of-week:
        auction_end = self.auction_end
        t = time(17, 24)
        intervals = [] ; answers = []
        intervals.append(yfcd.Interval.Mins1)  ; answers.append(time(17, 24))
        intervals.append(yfcd.Interval.Mins2)  ; answers.append(time(17, 24))
        intervals.append(yfcd.Interval.Mins5)  ; answers.append(time(17, 20))
        intervals.append(yfcd.Interval.Mins15) ; answers.append(time(17, 15))
        intervals.append(yfcd.Interval.Mins30) ; answers.append(time(17))
        intervals.append(yfcd.Interval.Mins60) ; answers.append(time(16, 30))
        intervals.append(yfcd.Interval.Mins90) ; answers.append(time(17))
        for i in range(len(intervals)):
            interval = intervals[i]
            for d in range(7, 11):
                day = date(2022, 3, d)
                dt = dtc(day, t, self.market_tz)
                intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval)
                answer = {"interval_open": dtc(day, answers[i], self.market_tz),
                          "interval_close": dtc(day, auction_end, self.market_tz)}
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

        # Before/after market hours
        # times = [time(9), time(17, 30)]
        times = [time(0, 15), time(9), time(17, 30), time(23, 59)]
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
            for d in range(6, 11):
                day = date(2022, 3, d)
                for t in times:
                    if d == 6 and t == time(17, 30):
                        t = time(15, 55)

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
        for d in range(6, 11):
            day = date(2022, 3, d)
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
        for d, t in [ (6, time(4)), (10, time(20))]:
            day = date(2022, 3, d)
            dt = dtc(day, t, self.market_tz)
            response = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, discardTimes, week7days)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} dt={dt}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise

        # Sabbath
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
            for d in [11, 12]:
                day = date(2022, 3, d)
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
            for d in [11, 12]:
                day = date(2022, 3, d)

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
            for d in [11, 12]:
                day = date(2022, 3, d)

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
        times = [self.market_open, time(12, 30)]
        intervals = []
        intervals.append(yfcd.Interval.Mins1)
        intervals.append(yfcd.Interval.Mins2)
        intervals.append(yfcd.Interval.Mins5)
        intervals.append(yfcd.Interval.Mins15)
        intervals.append(yfcd.Interval.Mins30)
        intervals.append(yfcd.Interval.Mins60)
        intervals.append(yfcd.Interval.Mins90)
        intervals.append(yfcd.Interval.Hours1)
        day = date(2022, 3, 17)
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
            for d in [11, 12]:
                day = date(2022, 3, d)

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
        for d in range(6, 21):
            d = date(2022, 3, d)
            dts.append(dtc(d, time(9, 58), self.market_tz))
            dts.append(dtc(d, time(9, 59), self.market_tz))
            dts.append(dtc(d, time(10), self.market_tz))
            for h in range(10, 18):
                dts.append(dtc(d, time(h, 30), self.market_tz))

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
        for d in range(6, 21):
            d = date(2022, 3, d)
            days.append(d)
            dts.append(dtc(d, time(9, 58), self.market_tz))
            dts.append(dtc(d, time(9, 59), self.market_tz))
            dts.append(dtc(d, time(10), self.market_tz))
            for h in range(10, 18):
                dts.append(dtc(d, time(h, 30), self.market_tz))

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
        for d in range(6, 21):
            d = date(2022, 3, d)
            days.append(d)
            dts.append(dtc(d, time(9, 58), self.market_tz))
            dts.append(dtc(d, time(9, 59), self.market_tz))
            dts.append(dtc(d, time(10, 00), self.market_tz))
            for h in range(10, 18):
                dts.append(dtc(d, time(h, 30), self.market_tz))

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
                        print(f"Test fail with day={days[i]}, discardTimes={discardTimes} week7days={week7days}")
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
        # If in morning before market open, next interval is same day session first interval:
        t = time(9)
        intervals = [] ; answers = []
        intervals.append(yfcd.Interval.Mins1)  ; answers.append(time(9, 59))
        intervals.append(yfcd.Interval.Mins2)  ; answers.append(time(9, 58))
        intervals.append(yfcd.Interval.Mins5)  ; answers.append(time(9, 55))
        intervals.append(yfcd.Interval.Mins15) ; answers.append(time(9, 45))
        intervals.append(yfcd.Interval.Mins30) ; answers.append(time(9, 30))
        intervals.append(yfcd.Interval.Mins60) ; answers.append(time(9, 30))
        intervals.append(yfcd.Interval.Mins90) ; answers.append(time(9, 30))
        intervals.append(yfcd.Interval.Hours1) ; answers.append(time(9, 30))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in range(7, 11):
                dt = dtc(date(2022, 3, d), t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

                answer_day = date(2022, 3, d)
                answer = {}
                answer["interval_open"] = dtc(answer_day, answers[i], self.market_tz)
                answer["interval_close"] = answer["interval_open"] + interval_td
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

        # If during day session, next interval is in same session:
        t = self.market_open
        intervals = [] ; answers = []
        intervals.append(yfcd.Interval.Mins1)  ; answers.append(time(10))
        intervals.append(yfcd.Interval.Mins2)  ; answers.append(time(10))
        intervals.append(yfcd.Interval.Mins5)  ; answers.append(time(10))
        intervals.append(yfcd.Interval.Mins15) ; answers.append(time(10))
        intervals.append(yfcd.Interval.Mins30) ; answers.append(time(10))
        intervals.append(yfcd.Interval.Mins60) ; answers.append(time(10, 30))
        intervals.append(yfcd.Interval.Hours1) ; answers.append(time(10, 30))
        intervals.append(yfcd.Interval.Mins90) ; answers.append(time(11))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in range(6, 11):
                dt = dtc(date(2022, 3, d), t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
                answer = {}
                answer["interval_open"] = dtc(date(2022, 3, d), answers[i], self.market_tz)
                answer["interval_close"] = answer["interval_open"]+interval_td

                if d == 6:
                    market_close = self.market_close_sunday
                else:
                    market_close = self.market_close
                if answer["interval_close"].time() > market_close:
                    answer["interval_close"] = dtc(answer["interval_close"].date(), self.market_close, self.market_tz)

                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

        t = time(13, 30)
        intervals = [] ; answers = []
        intervals.append(yfcd.Interval.Mins1)  ; answers.append(time(13, 31))
        intervals.append(yfcd.Interval.Mins2)  ; answers.append(time(13, 32))
        intervals.append(yfcd.Interval.Mins5)  ; answers.append(time(13, 35))
        intervals.append(yfcd.Interval.Mins15) ; answers.append(time(13, 45))
        intervals.append(yfcd.Interval.Mins30) ; answers.append(time(14))
        intervals.append(yfcd.Interval.Mins60) ; answers.append(time(14, 30))
        intervals.append(yfcd.Interval.Hours1) ; answers.append(time(14, 30))
        intervals.append(yfcd.Interval.Mins90) ; answers.append(time(14))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in range(6, 11):
                dt = dtc(date(2022, 3, d), t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
                answer = {}
                answer["interval_open"] = dtc(date(2022, 3, d), answers[i], self.market_tz)
                answer["interval_close"] = answer["interval_open"] + interval_td

                if d == 6:
                    market_close = self.market_close_sunday
                else:
                    market_close = self.market_close
                if answer["interval_close"].time() > market_close:
                    answer["interval_close"] = dtc(answer["interval_close"].date(), self.market_close_time, self.market_tz)

                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

        # If during the final interval of session, next interval is next day first interval
        intervals = [] ; starts = []
        intervals.append(yfcd.Interval.Mins1)  ; starts.append(time(9, 59))
        intervals.append(yfcd.Interval.Mins2)  ; starts.append(time(9, 58))
        intervals.append(yfcd.Interval.Mins5)  ; starts.append(time(9, 55))
        intervals.append(yfcd.Interval.Mins15) ; starts.append(time(9, 45))
        intervals.append(yfcd.Interval.Mins30) ; starts.append(time(9, 30))
        intervals.append(yfcd.Interval.Mins60) ; starts.append(time(9, 30))
        intervals.append(yfcd.Interval.Hours1) ; starts.append(time(9, 30))
        intervals.append(yfcd.Interval.Mins90) ; starts.append(time(9, 30))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]

            for d in range(6, 11):
                times = []
                m = min(15, interval_td.seconds//60//2)
                if d == 6:
                    times.append(time(15, 49))
                    times.append(time(15, 49-m))
                else:
                    times.append(time(17, 24))
                    times.append(time(17, 24-m))

                for t in times:
                    dt = dtc(date(2022, 3, d), t, self.market_tz)
                    response = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

                    next_day = date(2022, 3, d+1)
                    while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                        next_day += self.td_1d

                    answer = {}
                    answer["interval_open"] = dtc(next_day, starts[i], self.market_tz)
                    answer["interval_close"] = answer["interval_open"]+interval_td
                    try:
                        self.assertEqual(response, answer)
                    except:
                        print(f"interval={interval} dt={dt}")
                        print("response:") ; pprint(response)
                        print("answer:") ; pprint(answer)
                        raise

        # If in evening after auction, next interval is next session first interval:
        intervals = [] ; answers = []
        intervals.append(yfcd.Interval.Mins1)  ; answers.append(time(9, 59))
        intervals.append(yfcd.Interval.Mins2)  ; answers.append(time(9, 58))
        intervals.append(yfcd.Interval.Mins5)  ; answers.append(time(9, 55))
        intervals.append(yfcd.Interval.Mins15) ; answers.append(time(9, 45))
        intervals.append(yfcd.Interval.Mins30) ; answers.append(time(9, 30))
        intervals.append(yfcd.Interval.Mins60) ; answers.append(time(9, 30))
        intervals.append(yfcd.Interval.Mins90) ; answers.append(time(9, 30))
        intervals.append(yfcd.Interval.Hours1) ; answers.append(time(9, 30))
        t = time(18)
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = yfcd.intervalToTimedelta[interval]
            for d in range(6, 11):
                dt = dtc(date(2022, 3, d), t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

                next_day = date(2022, 3, d+1)
                while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                    next_day += self.td_1d
                answer = {}
                answer["interval_open"] = dtc(next_day, answers[i], self.market_tz)
                answer["interval_close"] = answer["interval_open"]+interval_td
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
        # - morning -> today
        # - evening -> next day
        discardTimes = False
        times = [time(4), self.market_open, time(15, 30), time(20)]
        for d in range(6, 11):
            day = date(2022, 3, d)

            next_day = date(2022, 3, d + 1)
            while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                next_day += self.td_1d

            for t in times:
                dt = dtc(day, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes)

                if t < self.market_open:
                    answer = {"interval_open": dtc(day, self.market_open, self.market_tz)}
                    if day.weekday() == 6:
                        answer["interval_close"] = dtc(day, self.auction_end_sunday, self.market_tz)
                    else:
                        answer["interval_close"] = dtc(day, self.auction_end, self.market_tz)
                else:
                    answer = {"interval_open": dtc(next_day, self.market_open, self.market_tz)}
                    if next_day.weekday() == 6:
                        answer["interval_close"] = dtc(next_day, self.auction_end_sunday, self.market_tz)
                    else:
                        answer["interval_close"] = dtc(next_day, self.auction_end, self.market_tz)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt} discardTimes={discardTimes}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        # - Sabbath -> Sunday morning
        answer = {"interval_open": dtc(date(2022, 3, 13), self.market_open, self.market_tz),
                  "interval_close": dtc(date(2022, 3, 13), self.auction_end_sunday, self.market_tz)}
        for d in [11, 12]:
            d = date(2022, 3, d)
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

        # If a day interval and discardTimes=True, next day regardless of time
        discardTimes = True
        interval = yfcd.Interval.Days1
        times = [time(4), self.market_open, time(15, 30), time(20)]
        for d in range(6, 11):
            for t in times:
                dt = dtc(date(2022, 3, d), t, self.market_tz)
                intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes)
                next_day = date(2022, 3, d + 1)
                while not yfct.ExchangeOpenOnDay(self.exchange, next_day):
                    next_day += self.td_1d

                answer = {"interval_open": next_day, "interval_close": next_day + self.td_1d}
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print(f"interval={interval} dt={dt} discardTimes={discardTimes}")
                    print("intervalRange:") ; pprint(intervalRange)
                    print("answer:")        ; pprint(answer)
                    raise

        # ts is date
        # - any day -> next working day
        discardTimes = True
        for d in range(6, 11):
            d = date(2022, 3, d)

            next_day = d + self.td_1d
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

        # ts is Sunday holiday -> Monday (or next open day)
        d = date(2022, 9, 27) # Tuesday (Sun-Mon also holidays)
        discardTimes = True
        answer = {"interval_open": date(2022, 9, 28), "interval_close": date(2022, 9, 29)}
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
        answer = {"interval_open": dtc(date(2022, 9, 28), self.market_open, self.market_tz),
                  "interval_close": dtc(date(2022, 9, 28), self.market_close, self.market_tz)}
        for dt in dts:
            response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes)
            try:
                self.assertEqual(response, answer)
            except:
                print(f"interval={interval} d={d} discardTimes={discardTimes}")
                print("response:") ; pprint(response)
                print("answer:") ; pprint(answer)
                raise

    def test_GetTimestampNextInterval_weekly_typeDatetime(self):
        interval = yfcd.Interval.Week

        times = [time(1), time(12), time(15), time(20)]

        # Sabbath
        discardTimes = False ; week7days = False
        answer = {"interval_open": dtc(date(2022, 3, 6), self.market_open, self.market_tz),  # Sunday open
                  "interval_close": dtc(date(2022, 3, 10), self.market_close, self.market_tz)}  # Thursday close
        for d in [4, 5]:
            d = date(2022, 3, d)
            for t in times:
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt} discardTimes={discardTimes} week7days={week7days}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        discardTimes = True ; week7days = False
        answer = {"interval_open": date(2022, 3, 6), "interval_close": date(2022, 3, 11)}  # Sunday -> Friday
        for d in [4, 5]:
            d = date(2022, 3, d)
            for t in times:
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt} discardTimes={discardTimes} week7days={week7days}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise
        discardTimes = True ; week7days = True
        answer = {"interval_open": date(2022, 3, 7), "interval_close": date(2022, 3, 14)}  # Monday -> next Monday
        for d in [4, 5]:
            d = date(2022, 3, d)
            for t in times:
                dt = dtc(d, t, self.market_tz)
                response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
                try:
                    self.assertEqual(response, answer)
                except:
                    print(f"interval={interval} dt={dt} discardTimes={discardTimes} week7days={week7days}")
                    print("response:") ; pprint(response)
                    print("answer:") ; pprint(answer)
                    raise

        # Sunday morning
        dt = dtc(date(2022, 3, 6), time(6), self.market_tz)
        discardTimes = False ; week7days = False
        answer = {"interval_open": dtc(date(2022, 3, 6), self.market_open, self.market_tz),  # Sunday open
                  "interval_close": dtc(date(2022, 3, 10), self.market_close, self.market_tz)}  # Thursday close
        response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} dt={dt}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise
        discardTimes = True ; week7days = False
        answer = {"interval_open": date(2022, 3, 13), "interval_close": date(2022, 3, 17)}  # next Sunday -> Thursday (holiday)
        response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} dt={dt}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise
        discardTimes = True ; week7days = True
        answer = {"interval_open": date(2022, 3, 7), "interval_close": date(2022, 3, 14)}  # next Monday -> next next Monday
        response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} dt={dt}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise
        return

        # Monday holiday
        dt = dtc(date(2022, 9, 27), time(12), self.market_tz)  # Tuesday
        discardTimes = False ; week7days = False
        answer = {"interval_open": dtc(date(2022, 9, 28), self.market_open, self.market_tz),  # Wednesday open
                  "interval_close": dtc(date(2022, 9, 29), self.market_close, self.market_tz)}  # Thursday close
        response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} dt={dt}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise
        discardTimes = True ; week7days = False
        answer = {"interval_open": date(2022, 9, 28), "interval_close": date(2022, 9, 30)}  # Wednesday -> Friday
        response = yfct.GetTimestampNextInterval(self.exchange, dt, interval, discardTimes, week7days)
        try:
            self.assertEqual(response, answer)
        except:
            print(f"interval={interval} dt={dt}")
            print("response:") ; pprint(response)
            print("answer:") ; pprint(answer)
            raise
        discardTimes = True ; week7days = True
        answer = {"interval_open": date(2022, 10, 2), "interval_close": date(2022, 10, 9)}  # next Monday -> next next Monday
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
        answer = {"interval_open": dtc(date(2022, 3, 13), self.market_open, self.market_tz),  # Sunday open
                  "interval_close": dtc(date(2022, 3, 16), self.market_close, self.market_tz)}  # Wednesday close (Thursday holiday)
        for d in range(6, 13):
            d = date(2022, 3, d)
            for t in times:
                if d == date(2022, 3, 6) and t < self.market_open:
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
        answer = {"interval_open": date(2022, 3, 13), "interval_close": date(2022, 3, 17)}  # Sunday -> Thursday (holiday)
        for d in range(6, 13):
            d = date(2022, 3, d)
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
        answer = {"interval_open": date(2022, 3, 14), "interval_close": date(2022, 3, 21)}  # Monday -> next Monday
        for d in range(6, 13):
            d = date(2022, 3, d)
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
        for d in range(6, 28):
            d = date(2022, 3, d)
            dts.append(dtc(d, time(9, 15), self.market_tz))
            dts.append(dtc(d, time(9, 30), self.market_tz))
            for h in range(10, 16):
                dts.append(dtc(d, time(h, 30), self.market_tz))
            dts.append(dtc(d, time(16, 1), self.market_tz))
            dts.append(dtc(d, time(20), self.market_tz))

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
        for d in range(6, 28):
            d = date(2022, 4, d)
            days.append(d)
            dts.append(dtc(d, time(9, 29), self.market_tz))
            dts.append(dtc(d, time(9, 30), self.market_tz))
            for h in range(10, 16):
                dts.append(dtc(d, time(h, 30), self.market_tz))
            dts.append(dtc(d, time(16, 1), self.market_tz))
            dts.append(dtc(d, time(20), self.market_tz))

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
        for d in range(6, 28):
            d = date(2022, 3, d)
            days.append(d)
            dts.append(dtc(d, time(9, 29), self.market_tz))
            dts.append(dtc(d, time(9, 30), self.market_tz))
            for h in range(10, 16):
                dts.append(dtc(d, time(h, 30), self.market_tz))
            dts.append(dtc(d, time(16, 1), self.market_tz))
            dts.append(dtc(d, time(20), self.market_tz))

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
