import unittest

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

class Test_Market_Schedules(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.market_close_time = time(hour=16, minute=0)

        self.monday = date(year=2022, month=2, day=7)
        self.friday = date(year=2022, month=2, day=11)

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
                        answer = {"interval_open":datetime.combine(self.monday+timedelta(days=weekday), time(hour=9, minute=30), self.market_tz)}
                        answer["interval_close"] = datetime.combine(self.monday+timedelta(days=weekday), time(hour=16), self.market_tz)
                    else:
                        answer = {"interval_open":datetime.combine(self.monday, time(hour=9, minute=30), self.market_tz)}
                        answer["interval_close"] = datetime.combine(self.friday, time(hour=16), self.market_tz)
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

        allowLateDailyData = True
        t = time(17, 10)
        interval = yfcd.Interval.Days1
        for weekday in range(5):
            dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
            intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, allowLateDailyData=allowLateDailyData)
            answer = {"interval_open":datetime.combine(self.monday+timedelta(days=weekday), time(hour=9, minute=30), self.market_tz)}
            answer["interval_close"] = datetime.combine(self.monday+timedelta(days=weekday), time(hour=16), self.market_tz)
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

        weeklyUseYahooDef = True
        intervals = []
        intervals.append(yfcd.Interval.Days5)
        intervals.append(yfcd.Interval.Week)
        times = []
        times.append(time(hour=8))
        times.append(time(hour=18))
        for i in range(len(intervals)):
            interval = intervals[i]
            for weekday in range(5):
                for t in times:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, weeklyUseYahooDef=True)
                    answer = {"interval_open":datetime.combine(self.monday, time(hour=0), self.market_tz)}
                    answer["interval_close"] = datetime.combine(self.friday, time(hour=23, minute=59, second=59), self.market_tz)
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
        intervals.append(yfcd.Interval.Days1)
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

                    if interval == yfcd.Interval.Days1:
                        # With allowLateDailyData=True result should be same
                        intervalRange = yfct.GetTimestampCurrentInterval(self.exchange, dt, interval, allowLateDailyData=True)
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
        ## 1) - Between Friday close and next Monday open:
        dates = []
        dates.append(datetime.combine(self.friday, time(hour=16, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=1), time(hour=10, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=2), time(hour=10, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=3), time(hour=9, minute=0), self.market_tz))
        for i in range(len(intervals)):
            interval = intervals[i]
            for dt in dates:
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
        ## 2) - Between Friday midnight and Sunday midnight because weeklyUseYahooDef=True
        dates = []
        dates.append(datetime.combine(self.friday+timedelta(days=1), time(hour=10, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=2), time(hour=10, minute=0), self.market_tz))
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

    # def test_GetTimestampMostRecentInterval_closed(self):
    #     ## Only test when market closed now, because if open then 
    #     ## yfct.GetTimestampMostRecentInterval() = yfct.GetTimestampCurrentInterval()

    #     t = time(hour=9, minute=29)
    #     intervals = []
    #     intervals.append(yfcd.Interval.Mins1)
    #     intervals.append(yfcd.Interval.Mins2)
    #     intervals.append(yfcd.Interval.Mins5)
    #     intervals.append(yfcd.Interval.Mins15)
    #     intervals.append(yfcd.Interval.Mins30)
    #     intervals.append(yfcd.Interval.Mins60)
    #     intervals.append(yfcd.Interval.Mins90)
    #     intervals.append(yfcd.Interval.Hours1)
    #     for weekday in range(7):
    #         dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #         answer_day = dt.date() - timedelta(days=1)
    #         if answer_day.weekday() == 6:
    #             answer_day -= timedelta(days=2)
    #         elif answer_day.weekday() == 5:
    #             answer_day -= timedelta(days=1)

    #         for i in range(len(intervals)):
    #             interval = intervals[i]
    #             interval_td = yfcd.intervalToTimedelta[interval]
    #             answer = {"interval_close":datetime.combine(answer_day, time(16, 0), self.market_tz)}
    #             answer["interval_open"] = answer["interval_close"] - interval_td

    #             intervalRange = yfct.GetTimestampMostRecentInterval(self.exchange, dt, interval)
    #             try:
    #                 self.assertEqual(intervalRange, answer)
    #             except:
    #                 print("interval = {0}".format(interval))
    #                 print("dt = {0} (weekday={1})".format(dt, dt.weekday()))
    #                 print("intervalRange:")
    #                 pprint(intervalRange)
    #                 print("answer:")
    #                 pprint(answer)
    #                 raise
    #     t = time(hour=16, minute=0)
    #     intervals = []
    #     intervals.append(yfcd.Interval.Mins1)
    #     intervals.append(yfcd.Interval.Mins2)
    #     intervals.append(yfcd.Interval.Mins5)
    #     intervals.append(yfcd.Interval.Mins15)
    #     intervals.append(yfcd.Interval.Mins30)
    #     intervals.append(yfcd.Interval.Mins60)
    #     intervals.append(yfcd.Interval.Mins90)
    #     intervals.append(yfcd.Interval.Hours1)
    #     for weekday in range(7):
    #         dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #         answer_day = dt.date()
    #         if answer_day.weekday() == 6:
    #             answer_day -= timedelta(days=2)
    #         elif answer_day.weekday() == 5:
    #             answer_day -= timedelta(days=1)

    #         for i in range(len(intervals)):
    #             interval = intervals[i]
    #             interval_td = yfcd.intervalToTimedelta[interval]
    #             answer = {"interval_close":datetime.combine(answer_day, time(16, 0), self.market_tz)}
    #             answer["interval_open"] = answer["interval_close"] - interval_td

    #             intervalRange = yfct.GetTimestampMostRecentInterval(self.exchange, dt, interval)
    #             try:
    #                 self.assertEqual(intervalRange, answer)
    #             except:
    #                 print("interval = {0}".format(interval))
    #                 print("dt = {0} (weekday={1})".format(dt, dt.weekday()))
    #                 print("intervalRange:")
    #                 pprint(intervalRange)
    #                 print("answer:")
    #                 pprint(answer)
    #                 raise

    #     t = time(hour=9, minute=29)
    #     interval = yfcd.Interval.Days1
    #     for weekday in range(7):
    #         dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #         answer_day = dt.date() - timedelta(days=1)
    #         if answer_day.weekday() == 6:
    #             answer_day -= timedelta(days=2)
    #         elif answer_day.weekday() == 5:
    #             answer_day -= timedelta(days=1)

    #         answer = {}
    #         answer["interval_open"]  = datetime.combine(answer_day, time(9, 30), self.market_tz)
    #         answer["interval_close"] = datetime.combine(answer_day, time(16, 0), self.market_tz)

    #         intervalRange = yfct.GetTimestampMostRecentInterval(self.exchange, dt, interval)
    #         try:
    #             self.assertEqual(intervalRange, answer)
    #         except:
    #             print("interval = {0}".format(interval))
    #             print("dt = {0} (weekday={1})".format(dt, dt.weekday()))
    #             print("intervalRange:")
    #             pprint(intervalRange)
    #             print("answer:")
    #             pprint(answer)
    #             raise
    #     t = time(hour=16, minute=00)
    #     interval = yfcd.Interval.Days1
    #     for weekday in range(7):
    #         dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #         answer_day = dt.date()
    #         if answer_day.weekday() == 6:
    #             answer_day -= timedelta(days=2)
    #         elif answer_day.weekday() == 5:
    #             answer_day -= timedelta(days=1)

    #         answer = {}
    #         answer["interval_open"]  = datetime.combine(answer_day, time(9, 30), self.market_tz)
    #         answer["interval_close"] = datetime.combine(answer_day, time(16, 0), self.market_tz)

    #         intervalRange = yfct.GetTimestampMostRecentInterval(self.exchange, dt, interval)
    #         try:
    #             self.assertEqual(intervalRange, answer)
    #         except:
    #             print("interval = {0}".format(interval))
    #             print("dt = {0} (weekday={1})".format(dt, dt.weekday()))
    #             print("intervalRange:")
    #             pprint(intervalRange)
    #             print("answer:")
    #             pprint(answer)
    #             raise

    #     ## Week-intervals
    #     ## Check that between Friday close and Monday open returns last week:
    #     intervals = [yfcd.Interval.Days5, yfcd.Interval.Week]
    #     dates = []
    #     dates.append(datetime.combine(self.friday,                   time(hour=17, minute=0), self.market_tz))
    #     dates.append(datetime.combine(self.friday+timedelta(days=1), time(hour=12, minute=0), self.market_tz))
    #     dates.append(datetime.combine(self.friday+timedelta(days=2), time(hour=12, minute=0), self.market_tz))
    #     dates.append(datetime.combine(self.friday+timedelta(days=3), time(hour=9, minute=29), self.market_tz))
    #     for dt in dates:
    #         answer_week_start_day = self.monday
    #         answer_week_end_day = self.monday + timedelta(days=4)
    #         answer = {}
    #         answer["interval_open"]  = datetime.combine(answer_week_start_day, time(9, 30), self.market_tz)
    #         answer["interval_close"] = datetime.combine(answer_week_end_day,   time(16, 0), self.market_tz)

    #         for interval in intervals:
    #             intervalRange = yfct.GetTimestampMostRecentInterval(self.exchange, dt, interval)
    #             try:
    #                 self.assertEqual(intervalRange, answer)
    #             except:
    #                 print("interval = {0}".format(interval))
    #                 print("dt = {0} (weekday={1})".format(dt, dt.weekday()))
    #                 print("intervalRange:")
    #                 pprint(intervalRange)
    #                 print("answer:")
    #                 pprint(answer)
    #                 raise

    # def test_GetTimestampNextInterval_open(self):
    #     ## If during day session, next interval is in same session:
    #     intervals = []
    #     intervals.append(yfcd.Interval.Mins1)
    #     intervals.append(yfcd.Interval.Mins2)
    #     intervals.append(yfcd.Interval.Mins5)
    #     intervals.append(yfcd.Interval.Mins15)
    #     intervals.append(yfcd.Interval.Mins30)
    #     intervals.append(yfcd.Interval.Mins60)
    #     intervals.append(yfcd.Interval.Hours1)
    #     times = []
    #     times.append(time(hour=9, minute=30))
    #     times.append(time(hour=13, minute=30))
    #     for i in range(len(intervals)):
    #         interval = intervals[i]
    #         interval_td = yfcd.intervalToTimedelta[interval]
    #         for weekday in range(5):
    #             for t in times:
    #                 ## dt at start of interval:
    #                 dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #                 intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
    #                 answer = {}
    #                 answer["interval_open"] = dt+interval_td
    #                 answer["interval_close"] = answer["interval_open"]+interval_td
    #                 if answer["interval_close"].time() > self.market_close_time:
    #                     answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
    #                 try:
    #                     self.assertEqual(intervalRange, answer)
    #                 except:
    #                     print("interval = {0}".format(interval))
    #                     print("dt = {0}".format(dt))
    #                     print("intervalRange:")
    #                     pprint(intervalRange)
    #                     print("answer:")
    #                     pprint(answer)
    #                     raise

    #                 ## dt in middle of interval:
    #                 dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #                 dt += interval_td * 0.5
    #                 market_close_dt = datetime.combine(dt.date(), self.market_close_time, self.market_tz)
    #                 if dt >= market_close_dt:
    #                     dt = market_close_dt - timedelta(minutes=1)
    #                 intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
    #                 answer = {}
    #                 answer["interval_open"] = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz) +interval_td
    #                 answer["interval_close"] = answer["interval_open"]+interval_td
    #                 if answer["interval_close"].time() > self.market_close_time:
    #                     answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
    #                 try:
    #                     self.assertEqual(intervalRange, answer)
    #                 except:
    #                     print("interval = {0}".format(interval))
    #                     print("dt = {0}".format(dt))
    #                     print("intervalRange:")
    #                     pprint(intervalRange)
    #                     print("answer:")
    #                     pprint(answer)
    #                     raise
    #     interval = yfcd.Interval.Mins90
    #     times = []
    #     answers = []
    #     times.append(time(hour=9, minute=30))  ; answers.append({"interval_open":time(hour=11, minute=0), "interval_close":time(hour=12, minute=30)})
    #     times.append(time(hour=9, minute=45))  ; answers.append({"interval_open":time(hour=11, minute=0), "interval_close":time(hour=12, minute=30)})
    #     times.append(time(hour=13, minute=30)) ; answers.append({"interval_open":time(hour=14, minute=0), "interval_close":time(hour=15, minute=30)})
    #     times.append(time(hour=13, minute=45)) ; answers.append({"interval_open":time(hour=14, minute=0), "interval_close":time(hour=15, minute=30)})
    #     for i in range(len(times)):
    #         t = times[i]
    #         answer_t = answers[i]
    #         for weekday in range(5):
    #             dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #             intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
    #             answer = {}
    #             answer["interval_open"]  = datetime.combine(self.monday+timedelta(days=weekday), answer_t["interval_open"], self.market_tz)
    #             answer["interval_close"] = datetime.combine(self.monday+timedelta(days=weekday), answer_t["interval_close"], self.market_tz)
    #             try:
    #                 self.assertEqual(intervalRange, answer)
    #             except:
    #                 print("interval = {0}".format(interval))
    #                 print("dt = {0}".format(dt))
    #                 print("intervalRange:")
    #                 pprint(intervalRange)
    #                 print("answer:")
    #                 pprint(answer)
    #                 raise

    #     # If during the final interval of session, next interval is next day first interval
    #     intervals = []
    #     intervals.append(yfcd.Interval.Mins1)
    #     intervals.append(yfcd.Interval.Mins2)
    #     intervals.append(yfcd.Interval.Mins5)
    #     intervals.append(yfcd.Interval.Mins15)
    #     intervals.append(yfcd.Interval.Mins30)
    #     intervals.append(yfcd.Interval.Mins60)
    #     intervals.append(yfcd.Interval.Hours1)
    #     intervals.append(yfcd.Interval.Mins90)
    #     for i in range(len(intervals)):
    #         interval = intervals[i]
    #         interval_td = yfcd.intervalToTimedelta[interval]

    #         times = []
    #         times.append(time(hour=15, minute=59))
    #         m = min(15, interval_td.seconds//60//2)
    #         times.append(time(hour=15, minute=59-m))
    #         for weekday in range(5):
    #             for t in times:
    #                 dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #                 intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

    #                 next_day = dt.date() + timedelta(days=1)
    #                 if next_day.weekday() == 5:
    #                     next_day += timedelta(days=2)
    #                 elif next_day.weekday() == 6:
    #                     next_day += timedelta(days=1)
    #                 answer = {}
    #                 answer["interval_open"] = datetime.combine(next_day, time(hour=9, minute=30), self.market_tz)
    #                 answer["interval_close"] = answer["interval_open"]+interval_td
    #                 if answer["interval_close"].time() > self.market_close_time:
    #                     answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
    #                 try:
    #                     self.assertEqual(intervalRange, answer)
    #                 except:
    #                     print("interval = {0}".format(interval))
    #                     print("dt = {0}".format(dt))
    #                     print("intervalRange:")
    #                     pprint(intervalRange)
    #                     print("answer:")
    #                     pprint(answer)
    #                     raise

    #     # If a day/week interval, is next working day/week regardless of today
    #     intervals = []
    #     intervals.append(yfcd.Interval.Days1)
    #     intervals.append(yfcd.Interval.Days5)
    #     intervals.append(yfcd.Interval.Week)
    #     times = []
    #     times.append(time(hour=9, minute=30))
    #     times.append(time(hour=15, minute=30))
    #     for i in range(len(intervals)):
    #         interval = intervals[i]
    #         for weekday in range(4):
    #             for t in times:
    #                 dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #                 intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
    #                 answer = {}
    #                 if interval == yfcd.Interval.Days1:
    #                     # Next day
    #                     answer["interval_open"]  = datetime.combine(self.monday+timedelta(days=weekday+1), time(hour=9, minute=30), self.market_tz)
    #                     answer["interval_close"] = datetime.combine(self.monday+timedelta(days=weekday+1), time(hour=16), self.market_tz)
    #                 else:
    #                     # Next week
    #                     answer["interval_open"]  = datetime.combine(self.monday+timedelta(days=7), time(hour=9, minute=30), self.market_tz)
    #                     answer["interval_close"] = datetime.combine(self.friday+timedelta(days=7), time(hour=16), self.market_tz)
    #                 try:
    #                     self.assertEqual(intervalRange, answer)
    #                 except:
    #                     print("interval = {0}".format(interval))
    #                     print("dt = {0}".format(dt))
    #                     print("intervalRange:")
    #                     pprint(intervalRange)
    #                     print("answer:")
    #                     pprint(answer)
    #                     raise
    #         weekday = 5
    #         dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #         intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
    #         answer = {}
    #         answer["interval_open"]  = datetime.combine(self.monday+timedelta(days=7), time(hour=9, minute=30), self.market_tz)
    #         if interval == yfcd.Interval.Days1:
    #             answer["interval_close"] = datetime.combine(self.monday+timedelta(days=7), time(hour=16), self.market_tz)
    #         else:
    #             answer["interval_close"] = datetime.combine(self.friday+timedelta(days=7), time(hour=16), self.market_tz)
    #         try:
    #             self.assertEqual(intervalRange, answer)
    #         except:
    #             print("interval = {0}".format(interval))
    #             print("dt = {0}".format(dt))
    #             print("intervalRange = {0}".format(intervalRange))
    #             print("answer = {0}".format(answer))
    #             raise

    # def test_GetTimestampNextInterval_closed(self):
    #     ## If in morning before market open, next interval next session first interval:
    #     intervals = []
    #     intervals.append(yfcd.Interval.Mins1)
    #     intervals.append(yfcd.Interval.Mins2)
    #     intervals.append(yfcd.Interval.Mins5)
    #     intervals.append(yfcd.Interval.Mins15)
    #     intervals.append(yfcd.Interval.Mins30)
    #     intervals.append(yfcd.Interval.Mins60)
    #     intervals.append(yfcd.Interval.Mins90)
    #     intervals.append(yfcd.Interval.Hours1)
    #     intervals.append(yfcd.Interval.Days1)
    #     t = time(hour=9, minute=0)
    #     for i in range(len(intervals)):
    #         interval = intervals[i]
    #         interval_td = yfcd.intervalToTimedelta[interval]
    #         for weekday in range(5):
    #             dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #             intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

    #             answer_day = dt.date()
    #             answer = {}
    #             answer["interval_open"]  = datetime.combine(answer_day, time(hour=9, minute=30), self.market_tz)
    #             if interval == yfcd.Interval.Days1:
    #                 answer["interval_close"] = datetime.combine(answer_day, time(hour=16, minute=0), self.market_tz)
    #             else:
    #                 answer["interval_close"] = answer["interval_open"]+interval_td
    #                 if answer["interval_close"].time() > self.market_close_time:
    #                     answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
    #             try:
    #                 self.assertEqual(intervalRange, answer)
    #             except:
    #                 print("interval = {0}".format(interval))
    #                 print("dt = {0}".format(dt))
    #                 print("intervalRange:")
    #                 pprint(intervalRange)
    #                 print("answer:")
    #                 pprint(answer)
    #                 raise

    #     ## If in afternoon after market close, next interval is next session first interval:
    #     intervals = []
    #     intervals.append(yfcd.Interval.Mins1)
    #     intervals.append(yfcd.Interval.Mins2)
    #     intervals.append(yfcd.Interval.Mins5)
    #     intervals.append(yfcd.Interval.Mins15)
    #     intervals.append(yfcd.Interval.Mins30)
    #     intervals.append(yfcd.Interval.Mins60)
    #     intervals.append(yfcd.Interval.Mins90)
    #     intervals.append(yfcd.Interval.Hours1)
    #     intervals.append(yfcd.Interval.Days1)
    #     t = time(hour=16, minute=0)
    #     for i in range(len(intervals)):
    #         interval = intervals[i]
    #         interval_td = yfcd.intervalToTimedelta[interval]
    #         for weekday in range(5):
    #             dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #             intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)

    #             answer_day = dt.date()+timedelta(days=1)
    #             if answer_day.weekday() == 5:
    #                 answer_day += timedelta(days=2)
    #             elif answer_day.weekday() == 6:
    #                 answer_day += timedelta(days=1)
    #             answer = {}
    #             answer["interval_open"]  = datetime.combine(answer_day, time(hour=9, minute=30), self.market_tz)
    #             if interval == yfcd.Interval.Days1:
    #                 answer["interval_close"] = datetime.combine(answer_day, time(hour=16, minute=0), self.market_tz)
    #             else:
    #                 answer["interval_close"] = answer["interval_open"]+interval_td
    #             try:
    #                 self.assertEqual(intervalRange, answer)
    #             except:
    #                 print("interval = {0}".format(interval))
    #                 print("dt = {0}".format(dt))
    #                 print("intervalRange:")
    #                 pprint(intervalRange)
    #                 print("answer:")
    #                 pprint(answer)
    #                 raise

    #     # If a week interval, is next week regardless of today
    #     intervals = []
    #     intervals.append(yfcd.Interval.Days5)
    #     intervals.append(yfcd.Interval.Week)
    #     dates = []
    #     dates.append(datetime.combine(self.friday,                   time(hour=16, minute=0), self.market_tz))
    #     dates.append(datetime.combine(self.friday+timedelta(days=1), time(hour=12, minute=0), self.market_tz))
    #     dates.append(datetime.combine(self.friday+timedelta(days=2), time(hour=12, minute=0), self.market_tz))
    #     dates.append(datetime.combine(self.friday+timedelta(days=3), time(hour=9,  minute=0), self.market_tz))
    #     for i in range(len(intervals)):
    #         interval = intervals[i]
    #         for dt in dates:
    #             dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
    #             intervalRange = yfct.GetTimestampNextInterval(self.exchange, dt, interval)
    #             answer = {}
    #             answer["interval_open"]  = datetime.combine(self.monday+timedelta(days=7), time(hour=9, minute=30), self.market_tz)
    #             answer["interval_close"] = datetime.combine(self.friday+timedelta(days=7), time(hour=16, minute=0), self.market_tz)
    #             try:
    #                 self.assertEqual(intervalRange, answer)
    #             except:
    #                 print("interval = {0}".format(interval))
    #                 print("dt = {0}".format(dt))
    #                 print("intervalRange:")
    #                 pprint(intervalRange)
    #                 print("answer:")
    #                 pprint(answer)
    #                 raise

    def test_GetTimestampCurrentInterval_delay(self):
        ## If market pricing data is delayed, then fetching price data near very end of interval
        ## can appear as after interval
        exchange = "LSE" # 15min delay
        delay = timedelta(minutes=15)
        market_tz = ZoneInfo("Europe/London")
        interval = yfcd.Interval.Days1

        ## Just before market close:
        dt = datetime.combine(self.monday, self.market_close_time, market_tz) - timedelta(minutes=1)
        ## ... so replicate price delay:
        dt += delay
        answer = {"interval_open":datetime.combine(self.monday, time(8), market_tz),
                "interval_close":datetime.combine(self.monday, time(16,30), market_tz)}
        intervalRange = yfct.GetTimestampCurrentInterval(exchange, dt, interval)
        try:
            self.assertEqual(intervalRange, answer)
        except:
            print("answer:")
            pprint(answer)
            print("intervalRange:")
            pprint(intervalRange)
            raise

if __name__ == '__main__':
    unittest.main()
